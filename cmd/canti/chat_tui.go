package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/charmbracelet/x/ansi"
	"github.com/charmbracelet/x/term"
)

const chatDisplayLimit = 64 << 10
const chatTranscriptLimit = 64 << 10

type chatDisplay struct {
	mu      sync.Mutex
	text    string
	omitted bool
}

func (d *chatDisplay) Write(p []byte) (int, error) {
	n := len(p)
	omitted := n > chatDisplayLimit
	if omitted {
		p = p[n-chatDisplayLimit:]
	}
	s := clean(ansi.Strip(string(p)))
	d.mu.Lock()
	defer d.mu.Unlock()
	d.omitted = d.omitted || omitted
	d.text += s
	if len(d.text) > chatDisplayLimit {
		d.text = boundedTail(d.text, chatDisplayLimit)
		d.omitted = true
	}
	return n, nil
}

func (d *chatDisplay) Drain() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.text
	if d.omitted {
		s = "… earlier output omitted\n" + s
	}
	d.text, d.omitted = "", false
	return s
}

func boundedTail(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[len(s)-limit:]
	for len(s) > 0 && !utf8.RuneStart(s[0]) {
		s = s[1:]
	}
	return s
}

type chatDone struct {
	code int
	err  error
	sig  os.Signal
}

type chatSignal struct{ sig os.Signal }
type chatOutputFailure struct{ err error }
type chatTick time.Time

type chatTUI struct {
	session           string
	prepare           func()
	submit            func(string) (int, error, os.Signal)
	interrupt         func(os.Signal)
	signals           <-chan os.Signal
	outputFailure     <-chan error
	display           *chatDisplay
	inputGate         *chatInputGate
	editor            textarea.Model
	steps             viewport.Model
	transcript        string
	transcriptOmitted bool
	width, height     int
	busy              bool
	stopping          bool
	stopCount         int
	exitAfterRun      bool
	code              int
	err               error
}

func newChatTUI(session string, prepare func(), submit func(string) (int, error, os.Signal), interrupt func(os.Signal), signals <-chan os.Signal, outputFailure <-chan error, display *chatDisplay) chatTUI {
	editor := textarea.New()
	editor.Placeholder = "Write a message"
	editor.Prompt = "› "
	editor.CharLimit = harness.DefaultPromptLimits().Goal
	editor.SetHeight(3)
	editor.SetWidth(76)
	editor.Focus()
	return chatTUI{
		session: session, prepare: prepare, submit: submit, interrupt: interrupt,
		signals: signals, outputFailure: outputFailure, display: display,
		editor: editor, steps: viewport.New(viewport.WithWidth(80), viewport.WithHeight(12)),
		width: 80, height: 24,
	}
}

func (m chatTUI) Init() tea.Cmd {
	return chatPoll()
}

func chatPoll() tea.Cmd {
	return tea.Tick(75*time.Millisecond, func(t time.Time) tea.Msg { return chatTick(t) })
}

func (m chatTUI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = max(20, msg.Width), max(8, msg.Height)
		m.editor.SetWidth(m.width - 2)
		m.steps.SetWidth(m.width)
		m.steps.SetHeight(max(1, m.height-8))
		return m, nil
	case chatTick:
		m.drainOutput()
		select {
		case sig := <-m.signals:
			return m.Update(chatSignal{sig: sig})
		default:
		}
		select {
		case err := <-m.outputFailure:
			return m.Update(chatOutputFailure{err: err})
		default:
		}
		return m, chatPoll()
	case chatSignal:
		if m.busy {
			m.stop(msg.sig)
			if msg.sig == syscall.SIGTERM {
				m.exitAfterRun = true
			}
			return m, chatPoll()
		}
		m.code = exitOK
		if msg.sig == syscall.SIGTERM {
			m.code = exitStopped
		}
		return m, tea.Quit
	case chatOutputFailure:
		if msg.err == nil {
			return m, nil
		}
		m.err = msg.err
		m.code = exitError
		if m.busy {
			m.exitAfterRun = true
			m.cancel()
			return m, chatPoll()
		}
		return m, tea.Quit
	case chatInputFailure:
		m.err = fmt.Errorf("terminal input: %w", msg.err)
		m.code = exitError
		if m.busy {
			m.exitAfterRun = true
			m.cancel()
			return m, chatPoll()
		}
		return m, tea.Quit
	case chatDone:
		m.drainOutput()
		m.busy, m.stopping = false, false
		m.stopCount = 0
		if m.err == nil {
			m.code = msg.code
		}
		if msg.err != nil {
			m.addTranscript("\nerror: " + msg.err.Error() + "\n")
		}
		if m.exitAfterRun || msg.sig != nil && msg.sig != os.Interrupt || msg.err != nil && msg.code != exitUsage || msg.code != exitOK && msg.code != exitStopped && msg.code != exitUsage {
			if m.err == nil {
				m.err = msg.err
			}
			return m, tea.Quit
		}
		m.code = exitOK
		if m.inputGate != nil {
			if err := m.inputGate.reopen(); err != nil {
				m.err = fmt.Errorf("resetting terminal input: %w", err)
				m.code = exitError
				return m, tea.Quit
			}
		}
		m.busy = false
		return m, nil
	case chatKeys:
		stale := m.inputGate != nil && msg.epoch != m.inputGate.currentEpoch()
		var cmds []tea.Cmd
		for _, event := range msg.events {
			if stale {
				key, ok := event.(tea.KeyPressMsg)
				if !ok || key.String() != "ctrl+c" {
					continue
				}
			}
			idleQuit := false
			if key, ok := event.(tea.KeyPressMsg); ok {
				idleQuit = !m.busy && key.String() == "ctrl+c"
			}
			next, cmd := m.Update(event)
			m = next.(chatTUI)
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
			if idleQuit {
				break
			}
		}
		return m, tea.Batch(cmds...)
	case chatPasteOverflow:
		if !m.busy {
			m.addTranscript("\nerror: pasted input exceeds the goal limit\n")
		}
		return m, nil
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c":
			if m.busy {
				m.stop(os.Interrupt)
				return m, nil
			}
			return m, tea.Quit
		case "enter":
			if m.busy {
				return m, nil
			}
			goal := strings.TrimSpace(m.editor.Value())
			if goal == "" {
				return m, nil
			}
			if m.prepare != nil {
				m.prepare()
			}
			m.addTranscript("\n› " + goal + "\n")
			m.editor.SetValue("")
			if m.inputGate != nil {
				m.inputGate.enterBusy()
			}
			m.busy = true
			m.stopCount = 0
			return m, func() tea.Msg {
				if m.submit == nil {
					return chatDone{code: exitError, err: errors.New("chat submit is unavailable")}
				}
				code, err, sig := m.submit(goal)
				return chatDone{code: code, err: err, sig: sig}
			}
		case "alt+enter":
			if !m.busy {
				m.editor.InsertString("\n")
			}
			return m, nil
		}
	}
	if key, ok := msg.(tea.KeyPressMsg); ok && (key.String() == "pgup" || key.String() == "pgdown") {
		var cmd tea.Cmd
		m.steps, cmd = m.steps.Update(msg)
		return m, cmd
	}
	if m.busy {
		return m, nil
	}
	var cmd tea.Cmd
	m.editor, cmd = m.editor.Update(msg)
	return m, cmd
}

func (m *chatTUI) stop(sig os.Signal) {
	if m.stopCount >= 2 {
		return
	}
	m.stopCount++
	if m.stopCount == 1 && m.stopping {
		return
	}
	m.stopping = true
	if m.interrupt != nil {
		m.interrupt(sig)
	}
}

func (m *chatTUI) cancel() {
	if !m.stopping {
		m.stopping = true
		if m.interrupt != nil {
			m.interrupt(os.Interrupt)
		}
	}
}

func (m *chatTUI) drainOutput() {
	if m.display != nil {
		m.addTranscript(m.display.Drain())
	}
}

func (m *chatTUI) addTranscript(s string) {
	if s == "" {
		return
	}
	atBottom := m.steps.AtBottom()
	if len(s) > chatTranscriptLimit {
		s = boundedTail(s, chatTranscriptLimit)
		m.transcriptOmitted = true
	}
	m.transcript += clean(ansi.Strip(s))
	if len(m.transcript) > chatTranscriptLimit {
		m.transcript = boundedTail(m.transcript, chatTranscriptLimit)
		m.transcriptOmitted = true
	}
	content := m.transcript
	if m.transcriptOmitted {
		content = "… earlier transcript omitted\n" + content
	}
	m.steps.SetContent(content)
	if atBottom {
		m.steps.GotoBottom()
	}
}

func (m chatTUI) View() tea.View {
	status := "ready"
	if m.busy {
		status = "running · new input discarded"
	}
	if m.stopping {
		status = "stopping · waiting for cleanup"
	}
	if m.inputGate != nil && m.inputGate.discarding() {
		status = "discarding incomplete terminal input"
	}
	content := "canti chat · session " + m.session + "\n" + status + " · current launch flags apply to each new input\n\n" + m.steps.View() + "\n\n" + m.editor.View() + "\nEnter send · Alt+Enter newline · PgUp/PgDn scroll · Ctrl+C stop/exit"
	v := tea.NewView(content)
	v.AltScreen = true
	return v
}

func runChatTUI(session string, prepare func(), submit func(string) (int, error, os.Signal), interrupt func(os.Signal), signals <-chan os.Signal, display *chatDisplay, outputFailure <-chan error, input io.Reader, terminal io.Writer) (int, error) {
	f, ok := input.(*os.File)
	if !ok || !term.IsTerminal(f.Fd()) {
		return exitError, errors.New("chat terminal input is unavailable")
	}
	var gate sync.Mutex
	var active sync.WaitGroup
	closed := false
	running := false
	interrupted := false
	wrappedPrepare := func() {
		gate.Lock()
		interrupted = false
		gate.Unlock()
		if prepare != nil {
			prepare()
		}
	}
	wrappedInterrupt := func(sig os.Signal) {
		gate.Lock()
		interrupted = true
		gate.Unlock()
		if interrupt != nil {
			interrupt(sig)
		}
	}
	wrappedSubmit := func(goal string) (int, error, os.Signal) {
		gate.Lock()
		if closed {
			gate.Unlock()
			return exitError, errors.New("chat is closing"), nil
		}
		active.Add(1)
		running = true
		gate.Unlock()
		defer func() {
			gate.Lock()
			running = false
			gate.Unlock()
			active.Done()
		}()
		return submit(goal)
	}
	m := newChatTUI(session, wrappedPrepare, wrappedSubmit, wrappedInterrupt, signals, outputFailure, display)
	var p *tea.Program
	ready := make(chan struct{})
	gateInput, stopInput, err := startChatInput(f, func(msg tea.Msg) {
		<-ready
		p.Send(msg)
	})
	if err != nil {
		return exitError, err
	}
	m.inputGate = gateInput
	if out, ok := terminal.(*chatOutput); ok {
		gateInput.mu.Lock()
		gateInput.outputFD = out.fd
		gateInput.mu.Unlock()
		out.gate = gateInput
	}
	p = tea.NewProgram(m, tea.WithInput(nil), tea.WithOutput(terminal), tea.WithWindowSize(80, 24), tea.WithoutSignalHandler())
	close(ready)
	final, err := p.Run()
	inputErr := stopInput()
	if out, ok := terminal.(*chatOutput); ok {
		out.mu.Lock()
		out.gate = nil
		out.mu.Unlock()
	}
	gate.Lock()
	closed = true
	needsStop := running && !interrupted
	gate.Unlock()
	if needsStop && interrupt != nil {
		interrupt(os.Interrupt)
	}
	active.Wait()
	if err != nil {
		return exitError, err
	}
	if inputErr != nil {
		return exitError, inputErr
	}
	m = final.(chatTUI)
	m.drainOutput()
	transcript := m.transcript
	if m.transcriptOmitted {
		transcript = "… earlier transcript omitted\n" + transcript
	}
	if transcript != "" {
		if _, err := fmt.Fprint(terminal, transcript); err != nil {
			return exitError, err
		}
	}
	return m.code, m.err
}
