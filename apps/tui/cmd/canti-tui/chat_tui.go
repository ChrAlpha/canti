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
	abandon           func() // detaches from the active run; nil stops it
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

func newChatTUI(session string, goalLimit int, prepare func(), submit func(string) (int, error, os.Signal), interrupt func(os.Signal), signals <-chan os.Signal, outputFailure <-chan error, display *chatDisplay) chatTUI {
	editor := textarea.New()
	editor.Placeholder = "Write a message"
	editor.Prompt = "› "
	editor.CharLimit = goalLimit
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
		return m.tick()
	case chatSignal:
		return m.signal(msg.sig)
	case chatOutputFailure:
		if msg.err == nil {
			return m, nil
		}
		return m.fail(msg.err)
	case chatInputFailure:
		return m.fail(fmt.Errorf("terminal input: %w", msg.err))
	case chatDone:
		return m.done(msg)
	case chatKeys:
		return m.keys(msg)
	case chatPasteOverflow:
		if !m.busy {
			m.addTranscript("\nerror: pasted input exceeds the goal limit\n")
		}
		return m, nil
	case tea.KeyPressMsg:
		if next, cmd, handled := m.key(msg); handled {
			return next, cmd
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

func (m chatTUI) tick() (tea.Model, tea.Cmd) {
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
}

func (m chatTUI) signal(sig os.Signal) (tea.Model, tea.Cmd) {
	if sig == syscall.SIGHUP {
		return m.Update(chatInputFailure{err: errors.New("hung up")})
	}
	if m.busy {
		m.stop(sig)
		if sig == syscall.SIGTERM {
			m.exitAfterRun = true
		}
		return m, chatPoll()
	}
	m.code = exitOK
	if sig == syscall.SIGTERM {
		m.code = exitStopped
	}
	return m, tea.Quit
}

func (m chatTUI) fail(err error) (tea.Model, tea.Cmd) {
	m.err = err
	m.code = exitError
	if m.busy {
		m.exitAfterRun = true
		m.cancel()
		return m, chatPoll()
	}
	return m, tea.Quit
}

func (m chatTUI) done(msg chatDone) (tea.Model, tea.Cmd) {
	m.drainOutput()
	m.busy, m.stopping = false, false
	m.stopCount = 0
	if m.err == nil {
		m.code = msg.code
	}
	if msg.err != nil {
		m.addTranscript("\nerror: " + msg.err.Error() + "\n")
	}
	if m.exitAfterRun || msg.sig != nil && msg.sig != os.Interrupt || msg.err != nil && msg.code != exitUsage || msg.code != exitOK && msg.code != exitStopped && msg.code != exitUsage || errors.As(msg.err, new(errNoInputFits)) {
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
}

func (m chatTUI) keys(msg chatKeys) (tea.Model, tea.Cmd) {
	stale := m.inputGate != nil && msg.epoch != m.inputGate.currentEpoch()
	var cmds []tea.Cmd
	for _, event := range msg.events {
		key, isKey := event.(tea.KeyPressMsg)
		if stale && (!isKey || key.String() != "ctrl+c") {
			continue
		}
		idleQuit := isKey && !m.busy && key.String() == "ctrl+c"
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
}

func (m chatTUI) key(msg tea.KeyPressMsg) (tea.Model, tea.Cmd, bool) {
	switch msg.String() {
	case "ctrl+c":
		if m.busy {
			m.stop(os.Interrupt)
			return m, nil, true
		}
		return m, tea.Quit, true
	case "enter":
		if m.busy {
			return m, nil, true
		}
		goal := strings.TrimSpace(m.editor.Value())
		if goal == "" {
			return m, nil, true
		}
		return m.start(goal)
	case "alt+enter":
		if !m.busy {
			m.editor.InsertString("\n")
		}
		return m, nil, true
	}
	return m, nil, false
}

func (m chatTUI) start(goal string) (tea.Model, tea.Cmd, bool) {
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
	}, true
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

// cancel ends the active run when the chat can no longer show it: it stops
// the run, or, attached to a host process that keeps driving it, detaches.
func (m *chatTUI) cancel() {
	if !m.stopping {
		m.stopping = true
		switch {
		case m.abandon != nil:
			m.abandon()
		case m.interrupt != nil:
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

// runChatTUI runs the chat terminal UI. A chat that ends while a run is
// active calls abandon, or with a nil abandon interrupts the run, as it does
// when it can no longer show the run.
func runChatTUI(session string, goalLimit int, prepare func(), submit func(string) (int, error, os.Signal), interrupt func(os.Signal), abandon func(), signals <-chan os.Signal, display *chatDisplay, outputFailure <-chan error, input io.Reader, terminal io.Writer) (int, error) {
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
	m := newChatTUI(session, goalLimit, wrappedPrepare, wrappedSubmit, wrappedInterrupt, signals, outputFailure, display)
	if abandon != nil {
		m.abandon = func() {
			gate.Lock()
			interrupted = true
			gate.Unlock()
			abandon()
		}
	}
	var p *tea.Program
	ready := make(chan struct{})
	gateInput, stopInput, err := startChatInput(f, goalLimit, func(msg tea.Msg) {
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
	switch {
	case needsStop && abandon != nil:
		abandon()
	case needsStop && interrupt != nil:
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
