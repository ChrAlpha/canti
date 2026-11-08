// Package ui presents a disposable projection of a chat session.
package ui

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unicode/utf8"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/apps/tui/internal/present"
	"github.com/ChrAlpha/canti/apps/tui/internal/session"
)

const transcriptBytes = 64 << 10
const transcriptEntries = 256

type phase uint8

const (
	ready phase = iota
	submitting
	running
	stopping
	unconfirmed
	closing
)

type fact struct {
	turn  *session.Turn
	event session.Event
}
type signalMsg struct{ signal os.Signal }
type outputError struct{ err error }

type Model struct {
	controller        *session.Controller
	ctx               context.Context
	turn              *session.Turn
	pending, rejected *session.Snapshot
	editor            textarea.Model
	viewport          viewport.Model
	phase             phase
	entries           []string
	bytes             int
	omitted           bool
	final, notice     string
	width, height     int
	exitAfter         bool
	code              int
	err               error
}

func New(ctx context.Context, c *session.Controller) Model {
	editor := textarea.New()
	editor.Placeholder = "Write a message"
	editor.Prompt = "› "
	editor.CharLimit = c.GoalLimit
	editor.SetHeight(3)
	editor.SetWidth(76)
	editor.Focus()
	return Model{controller: c, ctx: ctx, editor: editor, viewport: viewport.New(viewport.WithWidth(80), viewport.WithHeight(12)), width: 80, height: 24}
}
func (m Model) Init() tea.Cmd { return textarea.Blink }
func next(t *session.Turn) tea.Cmd {
	return func() tea.Msg {
		e, err := t.Next(context.Background())
		if err != nil {
			return nil
		}
		return fact{t, e}
	}
}
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = max(20, msg.Width), max(8, msg.Height)
		m.editor.SetWidth(m.width - 2)
		m.viewport.SetWidth(m.width)
		m.viewport.SetHeight(max(1, m.height-9))
		return m, nil
	case fact:
		return m.fact(msg)
	case signalMsg:
		return m.signal(msg.signal)
	case outputError:
		return m.lost(msg.err)
	case tea.InputClosedMsg:
		return m.lost(msg.Err)
	case tea.PasteOverflowMsg:
		m.notice = fmt.Sprintf("Paste rejected: input exceeds %d bytes; draft preserved", msg.Limit)
		return m, nil
	case tea.KeyPressMsg:
		if handled, cmd := m.key(msg); handled {
			return m, cmd
		}
	case tea.PasteMsg:
		msg.Content = present.Clean(msg.Content)
		if len(m.editor.Value())+len(msg.Content) > m.controller.GoalLimit {
			m.notice = "Paste rejected: draft would exceed the input byte limit"
			return m, nil
		}
		return m.edit(msg)
	}
	return m.edit(msg)
}
func (m Model) edit(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && len(m.editor.Value())+len(key.Text) > m.controller.GoalLimit {
		m.notice = "Input byte limit reached"
		return m, nil
	}
	before := m.editor.Value()
	var cmd tea.Cmd
	m.editor, cmd = m.editor.Update(msg)
	if len(m.editor.Value()) > m.controller.GoalLimit {
		m.editor.SetValue(before)
		m.notice = "Input byte limit reached"
	}
	return m, cmd
}
func (m *Model) key(k tea.KeyPressMsg) (bool, tea.Cmd) {
	switch k.String() {
	case "ctrl+c":
		if m.turn != nil {
			m.phase = stopping
			m.turn.Interrupt()
			return true, nil
		}
		m.phase = closing
		return true, tea.Quit
	case "enter":
		if m.phase != ready {
			m.notice = "Send unavailable until the current input completes; draft retained"
			return true, nil
		}
		goal := strings.TrimSpace(m.editor.Value())
		if goal == "" {
			return true, nil
		}
		return true, m.start(session.NewSnapshot(goal), true)
	case "alt+enter":
		if len(m.editor.Value()) < m.controller.GoalLimit {
			m.editor.InsertString("\n")
		}
		return true, nil
	case "ctrl+r":
		if m.phase == unconfirmed && m.pending != nil {
			return true, m.start(*m.pending, false)
		}
		if m.rejected != nil {
			if m.editor.Value() != "" {
				m.notice = "Clear the current draft before restoring rejected input"
			} else {
				m.editor.SetValue(m.rejected.Goal)
				m.rejected = nil
				m.notice = "Rejected input restored for editing"
			}
		}
		return true, nil
	case "pgup", "pgdown":
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(k)
		return true, cmd
	}
	return false, nil
}
func (m *Model) start(s session.Snapshot, clear bool) tea.Cmd {
	t, err := m.controller.Start(m.ctx, s)
	if err != nil {
		m.notice = err.Error()
		return nil
	}
	m.turn = t
	m.pending = &s
	m.phase = submitting
	m.notice = ""
	m.final = ""
	if clear {
		m.editor.SetValue("")
		m.add("\n› " + s.Goal + "\n")
	}
	return next(t)
}
func (m Model) fact(f fact) (tea.Model, tea.Cmd) {
	if f.turn != m.turn {
		return m, nil
	}
	e := f.event
	switch {
	case e.Ack != nil:
		if m.phase == submitting {
			m.phase = running
		}
		if len(e.Ack.Differs) > 0 {
			m.add("Input replay keeps its original settings: " + strings.Join(e.Ack.Differs, ", ") + "\n")
		}
	case e.Diagnostic != "":
		m.add(e.Diagnostic + "\n")
	case e.Item != nil:
		var b strings.Builder
		present.Item(&b, *e.Item)
		m.add(b.String())
	case e.Omitted > 0:
		m.add(fmt.Sprintf("… %d progress entries omitted\n", e.Omitted))
	case e.Result != nil:
		return m.complete(*e.Result)
	}
	return m, next(m.turn)
}
func (m Model) complete(out session.Result) (tea.Model, tea.Cmd) {
	m.turn = nil
	answer, note := present.Result(out)
	m.final = answer + note
	m.add(m.final)
	if m.err == nil {
		m.code = out.Code
	}
	switch {
	case out.Uncertain:
		m.phase = unconfirmed
		m.notice = "Acceptance is unknown; Ctrl+R retries the same InputID. New draft preserved."
	case out.Rejected:
		m.phase = ready
		m.rejected = m.pending
		m.pending = nil
		m.notice = "Input rejected; Ctrl+R restores it into an empty draft"
	default:
		m.phase = ready
		m.pending = nil
	}
	if m.exitAfter || out.Interrupted || out.NoInputFits || out.Code == session.Resolution || out.Code == session.Active || out.Err != nil && !out.Rejected && !out.Uncertain {
		if m.err == nil {
			m.err = out.Err
		}
		m.phase = closing
		return m, tea.Quit
	}
	if !out.Uncertain {
		m.code = session.OK
	}
	return m, nil
}
func (m Model) signal(sig os.Signal) (tea.Model, tea.Cmd) {
	if sig == syscall.SIGHUP {
		return m.lost(fmt.Errorf("terminal hung up"))
	}
	if m.turn != nil {
		m.phase = stopping
		m.turn.Interrupt()
		m.exitAfter = m.exitAfter || sig == syscall.SIGTERM
		return m, nil
	}
	if sig == syscall.SIGTERM && m.err == nil && m.phase != unconfirmed {
		m.code = session.Stopped
	}
	m.phase = closing
	return m, tea.Quit
}
func (m Model) lost(err error) (tea.Model, tea.Cmd) {
	if m.err == nil {
		m.err = err
	}
	if err != nil {
		m.code = session.Error
	}
	if m.turn != nil {
		m.exitAfter = true
		m.phase = stopping
		m.turn.InputLost()
		return m, nil
	}
	m.phase = closing
	return m, tea.Quit
}
func tail(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[len(s)-limit:]
	for len(s) > 0 && !utf8.RuneStart(s[0]) {
		s = s[1:]
	}
	return s
}
func (m *Model) add(s string) {
	if s == "" {
		return
	}
	bottom := m.viewport.AtBottom()
	s = present.Clean(s)
	if len(s) > transcriptBytes {
		s = tail(s, transcriptBytes)
		m.omitted = true
	}
	for len(m.entries) > 0 && (m.bytes+len(s) > transcriptBytes || len(m.entries) >= transcriptEntries) {
		m.bytes -= len(m.entries[0])
		m.entries[0] = ""
		m.entries = m.entries[1:]
		m.omitted = true
	}
	m.entries = append(m.entries, s)
	m.bytes += len(s)
	m.viewport.SetContent(m.transcript())
	if bottom {
		m.viewport.GotoBottom()
	}
}
func (m Model) transcript() string {
	s := strings.Join(m.entries, "")
	if m.omitted {
		s = "… earlier transcript omitted\n" + s
	}
	return s
}
func (m Model) View() tea.View {
	status := map[phase]string{ready: "ready", submitting: "submitting · draft editable", running: "running · draft editable", stopping: "stopping · waiting for cleanup · draft editable", unconfirmed: "acceptance unknown · Ctrl+R reconcile", closing: "closing"}[m.phase]
	content := "canti-tui · session " + m.controller.ID() + "\n" + status + " · current launch flags apply to each new input\n" + m.notice + "\n" + m.viewport.View() + "\n\n" + m.editor.View() + "\nEnter send when ready · Alt+Enter newline · PgUp/PgDn scroll · Ctrl+C stop/exit · Ctrl+R recover"
	v := tea.NewView(content)
	v.AltScreen = true
	return v
}
