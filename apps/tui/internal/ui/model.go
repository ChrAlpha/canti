// Package ui presents a disposable projection of one chat.
package ui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unicode/utf8"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/apps/tui/internal/present"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
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
	turn  *chat.Turn
	event chat.Event
}
type signalMsg struct{ signal os.Signal }
type outputError struct{ err error }

// steerDone is the answer to a steer or queued follow-up submitted while the
// current turn runs. Its snapshot is retained for a distinction between a
// proven rejection and an unknown acceptance.
type steerDone struct {
	out      chat.SubmitOutcome
	snap     chat.Snapshot
	delivery string
	attempt  uint64
}

type pendingSteer struct {
	snap      chat.Snapshot
	delivery  string
	uncertain bool
	inFlight  bool
	attempt   uint64
}
type cancelDone struct {
	run hostrpc.Run
	err error
}
type withdrawDone struct {
	inputs []hostrpc.Input
	err    error
}

type Model struct {
	controller        *chat.Controller
	ctx               context.Context
	turn              *chat.Turn
	pending, rejected *chat.Snapshot
	editor            textarea.Model
	viewport          viewport.Model
	phase             phase
	entries           []string
	bytes             int
	omitted           bool
	final, notice     string
	shownTerminal     map[string]bool
	held              int
	steerPending      *pendingSteer
	lastSteerID       string
	lastSteerAttempt  uint64
	resolvedSteer     map[string]uint64
	width, height     int
	exitAfter         bool
	code              int
	err               error
}

func New(ctx context.Context, c *chat.Controller) Model {
	editor := textarea.New()
	editor.Placeholder = "Write a message"
	editor.Prompt = "› "
	editor.CharLimit = c.GoalLimit
	editor.SetHeight(3)
	editor.SetWidth(76)
	editor.Focus()
	return Model{controller: c, ctx: ctx, editor: editor, viewport: viewport.New(viewport.WithWidth(80), viewport.WithHeight(12)), width: 80, height: 24,
		resolvedSteer: map[string]uint64{}, shownTerminal: map[string]bool{}}
}
func (m Model) Init() tea.Cmd { return textarea.Blink }
func next(t *chat.Turn) tea.Cmd {
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
	case steerDone:
		return m.steered(msg)
	case cancelDone:
		return m.cancelled(msg)
	case withdrawDone:
		return m.withdrew(msg)
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
	// Navigation and edits that reduce an oversized recovered draft stay
	// available; only a change that grows past the submission limit is
	// refused. Sending enforces the limit as well.
	if key, ok := msg.(tea.KeyPressMsg); ok && len(key.Text) > 0 &&
		len(m.editor.Value()) <= m.controller.GoalLimit && len(m.editor.Value())+len(key.Text) > m.controller.GoalLimit {
		m.notice = "Input byte limit reached"
		return m, nil
	}
	before := m.editor.Value()
	var cmd tea.Cmd
	m.editor, cmd = m.editor.Update(msg)
	if after := m.editor.Value(); len(after) > m.controller.GoalLimit && len(after) > len(before) {
		m.editor.SetValue(before)
		m.notice = "Input byte limit reached"
	}
	return m, cmd
}
func (m *Model) key(k tea.KeyPressMsg) (bool, tea.Cmd) {
	if m.keyBlocked(k) {
		return true, nil
	}
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
		switch m.phase {
		case running:
			return true, m.submitDraft("steer")
		case ready:
			goal := strings.TrimSpace(m.editor.Value())
			if goal == "" {
				return true, nil
			}
			return true, m.start(chat.NewSnapshot(goal), true)
		default:
			m.notice = "Send unavailable until the current input completes; draft retained"
			return true, nil
		}
	case "ctrl+f":
		if m.phase == running {
			return true, m.submitDraft("follow_up")
		}
		m.notice = "A follow-up queues while the agent works; Enter sends a new input when it is ready"
		return true, nil
	case "ctrl+x":
		if m.phase != running && m.phase != submitting {
			m.notice = "No active run has an effect to cancel"
			return true, nil
		}
		return true, m.cancelEffect()
	case "alt+enter":
		if len(m.editor.Value()) < m.controller.GoalLimit {
			m.editor.InsertString("\n")
		}
		return true, nil
	case "ctrl+r":
		return true, m.reconcile()
	case "pgup", "pgdown":
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(k)
		return true, cmd
	}
	return false, nil
}

// reconcile handles Ctrl+R: withdraw held inputs, retry an uncertain
// submission, retry an unconfirmed opening, restore a rejected input, or
// withdraw the held inputs of a stopped run.
func (m *Model) reconcile() tea.Cmd {
	if m.held > 0 {
		return m.withdraw()
	}
	if m.phase == unconfirmed && m.pending != nil {
		return m.start(*m.pending, false)
	}
	if m.steerPending != nil {
		if m.steerPending.inFlight {
			m.notice = "Wait for the current submission before reconciling"
			return nil
		}
		if m.steerPending.uncertain {
			return m.retrySteer()
		}
		m.notice = "Wait for the current submission before reconciling"
		return nil
	}
	if m.rejected != nil {
		if m.editor.Value() != "" {
			m.notice = "Clear the current draft before restoring rejected input"
		} else {
			m.setDraft(m.rejected.Goal)
			m.rejected = nil
			m.notice = "Rejected input restored for editing"
		}
		return nil
	}
	return m.withdraw()
}

// submitDraft sends the draft as a steer or a queued follow-up, keeping it
// out of the draft until the host answers.
func (m *Model) submitDraft(delivery string) tea.Cmd {
	if m.steerPending != nil {
		m.pendingSubmission()
		return nil
	}
	goal := strings.TrimSpace(m.editor.Value())
	if goal == "" {
		return nil
	}
	if err := m.controller.Check(goal); err != nil {
		m.notice = err.Error()
		return nil
	}
	s := chat.NewSnapshot(goal)
	m.clearDraft()
	m.steerPending = &pendingSteer{snap: s, delivery: delivery, inFlight: true}
	m.lastSteerID, m.lastSteerAttempt = s.InputID, 0
	delete(m.resolvedSteer, s.InputID)
	m.notice = "Submitting the input to the working agent"
	if delivery == "steer" {
		m.add("\n› " + goal + "\n")
	} else {
		m.add("\n» " + goal + "\n")
	}
	ctx, c := m.ctx, m.controller
	return func() tea.Msg {
		return steerDone{out: c.Submit(ctx, s, delivery), snap: s, delivery: delivery}
	}
}

func (m *Model) cancelEffect() tea.Cmd {
	id := ""
	if m.turn != nil {
		id = m.turn.RunID()
	}
	if id == "" {
		m.notice = "The accepted effect is not known yet"
		return nil
	}
	ctx, c := m.ctx, m.controller
	return func() tea.Msg {
		r, err := c.CancelEffect(ctx, id)
		return cancelDone{run: r, err: err}
	}
}

func (m *Model) withdraw() tea.Cmd {
	ctx, c := m.ctx, m.controller
	return func() tea.Msg {
		inputs, err := c.Withdraw(ctx)
		return withdrawDone{inputs: inputs, err: err}
	}
}

// keyBlocked reports whether the key must be refused until an uncertain steer
// is reconciled; a new submission could duplicate it, so Ctrl+R comes first.
func (m *Model) keyBlocked(k tea.KeyPressMsg) bool {
	switch k.String() {
	case "enter", "ctrl+f":
		return m.pendingSubmission()
	}
	return false
}

// pendingSubmission reports a steer or follow-up that is still in flight or
// uncertain. At most one unresolved submission exists, so a new submission
// cannot overwrite its recovery identity.
func (m *Model) pendingSubmission() bool {
	if m.steerPending == nil {
		return false
	}
	if m.steerPending.uncertain {
		m.notice = "Acceptance of the pending steer is unknown; Ctrl+R reconciles the same InputID"
	} else {
		m.notice = "Wait for the current submission; the draft stays editable"
	}
	return true
}

func (m Model) steered(msg steerDone) (tea.Model, tea.Cmd) {
	pending := m.steerPending != nil && m.steerPending.snap.InputID == msg.snap.InputID && m.steerPending.attempt == msg.attempt
	if !pending && m.steerPending == nil && m.lastSteerID != "" &&
		(msg.snap.InputID != m.lastSteerID || msg.attempt != m.lastSteerAttempt || m.resolvedSteer[msg.snap.InputID] == msg.attempt) {
		m.notice = "Ignored a stale submission answer"
		return m, nil
	}
	if !pending && m.steerPending != nil {
		// A successful opened-run acknowledgement is authoritative even if
		// its snapshot is no longer the pending one; attach to it while
		// retaining the newer recovery identity.
		if msg.out.Err == nil && msg.out.Ack.Run != nil && m.turn == nil {
			if t, err := m.controller.Follow(m.ctx, msg.snap, msg.out.Ack); err == nil {
				m.turn, m.phase = t, running
				m.notice = "Following the opened run"
				return m, next(t)
			}
		}
		m.notice = "Ignored a stale submission answer"
		return m, nil
	}
	if msg.out.Err != nil {
		if !msg.out.Rejected {
			// A transport, closing, protocol or plain error, including a
			// failed Ctrl+R attempt, cannot prove the earlier admission did
			// not happen.
			m.steerPending = &pendingSteer{snap: msg.snap, delivery: msg.delivery, uncertain: true, attempt: msg.attempt}
			m.notice = "Acceptance of the input is unknown; Ctrl+R reconciles the same InputID. New draft preserved."
			return m, nil
		}
		if pending {
			m.steerPending = nil
			if m.resolvedSteer == nil {
				m.resolvedSteer = map[string]uint64{}
			}
			m.resolvedSteer[msg.snap.InputID] = msg.attempt
		}
		snap := msg.snap
		m.rejected = &snap
		if errors.Is(msg.out.Err, hostrpc.ErrHeld) {
			m.held = max(m.held, 1)
			m.notice = "Held inputs must be withdrawn; Ctrl+R withdraws them into the draft"
			return m, nil
		}
		m.notice = "Input refused; Ctrl+R restores it into an empty draft. New draft preserved."
		return m, nil
	}
	if pending {
		m.steerPending = nil
		if m.resolvedSteer == nil {
			m.resolvedSteer = map[string]uint64{}
		}
		m.resolvedSteer[msg.snap.InputID] = msg.attempt
	}
	if msg.out.Ack.Run != nil && m.turn == nil {
		if t, err := m.controller.Follow(m.ctx, msg.snap, msg.out.Ack); err == nil {
			m.turn, m.pending, m.phase = t, &msg.snap, running
			m.notice = "Following the opened run"
			return m, next(t)
		}
	}
	what := "steer"
	if msg.delivery == "follow_up" {
		what = "follow-up"
	}
	m.notice = fmt.Sprintf("%s %s at %d", what, msg.out.Ack.Disposition, msg.out.Ack.Pos)
	return m, nil
}

// retrySteer resends the retained steer snapshot with its exact InputID and a
// new attempt. A retry is itself single-flight; uncertainty remains knowledge
// until an answer for the current attempt arrives.
func (m *Model) retrySteer() tea.Cmd {
	if m.steerPending == nil || m.steerPending.inFlight {
		if m.steerPending != nil && m.steerPending.inFlight {
			m.notice = "Wait for the current reconciliation attempt"
		}
		return nil
	}
	next := *m.steerPending
	next.attempt++
	next.inFlight = true
	next.uncertain = true
	m.steerPending = &next
	m.lastSteerID, m.lastSteerAttempt = next.snap.InputID, next.attempt
	delete(m.resolvedSteer, next.snap.InputID)
	m.notice = "Reconciling the pending input"
	ctx, c := m.ctx, m.controller
	return func() tea.Msg {
		return steerDone{out: c.Submit(ctx, next.snap, next.delivery), snap: next.snap, delivery: next.delivery, attempt: next.attempt}
	}
}

func (m Model) cancelled(msg cancelDone) (tea.Model, tea.Cmd) {
	switch {
	case msg.err != nil:
		m.notice = "Cancelling the effect failed: " + msg.err.Error()
	case msg.run.Status == hostrpc.StatusActive:
		m.notice = "Effect cancelled; the run continues in phase " + msg.run.Phase
	default:
		m.notice = "Effect cancelled; the run is " + msg.run.Status
	}
	return m, nil
}

func (m Model) withdrew(msg withdrawDone) (tea.Model, tea.Cmd) {
	switch {
	case msg.err != nil:
		m.notice = "Withdrawing held inputs failed: " + msg.err.Error()
	case len(msg.inputs) == 0:
		m.notice = "No held input to withdraw"
	default:
		texts := make([]string, 0, len(msg.inputs))
		for _, in := range msg.inputs {
			texts = append(texts, in.Text)
		}
		restored := strings.Join(texts, "\n\n")
		if m.editor.Value() != "" {
			restored = m.editor.Value() + "\n\n" + restored
		}
		m.setDraft(restored)
		m.held = 0
		m.notice = fmt.Sprintf("Withdrew %d held input(s) into the draft", len(msg.inputs))
	}
	return m, nil
}

// setDraft places text in the editor without losing recovery or restored
// bytes to the submission limit. Submission still checks that limit; the
// editor's higher capacity only keeps the text available for editing.
func (m *Model) setDraft(text string) {
	m.editor.CharLimit = max(m.controller.GoalLimit, len(text))
	m.editor.SetValue(text)
}

// clearDraft empties the editor and restores the submission limit for new
// typing.
func (m *Model) clearDraft() {
	m.editor.CharLimit = m.controller.GoalLimit
	m.editor.SetValue("")
}

func (m *Model) start(s chat.Snapshot, clear bool) tea.Cmd {
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
		m.clearDraft()
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
		if m.phase == submitting && e.Ack.Run != nil {
			m.phase = running
		}
		if len(e.Ack.Differs) > 0 {
			m.add("Input replay keeps its original settings: " + strings.Join(e.Ack.Differs, ", ") + "\n")
		}
	case e.Diagnostic != "":
		m.add(e.Diagnostic + "\n")
	case e.Item != nil:
		if e.Item.Kind == "returned" || e.Item.Kind == "stopped" {
			if m.shownTerminal == nil {
				m.shownTerminal = map[string]bool{}
			}
			if e.Item.RunID != "" {
				m.shownTerminal[e.Item.RunID] = true
			}
		}
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
func (m Model) complete(out chat.Result) (tea.Model, tea.Cmd) {
	m.turn = nil
	answer, note := present.Result(out)
	if out.Run.RunID != "" && m.shownTerminal[out.Run.RunID] {
		answer, note = present.ResultAfterTerminal(out)
	}
	m.final = answer + note
	m.add(m.final)
	m.held = out.Run.Held
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
		if errors.Is(out.Err, hostrpc.ErrHeld) {
			m.held = max(m.held, 1)
			m.notice = "Held inputs must be withdrawn; Ctrl+R withdraws them into the draft"
		} else {
			m.notice = "Input rejected; Ctrl+R restores it into an empty draft"
		}
	default:
		m.phase = ready
		m.pending = nil
		if m.held > 0 {
			m.notice = fmt.Sprintf("%d held input(s); Ctrl+R withdraws them into the draft", m.held)
		}
	}
	if m.exitAfter || out.Interrupted || out.NoInputFits || out.Code == chat.Resolution || out.Code == chat.Active || out.Err != nil && !out.Rejected && !out.Uncertain {
		if m.err == nil {
			m.err = out.Err
		}
		m.phase = closing
		return m, tea.Quit
	}
	if !out.Uncertain {
		m.code = chat.OK
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
		m.code = chat.Stopped
	}
	m.phase = closing
	return m, tea.Quit
}
func (m Model) lost(err error) (tea.Model, tea.Cmd) {
	if m.err == nil {
		m.err = err
	}
	if err != nil {
		m.code = chat.Error
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
	content := "canti-tui\n" + status + " · current launch flags apply to each new input\n" + m.notice + "\n" + m.viewport.View() + "\n\n" + m.editor.View() + "\nEnter send, or steer while running · Ctrl+F follow-up while running · Ctrl+X cancel effect · Alt+Enter newline · PgUp/PgDn scroll · Ctrl+R recover or withdraw · Ctrl+C stop/exit"
	v := tea.NewView(content)
	v.AltScreen = true
	return v
}
