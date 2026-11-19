package ui

import (
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/apps/tui/internal/present"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// omissionNotice is the transcript header for any omitted history or live
// entries. It is outside the entry budget but its bytes are reserved.
const omissionNotice = "… earlier transcript omitted\n  use \"canti history\" for a run's full recorded transitions\n"

// historyBlock is one run's rendered entries, header and outcome included.
// The budget selects blocks whole; an item is never kept without its run.
type historyBlock struct {
	runID    string
	entries  []string
	terminal []string
}

// seedHistory places the recorded whole-run tail of the line before this
// invocation's activity. It budgets the rendered entries (run headers,
// outcomes, separators and guidance included), so the transcript's own
// eviction cannot leave a run's answer without its goal. When no run fits,
// it shows the omission notice and the idle-run guidance instead of a
// fragment.
func (m *Model) seedHistory(h chat.History) {
	m.history = h
	diagnostic := ""
	if h.Err != nil {
		diagnostic = "canti: earlier history unavailable: " + present.Clean(h.Err.Error()) + "\n"
	}
	blocks := m.historyBlocks(h)
	omitted := h.Omitted
	active := activeNotice(h)
	header, divider := "── earlier history ──\n", "\n── this session ──\n"
	fixedEntries, fixedBytes := 2, len(header)+len(divider)+len(active)
	if diagnostic != "" {
		fixedEntries++
		fixedBytes += len(diagnostic)
	}
	if active != "" {
		fixedEntries++
	}
	noticeBytes := 0
	entriesBytes := func(bs []historyBlock) (int, int) {
		n, size := fixedEntries, fixedBytes+noticeBytes
		for _, b := range bs {
			n += len(b.entries)
			for _, e := range b.entries {
				size += len(e)
			}
		}
		return n, size
	}
	if omitted {
		noticeBytes = len(omissionNotice)
	}
	for len(blocks) > 0 {
		n, size := entriesBytes(blocks)
		if n <= transcriptEntries && size <= transcriptBytes {
			break
		}
		blocks = blocks[1:]
		omitted, noticeBytes = true, len(omissionNotice)
	}
	if diagnostic != "" {
		m.add(diagnostic)
	}
	m.omitted = m.omitted || omitted
	if omitted {
		m.omission = omissionNotice
	}
	if len(blocks) == 0 {
		m.add(active)
		return
	}
	m.add(header)
	for _, b := range blocks {
		owner := "run:" + b.runID
		for _, e := range b.entries {
			m.addOwned(owner, e)
		}
		for _, runID := range b.terminal {
			m.terminalShown(runID)
		}
	}
	m.add(divider)
	m.add(active)
}

// historyBlocks renders whole runs in order, appending the line's queued
// principal inputs and withdrawal events to the last retained run's block.
func (m *Model) historyBlocks(h chat.History) []historyBlock {
	b := &historyBuilder{summaries: make(map[string]hostrpc.HistoryRun, len(h.Runs)), shown: map[string]map[string]bool{}, latest: h.Latest}
	for _, run := range h.Runs {
		b.summaries[run.RunID] = run
	}
	for _, it := range h.Items {
		b.add(it)
	}
	b.flush()
	return b.blocks
}

type historyBuilder struct {
	blocks    []historyBlock
	current   *historyBlock
	summaries map[string]hostrpc.HistoryRun
	shown     map[string]map[string]bool
	latest    *hostrpc.HistoryRun
}

func (b *historyBuilder) add(it hostrpc.Item) {
	if it.RunID == "" {
		b.addOrphan(it)
		return
	}
	if b.current == nil || b.current.runID != it.RunID {
		b.flush()
		b.current = &historyBlock{runID: it.RunID, entries: []string{"\n── earlier run ──\n"}}
	}
	b.markTerminal(it)
	held := false
	if it.Input != nil {
		held = b.summaries[it.RunID].Status == hostrpc.StatusStopped && !it.Input.Seen
	}
	b.appendItem(it, held)
}

func (b *historyBuilder) addOrphan(it hostrpc.Item) {
	if b.current != nil {
		b.flush()
	}
	if len(b.blocks) == 0 {
		return
	}
	held := b.latest != nil && (b.latest.Status == hostrpc.StatusStopped || b.latest.Status == hostrpc.StatusReturned)
	if text := historyItemText(it, b.latest, held); text != "" {
		b.blocks[len(b.blocks)-1].entries = append(b.blocks[len(b.blocks)-1].entries, text)
	}
}

func (b *historyBuilder) markTerminal(it hostrpc.Item) {
	if it.Kind != "returned" && it.Kind != "stopped" && it.Kind != "needs_resolution" {
		return
	}
	if b.shown[it.RunID] == nil {
		b.shown[it.RunID] = map[string]bool{}
	}
	b.shown[it.RunID][it.Kind] = true
	if it.Kind == "returned" || it.Kind == "stopped" {
		b.current.terminal = append(b.current.terminal, it.RunID)
	}
}

func (b *historyBuilder) appendItem(it hostrpc.Item, held bool) {
	if text := historyItemText(it, b.latest, held); text != "" {
		b.current.entries = append(b.current.entries, text)
	}
}

func (b *historyBuilder) flush() {
	if b.current == nil {
		return
	}
	if summary, ok := b.summaries[b.current.runID]; ok {
		text, terminal := historyOutcomeText(summary, b.shown[summary.RunID])
		if text != "" {
			b.current.entries = append(b.current.entries, text)
		}
		if terminal != "" {
			b.current.terminal = append(b.current.terminal, terminal)
		}
	}
	b.blocks = append(b.blocks, *b.current)
	b.current = nil
}

// historyItemText renders one item without adding it, so the budget can keep
// whole run blocks. The returned text is one transcript entry.
func historyItemText(it hostrpc.Item, latest *hostrpc.HistoryRun, held bool) string {
	switch it.Kind {
	case "run_started", "inputs_bound", "retrying", "stop_requested", "effect_recovered":
		return ""
	case "input":
		if it.Input == nil {
			return ""
		}
		var b strings.Builder
		present.Input(&b, *it.Input)
		text := b.String()
		if held && it.Input.Disposition != "opened" && it.Input.Disposition != "withdrawn" {
			text = strings.Replace(text, " (queued)\n", " (held)\n", 1)
			if !strings.Contains(text, " (held)\n") {
				text = strings.TrimSuffix(text, "\n") + " (held)\n"
			}
		}
		return text
	case "needs_resolution":
		msg := "canti: run " + present.Clean(it.RunID) + " needs resolution"
		if latest != nil && latest.RunID == it.RunID && latest.EffectID != "" {
			msg += " for effect " + present.Clean(latest.EffectID)
		}
		if it.Failure != "" {
			msg += ": " + present.Clean(it.Failure)
		}
		return msg + "\n"
	default:
		var b strings.Builder
		present.Item(&b, it)
		if it.Kind == "returned" && it.Truncated {
			b.WriteString(truncatedAnswer())
		}
		return b.String()
	}
}

// historyOutcomeText renders a summary outcome the rows did not show.
func historyOutcomeText(run hostrpc.HistoryRun, shown map[string]bool) (string, string) {
	switch run.Status {
	case hostrpc.StatusReturned:
		if shown["returned"] {
			return "", ""
		}
		answer := present.Clean(run.Result)
		if answer == "" && !run.Truncated {
			return "", run.RunID
		}
		if run.Truncated {
			return answer + "\n" + truncatedAnswer(), run.RunID
		}
		return answer + "\n", run.RunID
	case hostrpc.StatusStopped:
		if shown["stopped"] {
			return "", ""
		}
		reason := run.StopReason
		if reason == "" {
			reason = "stopped"
		}
		return fmt.Sprintf("canti: run %s stopped: %s\n", present.Clean(run.RunID), present.Clean(reason)), run.RunID
	case hostrpc.StatusNeedsResolution:
		if shown["needs_resolution"] {
			return "", ""
		}
		msg := "canti: run " + present.Clean(run.RunID) + " needs resolution"
		if run.EffectID != "" {
			msg += " for effect " + present.Clean(run.EffectID)
		}
		return msg + "; inspect it with canti status and resolve the effect\n", ""
	}
	return "", ""
}

func (m *Model) terminalShown(runID string) {
	if runID == "" {
		return
	}
	if m.shownTerminal == nil {
		m.shownTerminal = map[string]bool{}
	}
	m.shownTerminal[runID] = true
}

func truncatedAnswer() string {
	return "… answer truncated; use \"canti history\" for the full text\n"
}

// attachPoint is the run a reopening view follows: the latest run only when
// the history load completed and the host is already driving it, starting
// after the newest history row so the live handoff neither repeats nor skips
// an entry. An active run the host is not driving is shown from history and
// left for an explicit resume.
func attachPoint(h chat.History) (hostrpc.HistoryRun, string, bool) {
	if h.Err != nil || h.Latest == nil || h.Latest.Status != hostrpc.StatusActive || h.Latest.Driver != hostrpc.DriverDriving {
		return hostrpc.HistoryRun{}, "", false
	}
	return *h.Latest, h.Cursor, true
}

// followActive joins the active run a detached client left behind before the
// first view renders, so the editor starts in the run it is following.
func (m *Model) followActive() {
	if !m.history.Loaded || m.history.Err != nil {
		return
	}
	latest := m.controller.Latest()
	// In auto mode another client can start or finish a run after Setup's
	// recheck but before this frame; recheck again with the same bounded
	// deadline. Owned mode has no shared host to re-read.
	if !m.controller.Owned() {
		if refreshed, err := m.controller.RefreshLatest(m.ctx); err == nil && refreshed != nil {
			latest = refreshed
		}
	}
	if err := m.controller.LatestErr(); err != nil && latest == nil {
		m.notice = "Could not recheck the latest run: " + err.Error()
		return
	}
	if run, cursor, ok := attachPoint(m.history); ok && (latest == nil || latest.RunID == m.history.Latest.RunID) {
		m.attach(run, cursor)
		return
	}
	if !needsFollow(m.history, latest) {
		return
	}
	m.linePending = newFreshLine(m.history, latest)
	m.attach(*latest, m.history.Cursor)
}

// newFreshLine reports a fresh run that started after the history snapshot,
// so the old line is kept as history under an explicit separator and live
// following starts after the snapshot, never inside the old line.
func newFreshLine(h chat.History, latest *hostrpc.HistoryRun) bool {
	return latest != nil && latest.Line > h.LineStart
}

// needsFollow decides the one startup recheck. It follows when the run
// became driven, when the snapshot's active run has since closed, or when a
// run appeared after the snapshot; it does not attach to an idle active run.
func needsFollow(h chat.History, latest *hostrpc.HistoryRun) bool {
	if latest == nil {
		return false
	}
	if h.Latest == nil {
		return latest.Status != hostrpc.StatusActive || latest.Driver == hostrpc.DriverDriving
	}
	if latest.RunID != h.Latest.RunID {
		return latest.Status != hostrpc.StatusActive || latest.Driver == hostrpc.DriverDriving
	}
	if latest.Status != h.Latest.Status {
		return true
	}
	return latest.Status == hostrpc.StatusActive && latest.Driver == hostrpc.DriverDriving
}

func (m *Model) attach(run hostrpc.HistoryRun, cursor string) {
	t, err := m.controller.Attach(m.ctx, run, cursor)
	if err != nil {
		m.notice = "Following the active run failed: " + err.Error()
		return
	}
	m.turn, m.phase = t, running
	m.attachCmd = next(t)
}

// activeNotice describes an active run the view does not follow, so a reader
// knows the history is not yet complete.
func activeNotice(h chat.History) string {
	if h.Latest == nil || h.Latest.Status != hostrpc.StatusActive || h.Latest.Driver == hostrpc.DriverDriving {
		return ""
	}
	return fmt.Sprintf("canti: run %s is %s; continue it with \"canti resume -run %s\".\n", h.Latest.RunID, h.Latest.Phase, h.Latest.RunID)
}
