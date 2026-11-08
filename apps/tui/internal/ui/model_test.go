package ui

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/apps/tui/internal/session"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func model() Model {
	c := session.New(session.Config{SessionID: "session"})
	c.GoalLimit = 32768
	return New(context.Background(), c)
}
func update(m Model, msg tea.Msg) Model { next, _ := m.Update(msg); return next.(Model) }
func key(text string) tea.KeyPressMsg {
	return tea.KeyPressMsg(tea.Key{Code: []rune(text)[0], Text: text})
}
func TestDraftRemainsEditableDuringSubmissionRunAndStop(t *testing.T) {
	for _, state := range []phase{submitting, running, stopping} {
		m := model()
		m.phase = state
		m.editor.SetValue("draft")
		m = update(m, key("x"))
		m = update(m, tea.PasteMsg{Content: " pasted\x03"})
		m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter, Mod: tea.ModAlt}))
		if m.editor.Value() != "draftx pasted\n" {
			t.Fatalf("phase %d draft %q", state, m.editor.Value())
		}
		before := m.editor.Value()
		m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
		if m.editor.Value() != before || m.phase != state || m.pending != nil {
			t.Fatal("busy Enter submitted, queued or destroyed draft")
		}
		next, cmd := m.complete(session.Result{Code: session.OK, Run: hostrpc.Run{Status: hostrpc.StatusReturned, Result: "done"}})
		m = next.(Model)
		if cmd != nil || m.phase != ready || m.editor.Value() != before {
			t.Fatal("completion submitted or destroyed draft")
		}
	}
}
func TestCompletionAndBusyEnterHaveSerializedManualSendSemantics(t *testing.T) {
	m := model()
	m.phase = running
	m.editor.SetValue("next")
	m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	next, cmd := m.complete(session.Result{Code: session.OK})
	m = next.(Model)
	if cmd != nil || m.pending != nil || m.editor.Value() != "next" {
		t.Fatal("busy Enter replayed at completion")
	}
	// In the opposite event order, completion leaves the text ready for a new
	// explicit send. There is no callback or command created by completion.
	m.phase = running
	next, cmd = m.complete(session.Result{Code: session.OK})
	m = next.(Model)
	if cmd != nil || m.phase != ready {
		t.Fatal("completion is not ready for manual send")
	}
	// A local admission error proves Enter is processed now and leaves the draft.
	m.controller.GoalLimit = 1
	m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if !strings.Contains(m.notice, "at most 1 bytes") || m.editor.Value() != "next" {
		t.Fatal("manual Enter was lost")
	}
}
func TestLateRejectionPreservesNewDraftAndRecoverableSnapshot(t *testing.T) {
	m := model()
	s := session.NewSnapshot("submitted")
	m.pending = &s
	m.phase = submitting
	m.editor.SetValue("new edits")
	next, _ := m.complete(session.Result{Code: session.Usage, Err: errors.New("does not fit"), Rejected: true})
	m = next.(Model)
	if m.editor.Value() != "new edits" || m.rejected == nil || m.rejected.Goal != "submitted" {
		t.Fatal("late rejection overwrote draft or lost snapshot")
	}
	m = update(m, tea.KeyPressMsg(tea.Key{Code: 'r', Mod: tea.ModCtrl}))
	if m.editor.Value() != "new edits" {
		t.Fatal("restore overwrote newer edits")
	}
	m.editor.SetValue("")
	m = update(m, tea.KeyPressMsg(tea.Key{Code: 'r', Mod: tea.ModCtrl}))
	if m.editor.Value() != "submitted" || m.rejected != nil {
		t.Fatal("explicit restore failed")
	}
}
func TestUnknownAcceptanceRetainsInputIdentityAndNewDraft(t *testing.T) {
	m := model()
	s := session.NewSnapshot("pending")
	m.pending = &s
	m.phase = submitting
	m.editor.SetValue("new")
	next, cmd := m.complete(session.Result{Code: session.Error, Err: errors.New("lost acknowledgement"), Uncertain: true})
	m = next.(Model)
	if cmd != nil || m.phase != unconfirmed || *m.pending != s || m.editor.Value() != "new" {
		t.Fatal("uncertain submission lost identity/draft")
	}
	m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if *m.pending != s || m.editor.Value() != "new" {
		t.Fatal("unconfirmed input allowed a new submission")
	}
}
func TestDraftSurvivesResizeProgressAndPasteOverflow(t *testing.T) {
	m := model()
	m.phase = running
	m.editor.SetValue("keep")
	for range 1000 {
		m.add(strings.Repeat("progress\n", 128))
	}
	if m.bytes > transcriptBytes || len(m.entries) > transcriptEntries || !m.omitted {
		t.Fatal("unbounded transcript")
	}
	m = update(m, tea.WindowSizeMsg{Width: 120, Height: 40})
	m = update(m, tea.PasteOverflowMsg{Limit: 32768})
	if m.editor.Value() != "keep" {
		t.Fatal("projection changed draft")
	}
	m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyPgUp}))
	at := m.viewport.YOffset()
	m.add("new progress\n")
	if m.viewport.YOffset() != at {
		t.Fatal("progress moved reader away from scroll position")
	}
}
func TestEditorBoundsUTF8BytesAndKeepsControlBytesInPasteInert(t *testing.T) {
	m := model()
	m.controller.GoalLimit = 8
	m.editor.SetValue("abc")
	m = update(m, tea.PasteMsg{Content: "日本語"})
	if m.editor.Value() != "abc" {
		t.Fatal("oversized paste altered draft")
	}
	m = update(m, tea.PasteMsg{Content: "\x03\x04x"})
	if m.editor.Value() != "abcx" || m.phase != ready {
		t.Fatal("paste executed control keys")
	}
	m = update(m, key("日"))
	m = update(m, key("本"))
	if len(m.editor.Value()) > 8 || !strings.Contains(m.notice, "byte limit") {
		t.Fatal("UTF-8 byte cap not enforced")
	}
}

type failAfterWrites struct {
	remaining int
	err       error
}

func (w *failAfterWrites) Write(p []byte) (int, error) {
	if w.remaining == 0 {
		return 0, w.err
	}
	w.remaining--
	return len(p), nil
}
func TestExitOutputErrorsIncludeFinalFallbackAndContinuationHint(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		m := model()
		m.add("transcript\n")
		if fallback {
			m.final = "full final answer\n"
		}
		failure := errors.New("output lost")
		w := &failAfterWrites{remaining: 1, err: failure}
		if err := writeExit(w, m, "s"); !errors.Is(err, failure) {
			t.Fatalf("fallback=%v err=%v", fallback, err)
		}
	}
}

func TestExitKeepsUsageCodeAndPriorDisplayFailure(t *testing.T) {
	failure := errors.New("no input fits")
	m := model()
	updated, _ := m.complete(session.Result{Code: session.Usage, Err: failure, Rejected: true, NoInputFits: true})
	m = updated.(Model)
	if code, err := m.result(nil); code != session.Usage || !errors.Is(err, failure) {
		t.Fatalf("usage: %d %v", code, err)
	}
	m = model()
	m.err = failure
	m.code = session.Error
	m.exitAfter = true
	updated, _ = m.complete(session.Result{Code: session.Stopped})
	m = updated.(Model)
	if code, err := m.result(nil); code != session.Error || !errors.Is(err, failure) {
		t.Fatalf("display failure: %d %v", code, err)
	}
}

func TestClosingNeverAcceptsQueuedEnter(t *testing.T) {
	for _, event := range []tea.Msg{tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}), signalMsg{syscall.SIGTERM}, tea.InputClosedMsg{}} {
		m := model()
		m.editor.SetValue("next draft")
		m = update(m, event)
		if m.phase != closing {
			t.Fatalf("exit event left send enabled: %T", event)
		}
		m = update(m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
		if m.turn != nil || m.pending != nil || m.editor.Value() != "next draft" {
			t.Fatal("queued Enter submitted while closing")
		}
	}
	m := model()
	m.exitAfter = true
	updated, _ := m.complete(session.Result{Code: session.OK})
	m = updated.(Model)
	if m.phase != closing {
		t.Fatal("completion during exit reopened sending")
	}
}

func TestUnconfirmedExitKeepsRecoveryIdentityAndFailure(t *testing.T) {
	m := model()
	s := session.NewSnapshot("pending")
	m.pending = &s
	updated, _ := m.complete(session.Result{Code: session.Error, Err: errors.New("ambiguous response"), Uncertain: true})
	m = updated.(Model)
	m = update(m, tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}))
	if code, _ := m.result(nil); code != session.Error {
		t.Fatalf("unconfirmed exit code %d", code)
	}
	var output strings.Builder
	if err := writeExit(&output, m, "session"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "-input "+s.InputID) || strings.Contains(output.String(), "continue session with") {
		t.Fatalf("unsafe recovery hint: %s", output.String())
	}
}
