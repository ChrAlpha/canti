package ui

import (
	"errors"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestAttachedTruncatedAnswerShowsTheNotice(t *testing.T) {
	m := model()
	m = update(m, fact{event: chat.Event{Item: &hostrpc.Item{Pos: 1, RunID: "run", Kind: "returned", Result: "short answer", Truncated: true}}})
	if !strings.Contains(m.transcript(), "short answer") || !strings.Contains(m.transcript(), `use "canti history" for the full text`) {
		t.Fatalf("truncated attached answer = %q", m.transcript())
	}
}

func TestShutdownWinsOverAttachFailure(t *testing.T) {
	m := model()
	m.exitAfter = true
	next, cmd := m.complete(chat.Result{AttachFailed: true, Err: errors.New("host went away")})
	m = next.(Model)
	if cmd == nil || m.phase != closing || m.err == nil {
		t.Fatalf("shutdown over attach failure = phase %d, err %v, cmd %v", m.phase, m.err, cmd != nil)
	}
}
