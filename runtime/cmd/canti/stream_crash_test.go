package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
)

// TestARunKilledDuringAStreamRetriesTheAttempt kills a CLI process after its
// streaming request has started and the server has sent partial content. The
// journal has no response for that attempt, so resume counts it lost, resends
// the frozen bytes, and no partial decision is ever decoded.
func TestARunKilledDuringAStreamRetriesTheAttempt(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.model.answer(1, 1, reply{hang: true, text: `{"kind":"exec","payload":"touch forbidden"}`})
	streamed := f.model.watchStream()
	cmd, wait := f.start(nil, "run", "-goal", "Survive a crash during the model stream.", "-max-attempts", "2")
	select {
	case <-streamed:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		r := wait()
		t.Fatalf("the model never received the streamed request: %+v", r)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = wait()

	result := f.cli(nil, "resume")
	if result.code != exitOK || strings.TrimSpace(result.stdout) != "done" {
		t.Fatalf("resume = %+v", result)
	}
	h := f.history()
	want := []string{"request_frozen", "attempt_started", "attempt_started", "response_saved", "response_loaded"}
	if !slices.Equal(kinds(h), want) {
		t.Fatalf("history = %q, want %q", kinds(h), want)
	}
	for _, e := range h {
		if e.Kind == "response_saved" || e.Kind == "response_incomplete" {
			if e.Facts.Response == nil || e.Seq <= 3 {
				t.Fatalf("a response was committed for the killed attempt: %+v", e)
			}
		}
	}
	sent := f.model.requests()
	frozen := h[0].Facts.Request
	if frozen == nil || len(sent) != 2 || !bytes.Equal(sent[0], frozen.Body) || !bytes.Equal(sent[1], frozen.Body) {
		t.Fatalf("resume sent %d requests, want the same frozen bytes twice", len(sent))
	}
	if _, err := os.Stat(filepath.Join(f.workspace, "forbidden")); !os.IsNotExist(err) {
		t.Fatalf("the partial decision ran: %v", err)
	}
}
