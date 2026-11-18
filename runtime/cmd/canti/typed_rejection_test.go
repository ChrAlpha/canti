package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

// TestATypedRejectionDoesNotEchoThroughCLIJournalOrOutput runs the real CLI
// against a loopback provider whose OpenAI context-limit error echoes the
// credential in type, message and request id. The typed rejection, its
// failure text, the SQLite file and CLI output must not retain the echo.
func TestATypedRejectionDoesNotEchoThroughCLIJournalOrOutput(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first"}})
	secret := f.key
	f.model.answer(1, 2, reply{
		status:    400,
		requestID: secret,
		body:      `{"error":{"type":"` + secret + `_type","code":"context_length_exceeded","message":"` + secret + `"}}`,
	})
	env := []string{"OPENAI_API_KEY=" + secret}
	first := f.cli(env, "run", "-goal", "First.", "-api", "openai-responses", "-model", "fixture-model")
	if first.code != exitOK || strings.TrimSpace(first.stdout) != "first" {
		t.Fatalf("first run = %+v", first)
	}
	second := f.cli(env, "run", "-goal", "Reject.", "-api", "openai-responses", "-model", "fixture-model", "-max-attempts", "1")
	if second.code != exitStopped {
		t.Fatalf("second run did not reach the typed rejection: %+v", second)
	}
	if strings.Contains(first.stdout+first.stderr+second.stdout+second.stderr, secret) {
		t.Fatalf("CLI output echoes the credential: first=%+v second=%+v", first, second)
	}
	id := f.status().Run.ID
	h := f.history("-run", id)
	var rejected bool
	for _, entry := range h {
		if entry.Kind != "request_rejected" {
			continue
		}
		rejected = true
		signal := entry.Facts.Rejection.Signal
		for _, value := range []string{entry.Facts.Failure, signal.Error(), signal.Type, signal.Code, signal.StopReason, signal.RequestID} {
			if strings.Contains(value, secret) {
				t.Fatalf("journal echoes the credential: %q", value)
			}
		}
	}
	if !rejected {
		t.Fatalf("no request_rejected transition was journaled: %v", kinds(h))
	}
	if err := filepath.WalkDir(f.state, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, []byte(secret)) {
			t.Errorf("%s retains the credential echo", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
