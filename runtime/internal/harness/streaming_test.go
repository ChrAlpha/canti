package harness_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/openai"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

// TestARunSurvivesAGenerationLongerThanAWholeResponseTimeout drives a real
// adapter over a scripted SSE server with a 100ms idle timeout. The generation
// lasts longer than that whole-response timeout but keeps sending bytes, so
// the run must reach its return decision.
func TestARunSurvivesAGenerationLongerThanAWholeResponseTimeout(t *testing.T) {
	const idle = 100 * time.Millisecond
	decision := `{"kind":"return","payload":"done"}`
	chunks := []string{`{"kind":`, `"return",`, `"payload":`, `"done"}`, "", "", "", ""}
	for _, api := range []string{"anthropic-messages", "openai-responses"} {
		t.Run(api, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("content-type", "text/event-stream")
				if api == "anthropic-messages" {
					writeSlowMessages(w, chunks)
				} else {
					writeSlowResponses(w, decision, chunks)
				}
			}))
			defer srv.Close()
			var model harness.Model
			var err error
			if api == "anthropic-messages" {
				model, err = anthropic.New(anthropic.Config{BaseURL: srv.URL, APIKey: "test-credential-key", Model: "test-model", IdleTimeout: idle})
			} else {
				model, err = openai.New(openai.Config{BaseURL: srv.URL, APIKey: "test-credential-key", Model: "test-model", IdleTimeout: idle})
			}
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			store, err := sqlite.Open(context.Background(), filepath.Join(dir, "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			clock := &clock{now: created}
			executor := &fakeExec{clock: clock, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
			loop, err := harness.NewLoop(store, executor, model, harness.LoopConfig{
				Environment: "A Linux environment.", History: filepath.Join(dir, "history"), Poll: time.Millisecond, Backoff: -1, Now: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			state, err := core.NewState(runID, core.Limits{MaxSteps: 2, MaxAttempts: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CreateRun(context.Background(), harness.Run{ID: runID, Goal: "Return done.", Constraints: "none", Created: created}, state); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			started := time.Now()
			snap, err := loop.Run(ctx, runID)
			if err != nil || snap.State.Phase != core.PhaseReturned || snap.State.Decision.Payload != "done" {
				t.Fatalf("run = %+v, %v", snap.State, err)
			}
			if elapsed := time.Since(started); elapsed < 250*time.Millisecond {
				t.Fatalf("generation lasted %v, shorter than the 100ms whole-response timeout", elapsed)
			}
			transitions, err := store.Transitions(context.Background(), runID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			var attempts, saved int
			for _, transition := range transitions {
				switch transition.Event.Kind() {
				case "attempt_started":
					attempts++
				case "response_saved":
					saved++
				}
			}
			if attempts != 1 || saved != 1 {
				t.Fatalf("attempts=%d saved=%d, want one live attempt", attempts, saved)
			}
		})
	}
}

func writeSlowMessages(w http.ResponseWriter, chunks []string) {
	_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_slow\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"test-model\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
	_, _ = io.WriteString(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
	w.(http.Flusher).Flush()
	for _, chunk := range chunks {
		time.Sleep(40 * time.Millisecond)
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", data)
		w.(http.Flusher).Flush()
	}
	_, _ = io.WriteString(w, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
	_, _ = io.WriteString(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":9}}\n\n")
	_, _ = io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func writeSlowResponses(w http.ResponseWriter, decision string, chunks []string) {
	_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_slow\",\"status\":\"in_progress\",\"output\":[],\"usage\":null}}\n\n")
	w.(http.Flusher).Flush()
	for _, chunk := range chunks {
		time.Sleep(40 * time.Millisecond)
		data, _ := json.Marshal(chunk)
		fmt.Fprintf(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":%s}\n\n", data)
		w.(http.Flusher).Flush()
	}
	response, _ := json.Marshal(map[string]any{
		"id": "resp_slow", "object": "response", "model": "test-model", "status": "completed",
		"output": []any{map[string]any{"type": "message", "role": "assistant", "content": []map[string]any{{"type": "output_text", "text": decision}}}},
		"usage":  map[string]int{"input_tokens": 1, "output_tokens": 9},
	})
	fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
}

func TestATypedRejectionIsRedactedThroughTheJournal(t *testing.T) {
	const secret = "typed-secret-placeholder"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", secret)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"type":"`+secret+`_type","code":"context_length_exceeded","message":"`+secret+`"}}`)
	}))
	defer srv.Close()
	model, err := openai.New(openai.Config{BaseURL: srv.URL, APIKey: secret, Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := sqlite.Open(context.Background(), filepath.Join(dir, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clock := &clock{now: created}
	executor := &fakeExec{clock: clock, jobs: map[string]*fakeJob{}, fenced: map[string]bool{}}
	loop, err := harness.NewLoop(store, executor, model, harness.LoopConfig{
		Environment: "A Linux environment.", History: filepath.Join(dir, "history"), Poll: time.Millisecond, Backoff: -1, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := core.NewState(runID, core.Limits{MaxSteps: 2, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRun(context.Background(), harness.Run{ID: runID, Goal: "Reject.", Constraints: "none", Created: created}, state); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := loop.Run(ctx, runID); err != nil {
		t.Fatal(err)
	}
	transitions, err := store.Transitions(context.Background(), runID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var rejection *harness.ContextRejection
	var failure string
	for _, tr := range transitions {
		if tr.Facts.Rejection != nil {
			rejection, failure = tr.Facts.Rejection, tr.Facts.Failure
		}
	}
	if rejection == nil {
		t.Fatal("no committed context rejection")
	}
	if strings.Contains(failure, secret) || strings.Contains(rejection.Signal.Error(), secret) ||
		strings.Contains(rejection.Signal.Type, secret) || strings.Contains(rejection.Signal.Code, secret) ||
		strings.Contains(rejection.Signal.StopReason, secret) || strings.Contains(rejection.Signal.RequestID, secret) {
		t.Fatalf("rejection echoes the placeholder: failure=%q signal=%+v", failure, rejection.Signal)
	}
}
