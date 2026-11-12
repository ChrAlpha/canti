//go:build integration

package harness_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

type lostStabilityAck struct {
	harness.Journal
	remaining map[string]bool
	covered   map[string]int
}

var errLostStabilityAck = errors.New("injected lost commit acknowledgement")

func (j *lostStabilityAck) Commit(ctx context.Context, tr harness.Transition) error {
	if err := j.Journal.Commit(ctx, tr); err != nil {
		return err
	}
	kind := tr.Event.Kind()
	if j.remaining[kind] {
		delete(j.remaining, kind)
		j.covered[kind]++
		return errLostStabilityAck
	}
	return nil
}

func TestLoopLongStability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	steps := 80
	if raw := os.Getenv("CANTI_STABILITY_STEPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 10000 {
			t.Fatal("CANTI_STABILITY_STEPS must be 1..10000")
		}
		steps = n
	}
	maxSteps := steps
	if raw := os.Getenv("CANTI_STABILITY_UNLIMITED_STEPS"); raw != "" {
		unlimited, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatal(err)
		}
		if unlimited {
			maxSteps = 0
		}
	}
	reader, sources, manifest := stabilityCorpus(ctx, t)
	f := newFixture(t, core.Limits{MaxSteps: maxSteps, MaxAttempts: 5}, harness.Budget{})
	f.cfg.Reader = &reader
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 128 << 10
	defer func() { f.store.Close() }()
	ackKinds := func() map[string]bool {
		return map[string]bool{"request_frozen": true, "attempt_started": true, "response_incomplete": true, "request_rejected": true, "response_saved": true, "response_loaded": true, "performed": true, "observed": true}
	}
	ack := &lostStabilityAck{Journal: f.store, remaining: ackKinds(), covered: map[string]int{}}
	calls := map[int]int{}
	seenRequests := map[[2]int][32]byte{}
	retries, reopens, resets, repairs, sends, maxPrompt := 0, 0, 0, 0, 0, 0
	late := false
	type resource struct {
		heap        uint64
		goroutines  int
		descriptors int
	}
	resources := map[int]resource{}
	start := time.Now()
	for turn := 0; turn < steps*100; turn++ {
		before, err := f.store.Latest(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if before.State.Intent() == core.IntentHalt {
			break
		}
		step := before.State.Step
		if !late && steps >= 80 && step >= steps/2 {
			ack.remaining = ackKinds()
			late = true
		}
		var request harness.Request
		sending := before.State.Intent() == core.IntentSendRequest
		if sending {
			request, err = f.store.Request(ctx, runID, step, before.State.RequestRev)
			if err != nil || request.ContextState == nil {
				t.Fatalf("frozen request missing: %v", err)
			}
			answer := execReply(fmt.Sprintf("echo STABILITY_STEP_%05d", step))
			if step == steps {
				answer = returnReply("stability complete")
			} else if step%5 != 0 {
				source := sources[(step-step/5-1)%len(sources)]
				query, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/" + source, Reason: "verify original source", Limit: 8192})
				answer = execReply(harness.RetrievePrefix + string(query))
			}
			switch {
			case calls[step] == 0 && step > 1 && step%7 == 0:
				answer = reply{body: `{}`, err: &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}}
			case calls[step] == 0 && step%11 == 0:
				answer = reply{body: "partial", usage: usage, err: errors.New("scripted transport failure")}
			case before.State.RequestRev == 1 && step%17 == 0 && calls[step] <= 1:
				answer = reply{body: "invalid decision", usage: usage}
			}
			f.model.mu.Lock()
			f.model.replies = []reply{answer}
			f.model.mu.Unlock()
		}
		after, stepErr := f.loopOn(ack).Step(ctx, runID)
		if stepErr != nil && !errors.Is(stepErr, errLostStabilityAck) {
			t.Fatalf("step=%d turn=%d: %v", step, turn, stepErr)
		}
		if errors.Is(stepErr, errLostStabilityAck) {
			after, err = f.store.Latest(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.model.mu.Lock()
		if len(f.model.sent) > 0 {
			if !sending || len(f.model.sent) != 1 || !bytes.Equal(f.model.sent[0], request.Body) {
				t.Fatal("model send differs from committed bytes")
			}
			key := [2]int{step, before.State.RequestRev}
			sum := sha256.Sum256(request.Body)
			if prior, ok := seenRequests[key]; ok {
				if prior != sum {
					t.Fatal("same revision changed across retry")
				}
				retries++
			} else {
				seenRequests[key] = sum
			}
			calls[step]++
			sends++
		}
		for _, prompt := range f.model.prompts {
			n := len(prompt.System) + len(prompt.User())
			if n > f.cfg.Limits.Total {
				t.Fatal("request resource cap exceeded")
			}
			maxPrompt = max(maxPrompt, n)
		}
		f.model.prompts = nil
		f.model.sent = nil
		f.model.mu.Unlock()
		if after.State.Step != before.State.Step || after.State.Intent() == core.IntentHalt {
			completed := after.State.Step - 1
			if after.State.Intent() == core.IntentHalt {
				completed = steps
			}
			if completed == 50 || completed == 500 || after.State.Intent() == core.IntentHalt {
				runtime.GC()
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				descriptors, err := os.ReadDir("/proc/self/fd")
				if err != nil {
					t.Fatal(err)
				}
				resources[completed] = resource{mem.HeapAlloc, runtime.NumGoroutine(), len(descriptors)}
			}
		}
		if turn%13 == 0 {
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.store, err = sqlite.Open(ctx, f.path)
			if err != nil {
				t.Fatal(err)
			}
			ack.Journal = f.store
			reopens++
		}
	}
	final, err := f.store.Latest(ctx, runID)
	if err != nil || final.State.Phase != core.PhaseReturned || final.State.Step != steps {
		t.Fatalf("stability did not complete: %+v %v", final, err)
	}
	var want harness.Totals
	var previous *harness.Transition
	unique := map[string]bool{}
	retrievals, retrievedBytes := 0, 0
	current := harness.InitialContext()
	for after := int64(0); ; {
		page, err := f.store.Transitions(ctx, runID, after, 64)
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range page {
			if previous != nil && (previous.Seq+1 != tr.Seq || previous.To != tr.From) {
				t.Fatal("journal chain broken across restart")
			}
			next, err := tr.From.Next(tr.Event)
			if err != nil || next != tr.To {
				t.Fatalf("non-core transition: %+v %v", tr, err)
			}
			if tr.Facts.Request != nil {
				if tr.Facts.Request.ContextState == nil || !tr.Facts.Request.ContextState.Equal(current) {
					t.Fatal("frozen context differs from committed selection")
				}
				if len(tr.Facts.Request.Body) > f.cfg.Limits.Total {
					t.Fatal("serialized request exceeds cap")
				}
			}
			if tr.Event.Kind() == "request_rejected" {
				resets++
				if tr.Facts.ContextState == nil || tr.Facts.ContextState.AnchorSeq != tr.Seq || tr.Facts.ContextState.FirstStep != tr.From.Step {
					t.Fatal("reset changed step or lost anchor")
				}
			}
			if tr.Event.Kind() == "response_loaded" && tr.To.Phase == core.PhaseIdle {
				repairs++
			}
			if tr.Facts.ContextState != nil {
				current = *tr.Facts.ContextState
			}
			settled := tr.Event.Kind() == "response_saved" || tr.Event.Kind() == "response_incomplete" || tr.Event.Kind() == "request_rejected" || tr.Event.Kind() == "stop_requested" && tr.From.Phase == core.PhaseRequestSent
			lost := tr.Event.Kind() == "attempt_started" && tr.From.Phase == core.PhaseRequestSent
			if settled || lost {
				want.Coverage.Attempts++
				if response := tr.Facts.Response; response != nil {
					want.Usage.InputTokens += response.Usage.InputTokens
					want.Usage.OutputTokens += response.Usage.OutputTokens
					if response.Usage.InputReported {
						want.Coverage.InputReports++
					}
					if response.Usage.OutputReported {
						want.Coverage.OutputReports++
					}
				} else {
					want.Coverage.Lost++
				}
			}
			if tr.Event.Kind() == "observed" || tr.Event.Kind() == "performed" {
				want.Steps++
			}
			if observation := tr.Facts.Observation; observation != nil {
				want.OutputBytes += observation.Output.Bytes
			}
			switch tr.Event.Kind() {
			case "observed":
				if tr.Facts.Observation == nil || tr.Facts.Observation.Retrieval != nil || strings.HasPrefix(tr.From.Decision.Payload, "canti:") {
					t.Fatal("local action was observed as an external command")
				}
			case "performed":
				outcome := tr.Facts.Outcome
				if outcome == nil || outcome.Status != harness.OutcomeDone || outcome.Retrieval == nil || tr.Facts.Observation != nil || outcome.Retrieval.Error != "" || len(outcome.Retrieval.Sources) == 0 || !strings.Contains(tr.From.Decision.Payload, outcome.Retrieval.Query.Path) {
					t.Fatal("scheduled local retrieval did not commit its accepted original observation")
				}
			}
			if outcome := tr.Facts.Outcome; outcome != nil && outcome.Retrieval != nil {
				retrievals++
				record, err := f.store.Completion(ctx, runID, tr.Seq)
				if err != nil || record.Outcome == nil || record.Outcome.Retrieval == nil || len(record.Outcome.Retrieval.Evidence) != len(outcome.Retrieval.Sources) {
					t.Fatal("committed evidence does not read back")
				}
				for i, source := range outcome.Retrieval.Sources {
					text := record.Outcome.Retrieval.Evidence[i]
					sum := sha256.Sum256([]byte(text))
					if len(text) != source.Length || fmt.Sprintf("%x", sum) != source.SHA256 {
						t.Fatal("committed evidence differs from source hash")
					}
					unique[source.Path] = true
					retrievedBytes += source.Length
				}
			}
			copy := tr
			previous = &copy
			after = tr.Seq
		}
		if len(page) < 64 {
			break
		}
	}
	got, err := f.store.Totals(ctx, runID)
	if err != nil || got != want || got.Steps != steps-1 {
		t.Fatalf("durable totals=%+v want=%+v error=%v", got, want, err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = sqlite.Open(ctx, f.path)
	if err != nil {
		t.Fatal(err)
	}
	if durable, err := f.store.Totals(ctx, runID); err != nil || durable != got {
		t.Fatalf("totals changed after final reopen: %+v %v", durable, err)
	}
	final, err = f.store.Latest(ctx, runID)
	if err != nil || final.State.Phase != core.PhaseReturned || final.State.Step != steps {
		t.Fatalf("final state lost after reopen: %+v %v", final, err)
	}
	for _, job := range f.exec.jobs {
		if job.runs != 1 {
			t.Fatalf("accepted external effect replayed: %+v", job)
		}
	}
	if steps >= 80 {
		for kind := range ackKinds() {
			if ack.covered[kind] != 2 {
				t.Fatalf("lost acknowledgement %s covered %d times", kind, ack.covered[kind])
			}
		}
	}
	if steps >= 50 {
		base, end := resources[50], resources[steps]
		if base.heap == 0 || end.heap == 0 || end.heap > base.heap+(32<<20) || end.goroutines > base.goroutines+8 || end.descriptors > base.descriptors+8 {
			t.Fatalf("resource growth beyond bounds: first=%+v final=%+v", base, end)
		}
	}
	if steps >= 500 {
		base, mid := resources[50], resources[500]
		if mid.heap == 0 || mid.heap > base.heap+(32<<20) || mid.goroutines > base.goroutines+8 || mid.descriptors > base.descriptors+8 {
			t.Fatalf("resource growth at intermediate checkpoint: step50=%+v step500=%+v", base, mid)
		}
	}
	if steps >= 80 && (resets < 2 || retries == 0 || repairs == 0 || len(unique) == 0 || reopens == 0) {
		t.Fatalf("missing stability boundary: reset=%d retry=%d repair=%d sources=%d reopen=%d", resets, retries, repairs, len(unique), reopens)
	}
	if steps >= 8*len(sources) && len(unique) != len(sources) {
		t.Fatalf("read %d of %d corpus sources", len(unique), len(sources))
	}
	summary, _ := json.Marshal(map[string]any{
		"schedule": "loop-stability/2", "manifest_sha256": manifest, "time_budget_ms": (15 * time.Minute).Milliseconds(), "steps": steps, "requests": len(seenRequests), "sends": sends, "retries": retries, "retrievals": retrievals, "unique_files": len(unique), "retrieved_bytes": retrievedBytes, "context_resets": resets, "reopens": reopens, "max_prompt_bytes": maxPrompt, "elapsed_ms": time.Since(start).Milliseconds(), "repairs": repairs, "ack_counts": ack.covered, "heap_step_50": resources[50].heap, "heap_step_500": resources[500].heap, "heap_final": resources[steps].heap, "goroutines_step_50": resources[50].goroutines, "goroutines_step_500": resources[500].goroutines, "goroutines_final": resources[steps].goroutines, "descriptors_step_50": resources[50].descriptors, "descriptors_final": resources[steps].descriptors, "durable_totals": got,
	})
	t.Logf("CANTI_STABILITY %s", summary)
}
