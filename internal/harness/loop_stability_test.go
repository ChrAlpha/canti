//go:build integration

package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
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
		var err error
		steps, err = strconv.Atoi(raw)
		if err != nil || steps < 1 || steps > 10000 {
			t.Fatal("CANTI_STABILITY_STEPS must be 1..10000")
		}
	}
	windowSteps := 3
	if raw := os.Getenv("CANTI_STABILITY_WINDOW_STEPS"); raw != "" {
		var err error
		windowSteps, err = strconv.Atoi(raw)
		if err != nil || windowSteps != 0 && windowSteps != 3 {
			t.Fatal("CANTI_STABILITY_WINDOW_STEPS must be 0 or 3")
		}
	}
	reader, sources, manifestSHA := stabilityCorpus(ctx, t)
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
	f := newFixture(t, core.Limits{MaxSteps: maxSteps, MaxAttempts: 5}, harness.Budget{})
	f.cfg.Reader = &reader
	f.cfg.WindowSteps = windowSteps
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 128 << 10
	defer func() { _ = f.store.Close() }()
	ackKinds := func() map[string]bool {
		return map[string]bool{
			"request_frozen": true, "attempt_started": true, "response_incomplete": true,
			"response_saved": true, "response_loaded": true, "job_bound": true,
			"job_started": true, "observed": true,
		}
	}
	ack := &lostStabilityAck{Journal: f.store, remaining: ackKinds(), covered: map[string]int{}}
	attempts := map[int]int{}
	firstSent := map[string][]byte{}
	sourceByStep := map[int]string{}
	nextSource := 0
	var retries, reopens, handovers, maxPrompt, sent int
	lateArmed := false
	type resource struct {
		heap       uint64
		goroutines int
	}
	resources := map[int]resource{}
	start := time.Now()
	for turn := 0; turn < steps*100; turn++ {
		before, err := f.store.Latest(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if before.State.Intent() == core.IntentHalt {
			if before.State.Phase != core.PhaseReturned || before.State.Step != steps {
				t.Fatalf("silent or early stop at step %d: %+v", before.State.Step, before.State)
			}
			break
		}
		var req harness.Request
		sending := before.State.Intent() == core.IntentSendRequest
		if sending {
			req, err = f.store.Request(ctx, runID, before.State.Step, before.State.RequestRev)
			if err != nil || req.Window == nil {
				t.Fatalf("missing frozen request at step %d: %v", before.State.Step, err)
			}
			step := before.State.Step
			if !lateArmed && steps >= 80 && step >= steps/2 && req.Window.Requested {
				for kind := range ackKinds() {
					if ack.covered[kind] != 1 {
						t.Fatalf("initial lost acknowledgement missing: %s", kind)
					}
				}
				ack.remaining = ackKinds()
				lateArmed = true
			}
			answer := execReply(fmt.Sprintf("echo STABILITY_STEP_%05d", step))
			if step == steps {
				answer = returnReply("stability complete")
			} else if req.Window.Requested {
				answer = execReply(fmt.Sprintf(`canti:handover {"run_id":"%s","first_step":%d,"text":"Continue at step %d; verify original evidence references."}`, runID, req.Window.FirstStep, step))
			} else if step%5 != 0 {
				source := sourceByStep[step]
				if source == "" {
					source = sources[nextSource%len(sources)]
					sourceByStep[step] = source
					nextSource++
				}
				query, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/" + source, Reason: "verify original source", Limit: 8192})
				answer = execReply("canti:retrieve " + string(query))
			}
			if attempts[step] == 0 && step%11 == 0 {
				answer = reply{body: "partial", usage: usage, err: errors.New("incomplete scripted response")}
			} else if before.State.RequestRev == 1 && step%17 == 0 && attempts[step] <= 1 {
				answer = reply{body: "invalid decision", usage: usage}
			}
			f.model.mu.Lock()
			f.model.replies = []reply{answer}
			f.model.mu.Unlock()
		}
		loop := f.loopOn(ack)
		after, stepErr := loop.Step(ctx, runID)
		if errors.Is(stepErr, errLostStabilityAck) {
			after, err = f.store.Latest(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.model.mu.Lock()
		for _, p := range f.model.prompts {
			maxPrompt = max(maxPrompt, len(p.System)+len(p.User()))
		}
		for _, body := range f.model.sent {
			if !sending || !bytes.Equal(body, req.Body) {
				t.Fatal("model received bytes other than the frozen request")
			}
			key := fmt.Sprintf("%d/%d", before.State.Step, before.State.RequestRev)
			if prior, ok := firstSent[key]; ok && !bytes.Equal(prior, body) {
				t.Fatal("retry changed frozen request bytes")
			}
			firstSent[key] = body
			sent++
			attempts[before.State.Step]++
		}
		f.model.prompts, f.model.sent = nil, nil
		f.model.mu.Unlock()
		f.exec.mu.Lock()
		f.exec.calls = nil
		if after.State.Step != before.State.Step {
			delete(f.exec.jobs, before.State.EffectID)
			delete(f.exec.fenced, before.State.EffectID)
		}
		f.exec.mu.Unlock()
		if after.State.Step != before.State.Step {
			delete(attempts, before.State.Step)
			delete(sourceByStep, before.State.Step)
			for rev := 1; rev <= before.State.RequestRev; rev++ {
				delete(firstSent, fmt.Sprintf("%d/%d", before.State.Step, rev))
			}
		}
		if stepErr != nil {
			if !errors.Is(stepErr, errLostStabilityAck) {
				t.Fatalf("step %d: %v", before.State.Step, stepErr)
			}
		}
		if after.State.Phase == core.PhaseStopped || after.State.Phase == core.PhaseNeedsResolution {
			t.Fatalf("unexpected terminal phase at step %d: %+v", after.State.Step, after.State)
		}
		if stepErr != nil || after.State.Step != before.State.Step && after.State.Step%13 == 0 {
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
		completed := after.State.Step - 1
		if after.State.Intent() == core.IntentHalt {
			completed = steps
		}
		if completed == 50 || completed == 500 || completed == steps {
			if _, sampled := resources[completed]; !sampled {
				runtime.GC()
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				resources[completed] = resource{stats.HeapAlloc, runtime.NumGoroutine()}
			}
		}
		if after.State.Intent() == core.IntentHalt {
			break
		}
		if turn == steps*100-1 {
			t.Fatal("stability loop exceeded finite transition budget")
		}
	}
	var want harness.Totals
	requests, repairs, retrievals, retrievedBytes := 0, 0, 0, 0
	unique := map[string]bool{}
	sequence := int64(0)
	windowFirst := 1
	var pendingHandover *harness.Handover
	var pendingStep int
	evidenceCounts := map[string]int{}
	for {
		page, err := f.store.Transitions(ctx, runID, sequence, 256)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, tr := range page {
			if tr.Seq != sequence+1 {
				t.Fatalf("sequence gap after %d: %d", sequence, tr.Seq)
			}
			sequence = tr.Seq
			switch tr.Event.Kind() {
			case "request_frozen":
				requests++
				if tr.To.RequestRev > 1 {
					repairs++
					previous, err := f.store.Request(ctx, runID, tr.To.Step, tr.To.RequestRev-1)
					if err != nil || previous.Window == nil || tr.Facts.Request == nil || tr.Facts.Request.Window == nil || *previous.Window != *tr.Facts.Request.Window || !strings.Contains(string(tr.Facts.Request.Body), "Rejected response") {
						t.Fatal("protocol repair did not preserve the frozen context window")
					}
				}
				if tr.Facts.Request == nil || tr.Facts.Request.Window == nil || len(tr.Facts.Request.Body) > f.cfg.Limits.Total {
					t.Fatal("request missing window or exceeds total byte limit")
				}
				w := tr.Facts.Request.Window
				if tr.To.RequestRev == 1 {
					if pendingHandover != nil {
						if w.FirstStep != pendingStep+1 || w.SourceStep != pendingStep || w.Handover != pendingHandover.Text {
							t.Fatal("accepted handover did not advance to its exact next window")
						}
						windowFirst = w.FirstStep
						pendingHandover = nil
						clear(evidenceCounts)
					} else if w.FirstStep != windowFirst {
						t.Fatal("context window advanced without an accepted handover or moved backward")
					}
				}
				for evidence, count := range evidenceCounts {
					if strings.Count(string(tr.Facts.Request.Body), evidence) != count {
						t.Fatal("retrieved original evidence missing or duplicated in frozen request")
					}
				}
				if w.FirstStep > 1 && (w.SourceStep != w.FirstStep-1 || w.Handover == "") {
					t.Fatal("handover provenance is discontinuous")
				}
				if w.FirstStep > 1 && strings.Contains(string(tr.Facts.Request.Body), "### Step 1\n") {
					t.Fatal("old history flowed into a new window")
				}
				if w.FirstStep > 1 && strings.Contains(string(tr.Facts.Request.Body), fmt.Sprintf("### Step %d\n", w.FirstStep-1)) {
					t.Fatal("previous window flowed into a new window")
				}
			case "response_incomplete":
				retries++
				fallthrough
			case "response_saved":
				if tr.Facts.Response != nil {
					want.Usage.InputTokens += tr.Facts.Response.Usage.InputTokens
					want.Usage.OutputTokens += tr.Facts.Response.Usage.OutputTokens
				}
			case "observed":
				want.Steps++
				if o := tr.Facts.Observation; o != nil {
					want.OutputBytes += o.Output.Bytes
					payload := tr.From.Decision.Payload
					if strings.HasPrefix(payload, "canti:retrieve ") != (o.Retrieval != nil) || strings.HasPrefix(payload, "canti:handover ") != (o.Handover != nil) {
						t.Fatal("builtin decision and observed result disagree")
					}
					if o.Retrieval != nil || o.Handover != nil {
						if o.Execution != core.ExecutionExited || o.ExitCode == nil || *o.ExitCode != 0 {
							t.Fatal("builtin observation did not exit successfully")
						}
					}
					if o.Retrieval != nil {
						if o.Retrieval.Error != "" || len(o.Retrieval.Sources) == 0 {
							t.Fatal("scheduled retrieval did not deliver source evidence")
						}
						retrievals++
						if !strings.Contains(payload, o.Retrieval.Query.Path) {
							t.Fatal("retrieval observed a different source path")
						}
						for _, source := range o.Retrieval.Sources {
							text, err := reader.Expand(ctx, source)
							if err != nil {
								t.Fatal("cannot verify retrieved original evidence")
							}
							evidence := fmt.Sprintf("Source %s:\nBytes [%d,%d), sha256 %s:\n%s\n", source.Path, source.Offset, source.Offset+int64(source.Length), source.SHA256, text)
							evidenceCounts[evidence]++
							unique[source.Path] = true
							retrievedBytes += source.Length
						}
					}
					if o.Handover != nil {
						handovers++
						pendingHandover = o.Handover
						pendingStep = tr.From.Step
					}
				}
			}
		}
	}
	got, err := f.store.Totals(ctx, runID)
	if err != nil || got != want {
		t.Fatalf("durable totals %+v, want %+v: %v", got, want, err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = sqlite.Open(ctx, f.path)
	if err != nil {
		t.Fatal(err)
	}
	if durable, err := f.store.Totals(ctx, runID); err != nil || durable != got {
		t.Fatalf("totals changed after reopen: %+v, %v", durable, err)
	}
	final, err := f.store.Latest(ctx, runID)
	if err != nil || final.State.Phase != core.PhaseReturned || final.State.Step != steps || want.Steps != steps-1 {
		t.Fatalf("final state %+v, totals %+v: %v", final.State, want, err)
	}
	if steps >= 80 {
		if !lateArmed && (windowSteps == 3 || steps >= 2000) {
			t.Fatal("late lost acknowledgements were not armed after a handover request")
		}
		for kind := range ackKinds() {
			wantCount := 1
			if lateArmed {
				wantCount = 2
			}
			if ack.covered[kind] != wantCount {
				t.Fatalf("lost acknowledgement %s covered %d times, want %d", kind, ack.covered[kind], wantCount)
			}
		}
	}
	if steps >= 50 {
		base, end := resources[50], resources[steps]
		if base.heap == 0 || end.heap == 0 || end.heap > base.heap+(32<<20) || end.goroutines > base.goroutines+8 {
			t.Fatalf("retained resources grew beyond budget: step50=%+v final=%+v", base, end)
		}
		if steps >= 500 {
			mid := resources[500]
			if mid.heap == 0 || mid.heap > base.heap+(32<<20) || mid.goroutines > base.goroutines+8 {
				t.Fatalf("retained resources grew beyond budget: step50=%+v step500=%+v", base, mid)
			}
		}
	}
	if (windowSteps == 3 && steps >= 8 || steps >= 2000) && handovers == 0 || steps >= 11 && retries == 0 || steps >= 17 && repairs == 0 || steps >= 2 && retrievals == 0 || steps >= 13 && reopens == 0 || sent == 0 || requests == 0 {
		t.Fatalf("missing stability behavior: sends=%d requests=%d retries=%d repairs=%d retrievals=%d handovers=%d reopens=%d", sent, requests, retries, repairs, retrievals, handovers, reopens)
	}
	if steps >= 8*len(sources) && len(unique) != len(sources) {
		t.Fatalf("retrieved %d of %d frozen sources", len(unique), len(sources))
	}
	ackTypes := make([]string, 0, len(ack.covered))
	for kind := range ack.covered {
		ackTypes = append(ackTypes, kind)
	}
	slices.Sort(ackTypes)
	summary, _ := json.Marshal(struct {
		Schedule       string         `json:"schedule"`
		ManifestSHA256 string         `json:"manifest_sha256"`
		TimeBudgetMS   int64          `json:"time_budget_ms"`
		WindowSteps    int            `json:"window_steps"`
		Steps          int            `json:"steps"`
		Requests       int            `json:"requests"`
		Retries        int            `json:"retries"`
		Retrievals     int            `json:"retrievals"`
		UniqueFiles    int            `json:"unique_files"`
		RetrievedBytes int            `json:"retrieved_bytes"`
		Handovers      int            `json:"handovers"`
		Reopens        int            `json:"reopens"`
		MaxPromptBytes int            `json:"max_prompt_bytes"`
		ElapsedMS      int64          `json:"elapsed_ms"`
		Repairs        int            `json:"repairs"`
		AckTypes       []string       `json:"ack_types"`
		AckCounts      map[string]int `json:"ack_counts"`
		HeapStep50     uint64         `json:"heap_step_50"`
		HeapStep500    uint64         `json:"heap_step_500"`
		HeapFinal      uint64         `json:"heap_final"`
		Goroutines50   int            `json:"goroutines_step_50"`
		Goroutines500  int            `json:"goroutines_step_500"`
		GoroutinesEnd  int            `json:"goroutines_final"`
		DurableTotals  harness.Totals `json:"durable_totals"`
	}{"loop-stability/1", manifestSHA, (15 * time.Minute).Milliseconds(), windowSteps, steps, requests, retries, retrievals, len(unique), retrievedBytes, handovers, reopens, maxPrompt, time.Since(start).Milliseconds(), repairs, ackTypes, ack.covered, resources[50].heap, resources[500].heap, resources[steps].heap, resources[50].goroutines, resources[500].goroutines, resources[steps].goroutines, got})
	t.Logf("CANTI_STABILITY %s", summary)
}
