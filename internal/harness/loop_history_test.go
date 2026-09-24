//go:build integration

package harness_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
)

// Budgets of TestLoopBoundsRequestsAsHistoryGrows, chosen before measuring;
// the step and read budgets follow from the prompt total.
const (
	commandBytes  = 2000
	previewBytes  = 4000
	seedPageSteps = 5
	artifactBytes = 256 << 10
	repetitions   = 9

	requestSlack  = 16 << 10 // Anthropic envelope and JSON escaping of the fixture's shell text
	requestGrowth = 64       // step-number digits between the two largest histories
	allocCeiling  = 8 << 20  // raised from 4 MiB after measuring: a build decodes a whole page of records
	allocFactor   = 1.5
	timeCeiling   = 250 * time.Millisecond
	timeFactor    = 4.0
)

var historyLadder = []int{50, 500, 2000}

func historyCommand(step int) string {
	return fill(fmt.Sprintf("cd /workspace/module_%05d && ", step), `grep -rn "TODO" --include='*.go' . 2>&1 | sort -u > todo.txt; `, commandBytes)
}

func historyPreview(step int) string {
	var b strings.Builder
	for i := 0; b.Len() < previewBytes; i++ {
		fmt.Fprintf(&b, "./module_%05d/file_%03d.go:%d: // TODO: handle \"edge\" cases <#%05d> & retry\n", step, i, i+1, step)
	}
	return b.String()[:previewBytes]
}

func fill(head, unit string, size int) string {
	return (head + strings.Repeat(unit, size/len(unit)+1))[:size]
}

func historyRequest(step int) *harness.Request {
	w := &harness.Window{FirstStep: (step-1)/seedPageSteps*seedPageSteps + 1}
	if w.FirstStep > 1 {
		w.SourceStep = w.FirstStep - 1
		w.Handover = "Continue the seeded history measurement."
	}
	return &harness.Request{Window: w, Body: fmt.Appendf(nil, `{"seeded_step":%d}`, step), Model: "seed", Protocol: "seed/v1", Adapter: "seed"}
}

func historyResponse(step int) *harness.Response {
	return &harness.Response{Body: []byte(execReply(historyCommand(step)).body), Complete: true, Model: "seed", Protocol: "seed/v1", Adapter: "seed", Usage: usage}
}

func historyObservation(s core.State) *harness.Observation {
	code := 0
	return &harness.Observation{Execution: core.ExecutionExited, ExitCode: &code, Preview: historyPreview(s.Step),
		Output: harness.ArtifactRef{Path: jobID(s.EffectID) + ".log", Bytes: artifactBytes, Complete: true, Available: true}}
}

// growHistory commits the events and facts of the loop's exec cycle until the
// run is idle at step n+1, seeding bounded context windows, and
// publishes the step records of the whole run, as a loop would have.
func growHistory(t *testing.T, f *fixture, r harness.Runner, n int) {
	t.Helper()
	ctx := context.Background()
	snap, err := f.store.Latest(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	for s := snap.State; s.Phase != core.PhaseIdle || s.Step <= n; s = snap.State {
		var event core.Event
		var facts harness.Facts
		switch s.Phase {
		case core.PhaseIdle:
			event, facts = core.RequestFrozen{}, harness.Facts{Request: historyRequest(s.Step)}
		case core.PhaseRequestFrozen:
			event = core.AttemptStarted{Step: s.Step, Attempt: s.Attempt}
		case core.PhaseRequestSent:
			event, facts = core.ResponseSaved{Step: s.Step, Attempt: s.Attempt}, harness.Facts{Response: historyResponse(s.Step)}
		case core.PhaseResponseSaved:
			event = core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: historyResponse(s.Step).Body}
		case core.PhaseEffectSaved:
			event = core.JobBound{EffectID: s.EffectID, JobID: jobID(s.EffectID)}
		case core.PhaseJobBound:
			event = core.JobStarted{EffectID: s.EffectID, JobID: s.JobID}
		case core.PhaseAwaitingObservation:
			event, facts = core.Observed{EffectID: s.EffectID, Execution: core.ExecutionExited}, harness.Facts{Observation: historyObservation(s)}
		default:
			t.Fatalf("seeding reached %+v", s)
		}
		if snap, err = r.Commit(ctx, snap, event, facts); err != nil {
			t.Fatalf("seed %s at %+v: %v", event.Kind(), s, err)
		}
	}
	if err := os.RemoveAll(filepath.Join(f.cfg.History, runID)); err != nil {
		t.Fatal(err)
	}
	if err := harness.PublishHistory(ctx, f.store, f.cfg.History, f.cfg.ArtifactDir, runID); err != nil {
		t.Fatal(err)
	}
}

type historySample struct {
	req                                harness.Request
	steps, transitions                 int64
	storeBytes                         int64
	prompt, shown                      int
	reads, pages, records, recordBytes int
	allocBytes, allocs                 uint64
	elapsed, cold                      time.Duration
}

func median[T uint64 | time.Duration](xs []T) T {
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[len(s)/2]
}

// measureRequest builds the next request of the run as a restarted
// controller does, through Loop.Step on a newly opened store: repeatedly
// without recording it, then once committing it.
func measureRequest(t *testing.T, f *fixture, m harness.Model, n int) historySample {
	t.Helper()
	ctx := context.Background()
	store := f.open()
	p := &probe{Store: store}
	l, err := harness.NewLoop(p, f.exec, m, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	var s historySample
	var bytes, allocs []uint64
	var times []time.Duration
	var before, after runtime.MemStats
	for i := range repetitions + 1 {
		*p = probe{Store: store}
		// Two collections empty every sync.Pool, so no repetition reuses a buffer.
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		snap, err := l.Step(ctx, runID)
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		if err != nil || snap.State.Phase != core.PhaseRequestFrozen || snap.State.Step != n+1 || p.frozen == nil {
			t.Fatalf("Step() at %d steps = %+v, %v; want a request frozen for step %d", n, snap.State, err, n+1)
		}
		if i == 0 {
			s.cold, s.req = elapsed, *p.frozen
			s.reads, s.pages, s.records, s.recordBytes = p.reads, p.pages, p.records, p.recordBytes
			continue
		}
		if !reflect.DeepEqual(*p.frozen, s.req) || p.reads != s.reads || p.records != s.records {
			t.Fatalf("repeated request at %d steps differs: %d bytes, %d reads, %d records", n, len(p.frozen.Body), p.reads, p.records)
		}
		times = append(times, elapsed)
		bytes = append(bytes, after.TotalAlloc-before.TotalAlloc)
		allocs = append(allocs, after.Mallocs-before.Mallocs)
	}
	s.elapsed, s.allocBytes, s.allocs = median(times), median(bytes), median(allocs)

	committed, err := harness.NewLoop(store, f.exec, m, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := committed.Step(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if stored, err := store.Request(ctx, runID, n+1, 1); err != nil || !reflect.DeepEqual(stored, s.req) {
		t.Fatalf("committed request at %d steps = %d bytes, %v; want the %d measured bytes", n, len(stored.Body), err, len(s.req.Body))
	}
	totals, err := store.Totals(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	s.steps, s.transitions = int64(totals.Steps), snap.Seq

	var body struct {
		System   string
		Messages []struct {
			Role    string
			Content []struct{ Type, Text string }
		}
	}
	if err := json.Unmarshal(s.req.Body, &body); err != nil || len(body.Messages) != 1 {
		t.Fatalf("request body does not decode to one message: %v", err)
	}
	var user string
	for _, b := range body.Messages[0].Content {
		user += b.Text
	}
	s.prompt, s.shown = len(body.System)+len(user), strings.Count(user, "\n### Step ")
	wantContains(t, user, fmt.Sprintf("\n%d earlier steps are not shown.\n", n-s.shown), fmt.Sprintf("\n### Step %d\n", n-s.shown+1),
		fmt.Sprintf("\n### Step %d\n", n), historyPreview(n), "Continue the seeded history measurement.")

	for _, name := range []string{f.path, f.path + "-wal"} {
		if info, err := os.Stat(name); err == nil {
			s.storeBytes += info.Size()
		}
	}
	return s
}

func TestLoopBoundsRequestsAsHistoryGrows(t *testing.T) {
	top := historyLadder[len(historyLadder)-1]
	f := newFixture(t, core.Limits{MaxSteps: top + 1, MaxAttempts: 3}, harness.Budget{Tokens: 1 << 50, OutputBytes: 1 << 50})
	m, err := anthropic.New(anthropic.Config{APIKey: "offline-unused", Model: "claude-opus-5"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := harness.NewRunner(f.store, f.exec)
	if err != nil {
		t.Fatal(err)
	}
	limits := harness.DefaultPromptLimits()
	budget := limits.Total + requestSlack
	// Every shown step renders its whole command and preview.
	fit := seedPageSteps
	maxRecords, maxPages := fit+historyPage, fit/historyPage+1
	maxReads := 4 + maxPages

	var samples []historySample
	for _, n := range historyLadder {
		growHistory(t, f, r, n)
		samples = append(samples, measureRequest(t, f, m, n))
	}

	req := samples[0].req
	t.Logf("offline fixture: %s %s/%s GOMAXPROCS=%d, %s %s %s, limits %+v", runtime.Version(), runtime.GOOS, runtime.GOARCH,
		runtime.GOMAXPROCS(0), req.Adapter, req.Protocol, req.Model, limits)
	t.Logf("budgets: request <= %d B, growth <= %d B, shown <= %d, records <= %d in <= %d pages, reads <= %d, alloc <= %d B and %.1fx, time <= %v and %.1fx",
		budget, requestGrowth, fit, maxRecords, maxPages, maxReads, allocCeiling, allocFactor, timeCeiling, timeFactor)
	t.Logf("%6s %7s %10s %10s %8s %7s %5s %7s %5s %5s %7s %9s %6s %10s %10s", "steps", "trans", "store B", "history B",
		"request", "prompt", "shown", "records", "pages", "reads", "read B", "alloc B", "allocs", "median", "cold")
	for _, s := range samples {
		t.Logf("%6d %7d %10d %10d %8d %7d %5d %7d %5d %5d %7d %9d %6d %10v %10v", s.steps, s.transitions, s.storeBytes,
			s.steps*(commandBytes+previewBytes), len(s.req.Body), s.prompt, s.shown, s.records, s.pages, s.reads,
			s.recordBytes, s.allocBytes, s.allocs, s.elapsed, s.cold)
	}

	first, last := samples[0], samples[len(samples)-1]
	for i, s := range samples {
		n := historyLadder[i]
		if s.steps != int64(n) {
			t.Errorf("%d steps: totals count %d steps", n, s.steps)
		}
		if len(s.req.Body) > budget || s.prompt > limits.Total {
			t.Errorf("%d steps: request of %d bytes with a %d-byte prompt exceeds %d or %d bytes", n, len(s.req.Body), s.prompt, budget, limits.Total)
		}
		if s.shown < 1 || s.shown > fit || s.shown != first.shown || s.records > maxRecords || s.records != min(n, last.records) ||
			s.pages > maxPages || s.reads > maxReads || s.reads != first.reads {
			t.Errorf("%d steps: showed %d steps from %d records in %d pages and %d reads; want the %d steps shown at %d steps, "+
				"at most %d, from at most %d records or the whole history, in at most %d pages and the %d reads at %d steps, at most %d",
				n, s.shown, s.records, s.pages, s.reads, first.shown, historyLadder[0], fit, maxRecords, maxPages, first.reads, historyLadder[0], maxReads)
		}
		if s.allocBytes > allocCeiling || float64(s.allocBytes) > allocFactor*float64(first.allocBytes) || float64(s.allocs) > allocFactor*float64(first.allocs) {
			t.Errorf("%d steps: allocated %d bytes in %d allocations, over %d bytes or %.1fx of %d bytes in %d",
				n, s.allocBytes, s.allocs, allocCeiling, allocFactor, first.allocBytes, first.allocs)
		}
		if s.elapsed > timeCeiling || s.cold > timeCeiling || float64(s.elapsed) > timeFactor*float64(first.elapsed) {
			t.Errorf("%d steps: built in %v, cold %v, over %v or %.1fx of %v", n, s.elapsed, s.cold, timeCeiling, timeFactor, first.elapsed)
		}
	}
	if prev := samples[len(samples)-2]; len(last.req.Body)-len(prev.req.Body) > requestGrowth {
		t.Errorf("request grew from %d to %d bytes between %d and %d steps", len(prev.req.Body), len(last.req.Body), prev.steps, last.steps)
	}

	ctx := context.Background()
	ts, err := f.store.Transitions(ctx, runID, 0, len(cycle))
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, tr := range ts {
		kinds = append(kinds, tr.Event.Kind())
	}
	if !slices.Equal(kinds, cycle) {
		t.Fatalf("first transitions = %v, want step 1's cycle %v", kinds, cycle)
	}
	one := ts[len(ts)-1].From
	if one.Step != 1 || one.Decision.Payload != historyCommand(1) {
		t.Errorf("step 1 state = %+v, want its command", one)
	}
	if got := ts[0].Facts.Request; !reflect.DeepEqual(got, historyRequest(1)) {
		t.Errorf("step 1 request = %+v, want %+v", got, historyRequest(1))
	}
	if got := ts[2].Facts.Response; !reflect.DeepEqual(got, historyResponse(1)) {
		t.Errorf("step 1 response = %+v, want %+v", got, historyResponse(1))
	}
	if got := ts[len(ts)-1].Facts.Observation; !reflect.DeepEqual(got, historyObservation(one)) {
		t.Errorf("step 1 observation = %+v, want %+v", got, historyObservation(one))
	}
	for i, s := range samples[:len(samples)-1] {
		if req, err := f.store.Request(ctx, runID, historyLadder[i]+1, 1); err != nil || !reflect.DeepEqual(req, s.req) {
			t.Errorf("request of step %d = %d bytes, %v; want the %d bytes frozen then", historyLadder[i]+1, len(req.Body), err, len(s.req.Body))
		}
	}
}
