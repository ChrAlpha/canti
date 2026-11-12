package harness_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

func prepareFixture(t testing.TB, completed, window int) (*probe, *harness.Loop) {
	return prepareFixtureTotal(t, completed, window, 1<<20)
}

func prepareFixtureTotal(t testing.TB, completed, window, total int) (*probe, *harness.Loop) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(ctx, filepath.Join(dir, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	state, err := core.NewState(runID, core.Limits{MaxSteps: completed + 1, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	run := harness.Run{ID: runID, Goal: "Inspect the workspace", Created: created}
	if err := s.CreateRun(ctx, run, state); err != nil {
		t.Fatal(err)
	}
	exec := &fakeExec{}
	runner, err := harness.NewRunner(s)
	if err != nil {
		t.Fatal(err)
	}
	snap := harness.Snapshot{State: state}
	body := []byte(`{"kind":"exec","payload":"go test ./..."}`)
	current := harness.InitialContext()
	commit := func(event core.Event, facts harness.Facts) {
		t.Helper()
		var err error
		snap, err = runner.Commit(ctx, snap, event, facts)
		if err != nil {
			t.Fatal(err)
		}
	}
	freeze := func() {
		capture := &harness.ContextCapture{Anchor: current}
		if n := snap.State.Step - current.FirstStep; n > 0 {
			capture.Bytes.Steps = make([]harness.ContextStepBytes, n)
		}
		commit(core.RequestFrozen{}, harness.Facts{Request: &harness.Request{Body: []byte(`{"seed":"` + strings.Repeat("x", 32<<10) + `"}`), ContextState: &current, Context: capture}})
	}
	for step := 1; step <= completed; step++ {
		if first := completed - window + 1; first > 1 && step == first {
			freeze()
			commit(core.AttemptStarted{Step: step, Attempt: 1}, harness.Facts{})
			next := harness.ContextState{AnchorSeq: snap.Seq + 1, FirstStep: step, Minimal: current.Minimal,
				Rejection: &harness.RejectionRef{Seq: snap.Seq + 1, Step: step}}
			commit(core.RequestRejected{Step: step, Attempt: 1}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`)}, Failure: "confirmed overflow", Rejection: &harness.ContextRejection{Previous: current, Signal: harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}, EpochSteps: step - current.FirstStep, Minimal: current.Minimal}, ContextState: &next})
			current = next
		}
		freeze()
		attempt := snap.State.Attempt
		commit(core.AttemptStarted{Step: step, Attempt: attempt}, harness.Facts{})
		commit(core.ResponseSaved{Step: step, Attempt: attempt}, harness.Facts{Response: &harness.Response{Body: body, Complete: true}})
		commit(core.ResponseLoaded{Step: step, Attempt: attempt, Body: body}, harness.Facts{Action: &harness.Action{Name: harness.ActionCommand, Recovery: harness.RecoveryExternal}, Execution: fixtureExecution("go test ./...")})
		observation := harness.Observation{Execution: core.ExecutionFailedToStart, Preview: strings.Repeat("output\n", 600)}
		receipt := fixtureReceipt(snap.State, observation)
		commit(core.Observed{EffectID: snap.State.EffectID, Execution: core.ExecutionFailedToStart}, harness.Facts{Observation: &observation, Receipt: &receipt})
	}
	m, err := anthropic.New(anthropic.Config{APIKey: "offline-unused", Model: "offline-benchmark"})
	if err != nil {
		t.Fatal(err)
	}
	p := &probe{Store: s}
	limits := harness.DefaultPromptLimits()
	limits.Total = total
	l, err := harness.NewLoop(p, exec, m, harness.LoopConfig{
		Environment: "/workspace", History: filepath.Join(dir, "history"),
		Limits: limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.PublishHistory(ctx, s, filepath.Join(dir, "history"), runID); err != nil {
		t.Fatal(err)
	}
	return p, l
}

func BenchmarkPrepareRequest(b *testing.B) {
	for _, window := range []int{5, 80} {
		b.Run(fmt.Sprintf("window=%d", window), func(b *testing.B) {
			p, l := prepareFixture(b, 128, window)
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				*p = probe{Store: p.Store}
				if snap, err := l.Step(ctx, runID); err != nil || snap.State.Phase != core.PhaseRequestFrozen || p.frozen == nil {
					b.Fatalf("prepare: %+v, %v", snap.State, err)
				}
			}
			b.ReportMetric(float64(p.records), "records/op")
			b.ReportMetric(float64(len(p.frozen.Body)), "request-B/op")
		})
	}
}

func TestPrepareReadsOnlyActiveWindow(t *testing.T) {
	for _, tc := range []struct{ completed, window int }{{0, 0}, {128, 5}, {128, 80}} {
		t.Run(fmt.Sprintf("completed=%d/window=%d", tc.completed, tc.window), func(t *testing.T) {
			p, l := prepareFixture(t, tc.completed, tc.window)
			snap, err := l.Step(context.Background(), runID)
			if err != nil || snap.State.Phase != core.PhaseRequestFrozen || p.frozen == nil {
				t.Fatalf("prepare: %+v, %v; failure %q", snap.State, err, p.failure)
			}
			// The resident epoch is read in pages, and the newest index
			// entries before it in one receipt page.
			indexed := 0
			if tc.completed > tc.window && tc.window > 0 {
				indexed = min(tc.completed-tc.window, harness.IndexSteps)
			}
			pages := (tc.window + historyPage - 1) / historyPage
			if indexed > 0 {
				pages++
			}
			if p.records != tc.window+indexed || p.pages != pages {
				t.Fatalf("read %d records in %d pages for a %d-step epoch and %d index entries", p.records, p.pages, tc.window, indexed)
			}
			body := string(p.frozen.Body)
			if got := len(p.frozen.Context.Bytes.Steps); got != tc.window {
				t.Fatalf("request has %d steps, want %d", got, tc.window)
			}
			previous := -1
			if missing := strings.Count(body, "### Step "); missing != tc.window {
				t.Fatalf("body has %d step headings, want %d:\n%.400s", missing, tc.window, body)
			}
			for step := tc.completed - tc.window + 1; step <= tc.completed; step++ {
				at := strings.Index(body, fmt.Sprintf("### Step %d", step))
				if at <= previous {
					t.Fatalf("step %d is missing or out of order (at %d, previous %d)", step, at, previous)
				}
				previous = at
			}
		})
	}
}

func TestEpochLoadingStopsAtTheResourceCap(t *testing.T) {
	p, l := prepareFixtureTotal(t, 128, 80, 5000)
	snap, err := l.Step(context.Background(), runID)
	if err != nil || snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopUnrecoverable {
		t.Fatalf("cap stop = %+v, %v", snap.State, err)
	}
	if p.frozen != nil || !strings.Contains(p.failure, "resource cap") || p.pages != 1 {
		t.Fatalf("the epoch was materialized past the cap: frozen=%v failure=%q pages=%d records=%d", p.frozen != nil, p.failure, p.pages, p.records)
	}
}
