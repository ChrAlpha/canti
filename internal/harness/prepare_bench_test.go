package harness_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func prepareFixture(t testing.TB, completed, window int) (*probe, *harness.Loop) {
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
	for step := 1; step <= completed; step++ {
		w := &harness.Window{FirstStep: 1}
		if first := completed - window + 1; first > 1 && step >= first {
			w = &harness.Window{FirstStep: first, SourceStep: first - 1, Handover: "Continue inspecting the workspace."}
		}
		for _, event := range []core.Event{
			core.RequestFrozen{}, core.AttemptStarted{Step: step, Attempt: 1},
			core.ResponseSaved{Step: step, Attempt: 1}, core.ResponseLoaded{Step: step, Attempt: 1, Body: body},
		} {
			var facts harness.Facts
			switch event.(type) {
			case core.RequestFrozen:
				facts.Request = &harness.Request{Body: []byte(`{"seed":"` + strings.Repeat("x", 32<<10) + `"}`), Window: w}
			case core.ResponseSaved:
				facts.Response = &harness.Response{Body: body, Complete: true}
			case core.ResponseLoaded:
				facts.Action = &harness.Action{Name: harness.ActionCommand, Recovery: harness.RecoveryExternal}
				facts.Execution = fixtureExecution("go test ./...")
			}
			snap, err = runner.Commit(ctx, snap, event, facts)
			if err != nil {
				t.Fatal(err)
			}
		}
		effect := snap.State.EffectID

		snap, err = runner.Commit(ctx, snap, core.Observed{EffectID: effect, Execution: core.ExecutionFailedToStart},
			harness.Facts{Observation: &harness.Observation{Execution: core.ExecutionFailedToStart, Preview: strings.Repeat("output\n", 600)}, Receipt: &harness.Job{EffectID: effect, Spec: fixtureExecution("go test ./...").Spec, ID: "job-" + effect, Status: harness.JobFailedToStart, Preview: strings.Repeat("output\n", 600)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	m, err := anthropic.New(anthropic.Config{APIKey: "offline-unused", Model: "offline-benchmark"})
	if err != nil {
		t.Fatal(err)
	}
	p := &probe{Store: s}
	limits := harness.DefaultPromptLimits()
	limits.Total = 1 << 20
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
				t.Fatalf("prepare: %+v, %v", snap.State, err)
			}
			// After the first step, preparation also reads the previous
			// step's completion, which selects the window, in one record.
			completion := min(tc.completed, 1)
			if p.records != tc.window+completion || p.pages != (tc.window+historyPage-1)/historyPage+completion {
				t.Fatalf("read %d records in %d pages for a %d-step window", p.records, p.pages, tc.window)
			}
			body := string(p.frozen.Body)
			if got := strings.Count(body, "### Step "); got != tc.window {
				t.Fatalf("request has %d steps, want %d", got, tc.window)
			}
			previous := -1
			for step := tc.completed - tc.window + 1; step <= tc.completed; step++ {
				at := strings.Index(body, fmt.Sprintf("### Step %d\\n", step))
				if at <= previous {
					t.Fatalf("step %d is missing or out of order", step)
				}
				previous = at
			}
		})
	}
}
