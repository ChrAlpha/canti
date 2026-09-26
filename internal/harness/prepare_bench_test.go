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
	runner, err := harness.NewRunner(s, exec)
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
			}
			snap, err = runner.Commit(ctx, snap, event, facts)
			if err != nil {
				t.Fatal(err)
			}
		}
		snap, err = runner.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: core.ExecutionFailedToStart},
			harness.Facts{Observation: &harness.Observation{Execution: core.ExecutionFailedToStart, Preview: strings.Repeat("output\n", 600)}})
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
		Environment: "/workspace", ArtifactDir: "/canti/artifacts", History: filepath.Join(dir, "history"),
		RunsPath: "/canti/runs", Limits: limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.PublishHistory(ctx, s, filepath.Join(dir, "history"), "/canti/artifacts", runID); err != nil {
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
