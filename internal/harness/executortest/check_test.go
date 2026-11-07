package executortest_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/executortest"
)

type backend struct {
	mu      sync.Mutex
	mutant  string
	jobs    map[string]harness.Job
	effects map[string]harness.Effect
	runs    map[string]int
}
type memory struct{ *backend }

func (m *memory) Specification(command string) string {
	return harness.ConfigDigest([]byte("fixture:" + command))
}
func (m *memory) apply(ctx context.Context, e harness.Effect, op string) (harness.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil && m.mutant != "ignore cancellation" {
		return harness.Job{}, ctx.Err()
	}
	if e.Spec != m.Specification(e.Command) && m.mutant != "wrong spec" {
		return harness.Job{}, errors.New("wrong spec")
	}
	if old, ok := m.effects[e.ID]; ok && old != e && m.mutant != "changed identity" && m.mutant != "wrong spec" {
		return harness.Job{}, errors.New("changed effect")
	}
	j, ok := m.jobs[e.ID]
	if !ok {
		j = harness.Job{EffectID: e.ID, Spec: e.Spec, Status: harness.JobAbsent}
	}
	if op == "inspect" && m.mutant == "inspect creates" {
		op = "drive"
	}
	if op == "drive" && (!ok || m.mutant == "repeat launch" || m.mutant == "ignore fence" && j.Status == harness.JobCancelled) {
		now := time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)
		code := 0
		j = harness.Job{EffectID: e.ID, Spec: e.Spec, ID: "owner-" + e.ID, Status: harness.JobSucceeded, ExitCode: &code, StartedAt: now, FinishedAt: now, Preview: "ran"}
		if m.mutant == "foreign receipt" {
			j.EffectID = "foreign"
		}
		m.runs[e.ID]++
		ok = true
	}
	if op == "cancel" && (!ok || m.mutant == "rewrite terminal") {
		j = harness.Job{EffectID: e.ID, Spec: e.Spec, Status: harness.JobCancelled, Fenced: true}
		ok = true
	}
	if ok {
		m.jobs[e.ID], m.effects[e.ID] = j, e
	}
	return j, nil
}
func (m *memory) Drive(ctx context.Context, e harness.Effect) (harness.Job, error) {
	return m.apply(ctx, e, "drive")
}
func (m *memory) Inspect(ctx context.Context, e harness.Effect) (harness.Job, error) {
	return m.apply(ctx, e, "inspect")
}
func (m *memory) Cancel(ctx context.Context, e harness.Effect) (harness.Job, error) {
	return m.apply(ctx, e, "cancel")
}
func fixture(mutant string) executortest.Fixture {
	b := &backend{mutant: mutant, jobs: map[string]harness.Job{}, effects: map[string]harness.Effect{}, runs: map[string]int{}}
	return executortest.Fixture{
		Open: func(context.Context) (harness.Executor, error) { return &memory{b}, nil },
		Effect: func(name string) harness.Effect {
			return harness.Effect{ID: name, Command: "run " + name, Spec: (&memory{b}).Specification("run " + name)}
		},
		Runs: func(_ context.Context, id string) (int, error) {
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.runs[id], nil
		},
	}
}
func TestCheckAcceptsConformingExecutor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := executortest.Check(ctx, fixture("")); err != nil {
		t.Fatal(err)
	}
}
func TestCheckRejectsEachViolation(t *testing.T) {
	for _, mutant := range []string{"inspect creates", "changed identity", "wrong spec", "repeat launch", "ignore fence", "ignore cancellation", "foreign receipt", "rewrite terminal"} {
		t.Run(mutant, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := executortest.Check(ctx, fixture(mutant))
			if err == nil {
				t.Fatal("accepted violating executor")
			}
			if !strings.Contains(err.Error(), "executortest") {
				t.Fatal(fmt.Errorf("unexpected error: %w", err))
			}
		})
	}
}
