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

type memory struct {
	mutant string
	mu     sync.Mutex
	seq    int
	jobs   map[string]*memoryJob
	fenced map[string]bool
	runs   map[string]int
}

type memoryJob struct {
	effect harness.Effect
	job    harness.Job
}

func newMemory(mutant string) *memory {
	return &memory{mutant: mutant, jobs: map[string]*memoryJob{}, fenced: map[string]bool{}, runs: map[string]int{}}
}

func (m *memory) Bind(_ context.Context, effect harness.Effect) (harness.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[effect.ID]
	switch {
	case !ok || m.mutant == "duplicate bind":
		j = m.bind(effect)
	case j.effect != effect && m.mutant != "rebind":
		return harness.Job{}, errors.New("changed command under an existing effect")
	}
	if m.mutant == "fenced bind fails" && m.fenced[effect.ID] {
		return harness.Job{}, errors.New("effect is fenced")
	}
	return j.job, nil
}

func (m *memory) bind(effect harness.Effect) *memoryJob {
	m.seq++
	j := &memoryJob{effect: effect, job: harness.Job{ID: fmt.Sprintf("job-%d", m.seq), Status: harness.JobPending}}
	if m.fenced[effect.ID] && m.mutant != "late bind" {
		j.job.Status = harness.JobCancelled
	}
	m.jobs[effect.ID] = j
	if m.mutant == "bind runs" {
		m.run(j)
	}
	return j
}

func (m *memory) run(j *memoryJob) {
	m.runs[j.effect.ID]++
	code := 0
	j.job.Status, j.job.ExitCode = harness.JobSucceeded, &code
}

func (m *memory) Start(_ context.Context, effectID string) (harness.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[effectID]
	if !ok {
		if m.mutant != "unbound start" {
			return harness.Job{}, errors.New("no job is bound")
		}
		j = m.bind(harness.Effect{ID: effectID})
	}
	if m.mutant == "fenced start fails" && j.job.Status == harness.JobCancelled {
		return harness.Job{}, errors.New("job is cancelled")
	}
	pending := j.job.Status == harness.JobPending
	if m.mutant == "unlocked gate" {
		m.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		m.mu.Lock()
	}
	if pending || (m.mutant == "restart" && j.job.Status != harness.JobCancelled) {
		m.run(j)
	}
	if m.mutant == "foreign start" {
		return harness.Job{ID: "job-other", Status: j.job.Status}, nil
	}
	return j.job, nil
}

func (m *memory) Inspect(_ context.Context, effectID string) (harness.Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[effectID]
	if !ok && m.mutant == "inspect creates" {
		j, ok = m.bind(harness.Effect{ID: effectID}), true
	}
	if !ok && m.mutant == "fenced inspect fails" && m.fenced[effectID] {
		return harness.Job{}, false, errors.New("effect is fenced")
	}
	if !ok {
		return harness.Job{}, false, nil
	}
	return j.job, true, nil
}

func (m *memory) Cancel(_ context.Context, effectID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mutant == "unfenced" {
		return nil
	}
	if m.mutant == "cancel twice fails" && m.fenced[effectID] {
		return errors.New("already cancelled")
	}
	m.fenced[effectID] = true
	j, ok := m.jobs[effectID]
	if !ok && m.mutant == "cancel creates" {
		j, ok = m.bind(harness.Effect{ID: effectID}), true
	}
	if ok && (j.job.Status == harness.JobPending || m.mutant == "cancel rewrites") {
		j.job.Status = harness.JobCancelled
	}
	return nil
}

func fixture(m *memory) executortest.Fixture {
	return executortest.Fixture{
		Executor: m,
		Effect: func(name string) harness.Effect {
			return harness.Effect{ID: "effect-" + name, Command: "run " + name}
		},
		Runs: func(_ context.Context, effectID string) (int, error) {
			m.mu.Lock()
			defer m.mu.Unlock()
			return m.runs[effectID], nil
		},
	}
}

func checkContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCheckAcceptsConformingExecutor(t *testing.T) {
	if err := executortest.Check(checkContext(t), fixture(newMemory(""))); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRejectsEachViolation(t *testing.T) {
	for mutant, violated := range map[string]string{
		"bind runs":            "bind",
		"duplicate bind":       "bind",
		"rebind":               "changed-command",
		"restart":              "start-once",
		"foreign start":        "start-once",
		"cancel rewrites":      "start-once",
		"unbound start":        "unbound-start",
		"inspect creates":      "unbound-start",
		"unfenced":             "fence-bound",
		"cancel twice fails":   "fence-bound",
		"fenced start fails":   "fence-bound",
		"fenced bind fails":    "fence-unbound",
		"late bind":            "fence-unbound",
		"cancel creates":       "fence-unbound",
		"fenced inspect fails": "fence-unbound",
		"unlocked gate":        "concurrent-start",
	} {
		t.Run(mutant, func(t *testing.T) {
			err := executortest.Check(checkContext(t), fixture(newMemory(mutant)))
			if err == nil || !strings.Contains(err.Error(), "executortest: "+violated+":") {
				t.Fatalf("Check() = %v, want a %s violation", err, violated)
			}
		})
	}
}
