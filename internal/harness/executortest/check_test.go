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
	next   harness.JobStatus
}

type memory struct {
	*backend
	latched map[string]bool
}

func newBackend(mutant string) *backend {
	return &backend{mutant: mutant, jobs: map[string]*memoryJob{}, fenced: map[string]bool{}, runs: map[string]int{}}
}

func (b *backend) pause() {
	b.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	b.mu.Lock()
}

func (b *backend) bind(effect harness.Effect) *memoryJob {
	b.seq++
	j := &memoryJob{effect: effect, job: harness.Job{ID: fmt.Sprintf("job-%d", b.seq), Status: harness.JobPending}}
	if b.fenced[effect.ID] && b.mutant != "late bind" {
		j.job.Status = harness.JobCancelled
	}
	b.jobs[effect.ID] = j
	if b.mutant == "bind runs" {
		b.runs[effect.ID]++
	}
	return j
}

func (b *backend) run(j *memoryJob) {
	b.runs[j.effect.ID]++
	j.job.Status, j.next = harness.JobRunning, harness.JobSucceeded
}

func (m *memory) Bind(_ context.Context, effect harness.Effect) (harness.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[effect.ID]
	if !ok && m.mutant == "racy bind" {
		m.pause()
	}
	switch {
	case !ok || m.mutant == "duplicate bind" || (m.mutant == "bind resets" && j.job.Status == harness.JobSucceeded):
		j = m.bind(effect)
	case m.mutant == "fence resets" && j.job.Status == harness.JobCancelled:
		j = m.bind(effect)
		j.job.Status = harness.JobPending
	case j.effect != effect && m.mutant != "rebind":
		return harness.Job{}, errors.New("changed command under an existing effect")
	}
	if m.mutant == "fenced bind fails" && m.fenced[effect.ID] {
		return harness.Job{}, errors.New("effect is fenced")
	}
	return j.job, nil
}

func (m *memory) Start(_ context.Context, effectID string) (harness.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[effectID]
	if !ok {
		if m.mutant == "unbound start" {
			return harness.Job{}, nil
		}
		return harness.Job{}, errors.New("no job is bound")
	}
	cancelled := j.job.Status == harness.JobCancelled
	switch {
	case cancelled && m.mutant == "fenced start fails":
		return harness.Job{}, errors.New("job is cancelled")
	case j.job.Status == harness.JobRunning && m.mutant == "busy start":
		return harness.Job{}, errors.New("job is already running")
	case cancelled && m.mutant == "late run":
		time.AfterFunc(time.Millisecond, func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.runs[effectID]++
		})
	case cancelled && m.mutant == "lying fence":
		m.runs[effectID]++
	case m.mutant == "volatile latch":
		if !cancelled && !m.latched[effectID] {
			m.latched[effectID] = true
			if j.job.Status == harness.JobPending {
				m.run(j)
			} else {
				m.runs[effectID]++
			}
		}
	case j.job.Status != harness.JobPending || m.fenced[effectID]:
	case m.mutant == "racy fence":
		j.job.Status, j.next = harness.JobRunning, harness.JobSucceeded
		m.pause()
		m.runs[effectID]++
	default:
		if m.mutant == "unlocked gate" {
			m.pause()
		}
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
	if j.job.Status == harness.JobPending && m.mutant == "inspect starts" {
		m.run(j)
	}
	job := j.job
	if j.next != "" {
		j.job.Status, j.next = j.next, ""
	}
	return job, true, nil
}

func (m *memory) Cancel(_ context.Context, effectID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mutant == "cancel twice fails" && m.fenced[effectID] {
		return errors.New("already cancelled")
	}
	m.fenced[effectID] = true
	j, ok := m.jobs[effectID]
	if !ok {
		return nil
	}
	switch status := j.job.Status; {
	case status == harness.JobPending && m.mutant == "silent fence":
	case status == harness.JobRunning && m.mutant == "optimistic cancel":
		j.job.Status, j.next = harness.JobCancelled, harness.JobSucceeded
	case status == harness.JobRunning:
		j.next = harness.JobCancelled
	case status == harness.JobPending, m.mutant == "cancel rewrites":
		j.job.Status = harness.JobCancelled
	}
	return nil
}

func fixture(b *backend) executortest.Fixture {
	return executortest.Fixture{
		Open: func(context.Context) (harness.Executor, error) {
			return &memory{backend: b, latched: map[string]bool{}}, nil
		},
		Effect: func(name string) harness.Effect {
			return harness.Effect{ID: "effect-" + name, Command: "run " + name}
		},
		Runs: func(_ context.Context, effectID string) (int, error) {
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.runs[effectID], nil
		},
	}
}

func checkContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCheckAcceptsConformingExecutor(t *testing.T) {
	if err := executortest.Check(checkContext(t), fixture(newBackend(""))); err != nil {
		t.Fatal(err)
	}
}

func TestCheckRejectsEachViolation(t *testing.T) {
	for _, tt := range []struct{ mutant, violated string }{
		{"bind runs", "bind"},
		{"duplicate bind", "bind"},
		{"inspect starts", "bind"},
		{"rebind", "changed-command"},
		{"volatile latch", "start-once"},
		{"bind resets", "start-once"},
		{"foreign start", "start-once"},
		{"cancel rewrites", "start-once"},
		{"unbound start", "unbound-start"},
		{"inspect creates", "unbound-start"},
		{"silent fence", "fence-bound"},
		{"fence resets", "fence-bound"},
		{"cancel twice fails", "fence-bound"},
		{"fenced start fails", "fence-bound"},
		{"lying fence", "fence-bound fence-unbound"},
		{"late run", "fence-bound fence-unbound"},
		{"fenced bind fails", "fence-unbound"},
		{"late bind", "fence-unbound"},
		{"fenced inspect fails", "fence-unbound"},
		{"unlocked gate", "concurrent-start"},
		{"racy bind", "concurrent-start"},
		{"busy start", "concurrent-start"},
		{"racy fence", "start-cancel"},
		{"optimistic cancel", "start-cancel"},
	} {
		t.Run(tt.mutant, func(t *testing.T) {
			err := executortest.Check(checkContext(t), fixture(newBackend(tt.mutant)))
			for _, violated := range strings.Fields(tt.violated) {
				if err == nil || !strings.Contains(err.Error(), "executortest: "+violated+":") {
					t.Fatalf("Check() = %v, want a %s violation", err, violated)
				}
			}
		})
	}
}
