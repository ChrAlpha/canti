package harness_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// waiting is the fake executor with wait as its Waiter.
type waiting struct {
	*fakeExec
	wait func(ctx context.Context, effectID string) error
}

func (w waiting) Wait(ctx context.Context, effectID string) error {
	w.mu.Lock()
	w.calls = append(w.calls, "wait")
	w.mu.Unlock()
	return w.wait(ctx, effectID)
}

func (f *fixture) waitingLoop(wait func(ctx context.Context, effectID string) error) *harness.Loop {
	f.t.Helper()
	l, err := harness.NewLoop(f.store, waiting{f.exec, wait}, f.model, f.cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	return l
}

func succeed(j *harness.Job) {
	code := 0
	j.Status, j.ExitCode, j.Preview = harness.JobSucceeded, &code, "done\n"
	j.Output = harness.ArtifactRef{Path: j.ID + ".log", Bytes: 5, Complete: true, Available: true}
}

func TestLoopObservesAWaitedJobOnceItExits(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("sleep 1"))
	f.exec.hold = true
	f.cfg.Poll = time.Hour
	effect := f.steps(f.loop(), 5).State.EffectID
	exited := make(chan struct{})
	l := f.waitingLoop(func(ctx context.Context, _ string) error {
		select {
		case <-exited:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	time.AfterFunc(50*time.Millisecond, func() {
		f.exec.set(effect, succeed)
		close(exited)
	})
	began := time.Now()
	snap, err := l.Step(f.bounded(), runID)
	if err != nil || snap.State.Phase != core.PhaseIdle || time.Since(began) > 5*time.Second {
		t.Fatalf("Step() = %+v, %v after %v; want the exit observed at once despite a poll interval of an hour", snap.State, err, time.Since(began))
	}
	if obs := observation(t, f.last()); obs.Execution != core.ExecutionExited || obs.Preview != "done\n" {
		t.Fatalf("observation = %+v", obs)
	}
	if n := count(f.exec.log(), "wait"); n != 1 {
		t.Fatalf("waited %d times, want once", n)
	}
}

func TestLoopWaitsNoLongerThanTheBudgetAllows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		budget  harness.Budget
		elapsed time.Duration
		want    time.Duration
	}{
		{"no budget", harness.Budget{}, time.Hour, 30 * time.Second},
		{"job timeout", harness.Budget{JobTimeout: 10 * time.Second}, 9700 * time.Millisecond, 300 * time.Millisecond},
		{"wall time", harness.Budget{WallTime: 10 * time.Second}, 9700 * time.Millisecond, 300 * time.Millisecond},
		{"wall time first", harness.Budget{JobTimeout: 10 * time.Second, WallTime: 9900 * time.Millisecond}, 9700 * time.Millisecond, 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, limits, tc.budget, execReply("sleep 600"))
			f.exec.hold = true
			f.cfg.Poll = time.Hour
			wantState(t, f.steps(f.loop(), 5), core.PhaseAwaitingObservation, "")
			f.clock.Advance(tc.elapsed)
			var got time.Duration
			l := f.waitingLoop(func(ctx context.Context, effectID string) error {
				deadline, ok := ctx.Deadline()
				if got = time.Until(deadline); !ok {
					got = -1
				}
				f.exec.set(effectID, succeed)
				return nil
			})
			ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
			defer cancel()
			if snap, err := l.Step(ctx, runID); err != nil || snap.State.Phase != core.PhaseIdle {
				t.Fatalf("Step() = %+v, %v", snap.State, err)
			}
			if got > tc.want || got < tc.want-time.Second {
				t.Fatalf("the wait could last %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLoopPollsWhenAWaitIsNoUse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		waits int
	}{
		{"wait fails", errors.New("daemon unavailable"), 5},
		{"wait returns early", nil, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, execReply("sleep 600"))
			f.exec.hold = true
			f.cfg.Poll = 20 * time.Millisecond
			effect := f.steps(f.loop(), 5).State.EffectID
			inspections := 0
			f.exec.tick = func() {
				if inspections++; inspections == 6 {
					f.exec.finish(f.exec.jobs[effect], harness.JobSucceeded)
				}
			}
			l := f.waitingLoop(func(context.Context, string) error { return tc.err })
			began := time.Now()
			if snap, err := l.Step(f.bounded(), runID); err != nil || snap.State.Phase != core.PhaseIdle {
				t.Fatalf("Step() = %+v, %v", snap.State, err)
			}
			if waits := count(f.exec.log(), "wait"); waits != tc.waits || time.Since(began) < time.Duration(5-tc.waits)*f.cfg.Poll {
				t.Fatalf("waited %d times in %v over %d inspections, want %d with a poll between the others: %v", waits, time.Since(began), inspections, tc.waits, f.exec.log())
			}
		})
	}
}
