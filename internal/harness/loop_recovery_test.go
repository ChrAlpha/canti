package harness_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

var running = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "job_bound", "job_started"}

func count(calls []string, name string) int {
	n := 0
	for _, c := range calls {
		if c == name {
			n++
		}
	}
	return n
}

func observation(t *testing.T, tr harness.Transition) harness.Observation {
	t.Helper()
	if tr.Facts.Observation == nil {
		t.Fatalf("%s transition has no observation", tr.Event.Kind())
	}
	return *tr.Facts.Observation
}

func TestLoopStopsAtWallTime(t *testing.T) {
	t.Run("before a request", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{WallTime: time.Minute}, returnReply("done"))
		f.clock.Advance(time.Hour)
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopTimedOut)
		f.wantKinds([]string{"stop_requested"})
		wantContains(t, f.last().Facts.Failure, "1h0m0s", "wall-time budget")
		if prompts, sent := f.model.calls(); prompts+sent != 0 {
			t.Fatal("the model was used after the wall time ran out")
		}
	})
	t.Run("during a running job", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{WallTime: time.Minute}, execReply("sleep 600"))
		f.exec.hold, f.exec.tick = true, func() { f.clock.Advance(40 * time.Second) }
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopTimedOut)
		f.wantKinds(running, []string{"stop_requested", "observed"})
		if got := f.exec.log(); !slices.Equal(got, []string{"bind", "start", "inspect", "inspect", "cancel", "inspect"}) {
			t.Fatalf("executor calls = %v", got)
		}
		if obs := observation(t, f.last()); obs.Execution != core.ExecutionCancelled || obs.Preview != "ran: sleep 600" {
			t.Fatalf("observation = %+v, want the cancelled job's result", obs)
		}
	})
}

func TestLoopTimesOutARunningJob(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{JobTimeout: 10 * time.Second}, execReply("sleep 600"), returnReply("gave up"))
	f.exec.hold, f.exec.tick = true, func() { f.clock.Advance(6 * time.Second) }
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	f.wantKinds(cycle, closing)
	if got := f.exec.log(); !slices.Equal(got, []string{"bind", "start", "inspect", "inspect", "cancel", "inspect"}) {
		t.Fatalf("executor calls = %v", got)
	}
	if obs := observation(t, f.transitions()[6]); obs.Execution != core.ExecutionTimedOut {
		t.Fatalf("observation = %+v, want timed_out", obs)
	}
	wantContains(t, f.model.prompts[1].User, "Execution: timed_out")
}

func TestLoopCancelsATimedOutJobOnce(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{JobTimeout: 10 * time.Second}, execReply("sleep 600"), returnReply("gave up"))
	inspections := 0
	f.exec.inspect = func(id string) (harness.Job, bool, error) {
		inspections++
		f.clock.Advance(6 * time.Second)
		job := harness.Job{ID: jobID(id), Status: harness.JobRunning, StartedAt: created}
		if inspections == 4 {
			job.Status, job.FinishedAt = harness.JobCancelled, created
		}
		return job, true, nil
	}
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	if n := count(f.exec.log(), "cancel"); n != 1 || inspections != 4 {
		t.Fatalf("cancelled %d times over %d inspections, want once over 4", n, inspections)
	}
	if obs := observation(t, f.transitions()[6]); obs.Execution != core.ExecutionTimedOut {
		t.Fatalf("observation = %+v, want timed_out without backend timing", obs)
	}
}

func TestLoopClassifiesACancelledJobAfterRestart(t *testing.T) {
	for _, tt := range []struct {
		ran  time.Duration
		want core.ExecutionState
	}{{15 * time.Second, core.ExecutionTimedOut}, {10 * time.Second, core.ExecutionTimedOut}, {5 * time.Second, core.ExecutionCancelled}} {
		t.Run(tt.ran.String(), func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{JobTimeout: 10 * time.Second}, execReply("sleep 600"))
			f.exec.hold = true
			snap := f.steps(f.loop(), 5)
			wantState(t, snap, core.PhaseAwaitingObservation, "")
			f.exec.set(snap.State.EffectID, func(j *harness.Job) {
				j.Status, j.StartedAt, j.FinishedAt = harness.JobCancelled, created, created.Add(tt.ran)
			})
			snap, err := f.fresh().Step(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, snap, core.PhaseIdle, "")
			if obs := observation(t, f.last()); obs.Execution != tt.want {
				t.Fatalf("observation = %+v, want %s", obs, tt.want)
			}
		})
	}
}

func TestLoopHaltsOnUnknownOutcomes(t *testing.T) {
	lost := fmt.Errorf("%w: container removed", harness.ErrJobLost)
	tests := []struct {
		name     string
		inspect  func(string) (harness.Job, bool, error)
		failure  string
		inspects int
	}{
		{"lost job", func(string) (harness.Job, bool, error) { return harness.Job{}, false, lost }, "container removed", 1},
		{"missing job", func(string) (harness.Job, bool, error) { return harness.Job{}, false, nil }, "no job", 1},
		{"foreign job", func(string) (harness.Job, bool, error) {
			return harness.Job{ID: "job-other", Status: harness.JobRunning}, true, nil
		}, "job-other", 1},
		{"unknown status", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobUnknown}, true, nil
		}, "unknown", 1},
		{"pending status", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobPending}, true, nil
		}, "pending", 1},
		{"exit without code", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobFailed}, true, nil
		}, "exit code", 1},
		{"inspect errors", func(string) (harness.Job, bool, error) {
			return harness.Job{}, false, errors.New("daemon unavailable")
		}, "daemon unavailable", 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, execReply("make test"))
			f.cfg.InspectRetries = 2
			f.exec.inspect = tt.inspect
			snap := f.run(f.loop())
			wantState(t, snap, core.PhaseNeedsResolution, "")
			f.wantKinds(running, []string{"outcome_unknown"})
			wantContains(t, f.last().Facts.Failure, tt.failure)
			if n := count(f.exec.log(), "inspect"); n != tt.inspects {
				t.Fatalf("inspected %d times, want %d", n, tt.inspects)
			}
			calls := f.exec.log()
			prompts, sent := f.model.calls()
			if again := f.run(f.fresh()); again != snap {
				t.Fatalf("Run() moved an unresolved run to %+v", again)
			}
			if p, s := f.model.calls(); !slices.Equal(f.exec.log(), calls) || p != prompts || s != sent {
				t.Fatal("Run() acted on an unresolved run")
			}
		})
	}
}

func TestLoopToleratesTransientInspectErrors(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("make check"), returnReply("done"))
	f.cfg.InspectRetries = 1
	inspections := 0
	f.exec.inspect = func(id string) (harness.Job, bool, error) {
		inspections++
		switch inspections {
		case 1, 3:
			return harness.Job{}, false, errors.New("daemon busy")
		case 2:
			return harness.Job{ID: jobID(id), Status: harness.JobRunning}, true, nil
		}
		code := 1
		return harness.Job{ID: jobID(id), Status: harness.JobFailed, ExitCode: &code, Preview: "FAIL"}, true, nil
	}
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	f.wantKinds(cycle, closing)
	if obs := observation(t, f.transitions()[6]); inspections != 4 || obs.Execution != core.ExecutionExited || *obs.ExitCode != 1 {
		t.Fatalf("observation after %d inspections = %+v, want the failed exit", inspections, obs)
	}
}

func unresolved(t *testing.T, inspect func(string) (harness.Job, bool, error), replies ...reply) *fixture {
	t.Helper()
	f := newFixture(t, limits, harness.Budget{}, append([]reply{execReply("make test")}, replies...)...)
	f.exec.inspect = inspect
	wantState(t, f.run(f.loop()), core.PhaseNeedsResolution, "")
	return f
}

func TestLoopResolveJobResumesInspection(t *testing.T) {
	f := unresolved(t, func(id string) (harness.Job, bool, error) {
		return harness.Job{ID: jobID(id), Status: harness.JobUnknown}, true, nil
	}, returnReply("tests pass"))
	f.exec.inspect = nil
	l := f.fresh()
	snap, err := l.ResolveJob(context.Background(), runID)
	if err != nil {
		t.Fatalf("ResolveJob() error = %v", err)
	}
	wantState(t, snap, core.PhaseAwaitingObservation, "")
	wantState(t, f.run(l), core.PhaseReturned, "")
	f.wantKinds(running, []string{"outcome_unknown", "job_bound", "observed"}, closing)
	if n := count(f.exec.log(), "start"); n != 1 {
		t.Fatalf("started %d times, want once", n)
	}
}

func TestLoopResolveJobRequiresAFoundJob(t *testing.T) {
	f := unresolved(t, func(string) (harness.Job, bool, error) { return harness.Job{}, false, nil })
	before := f.transitions()
	if _, err := f.fresh().ResolveJob(context.Background(), runID); err == nil {
		t.Fatal("ResolveJob() without a job succeeded")
	}
	if got := f.transitions(); !reflect.DeepEqual(got, before) {
		t.Fatalf("ResolveJob() committed %v", got[len(before):])
	}
}

func TestLoopResolveRecordsAnExplicitOutcome(t *testing.T) {
	missing := func(string) (harness.Job, bool, error) { return harness.Job{}, false, nil }
	t.Run("exited continues", func(t *testing.T) {
		f := unresolved(t, missing, returnReply("done"))
		code := 0
		snap, err := f.fresh().Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionExited, ExitCode: &code, Preview: "checked by hand"})
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		wantState(t, snap, core.PhaseIdle, "")
		if snap.State.Step != 2 {
			t.Fatalf("resolved run is at step %d, want 2", snap.State.Step)
		}
		wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
		wantContains(t, f.model.prompts[1].User, "set by explicit resolution", "checked by hand")
	})
	t.Run("unknown abandons", func(t *testing.T) {
		f := unresolved(t, missing)
		snap, err := f.fresh().Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionUnknown})
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopUnresolved)
	})
	t.Run("outside needs_resolution", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{})
		l := f.loop()
		if _, err := l.Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionExited}); err == nil {
			t.Error("Resolve() on an idle run succeeded")
		}
		if _, err := l.ResolveJob(context.Background(), runID); err == nil {
			t.Error("ResolveJob() on an idle run succeeded")
		}
		if len(f.transitions()) != 0 || len(f.exec.log()) != 0 {
			t.Fatal("a refused resolution committed or inspected")
		}
	})
}

func TestLoopStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Run("awaiting observation", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{JobTimeout: 10 * time.Second}, execReply("sleep 600"))
		f.exec.hold = true
		wantState(t, f.steps(f.loop(), 5), core.PhaseAwaitingObservation, "")
		f.clock.Advance(15 * time.Second)
		snap, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		f.wantKinds(running, []string{"stop_requested", "observed"})
		if obs := observation(t, f.last()); obs.Execution != core.ExecutionCancelled {
			t.Fatalf("observation = %+v, want cancelled", obs)
		}
		if got := f.exec.log(); !slices.Equal(got, []string{"bind", "start", "cancel", "inspect"}) {
			t.Fatalf("executor calls = %v", got)
		}
	})
	t.Run("effect saved without a job", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("sleep 600"))
		wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
		snap, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		f.wantKinds(closing, []string{"stop_requested", "observed"})
		if got := f.exec.log(); !slices.Equal(got, []string{"cancel", "inspect"}) {
			t.Fatalf("executor calls = %v, want the fence then a lookup", got)
		}
		if obs := observation(t, f.last()); obs.Execution != core.ExecutionCancelled || !strings.Contains(obs.Output.Detail, "no job") {
			t.Fatalf("observation = %+v, want a jobless cancellation", obs)
		}
	})
	t.Run("effect saved with an unrecorded bind", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("sleep 600"))
		snap := f.steps(f.loop(), 3)
		if _, err := f.exec.Bind(ctx, harness.Effect{ID: snap.State.EffectID, Command: "sleep 600"}); err != nil {
			t.Fatal(err)
		}
		snap, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		f.wantKinds(closing, []string{"stop_requested", "job_bound", "observed"})
		if got := f.exec.log(); !slices.Equal(got, []string{"bind", "cancel", "inspect", "cancel", "inspect"}) || count(got, "start") != 0 {
			t.Fatalf("executor calls = %v", got)
		}
	})
	t.Run("effect saved with a lost job", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("sleep 600"))
		f.steps(f.loop(), 3)
		f.exec.inspect = func(string) (harness.Job, bool, error) {
			return harness.Job{}, false, fmt.Errorf("%w: started by a stale controller", harness.ErrJobLost)
		}
		snap, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseNeedsResolution, core.StopCancelled)
		f.wantKinds(closing, []string{"stop_requested", "outcome_unknown"})
		wantContains(t, f.last().Facts.Failure, "stale controller")
	})
	t.Run("idle", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{})
		snap, err := f.loop().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		f.wantKinds([]string{"stop_requested"})
		if prompts, sent := f.model.calls(); prompts+sent+len(f.exec.log()) != 0 {
			t.Fatal("stopping an idle run did I/O")
		}
	})
	t.Run("needs resolution", func(t *testing.T) {
		f := unresolved(t, func(string) (harness.Job, bool, error) { return harness.Job{}, false, nil })
		l := f.fresh()
		snap, err := l.Stop(ctx, runID, core.StopTimedOut)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseNeedsResolution, core.StopTimedOut)
		if snap, err = l.Resolve(ctx, runID, harness.Observation{Execution: core.ExecutionCancelled}); err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopTimedOut)
	})
	t.Run("closed", func(t *testing.T) {
		for _, closed := range []func(*fixture){
			func(f *fixture) { f.run(f.loop()) },
			func(f *fixture) { f.loop().Stop(ctx, runID, core.StopCancelled) },
		} {
			f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
			closed(f)
			before := f.transitions()
			want, err := f.store.Latest(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			snap, err := f.fresh().Stop(ctx, runID, core.StopTimedOut)
			if err != nil || snap != want || !reflect.DeepEqual(f.transitions(), before) {
				t.Fatalf("Stop() on a closed run = %+v, %v; want %+v unchanged", snap, err, want)
			}
		}
	})
}

func TestLoopResumesAfterControllerShutdown(t *testing.T) {
	t.Run("during a request", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f := newFixture(t, limits, harness.Budget{}, reply{hook: cancel}, returnReply("done"))
		snap, err := f.loop().Run(ctx, runID)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want cancellation", err)
		}
		wantState(t, snap, core.PhaseRequestSent, "")
		f.wantKinds([]string{"request_frozen", "attempt_started"})
		wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
		f.wantKinds([]string{"request_frozen", "attempt_started", "attempt_started"}, closing[2:])
		if lost := f.transitions()[2]; lost.Event != (core.AttemptStarted{Step: 1, Attempt: 2}) || lost.To.Attempt != 2 {
			t.Fatalf("resume committed %#v to %+v, want attempt 2 counted", lost.Event, lost.To)
		}
		frozen := f.frozen(1, 1)
		if len(f.model.sent) != 2 || !bytes.Equal(f.model.sent[0], frozen) || !bytes.Equal(f.model.sent[1], frozen) {
			t.Fatal("the resumed send did not reuse the frozen bytes")
		}
	})
	t.Run("on the last attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 1}, harness.Budget{}, reply{hook: cancel}, returnReply("done"))
		if _, err := f.loop().Run(ctx, runID); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want cancellation", err)
		}
		wantState(t, f.run(f.fresh()), core.PhaseStopped, core.StopAttemptsExhausted)
		f.wantKinds([]string{"request_frozen", "attempt_started", "attempt_started"})
		if _, sent := f.model.calls(); sent != 1 {
			t.Fatalf("sent %d requests, want the lost one only", sent)
		}
	})
	t.Run("during inspection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f := newFixture(t, limits, harness.Budget{}, execReply("sleep 1"), returnReply("done"))
		f.exec.hold, f.exec.tick = true, cancel
		wantState(t, f.steps(f.loop(), 5), core.PhaseAwaitingObservation, "")
		if _, err := f.loop().Step(ctx, runID); !errors.Is(err, context.Canceled) {
			t.Fatalf("Step() error = %v, want cancellation", err)
		}
		f.wantKinds(running)
		f.exec.hold = false
		wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
		f.wantKinds(cycle, closing)
	})
}

func TestLoopColdRestartAfterEveryStepMatchesAnUninterruptedRun(t *testing.T) {
	ctx := context.Background()
	whole := newFixture(t, limits, harness.Budget{}, twoSteps()...)
	whole.run(whole.loop())
	cold := newFixture(t, limits, harness.Budget{}, twoSteps()...)
	for i := 0; ; i++ {
		s, err := sqlite.Open(ctx, cold.path)
		if err != nil {
			t.Fatal(err)
		}
		snap, err := cold.loopOn(s).Step(ctx, runID)
		s.Close()
		if err != nil {
			t.Fatalf("step %d error = %v", i, err)
		}
		if snap.State.Intent() == core.IntentHalt {
			break
		}
		if i > 50 {
			t.Fatal("the cold run does not halt")
		}
	}
	want, got := whole.transitions(), cold.transitions()
	if len(want) != 18 || len(got) != len(want) {
		t.Fatalf("committed %d cold and %d uninterrupted transitions, want 18", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("transition %d differs:\ncold  %+v\nwhole %+v", i+1, got[i], want[i])
		}
	}
	if !slices.Equal(cold.exec.log(), whole.exec.log()) {
		t.Fatalf("executor calls differ: %v vs %v", cold.exec.log(), whole.exec.log())
	}
}
