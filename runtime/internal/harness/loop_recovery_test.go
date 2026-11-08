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

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

var running = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded"}

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
	t.Run("during a model request", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{WallTime: time.Minute}, reply{block: true})
		f.clock.Advance(time.Minute - 20*time.Millisecond)
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopTimedOut)
		f.wantKinds([]string{"request_frozen", "attempt_started", "stop_requested"})
		wantContains(t, f.last().Facts.Failure, "wall-time budget")
	})
	t.Run("during a running job", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{WallTime: time.Minute}, execReply("sleep 600"))
		f.exec.hold, f.exec.tick = true, func() { f.clock.Advance(40 * time.Second) }
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopTimedOut)
		f.wantKinds(running, []string{"stop_requested", "observed"})
		if got := f.exec.log(); !slices.Equal(got, []string{"drive", "drive", "cancel"}) {
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
	if got := f.exec.log(); !slices.Equal(got, []string{"drive", "drive", "cancel"}) {
		t.Fatalf("executor calls = %v", got)
	}
	if obs := observation(t, f.transitions()[4]); obs.Execution != core.ExecutionTimedOut {
		t.Fatalf("observation = %+v, want timed_out", obs)
	}
	wantContains(t, f.model.prompts[1].User(), "Execution: timed_out")
}

func TestLoopCancelsATimedOutJobOnce(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{JobTimeout: 10 * time.Second}, execReply("sleep 600"), returnReply("gave up"))
	inspections := 0
	f.exec.inspect = func(id string) (harness.Job, bool, error) {
		inspections++
		f.clock.Advance(6 * time.Second)
		job := harness.Job{ID: jobID(id), Status: harness.JobRunning, StartedAt: created}
		if inspections == 4 {
			job.Status, job.FinishedAt = harness.JobCancelled, created.Add(24*time.Second)
		}
		return job, true, nil
	}
	wantState(t, f.run(f.loop()), core.PhaseReturned, "")
	if n := count(f.exec.log(), "cancel"); n != 1 || inspections != 4 {
		t.Fatalf("cancelled %d times over %d inspections, want once over 4", n, inspections)
	}
	if obs := observation(t, f.transitions()[4]); obs.Execution != core.ExecutionTimedOut {
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
			snap := f.steps(f.loop(), 3)
			if _, err := f.exec.Drive(context.Background(), harness.Effect{ID: snap.State.EffectID, Command: "sleep 600", Spec: f.exec.Specification("sleep 600")}); err != nil {
				t.Fatal(err)
			}
			wantState(t, snap, core.PhaseEffectSaved, "")
			f.exec.set(snap.State.EffectID, func(j *harness.Job) {
				j.Status, j.StartedAt, j.FinishedAt = harness.JobCancelled, created, created.Add(tt.ran)
				j.Fenced = true
				code := 143
				j.ExitCode = &code
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
		{"missing job", func(string) (harness.Job, bool, error) { return harness.Job{}, false, nil }, "absent", 1},
		{"foreign job", func(string) (harness.Job, bool, error) {
			return harness.Job{EffectID: "foreign", ID: "job-other", Status: harness.JobRunning}, true, nil
		}, "accepted effect", 1},
		{"unknown status", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobUnknown}, true, nil
		}, "unknown", 1},
		{"unknown status with a reason", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobUnknown, Output: harness.ArtifactRef{Detail: "exit status not recorded"}}, true, nil
		}, "reports unknown: exit status not recorded", 1},
		{"pending with exit evidence", func(id string) (harness.Job, bool, error) {
			code := 0
			return harness.Job{ID: jobID(id), Status: harness.JobPending, ExitCode: &code}, true, nil
		}, "pending", 1},
		{"exit without code", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobFailed}, true, nil
		}, "failed", 1},
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
			if n := count(f.exec.log(), "drive"); n != tt.inspects {
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
	if obs := observation(t, f.transitions()[4]); inspections != 4 || obs.Execution != core.ExecutionExited || *obs.ExitCode != 1 {
		t.Fatalf("observation after %d inspections = %+v, want the failed exit", inspections, obs)
	}
}

func TestLoopObservesAFailedStart(t *testing.T) {
	failed := func(id string) (harness.Job, bool, error) {
		out := harness.ArtifactRef{Path: jobID(id) + ".log", Complete: true, Available: true}
		return harness.Job{ID: jobID(id), Status: harness.JobFailedToStart, Preview: "never ran", Output: out}, true, nil
	}
	want := func(id string) harness.Observation {
		job, _, _ := failed(id)
		return harness.Observation{Execution: core.ExecutionFailedToStart, Preview: job.Preview, Output: job.Output}
	}
	t.Run("awaiting observation", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("make test"), returnReply("gave up"))
		f.exec.inspect = failed
		wantState(t, f.run(f.loop()), core.PhaseReturned, "")
		f.wantKinds(cycle, closing)
		observed := f.transitions()[4]
		if obs := observation(t, observed); !reflect.DeepEqual(obs, want(observed.From.EffectID)) {
			t.Fatalf("observation = %+v, want %+v", obs, want(observed.From.EffectID))
		}
		wantContains(t, f.model.prompts[1].User(), "Execution: failed_to_start")
	})
	t.Run("cancelling", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("make test"))
		f.exec.hold = true
		wantState(t, f.runningEffect(), core.PhaseEffectSaved, "")
		f.exec.inspect = failed
		snap, err := f.fresh().Stop(f.bounded(), runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		f.wantKinds(running, []string{"stop_requested", "observed"})
		if obs := observation(t, f.last()); !reflect.DeepEqual(obs, want(snap.State.EffectID)) {
			t.Fatalf("observation = %+v, want %+v", obs, want(snap.State.EffectID))
		}
	})
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
	wantState(t, snap, core.PhaseIdle, "")
	wantState(t, f.run(l), core.PhaseReturned, "")
	f.wantKinds(running, []string{"outcome_unknown", "resolved"}, closing)
	if n := f.exec.jobs[f.transitions()[3].To.EffectID].runs; n != 1 {
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

func TestLoopResolveJobRefusesAJobOfUnknownOutcome(t *testing.T) {
	unknown := func(id string) (harness.Job, bool, error) {
		return harness.Job{ID: jobID(id), Status: harness.JobUnknown, Output: harness.ArtifactRef{Detail: "no exit record"}}, true, nil
	}
	f := unresolved(t, unknown, returnReply("done"))
	before, calls := f.transitions(), f.exec.log()
	_, err := f.fresh().ResolveJob(context.Background(), runID)
	if got := f.transitions(); !reflect.DeepEqual(got, before) {
		t.Fatalf("ResolveJob() = %v and committed %+v", err, got[len(before):])
	}
	if !errors.Is(err, harness.ErrJobUnknown) || !strings.Contains(err.Error(), "no exit record") {
		t.Fatalf("ResolveJob() error = %v, want %v with the detail", err, harness.ErrJobUnknown)
	}
	if got := f.exec.log(); !slices.Equal(got, append(calls, "inspect")) {
		t.Fatalf("executor calls = %v, want one more inspection", got[len(calls):])
	}
	snap, err := f.fresh().Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionCancelled, Preview: "checked cancelled by operator"}, "operator-owner")
	if err != nil {
		t.Fatalf("Resolve() after the refusal: %v", err)
	}
	wantState(t, snap, core.PhaseIdle, "")
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
}

func TestLoopResolveRecordsAnExplicitOutcome(t *testing.T) {
	missing := func(string) (harness.Job, bool, error) { return harness.Job{}, false, nil }
	t.Run("exited continues", func(t *testing.T) {
		f := unresolved(t, missing, returnReply("done"))
		code := 0
		snap, err := f.fresh().Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionExited, ExitCode: &code, Preview: "checked by hand"}, "operator-owner")
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		wantState(t, snap, core.PhaseIdle, "")
		if snap.State.Step != 2 {
			t.Fatalf("resolved run is at step %d, want 2", snap.State.Step)
		}
		wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
		wantContains(t, f.model.prompts[1].User(), "set by explicit resolution", "checked by hand")
	})
	t.Run("unknown abandons", func(t *testing.T) {
		f := unresolved(t, missing)
		snap, err := f.fresh().Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionUnknown}, "operator-owner")
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopUnresolved)
	})
	t.Run("outside needs_resolution", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{})
		l := f.loop()
		if _, err := l.Resolve(context.Background(), runID, harness.Observation{Execution: core.ExecutionExited}, "operator-owner"); err == nil {
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
		wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
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
		if got := f.exec.log(); !slices.Equal(got, []string{"cancel"}) {
			t.Fatalf("executor calls = %v", got)
		}
	})
	t.Run("a job that exits after the cancel", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("make install"))
		wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
		inspections := 0
		f.exec.inspect = func(id string) (harness.Job, bool, error) {
			if inspections++; inspections == 1 {
				return harness.Job{ID: jobID(id), Status: harness.JobRunning}, true, nil
			}
			code := 0
			return harness.Job{ID: jobID(id), Status: harness.JobSucceeded, ExitCode: &code}, true, nil
		}
		snap, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		if obs := observation(t, f.last()); obs.Execution != core.ExecutionExited || inspections != 2 {
			t.Fatalf("observation after %d inspections = %+v, want the true exit", inspections, obs)
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
		if got := f.exec.log(); !slices.Equal(got, []string{"cancel"}) {
			t.Fatalf("executor calls = %v, want the fence then a lookup", got)
		}
		if obs := observation(t, f.last()); obs.Execution != core.ExecutionCancelled || f.last().Facts.Receipt.ID != "" || !f.last().Facts.Receipt.Fenced {
			t.Fatalf("observation = %+v, want a jobless cancellation", obs)
		}
	})
	t.Run("effect saved with an unrecorded bind", func(t *testing.T) {
		f := newFixture(t, limits, harness.Budget{}, execReply("sleep 600"))
		snap := f.steps(f.loop(), 3)
		f.exec.jobs[snap.State.EffectID] = &fakeJob{Job: harness.Job{EffectID: snap.State.EffectID, Spec: f.exec.Specification("sleep 600"), ID: jobID(snap.State.EffectID), Status: harness.JobPending}, command: "sleep 600"}
		snap, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		wantState(t, snap, core.PhaseStopped, core.StopCancelled)
		f.wantKinds(closing, []string{"stop_requested", "observed"})
		if got := f.exec.log(); !slices.Equal(got, []string{"cancel"}) || count(got, "start") != 0 {
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
		if snap, err = l.Resolve(ctx, runID, harness.Observation{Execution: core.ExecutionCancelled, Preview: "checked cancelled by operator"}, "operator-owner"); err != nil {
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
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("during a request answered late %v", late), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lost := returnReply("lost")
			lost.hook, lost.late = cancel, late
			f := newFixture(t, limits, harness.Budget{}, lost, returnReply("done"))
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
	}
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
		wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
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
	eventful := func() *fixture {
		f := newFixture(t, limits, harness.Budget{JobTimeout: 10 * time.Second},
			reply{body: `{"kind":`, usage: usage, err: errors.New("stream reset")}, reply{body: "Let me think.", usage: usage},
			execReply("sleep 600"), execReply("ls -la /workspace"), returnReply("done"))
		f.exec.hold, f.exec.tick = true, func() { f.clock.Advance(6 * time.Second) }
		return f
	}
	whole := eventful()
	wantState(t, whole.run(whole.loop()), core.PhaseReturned, "")
	cold := eventful()
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
	if len(want) != 20 || len(got) != len(want) {
		t.Fatalf("committed %d cold and %d uninterrupted transitions, want 20", len(got), len(want))
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

// acceptance replaces the acceptance its journal reports.
type acceptance struct {
	harness.Journal
	accepted harness.Accepted
	err      error
}

func (j acceptance) Accepted(context.Context, string, int, int) (harness.Accepted, error) {
	return j.accepted, j.err
}

func TestLoopActsOnlyOnARecordedAcceptance(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("printf ok"))
	ctx := f.bounded()
	snap := f.stepUntil(ctx, core.PhaseEffectSaved)
	j := acceptance{f.store, harness.Accepted{}, sqlite.ErrIncompatible}
	if _, err := f.loopOn(j).Step(ctx, runID); err == nil {
		t.Fatal("effect carried out without recorded acceptance")
	}
	if latest, err := f.store.Latest(ctx, runID); err != nil || latest != snap || len(f.exec.calls) != 0 {
		t.Fatalf("refusal changed facts or executor: %+v %v %v", latest, err, f.exec.calls)
	}
	stopped, err := f.loopOn(j).Stop(ctx, runID, core.StopCancelled)
	if err == nil || stopped.State.Phase != core.PhaseCancelling || len(f.exec.calls) != 0 {
		t.Fatalf("corrupt acceptance stop: %+v %v %v", stopped, err, f.exec.calls)
	}
}

// TestStopNeverDecodesASavedResponse pins that a stop of a run with a saved
// response stops at once, so that a loop built without the run's context
// settings, as stop builds it, never normalizes a decision.
func TestStopNeverDecodesASavedResponse(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve {}"))
	ctx := f.bounded()
	f.stepUntil(ctx, core.PhaseResponseSaved)
	s, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
	if err != nil || s.State.Phase != core.PhaseStopped || f.last().Event.Kind() != "stop_requested" || slices.Contains(f.kinds(), "response_loaded") {
		t.Fatalf("stop = %+v, %v after %v", s.State, err, f.kinds())
	}
}

func TestCancellationWithoutAnExitCodeRequiresMatchingCessation(t *testing.T) {
	e := harness.Effect{ID: "effect_environment", Spec: strings.Repeat("a", 64)}
	at := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	binding := harness.EnvironmentBinding{BackendID: "daemon", EnvironmentID: "container", StartEpoch: "epoch", Incarnation: 1, Generation: 2}
	witness := harness.Cessation{EnvironmentID: "container", StartEpoch: "epoch", Generation: 2, Kind: harness.CessationStopped, RequestedAt: at, ConfirmedAt: at.Add(time.Second)}
	good := harness.Job{EffectID: e.ID, Spec: e.Spec, ID: "exec", Status: harness.JobCancelled, Fenced: true, DispatchedAt: at, Binding: &binding, Cessation: &witness}
	if err := good.Validate(e); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*harness.Job){
		"missing witness":    func(j *harness.Job) { j.Cessation = nil },
		"wrong environment":  func(j *harness.Job) { j.Cessation.EnvironmentID = "other" },
		"wrong epoch":        func(j *harness.Job) { j.Cessation.StartEpoch = "new" },
		"stale generation":   func(j *harness.Job) { j.Cessation.Generation++ },
		"unconfirmed":        func(j *harness.Job) { j.Cessation.ConfirmedAt = time.Time{} },
		"invented exit time": func(j *harness.Job) { j.FinishedAt = at },
		"no fence":           func(j *harness.Job) { j.Fenced = false },
		"no binding":         func(j *harness.Job) { j.Binding = nil },
	} {
		t.Run(name, func(t *testing.T) {
			j := good
			w := witness
			j.Cessation = &w
			mutate(&j)
			if err := j.Validate(e); err == nil {
				t.Fatalf("accepted invalid receipt: %+v", j)
			}
		})
	}
	if !good.StartedAt.IsZero() || !good.FinishedAt.IsZero() {
		t.Fatal("cessation manufactured process timestamps")
	}
}

func TestRunningEnvironmentCommandKeepsDispatchSeparateFromProcessTime(t *testing.T) {
	e := harness.Effect{ID: "effect_environment", Spec: strings.Repeat("a", 64)}
	j := harness.Job{EffectID: e.ID, Spec: e.Spec, ID: "exec", Status: harness.JobRunning, DispatchedAt: created, Binding: &harness.EnvironmentBinding{BackendID: "daemon", EnvironmentID: "container", StartEpoch: "epoch", Incarnation: 1, Generation: 1}}
	if err := j.Validate(e); err != nil {
		t.Fatal(err)
	}
	if !j.StartedAt.IsZero() {
		t.Fatal("dispatch time became process start time")
	}
}
