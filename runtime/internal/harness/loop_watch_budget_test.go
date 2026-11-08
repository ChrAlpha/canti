package harness_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestLoopWatchPersistsWallTimeStopAfterInspectionFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		retries int
		delay   time.Duration
		calls   int
	}{{"first failure", 5, 2 * time.Second, 1}, {"final failure", 1, 600 * time.Millisecond, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{WallTime: time.Second}, execReply("sleep 600"))
			f.cfg.InspectRetries = tc.retries
			wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
			f.exec.inspect = func(string) (harness.Job, bool, error) {
				f.clock.Advance(tc.delay)
				return harness.Job{}, false, errors.New("daemon unavailable")
			}

			snap, err := f.loop().Step(f.bounded(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if snap.State.Phase != core.PhaseCancelling || snap.State.Reason != core.StopTimedOut {
				t.Fatalf("after %d failed inspections at %v: state=%+v; want durable timed_out cancellation intent",
					count(f.exec.log(), "drive"), f.clock.Now().Sub(created), snap.State)
			}
			if count(f.exec.log(), "cancel") != 0 {
				t.Fatal("cancellation was dispatched before its intent was committed")
			}
			if calls := count(f.exec.log(), "drive"); calls != tc.calls {
				t.Fatalf("inspected %d times, want %d before recording the timeout", calls, tc.calls)
			}
			f.wantKinds(running, []string{"stop_requested"})
			wantContains(t, f.last().Facts.Failure, "wall-time budget")
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.store = f.open()
			wantState(t, f.run(f.loop()), core.PhaseNeedsResolution, core.StopTimedOut)
			f.wantKinds(running, []string{"stop_requested", "outcome_unknown"})
			if count(f.exec.log(), "cancel") != f.cfg.InspectRetries+1 || count(f.exec.log(), "drive") < 1 {
				t.Fatalf("executor calls after reopen = %v", f.exec.log())
			}
		})
	}
}

func TestLoopWatchPreservesTerminalResultReturnedAfterWallTime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status harness.JobStatus
		code   int
	}{{"succeeded", harness.JobSucceeded, 0}, {"failed", harness.JobFailed, 7}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{WallTime: time.Second}, execReply("make check"))
			wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
			output := harness.ArtifactRef{Path: "result.log", Bytes: 12, Complete: true, Available: true}
			f.exec.inspect = func(id string) (harness.Job, bool, error) {
				f.clock.Advance(2 * time.Second)
				return harness.Job{ID: jobID(id), Status: tc.status, ExitCode: &tc.code,
					StartedAt: created, FinishedAt: created.Add(500 * time.Millisecond),
					Preview: "real outcome", Output: output}, true, nil
			}

			snap, err := f.loop().Step(f.bounded(), runID)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, snap, core.PhaseIdle, "")
			obs := observation(t, f.last())
			if obs.Execution != core.ExecutionExited || obs.ExitCode == nil || *obs.ExitCode != tc.code ||
				obs.Preview != "real outcome" || obs.Output != output {
				t.Fatalf("late terminal observation = %+v, want the confirmed execution result", obs)
			}
			wantState(t, f.run(f.fresh()), core.PhaseStopped, core.StopTimedOut)
			f.wantKinds(cycle, []string{"stop_requested"})
			if count(f.exec.log(), "cancel") != 0 || count(f.exec.log(), "drive") != 1 {
				t.Fatalf("confirmed terminal job was cancelled or inspected again: %v", f.exec.log())
			}
			if _, sent := f.model.calls(); sent != 1 {
				t.Fatalf("sent %d requests, want no new request after the wall-time budget", sent)
			}
		})
	}
}

func TestLoopWatchIsolatesUnknownResultReturnedAfterWallTime(t *testing.T) {
	for _, tc := range []struct {
		name    string
		inspect func(string) (harness.Job, bool, error)
		failure string
	}{
		{"foreign job", func(string) (harness.Job, bool, error) {
			code := 0
			return harness.Job{EffectID: "foreign", ID: "foreign", Status: harness.JobSucceeded, ExitCode: &code}, true, nil
		}, "accepted effect"},
		{"lost job", func(string) (harness.Job, bool, error) {
			return harness.Job{}, false, errors.Join(harness.ErrJobLost, errors.New("container removed"))
		}, "container removed"},
		{"missing job", func(string) (harness.Job, bool, error) {
			return harness.Job{}, false, nil
		}, "reports absent"},
		{"unknown status", func(id string) (harness.Job, bool, error) {
			return harness.Job{ID: jobID(id), Status: harness.JobUnknown}, true, nil
		}, "reports unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{WallTime: time.Second}, execReply("make check"))
			wantState(t, f.steps(f.loop(), 3), core.PhaseEffectSaved, "")
			f.exec.inspect = func(id string) (harness.Job, bool, error) {
				f.clock.Advance(2 * time.Second)
				return tc.inspect(id)
			}

			snap, err := f.loop().Step(f.bounded(), runID)
			if err != nil {
				t.Fatal(err)
			}
			wantState(t, snap, core.PhaseNeedsResolution, "")
			f.wantKinds(running, []string{"outcome_unknown"})
			wantContains(t, f.last().Facts.Failure, tc.failure)
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.store = f.open()
			if got := f.run(f.loop()); got != snap {
				t.Fatalf("unknown outcome changed after reopen: got=%+v want=%+v", got, snap)
			}
			if calls := f.exec.log(); count(calls, "drive") != 1 || count(calls, "cancel") != 0 {
				t.Fatalf("unknown job was inspected again, cancelled, or restarted: %v", calls)
			}
			if _, sent := f.model.calls(); sent != 1 {
				t.Fatalf("sent %d requests, want no new request for an unknown outcome", sent)
			}
		})
	}
}
