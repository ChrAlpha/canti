package harness_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const readEvidence = `canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify prior result"}`

// importing returns a reader of a fresh imports directory whose evidence
// file holds text, and the path of that file.
func importing(t *testing.T, text string) (*archive.Reader, string) {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "evidence")
	if err := os.WriteFile(file, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return &archive.Reader{Imports: dir}, file
}

// stepUntil steps fresh loops until the run reaches phase.
func (f *fixture) stepUntil(ctx context.Context, phase core.Phase) harness.Snapshot {
	f.t.Helper()
	for i := 0; ; i++ {
		snap, err := f.fresh().Step(ctx, runID)
		if err != nil {
			f.t.Fatal(err)
		}
		if snap.State.Phase == phase {
			return snap
		}
		if i == 32 || snap.State.Intent() == core.IntentHalt {
			f.t.Fatalf("the run reached %+v, not %s", snap.State, phase)
		}
	}
}

// TestRetrievalCommitsItsEvidenceWithItsOutcome requires that a retrieval is
// accepted with its action, performed in one transition that commits the
// bytes of each source with its outcome and no observation, keeps them out
// of the published preview, runs no executor, and shows those bytes in the
// next request even after the source changed.
func TestRetrievalCommitsItsEvidenceWithItsOutcome(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(readEvidence), returnReply("done"))
	reader, file := importing(t, "unique original evidence")
	f.cfg.Reader = reader
	ctx := context.Background()
	saved := f.stepUntil(ctx, core.PhaseEffectSaved)
	accepted, err := f.store.Accepted(ctx, runID, 1, 1)
	if err != nil || accepted.Semantics != core.SemanticsVersion ||
		accepted.Action != (harness.Action{Name: harness.ActionRetrieve, Recovery: harness.RecoveryRead, Args: len(harness.RetrievePrefix)}) {
		t.Fatalf("acceptance %+v, %v", accepted, err)
	}
	if snap := f.stepUntil(ctx, core.PhaseIdle); snap.Seq != saved.Seq+1 {
		t.Fatalf("the retrieval took %d transitions, want one", snap.Seq-saved.Seq)
	}
	f.wantKinds(cycle[:4], []string{"performed"})
	steps, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(steps) != 1 || steps[0].Outcome == nil || steps[0].Observation != (harness.Observation{}) {
		t.Fatalf("steps: %+v %v", steps, err)
	}
	o := steps[0].Outcome
	if o.Status != harness.OutcomeDone || o.Retrieval == nil || len(o.Retrieval.Sources) != 1 ||
		len(o.Retrieval.Evidence) != 1 || o.Retrieval.Evidence[0] != "unique original evidence" {
		t.Fatalf("outcome %+v", o)
	}
	if len(f.exec.calls) != 0 {
		t.Fatalf("retrieval called the executor: %v", f.exec.calls)
	}
	if err := os.WriteFile(file, []byte("replaced afterwards"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.fresh().Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	last := string(f.frozen(2, 1))
	if !strings.Contains(last, "unique original evidence") || strings.Contains(last, "replaced afterwards") {
		t.Fatalf("the next request does not show the evidence frozen with the outcome:\n%s", last)
	}
	preview, err := os.ReadFile(filepath.Join(f.cfg.History, runID, "steps", archive.StepDir(1), archive.PreviewFile))
	if err != nil || strings.Contains(string(preview), "unique original evidence") || !strings.Contains(string(preview), `"sha256"`) {
		t.Fatalf("published preview %q, %v", preview, err)
	}
}

// TestLocalActionStopNeverCallsTheExecutor requires that a stop of a
// retrieval or handover accepted but not yet performed, with or without a
// reader, cancels it in process: no executor call, and the effect observed
// cancelled without a job.
func TestLocalActionStopNeverCallsTheExecutor(t *testing.T) {
	for _, payload := range []string{readEvidence, harness.HandoverPrefix + "Next: report."} {
		for _, disabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reader disabled %v", payload[:14], disabled), func(t *testing.T) {
				f := newFixture(t, limits, harness.Budget{}, execReply(payload))
				f.cfg.Reader, _ = importing(t, "evidence")
				if disabled {
					f.cfg.Reader = nil
				}
				ctx := f.bounded()
				f.stepUntil(ctx, core.PhaseEffectSaved)
				s, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
				if err != nil || s.State.Phase != core.PhaseStopped || len(f.exec.calls) != 0 {
					t.Fatalf("state=%+v err=%v executor=%v", s.State, err, f.exec.calls)
				}
				last := f.last()
				if obs := last.Facts.Observation; last.Event.Kind() != "observed" || obs == nil || obs.Execution != core.ExecutionCancelled ||
					obs.ExitCode != nil || obs.Output.Available || last.Facts.Receipt != nil || last.Facts.Window != nil {
					t.Fatalf("cancellation %+v", last)
				}
			})
		}
	}
}

// failingCommit fails the commit of the first transition of kind, after the
// store recorded it when landed is set and before otherwise.
type failingCommit struct {
	harness.Journal
	kind   string
	landed bool
	failed *bool
}

func (j failingCommit) Commit(ctx context.Context, tr harness.Transition) error {
	if tr.Event.Kind() != j.kind || *j.failed {
		return j.Journal.Commit(ctx, tr)
	}
	*j.failed = true
	if j.landed {
		if err := j.Journal.Commit(ctx, tr); err != nil {
			return err
		}
	}
	return errors.New("injected: the acknowledgement of the commit is lost")
}

// TestRetrievalAcknowledgementLossNeverReadsAgain requires that when the
// performed commit of a retrieval lands but its acknowledgement is lost, the
// next step reloads and keeps the committed evidence, never reading the
// changed source, and that when it never landed, the read is performed again
// from the source as it is then.
func TestRetrievalAcknowledgementLossNeverReadsAgain(t *testing.T) {
	for _, landed := range []bool{true, false} {
		t.Run(map[bool]string{true: "landed", false: "lost"}[landed], func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, execReply(readEvidence), returnReply("done"))
			reader, file := importing(t, "original evidence")
			f.cfg.Reader = reader
			ctx := f.bounded()
			f.stepUntil(ctx, core.PhaseEffectSaved)
			var failed bool
			if _, err := f.loopOn(failingCommit{f.store, "performed", landed, &failed}).Step(ctx, runID); err == nil {
				t.Fatal("the lost acknowledgement was not surfaced")
			}
			if err := os.WriteFile(file, []byte("replaced evidence"), 0o600); err != nil {
				t.Fatal(err)
			}
			wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
			steps, err := f.store.Recent(ctx, runID, 0, 1)
			if err != nil || len(steps) != 1 || steps[0].Outcome == nil || steps[0].Outcome.Retrieval == nil {
				t.Fatalf("steps %+v %v", steps, err)
			}
			want := map[bool]string{true: "original evidence", false: "replaced evidence"}[landed]
			if got := steps[0].Outcome.Retrieval.Evidence; len(got) != 1 || got[0] != want {
				t.Fatalf("committed evidence %q, want %q", got, want)
			}
			if n := strings.Count(strings.Join(f.kinds(), " "), "performed"); n != 1 {
				t.Fatalf("%d performed transitions", n)
			}
		})
	}
}

// TestAcceptanceAcknowledgementLossRecordsOneAction requires that a lost
// acknowledgement of an accepting ResponseLoaded is resolved by reloading,
// and that a retry of the identical transition is confirmed without a second
// record.
func TestAcceptanceAcknowledgementLossRecordsOneAction(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("printf ok"), returnReply("done"))
	ctx := f.bounded()
	f.stepUntil(ctx, core.PhaseResponseSaved)
	var failed bool
	if _, err := f.loopOn(failingCommit{f.store, "response_loaded", true, &failed}).Step(ctx, runID); err == nil {
		t.Fatal("the lost acknowledgement was not surfaced")
	}
	accepting := f.last()
	if err := f.store.Commit(ctx, accepting); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if a := accepting.Facts.Action; a == nil || *a != (harness.Action{Name: harness.ActionCommand, Recovery: harness.RecoveryExternal}) {
		t.Fatalf("action %+v", a)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	f.wantKinds(cycle, closing)
}

// racingStop commits a stop through another runner just before the first
// performed commit, as a concurrent stop process would.
type racingStop struct {
	harness.Journal
	t    *testing.T
	exec harness.Executor
	done *bool
}

func (j racingStop) Commit(ctx context.Context, tr harness.Transition) error {
	if _, ok := tr.Event.(core.Performed); ok && !*j.done {
		*j.done = true
		r, err := harness.NewRunner(j.Journal)
		if err != nil {
			j.t.Fatal(err)
		}
		snap, err := j.Journal.Latest(ctx, tr.From.RunID)
		if err == nil {
			_, err = r.Commit(ctx, snap, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
		}
		if err != nil {
			j.t.Fatal(err)
		}
	}
	return j.Journal.Commit(ctx, tr)
}

// TestStopRacingAPerformanceWins requires that a stop that lands before a
// handover's commit makes that commit stale, opens no window, and cancels
// the handover in process.
func TestStopRacingAPerformanceWins(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("printf ok"), execReply(harness.HandoverPrefix+"Next: report."))
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	f.stepUntil(ctx, core.PhaseIdle)
	f.stepUntil(ctx, core.PhaseEffectSaved)
	calls := len(f.exec.log())
	var raced bool
	if _, err := f.loopOn(racingStop{f.store, t, f.exec, &raced}).Step(ctx, runID); !errors.Is(err, harness.ErrStaleSnapshot) {
		t.Fatalf("the performance after the stop: %v", err)
	}
	wantState(t, f.run(f.fresh()), core.PhaseStopped, core.StopCancelled)
	for _, tr := range f.transitions() {
		if tr.Facts.Window != nil || tr.Event.Kind() == "performed" {
			t.Fatalf("a window or performance survived the stop: %+v", tr)
		}
	}
	if got := f.exec.log(); len(got) != calls {
		t.Fatalf("the handover reached the executor: %v", got[calls:])
	}
}

// TestALocalActionCannotCompleteWithAnExecutorReceipt requires that the store refuses to bind an
// executor job to the effect of a recorded local action, writing nothing,
// and that the loop then still performs it in process.
func TestALocalActionCannotCompleteWithAnExecutorReceipt(t *testing.T) {
	for _, payload := range []string{`canti:retrieve {}`, `canti:handover {}`, `canti:unknown`} {
		t.Run(payload, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, execReply(payload), returnReply("done"))
			f.cfg.Reader, _ = importing(t, "evidence")
			ctx := context.Background()
			snap := f.stepUntil(ctx, core.PhaseEffectSaved)
			r, err := harness.NewRunner(f.store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: core.ExecutionFailedToStart}, harness.Facts{Observation: &harness.Observation{Execution: core.ExecutionFailedToStart}, Receipt: &harness.Job{EffectID: snap.State.EffectID, Spec: f.exec.Specification(payload), ID: "job-1", Status: harness.JobFailedToStart}}); !errors.Is(err, harness.ErrFacts) {
				t.Fatalf("binding a job to a local action: %v", err)
			}
			if latest, err := f.store.Latest(ctx, runID); err != nil || latest != snap {
				t.Fatalf("the refused bind wrote %+v, %v", latest, err)
			}
			wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
			if len(f.exec.calls) != 0 {
				t.Fatalf("executor calls %v", f.exec.calls)
			}
		})
	}
}

// TestAnExternalEffectCannotCompleteAsLocal requires that the store refuses
// a performed completion, a jobless failed start, or an observation that
// carries a retrieval for the effect of a recorded command.
func TestAnExternalEffectCannotCompleteAsLocal(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("printf ok"))
	ctx := context.Background()
	snap := f.stepUntil(ctx, core.PhaseEffectSaved)
	r, err := harness.NewRunner(f.store)
	if err != nil {
		t.Fatal(err)
	}
	effect := snap.State.EffectID
	for name, c := range map[string]struct {
		event core.Event
		facts harness.Facts
	}{
		"performed":         {core.Performed{EffectID: effect}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeFailed, Detail: "x"}}},
		"jobless failure":   {core.Observed{EffectID: effect, Execution: core.ExecutionFailedToStart}, harness.Facts{Observation: &harness.Observation{Execution: core.ExecutionFailedToStart}}},
		"local observation": {core.Observed{EffectID: effect, Execution: core.ExecutionFailedToStart}, harness.Facts{Observation: &harness.Observation{Execution: core.ExecutionFailedToStart, Retrieval: &archive.Retrieval{}}}},
	} {
		if _, err := r.Commit(ctx, snap, c.event, c.facts); !errors.Is(err, harness.ErrFacts) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if latest, err := f.store.Latest(ctx, runID); err != nil || latest != snap {
		t.Fatalf("a refused completion wrote %+v, %v", latest, err)
	}
}

// TestARetrievalAcceptedWithAReaderFailsWithoutOne requires that a loop
// rebuilt without a reader after a retrieval was accepted with one fails
// the read as disabled, as version 12 did, instead of reading or panicking.
func TestARetrievalAcceptedWithAReaderFailsWithoutOne(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(readEvidence), returnReply("done"))
	f.cfg.Reader, _ = importing(t, "evidence")
	ctx := f.bounded()
	f.stepUntil(ctx, core.PhaseEffectSaved)
	f.cfg.Reader = nil
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	steps, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(steps) != 1 || steps[0].Outcome == nil || steps[0].Outcome.Status != harness.OutcomeFailed ||
		steps[0].Outcome.Detail != "retrieval is disabled for this run" || len(f.exec.calls) != 0 {
		t.Fatalf("steps %+v, %v, executor %v", steps, err, f.exec.calls)
	}
}
