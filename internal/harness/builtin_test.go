package harness_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
)

// TestRetrievalCommitsItsEvidenceWithTheObservation requires that a
// retrieval commits the bytes of each source it returns with its
// observation, keeps them out of the published preview, runs no executor,
// and shows those bytes in the next request even after the source changed.
func TestRetrievalCommitsItsEvidenceWithTheObservation(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify prior result"}`), returnReply("done"))
	imported := t.TempDir()
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("unique original evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: imported}
	ctx := context.Background()
	for {
		snap, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Step == 2 {
			break
		}
	}
	steps, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps: %v %v", steps, err)
	}
	r := steps[0].Observation.Retrieval
	if r == nil || len(r.Sources) != 1 {
		t.Fatalf("retrieval: %#v", r)
	}
	if len(r.Evidence) != 1 || r.Evidence[0] != "unique original evidence" || strings.Contains(steps[0].Observation.Preview, "unique original evidence") {
		t.Fatalf("committed evidence %q, preview %s", r.Evidence, steps[0].Observation.Preview)
	}
	if len(f.exec.calls) != 0 {
		t.Fatalf("retrieval called external executor: %v", f.exec.calls)
	}
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte("replaced afterwards"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.fresh().Step(ctx, runID); err != nil {
		t.Fatal(err)
	}
	last := string(f.frozen(2, 1))
	if !strings.Contains(last, "unique original evidence") || strings.Contains(last, "replaced afterwards") {
		t.Fatalf("the next request does not show the evidence frozen at observation:\n%s", last)
	}
}

func TestRetrievalCancellationNeverUsesShellExecutor(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/missing","reason":"verify"}`))
	f.cfg.Reader = &archive.Reader{Imports: t.TempDir()}
	ctx := context.Background()
	for {
		s, err := f.loop().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Phase == core.PhaseEffectSaved {
			break
		}
	}
	s, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
	if err != nil {
		t.Fatal(err)
	}
	if s.State.Phase != core.PhaseStopped || len(f.exec.calls) != 0 {
		t.Fatalf("state %#v executor %v", s.State, f.exec.calls)
	}
}

func TestBuiltinCancellationAfterBindAndStartWithReaderDisabled(t *testing.T) {
	for _, phase := range []core.Phase{core.PhaseEffectSaved, core.PhaseJobBound, core.PhaseAwaitingObservation} {
		t.Run(string(phase), func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify"}`))
			f.cfg.Reader = &archive.Reader{Imports: t.TempDir()}
			ctx := f.bounded()
			for {
				s, err := f.fresh().Step(ctx, runID)
				if err != nil {
					t.Fatal(err)
				}
				if s.State.Phase == phase {
					break
				}
			}
			f.cfg.Reader = nil
			s, err := f.fresh().Stop(ctx, runID, core.StopCancelled)
			if err != nil || s.State.Phase != core.PhaseStopped || len(f.exec.calls) != 0 {
				t.Fatalf("state=%+v err=%v executor=%v", s.State, err, f.exec.calls)
			}
		})
	}
}

type observationLostAck struct {
	harness.Journal
}

func (j observationLostAck) Commit(ctx context.Context, tr harness.Transition) error {
	if err := j.Journal.Commit(ctx, tr); err != nil {
		return err
	}
	if _, ok := tr.Event.(core.Observed); ok {
		return errors.New("observation committed but acknowledgement lost")
	}
	return nil
}

func TestRetrievalLostObservationAckKeepsPinnedReference(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify"}`), returnReply("done"))
	imported := t.TempDir()
	file := filepath.Join(imported, "evidence")
	if err := os.WriteFile(file, []byte("original immutable evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &archive.Reader{Imports: imported}
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Phase == core.PhaseAwaitingObservation {
			break
		}
	}
	if _, err := f.loopOn(observationLostAck{f.store}).Step(ctx, runID); err == nil {
		t.Fatal("lost acknowledgement was not surfaced")
	}
	before, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(before) != 1 || before[0].Observation.Retrieval == nil {
		t.Fatalf("committed observation: %+v %v", before, err)
	}
	source := before[0].Observation.Retrieval.Sources[0]
	replacement := strings.Repeat("z", len("original immutable evidence"))
	if err := os.WriteFile(file, []byte(replacement), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.fresh().Run(ctx, runID); err != nil {
		t.Fatal(err)
	}
	after, err := f.store.Recent(ctx, runID, 0, 1)
	if err != nil || len(after) != 1 || after[0].Observation.Retrieval.Sources[0] != source {
		t.Fatalf("pinned reference changed: %+v %v", after, err)
	}
	last := string(f.model.sent[len(f.model.sent)-1])
	if !strings.Contains(last, "original immutable evidence") || strings.Contains(last, replacement) || len(f.exec.calls) != 0 {
		t.Fatalf("the frozen evidence was not shown, the replacement was, or the executor was called: %v", f.exec.calls)
	}
}

func TestAJobBoundByTheExecutorStaysWithIt(t *testing.T) {
	for _, payload := range []string{`canti:retrieve {}`, `canti:handover {}`} {
		t.Run(payload, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{}, execReply(payload), returnReply("done"))
			f.cfg.Reader = &archive.Reader{Imports: t.TempDir()}
			ctx := context.Background()
			snap := f.steps(f.loop(), 1)
			for snap.State.Phase != core.PhaseEffectSaved {
				snap = f.steps(f.loop(), 1)
			}
			job, err := f.exec.Bind(ctx, harness.Effect{ID: snap.State.EffectID, Command: payload})
			if err != nil {
				t.Fatal(err)
			}
			r, err := harness.NewRunner(f.store, f.exec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Commit(ctx, snap, core.JobBound{EffectID: snap.State.EffectID, JobID: job.ID}, harness.Facts{}); err != nil {
				t.Fatal(err)
			}
			wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
			steps, err := f.store.Recent(ctx, runID, 0, 1)
			if err != nil || len(steps) != 1 {
				t.Fatalf("steps=%v err=%v", steps, err)
			}
			if obs := steps[0].Observation; obs.Retrieval != nil || obs.Preview != "ran: "+payload {
				t.Fatalf("the bound job was not observed through its executor: %+v", obs)
			}
			if calls := f.exec.log(); !slices.Contains(calls, "start") || !slices.Contains(calls, "inspect") {
				t.Fatalf("executor calls %v", calls)
			}
		})
	}
}
