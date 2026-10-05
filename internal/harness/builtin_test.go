package harness_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestRetrievalCommitsReferencesWithoutCopiedEvidence(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify prior result"}`), returnReply("done"))
	archive := t.TempDir()
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte("unique original evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &harness.Reader{Imports: archive}
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
	b, _ := json.Marshal(steps)
	if strings.Contains(string(b), "unique original evidence") {
		t.Fatal("copied evidence into observation")
	}
	if len(f.exec.calls) != 0 {
		t.Fatalf("retrieval called external executor: %v", f.exec.calls)
	}
	text, err := f.cfg.Reader.Expand(ctx, r.Sources[0])
	if err != nil || text != "unique original evidence" {
		t.Fatalf("expand %q %v", text, err)
	}
}

func TestRetrievalCancellationNeverUsesShellExecutor(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/missing","reason":"verify"}`))
	f.cfg.Reader = &harness.Reader{Imports: t.TempDir()}
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
			f.cfg.Reader = &harness.Reader{Imports: t.TempDir()}
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
	archive := t.TempDir()
	file := filepath.Join(archive, "evidence")
	if err := os.WriteFile(file, []byte("original immutable evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &harness.Reader{Imports: archive}
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
	if !strings.Contains(last, "sha256 mismatch") || strings.Contains(last, replacement) || len(f.exec.calls) != 0 {
		t.Fatalf("replacement source was silently used or executor was called: %v", f.exec.calls)
	}
}
