package harness_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestRetrievalSearchContinuationSurvivesPromptPreviewLimit(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"search","path":"/canti/imports/evidence","reason":"find earlier facts","query":"x","limit":8192}`),
		returnReply("done"))
	archive := t.TempDir()
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(strings.Repeat("x", 10000)), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &harness.Reader{Imports: archive}
	if _, err := f.loop().Run(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if len(f.model.sent) != 2 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	request := string(f.model.sent[1])
	if !strings.Contains(request, `"next_offset":8192`) || !strings.Contains(request, `"more":true`) {
		t.Fatal("search continuation metadata was truncated before reaching the model")
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

func TestRevisitingRetrievalAfterHandoverDoesNotExpandItsEvidence(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify original"}`),
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Inspect step 1 access metadata."}`),
		execReply(`canti:retrieve {"operation":"read","path":"/canti/runs/run-1/steps/00000001/preview","reason":"inspect prior access"}`),
		execReply(`canti:handover {"run_id":"run-1","first_step":3,"text":"Finish."}`),
		returnReply("done"))
	archive := t.TempDir()
	const original = "UNIQUE_ORIGINAL_EVIDENCE_NOT_METADATA"
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &harness.Reader{Imports: archive, History: f.cfg.History}
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			break
		}
	}
	if len(f.model.sent) != 5 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	if !strings.Contains(string(f.model.sent[1]), original) {
		t.Fatal("initial retrieval evidence missing")
	}
	for _, request := range f.model.sent[2:] {
		if strings.Contains(string(request), original) {
			t.Fatal("original evidence recursively reappeared after handover")
		}
	}
	if !strings.Contains(string(f.model.sent[3]), "verify original") || !strings.Contains(string(f.model.sent[3]), "/canti/imports/evidence") {
		t.Fatal("revisited access did not expose original reason and source")
	}
}

func TestHandoverProtocolRepairPreservesFailureCountAndWindow(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply("true"),
		execReply(`canti:handover {"run_id":"wrong","first_step":1,"text":"invalid"}`),
		reply{body: "not a decision", usage: usage},
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue from the verified source."}`),
		returnReply("done"))
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			if s.State.Phase != core.PhaseReturned {
				t.Fatalf("unexpected halt: %+v", s.State)
			}
			break
		}
	}
	for _, revision := range []int{1, 2} {
		req, err := f.store.Request(ctx, runID, 3, revision)
		if err != nil || req.Window == nil || req.Window.FirstStep != 1 || req.Window.Failures != 1 || !req.Window.Requested {
			t.Fatalf("revision %d window=%+v err=%v", revision, req.Window, err)
		}
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != 4 || req.Window.SourceStep != 3 || req.Window.Failures != 0 {
		t.Fatalf("new window=%+v err=%v", req.Window, err)
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
