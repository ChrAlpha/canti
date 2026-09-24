package harness_test

import (
	"context"
	"encoding/json"
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
