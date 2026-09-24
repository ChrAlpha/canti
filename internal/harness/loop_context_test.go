package harness_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestWindowHandoverRecoversAndDoesNotCarryOldEvidence(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"check source"}`),
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue at the evidence reference; verify before use."}`),
		reply{err: errors.New("transport interrupted")}, returnReply("done"))
	archive := t.TempDir()
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte("ORIGINAL_EVIDENCE_SENTINEL"), 0600); err != nil {
		t.Fatal(err)
	}
	f.cfg.Reader = &harness.Reader{Imports: archive}
	f.cfg.WindowSteps = 1
	ctx := context.Background()
	for {
		snap, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Intent() == core.IntentHalt {
			break
		}
	}
	if len(f.model.prompts) != 3 || len(f.model.sent) != 4 {
		t.Fatalf("prompts=%d sends=%d", len(f.model.prompts), len(f.model.sent))
	}
	if !strings.Contains(f.model.prompts[1].User(), "ORIGINAL_EVIDENCE_SENTINEL") {
		t.Fatal("retrieved evidence did not enter actual request")
	}
	last := f.model.prompts[2].User()
	if strings.Contains(last, "ORIGINAL_EVIDENCE_SENTINEL") || strings.Contains(last, "### Step 1") || strings.Contains(last, "## Notes") {
		t.Fatalf("old context leaked: %s", last)
	}
	if !strings.Contains(last, "Continue at the evidence reference") {
		t.Fatal("handover missing")
	}
	if !bytes.Equal(f.model.sent[2], f.model.sent[3]) {
		t.Fatal("retry changed frozen window bytes")
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != 3 || req.Window.SourceStep != 2 {
		t.Fatalf("window: %#v %v", req.Window, err)
	}
	if _, err := os.Stat(filepath.Join(f.cfg.History, runID, "steps", "00000002", "preview")); err != nil {
		t.Fatal("last old-page step not published", err)
	}
}

func TestWindowRefusesRepeatedInvalidHandover(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{}, execReply("true"),
		execReply(`canti:handover {"run_id":"another-run","first_step":1,"text":"wrong"}`),
		execReply(`canti:handover {"run_id":"run-1","first_step":0,"text":"stale"}`))
	f.cfg.WindowSteps = 1
	s, err := f.loop().Run(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if s.State.Reason != core.StopUnrecoverable {
		t.Fatalf("state %#v", s.State)
	}
	req, err := f.store.LatestRequest(context.Background(), runID)
	if err != nil || req.Window.FirstStep != 1 {
		t.Fatalf("discarded old page: %#v %v", req.Window, err)
	}
}

func TestWindowWithoutHandoverAblation(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("echo first-page-marker"), returnReply("done"))
	f.cfg.WindowSteps = 1
	f.cfg.WithoutHandover = true
	ctx := context.Background()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			break
		}
	}
	p := f.model.prompts[1].User()
	if strings.Contains(p, "first-page-marker") || strings.Contains(p, "Handover required") {
		t.Fatalf("ablation retained old context: %s", p)
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window.FirstStep != 2 || !req.Window.WithoutHandover || req.Window.Handover != "" {
		t.Fatalf("window %#v %v", req.Window, err)
	}
}
