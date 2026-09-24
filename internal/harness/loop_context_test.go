package harness_test

import (
	"bytes"
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

func TestZeroTriggersRequestAndAdvanceHandoverAtCapacity(t *testing.T) {
	var replies []reply
	for i := 0; i < 24; i++ {
		replies = append(replies, execReply("echo CAPACITY_PAGE_MARKER "+strings.Repeat("x", 1800)))
	}
	f := newFixture(t, core.Limits{MaxSteps: 30, MaxAttempts: 3}, harness.Budget{}, replies...)
	if f.cfg.WindowSteps != 0 || f.cfg.WindowBytes != 0 {
		t.Fatalf("default triggers = %d steps, %d bytes", f.cfg.WindowSteps, f.cfg.WindowBytes)
	}
	ctx := f.bounded()
	var handoverStep int
	for i := 0; i < 250; i++ {
		snap, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase != core.PhaseRequestFrozen {
			continue
		}
		req, err := f.store.Request(ctx, runID, snap.State.Step, snap.State.RequestRev)
		if err != nil || req.Window == nil {
			t.Fatalf("frozen request has no window: %+v %v", req, err)
		}
		if req.Window.Requested {
			handoverStep = snap.State.Step
			if !strings.Contains(string(req.Body), "Handover required") || !strings.Contains(string(req.Body), "### Step 1") {
				t.Fatal("capacity trigger dropped current-page history before handover")
			}
			break
		}
	}
	if handoverStep < 2 {
		t.Fatal("automatic capacity trigger did not request a handover")
	}
	f.model.mu.Lock()
	f.model.replies = []reply{
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue on the next page; inspect the old record if needed."}`),
		returnReply("done"),
	}
	f.model.mu.Unlock()
	snap, err := f.fresh().Run(ctx, runID)
	if err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("run = %+v, %v", snap.State, err)
	}
	req, err := f.store.LatestRequest(ctx, runID)
	if err != nil || req.Window == nil || req.Window.FirstStep != handoverStep+1 || req.Window.SourceStep != handoverStep {
		t.Fatalf("advanced window = %+v, %v", req.Window, err)
	}
	if strings.Contains(string(req.Body), "CAPACITY_PAGE_MARKER") || !strings.Contains(string(req.Body), "Continue on the next page") {
		t.Fatal("new page inherited old steps or lost handover")
	}
}

func TestDirectorySearchCursorAndEvidenceReachFrozenRequest(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply(`canti:retrieve {"operation":"search","path":"/canti/imports","query":"needle","limit":6,"reason":"locate original evidence"}`), returnReply("done"))
	root := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("needle"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.cfg.Reader = &harness.Reader{Imports: root}
	if _, err := f.loop().Run(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	steps, err := f.store.Recent(context.Background(), runID, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := steps[0].Observation.Retrieval
	if r == nil || !r.More || r.NextCursor == "" {
		t.Fatalf("missing cursor %#v", r)
	}
	quoted, _ := json.Marshal(r.NextCursor)
	body := string(f.model.sent[1])
	if !strings.Contains(body, `"next_cursor":`+string(quoted)) || !strings.Contains(body, "needle") || !strings.Contains(body, "Source /canti/imports/a.txt") {
		t.Fatalf("cursor/evidence absent in actual request: %s", body)
	}
}
