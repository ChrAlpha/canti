package harness_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

type missingLatestWindowRequest struct {
	harness.Journal
}

func TestDirectorySearchGeneratedCursorCanAlwaysBeResumed(t *testing.T) {
	for _, tc := range []struct{ label, name string }{
		{"html", strings.Repeat("<", 255)},
		{"control", strings.Repeat("\x01", 255)},
		{"unicode", strings.Repeat("中", 85)},
	} {
		t.Run(tc.label, func(t *testing.T) {
			testDirectorySearchGeneratedCursor(t, tc.name)
		})
	}
}

func testDirectorySearchGeneratedCursor(t *testing.T, name string) {
	archive := t.TempDir()
	if err := os.WriteFile(filepath.Join(archive, name), []byte("needle needle"), 0600); err != nil {
		t.Fatal(err)
	}
	r := harness.Reader{Imports: archive}
	q := harness.RetrievalQuery{Operation: "search", Path: "/canti/imports", Query: "needle", Reason: "locate evidence", Limit: 6}
	first, err := r.Retrieve(context.Background(), q)
	if err != nil || first.Error != "" || !first.More || first.NextCursor == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	q.Cursor = first.NextCursor
	second, err := r.Retrieve(context.Background(), q)
	if err != nil || second.Error != "" || second.More || len(second.Sources) != 1 {
		t.Fatalf("generated %d-byte cursor cannot resume: error=%q more=%v sources=%d err=%v", len(q.Cursor), second.Error, second.More, len(second.Sources), err)
	}
}

func (j missingLatestWindowRequest) LatestRequest(context.Context, string) (harness.Request, error) {
	return harness.Request{}, harness.ErrNotFound
}

func TestWindowRecoveryRejectsMissingDurableRequestAfterProgress(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{},
		execReply("echo old-window-marker"),
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue with the next operation."}`))
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 3 {
			break
		}
	}
	before := len(f.model.prompts)
	s, err := f.loopOn(missingLatestWindowRequest{f.open()}).Step(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if s.State.Phase != core.PhaseStopped || s.State.Reason != core.StopUnrecoverable || len(f.model.prompts) != before {
		t.Fatalf("missing durable window silently rebuilt context: state=%+v new_prompts=%d", s.State, len(f.model.prompts)-before)
	}
}

func TestWindowProtocolRepairRejectsMissingInitialFrozenRequest(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, reply{body: "invalid decision", usage: usage})
	f.cfg.WindowSteps = 1
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentPrepareRequest && s.State.RequestRev > 0 {
			break
		}
	}
	before := len(f.model.prompts)
	s, err := f.loopOn(missingLatestWindowRequest{f.open()}).Step(ctx, runID)
	if err != nil || s.State.Phase != core.PhaseStopped || s.State.Reason != core.StopUnrecoverable || len(f.model.prompts) != before {
		t.Fatalf("protocol repair recreated missing initial request: state=%+v new_prompts=%d err=%v", s.State, len(f.model.prompts)-before, err)
	}
}
