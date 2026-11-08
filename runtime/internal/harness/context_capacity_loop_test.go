package harness_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
)

func TestTenMaximumReadsStayOnOnePage(t *testing.T) {
	imported := t.TempDir()
	var content strings.Builder
	var replies []reply
	for i := range 10 {
		marker := fmt.Sprintf("EVIDENCE_%02d", i)
		content.WriteString(marker + strings.Repeat("x", archive.MaxBytes-len(marker)))
		q, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/evidence", Reason: "inspect next part", Offset: int64(i * archive.MaxBytes), Limit: archive.MaxBytes})
		replies = append(replies, execReply("canti:retrieve "+string(q)))
	}
	replies = append(replies, returnReply("done"))
	if err := os.WriteFile(filepath.Join(imported, "evidence"), []byte(content.String()), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 12, MaxAttempts: 3}, harness.Budget{}, replies...)
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 256 << 10
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Intent() == core.IntentHalt {
			wantState(t, s, core.PhaseReturned, "")
			break
		}
	}
	if len(f.model.prompts) != 11 {
		t.Fatalf("prompts=%d, want 11", len(f.model.prompts))
	}
	last := f.model.prompts[10].User()
	for i := range 10 {
		marker := fmt.Sprintf("EVIDENCE_%02d", i)
		if strings.Contains(last, marker) != (i == 9) {
			t.Fatalf("retrieval %d has the wrong transient lifetime", i)
		}
	}
	if strings.Contains(last, "Handover required") {
		t.Fatal("256 KiB page handed over before ten legal reads")
	}
}

// TestRemovedEvidenceStaysFrozenAcrossSQLiteReopen requires that evidence
// a retrieval committed remains in its next decision step after a journal
// reopen and source removal.
func TestRemovedEvidenceStaysFrozenAcrossSQLiteReopen(t *testing.T) {
	imported := t.TempDir()
	file := filepath.Join(imported, "evidence")
	if err := os.WriteFile(file, []byte(strings.Repeat("x", archive.MaxBytes)), 0600); err != nil {
		t.Fatal(err)
	}
	q, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/evidence", Reason: "inspect source", Limit: archive.MaxBytes})
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 3}, harness.Budget{},
		execReply("canti:retrieve "+string(q)), execReply("canti:retrieve "+string(q)), returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 96 << 10
	ctx := f.bounded()
	for i := 0; i < 20; i++ {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 3 {
			break
		}
		if i == 19 {
			t.Fatal("retrieval did not finish")
		}
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	if len(f.model.prompts) != 3 {
		t.Fatalf("prompts = %d, want 3", len(f.model.prompts))
	}
	evidence := "sha256 " + fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Repeat("x", archive.MaxBytes)))) + ":\n" + strings.Repeat("x", archive.MaxBytes) + "\n"
	if last := f.model.prompts[2].User(); strings.Count(last, evidence) != 1 || strings.Contains(last, "Evidence unavailable") {
		t.Fatalf("the request after removing the source does not show its frozen latest result:\n%s", last)
	}
}

func TestSerializedRequestCapIncludesProviderEscaping(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
	f.cfg.Environment = strings.Repeat("\x01", 1000)
	f.cfg.Limits = harness.DefaultPromptLimits()
	// The fake model freezes the unescaped rendering. Use a real adapter to
	// prove that escaped serialized bytes are independently capped.
	model, err := anthropic.New(anthropic.Config{APIKey: "offline-unused", Model: "offline"})
	if err != nil {
		t.Fatal(err)
	}
	frame := harness.Frame{Goal: "Count the Go files.", Constraints: "Do not modify files.", Environment: f.cfg.Environment, RunID: runID, ContextState: &harness.ContextState{FirstStep: 1}, Step: 1, MaxSteps: limits.MaxSteps, Attempt: 1, MaxAttempts: limits.MaxAttempts}
	prompt, err := frame.Prompt(f.cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.Limits.Total = len(prompt.System) + len(prompt.User()) + 100
	loop, err := harness.NewLoop(f.store, f.exec, model, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := loop.Step(context.Background(), runID)
	if err != nil || snap.State.Reason != core.StopUnrecoverable || !strings.Contains(f.last().Facts.Failure, "serialized") {
		t.Fatalf("serialization cap=%+v %v", snap, err)
	}
	if len(f.transitions()) != 1 {
		t.Fatal("resource failure froze or sent a request")
	}
}
