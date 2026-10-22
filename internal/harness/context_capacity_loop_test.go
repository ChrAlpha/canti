package harness_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
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
		if !strings.Contains(last, marker) {
			t.Fatalf("retrieval %d disappeared from the retained source range", i)
		}
	}
	if len(f.model.summarySent) > 0 {
		t.Fatal("256 KiB page compacted before ten legal reads")
	}
}

// TestRemovedEvidenceStaysFrozenAcrossSQLiteReopen requires that evidence
// a retrieval committed is shown, byte for byte, by every later request of
// a loop reopened on the journal after its source was removed.
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
	f.cfg.SummaryBytes = 2048
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
	if last := f.model.prompts[2].User(); strings.Count(last, evidence) != 2 || strings.Contains(last, "Evidence unavailable") {
		t.Fatalf("the request after removing the source does not show both frozen evidence blocks:\n%s", last)
	}
}

func TestRetrievalBudgetIncludesSourceHeadersAndFencesAcrossReopen(t *testing.T) {
	imported := t.TempDir()
	relative := strings.Repeat(strings.Repeat("a", 200)+"/", 15) + "evidence"
	file := filepath.Join(imported, relative)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	evidence := strings.Repeat("`", archive.MaxBytes)
	if err := os.WriteFile(file, []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	q, err := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/" + relative, Reason: "verify original bytes", Limit: archive.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve "+string(q)), execReply("canti:retrieve "+string(q)), returnReply("done"))
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 128 << 10
	f.cfg.Reader = &archive.Reader{Imports: imported}
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
	last := f.model.prompts[len(f.model.prompts)-1]
	used := 0
	for _, block := range last.Blocks {
		if i := strings.Index(block, "Retrieved original evidence:\n"); i >= 0 {
			used += len(block[i:])
		}
	}
	if used < 2*len(evidence) || strings.Count(last.User(), "\n"+evidence+"\n") != 2 {
		t.Fatal("both accepted evidence expansions must remain complete on the same page")
	}
}

func TestRetrievalRenderedSourceOverheadDoesNotHideNewestObservation(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "unchanged"
		if changed {
			name = "changed"
		}
		t.Run(name, func(t *testing.T) {
			testRetrievalRenderedSourceOverhead(t, changed)
		})
	}
}

func testRetrievalRenderedSourceOverhead(t *testing.T, changed bool) {
	imported := t.TempDir()
	relative := strings.Repeat(strings.Repeat("a", 100)+"/", 20) + "evidence"
	file := filepath.Join(imported, relative)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.Repeat("x", 10000)), 0600); err != nil {
		t.Fatal(err)
	}
	query, err := json.Marshal(archive.RetrievalQuery{
		Operation: "search", Path: "/canti/imports/" + relative, Reason: "recover earlier facts", Query: "x", Limit: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve "+string(query)), returnReply("done"))
	f.cfg.Reader = &archive.Reader{Imports: imported}
	f.cfg.CompactBytes = 48 << 10
	ctx := f.bounded()
	for {
		s, err := f.fresh().Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Step == 2 {
			break
		}
	}
	if changed {
		if err := os.WriteFile(file, []byte(strings.Repeat("y", 10000)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.fresh().Run(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if len(f.model.sent) != 2 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	last := string(f.model.sent[1])
	if !strings.Contains(last, "### Step 1") || !strings.Contains(last, "Retrieval continuation:") {
		t.Fatal("source citation overhead caused the whole newest observation and its continuation to disappear")
	}
	if changed && (strings.Contains(last, "yyyy") || !strings.Contains(last, "xxxx")) {
		t.Fatal("a source changed after its retrieval replaced the evidence frozen at observation")
	}
}

// TestContextFitNormalizesAsNewLoop requires that ContextFit applies the
// defaults of NewLoop and refuses the configurations NewLoop refuses.
func TestContextFitNormalizesAsNewLoop(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	run := harness.Run{ID: runID, Goal: "Count the Go files."}
	f.cfg.Reader = &archive.Reader{}
	implicit, err := harness.ContextFit(f.cfg, run, limits)
	if err != nil {
		t.Fatal(err)
	}
	explicit := f.cfg
	explicit.SummaryBytes, explicit.Limits = 16384, harness.DefaultPromptLimits()
	if fit, err := harness.ContextFit(explicit, run, limits); err != nil || fit != implicit || fit.Summary != 16384 || fit.Limit != explicit.Limits.Total {
		t.Errorf("the defaults account %+v, their values %+v: %v", implicit, fit, err)
	}
	for _, bad := range []func(*harness.LoopConfig){
		func(c *harness.LoopConfig) { c.SummaryBytes = -1 }, func(c *harness.LoopConfig) { c.SummaryBytes = 65537 },
		func(c *harness.LoopConfig) { c.Limits = harness.PromptLimits{Total: 1} },
		func(c *harness.LoopConfig) { c.Environment = "" }, func(c *harness.LoopConfig) { c.CompactSteps = -1 },
	} {
		c := f.cfg
		bad(&c)
		_, fitErr := harness.ContextFit(c, run, limits)
		_, loopErr := harness.NewLoop(f.store, f.exec, f.model, c)
		if fitErr == nil || loopErr == nil || fitErr.Error() != loopErr.Error() {
			t.Errorf("ContextFit refuses %+v with %v, NewLoop with %v", c, fitErr, loopErr)
		}
	}
}
