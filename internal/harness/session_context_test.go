package harness

import (
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestSessionProjectionKeepsPriorStepsAndTerminalStatus(t *testing.T) {
	l := &Loop{cfg: LoopConfig{Environment: "Linux", Limits: DefaultPromptLimits(), SummaryBytes: 2048}}
	s, _ := core.NewState("current", core.Limits{MaxSteps: 10, MaxAttempts: 3})
	selection := contextSelection{Entries: []ContextEntry{
		{Index: 1, RunID: "previous", Kind: "input", Goal: "preserve the original task", Constraints: "read only"},
		{Index: 2, RunID: "previous", Kind: "step", Step: &StepRecord{Step: 1, Command: "inspect source", Observation: Observation{Execution: core.ExecutionExited}}},
		{Index: 3, RunID: "previous", Kind: "result", Phase: core.PhaseStopped, Reason: core.StopCancelled},
		{Index: 4, RunID: "current", Kind: "input", Goal: "continue"},
	}}
	f := l.selectedFrame(Run{ID: "current", Goal: "continue"}, s, selection)
	p, err := f.Prompt(l.cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"preserve the original task", "read only", "inspect source", "stopped, reason cancelled", "source /canti/runs/previous"} {
		if !strings.Contains(p.User(), text) {
			t.Fatalf("missing %q", text)
		}
	}
	if strings.Count(p.User(), "continue") != 1 {
		t.Fatal("current input duplicated")
	}
	if len(f.Steps) != 0 || f.Window.FirstStep != 1 {
		t.Fatal("prior steps changed current run numbering")
	}
}

func TestSessionProjectionKeepsPreviousRetainedTailInNextSources(t *testing.T) {
	l := &Loop{cfg: LoopConfig{Environment: "Linux", Limits: DefaultPromptLimits(), SummaryBytes: 2048, RetainBytes: 1}}
	s, _ := core.NewState("current", core.Limits{MaxSteps: 20, MaxAttempts: 3})
	s.Step = 4
	selection := contextSelection{Checkpoint: Checkpoint{ID: "first", Through: 40, Summary: "older work"}, Entries: []ContextEntry{
		{Index: 41, RunID: "current", Kind: "step", Step: &StepRecord{Step: 1, Command: "first retained", Observation: Observation{Execution: core.ExecutionExited}}},
		{Index: 42, RunID: "current", Kind: "step", Step: &StepRecord{Step: 2, Command: "second retained", Observation: Observation{Execution: core.ExecutionExited}}},
		{Index: 43, RunID: "current", Kind: "step", Step: &StepRecord{Step: 3, Command: "newest", Observation: Observation{Execution: core.ExecutionExited}}},
	}}
	cut := l.compactionCut(Run{ID: "current"}, selection)
	if cut != 2 || selection.Entries[:cut][0].Index != 41 {
		t.Fatalf("cut %d skipped retained sources", cut)
	}
	f := l.selectedFrame(Run{ID: "current", Goal: "continue"}, s, selection)
	if _, err := f.Prompt(l.cfg.Limits); err != nil {
		t.Fatal(err)
	}
}

func TestSessionProjectionOrdersSummaryBeforeRetainedCorrections(t *testing.T) {
	l := &Loop{cfg: LoopConfig{Environment: "Linux", Limits: DefaultPromptLimits(), SummaryBytes: 2048, RetainBytes: 1}}
	s, _ := core.NewState("current", core.Limits{MaxSteps: 20, MaxAttempts: 3})
	s.Step = 2
	selection := contextSelection{Checkpoint: Checkpoint{ID: "cp", Through: 3, Summary: "OLD: use async"}, Entries: []ContextEntry{
		{Index: 4, RunID: "previous", Kind: "input", Goal: "CORRECTION: use sync"},
		{Index: 5, RunID: "previous", Kind: "result", Result: "RESULT: sync selected", Phase: core.PhaseReturned},
		{Index: 6, RunID: "current", Kind: "input", Goal: "continue"},
		{Index: 7, RunID: "current", SourceSeq: 7, Kind: "step", Step: &StepRecord{Seq: 7, Step: 1, Command: "VERIFY: inspect sync", Observation: Observation{Execution: core.ExecutionExited}}},
	}}
	f := l.selectedFrame(Run{ID: "current", SessionID: "session", Goal: "continue"}, s, selection)
	p, err := f.Prompt(l.cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	last := -1
	for _, marker := range []string{"OLD: use async", "CORRECTION: use sync", "RESULT: sync selected", "VERIFY: inspect sync"} {
		at := strings.Index(p.User(), marker)
		if at <= last || strings.Count(p.User(), marker) != 1 {
			t.Fatalf("history reordered or duplicated %q", marker)
		}
		last = at
	}
	if p.CacheThrough != 4 || p.Stable != 5 || f.capture.Prefix.Through != 6 {
		t.Fatalf("cache boundary: %+v", f.capture.Prefix)
	}
	for i, end := range []int{2, 3, 4, 5} {
		if f.capture.Entries[i].BlockEnd != end {
			t.Fatalf("source %d has wrong block boundary", i+4)
		}
	}
	if f.capture.Bytes.Total != len(p.System)+len(p.User()) {
		t.Fatal("source blocks escaped byte accounting")
	}
}

func TestSummaryOnlySourceRangeHasOneHash(t *testing.T) {
	if ContextSourceHash(nil) != ContextSourceHash([]ContextEntry{}) {
		t.Fatal("empty source range changes identity between planning and SQLite")
	}
}

func TestSummaryOnlyPrefixRejectsUnmappedOrWrongCheckpointHistory(t *testing.T) {
	cp := Checkpoint{ID: "checkpoint", Through: 1}
	r := Request{CheckpointID: cp.ID, Context: &ContextCapture{CheckpointID: cp.ID, Through: 1,
		Entries: []ContextEntryRef{{Index: 2, RunID: "run", Kind: "step"}}}}
	if MatchingPrefix(r, cp, nil) != 0 {
		t.Fatal("an unmapped header may include uncovered history")
	}
	r.Context.Entries[0].BlockEnd = 2
	r.Context.CheckpointID = "different"
	if MatchingPrefix(r, cp, nil) != 0 {
		t.Fatal("accepted inconsistent checkpoint provenance")
	}
	r.Context.CheckpointID = cp.ID
	if MatchingPrefix(r, cp, nil) != 1 {
		t.Fatal("mapped summary header is not reusable")
	}
}
