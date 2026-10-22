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
