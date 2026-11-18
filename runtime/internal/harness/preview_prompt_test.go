package harness_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func largePreview(string) string { return strings.Repeat("line\n", 10000) }

func TestStepBlocksStayByteIdenticalAcrossRequestsWithinAnEpoch(t *testing.T) {
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 3}, harness.Budget{},
		execReply("first"), execReply("second"), returnReply("done"))
	f.exec.preview = largePreview
	f.run(f.loop())
	if len(f.model.prompts) != 3 {
		t.Fatalf("prompts = %d, want 3", len(f.model.prompts))
	}
	block := func(p harness.Prompt, step int) string {
		t.Helper()
		prefix := fmt.Sprintf("### Step %d\n", step)
		for _, block := range p.Blocks {
			if strings.HasPrefix(block, prefix) {
				return block
			}
		}
		t.Fatalf("prompt has no block for step %d", step)
		return ""
	}
	first := block(f.model.prompts[1], 1)
	if len(first) < 50_000 {
		t.Fatalf("step block is only %d bytes", len(first))
	}
	if later := block(f.model.prompts[2], 1); later != first {
		t.Fatalf("the second request rewrote the first step block:\nfirst %q\nlater %q", first, later)
	}
}

func TestLargeOutputUnderSmallContextBudgetOpensAnAnchor(t *testing.T) {
	const budget = 1000
	f := newFixture(t, core.Limits{MaxSteps: 6, MaxAttempts: 3}, harness.Budget{ContextTokens: budget},
		reported(execReply("first"), 10), reported(execReply("second"), 10), reported(returnReply("done"), 10))
	f.exec.preview = largePreview
	snap := f.run(f.loop())
	if snap.State.Phase != core.PhaseReturned {
		t.Fatalf("run ended in %+v", snap.State)
	}
	var anchor harness.Transition
	for _, tr := range budgetRequests(f) {
		if tr.Facts.ContextState != nil {
			anchor = tr
		}
	}
	if anchor.Seq == 0 || anchor.From.Step != 3 {
		t.Fatalf("no budget anchor at step 3: %+v", anchor)
	}
	b := anchor.Facts.Request.Context.Budget
	if b == nil || b.Tokens != budget || b.Projected <= budget || b.Bytes < 50_000 {
		t.Fatalf("anchor budget = %+v", b)
	}
}

func TestPreviewBlockKeepsContentWholeBeyondItsFence(t *testing.T) {
	for _, ticks := range []int{30000, 51196} {
		preview := strings.Repeat("`", ticks) + "TAIL"
		zero := 0
		frame := harness.Frame{Goal: "Read the whole line.", Environment: "shell", RunID: "run_1", Step: 2, MaxSteps: 5, Attempt: 1, MaxAttempts: 3,
			ContextState: &harness.ContextState{FirstStep: 1},
			Steps: []harness.StepRecord{{Seq: 1, Step: 1, Command: "cat fence.txt",
				Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: preview}}}}
		prompt, err := frame.Prompt(harness.DefaultPromptLimits())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(prompt.Blocks[1], preview) || !strings.Contains(prompt.Blocks[1], "TAIL") {
			t.Fatalf("ticks=%d: the request cut content inside its %d-byte limit: block %d bytes, suffix %q", ticks, harness.DefaultPromptLimits().Preview, len(prompt.Blocks[1]), prompt.Blocks[1][max(0, len(prompt.Blocks[1])-64):])
		}
	}
}
