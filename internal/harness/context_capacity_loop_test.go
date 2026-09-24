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

func TestRequestedHandoverRejectsWorkAcrossReopen(t *testing.T) {
	for _, command := range []string{"touch should-not-execute", `canti:retrieve {"operation":"read","path":"/canti/imports/missing","reason":"ignore handover"}`} {
		t.Run(command, func(t *testing.T) {
			f := newFixture(t, core.Limits{MaxSteps: 8, MaxAttempts: 3}, harness.Budget{}, execReply("true"), execReply(command),
				execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue after checking original evidence."}`), returnReply("done"))
			f.cfg.Reader = &harness.Reader{Imports: t.TempDir()}
			f.cfg.WindowSteps = 1
			ctx := context.Background()
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
			steps, err := f.store.Recent(ctx, runID, 0, 3)
			if err != nil || len(steps) != 3 {
				t.Fatalf("steps=%v err=%v", steps, err)
			}
			obs := steps[1].Observation
			if obs.Retrieval != nil || !strings.Contains(obs.Preview, "handover is required") || obs.ExitCode == nil || *obs.ExitCode != 1 {
				t.Fatalf("work was not rejected: %+v", obs)
			}
			if calls := f.exec.log(); len(calls) != 3 {
				t.Fatalf("only first work step may reach executor: %v", calls)
			}
		})
	}
}

func TestMaximumFencedHandoverStartsUsableWindow(t *testing.T) {
	h := harness.Handover{RunID: runID, FirstStep: 1, Text: strings.Repeat("`", 8192)}
	encoded, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{}, execReply("true"), execReply("canti:handover "+string(encoded)), returnReply("done"))
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 128 << 10
	f.cfg.HandoverBytes, f.cfg.WindowSteps = 8192, 1
	f.cfg.Reader = &harness.Reader{Imports: t.TempDir()}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	last := f.model.prompts[len(f.model.prompts)-1]
	if !strings.Contains(last.User(), "\n"+h.Text+"\n") || strings.Contains(last.User(), "### Step 1") {
		t.Fatal("maximum accepted handover was not preserved on a fresh page")
	}
}

func TestRetrievalBudgetIncludesSourceHeadersAndFencesAcrossReopen(t *testing.T) {
	archive := t.TempDir()
	relative := strings.Repeat(strings.Repeat("a", 200)+"/", 15) + "evidence"
	file := filepath.Join(archive, relative)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	evidence := strings.Repeat("`", harness.RetrievalMaxBytes)
	if err := os.WriteFile(file, []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	q, err := json.Marshal(harness.RetrievalQuery{Operation: "read", Path: "/canti/imports/" + relative, Reason: "verify original bytes", Limit: harness.RetrievalMaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve "+string(q)), execReply("canti:retrieve "+string(q)), returnReply("done"))
	f.cfg.Limits = harness.DefaultPromptLimits()
	f.cfg.Limits.Total = 80 << 10
	f.cfg.Reader = &harness.Reader{Imports: archive}
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
	if used > f.cfg.Limits.Total/2 {
		t.Fatalf("evidence rendered %d bytes over budget %d", used, f.cfg.Limits.Total/2)
	}
	if !strings.Contains(last.User(), "\n"+evidence+"\n") || !strings.Contains(last.User(), "active retrieval byte budget reached") {
		t.Fatal("newest evidence must remain complete and exhausted older expansion must be explicit")
	}
}
