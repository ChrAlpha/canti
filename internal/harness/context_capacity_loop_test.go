package harness_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestTenMaximumReadsStayOnOnePage(t *testing.T) {
	archive := t.TempDir()
	var content strings.Builder
	var replies []reply
	for i := range 10 {
		marker := fmt.Sprintf("EVIDENCE_%02d", i)
		content.WriteString(marker + strings.Repeat("x", harness.RetrievalMaxBytes-len(marker)))
		q, _ := json.Marshal(harness.RetrievalQuery{Operation: "read", Path: "/canti/imports/evidence", Reason: "inspect next part", Offset: int64(i * harness.RetrievalMaxBytes), Limit: harness.RetrievalMaxBytes})
		replies = append(replies, execReply("canti:retrieve "+string(q)))
	}
	replies = append(replies, returnReply("done"))
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(content.String()), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, core.Limits{MaxSteps: 12, MaxAttempts: 3}, harness.Budget{}, replies...)
	f.cfg.Reader = &harness.Reader{Imports: archive}
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
			t.Fatalf("retrieval %d disappeared before handover", i)
		}
	}
	if strings.Contains(last, "Handover required") {
		t.Fatal("256 KiB page handed over before ten legal reads")
	}
}

func TestUnavailableEvidenceTriggersHandoverAcrossSQLiteReopen(t *testing.T) {
	archive := t.TempDir()
	file := filepath.Join(archive, "evidence")
	if err := os.WriteFile(file, []byte(strings.Repeat("x", harness.RetrievalMaxBytes)), 0600); err != nil {
		t.Fatal(err)
	}
	q, _ := json.Marshal(harness.RetrievalQuery{Operation: "read", Path: "/canti/imports/evidence", Reason: "inspect source", Limit: harness.RetrievalMaxBytes})
	f := newFixture(t, core.Limits{MaxSteps: 5, MaxAttempts: 3}, harness.Budget{},
		execReply("canti:retrieve "+string(q)),
		execReply("canti:retrieve "+string(q)),
		execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Resume after checking original evidence."}`), returnReply("done"))
	f.cfg.Reader = &harness.Reader{Imports: archive}
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
	if len(f.model.prompts) != 4 || !strings.Contains(f.model.prompts[2].User(), "Evidence unavailable:") || !strings.Contains(f.model.prompts[2].User(), "Handover required") {
		t.Fatal("changed source did not remain explicit and trigger safe handover")
	}
	if strings.Contains(f.model.prompts[3].User(), "### Step 1") || !strings.Contains(f.model.prompts[3].User(), "Resume after checking original evidence") {
		t.Fatal("handover did not release old page across SQLite reopen")
	}
}

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
	f.cfg.Limits.Total = 128 << 10
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
	if used < 2*len(evidence) || strings.Count(last.User(), "\n"+evidence+"\n") != 2 {
		t.Fatal("both accepted evidence expansions must remain complete on the same page")
	}
}

func TestByteOnlyWindowTriggerPersistsHandoverAcrossReopen(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int
		requested bool
	}{{"threshold reached", 4096, true}, {"below threshold", 16384, false}} {
		t.Run(tc.name, func(t *testing.T) {
			archive := t.TempDir()
			evidence := "BYTE_WINDOW_ORIGINAL_EVIDENCE:" + strings.Repeat("x", 5000)
			if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(evidence), 0600); err != nil {
				t.Fatal(err)
			}
			replies := []reply{execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"recover original evidence","limit":8192}`)}
			if tc.requested {
				replies = append(replies, execReply(`canti:handover {"run_id":"run-1","first_step":1,"text":"Continue using original evidence references as needed."}`))
			}
			replies = append(replies, returnReply("done"))
			f := newFixture(t, limits, harness.Budget{}, replies...)
			f.cfg.Reader = &harness.Reader{Imports: archive}
			f.cfg.WindowBytes = tc.threshold
			ctx := f.bounded()
			for {
				s, err := f.fresh().Step(ctx, runID)
				if err != nil {
					t.Fatal(err)
				}
				if s.State.Intent() == core.IntentHalt {
					if s.State.Phase != core.PhaseReturned {
						t.Fatalf("unexpected halt: %+v", s.State)
					}
					break
				}
			}
			request, err := f.store.Request(ctx, runID, 2, 1)
			if err != nil || request.Window == nil || request.Window.FirstStep != 1 || request.Window.Requested != tc.requested {
				t.Fatalf("threshold %d window=%+v err=%v", tc.threshold, request.Window, err)
			}
			if !strings.Contains(string(request.Body), evidence) {
				t.Fatal("byte threshold discarded original evidence before handover")
			}
			if tc.requested {
				request, err = f.store.Request(ctx, runID, 3, 1)
				if err != nil || request.Window == nil || request.Window.FirstStep != 3 || request.Window.SourceStep != 2 || request.Window.Requested {
					t.Fatalf("new byte-triggered window=%+v err=%v", request.Window, err)
				}
				body := string(request.Body)
				if !strings.Contains(body, "Continue using original evidence references") || strings.Contains(body, evidence) || strings.Contains(body, "### Step 1") || strings.Contains(body, "### Step 2") {
					t.Fatal("new byte-triggered window did not retain only the accepted handover")
				}
			}
		})
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
	archive := t.TempDir()
	relative := strings.Repeat(strings.Repeat("a", 100)+"/", 20) + "evidence"
	file := filepath.Join(archive, relative)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(strings.Repeat("x", 10000)), 0600); err != nil {
		t.Fatal(err)
	}
	query, err := json.Marshal(harness.RetrievalQuery{
		Operation: "search", Path: "/canti/imports/" + relative, Reason: "recover earlier facts", Query: "x", Limit: 8192,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{}, execReply("canti:retrieve "+string(query)), returnReply("done"))
	f.cfg.Reader = &harness.Reader{Imports: archive}
	f.cfg.WindowBytes = 48 << 10
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
	if changed && !strings.Contains(last, "sha256 mismatch") {
		t.Fatal("source change diagnostic did not reach the model")
	}
}

// TestContextFitNormalizesAsNewLoop requires that ContextFit applies the
// defaults of NewLoop and refuses the configurations NewLoop refuses.
func TestContextFitNormalizesAsNewLoop(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	run := harness.Run{ID: runID, Goal: "Count the Go files."}
	f.cfg.Reader = &harness.Reader{}
	implicit, err := harness.ContextFit(f.cfg, run, limits)
	if err != nil {
		t.Fatal(err)
	}
	explicit := f.cfg
	explicit.HandoverBytes, explicit.Limits = 2048, harness.DefaultPromptLimits()
	if fit, err := harness.ContextFit(explicit, run, limits); err != nil || fit != implicit || fit.Handover != 2048 || fit.Limit != explicit.Limits.Total {
		t.Errorf("the defaults account %+v, their values %+v: %v", implicit, fit, err)
	}
	for _, bad := range []func(*harness.LoopConfig){
		func(c *harness.LoopConfig) { c.HandoverBytes = -1 }, func(c *harness.LoopConfig) { c.HandoverBytes = 8193 },
		func(c *harness.LoopConfig) { c.Limits = harness.PromptLimits{Total: 1} }, func(c *harness.LoopConfig) { c.RunsPath = " " },
		func(c *harness.LoopConfig) { c.Environment = "" }, func(c *harness.LoopConfig) { c.WindowSteps = -1 },
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
