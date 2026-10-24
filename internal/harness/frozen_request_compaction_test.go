package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func compactScenarios() []frozenScenario {
	command := "inspect " + strings.Repeat("x", 512)
	base := frozenScenario{name: "independent-compact", windowSteps: 2, limits: core.Limits{MaxSteps: 0, MaxAttempts: 3}, outputs: map[string]string{command: "committed evidence\n"},
		runs: []frozenRun{{id: corpusRun, goal: "Keep the original obligation.", replies: []string{
			corpusPurpose(core.Exec, "Inspect the original record, then verify its result.", command),
			corpusPurpose(core.Exec, "Verify the observation.", command),
			corpusRetrieve("read", corpusSteps+"/00000001/purpose", "", "Recover the purpose after compact."),
			corpusDecision(core.Return, "The original purpose was verified."),
		}, steps: []string{"1 exited 0 output complete", "2 exited 0 output complete", "3 exited 0 retrieval 1 sources 0 entries"}}},
		marks: map[string][]string{"run1-step03-rev1.json": {"## Session summary", "### Step 2\n", "Step 3 (unlimited steps)"}, "run1-step04-rev1.json": {"Inspect the original record, then verify its result."}},
		edge: func(t *testing.T, r map[string]harness.Request) {
			a, b := r["run1-step03-rev1.json"], r["run1-step04-rev1.json"]
			if a.CheckpointID == "" || a.CheckpointID == b.CheckpointID || a.Window.Through != 2 || b.Window.Through != 3 {
				t.Fatal("repeat compaction did not consume the previous retained tail")
			}
		},
	}
	cross := base
	cross.name = "session-compact"
	cross.runs = append([]frozenRun{}, base.runs...)
	cross.runs = append(cross.runs, frozenRun{id: corpusNextRun, goal: "Continue, but use sync now.", constraints: "Read only; preserve completed effects.", replies: []string{corpusDecision(core.Return, "Earlier results retained.")}})
	cross.marks = map[string][]string{"run2-step01-rev1.json": {"Continue, but use sync now.", "Read only; preserve completed effects.", "## Session summary", "Earlier assistant return:", "The original purpose was verified."}}
	cross.edge = func(t *testing.T, requests map[string]harness.Request) {
		base.edge(t, requests)
		body := string(requests["run2-step01-rev1.json"].Body)
		if strings.Index(body, "## Session summary") > strings.Index(body, "Earlier assistant return:") {
			t.Fatal("retained history precedes its older summary")
		}
	}
	tool := cross
	tool.name, tool.tool = "tool-session-compact", true
	failed := base
	failed.name = "summary-exhausted"
	failed.edge = nil
	failed.marks = nil
	failed.runs = []frozenRun{{id: corpusRun, goal: "Never discard the original context on summary failure.", replies: []string{corpusDecision(core.Exec, command), corpusDecision(core.Exec, command)}, steps: []string{"1 exited 0 output complete", "2 exited 0 output complete"}, stop: core.StopUnrecoverable}}
	failed.summaryReplies = []string{`{"stop_reason":"max_tokens","content":[{"type":"text","text":"partial"}]}`, `{"stop_reason":"end_turn","content":[{"type":"tool_use","name":"exec"}]}`, `{"stop_reason":"end_turn","content":[{"type":"text","text":" "}]}`}
	executable := failed
	executable.name = "summary-exec-rejected"
	bad, _ := json.Marshal(core.Decision{Kind: core.Exec, Purpose: "Must never execute.", Payload: "touch unexpected-effect"})
	text, _ := json.Marshal(string(bad))
	response := `{"stop_reason":"end_turn","content":[{"type":"text","text":` + string(text) + `}]}`
	executable.summaryReplies = []string{response, response, response}
	return []frozenScenario{base, cross, tool, failed, executable}
}

func (r *corpusRig) compactionRecords(t *testing.T, runs []frozenRun, got map[string][]byte) {
	t.Helper()
	var sent [][]byte
	for i, run := range runs {
		records, err := r.store.Compactions(context.Background(), run.id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		for n, record := range records {
			c := record.Operation
			name := fmt.Sprintf("run%d-summary%02d.json", i+1, n+1)
			got[name] = c.Plan.Request.Body
			for range c.Attempts {
				sent = append(sent, c.Plan.Request.Body)
			}
			c.OmitBodies()
			data, err := json.MarshalIndent(c, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got[fmt.Sprintf("run%d-compact%02d.json", i+1, n+1)] = append(data, '\n')
		}
	}
	r.model.mu.Lock()
	defer r.model.mu.Unlock()
	if len(sent) != len(r.model.summaries) {
		t.Fatalf("summary send/commit mismatch %d/%d", len(r.model.summaries), len(sent))
	}
	for i := range sent {
		if !bytes.Equal(sent[i], r.model.summaries[i]) {
			t.Fatal("summary sent different bytes than frozen")
		}
	}
}
