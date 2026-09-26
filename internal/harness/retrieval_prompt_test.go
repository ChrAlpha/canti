package harness

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestRetrievalPromptShowsEvidenceAndContinuationOnce(t *testing.T) {
	for _, operation := range []string{"read", "search", "list"} {
		t.Run(operation, func(t *testing.T) {
			r := Retrieval{Query: RetrievalQuery{Operation: operation, Path: "/canti/runs/R/steps/00000001/preview", Reason: "verify approved timeout"},
				NextOffset: 321, NextCursor: "continuation-token", More: true}
			if operation == "list" {
				r.Entries = []string{"first", "second"}
			} else {
				r.Sources = []SourceRange{{Path: r.Query.Path, Offset: 123, Length: 30, SHA256: strings.Repeat("a", 64)}}
			}
			body, _ := json.Marshal(r)
			query, _ := json.Marshal(r.Query)
			f := testFrame()
			f.Step = 2
			f.Steps = []StepRecord{{Step: 1, Command: retrievePrefix + string(query), Observation: Observation{
				Execution: core.ExecutionExited, Preview: string(body), Retrieval: &r,
			}}}
			if operation != "list" {
				f.Retrieved = map[int]string{1: "Source " + r.Query.Path + ":\nBytes [123,153), sha256 " + r.Sources[0].SHA256 + ":\nUse 47 seconds.\n```\nExact tail.\n"}
			}
			p := mustPrompt(t, f, DefaultPromptLimits()).User()
			for _, text := range []string{r.Query.Reason, r.NextCursor} {
				if strings.Count(p, text) != 1 {
					t.Fatalf("expected one %q in prompt: %s", text, p)
				}
			}
			if strings.Contains(p, "Preview:") || !strings.Contains(p, `"next_offset":321`) || !strings.Contains(p, `"more":true`) {
				t.Fatalf("redundant preview or missing continuation: %s", p)
			}
			if operation != "list" && (!strings.Contains(p, f.Retrieved[1]) || strings.Count(p, r.Sources[0].SHA256) != 1) {
				t.Fatalf("original evidence was changed or repeated: %s", p)
			}
			if f.Steps[0].Observation.Preview != string(body) {
				t.Fatal("rendering changed the persisted preview")
			}
			t.Logf("operation=%s rendered_step_bytes=%d persisted_preview_bytes=%d", operation, len(f.step(f.Steps[0], DefaultPromptLimits())), len(body))
		})
	}
}

func TestRetrievalPromptKeepsFailureWithoutPreview(t *testing.T) {
	f := testFrame()
	r := &Retrieval{Error: "source unavailable: permission denied"}
	s := StepRecord{Step: 1, Command: "canti:retrieve {}", Observation: Observation{Retrieval: r, Preview: "duplicate failure"}}
	text := f.step(s, DefaultPromptLimits())
	if !strings.Contains(text, r.Error) || strings.Contains(text, "duplicate failure") {
		t.Fatalf("failure rendering: %s", text)
	}
	s.Observation.Retrieval = nil
	s.Observation.Preview = "Context action failed: malformed action"
	if !strings.Contains(f.step(s, DefaultPromptLimits()), s.Observation.Preview) {
		t.Fatal("malformed builtin failure was hidden")
	}
}

func TestRetrievalPromptKeepsQueryWhenCommandIsClipped(t *testing.T) {
	for _, query := range []RetrievalQuery{
		{Operation: "search", Path: "/canti/runs/R/steps/00000001/preview", Query: strings.Repeat("x", 2500), Reason: "check the original scope"},
		{Operation: "read", Path: "/canti/imports/" + strings.Repeat("segment/", 375) + "evidence", Reason: "check the original scope"},
		{Operation: "search", Path: "/canti/imports/evidence", Query: strings.Repeat("`", 800), Reason: "check the original scope"},
	} {
		r := Retrieval{Query: query}
		body, _ := json.Marshal(r)
		command, _ := json.Marshal(query)
		s := StepRecord{Step: 1, Command: retrievePrefix + string(command), Observation: Observation{Retrieval: &r, Preview: string(body)}}
		text := testFrame().step(s, DefaultPromptLimits())
		if !strings.Contains(text, "[truncated") || !strings.Contains(text, string(body)) {
			t.Fatalf("clipped command lost retrieval query: %s", text)
		}
	}
}
