package anthropic

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness"
)

func TestSummaryFreezesWithoutDecisionToolsOrFallbacks(t *testing.T) {
	m, err := New(Config{APIKey: "test", Model: "model", DecisionFormat: "tool", StructuredOutput: true, Fallbacks: true})
	if err != nil {
		t.Fatal(err)
	}
	req, err := m.FreezeSummary(harness.Prompt{System: "Summarize only.", Blocks: []string{"history"}, Stable: 1}, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var b map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &b); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tools", "tool_choice", "output_config", "fallbacks"} {
		if _, ok := b[key]; ok {
			t.Fatalf("summary has %s", key)
		}
	}
	if strings.Contains(string(req.Body), "cache_control") || string(b["max_tokens"]) != "2048" || req.Adapter != summaryAdapter {
		t.Fatalf("request: %s", req.Body)
	}
}

func TestSummaryRejectsRefusalTruncationToolsAndEmptyText(t *testing.T) {
	m := &Model{}
	for _, body := range []string{
		`{"stop_reason":"max_tokens","content":[{"type":"text","text":"partial"}]}`,
		`{"stop_reason":"refusal","content":[{"type":"text","text":"refused"}]}`,
		`{"stop_reason":"end_turn","content":[{"type":"tool_use","text":"wrong"}]}`,
		`{"stop_reason":"end_turn","content":[{"type":"text","text":" "}]}`,
	} {
		if text, err := m.Summary(harness.Response{Adapter: summaryAdapter, Complete: true, Body: []byte(body)}); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
	text, err := m.Summary(harness.Response{Adapter: summaryAdapter, Complete: true, Body: []byte(`{"stop_reason":"end_turn","content":[{"type":"text","text":"Continue the work."}]}`)})
	if err != nil || text != "Continue the work." {
		t.Fatalf("%q %v", text, err)
	}
}

func TestSummaryUsageRequiresExplicitNonnegativeCounts(t *testing.T) {
	for _, body := range []string{`{}`, `{"usage":{}}`, `{"usage":{"input_tokens":1}}`, `{"usage":{"input_tokens":1,"output_tokens":-1}}`, `{"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":-2}}`, `{"usage":{"input_tokens":9223372036854775807,"output_tokens":1}}`, `{"usage":{"input_tokens":0,"input_tokens":1,"output_tokens":0}}`} {
		if _, ok := reportedUsage([]byte(body)); ok {
			t.Fatalf("invalid usage accepted %s", body)
		}
	}
	u, ok := reportedUsage([]byte(`{"usage":{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":5,"cache_creation_input_tokens":3}}`))
	if !ok || u.InputTokens != 18 || u.OutputTokens != 2 {
		t.Fatalf("usage %+v %t", u, ok)
	}
}
