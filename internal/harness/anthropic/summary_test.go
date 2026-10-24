package anthropic

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestSummaryPreservesFrozenPrefixAndDecisionProtocol(t *testing.T) {
	for _, format := range []string{"text", "tool"} {
		t.Run(format, func(t *testing.T) {
			m, err := New(Config{APIKey: "test", Model: "original", DecisionFormat: format, StructuredOutput: true, Fallbacks: true})
			if err != nil {
				t.Fatal(err)
			}
			p := harness.Prompt{System: "Original system.", Blocks: []string{"head", "covered history", "retained tail", "progress"}, Stable: 3, CacheThrough: 2}
			parent, err := m.Freeze(p)
			if err != nil {
				t.Fatal(err)
			}
			original := string(parent.Body)
			m.cfg.Model, m.cfg.Fallbacks = "different", false
			instruction := harness.CompactionPromptPrefix + "\nReturn the summary in payload."
			req, err := m.FreezeSummary(harness.SummaryInput{Parent: &parent, Blocks: 2, Instruction: instruction}, 2048)
			if err != nil {
				t.Fatal(err)
			}
			var before, after map[string]json.RawMessage
			_ = json.Unmarshal(parent.Body, &before)
			_ = json.Unmarshal(req.Body, &after)
			for key, value := range before {
				if key != "messages" && key != "max_tokens" && string(value) != string(after[key]) {
					t.Fatalf("changed %s", key)
				}
			}
			var old, next []userMessage
			_ = json.Unmarshal(before["messages"], &old)
			_ = json.Unmarshal(after["messages"], &next)
			if len(next) != 2 || !reflect.DeepEqual(next[0].Content, old[0].Content[:2]) || next[1].Content[0].Text != instruction {
				t.Fatal("frozen prefix changed")
			}
			if strings.Contains(string(req.Body), "retained tail") || strings.Contains(string(req.Body), "progress") || string(parent.Body) != original {
				t.Fatal("summary included uncovered data or mutated parent")
			}
			if req.Protocol != parent.Protocol || req.Model != parent.Model || req.MaxOutputTokens != 2048 || usesTool(req.Adapter) != (format == "tool") {
				t.Fatalf("provenance: %+v", req)
			}
		})
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
	text, err := m.Summary(summaryDecisionResponse(t, false, core.Return, "Continue the work."))
	if err != nil || text != "Continue the work." {
		t.Fatalf("%q %v", text, err)
	}
}

func summaryDecisionResponse(t *testing.T, tool bool, kind core.Kind, payload string) harness.Response {
	t.Helper()
	d := core.Decision{Kind: kind, Purpose: "Preserve the context.", Payload: payload}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	stop, adapterName := "end_turn", summaryAdapter
	content := []map[string]any{{"type": "text", "text": string(raw)}}
	if tool {
		stop, adapterName = "tool_use", summaryToolAdapter
		content = []map[string]any{{"type": "tool_use", "id": "summary", "name": "canti_decision", "input": d}}
	}
	body, err := json.Marshal(map[string]any{"stop_reason": stop, "content": content})
	if err != nil {
		t.Fatal(err)
	}
	return harness.Response{Adapter: adapterName, Complete: true, Body: body}
}

func TestSummaryAcceptsOnlyReturnPayloadInEitherTransport(t *testing.T) {
	m := &Model{}
	for _, tool := range []bool{false, true} {
		for _, kind := range []core.Kind{core.Exec, core.Return} {
			got, err := m.Summary(summaryDecisionResponse(t, tool, kind, "preserve this fact"))
			if kind == core.Exec && err == nil {
				t.Fatal("accepted an executable summary")
			}
			if kind == core.Return && (err != nil || got != "preserve this fact") {
				t.Fatalf("summary %q: %v", got, err)
			}
		}
	}
}

func TestSummaryUsageRequiresExplicitNonnegativeCounts(t *testing.T) {
	for _, body := range []string{`{}`, `{"usage":{}}`, `{"usage":{"input_tokens":1}}`, `{"usage":{"input_tokens":1,"output_tokens":-1}}`, `{"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":-2}}`, `{"usage":{"input_tokens":9223372036854775807,"output_tokens":1}}`, `{"usage":{"input_tokens":0,"input_tokens":1,"output_tokens":0}}`} {
		if _, ok := reportedUsage([]byte(body)); ok {
			t.Fatalf("invalid usage accepted %s", body)
		}
	}
	u, ok := reportedUsage([]byte(`{"usage":{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":5,"cache_creation_input_tokens":3}}`))
	if !ok || u.InputTokens != 18 || u.OutputTokens != 2 || u.Cache == nil || u.Cache.ReadTokens != 5 || u.Cache.WriteTokens != 3 {
		t.Fatalf("usage %+v %t", u, ok)
	}
	for _, body := range []string{`{"usage":{"input_tokens":10,"output_tokens":2}}`, `{"usage":{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":0}}`} {
		u, ok := reportedUsage([]byte(body))
		if !ok || u.Cache != nil {
			t.Fatal("missing cache counts treated as zero")
		}
	}
}
