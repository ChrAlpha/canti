package anthropic_test

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
)

const toolAdapter = "canti-anthropic/3-tool"

func toolMessage(stop, content string) string { return message(stop, content) }

func TestToolDecisionPreservesInputAndRejectsAmbiguity(t *testing.T) {
	input := `{"kind":"exec","payload":"printf '雪 🐈'"}`
	valid := `{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":` + input + `}`
	for _, tc := range []struct {
		name, stop, blocks, want string
		complete                 bool
	}{
		{"valid", `"tool_use"`, `[` + valid + `]`, input, true},
		{"narration", `"tool_use"`, `[{"type":"text","text":"ignored"},` + valid + `]`, input, true},
		{"duplicate field", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{"kind":"return","kind":"exec","payload":"x"}}]`, ``, true},
		{"two tools", `"tool_use"`, `[` + valid + `,` + valid + `]`, ``, true},
		{"wrong name", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"other","input":` + input + `}]`, ``, true},
		{"missing id", `"tool_use"`, `[{"type":"tool_use","name":"canti_decision","input":` + input + `}]`, ``, true},
		{"missing input", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision"}]`, ``, true},
		{"text only", `"tool_use"`, decision, ``, true},
		{"wrong stop", `"end_turn"`, `[` + valid + `]`, ``, true},
		{"incomplete", `"tool_use"`, `[` + valid + `]`, ``, false},
		{"server tool", `"tool_use"`, `[` + valid + `,{"type":"server_tool_use","id":"srv_1","name":"web_search","input":{}}]`, ``, true},
		{"duplicate input", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{"kind":"return","payload":"first"},"input":` + input + `}]`, ``, true},
		{"duplicate type", `"tool_use"`, `[{"type":"text","type":"tool_use","id":"toolu_1","name":"canti_decision","input":` + input + `}]`, ``, true},
		{"duplicate name", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"other","name":"canti_decision","input":` + input + `}]`, ``, true},
		{"name alias", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"other","Name":"canti_decision","input":` + input + `}]`, ``, true},
		{"input alias", `"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":null,"Input":` + input + `}]`, ``, true},
		{"type alias", `"tool_use"`, `[{"type":"text","Type":"tool_use","id":"toolu_1","name":"canti_decision","input":` + input + `}]`, ``, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := toolMessage(tc.stop, tc.blocks)
			m := newModel(t, "https://unused.test", nil)
			got := m.Decision(harness.Response{Body: []byte(body), Complete: tc.complete, Adapter: toolAdapter})
			if tc.want != "" && string(got) != tc.want {
				t.Fatalf("Decision = %s, want %s", got, tc.want)
			}
			_, err := core.Decode(got)
			if (err == nil) != (tc.want != "") {
				t.Fatalf("Decode(%s) = %v", got, err)
			}
		})
	}
}

func TestToolDecisionRejectsDuplicateTopLevelFields(t *testing.T) {
	valid := `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{"kind":"return","payload":"x"}}]`
	for _, body := range []string{
		`{"stop_reason":"end_turn","stop_reason":"tool_use","content":` + valid + `}`,
		`{"stop_reason":"tool_use","content":[],"content":` + valid + `}`,
		`{"stop_reason":"end_turn","Stop_Reason":"tool_use","content":` + valid + `}`,
		`{"stop_reason":"tool_use","content":[],"Content":` + valid + `}`,
	} {
		got := newModel(t, "https://unused.test", nil).Decision(harness.Response{Body: []byte(body), Complete: true, Adapter: toolAdapter})
		if _, err := core.Decode(got); err == nil {
			t.Fatalf("accepted duplicate response field: %s", body)
		}
	}
}

func TestToolDecisionUsesOnlyExactFieldNames(t *testing.T) {
	input := `{"kind":"return","payload":"ok"}`
	body := `{"stop_reason":"tool_use","Stop_Reason":"end_turn","content":[{"type":"tool_use","Type":"text","id":"toolu_1","name":"canti_decision","Name":"other","input":` + input + `,"Input":null}],"Content":[]}`
	got := newModel(t, "https://unused.test", nil).Decision(harness.Response{Body: []byte(body), Complete: true, Adapter: toolAdapter})
	if string(got) != input {
		t.Fatalf("exact fields changed: %s", got)
	}
}

func TestCompleteUsesFrozenToolAdapterAfterConfigChange(t *testing.T) {
	input := `{"kind":"return","payload":"ok"}`
	body := toolMessage(`"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":`+input+`}]`)
	srv := serve(t, reply(http.StatusOK, body))
	m := newModel(t, srv.URL, nil)
	req := harness.Request{Body: []byte(`{"model":"claude-opus-5"}`), Model: "claude-opus-5", Protocol: protocol, Adapter: toolAdapter}
	resp, err := m.Complete(context.Background(), req)
	if err != nil || !resp.Complete || resp.Adapter != toolAdapter || string(resp.Body) != body {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if got := string(m.Decision(resp)); got != input {
		t.Fatalf("Decision = %s", got)
	}
	if strings.Contains(string(resp.Body), apiKey) {
		t.Fatal("credential leaked")
	}
}

func TestFreezeToolRequest(t *testing.T) {
	m := newModel(t, "https://unused.test", func(c *anthropic.Config) {
		c.DecisionFormat = "tool"
		c.StructuredOutput = true
	})
	req := freeze(t, m)
	if req.Adapter != toolAdapter || req.Protocol != protocol || req.Model != "claude-opus-5" {
		t.Fatalf("frozen identity = %+v", req)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["output_config"]; ok {
		t.Fatal("tool request includes output_config")
	}
	if string(body["system"]) != `"sys"` {
		t.Fatalf("system changed: %s", body["system"])
	}
	var tools []struct {
		Name, Description string
		InputSchema       map[string]any `json:"input_schema"`
	}
	if err := json.Unmarshal(body["tools"], &tools); err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0].Name != "canti_decision" || !strings.Contains(tools[0].Description, "tool input") {
		t.Fatalf("tools = %+v", tools)
	}
	schema := tools[0].InputSchema
	if schema["type"] != "object" || schema["additionalProperties"] != false || !reflect.DeepEqual(schema["required"], []any{"kind", "payload", "purpose"}) {
		t.Fatalf("schema = %+v", schema)
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok || len(props) != 3 || props["payload"].(map[string]any)["type"] != "string" || !reflect.DeepEqual(props["kind"].(map[string]any)["enum"], []any{"exec", "return"}) ||
		!reflect.DeepEqual(props["purpose"], map[string]any{"type": "string", "description": "One short line: what this step is for and what you will do next with its result."}) {
		t.Fatalf("schema properties = %+v", props)
	}
	if !strings.Contains(string(body["tools"]), `"properties":{"kind":{"type":"string","enum":["exec","return"]},`+purposeField+`,"payload":{"type":"string"}}`) {
		t.Fatalf("tool schema does not order kind, purpose, payload: %s", body["tools"])
	}
	var choice map[string]any
	if err := json.Unmarshal(body["tool_choice"], &choice); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(choice, map[string]any{"type": "tool", "name": "canti_decision", "disable_parallel_tool_use": true}) {
		t.Fatalf("choice = %+v", choice)
	}
}

func TestCompleteUsesFrozenTextAdapterAfterConfigChange(t *testing.T) {
	srv := serve(t, reply(http.StatusOK, message(`"end_turn"`, decision)))
	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.DecisionFormat = "tool" })
	req := harness.Request{Body: []byte(`{"model":"claude-opus-5"}`), Model: "claude-opus-5", Protocol: protocol, Adapter: adapter}
	resp, err := m.Complete(context.Background(), req)
	if err != nil || !resp.Complete || resp.Adapter != adapter {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if got := string(m.Decision(resp)); got != `{"kind":"return","payload":"done"}` {
		t.Fatalf("Decision = %s", got)
	}
}

func TestToolDecisionRejectsUnknownAdapter(t *testing.T) {
	m := newModel(t, "https://unused.test", nil)
	if _, err := core.Decode(m.Decision(harness.Response{Adapter: "unknown", Complete: true, Body: []byte(`{"kind":"return","payload":"bad"}`)})); err == nil {
		t.Fatal("unknown adapter accepted")
	}
}

func TestToolDecisionPassesDuplicateFieldsToCore(t *testing.T) {
	input := `{"kind":"return","kind":"exec","payload":"echo 雪"}`
	body := toolMessage(`"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":`+input+`}]`)
	got := newModel(t, "https://unused.test", nil).Decision(harness.Response{Body: []byte(body), Complete: true, Adapter: toolAdapter})
	if string(got) != input {
		t.Fatalf("input changed: %s", got)
	}
	if _, err := core.Decode(got); err == nil {
		t.Fatal("core accepted duplicate field")
	}
}

func TestToolDecisionRejectsEndTurnAfterComplete(t *testing.T) {
	body := toolMessage(`"end_turn"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{"kind":"return","payload":"x"}}]`)
	m := newModel(t, serve(t, reply(http.StatusOK, body)).URL, nil)
	req := harness.Request{Body: []byte(`{}`), Model: "claude-opus-5", Protocol: protocol, Adapter: toolAdapter}
	resp, err := m.Complete(context.Background(), req)
	if err != nil || !resp.Complete || string(resp.Body) != body || resp.Adapter != toolAdapter {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if got := m.Decision(resp); len(got) != 0 {
		t.Fatalf("incomplete decision = %s", got)
	}
}

func TestNewRejectsUnknownDecisionFormat(t *testing.T) {
	_, err := anthropic.New(anthropic.Config{APIKey: apiKey, Model: "claude-opus-5", DecisionFormat: "unknown"})
	if err == nil {
		t.Fatal("unknown decision format accepted")
	}
}

func TestCompleteSendsRequestsFrozenBeforePurpose(t *testing.T) {
	const earlierSchema = `{"type":"object","properties":{"kind":{"type":"string","enum":["exec","return"]},"payload":{"type":"string"}},"required":["kind","payload"],"additionalProperties":false}`
	messages := `"messages":[{"role":"user","content":[{"type":"text","text":"head"` + cached + `},{"type":"text","text":"tail"}]}]`
	for _, tc := range []struct {
		name, adapter, body string
		replies             map[string]string
	}{
		{"tool", toolAdapter,
			`{"model":"claude-opus-5","max_tokens":16000,"system":"sys",` + messages +
				`,"tools":[{"name":"canti_decision","description":"Submit the next canti decision as tool input instead of replying with JSON text.","input_schema":` + earlierSchema + `}],` +
				`"tool_choice":{"type":"tool","name":"canti_decision","disable_parallel_tool_use":true}}`,
			map[string]string{
				"":      toolMessage(`"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{"kind":"exec","payload":"ls"}}]`),
				"check": toolMessage(`"tool_use"`, `[{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{"kind":"exec","purpose":"check","payload":"ls"}}]`),
			}},
		{"text", adapter,
			`{"model":"claude-opus-5","max_tokens":16000,"system":"sys",` + messages + `,"output_config":{"format":{"type":"json_schema","schema":` + earlierSchema + `}}}`,
			map[string]string{
				"":      message(`"end_turn"`, `[{"type":"text","text":"{\"kind\":\"exec\",\"payload\":\"ls\"}"}]`),
				"check": message(`"end_turn"`, `[{"type":"text","text":"{\"kind\":\"exec\",\"purpose\":\"check\",\"payload\":\"ls\"}"}]`),
			}},
	} {
		for purpose, answer := range tc.replies {
			t.Run(tc.name+"/"+purpose, func(t *testing.T) {
				srv, got := recorder(t, http.StatusOK, answer)
				m := newModel(t, srv.URL, func(c *anthropic.Config) { c.DecisionFormat, c.StructuredOutput = "text", true })
				req := harness.Request{Body: []byte(tc.body), Model: "claude-opus-5", Protocol: protocol, Adapter: tc.adapter}
				resp, err := m.Complete(context.Background(), req)
				if err != nil || !resp.Complete || resp.Adapter != tc.adapter {
					t.Fatalf("Complete = %+v, %v", resp, err)
				}
				if sent := (<-got).body; string(sent) != tc.body {
					t.Fatalf("sent %s, want the frozen %s", sent, tc.body)
				}
				d, err := core.Decode(m.Decision(resp))
				if err != nil || d != (core.Decision{Kind: core.Exec, Purpose: purpose, Payload: "ls"}) {
					t.Fatalf("decision = %#v, %v", d, err)
				}
			})
		}
	}
}
