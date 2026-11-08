package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const apiKey = "sk-proj-SECRET-key"

var prompt = harness.Prompt{System: "sys", Blocks: []string{"head", "step", "tail"}, Stable: 2}

func newModel(t *testing.T, url string, edit func(*Config)) *Model {
	t.Helper()
	cfg := Config{BaseURL: url, APIKey: apiKey, Model: "test-model"}
	if edit != nil {
		edit(&cfg)
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func freeze(t *testing.T, m *Model) harness.Request {
	t.Helper()
	req, err := m.Freeze(prompt)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func reply(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		w.Header().Set("x-request-id", "req_hdr")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func respond(status, output string) string {
	return `{"id":"resp_1","object":"response","model":"test-model-2026","status":` + status + `,"output":` + output +
		`,"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":4},"output_tokens":7,"output_tokens_details":{"reasoning_tokens":3},"total_tokens":17}}`
}

const textOutput = `[{"type":"reasoning","id":"rs_1","summary":[]},{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":` +
	`[{"type":"output_text","text":"{\"kind\":\"return\",","annotations":[]},{"type":"output_text","text":"\"payload\":\"done\"}","annotations":[]}]}]`

func call(name, arguments string) string {
	args, _ := json.Marshal(arguments)
	return `{"type":"function_call","id":"fc_1","call_id":"call_1","name":"` + name + `","arguments":` + string(args) + `,"status":"completed"}`
}

func stopReason(err error) core.StopReason {
	var stop *harness.StopError
	if errors.As(err, &stop) {
		return stop.Reason
	}
	return ""
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no credential":    {Model: "m"},
		"no model":         {APIKey: apiKey},
		"relative url":     {APIKey: apiKey, Model: "m", BaseURL: "api.openai.com/v1"},
		"cleartext remote": {APIKey: apiKey, Model: "m", BaseURL: "http://api.example.com/v1"},
		"negative tokens":  {APIKey: apiKey, Model: "m", MaxTokens: -1},
		"unknown effort":   {APIKey: apiKey, Model: "m", Effort: "extreme"},
		"unknown format":   {APIKey: apiKey, Model: "m", DecisionFormat: "json"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New succeeded", name)
		} else if strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%s: credential leaked: %v", name, err)
		}
	}
}

func TestFreeze(t *testing.T) {
	base := `{"model":"test-model","instructions":"sys","input":[{"role":"user","content":[{"type":"input_text","text":"head"},` +
		`{"type":"input_text","text":"step"},{"type":"input_text","text":"tail"}]}],"max_output_tokens":16000`
	for _, tc := range []struct {
		name    string
		edit    func(*Config)
		body    string
		adapter string
	}{
		{"plain", nil, base + `,"truncation":"disabled","store":false}`, adapter},
		{"structured output", func(c *Config) { c.StructuredOutput = true }, base + `,"text":` + decisionFormat + `,"truncation":"disabled","store":false}`, adapter},
		{"effort and max tokens", func(c *Config) { c.Effort, c.MaxTokens = "xhigh", 512 },
			strings.Replace(base, "16000", "512", 1) + `,"reasoning":{"effort":"xhigh"},"truncation":"disabled","store":false}`, adapter},
		{"tool", func(c *Config) { c.DecisionFormat, c.StructuredOutput, c.Effort = "tool", true, "high" },
			base + `,"reasoning":{"effort":"high"},"tools":` + decisionTool + `,"tool_choice":"auto","parallel_tool_calls":false,"truncation":"disabled","store":false}`, toolAdapter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := freeze(t, newModel(t, "https://unused.test/v1", tc.edit))
			want := harness.Request{Body: []byte(tc.body), Model: "test-model", Protocol: protocol, Adapter: tc.adapter}
			if !reflect.DeepEqual(req, want) {
				t.Fatalf("Freeze =\n%s %s\nwant\n%s %s", req.Body, req.Adapter, want.Body, want.Adapter)
			}
			if !json.Valid(req.Body) || strings.Contains(string(req.Body), "SECRET") {
				t.Fatalf("body %s", req.Body)
			}
		})
	}
}

func TestFreezeSchemasAreStrictAndOrdered(t *testing.T) {
	var text struct {
		Format struct {
			Type, Name string
			Strict     bool
			Schema     json.RawMessage
		}
	}
	if err := json.Unmarshal([]byte(decisionFormat), &text); err != nil || text.Format.Type != "json_schema" || text.Format.Name != "canti_decision" || !text.Format.Strict ||
		string(text.Format.Schema) != harness.DecisionSchema {
		t.Fatalf("text format = %+v, %v", text, err)
	}
	var tools []struct {
		Type, Name, Description string
		Strict                  bool
		Parameters              json.RawMessage
	}
	if err := json.Unmarshal([]byte(decisionTool), &tools); err != nil || len(tools) != 1 || tools[0].Type != "function" || tools[0].Name != "canti_decision" ||
		!tools[0].Strict || string(tools[0].Parameters) != harness.DecisionSchema {
		t.Fatalf("tools = %+v, %v", tools, err)
	}
}

func TestFreezeRejectsAnEmptyPrompt(t *testing.T) {
	m := newModel(t, "https://unused.test/v1", nil)
	for _, p := range []harness.Prompt{{}, {Blocks: []string{"a", ""}}, {Blocks: []string{"a"}, Stable: 2}} {
		if _, err := m.Freeze(p); err == nil {
			t.Errorf("Freeze(%+v) succeeded", p)
		}
	}
	req, err := m.Freeze(harness.Prompt{Blocks: []string{"user"}})
	if err != nil || strings.Contains(string(req.Body), "instructions") {
		t.Fatalf("Freeze without system = %s, %v", req.Body, err)
	}
}

func TestCompleteSendsFrozenBytesOnceWithBearer(t *testing.T) {
	var hits atomic.Int32
	var got *http.Request
	var body []byte
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		got, body = r, nil
		body, _ = io.ReadAll(r.Body)
		reply(http.StatusOK, respond(`"completed"`, textOutput))(w, r)
	})
	m := newModel(t, srv.URL+"/v1/", nil)
	req := freeze(t, m)
	resp, err := m.Complete(context.Background(), req)
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if hits.Load() != 1 || got.URL.Path != "/v1/responses" || got.Method != http.MethodPost || string(body) != string(req.Body) {
		t.Fatalf("request %s %s sent %d times: %s", got.Method, got.URL.Path, hits.Load(), body)
	}
	if got.Header.Get("Authorization") != "Bearer "+apiKey || got.Header.Get("Content-Type") != "application/json" ||
		got.Header.Get("x-api-key") != "" || got.Header.Get("anthropic-version") != "" {
		t.Fatalf("headers = %v", got.Header)
	}
	if resp.Model != "test-model-2026" || resp.Usage != (harness.Usage{InputTokens: 10, OutputTokens: 7, InputReported: true, OutputReported: true}) || resp.Protocol != protocol || resp.Adapter != adapter {
		t.Fatalf("response = %+v", resp)
	}
	if d := string(m.Decision(resp)); d != `{"kind":"return","payload":"done"}` {
		t.Fatalf("Decision = %s", d)
	}
}

func TestCompleteMapsStatus(t *testing.T) {
	for _, tc := range []struct {
		status    int
		code, msg string
		want      core.StopReason
	}{
		{400, "invalid_value", "bad max_output_tokens", core.StopUnrecoverable},
		{401, "invalid_api_key", "Incorrect API key", core.StopUnrecoverable},
		{402, "", "payment required", core.StopBudgetExhausted},
		{404, "model_not_found", "no such model", core.StopUnrecoverable},
		{408, "", "timeout", ""},
		{409, "", "conflict", ""},
		{429, "rate_limit_exceeded", "slow down", ""},
		{429, "insufficient_quota", "You exceeded your current quota", core.StopBudgetExhausted},
		{500, "server_error", "internal", ""},
		{503, "", "unavailable", ""},
	} {
		t.Run(strconv.Itoa(tc.status)+" "+tc.code, func(t *testing.T) {
			body := `{"error":{"message":"` + tc.msg + `","type":"invalid_request_error","param":null,"code":` + strconv.Quote(tc.code) + `}}`
			m := newModel(t, serve(t, reply(tc.status, body)).URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || string(resp.Body) != body {
				t.Fatalf("Complete = %+v, %v; want failure", resp, err)
			}
			if got := stopReason(err); got != tc.want {
				t.Fatalf("stop reason = %q, want %q (err %v)", got, tc.want, err)
			}
			for _, part := range []string{strconv.Itoa(tc.status), tc.msg, "req_hdr"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q lacks %q", err, part)
				}
			}
		})
	}
}

func TestCompleteRequiresACompletedStatus(t *testing.T) {
	for _, tc := range []struct{ name, body, text string }{
		{"max output tokens", `{"model":"m","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[],"usage":{"input_tokens":1,"output_tokens":2}}`, "max_output_tokens"},
		{"content filter", `{"model":"m","status":"incomplete","incomplete_details":{"reason":"content_filter"},"output":[]}`, "content_filter"},
		{"failed", `{"model":"m","status":"failed","error":{"code":"server_error","message":"boom"},"output":[]}`, "server_error"},
		{"in progress", `{"model":"m","status":"in_progress","output":[]}`, "in_progress"},
		{"duplicate status", `{"model":"m","status":"completed","status":"failed","output":[]}`, "malformed"},
		{"not json", `{"model":`, "malformed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newModel(t, serve(t, reply(http.StatusOK, tc.body)).URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || stopReason(err) != "" || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("Complete = %+v, %v; want a retryable incomplete response naming %q", resp.Complete, err, tc.text)
			}
		})
	}
}

func TestCompleteRedactsAnEchoedCredential(t *testing.T) {
	body := `{"error":{"message":"bad key ` + apiKey + `","code":"invalid_api_key"}}`
	m := newModel(t, serve(t, reply(http.StatusUnauthorized, body)).URL, nil)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || strings.Contains(err.Error()+string(resp.Body), "SECRET") || !strings.Contains(string(resp.Body), "bad key [redacted]") {
		t.Fatalf("Complete = %s, %v", resp.Body, err)
	}
}

func TestCompleteRefusesAnotherProtocolOrAdapter(t *testing.T) {
	m := newModel(t, "https://unused.test/v1", nil)
	for _, req := range []harness.Request{
		{Body: []byte(`{}`), Protocol: "anthropic-messages/2023-06-01", Adapter: adapter},
		{Body: []byte(`{}`), Protocol: protocol, Adapter: "canti-anthropic/4"},
	} {
		if _, err := m.Complete(context.Background(), req); stopReason(err) != core.StopUnrecoverable {
			t.Errorf("Complete(%s %s) = %v, want unrecoverable", req.Protocol, req.Adapter, err)
		}
	}
}

func TestDecisionWithoutTextReturnsTheBody(t *testing.T) {
	m := newModel(t, "https://unused.test/v1", nil)
	refusal := respond(`"completed"`, `[{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"I can't help with that."}]}]`)
	if got := string(m.Decision(harness.Response{Body: []byte(refusal), Complete: true, Adapter: adapter})); got != refusal {
		t.Fatalf("Decision = %s", got)
	}
	if got := m.Decision(harness.Response{Body: []byte(refusal), Complete: true, Adapter: "canti-anthropic/4"}); got != nil {
		t.Fatalf("Decision of another adapter = %s", got)
	}
}

func TestToolDecision(t *testing.T) {
	args := `{"kind":"exec","purpose":"Look.","payload":"ls"}`
	reasoning := `{"type":"reasoning","id":"rs_1","summary":[]}`
	note := `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Listing."}]}`
	for _, tc := range []struct {
		name, output string
		complete     bool
		want         string
	}{
		{"single call", `[` + call("canti_decision", args) + `]`, true, args},
		{"with reasoning and a note", `[` + reasoning + `,` + note + `,` + call("canti_decision", args) + `]`, true, args},
		{"duplicate fields reach core", `[` + call("canti_decision", `{"kind":"return","kind":"exec","payload":"x"}`) + `]`, true, `{"kind":"return","kind":"exec","payload":"x"}`},
		{"text only", `[` + note + `]`, true, ""},
		{"two calls", `[` + call("canti_decision", args) + `,` + call("canti_decision", args) + `]`, true, ""},
		{"another tool", `[` + call("shell", args) + `]`, true, ""},
		{"empty arguments", `[` + call("canti_decision", "") + `]`, true, ""},
		{"unknown item", `[{"type":"web_search_call","id":"ws_1"},` + call("canti_decision", args) + `]`, true, ""},
		{"incomplete", `[` + call("canti_decision", args) + `]`, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newModel(t, "https://unused.test/v1", func(c *Config) { c.DecisionFormat = "tool" })
			got := m.Decision(harness.Response{Body: []byte(respond(`"completed"`, tc.output)), Complete: tc.complete, Adapter: toolAdapter})
			if string(got) != tc.want {
				t.Fatalf("Decision = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConfirmedContextLimitSignals(t *testing.T) {
	for _, format := range []string{"text", "tool"} {
		for _, tc := range []struct {
			name     string
			status   int
			body     string
			overflow bool
		}{
			{"http", 400, `{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"too large"}}`, true},
			{"failed", 200, `{"status":"failed","error":{"code":"context_length_exceeded","message":"too large"},"usage":{"input_tokens":12,"output_tokens":3}}`, true},
			{"incomplete", 200, `{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":12,"output_tokens":3}}`, false},
			{"unstructured", 400, `{"message":"context_length_exceeded"}`, false},
			{"generic", 400, `{"error":{"code":"invalid_value","message":"context_length_exceeded"}}`, false},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				srv := serve(t, reply(tc.status, tc.body))
				m := newModel(t, srv.URL, func(c *Config) { c.DecisionFormat = format })
				resp, err := m.Complete(context.Background(), freeze(t, m))
				var limit *harness.ContextLimitError
				if errors.As(err, &limit) != tc.overflow || err == nil || resp.Complete || string(resp.Body) != tc.body {
					t.Fatalf("response=%+v error=%v", resp, err)
				}
				if tc.name == "failed" && resp.Usage.InputTokens != 12 {
					t.Fatalf("lost partial usage: %+v", resp.Usage)
				}
			})
		}
	}
}

func TestUsagePresenceAndNumericContract(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		want        harness.Usage
		invalid     bool
	}{
		{"missing", `null`, harness.Usage{}, false},
		{"zero", `{"input_tokens":0,"output_tokens":0}`, harness.Usage{InputReported: true, OutputReported: true}, false},
		{"partial", `{"output_tokens":2}`, harness.Usage{OutputTokens: 2, OutputReported: true}, false},
		{"negative", `{"input_tokens":-1}`, harness.Usage{}, true},
		{"fractional", `{"output_tokens":0.5}`, harness.Usage{}, true},
		{"integer overflow", `{"input_tokens":9223372036854775808}`, harness.Usage{}, true},
		{"sum overflow", `{"input_tokens":9223372036854775807,"output_tokens":1}`, harness.Usage{}, true},
		{"duplicate", `{"input_tokens":1,"input_tokens":2}`, harness.Usage{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"status":"completed","usage":` + tc.usage + `}`
			srv := serve(t, reply(200, body))
			m := newModel(t, srv.URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			var stop *harness.StopError
			if errors.As(err, &stop) != tc.invalid || resp.Usage != tc.want || string(resp.Body) != body || resp.Complete == tc.invalid {
				t.Fatalf("usage=%+v complete=%v error=%v", resp.Usage, resp.Complete, err)
			}
		})
	}
}

func TestContextLimitClassificationRequiresExactUniqueDiscriminators(t *testing.T) {
	for _, format := range []string{"text", "tool"} {
		for _, tc := range []struct {
			name   string
			status int
			body   string
		}{
			{"http200-duplicate-code-forward", 200, `{"status":"failed","error":{"code":"server_error","code":"context_length_exceeded"}}`},
			{"http200-duplicate-code-reverse", 200, `{"status":"failed","error":{"code":"context_length_exceeded","code":"server_error"}}`},
			{"http200-duplicate-overflow-code", 200, `{"status":"failed","error":{"code":"context_length_exceeded","code":"context_length_exceeded"}}`},
			{"http200-case-code-override-forward", 200, `{"status":"failed","error":{"code":"server_error","Code":"context_length_exceeded"}}`},
			{"http200-case-code-override-reverse", 200, `{"status":"failed","error":{"Code":"context_length_exceeded","code":"server_error"}}`},
			{"http200-uppercase-code-only", 200, `{"status":"failed","error":{"Code":"context_length_exceeded"}}`},
			{"http200-case-error-override-forward", 200, `{"status":"failed","error":{"code":"server_error"},"Error":{"code":"context_length_exceeded"}}`},
			{"http200-case-error-override-reverse", 200, `{"status":"failed","Error":{"code":"context_length_exceeded"},"error":{"code":"server_error"}}`},
			{"http200-uppercase-error-only", 200, `{"status":"failed","Error":{"code":"context_length_exceeded"}}`},
			{"http200-uppercase-error-and-code", 200, `{"status":"failed","Error":{"Code":"context_length_exceeded"}}`},
			{"http200-duplicate-error-forward", 200, `{"status":"failed","error":{"code":"server_error"},"error":{"code":"context_length_exceeded"}}`},
			{"http200-duplicate-error-reverse", 200, `{"status":"failed","error":{"code":"context_length_exceeded"},"error":{"code":"server_error"}}`},
			{"http400-duplicate-code-forward", 400, `{"error":{"code":"server_error","code":"context_length_exceeded"}}`},
			{"http400-duplicate-code-reverse", 400, `{"error":{"code":"context_length_exceeded","code":"server_error"}}`},
			{"http400-duplicate-overflow-code", 400, `{"error":{"code":"context_length_exceeded","code":"context_length_exceeded"}}`},
			{"http400-case-code-override-forward", 400, `{"error":{"code":"server_error","Code":"context_length_exceeded"}}`},
			{"http400-case-code-override-reverse", 400, `{"error":{"Code":"context_length_exceeded","code":"server_error"}}`},
			{"http400-uppercase-code-only", 400, `{"error":{"Code":"context_length_exceeded"}}`},
			{"http400-case-error-override-forward", 400, `{"error":{"code":"server_error"},"Error":{"code":"context_length_exceeded"}}`},
			{"http400-case-error-override-reverse", 400, `{"Error":{"code":"context_length_exceeded"},"error":{"code":"server_error"}}`},
			{"http400-uppercase-error-only", 400, `{"Error":{"code":"context_length_exceeded"}}`},
			{"http400-uppercase-error-and-code", 400, `{"Error":{"Code":"context_length_exceeded"}}`},
			{"http400-duplicate-error-forward", 400, `{"error":{"code":"server_error"},"error":{"code":"context_length_exceeded"}}`},
			{"http400-duplicate-error-reverse", 400, `{"error":{"code":"context_length_exceeded"},"error":{"code":"server_error"}}`},
			{"wrong-status-413", 413, `{"error":{"code":"context_length_exceeded"}}`},
			{"wrong-status-429", 429, `{"error":{"code":"context_length_exceeded"}}`},
			{"wrong-status-500", 500, `{"error":{"code":"context_length_exceeded"}}`},
			{"nonfailed-completed", 200, `{"status":"completed","error":{"code":"context_length_exceeded"}}`},
			{"nonfailed-incomplete", 200, `{"status":"incomplete","error":{"code":"context_length_exceeded"}}`},
			{"nonfailed-in_progress", 200, `{"status":"in_progress","error":{"code":"context_length_exceeded"}}`},
			{"nonfailed-cancelled", 200, `{"status":"cancelled","error":{"code":"context_length_exceeded"}}`},
			{"uppercase-status", 200, `{"Status":"failed","error":{"code":"context_length_exceeded"}}`},
			{"case-status-override-forward", 200, `{"status":"incomplete","Status":"failed","error":{"code":"context_length_exceeded"}}`},
			{"case-status-override-reverse", 200, `{"Status":"failed","status":"incomplete","error":{"code":"context_length_exceeded"}}`},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				m := newModel(t, serve(t, reply(tc.status, tc.body)).URL, func(c *Config) { c.DecisionFormat = format })
				resp, err := m.Complete(context.Background(), freeze(t, m))
				var limit *harness.ContextLimitError
				if errors.As(err, &limit) {
					t.Errorf("ambiguous/noncanonical envelope established overflow: %+v; body=%s", limit, tc.body)
				}
				if string(resp.Body) != tc.body {
					t.Errorf("lost original response: got %s want %s", resp.Body, tc.body)
				}
			})
		}
	}
}
