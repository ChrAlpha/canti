package anthropic_test

import (
	"bytes"
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
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
)

const (
	apiKey       = "sk-ant-api-SECRET-key"
	authToken    = "sk-ant-oat-SECRET-token"
	protocol     = "anthropic-messages/2023-06-01"
	fallbackBeta = "server-side-fallback-2026-07-01"
	adapter      = "canti-anthropic/2"
	cached       = `,"cache_control":{"type":"ephemeral"}`
	purposeField = `"purpose":{"type":"string","description":"One short line: what this step is for and what you will do next with its result."}`
	schema       = `{"format":{"type":"json_schema","schema":{"type":"object","properties":{"kind":{"type":"string","enum":["exec","return"]},` + purposeField + `,"payload":{"type":"string"}},"required":["kind","payload","purpose"],"additionalProperties":false}}}`
	decision     = `[{"type":"text","text":"{\"kind\":\"return\",\"payload\":\"done\"}"}]`
)

var prompt = harness.Prompt{System: "sys", Blocks: []string{"head", "step", "tail"}, Stable: 2}

func newModel(t *testing.T, url string, edit func(*anthropic.Config)) *anthropic.Model {
	t.Helper()
	cfg := anthropic.Config{BaseURL: url, APIKey: apiKey, Model: "claude-opus-5"}
	if edit != nil {
		edit(&cfg)
	}
	m, err := anthropic.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func freeze(t *testing.T, m *anthropic.Model) harness.Request {
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
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func message(stop, content string) string {
	return `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-live","content":` + content +
		`,"stop_reason":` + stop + `,"usage":{"input_tokens":10,"output_tokens":7,"cache_creation_input_tokens":5,"cache_read_input_tokens":3}}`
}

func noSecret(t *testing.T, err error, resp harness.Response) {
	t.Helper()
	s := string(resp.Body) + resp.Model + resp.Protocol + resp.Adapter
	if err != nil {
		s += err.Error()
	}
	if strings.Contains(s, "SECRET") {
		t.Fatalf("credential leaked: %s", s)
	}
}

func stopReason(err error) core.StopReason {
	var stop *harness.StopError
	if errors.As(err, &stop) {
		return stop.Reason
	}
	return ""
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for name, cfg := range map[string]anthropic.Config{
		"no credential":    {Model: "m"},
		"both credentials": {APIKey: apiKey, AuthToken: authToken, Model: "m"},
		"no model":         {APIKey: apiKey},
		"relative url":     {APIKey: apiKey, Model: "m", BaseURL: "api.anthropic.com"},
		"cleartext remote": {APIKey: apiKey, Model: "m", BaseURL: "http://api.example.com"},
		"negative tokens":  {APIKey: apiKey, Model: "m", MaxTokens: -1},
		"negative timeout": {APIKey: apiKey, Model: "m", Timeout: -time.Second},
		"negative limit":   {APIKey: apiKey, Model: "m", MaxResponseBytes: -1},
		"unknown effort":   {APIKey: apiKey, Model: "m", Effort: "extreme"},
	} {
		if _, err := anthropic.New(cfg); err == nil {
			t.Errorf("%s: New succeeded", name)
		} else if strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%s: credential leaked: %v", name, err)
		}
	}
}

func TestFreeze(t *testing.T) {
	base := `{"model":"claude-opus-5","max_tokens":16000,"system":"sys","messages":[{"role":"user","content":[` +
		`{"type":"text","text":"head"` + cached + `},{"type":"text","text":"step"` + cached + `},{"type":"text","text":"tail"}]}]`
	for _, tc := range []struct {
		name     string
		edit     func(*anthropic.Config)
		body     string
		protocol string
	}{
		{"plain", nil, base + `}`, protocol},
		{"structured output", func(c *anthropic.Config) { c.StructuredOutput = true }, base + `,"output_config":` + schema + `}`, protocol},
		{"fallbacks", func(c *anthropic.Config) { c.Fallbacks = true }, base + `,"fallbacks":"default"}`, protocol + "+" + fallbackBeta},
		{"both", func(c *anthropic.Config) { c.StructuredOutput, c.Fallbacks = true, true }, base + `,"output_config":` + schema + `,"fallbacks":"default"}`, protocol + "+" + fallbackBeta},
		{"effort", func(c *anthropic.Config) { c.Effort = "high" }, base + `,"output_config":{"effort":"high"}}`, protocol},
		{"effort and structured output", func(c *anthropic.Config) { c.Effort, c.StructuredOutput = "xhigh", true }, base + `,"output_config":{"effort":"xhigh",` + schema[1:] + `}`, protocol},
		{"auth token and max tokens", func(c *anthropic.Config) { c.APIKey, c.AuthToken, c.MaxTokens = "", authToken, 512 }, strings.Replace(base, "16000", "512", 1) + `}`, protocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newModel(t, "https://unused.test", tc.edit)
			req := freeze(t, m)
			want := harness.Request{Body: []byte(tc.body), Model: "claude-opus-5", Protocol: tc.protocol, Adapter: adapter}
			if !reflect.DeepEqual(req, want) {
				t.Fatalf("Freeze =\n%s %s %s %s\nwant\n%s %s %s %s", req.Body, req.Model, req.Protocol, req.Adapter, want.Body, want.Model, want.Protocol, want.Adapter)
			}
			if again := freeze(t, newModel(t, "https://other.test", tc.edit)); !reflect.DeepEqual(again, req) {
				t.Fatalf("Freeze is not deterministic: %s vs %s", again.Body, req.Body)
			}
			noSecret(t, nil, harness.Response{Body: req.Body, Model: req.Model, Protocol: req.Protocol, Adapter: req.Adapter})
		})
	}
}

func TestFreezeOmitsEmptySystemAndRequiresUser(t *testing.T) {
	m := newModel(t, "https://unused.test", nil)
	req, err := m.Freeze(harness.Prompt{Blocks: []string{"hi"}, Stable: 1})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"model":"claude-opus-5","max_tokens":16000,"messages":[{"role":"user","content":[{"type":"text","text":"hi"` + cached + `}]}]}`; string(req.Body) != want {
		t.Fatalf("body = %s, want %s", req.Body, want)
	}
	for name, p := range map[string]harness.Prompt{
		"no blocks":            {System: "sys"},
		"an empty block":       {System: "sys", Blocks: []string{"head", "", "tail"}, Stable: 2},
		"only an empty block":  {Blocks: []string{""}},
		"stable beyond blocks": {Blocks: []string{"head", "tail"}, Stable: 3},
		"negative stable":      {Blocks: []string{"head"}, Stable: -1},
	} {
		if req, err := m.Freeze(p); err == nil {
			t.Errorf("%s: Freeze = %s, want an error", name, req.Body)
		}
	}
}

func TestFreezeMarksTheFirstAndLastStableBlocks(t *testing.T) {
	m := newModel(t, "https://unused.test", nil)
	for _, tc := range []struct {
		blocks []string
		stable int
		want   string
	}{
		{[]string{"head", "tail"}, 0, `{"type":"text","text":"head"},{"type":"text","text":"tail"}`},
		{[]string{"head", "tail"}, 1, `{"type":"text","text":"head"` + cached + `},{"type":"text","text":"tail"}`},
		{[]string{"head", "s1", "tail"}, 2, `{"type":"text","text":"head"` + cached + `},{"type":"text","text":"s1"` + cached + `},{"type":"text","text":"tail"}`},
		{[]string{"head", "s1", "s2", "tail"}, 3, `{"type":"text","text":"head"` + cached + `},{"type":"text","text":"s1"},{"type":"text","text":"s2"` + cached + `},{"type":"text","text":"tail"}`},
		{[]string{"only"}, 1, `{"type":"text","text":"only"` + cached + `}`},
	} {
		req, err := m.Freeze(harness.Prompt{Blocks: tc.blocks, Stable: tc.stable})
		want := `{"model":"claude-opus-5","max_tokens":16000,"messages":[{"role":"user","content":[` + tc.want + `]}]}`
		if err != nil || string(req.Body) != want {
			t.Errorf("Freeze(%q, %d stable) = %s, %v\nwant %s", tc.blocks, tc.stable, req.Body, err, want)
		}
	}

	f := harness.Frame{Goal: "g", Environment: "e", MaxSteps: 4, Attempt: 1, MaxAttempts: 1, RunID: "run_x", Window: &harness.Window{FirstStep: 1}}
	for n := 1; n <= 3; n++ {
		f.Step = n
		p, err := f.Prompt(harness.DefaultPromptLimits())
		if err != nil {
			t.Fatal(err)
		}
		req, err := m.Freeze(p)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Messages []struct {
				Role    string `json:"role"`
				Content []struct {
					Type         string          `json:"type"`
					Text         string          `json:"text"`
					CacheControl json.RawMessage `json:"cache_control"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(req.Body, &body); err != nil || len(body.Messages) != 1 || len(body.Messages[0].Content) != len(p.Blocks) {
			t.Fatalf("step %d: body %s does not hold the %d blocks in one message: %v", n, req.Body, len(p.Blocks), err)
		}
		for i, b := range body.Messages[0].Content {
			mark := ""
			if i == 0 || i == p.Stable-1 {
				mark = `{"type":"ephemeral"}`
			}
			if b.Type != "text" || b.Text != p.Blocks[i] || string(b.CacheControl) != mark {
				t.Errorf("step %d block %d of %d (%d stable) = %+v", n, i, len(p.Blocks), p.Stable, b)
			}
		}
		if c, want := bytes.Count(req.Body, []byte("cache_control")), min(2, p.Stable); c != want {
			t.Errorf("step %d: %d cache breakpoints, want %d", n, c, want)
		}
		f.Steps = append(f.Steps, harness.StepRecord{Step: n, Command: "true", Observation: harness.Observation{Execution: core.ExecutionExited}})
	}
}

type capture struct {
	method, path string
	header       http.Header
	body         []byte
}

func recorder(t *testing.T, status int, body string) (*httptest.Server, <-chan capture) {
	t.Helper()
	got := make(chan capture, 1)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- capture{r.Method, r.URL.Path, r.Header.Clone(), b}
		reply(status, body)(w, r)
	})
	return srv, got
}

func TestCompleteSendsFrozenBytesAndHeaders(t *testing.T) {
	for _, tc := range []struct {
		name      string
		edit      func(*anthropic.Config)
		key, auth string
		beta      string
	}{
		{"api key", nil, apiKey, "", ""},
		{"auth token", func(c *anthropic.Config) { c.APIKey, c.AuthToken = "", authToken }, "", "Bearer " + authToken, ""},
		{"api key with fallbacks", func(c *anthropic.Config) { c.Fallbacks = true }, apiKey, "", fallbackBeta},
		{"auth token with fallbacks", func(c *anthropic.Config) { c.APIKey, c.AuthToken, c.Fallbacks = "", authToken, true }, "", "Bearer " + authToken, fallbackBeta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, got := recorder(t, http.StatusOK, message(`"end_turn"`, decision))
			m := newModel(t, srv.URL, func(c *anthropic.Config) {
				c.StructuredOutput = true
				if tc.edit != nil {
					tc.edit(c)
				}
			})
			req, err := m.Freeze(harness.Prompt{System: "Use <tags> & \"quotes\"", Blocks: []string{"ünïcode\n\tline & <b>", "tail"}, Stable: 1})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := m.Complete(context.Background(), req)
			if err != nil || !resp.Complete {
				t.Fatalf("Complete = %v, %v", resp.Complete, err)
			}
			c := <-got
			if c.method != http.MethodPost || c.path != "/v1/messages" {
				t.Fatalf("request = %s %s", c.method, c.path)
			}
			if !bytes.Equal(c.body, req.Body) {
				t.Fatalf("wire body =\n%s\nwant frozen\n%s", c.body, req.Body)
			}
			for name, want := range map[string]string{
				"Content-Type":      "application/json",
				"Anthropic-Version": "2023-06-01",
				"X-Api-Key":         tc.key,
				"Authorization":     tc.auth,
				"Anthropic-Beta":    tc.beta,
			} {
				if got := c.header.Get(name); got != want {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
			noSecret(t, err, resp)
		})
	}
}

func TestCompleteSendsRequestBodyVerbatim(t *testing.T) {
	srv, got := recorder(t, http.StatusOK, message(`"end_turn"`, decision))
	body := []byte("{ \"messages\" : [{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"<&>\"" + cached + "},{\"type\":\"text\",\"text\":\"x\"}]}],\n\"model\":\"claude-opus-5\", \"max_tokens\":1 }\n")
	req := harness.Request{Body: body, Model: "claude-opus-5", Protocol: protocol, Adapter: adapter}
	resp, err := newModel(t, srv.URL+"/", nil).Complete(context.Background(), req)
	if err != nil || !resp.Complete || resp.Adapter != adapter {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	c := <-got
	if c.path != "/v1/messages" || !bytes.Equal(c.body, body) {
		t.Fatalf("sent %s %q, want /v1/messages %q", c.path, c.body, body)
	}
}

func TestCompleteHonorsFrozenProtocol(t *testing.T) {
	srv, got := recorder(t, http.StatusOK, message(`"end_turn"`, decision))
	req := freeze(t, newModel(t, srv.URL, func(c *anthropic.Config) { c.Fallbacks = true }))
	if _, err := newModel(t, srv.URL, nil).Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if beta := (<-got).header.Get("Anthropic-Beta"); beta != fallbackBeta {
		t.Fatalf("anthropic-beta = %q, want %q", beta, fallbackBeta)
	}

	var hits atomic.Int32
	idle := serve(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) })
	m := newModel(t, idle.URL, nil)
	for name, req := range map[string]harness.Request{
		"unknown protocol": {Body: req.Body, Model: req.Model, Protocol: "anthropic-messages/2099-01-01", Adapter: adapter},
		"unknown adapter":  {Body: req.Body, Model: req.Model, Protocol: protocol, Adapter: "other/1"},
		"earlier adapter":  {Body: req.Body, Model: req.Model, Protocol: protocol, Adapter: "canti-anthropic/1"},
		"future adapter":   {Body: req.Body, Model: req.Model, Protocol: protocol, Adapter: "canti-anthropic/3"},
		"no adapter":       {Body: req.Body, Model: req.Model, Protocol: protocol},
	} {
		if _, err := m.Complete(context.Background(), req); stopReason(err) != core.StopUnrecoverable {
			t.Errorf("%s: err = %v, want unrecoverable stop", name, err)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("sent %d foreign requests", hits.Load())
	}
}

func TestCompleteMapsStatus(t *testing.T) {
	for _, tc := range []struct {
		status    int
		typ, text string
		want      core.StopReason
	}{
		{400, "invalid_request_error", "max_tokens: must be positive", core.StopUnrecoverable},
		{400, "invalid_request_error", "Your credit balance is too low to access the Anthropic API.", core.StopBudgetExhausted},
		{401, "authentication_error", "invalid x-api-key", core.StopUnrecoverable},
		{402, "billing_error", "payment required", core.StopBudgetExhausted},
		{403, "permission_error", "forbidden", core.StopUnrecoverable},
		{404, "not_found_error", "model: claude-nope", core.StopUnrecoverable},
		{413, "request_too_large", "request exceeds the maximum size", core.StopUnrecoverable},
		{422, "invalid_request_error", "unprocessable", core.StopUnrecoverable},
		{408, "timeout_error", "request timed out", ""},
		{409, "conflict_error", "conflict", ""},
		{429, "rate_limit_error", "rate limited", ""},
		{500, "api_error", "internal error", ""},
		{502, "api_error", "bad gateway", ""},
		{503, "api_error", "unavailable", ""},
		{504, "api_error", "gateway timeout", ""},
		{529, "overloaded_error", "Overloaded", ""},
		{599, "api_error", "unknown", ""},
	} {
		t.Run(strconv.Itoa(tc.status)+" "+tc.text, func(t *testing.T) {
			body := `{"type":"error","error":{"type":"` + tc.typ + `","message":"` + tc.text + `"},"request_id":"req_42"}`
			var hits atomic.Int32
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				reply(tc.status, body)(w, r)
			})
			m := newModel(t, srv.URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete {
				t.Fatalf("Complete = %v, %v; want failure", resp.Complete, err)
			}
			if got := stopReason(err); got != tc.want {
				t.Fatalf("stop reason = %q, want %q (err %v)", got, tc.want, err)
			}
			for _, part := range []string{strconv.Itoa(tc.status), tc.typ, tc.text, "req_42"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q lacks %q", err, part)
				}
			}
			if string(resp.Body) != body || resp.Model != "claude-opus-5" || resp.Protocol != protocol || resp.Adapter != adapter || resp.Usage != (harness.Usage{}) {
				t.Errorf("response = %+v", resp)
			}
			if hits.Load() != 1 {
				t.Errorf("sent %d requests, want 1", hits.Load())
			}
			noSecret(t, err, resp)
		})
	}
}

func TestNewAllowsCleartextOnlyToLoopback(t *testing.T) {
	for _, url := range []string{"http://127.0.0.1:8080", "http://[::1]:8080", "http://localhost:8080", "https://api.example.com"} {
		if _, err := anthropic.New(anthropic.Config{APIKey: apiKey, Model: "m", BaseURL: url}); err != nil {
			t.Errorf("New(%s) = %v", url, err)
		}
	}
}

func TestCompleteRedactsAnEchoedCredential(t *testing.T) {
	for name, edit := range map[string]func(*anthropic.Config){
		"api key":    nil,
		"auth token": func(c *anthropic.Config) { c.APIKey, c.AuthToken = "", authToken },
	} {
		t.Run(name, func(t *testing.T) {
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				echoed := r.Header.Get("x-api-key") + strings.TrimPrefix(r.Header.Get("authorization"), "Bearer ")
				reply(http.StatusBadRequest, `{"type":"error","error":{"type":"invalid_request_error","message":"bad key `+echoed+`"}}`)(w, r)
			})
			m := newModel(t, srv.URL, edit)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || !strings.Contains(err.Error(), "bad key [redacted]") || !strings.Contains(string(resp.Body), "bad key [redacted]") {
				t.Fatalf("Complete = %q, %v; want the echoed credential redacted", resp.Body, err)
			}
			noSecret(t, err, resp)
		})
	}
}

func TestCompleteReportsNonJSONErrorWithHeaderRequestID(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("request-id", "req_hdr")
		reply(http.StatusBadGateway, "<html>bad gateway</html>")(w, r)
	})
	m := newModel(t, srv.URL, nil)
	_, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || stopReason(err) != "" || !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "req_hdr") {
		t.Fatalf("err = %v, want retryable 502 naming req_hdr", err)
	}
}

func TestCompleteClassifiesStopReason(t *testing.T) {
	for _, tc := range []struct {
		stop     string
		complete bool
	}{
		{`"end_turn"`, true},
		{`"stop_sequence"`, true},
		{`"refusal"`, true},
		{`"max_tokens"`, false},
		{`"pause_turn"`, false},
		{`"model_context_window_exceeded"`, false},
		{`"tool_use"`, false},
		{`"something_new"`, false},
		{`null`, false},
	} {
		t.Run(tc.stop, func(t *testing.T) {
			body := message(tc.stop, decision)
			srv := serve(t, reply(http.StatusOK, body))
			m := newModel(t, srv.URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if resp.Complete != tc.complete || (err == nil) != tc.complete {
				t.Fatalf("Complete = %v, %v; want complete %v", resp.Complete, err, tc.complete)
			}
			if stopReason(err) != "" {
				t.Fatalf("err = %v, want retryable", err)
			}
			want := harness.Response{Body: []byte(body), Complete: tc.complete, Model: "claude-opus-5-live", Protocol: protocol, Adapter: adapter, Usage: harness.Usage{InputTokens: 18, OutputTokens: 7}}
			if !reflect.DeepEqual(resp, want) {
				t.Fatalf("response = %+v\nwant %+v", resp, want)
			}
		})
	}
}

func TestCompleteDefaultsResponseModel(t *testing.T) {
	srv := serve(t, reply(http.StatusOK, `{"type":"message","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}`))
	m := newModel(t, srv.URL, nil)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || resp.Model != "claude-opus-5" || resp.Usage != (harness.Usage{InputTokens: 1, OutputTokens: 2}) {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
}

func cut(t *testing.T, raw string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = buf.WriteString(raw)
		_ = buf.Flush()
		_ = conn.Close()
	}
}

func TestCompleteTreatsTruncatedBodyAsIncomplete(t *testing.T) {
	partial := `{"id":"msg_1","type":"message","content":[{"type":"text","text":"{\"kind\"`
	trailing := message(`"end_turn"`, decision) + "x"
	for _, tc := range []struct {
		name string
		h    http.HandlerFunc
		sent string
	}{
		{"short content length", cut(t, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4096\r\n\r\n"+partial), partial},
		{"unterminated chunks", cut(t, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n"+strconv.FormatInt(int64(len(partial)), 16)+"\r\n"+partial+"\r\n"), partial},
		{"cut json", reply(http.StatusOK, partial), partial},
		{"trailing bytes", reply(http.StatusOK, trailing), trailing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newModel(t, serve(t, tc.h).URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || stopReason(err) != "" {
				t.Fatalf("Complete = %v, %v; want retryable incomplete", resp.Complete, err)
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, not a context error", err)
			}
			if string(resp.Body) != tc.sent {
				t.Fatalf("body = %q, want the bytes read %q", resp.Body, tc.sent)
			}
		})
	}
}

func TestCompleteBoundsResponseSize(t *testing.T) {
	body := message(`"end_turn"`, decision)
	srv := serve(t, reply(http.StatusOK, body))
	limit := int64(len(body))

	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = limit })
	if resp, err := m.Complete(context.Background(), freeze(t, m)); err != nil || !resp.Complete {
		t.Fatalf("at limit: Complete = %v, %v", resp.Complete, err)
	}

	m = newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = limit - 1 })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != "" {
		t.Fatalf("over limit: Complete = %v, %v; want retryable incomplete", resp.Complete, err)
	}
	if int64(len(resp.Body)) != limit-1 || !strings.HasPrefix(body, string(resp.Body)) {
		t.Fatalf("over limit: kept %d bytes, want %d", len(resp.Body), limit-1)
	}
}

func block(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	<-r.Context().Done()
}

func TestCompleteTimesOutEachAttempt(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"before headers": block,
		"during body": func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"type":"message"`)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newModel(t, serve(t, h).URL, func(c *anthropic.Config) { c.Timeout = 50 * time.Millisecond })
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || stopReason(err) != "" {
				t.Fatalf("Complete = %v, %v; want retryable timeout", resp.Complete, err)
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Fatalf("attempt timeout %v must not look like a done controller context", err)
			}
			if name == "during body" && string(resp.Body) != `{"type":"message"` {
				t.Fatalf("body = %q, want the partial bytes", resp.Body)
			}
		})
	}
}

func TestCompleteReturnsContextError(t *testing.T) {
	var hits atomic.Int32
	started := make(chan struct{}, 1)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		started <- struct{}{}
		block(w, r)
	})
	m := newModel(t, srv.URL, nil)
	req := freeze(t, m)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Complete(ctx, req); !errors.Is(err, context.Canceled) || hits.Load() != 0 {
		t.Fatalf("cancelled before: err = %v, hits = %d", err, hits.Load())
	}

	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	if _, err := m.Complete(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled during: err = %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.Complete(ctx, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("controller deadline: err = %v", err)
	}
}

func TestCompleteDoesNotFollowRedirects(t *testing.T) {
	var leaked atomic.Int32
	other := serve(t, func(http.ResponseWriter, *http.Request) { leaked.Add(1) })
	custom := &http.Client{}
	for name, client := range map[string]*http.Client{"default client": nil, "custom client": custom} {
		t.Run(name, func(t *testing.T) {
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, other.URL+"/v1/messages", http.StatusTemporaryRedirect)
			})
			m := newModel(t, srv.URL, func(c *anthropic.Config) { c.Client = client })
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if resp.Complete || stopReason(err) != core.StopUnrecoverable {
				t.Fatalf("Complete = %v, %v; want unrecoverable stop", resp.Complete, err)
			}
			noSecret(t, err, resp)
		})
	}
	if leaked.Load() != 0 {
		t.Fatalf("redirect target received %d requests", leaked.Load())
	}
	if custom.CheckRedirect != nil {
		t.Fatal("New mutated the caller's client")
	}
}

func TestDecision(t *testing.T) {
	exec := `{"kind":"exec","payload":"ls"}`
	thinkingOnly := message(`"end_turn"`, `[{"type":"thinking","thinking":"hmm","signature":"sig"}]`)
	refused := message(`"refusal"`, `[]`)
	for _, tc := range []struct {
		name, body, want string
	}{
		{"text", message(`"end_turn"`, `[{"type":"text","text":"{\"kind\":\"exec\",\"payload\":\"ls\"}"}]`), exec},
		{"thinking then text", message(`"end_turn"`, `[{"type":"thinking","thinking":"{\"kind\":\"return\",\"payload\":\"no\"}","signature":"sig"},{"type":"text","text":"{\"kind\":\"exec\",\"payload\":\"ls\"}"}]`), exec},
		{"multiple text blocks", message(`"end_turn"`, `[{"type":"text","text":"{\"kind\":\"exec\","},{"type":"thinking","thinking":"x","signature":"s"},{"type":"text","text":"\"payload\":\"ls\"}"}]`), exec},
		{"no text", thinkingOnly, thinkingOnly},
		{"refusal without content", refused, refused},
		{"not json", "<html>oops</html>", "<html>oops</html>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := newModel(t, "https://unused.test", nil).Decision(harness.Response{Body: []byte(tc.body), Complete: true})
			if string(got) != tc.want {
				t.Fatalf("Decision = %s, want %s", got, tc.want)
			}
			if _, err := core.Decode(got); (err == nil) != (tc.want == exec) {
				t.Fatalf("core.Decode(%s) = %v", got, err)
			}
		})
	}
}
