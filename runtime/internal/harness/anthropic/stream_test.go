package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

func streamServer(t *testing.T, body string) *anthropic.Model {
	t.Helper()
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.Header().Set("request-id", "req_stream")
		_, _ = io.WriteString(w, body)
	})
	return newModel(t, srv.URL, nil)
}

func event(typ, data string) string { return "event: " + typ + "\ndata: " + data + "\n\n" }

func decisionFor(t *testing.T, m *anthropic.Model, resp harness.Response) []byte {
	t.Helper()
	if resp.Extraction == nil {
		extraction := anthropic.ExtractForTest(m, resp)
		resp.Extraction = &extraction
	}
	if resp.Adapter != "" && resp.Adapter != adapter && resp.Adapter != "canti-anthropic/5-tool" {
		return nil
	}
	if got := m.Decision(resp); got != nil || resp.Extraction == nil || resp.Extraction.Decision != nil {
		return got
	}
	if resp.Adapter == "canti-anthropic/5-tool" && !resp.Extraction.Found {
		return nil
	}
	return resp.Extraction.Body
}

func TestCompleteAssemblesTheFinalMessageFromAStream(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_stream","type":"message","role":"assistant","model":"claude-opus-5-live","content":[],"stop_reason":null,"usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":5,"cache_read_input_tokens":3}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Plan."}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"{\"kind\":\"return\","}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"\"payload\":\"done\"}"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":1}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":7}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if resp.Model != "claude-opus-5-live" || resp.Usage != (harness.Usage{InputTokens: 18, OutputTokens: 7, InputReported: true, OutputReported: true}) {
		t.Fatalf("response = %+v", resp)
	}
	if got := string(decisionFor(t, m, resp)); got != `{"kind":"return","payload":"done"}` {
		t.Fatalf("Decision = %s", got)
	}
	var message struct {
		Content []struct {
			Type      string `json:"type"`
			Thinking  string `json:"thinking"`
			Signature string `json:"signature"`
			Text      string `json:"text"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(resp.Body, &message); err != nil || len(message.Content) != 2 || message.Content[0].Thinking != "Plan." || message.Content[0].Signature != "sig" ||
		message.Content[1].Text != `{"kind":"return","payload":"done"}` || message.StopReason != "end_turn" ||
		message.Usage.InputTokens != 10 || message.Usage.OutputTokens != 7 {
		t.Fatalf("assembled body = %s (%+v, %v)", resp.Body, message, err)
	}
}

func TestCompleteAssemblesToolInputFromDeltas(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_tool","type":"message","role":"assistant","model":"claude-opus-5","content":[],"stop_reason":null,"usage":{"input_tokens":4,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"canti_decision","input":{}}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"kind\":\"exec\","}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"payload\":\"ls\"}"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":6}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	req := freeze(t, newModel(t, "https://unused.test", func(c *anthropic.Config) { c.DecisionFormat = "tool" }))
	req.Adapter = toolAdapter
	resp, err := m.Complete(context.Background(), req)
	if err != nil || !resp.Complete || string(decisionFor(t, m, resp)) != `{"kind":"exec","payload":"ls"}` {
		t.Fatalf("Complete = %+v, %v; Decision = %s", resp, err, decisionFor(t, m, resp))
	}
}

func TestCompleteMapsAStreamErrorEvent(t *testing.T) {
	for _, tc := range []struct {
		typ, message string
		status       int
		want         core.StopReason
		overflow     bool
	}{
		{"overloaded_error", "Overloaded", 529, "", false},
		{"rate_limit_error", "slow down", 429, "", false},
		{"api_error", "internal", 500, "", false},
		{"authentication_error", "bad key", 401, core.StopUnrecoverable, false},
		{"invalid_request_error", "prompt is too long: 200001 tokens > 200000 maximum", 400, core.StopUnrecoverable, false},
		{"billing_error", "credit balance is too low", 402, core.StopBudgetExhausted, false},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			data := `{"type":"error","error":{"type":"` + tc.typ + `","message":"` + tc.message + `"}}`
			m := streamServer(t, event("error", data))
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete {
				t.Fatalf("Complete = %v, %v", resp.Complete, err)
			}
			var limit *harness.ContextLimitError
			if errors.As(err, &limit) != tc.overflow || stopReason(err) != tc.want {
				t.Fatalf("error = %v, stop = %q, overflow = %v", err, stopReason(err), errors.As(err, &limit))
			}
			for _, part := range []string{tc.typ, "req_stream"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q lacks %q", err, part)
				}
			}
			if !tc.overflow && !strings.Contains(err.Error(), tc.message) {
				t.Errorf("error %q lacks %q", err, tc.message)
			}
		})
	}
}

func TestCompleteIgnoresCommentsAndKeepAlives(t *testing.T) {
	body := ": keep-alive\n\n" + event("ping", `{"type":"ping"}`) + streamReply(message(`"end_turn"`, decision))
	m := streamServer(t, body)
	if resp, err := m.Complete(context.Background(), freeze(t, m)); err != nil || !resp.Complete {
		t.Fatalf("Complete = %v, %v", resp.Complete, err)
	}
}

func TestCompleteRejectsAnOversizedStreamEvent(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("message_start", `{"type":"message_start","message":{"model":"m","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1},"padding":"`+strings.Repeat("x", 70<<10)+`"}}`))
	})
	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = 32 })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != "" || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Complete = %v, %v; want an oversized-event retry", resp.Complete, err)
	}
}

func TestCompleteRequiresAnEventStream(t *testing.T) {
	srv := serve(t, reply(http.StatusOK, message(`"end_turn"`, decision)))
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, message(`"end_turn"`, decision))
	})
	m := newModel(t, srv.URL, nil)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != "" || !strings.Contains(err.Error(), "not text/event-stream") {
		t.Fatalf("Complete = %v, %v", resp.Complete, err)
	}
}

func TestCompleteReadsASlowLiveStream(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("message_start", `{"type":"message_start","message":{"id":"msg_slow","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`))
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
		for range 8 {
			time.Sleep(20 * time.Millisecond)
			_, _ = io.WriteString(w, event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`))
			w.(http.Flusher).Flush()
		}
		_, _ = io.WriteString(w, event("content_block_stop", `{"type":"content_block_stop","index":0}`))
		_, _ = io.WriteString(w, event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":9}}`))
		_, _ = io.WriteString(w, event("message_stop", `{"type":"message_stop"}`))
	})
	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.IdleTimeout = 60 * time.Millisecond })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || resp.Usage.OutputTokens != 9 {
		t.Fatalf("Complete = %+v, %v; a live stream must outlast the idle timeout", resp, err)
	}
}

func TestCompleteRejectsUnknownStreamFraming(t *testing.T) {
	for name, body := range map[string]string{
		"unknown event":   event("mystery", `{"type":"mystery"}`),
		"name mismatch":   event("message_start", `{"type":"message_stop"}`),
		"malformed event": "event: message_start\ndata: {\"type\":\n\n",
		"missing stop":    event("message_start", `{"type":"message_start","message":{"model":"m","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) + event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`),
		"unknown delta":   event("message_start", `{"type":"message_start","message":{"model":"m","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) + event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"mystery"}}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, body)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || stopReason(err) != "" {
				t.Fatalf("Complete = %v, %v; want a retryable malformed stream", resp.Complete, err)
			}
		})
	}
}

func statusStreamServer(t *testing.T, status int, body string, edit func(*anthropic.Config)) *anthropic.Model {
	t.Helper()
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.Header().Set("request-id", "req_stream")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	return newModel(t, srv.URL, edit)
}

func textStream(usage, text, stop string) string {
	return event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":`+usage+`}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(text)+`}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":`+stop+`},"usage":{"output_tokens":1}}`) +
		event("message_stop", `{"type":"message_stop"}`)
}

func TestCompleteRedactsTheAssembledBodyAndEvents(t *testing.T) {
	split := len(apiKey) / 2
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(apiKey[:split])+`}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(apiKey[split:])+`}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(resp.Body), "SECRET") || !strings.Contains(string(resp.Body), "[redacted]") {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if got := string(decisionFor(t, m, resp)); strings.Contains(got, "SECRET") || !strings.Contains(got, "[redacted]") {
		t.Fatalf("Decision = %q", got)
	}

	for name, tc := range map[string]string{
		"error event":   event("error", `{"type":"error","error":{"type":"invalid_request_error","message":"bad key `+apiKey+`"}}`),
		"success body":  textStream(`{"input_tokens":1,"output_tokens":1}`, "key "+apiKey, `"end_turn"`),
		"partial split": event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) + event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(apiKey[:split])+`}}`) + event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(apiKey[split:])+`}}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, tc)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			joined := string(resp.Body)
			if err != nil {
				joined += err.Error()
			}
			if strings.Contains(joined, "SECRET") {
				t.Fatalf("credential escaped: response=%q error=%v", resp.Body, err)
			}
			if name == "success body" {
				if err != nil || !resp.Complete {
					t.Fatalf("successful redacted stream = %v, %v", resp.Complete, err)
				}
			} else if err == nil {
				t.Fatalf("%s did not fail", name)
			}
		})
	}
}

func TestCompleteMergesMultipleMessageDeltas(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":2}}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || resp.Usage.InputTokens != 3 || resp.Usage.OutputTokens != 7 || !resp.Usage.OutputReported {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	var message struct {
		StopReason string `json:"stop_reason"`
	}
	if json.Unmarshal(resp.Body, &message) != nil || message.StopReason != "end_turn" {
		t.Fatalf("final stop reason = %s", resp.Body)
	}
}

func TestCompleteKeepsOutputUnknownWithoutFinalUsage(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":5,"cache_creation_input_tokens":2,"cache_read_input_tokens":3,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || resp.Usage.InputTokens != 10 || resp.Usage.OutputReported {
		t.Fatalf("Complete = %+v, %v; output must stay unknown", resp, err)
	}
}

func TestCompleteKeepsObservedUsageThroughAnInStreamError(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":10,"cache_read_input_tokens":4,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}`) +
		event("message_delta", `{"type":"message_delta","delta":{},"usage":{"output_tokens":5}}`) +
		event("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || resp.Usage.InputTokens != 14 || resp.Usage.OutputTokens != 5 || !resp.Usage.OutputReported {
		t.Fatalf("usage lost through an error event: %+v, %v", resp, err)
	}
}

func TestCompleteEnforcesStrictBlockStateAndTypes(t *testing.T) {
	start := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`)
	for name, body := range map[string]string{
		"delta after stop":         start + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) + event("content_block_stop", `{"type":"content_block_stop","index":0}`) + event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`),
		"delta on unknown block":   start + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"mystery"}}`) + event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`),
		"null index":               start + event("content_block_start", `{"type":"content_block_start","index":null,"content_block":{"type":"text","text":""}}`),
		"null text delta":          start + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) + event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":null}}`),
		"null thinking start":      start + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":null}}`),
		"case variant block field": start + event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","Text":""}}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, body)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || stopReason(err) != "" {
				t.Fatalf("Complete = %v, %v; want a retryable malformed stream", resp.Complete, err)
			}
		})
	}
}

func TestCompletePreservesMessageAndDeltaExtensions(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","container":{"id":"c"},"x_extra":"keep","content":[{"type":"thinking","thinking":"","signature":"","x_block":"keep"}],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":""}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"refusal","stop_details":{"type":"refusal"}},"x_event":"keep","usage":{"output_tokens":2}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	var message struct {
		Container map[string]string        `json:"container"`
		Extra     string                   `json:"x_extra"`
		Event     string                   `json:"x_event"`
		Stop      map[string]string        `json:"stop_details"`
		Content   []map[string]interface{} `json:"content"`
	}
	if err := json.Unmarshal(resp.Body, &message); err != nil || message.Container["id"] != "c" || message.Extra != "keep" || message.Event != "keep" || message.Stop["type"] != "refusal" ||
		len(message.Content) != 1 || message.Content[0]["thinking"] != "" || message.Content[0]["x_block"] != "keep" {
		t.Fatalf("assembled object lost extensions: %s (%v)", resp.Body, err)
	}
}

func TestCompleteDiscardsContentBeforeAMessagesFallback(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"declined-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"declined prefix"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"fallback","to":{"model":"fallback-model"}}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":1}`) +
		event("content_block_start", `{"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"served answer"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":2}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || resp.Model != "fallback-model" || string(decisionFor(t, m, resp)) != "served answer" || strings.Contains(string(resp.Body), "declined prefix") {
		t.Fatalf("fallback boundary = %+v, %v", resp, err)
	}
}

func TestCompleteRejectsASecondMessageStart(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"declined-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("message_start", `{"type":"message_start","message":{"id":"msg_2","type":"message","role":"assistant","model":"fallback-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || !strings.Contains(err.Error(), "duplicate message_start") {
		t.Fatalf("Complete = %v, %v", resp.Complete, err)
	}
}

func TestCompleteRequiresExactStatusForContextStopReason(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"model_context_window_exceeded"}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := statusStreamServer(t, http.StatusCreated, body, nil)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	var limit *harness.ContextLimitError
	if err == nil || resp.Complete || errors.As(err, &limit) {
		t.Fatalf("Complete = %+v, %v; a non-200 stream must not confirm overflow", resp, err)
	}
}

func TestCompleteEnforcesTheAssembledLimitIncrementally(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("message_start", `{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`))
		_, _ = io.WriteString(w, event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
		_, _ = io.WriteString(w, event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"`+strings.Repeat("x", 400)+`"}}`))
		_, _ = io.WriteString(w, event("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`))
	})
	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = 200 })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || !strings.Contains(err.Error(), "response exceeds") || strings.Contains(err.Error(), "Overloaded") {
		t.Fatalf("Complete = %v, %v; the size limit must win", resp.Complete, err)
	}
	if int64(len(resp.Body)) > 200 {
		t.Fatalf("partial body %d bytes exceeds the limit", len(resp.Body))
	}
}

func TestCompleteMapsTheHTTP400FixtureThroughTheStreamPath(t *testing.T) {
	data := `{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: must be positive"},"request_id":"req_http"}`
	m := streamServer(t, event("error", data))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != core.StopUnrecoverable || !strings.Contains(err.Error(), "max_tokens: must be positive") {
		t.Fatalf("Complete = %v, %v", resp.Complete, err)
	}
}

func TestCompleteRedactsDecodedAndEventNameEchoes(t *testing.T) {
	escaped := `\u0073` + apiKey[1:]
	errorBody := `{"type":"error","error":{"type":"invalid_request_error","message":"bad key ` + escaped + `"}}`
	m := streamServer(t, event("error", errorBody))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || strings.Contains(string(resp.Body)+err.Error(), apiKey) || strings.Contains(string(resp.Body)+err.Error(), `\u0073`+apiKey[1:]) {
		t.Fatalf("decoded error echo leaked: response=%q error=%v", resp.Body, err)
	}

	success := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"`+escaped+`"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m = streamServer(t, success)
	resp, err = m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(resp.Body), apiKey) || strings.Contains(string(decisionFor(t, m, resp)), apiKey) {
		t.Fatalf("decoded success echo leaked: response=%q decision=%q error=%v", resp.Body, decisionFor(t, m, resp), err)
	}

	m = streamServer(t, "event: "+apiKey+"\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"content\":[],\"usage\":{}}}\n\n")
	_, err = m.Complete(context.Background(), freeze(t, m))
	if err == nil || strings.Contains(err.Error(), apiKey) || !strings.Contains(err.Error(), "<invalid identifier>") {
		t.Fatalf("event-name echo leaked: %v", err)
	}
}

func TestDecisionDoesNotDropAMalformedBlock(t *testing.T) {
	body := `{"content":[{"type":"text","text":"first","text":"second"},{"type":"text","text":"{\"kind\":\"return\",\"payload\":\"done\"}"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`
	m := newModel(t, "https://unused.test", nil)
	got := decisionFor(t, m, harness.Response{Body: []byte(body), Complete: true, Adapter: adapter})
	if string(got) != body {
		t.Fatalf("Decision dropped a malformed block: %s", got)
	}
	if _, err := core.Decode(got); err == nil {
		t.Fatal("a malformed block reached a decision")
	}
}

func TestCompleteChargesRetainedExtensionsNotWireTraffic(t *testing.T) {
	padding := strings.Repeat("x", 200)
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`)
	for range 20 {
		body += event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_details":{"type":"refusal","padding":"`+padding+`"}},"usage":{"output_tokens":5,"padding":"`+padding+`"}}`)
	}
	body += event("message_stop", `{"type":"message_stop"}`)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	})
	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = 1200 })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %+v, %v; repeated retained fields must not consume the wire budget", resp, err)
	}
}

func TestCompleteAcceptsNullableCitations(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"","citations":null}],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || string(decisionFor(t, m, resp)) != "done" {
		t.Fatalf("nullable citations = %+v, %v", resp, err)
	}
}

func TestCompleteRedactsADecisionSplitAcrossTextBlocks(t *testing.T) {
	split := len(apiKey) / 2
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(apiKey[:split])+`}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":`+strconv.Quote(apiKey[split:])+`}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":1}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(decisionFor(t, m, resp)), apiKey) || !strings.Contains(string(decisionFor(t, m, resp)), "[redacted]") {
		t.Fatalf("split decision = %q, %v", decisionFor(t, m, resp), err)
	}
}

func TestCompleteRejectsInvalidBlocksInMessageStart(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"","text":"x"}],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("message_delta", `{"type":"message_delta","delta":{},"usage":null}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || !strings.Contains(err.Error(), "block") {
		t.Fatalf("Complete = %v, %v; a malformed message_start block must fail", resp.Complete, err)
	}
}

func TestCompleteRedactsATruncatedErrorBody(t *testing.T) {
	escaped := `\u0073` + apiKey[1:]
	for name, body := range map[string]string{
		"literal": `{"error":{"message":"bad key ` + apiKey,
		"escaped": `{"error":{"message":"bad key ` + escaped,
	} {
		t.Run(name, func(t *testing.T) {
			m := newModel(t, serve(t, reply(http.StatusBadRequest, body)).URL, nil)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			joined := string(resp.Body)
			if err != nil {
				joined += err.Error()
			}
			if strings.Contains(joined, apiKey) || !strings.Contains(string(resp.Body), "[truncated: unparseable remainder]") {
				t.Fatalf("truncated body = %q, %v", resp.Body, err)
			}
		})
	}
}

func TestCompleteRedactsAnEscapedSecretInTheDecisionJSON(t *testing.T) {
	escaped := `\u0073` + apiKey[1:]
	text := `{"kind":"return","payload":"` + escaped + `"}`
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(text)+`}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(decisionFor(t, m, resp)), apiKey) {
		t.Fatalf("escaped decision secret survived: %q, %v", decisionFor(t, m, resp), err)
	}
	decision, err := core.Decode(decisionFor(t, m, resp))
	if err != nil || !strings.Contains(decision.Payload, "[redacted]") {
		t.Fatalf("decoded decision = %+v, %v", decision, err)
	}
}

func TestCompleteBoundsNon2xxBodies(t *testing.T) {
	t.Run("overflow status", func(t *testing.T) {
		body := `{"message":"` + strings.Repeat("x", 160) + `"}`
		m := newModel(t, serve(t, reply(http.StatusPaymentRequired, body)).URL, func(c *anthropic.Config) { c.MaxResponseBytes = 64 })
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err == nil || stopReason(err) != "" || !strings.Contains(err.Error(), "response exceeds") || len(resp.Body) > 64 {
			t.Fatalf("overflow body = %d bytes, err=%v; want a bounded retryable over-limit failure", len(resp.Body), err)
		}
	})

	t.Run("read error marker stays bounded", func(t *testing.T) {
		body := `{"type":"error","error":{"message":"` + strings.Repeat("x", 20)
		srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "application/json")
			w.Header().Set("Content-Length", "200")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, body)
		})
		m := newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = 64 })
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err == nil || stopReason(err) != "" || !strings.Contains(err.Error(), "unexpected EOF") || len(resp.Body) > 64 || !strings.Contains(string(resp.Body), "[truncated: unparseable remainder]") {
			t.Fatalf("read-error body = %d bytes (%q), err=%v", len(resp.Body), resp.Body, err)
		}
	})
}

func TestCompleteKeepsTheLastValidUsageSnapshot(t *testing.T) {
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":11,"output_tokens":0}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`) +
		event("message_delta", `{"type":"message_delta","delta":{},"usage":{"output_tokens":-1}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || resp.Usage != (harness.Usage{InputTokens: 11, OutputTokens: 2, InputReported: true, OutputReported: true}) {
		t.Fatalf("invalid counter lost the last valid usage: %+v, %v body=%s", resp.Usage, err, resp.Body)
	}
}

func TestCompleteClassifiesAndCountsAnInBoundsErrorBody(t *testing.T) {
	const short = "credential-sixteen-x"
	t.Run("credit balance at the cap", func(t *testing.T) {
		body := `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low ` + short + `"}}`
		m := newModel(t, serve(t, reply(http.StatusBadRequest, body)).URL, func(c *anthropic.Config) {
			c.APIKey, c.MaxResponseBytes = short, int64(len(body))
		})
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if stopReason(err) != core.StopBudgetExhausted {
			t.Fatalf("credit balance lost to diagnostic clipping: %v, body=%q", err, resp.Body)
		}
	})
	t.Run("usage at the cap", func(t *testing.T) {
		body := `{"type":"error","error":{"type":"invalid_request_error","message":"` + short + `"},"usage":{"input_tokens":7,"output_tokens":3}}`
		m := newModel(t, serve(t, reply(http.StatusBadRequest, body)).URL, func(c *anthropic.Config) {
			c.APIKey, c.MaxResponseBytes = short, int64(len(body))
		})
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err == nil || resp.Usage != (harness.Usage{InputTokens: 7, OutputTokens: 3, InputReported: true, OutputReported: true}) {
			t.Fatalf("usage lost to diagnostic clipping: %+v, %v", resp.Usage, err)
		}
	})
}

func TestCompleteKeepsAValidJSONScalarErrorBody(t *testing.T) {
	const short = "1234567890123456"
	m := newModel(t, serve(t, reply(http.StatusBadRequest, short)).URL, func(c *anthropic.Config) { c.APIKey = short })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || string(resp.Body) != short {
		t.Fatalf("scalar body = %q, %v", resp.Body, err)
	}
}

func TestCompleteKeepsValidatedUsageAfterInvalidDeltaWithAnOversizedDiagnostic(t *testing.T) {
	text := strings.Repeat(`"`, 40)
	body := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":7,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+strconv.Quote(text)+`}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`) +
		event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":11,"output_tokens":2}}`) +
		event("message_delta", `{"type":"message_delta","delta":{},"usage":{"output_tokens":-1}}`) +
		event("message_stop", `{"type":"message_stop"}`)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	})
	m := newModel(t, srv.URL, func(c *anthropic.Config) { c.MaxResponseBytes = 300 })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || resp.Usage != (harness.Usage{InputTokens: 11, OutputTokens: 2, InputReported: true, OutputReported: true}) {
		t.Fatalf("oversized diagnostic lost validated usage: %+v, %v", resp.Usage, err)
	}
}

func TestCompleteKeepsProvisionalOutputUnsettled(t *testing.T) {
	start := event("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":null,"usage":{"input_tokens":7,"output_tokens":1}}}`) +
		event("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`) +
		event("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}`) +
		event("content_block_stop", `{"type":"content_block_stop","index":0}`)
	t.Run("input only", func(t *testing.T) {
		m := streamServer(t, start+event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":11}}`)+event("message_stop", `{"type":"message_stop"}`))
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err != nil || !resp.Complete || resp.Usage != (harness.Usage{InputTokens: 11, InputReported: true}) {
			t.Fatalf("provisional output settled: %+v, %v", resp.Usage, err)
		}
	})
	t.Run("validated output survives later input", func(t *testing.T) {
		m := streamServer(t, start+event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`)+event("message_delta", `{"type":"message_delta","delta":{},"usage":{"input_tokens":11}}`)+event("message_stop", `{"type":"message_stop"}`))
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err != nil || !resp.Complete || resp.Usage != (harness.Usage{InputTokens: 11, OutputTokens: 2, InputReported: true, OutputReported: true}) {
			t.Fatalf("validated output lost: %+v, %v", resp.Usage, err)
		}
	})
}

func TestCompleteClassifiesOverlappingPlaceholders(t *testing.T) {
	for name, usage := range map[string]string{
		"without usage": "",
		"with usage":    `,"usage":{"input_tokens":7,"output_tokens":3}`,
	} {
		t.Run(name, func(t *testing.T) {
			const secret = "credit-balance-placeholder"
			body := `{"type":"error","error":{"type":"invalid_request_error","message":"credit balance is too low: ` + secret + `"}` + usage + `}`
			m := newModel(t, serve(t, reply(http.StatusBadRequest, body)).URL, func(c *anthropic.Config) { c.APIKey = secret })
			resp, err := m.Complete(context.Background(), freeze(t, m))
			joined := string(resp.Body)
			if err != nil {
				joined += err.Error()
			}
			if stopReason(err) != core.StopBudgetExhausted || strings.Contains(joined, secret) {
				t.Fatalf("classification = %v, response=%q error=%v", stopReason(err), resp.Body, err)
			}
		})
	}
}

func TestCompleteRedactsEveryTypedErrorField(t *testing.T) {
	const secret = "typed-secret-placeholder"
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"context", http.StatusBadRequest, `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: ` + secret + `"}}`},
		{"budget", http.StatusPaymentRequired, `{"type":"error","error":{"type":"billing_error","message":"` + secret + `"}}`},
		{"unrecoverable", http.StatusUnauthorized, `{"type":"error","error":{"type":"authentication_error","message":"` + secret + `"}}`},
		{"retryable", http.StatusTooManyRequests, `{"type":"error","error":{"type":"rate_limit_error","message":"` + secret + `"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("content-type", "application/json")
				w.Header().Set("request-id", secret)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			m := newModel(t, srv.URL, func(c *anthropic.Config) { c.APIKey = secret })
			_, err := m.Complete(context.Background(), freeze(t, m))
			assertNoTypedEcho(t, secret, err)
		})
	}
}

func assertNoTypedEcho(t *testing.T, secret string, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a typed error")
	}
	assertNoEchoInChain(t, secret, err)
	var limit *harness.ContextLimitError
	if errors.As(err, &limit) {
		value := reflect.ValueOf(limit).Elem()
		for i := 0; i < value.NumField(); i++ {
			if field := value.Field(i); field.Kind() == reflect.String && strings.Contains(field.String(), secret) {
				t.Fatalf("typed field %s echoes the placeholder: %+v", value.Type().Field(i).Name, limit)
			}
		}
	}
	if limit != nil {
		allowed := append(append([]string{}, allowedTypes...), append(allowedCodes, allowedStopReasons...)...)
		for name, value := range map[string]string{"Type": limit.Type, "Code": limit.Code, "StopReason": limit.StopReason} {
			if value != "" && provider.CanonicalEnum(value, allowed...) != value && value != "unrecognized" {
				t.Fatalf("enum field %s is not canonical: %q", name, value)
			}
		}
	}
	var stop *harness.StopError
	if errors.As(err, &stop) && stop.Err != nil && strings.Contains(stop.Err.Error(), secret) {
		t.Fatalf("stop error echoes the placeholder: %v", stop.Err)
	}
}

var allowedTypes = []string{"invalid_request_error", "authentication_error", "billing_error", "permission_error", "not_found_error", "request_too_large", "rate_limit_error", "api_error", "overloaded_error", "timeout_error", "conflict_error"}
var allowedCodes = []string{"prompt_too_long", "context_length_exceeded"}
var allowedStopReasons = []string{"end_turn", "stop_sequence", "refusal", "tool_use", "max_tokens", "pause_turn", "model_context_window_exceeded"}

func TestCompleteClassifiesSSECreditOverlap(t *testing.T) {
	const secret = "credit-balance-placeholder"
	m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("error", `{"type":"error","error":{"type":"invalid_request_error","message":"credit balance is too low: `+secret+`"}}`))
	}).URL, func(c *anthropic.Config) { c.APIKey = secret })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if stopReason(err) != core.StopBudgetExhausted || strings.Contains(string(resp.Body)+err.Error(), secret) {
		t.Fatalf("credit SSE = %v, %v, body=%q", stopReason(err), err, resp.Body)
	}
}

func assertNoEchoInChain(t *testing.T, secret string, err error) {
	t.Helper()
	seen := map[error]bool{}
	var walk func(error)
	walk = func(err error) {
		if err == nil || seen[err] {
			return
		}
		seen[err] = true
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error chain echoes the placeholder: %v", err)
		}
		value := reflect.ValueOf(err)
		for i := 0; i < value.NumMethod(); i++ {
			method := value.Method(i)
			typ := method.Type()
			if typ.NumIn() != 0 || typ.NumOut() != 1 || typ.Out(0).Kind() != reflect.String {
				continue
			}
			if text := method.Call(nil)[0].String(); strings.Contains(text, secret) {
				t.Fatalf("error method %s echoes the placeholder: %q", value.Type().Method(i).Name, text)
			}
		}
		walkFields(t, secret, value, walk, 0)
		if multi, ok := err.(interface{ Unwrap() []error }); ok {
			for _, cause := range multi.Unwrap() {
				walk(cause)
			}
		}
		if one, ok := err.(interface{ Unwrap() error }); ok {
			walk(one.Unwrap())
		}
	}
	walk(err)
}

func assertStringMethods(t *testing.T, secret string, value reflect.Value) {
	t.Helper()
	for i := 0; i < value.NumMethod(); i++ {
		method := value.Method(i)
		typ := method.Type()
		if typ.NumIn() != 0 || typ.NumOut() != 1 || typ.Out(0).Kind() != reflect.String {
			continue
		}
		if text := method.Call(nil)[0].String(); strings.Contains(text, secret) {
			t.Fatalf("value method %s echoes the placeholder: %q", value.Type().Method(i).Name, text)
		}
	}
}

func walkFields(t *testing.T, secret string, value reflect.Value, walk func(error), depth int) {
	t.Helper()
	if depth > 4 || !value.IsValid() {
		return
	}
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !field.CanInterface() {
			continue
		}
		if text, ok := field.Interface().(string); ok && strings.Contains(text, secret) {
			t.Fatalf("error field %s echoes the placeholder", value.Type().Field(i).Name)
		}
		if addr, ok := field.Interface().(net.Addr); ok && addr != nil {
			if strings.Contains(addr.Network(), secret) || strings.Contains(addr.String(), secret) {
				t.Fatalf("address %s echoes the placeholder: %q", value.Type().Field(i).Name, addr)
			}
			assertStringMethods(t, secret, reflect.ValueOf(addr))
			continue
		}
		if cause, ok := field.Interface().(error); ok {
			walk(cause)
			continue
		}
		if field.Kind() == reflect.Pointer || field.Kind() == reflect.Struct {
			walkFields(t, secret, field, walk, depth+1)
		}
	}
}

func TestDiagnosticKeyCollisionPreservesTheRawDecision(t *testing.T) {
	const secret = "raw-key-collision-placeholder"
	m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("message_start", `{"type":"message_start","message":{"model":"m","[redacted]":0,`+strconv.Quote(secret)+`:1,"content":[{"type":"text","text":"{\"kind\":\"return\",\"payload\":\"done\"}"}]}}`)+
			event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`)+
			event("message_stop", `{"type":"message_stop"}`))
	}).URL, func(c *anthropic.Config) { c.APIKey = secret })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete=%v err=%v", resp.Complete, err)
	}
	decision, err := core.Decode(decisionFor(t, m, resp))
	if err != nil || decision.Kind != core.Return || decision.Payload != "done" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestRawProviderDuplicateKeysAreStillRejected(t *testing.T) {
	m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("message_start", `{"type":"message_start","message":{"model":"m","model":"n","content":[]}}`)+
			event("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`)+
			event("message_stop", `{"type":"message_stop"}`))
	}).URL, nil)
	if resp, err := m.Complete(context.Background(), freeze(t, m)); err == nil || resp.Complete {
		t.Fatalf("duplicate raw key accepted: Complete=%v err=%v", resp.Complete, err)
	}
}
