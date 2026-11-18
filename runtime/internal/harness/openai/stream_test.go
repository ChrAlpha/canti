package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

func event(typ, data string) string { return "event: " + typ + "\ndata: " + data + "\n\n" }

func streamServer(t *testing.T, body string) *Model {
	t.Helper()
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.Header().Set("x-request-id", "req_stream")
		_, _ = io.WriteString(w, body)
	})
	return newModel(t, srv.URL, nil)
}

func decisionFor(t *testing.T, m *Model, resp harness.Response) []byte {
	t.Helper()
	if resp.Extraction == nil {
		extraction := m.extract(resp.Body, resp.Body, resp.Adapter, resp.Complete)
		resp.Extraction = &extraction
	}
	if resp.Adapter != "" && resp.Adapter != adapter && resp.Adapter != toolAdapter {
		return nil
	}
	if got := m.Decision(resp); got != nil || resp.Extraction == nil || resp.Extraction.Decision != nil {
		return got
	}
	if resp.Adapter == toolAdapter && !resp.Extraction.Found {
		return nil
	}
	return resp.Extraction.Body
}

func TestCompleteAssemblesTheFinalResponseFromAStream(t *testing.T) {
	response := respond(`"completed"`, textOutput)
	body := event("response.created", `{"type":"response.created","response":{"id":"resp_1","model":"test-model-2026","status":"in_progress","output":[],"usage":null}}`) +
		event("response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"{\"kind\""}`) +
		event("response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"\":\"return\",\"payload\":\"done\"}"}`) +
		event("response.completed", `{"type":"response.completed","response":`+response+`}`)
	m := streamServer(t, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	if string(resp.Body) != response || resp.Model != "test-model-2026" {
		t.Fatalf("response = %+v", resp)
	}
	if resp.Usage != (harness.Usage{InputTokens: 10, OutputTokens: 7, InputReported: true, OutputReported: true}) {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	if got := string(decisionFor(t, m, resp)); got != `{"kind":"return","payload":"done"}` {
		t.Fatalf("Decision = %s", got)
	}
}

func TestCompleteKeepsTheTerminalUsageDetails(t *testing.T) {
	response := respond(`"completed"`, textOutput)
	m := streamServer(t, event("response.completed", `{"type":"response.completed","response":`+response+`}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete = %+v, %v", resp, err)
	}
	var envelope struct {
		Usage struct {
			InputTokensDetails  map[string]int `json:"input_tokens_details"`
			OutputTokensDetails map[string]int `json:"output_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(resp.Body, &envelope); err != nil || envelope.Usage.InputTokensDetails["cached_tokens"] != 4 || envelope.Usage.OutputTokensDetails["reasoning_tokens"] != 3 {
		t.Fatalf("cached and reasoning details were lost: %s (%v)", resp.Body, err)
	}
}

func TestCompleteMapsAStreamErrorEvent(t *testing.T) {
	for _, tc := range []struct {
		code, message string
		want          core.StopReason
		overflow      bool
	}{
		{"context_length_exceeded", "too large", "", true},
		{"insufficient_quota", "no credit", core.StopBudgetExhausted, false},
		{"rate_limit_exceeded", "slow down", "", false},
		{"server_error", "internal", "", false},
		{"invalid_api_key", "bad key", core.StopUnrecoverable, false},
		{"invalid_request_error", "bad input", core.StopUnrecoverable, false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			data := `{"type":"error","code":"` + tc.code + `","message":"` + tc.message + `"}`
			m := streamServer(t, event("error", data))
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete {
				t.Fatalf("Complete = %v, %v", resp.Complete, err)
			}
			var limit *harness.ContextLimitError
			if errors.As(err, &limit) != tc.overflow || stopReason(err) != tc.want {
				t.Fatalf("error = %v, stop = %q, overflow = %v", err, stopReason(err), errors.As(err, &limit))
			}
			statusText := "status 200"
			if tc.overflow {
				statusText = "status=200"
			}
			if !strings.Contains(err.Error(), tc.code) || !strings.Contains(err.Error(), "req_stream") || !strings.Contains(err.Error(), statusText) {
				t.Fatalf("error %q lacks its code, request id, or actual status", err)
			}
		})
	}
}

func TestCompleteIgnoresCommentsAndKeepAlives(t *testing.T) {
	body := ": keep-alive\n\n" + event("response.created", `{"type":"response.created","response":{"id":"resp_1","status":"in_progress","output":[],"usage":null}}`) +
		event("response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_1","status":"in_progress","output":[],"usage":null}}`) +
		event("response.completed", `{"type":"response.completed","response":`+respond(`"completed"`, textOutput)+`}`)
	m := streamServer(t, body)
	if resp, err := m.Complete(context.Background(), freeze(t, m)); err != nil || !resp.Complete {
		t.Fatalf("Complete = %v, %v", resp.Complete, err)
	}
}

func TestCompleteRejectsAnOversizedStreamEvent(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("response.created", `{"type":"response.created","response":{"id":"resp_1","status":"in_progress","padding":"`+strings.Repeat("x", 70<<10)+`"}}`))
	})
	m := newModel(t, srv.URL, func(c *Config) { c.MaxResponseBytes = 32 })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != "" || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Complete = %v, %v; want an oversized-event retry", resp.Complete, err)
	}
}

func TestCompleteRequiresAnEventStream(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, respond(`"completed"`, textOutput))
	})
	m := newModel(t, srv.URL, nil)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != "" || !strings.Contains(err.Error(), "not text/event-stream") {
		t.Fatalf("Complete = %v, %v", resp.Complete, err)
	}
}

func TestCompleteReadsASlowLiveStream(t *testing.T) {
	response := respond(`"completed"`, textOutput)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("response.created", `{"type":"response.created","response":{"id":"resp_1","status":"in_progress","output":[],"usage":null}}`))
		w.(http.Flusher).Flush()
		for range 8 {
			time.Sleep(20 * time.Millisecond)
			_, _ = io.WriteString(w, event("response.output_text.delta", `{"type":"response.output_text.delta","delta":"x"}`))
			w.(http.Flusher).Flush()
		}
		_, _ = io.WriteString(w, event("response.completed", `{"type":"response.completed","response":`+response+`}`))
	})
	m := newModel(t, srv.URL, func(c *Config) { c.IdleTimeout = 60 * time.Millisecond })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || resp.Usage.OutputTokens != 7 {
		t.Fatalf("Complete = %+v, %v; a live stream must outlast the idle timeout", resp, err)
	}
}

func TestCompleteTreatsAStreamCutBeforeATerminalEventAsIncomplete(t *testing.T) {
	partial := `{"id":"resp_1","status":"in_progress","model":"test-model","output":[],"usage":null}`
	body := event("response.created", `{"type":"response.created","response":`+partial+`}`)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.Header().Set("Content-Length", "4096")
		_, _ = io.WriteString(w, body)
	})
	m := newModel(t, srv.URL, nil)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || stopReason(err) != "" || string(resp.Body) != partial ||
		resp.Usage != (harness.Usage{}) || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("Complete = %+v, %v; want a retryable cut with unknown usage", resp, err)
	}
}

func TestCompleteRejectsUnknownStreamFraming(t *testing.T) {
	body := respond(`"completed"`, textOutput)
	for name, stream := range map[string]string{
		"unknown event":    event("mystery", `{"type":"mystery"}`),
		"name mismatch":    event("response.completed", `{"type":"response.failed"}`),
		"malformed event":  "event: response.completed\ndata: {\"type\":\n\n",
		"missing response": event("response.completed", `{"type":"response.completed"}`),
		"only a delta":     event("response.output_text.delta", `{"type":"response.output_text.delta","delta":"x"}`),
		"wrong terminal":   event("response.completed", `{"type":"response.completed","response":[`+body+`]}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, stream)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || stopReason(err) != "" {
				t.Fatalf("Complete = %v, %v; want a retryable malformed stream", resp.Complete, err)
			}
		})
	}
}

func statusStreamServer(t *testing.T, status int, body string) *Model {
	t.Helper()
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.Header().Set("x-request-id", "req_stream")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	return newModel(t, srv.URL, nil)
}

func TestCompleteRedactsTheResponseAndErrorEvents(t *testing.T) {
	response := `{"id":"resp_1","status":"completed","model":"` + apiKey + `","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"key ` + apiKey + `"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`
	m := streamServer(t, event("response.completed", `{"type":"response.completed","response":`+response+`}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(resp.Body), "SECRET") || !strings.Contains(string(resp.Body), "[redacted]") {
		t.Fatalf("successful redaction = %+v, %v", resp, err)
	}

	m = streamServer(t, event("error", `{"type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"bad key `+apiKey+`"}}`))
	resp, err = m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || strings.Contains(string(resp.Body)+err.Error(), "SECRET") {
		t.Fatalf("error redaction = %q, %v", resp.Body, err)
	}

	partial := `{"id":"resp_1","status":"in_progress","model":"` + apiKey + `","output":[],"usage":null}`
	m = streamServer(t, event("response.created", `{"type":"response.created","response":`+partial+`}`))
	resp, err = m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || strings.Contains(string(resp.Body), "SECRET") || !strings.Contains(string(resp.Body), "[redacted]") {
		t.Fatalf("partial redaction = %q, %v", resp.Body, err)
	}
}

func TestCompleteRequiresTheTerminalKindToMatchItsStatus(t *testing.T) {
	completed := `{"id":"resp_1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`
	failed := `{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"boom"},"output":[]}`
	for name, body := range map[string]string{
		"completed event, failed object":     event("response.completed", `{"type":"response.completed","response":`+failed+`}`),
		"failed event, completed object":     event("response.failed", `{"type":"response.failed","response":`+completed+`}`),
		"incomplete event, completed object": event("response.incomplete", `{"type":"response.incomplete","response":`+completed+`}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, body)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || !strings.Contains(err.Error(), "carries status") {
				t.Fatalf("Complete = %v, %v; terminal kind and status must agree", resp.Complete, err)
			}
		})
	}
}

func TestCompleteRefusesUnknownResponseEvents(t *testing.T) {
	for name, body := range map[string]string{
		"unknown":              event("response.mystery", `{"type":"response.mystery"}`),
		"unknown non-response": event("mystery", `{"type":"mystery"}`),
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, body)
			resp, err := m.Complete(context.Background(), freeze(t, m))
			if err == nil || resp.Complete || !strings.Contains(err.Error(), "unknown stream event") {
				t.Fatalf("Complete = %v, %v", resp.Complete, err)
			}
		})
	}
	m := streamServer(t, event("response.queued", `{"type":"response.queued"}`)+event("response.compaction.compacting", `{"type":"response.compaction.compacting"}`)+event("response.output_text.delta", `{"type":"response.output_text.delta","delta":"x"}`)+event("response.completed", `{"type":"response.completed","response":`+respond(`"completed"`, textOutput)+`}`))
	if resp, err := m.Complete(context.Background(), freeze(t, m)); err != nil || !resp.Complete {
		t.Fatalf("documented event was refused: %v, %v", resp.Complete, err)
	}
}

func TestCompleteKeepsObservedUsageThroughAnErrorEvent(t *testing.T) {
	created := `{"id":"resp_1","status":"in_progress","output":[],"usage":{"input_tokens":3,"output_tokens":0}}`
	cleared := `{"id":"resp_1","status":"in_progress","output":[],"usage":null}`
	m := streamServer(t, event("response.created", `{"type":"response.created","response":`+created+`}`)+event("response.in_progress", `{"type":"response.in_progress","response":`+cleared+`}`)+event("error", `{"type":"error","code":"server_error","message":"boom"}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || resp.Usage.InputTokens != 3 || !resp.Usage.InputReported {
		t.Fatalf("usage lost through an error event: %+v, %v", resp, err)
	}
}

func TestCompleteClassifiesStreamErrorsByNestedType(t *testing.T) {
	for name, tc := range map[string]struct {
		data     string
		want     core.StopReason
		overflow bool
	}{
		"invalid request wins over its code": {
			data: `{"type":"error","error":{"type":"invalid_request_error","code":"insufficient_quota","message":"no credit"}}`,
			want: core.StopUnrecoverable,
		},
		"invalid value": {
			data: `{"type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"bad max_output_tokens"}}`,
			want: core.StopUnrecoverable,
		},
		"http 400 fixture": {
			data: `{"type":"error","error":{"message":"bad max_output_tokens","type":"invalid_request_error","param":null,"code":"invalid_value"}}`,
			want: core.StopUnrecoverable,
		},
		"quota type": {
			data: `{"type":"error","error":{"type":"insufficient_quota","message":"no credit"}}`,
			want: core.StopBudgetExhausted,
		},
		"context code": {
			data:     `{"type":"error","error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"too large"}}`,
			overflow: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := streamServer(t, event("error", tc.data))
			resp, err := m.Complete(context.Background(), freeze(t, m))
			var limit *harness.ContextLimitError
			if err == nil || resp.Complete || errors.As(err, &limit) != tc.overflow || stopReason(err) != tc.want {
				t.Fatalf("Complete = %+v, %v; stop=%q overflow=%v", resp, err, stopReason(err), errors.As(err, &limit))
			}
		})
	}
}

func TestCompleteRequiresExactStatusForAFailedContextLimit(t *testing.T) {
	body := event("response.failed", `{"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":"context_length_exceeded","message":"too large"},"output":[],"usage":{"input_tokens":1,"output_tokens":0}}}`)
	m := statusStreamServer(t, http.StatusCreated, body)
	resp, err := m.Complete(context.Background(), freeze(t, m))
	var limit *harness.ContextLimitError
	if err == nil || resp.Complete || errors.As(err, &limit) {
		t.Fatalf("Complete = %+v, %v; a non-200 failure must not confirm overflow", resp, err)
	}
}

func TestCompleteRejectsCaseVariantResponseFields(t *testing.T) {
	body := `{"id":"resp_1","Status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`
	m := streamServer(t, event("response.completed", `{"type":"response.completed","response":`+body+`}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || !strings.Contains(err.Error(), "carries status") {
		t.Fatalf("Complete = %v, %v; a Status alias must not become status", resp.Complete, err)
	}
}

func TestCompleteRedactsDecodedAndEventNameEchoes(t *testing.T) {
	escaped := `\u0073` + apiKey[1:]
	errorBody := `{"type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"bad key ` + escaped + `"}}`
	m := streamServer(t, event("error", errorBody))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || strings.Contains(string(resp.Body)+err.Error(), apiKey) || strings.Contains(string(resp.Body)+err.Error(), `\u0073`+apiKey[1:]) {
		t.Fatalf("decoded error echo leaked: response=%q error=%v", resp.Body, err)
	}

	response := `{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + escaped + `"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`
	m = streamServer(t, event("response.completed", `{"type":"response.completed","response":`+response+`}`))
	resp, err = m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(resp.Body), apiKey) || strings.Contains(string(decisionFor(t, m, resp)), apiKey) {
		t.Fatalf("decoded success echo leaked: response=%q decision=%q error=%v", resp.Body, decisionFor(t, m, resp), err)
	}

	m = streamServer(t, "event: "+apiKey+"\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\",\"output\":[],\"usage\":null}}\n\n")
	_, err = m.Complete(context.Background(), freeze(t, m))
	if err == nil || strings.Contains(err.Error(), apiKey) || !strings.Contains(err.Error(), "<invalid identifier>") {
		t.Fatalf("event-name echo leaked: %v", err)
	}
}

func TestCompleteKeepsAnObservedUsageSnapshot(t *testing.T) {
	created := `{"id":"resp_1","status":"in_progress","output":[],"usage":{"input_tokens":3,"output_tokens":0}}`
	cleared := `{"id":"resp_1","status":"in_progress","output":[],"usage":null}`
	terminal := `{"id":"resp_1","status":"completed","output":[],"usage":null}`
	m := streamServer(t, event("response.created", `{"type":"response.created","response":`+created+`}`)+event("response.in_progress", `{"type":"response.in_progress","response":`+cleared+`}`)+event("response.completed", `{"type":"response.completed","response":`+terminal+`}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || resp.Usage.InputTokens != 3 || !resp.Usage.InputReported {
		t.Fatalf("usage snapshot lost: %+v, %v", resp.Usage, err)
	}
}

func TestCompleteRedactsADecisionSplitAcrossOutputParts(t *testing.T) {
	split := len(apiKey) / 2
	response := `{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[` +
		`{"type":"output_text","text":` + strconv.Quote(apiKey[:split]) + `},` +
		`{"type":"output_text","text":` + strconv.Quote(apiKey[split:]) + `}]}],"usage":{"input_tokens":1,"output_tokens":2}}`
	m := streamServer(t, event("response.completed", `{"type":"response.completed","response":`+response+`}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete || strings.Contains(string(decisionFor(t, m, resp)), apiKey) || !strings.Contains(string(decisionFor(t, m, resp)), "[redacted]") {
		t.Fatalf("split decision = %q, %v", decisionFor(t, m, resp), err)
	}
}

func TestCompleteKeepsUsageWhenATerminalIsMalformed(t *testing.T) {
	created := `{"id":"resp_1","status":"in_progress","output":[],"usage":{"input_tokens":4,"output_tokens":0}}`
	m := streamServer(t, event("response.created", `{"type":"response.created","response":`+created+`}`)+event("response.completed", `{"type":"response.completed"}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || resp.Usage.InputTokens != 4 || !resp.Usage.InputReported {
		t.Fatalf("malformed terminal lost usage: %+v, %v", resp.Usage, err)
	}
}

func TestCompleteKeepsUsageWhenTerminalUsageIsInvalid(t *testing.T) {
	created := `{"id":"resp_1","status":"in_progress","output":[],"usage":{"input_tokens":3,"output_tokens":0}}`
	terminal := `{"id":"resp_1","status":"completed","output":[],"usage":{"input_tokens":"bad","output_tokens":0}}`
	m := streamServer(t, event("response.created", `{"type":"response.created","response":`+created+`}`)+event("response.completed", `{"type":"response.completed","response":`+terminal+`}`))
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || resp.Complete || resp.Usage.InputTokens != 3 || !resp.Usage.InputReported {
		t.Fatalf("invalid terminal usage lost snapshot: %+v, %v", resp.Usage, err)
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
	response := `{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":` + strconv.Quote(text) + `}]}],"usage":{"input_tokens":1,"output_tokens":2}}`
	m := streamServer(t, event("response.completed", `{"type":"response.completed","response":`+response+`}`))
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
		m := newModel(t, serve(t, reply(http.StatusPaymentRequired, body)).URL, func(c *Config) { c.MaxResponseBytes = 64 })
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err == nil || stopReason(err) != "" || !strings.Contains(err.Error(), "response exceeds") || len(resp.Body) > 64 {
			t.Fatalf("overflow body = %d bytes, err=%v; want a bounded retryable over-limit failure", len(resp.Body), err)
		}
	})

	t.Run("read error marker stays bounded", func(t *testing.T) {
		body := `{"error":{"message":"` + strings.Repeat("x", 20)
		srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "application/json")
			w.Header().Set("Content-Length", "200")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, body)
		})
		m := newModel(t, srv.URL, func(c *Config) { c.MaxResponseBytes = 64 })
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if err == nil || stopReason(err) != "" || !strings.Contains(err.Error(), "unexpected EOF") || len(resp.Body) > 64 || !strings.Contains(string(resp.Body), "[truncated: unparseable remainder]") {
			t.Fatalf("read-error body = %d bytes (%q), err=%v", len(resp.Body), resp.Body, err)
		}
	})
}

func TestCompleteClassifiesAndCountsAnInBoundsErrorBody(t *testing.T) {
	const short = "credential-sixteen-x"
	t.Run("insufficient quota at the cap", func(t *testing.T) {
		body := `{"error":{"message":"insufficient quota ` + short + `","type":"insufficient_quota","code":"insufficient_quota"}}`
		m := newModel(t, serve(t, reply(http.StatusTooManyRequests, body)).URL, func(c *Config) {
			c.APIKey, c.MaxResponseBytes = short, int64(len(body))
		})
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if stopReason(err) != core.StopBudgetExhausted {
			t.Fatalf("quota lost to diagnostic clipping: %v, body=%q", err, resp.Body)
		}
	})
	t.Run("usage at the cap", func(t *testing.T) {
		body := `{"error":{"message":"` + short + `","code":"invalid_value"},"usage":{"input_tokens":7,"output_tokens":3}}`
		m := newModel(t, serve(t, reply(http.StatusBadRequest, body)).URL, func(c *Config) {
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
	m := newModel(t, serve(t, reply(http.StatusBadRequest, short)).URL, func(c *Config) { c.APIKey = short })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil || string(resp.Body) != short {
		t.Fatalf("scalar body = %q, %v", resp.Body, err)
	}
}

func TestCompleteClassifiesOverlappingPlaceholders(t *testing.T) {
	for name, usage := range map[string]string{
		"without usage": "",
		"with usage":    `,"usage":{"input_tokens":7,"output_tokens":3}`,
	} {
		t.Run(name, func(t *testing.T) {
			const secret = "insufficient_quota_extra"
			body := `{"error":{"type":"invalid_request_error","code":"insufficient_quota","message":"` + secret + ` exceeded"}` + usage + `}`
			m := newModel(t, serve(t, reply(http.StatusTooManyRequests, body)).URL, func(c *Config) { c.APIKey = secret })
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
		{"context", http.StatusBadRequest, `{"error":{"type":"` + secret + `_type","code":"context_length_exceeded","message":"` + secret + `"}}`},
		{"budget", http.StatusTooManyRequests, `{"error":{"type":"` + secret + `_type","code":"insufficient_quota","message":"` + secret + `"}}`},
		{"unrecoverable", http.StatusUnauthorized, `{"error":{"type":"` + secret + `_type","code":"invalid_api_key","message":"` + secret + `"}}`},
		{"retryable", http.StatusInternalServerError, `{"error":{"type":"` + secret + `_type","code":"server_error","message":"` + secret + `"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("content-type", "application/json")
				w.Header().Set("x-request-id", secret)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			m := newModel(t, srv.URL, func(c *Config) { c.APIKey = secret })
			_, err := m.Complete(context.Background(), freeze(t, m))
			assertNoTypedEcho(t, secret, err)
		})
	}
}

func TestCompleteRedactsAStreamContextLimitType(t *testing.T) {
	const secret = "typed-secret-placeholder"
	body := event("error", `{"type":"error","error":{"type":"`+secret+`_type","code":"context_length_exceeded","message":"`+secret+`"}}`)
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	})
	m := newModel(t, srv.URL, func(c *Config) { c.APIKey = secret })
	_, err := m.Complete(context.Background(), freeze(t, m))
	assertNoTypedEcho(t, secret, err)
}

func assertNoTypedEcho(t *testing.T, secret string, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a typed error")
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if strings.Contains(cause.Error(), secret) {
			t.Fatalf("error chain echoes the placeholder: %v", cause)
		}
	}
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
		for name, value := range map[string]string{"Type": limit.Type, "Code": limit.Code, "StopReason": limit.StopReason} {
			if value != "" && provider.CanonicalEnum(value, append(errorTypes, errorCodes...)...) != value && value != "unrecognized" {
				t.Fatalf("enum field %s is not canonical: %q", name, value)
			}
		}
	}
	var stop *harness.StopError
	if errors.As(err, &stop) && stop.Err != nil && strings.Contains(stop.Err.Error(), secret) {
		t.Fatalf("stop error echoes the placeholder: %v", stop.Err)
	}
}

func TestCompleteClassifiesSSEOverlappingPlaceholders(t *testing.T) {
	t.Run("quota budget", func(t *testing.T) {
		const secret = "insufficient_quota_extra"
		m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			_, _ = io.WriteString(w, event("error", `{"type":"error","code":"insufficient_quota","message":"`+secret+`"}`))
		}).URL, func(c *Config) { c.APIKey = secret })
		resp, err := m.Complete(context.Background(), freeze(t, m))
		if stopReason(err) != core.StopBudgetExhausted || !strings.Contains(string(resp.Body), `"code":"insufficient_quota"`) {
			t.Fatalf("quota SSE = %v, %v, body=%q", stopReason(err), err, resp.Body)
		}
	})

	t.Run("context event", func(t *testing.T) {
		const secret = "context_length_exceeded_extra"
		m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			_, _ = io.WriteString(w, event("error", `{"type":"error","error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"`+secret+`"}}`))
		}).URL, func(c *Config) { c.APIKey = secret })
		_, err := m.Complete(context.Background(), freeze(t, m))
		var limit *harness.ContextLimitError
		if !errors.As(err, &limit) || !limit.Confirmed() || limit.Code != "context_length_exceeded" {
			t.Fatalf("context SSE = %+v, %v", limit, err)
		}
	})

	t.Run("failed terminal", func(t *testing.T) {
		const secret = "context_length_exceeded_extra"
		response := `{"id":"resp_1","status":"failed","error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"` + secret + `"},"output":[]}`
		m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			_, _ = io.WriteString(w, event("response.failed", `{"type":"response.failed","response":`+response+`}`))
		}).URL, func(c *Config) { c.APIKey = secret })
		resp, err := m.Complete(context.Background(), freeze(t, m))
		var limit *harness.ContextLimitError
		if !errors.As(err, &limit) || !limit.Confirmed() || limit.Code != "context_length_exceeded" || !strings.Contains(string(resp.Body), `"status":"failed"`) {
			t.Fatalf("failed terminal = %+v, %v, body=%q", limit, err, resp.Body)
		}
	})
}

func TestContextLimitConfirmationSurvivesAnOverlappingFreeTextPlaceholder(t *testing.T) {
	const secret = "context_length_exceeded_extra"
	m := newModel(t, serve(t, reply(http.StatusBadRequest, `{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"`+secret+`"}}`)).URL, func(c *Config) { c.APIKey = secret })
	_, err := m.Complete(context.Background(), freeze(t, m))
	var limit *harness.ContextLimitError
	if !errors.As(err, &limit) || !limit.Confirmed() || limit.Code != "context_length_exceeded" {
		t.Fatalf("confirmed context limit = %+v, %v", limit, err)
	}
}

func TestCompleteScrubsATransportTypedCause(t *testing.T) {
	const secret = "transport-secret"
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 %s\r\n\r\n", secret)
		_ = buf.Flush()
	})
	m := newModel(t, srv.URL, func(c *Config) { c.APIKey = secret })
	_, err := m.Complete(context.Background(), freeze(t, m))
	assertNoTypedEcho(t, secret, err)
	var network *url.Error
	if errors.As(err, &network) {
		if strings.Contains(network.Op, secret) || strings.Contains(network.URL, secret) || (network.Err != nil && strings.Contains(network.Err.Error(), secret)) {
			t.Fatalf("url.Error retains the echo: %+v", network)
		}
	}
}

type multiCause struct{ cause error }

func (e multiCause) Error() string   { return "neutral transport error" }
func (e multiCause) Unwrap() []error { return []error{e.cause} }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCompleteSanitizesAMultiCauseTransport(t *testing.T) {
	const secret = "multi-cause-transport-placeholder"
	m := newModel(t, "https://model.test/v1", func(c *Config) {
		c.APIKey = secret
		c.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return nil, multiCause{cause: fmt.Errorf("echo %s", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))}
		})}
	})
	_, err := m.Complete(context.Background(), freeze(t, m))
	if err == nil {
		t.Fatal("expected a transport failure")
	}
	var u *url.Error
	if !errors.As(err, &u) {
		t.Fatalf("transport wrapper lost: %T %v", err, err)
	}
	seen := map[error]bool{}
	var walk func(error)
	walk = func(err error) {
		if err == nil || seen[err] {
			return
		}
		seen[err] = true
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("multi-cause branch echoes the placeholder: %v", err)
		}
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

func TestResponsesDiagnosticKeyCollisionPreservesTheRawDecision(t *testing.T) {
	const secret = "raw-key-collision-placeholder"
	m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("response.completed", `{"type":"response.completed","response":{"status":"completed","[redacted]":0,`+strconv.Quote(secret)+`:1,"output":[{"type":"message","content":[{"type":"output_text","text":"{\"kind\":\"return\",\"payload\":\"done\"}"}]}]}}`))
	}).URL, func(c *Config) { c.APIKey = secret })
	resp, err := m.Complete(context.Background(), freeze(t, m))
	if err != nil || !resp.Complete {
		t.Fatalf("Complete=%v err=%v", resp.Complete, err)
	}
	decision, err := core.Decode(decisionFor(t, m, resp))
	if err != nil || decision.Kind != core.Return || decision.Payload != "done" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestResponsesRawProviderDuplicateKeysAreStillRejected(t *testing.T) {
	m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, event("response.completed", `{"type":"response.completed","response":{"status":"completed","status":"completed","output":[]}}`))
	}).URL, nil)
	if resp, err := m.Complete(context.Background(), freeze(t, m)); err == nil || resp.Complete {
		t.Fatalf("duplicate raw key accepted: Complete=%v err=%v", resp.Complete, err)
	}
}

func TestCompleteKeepsDeadlineIdentityThroughAnHTTPClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond)
	}))
	defer srv.Close()
	m := newModel(t, srv.URL, func(c *Config) { c.Client = &http.Client{Timeout: 50 * time.Millisecond} })
	_, err := m.Complete(context.Background(), freeze(t, m))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("client timeout lost DeadlineExceeded: %v", err)
	}
	var u *url.Error
	if !errors.As(err, &u) {
		t.Fatalf("url.Error wrapper lost: %T %v", err, err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("timeout behavior lost: %#v", err)
	}
}

func TestVocabularyWordsAreNotUnknownEvents(t *testing.T) {
	for _, name := range []string{"payload", "completed", "insufficient_quota", "[redacted]"} {
		if knownEvent(name) {
			t.Errorf("knownEvent(%q) = true; a vocabulary word is not an event", name)
		}
	}
	for _, name := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if ProtocolIsEvent(name) == false {
			t.Errorf("ProtocolIsEvent(%q) = false", name)
		}
	}
}

func TestCompleteRejectsVocabularyWordsAsUnknownEvents(t *testing.T) {
	for _, name := range []string{"payload", "completed", "insufficient_quota", "[redacted]"} {
		t.Run(name, func(t *testing.T) {
			m := newModel(t, serve(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("content-type", "text/event-stream")
				_, _ = io.WriteString(w, event(name, `{"type":`+strconv.Quote(name)+`}`))
			}).URL, nil)
			if resp, err := m.Complete(context.Background(), freeze(t, m)); err == nil || resp.Complete || !strings.Contains(err.Error(), "unknown stream event") {
				t.Fatalf("event %q accepted: Complete=%v err=%v", name, resp.Complete, err)
			}
		})
	}
}
