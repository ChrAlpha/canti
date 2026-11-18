package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func sweepString(value string, escaped bool) string {
	if escaped {
		return `"\u0073` + value[1:] + `"`
	}
	out, _ := json.Marshal(value)
	return string(out)
}

func sweepNested(value string) string {
	obj, _ := json.Marshal(`{"echo":"` + value + `"}`)
	return string(obj)
}

func sweepValue(kind, key string) (string, bool) {
	switch kind {
	case "literal":
		return key, false
	case "escaped":
		return key, true
	default:
		return sweepNested(key), false
	}
}

// sweepProvider writes a synthetic provider response that puts value into
// field. HTTP status is non-2xx for error fields, so the CLI always stops.
func sweepProvider(t *testing.T, api, field, value string, escaped bool, sent *atomic.Int32) *httptest.Server {
	t.Helper()
	if escaped {
		value = sweepString(value, true)
	} else {
		value = sweepString(value, false)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		if strings.Contains(field, "request_id") {
			w.Header().Set("request-id", strings.Trim(value, `"`))
			w.Header().Set("x-request-id", strings.Trim(value, `"`))
		}
		if field == "plain_text_error_body" {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = fmt.Fprintf(w, "gateway error: %s", value)
			return
		}
		if field == "http_error_type" || field == "error_type" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = fmt.Fprintf(w, `{"error":{"type":%s,"code":"invalid_request_error","message":"bad"}}`, value)
			return
		}
		if field == "http_error_code" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","code":%s,"message":"bad"}}`, value)
			return
		}
		if strings.Contains(field, "failure") || strings.Contains(field, "http_error") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","code":"invalid_request_error","message":%s}}`, value)
			return
		}
		if field == "context_request_id" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"too long"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if strings.Contains(field, "unknown_event") {
			name := strings.Trim(value, `"`)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: {\"type\":%s}\n\n", name, value)
			return
		}
		decision := `{"kind":"return","payload":"done"}`
		switch field {
		case "decision_payload":
			decision = `{"kind":"return","payload":` + value + `}`
		case "decision_kind":
			decision = `{"kind":` + value + `,"payload":"done"}`
		case "decision_purpose":
			decision = `{"kind":"return","purpose":` + value + `,"payload":"done"}`
		case "malformed_decision":
			decision = `{"kind":"return","payload":"done"} ` + value
		case "inch_mark_prose":
			decision = `{"kind":"return","payload":` + strconv.Quote(`panel 6" reported value `+value) + `}`
		}
		text, _ := json.Marshal(decision)
		if api == "anthropic-messages" {
			block := fmt.Sprintf(`{"type":"text","text":%s}`, text)
			stop := "end_turn"
			if field == "tool_name" || field == "tool_arguments" {
				input := decision
				if field == "tool_arguments" {
					input = `{"kind":"return","payload":` + value + `}`
				}
				if field == "tool_name" {
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"content\":[{\"type\":\"tool_use\",\"id\":\"call\",\"name\":%s,\"input\":%s}]}}\n\n", value, input)
				} else {
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"content\":[{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"canti_decision\",\"input\":%s}]}}\n\n", input)
				}
				stop = "tool_use"
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":%q}}\n\n", stop)
				_, _ = io.WriteString(w, "data: {\"type\":\"message_stop\"}\n\n")
				return
			}
			if field == "sse_error" {
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"code\":%s,\"message\":%s}}\n\n", value, value)
				return
			}
			if field == "stop_reason" {
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"content\":[]}}\n\n")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":%s}}\n\n", value)
				_, _ = io.WriteString(w, "data: {\"type\":\"message_stop\"}\n\n")
				return
			}
			if field == "delta_type" {
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"content\":[]}}\n\n")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":%s}}\n\n", value)
				return
			}
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"model\":%s,\"content\":[%s]}}\n\n", value, block)
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":%q}}\n\n", stop)
			_, _ = io.WriteString(w, "data: {\"type\":\"message_stop\"}\n\n")
			return
		}
		var item string
		if field == "tool_name" {
			item = fmt.Sprintf(`{"type":"function_call","name":%s,"call_id":"call","arguments":%s}`, value, text)
		} else if field == "tool_arguments" {
			decision = `{"kind":"return","payload":` + value + `}`
			text, _ = json.Marshal(decision)
			item = fmt.Sprintf(`{"type":"function_call","name":"canti_decision","call_id":"call","arguments":%s}`, text)
		} else {
			item = fmt.Sprintf(`{"type":"message","content":[{"type":"output_text","text":%s}]}`, text)
		}
		if field == "sse_error" {
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"code\":%s,\"message\":%s}}\n\n", value, value)
			return
		}
		if field == "output_item_type" {
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":\"m\",\"output\":[{\"type\":%s}]}}\n\n", value)
			return
		}
		if field == "delta_type" {
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":%s}}\n\n", value)
			return
		}
		if field == "stop_reason" || field == "status" {
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":%s,\"model\":\"m\",\"output\":[]}}\n\n", value)
			return
		}
		if field == "incomplete_reason" {
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":%s},\"model\":\"m\",\"output\":[]}}\n\n", value)
			return
		}
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":%s,\"output\":[%s]}}\n\n", value, item)
	}))
}

func decodesToKey(value, key string, depth int) bool {
	if depth > 6 {
		return false
	}
	if strings.Contains(value, key) {
		return true
	}
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return false
	}
	return decodesValue(decoded, key, depth+1)
}

func decodesValue(value any, key string, depth int) bool {
	if depth > 6 {
		return false
	}
	switch v := value.(type) {
	case string:
		return decodesToKey(v, key, depth+1)
	case []any:
		for _, item := range v {
			if decodesValue(item, key, depth+1) {
				return true
			}
		}
	case map[string]any:
		for _, item := range v {
			if decodesValue(item, key, depth+1) {
				return true
			}
		}
	}
	return false
}

func TestRedactionSweepCoversEveryProviderStringField(t *testing.T) {
	const key = "sk-sol-sweep-credential-012"
	fields := []string{"serving_model", "request_id_success", "context_request_id", "failure_message", "plain_text_error_body", "http_error_code", "sse_error", "unknown_event", "decision_payload", "decision_kind", "decision_purpose", "malformed_decision", "inch_mark_prose", "tool_name", "tool_arguments", "delta_type", "stop_reason", "output_item_type", "incomplete_reason", "error_type"}
	for _, api := range []string{"anthropic-messages", "openai-responses"} {
		for _, field := range fields {
			for _, encoding := range []string{"literal", "escaped", "nested"} {
				t.Run(api+"/"+field+"/"+encoding, func(t *testing.T) {
					f := newFixture(t, map[int]core.Decision{})
					value, escaped := sweepValue(encoding, key)
					var sent atomic.Int32
					srv := sweepProvider(t, api, field, value, escaped, &sent)
					defer srv.Close()
					format := "text"
					if field == "tool_name" || field == "tool_arguments" {
						format = "tool"
					}
					r := runCLI(t, []string{"ANTHROPIC_API_KEY=" + key, "OPENAI_API_KEY=" + key}, "run",
						"-workspace", f.workspace, "-state", f.state, "-model-url", srv.URL, "-api", api,
						"-decision-format", format, "-model", "fixture-model", "-context-tokens", "6100", "-ephemeral",
						"-goal", "redaction sweep", "-max-attempts", "1")
					if sent.Load() == 0 {
						t.Fatalf("%s/%s/%s did not reach the provider", api, field, encoding)
					}
					h := f.history()
					if len(h) == 0 {
						t.Fatalf("%s/%s/%s produced no journal", api, field, encoding)
					}
					emit := []string{r.stdout, r.stderr}
					raw, _ := json.Marshal(h)
					emit = append(emit, string(raw))
					switch field {
					case "delta_type":
						if api == "anthropic-messages" && !strings.Contains(string(raw), "invalid identifier") {
							t.Fatalf("%s did not reach the invalid-identifier diagnostic: %s", field, raw)
						}
					case "decision_kind", "unknown_event":
						if !strings.Contains(string(raw), "invalid identifier") {
							t.Fatalf("%s did not reach the invalid-identifier diagnostic: %s", field, raw)
						}
					case "malformed_decision":
						want := "[redacted]"
						if encoding == "literal" {
							want = "trailing value of type string"
						}
						if !strings.Contains(string(raw), want) {
							t.Fatalf("malformed decision did not reach %q: %s", want, raw)
						}
					}
					if data, err := os.ReadFile(filepath.Join(f.state, "canti.db")); err == nil {
						emit = append(emit, string(data))
					}
					for _, text := range emit {
						if decodesToKey(text, key, 0) {
							t.Fatalf("recovered sweep credential from %s/%s/%s: %s", api, field, encoding, text)
						}
					}
				})
			}
		}
	}
}

func TestRedactionSweepDeepQuotedProse(t *testing.T) {
	const key = "sk-sol-deep-quoted-credential"
	for _, api := range []string{"anthropic-messages", "openai-responses"} {
		t.Run(api, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{})
			value := key
			for i := 0; i < 6; i++ {
				value = "value " + strconv.Quote(value)
			}
			decision, _ := json.Marshal(map[string]string{"kind": "return", "payload": value})
			var sent atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sent.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				if api == "anthropic-messages" {
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"model\":\"m\",\"content\":[{\"type\":\"text\",\"text\":%s}]}}\n\n", strconv.Quote(string(decision)))
					_, _ = io.WriteString(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n")
					return
				}
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":\"m\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":%s}]}]}}\n\n", strconv.Quote(string(decision)))
			}))
			defer srv.Close()
			r := runCLI(t, []string{"ANTHROPIC_API_KEY=" + key, "OPENAI_API_KEY=" + key}, "run",
				"-workspace", f.workspace, "-state", f.state, "-model-url", srv.URL, "-api", api,
				"-decision-format", "text", "-model", "fixture-model", "-context-tokens", "6100", "-ephemeral",
				"-goal", "deep quoted sweep", "-max-attempts", "1")
			if sent.Load() == 0 {
				t.Fatalf("deep quoted sweep did not reach the provider")
			}
			h := f.history()
			raw, _ := json.Marshal(h)
			emit := []string{r.stdout, r.stderr, string(raw)}
			if data, err := os.ReadFile(filepath.Join(f.state, "canti.db")); err == nil {
				emit = append(emit, string(data))
			}
			for _, text := range emit {
				if decodesToKey(text, key, 0) {
					t.Fatalf("deep quoted credential recovered from %s: %s", api, text)
				}
			}
		})
	}
}
