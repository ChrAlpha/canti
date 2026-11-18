package openai

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

// readResponseStream returns the response object of the stream's terminal
// event. Delta events are not assembled: response.completed,
// response.failed, and response.incomplete each carry the whole response
// object, which is the same body the non-streaming transport returned. The
// terminal event kind must agree with the object's status; a mismatch is not
// a complete response.
type terminalResult struct {
	kind, model, reason, errorType, errorCode, errorMessage string
	hasError                                                bool
	raw                                                     []byte
}

func readResponseStream(s *provider.Stream) ([]byte, harness.Usage, terminalResult, error) {
	var last json.RawMessage
	var observed harness.Usage
	for {
		event, err := s.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return last, observed, terminalResult{}, fmt.Errorf("openai: stream ended before a terminal response event: %w", io.ErrUnexpectedEOF)
			}
			return last, observed, terminalResult{}, err
		}
		rawFields, typ, err := streamEvent(event, s.Ident)
		if err != nil {
			return last, observed, terminalResult{}, err
		}
		if terminal(typ) {
			return terminalResponse(rawFields, typ, s, observed)
		}
		if typ == "error" {
			body := s.RedactJSON(event.RawData)
			return body, observed, terminalResult{}, streamError(event.RawData, body, s.Status, s.Header, s.RedactValue)
		}
		if !knownEvent(typ) {
			return last, observed, terminalResult{}, fmt.Errorf("openai: unknown stream event %q", s.Ident(typ))
		}
		rawResponse := provider.ExactField(rawFields, "response")
		if len(rawResponse) > 0 && rawResponse[0] == '{' && json.Valid(rawResponse) {
			last = s.RedactJSON(rawResponse)
			observed = observeUsage(observed, rawResponse)
		}
	}
}

func streamEvent(event provider.Event, ident func(string) string) (map[string]json.RawMessage, string, error) {
	raw := event.RawData
	if len(raw) == 0 {
		raw = event.Data
	}
	fields, ok := provider.ExactObject(raw)
	if !ok || !provider.NoCaseAliases(fields, "type", "response", "error", "code", "message") {
		return nil, "", errors.New("openai: malformed stream event")
	}
	typ := provider.Discriminator(fields, "type")
	if typ == "" {
		return nil, "", errors.New("openai: stream event has no type")
	}
	name := event.RawName
	if name == "" {
		name = event.Name
	}
	if name != "" && name != typ {
		return nil, "", fmt.Errorf("openai: stream event %q carries a different type", ident(name))
	}
	return fields, typ, nil
}

func terminal(typ string) bool {
	return typ == "response.completed" || typ == "response.incomplete" || typ == "response.failed"
}

func terminalResponse(rawFields map[string]json.RawMessage, typ string, s *provider.Stream, observed harness.Usage) ([]byte, harness.Usage, terminalResult, error) {
	rawResponse := provider.ExactField(rawFields, "response")
	if len(rawResponse) == 0 || rawResponse[0] != '{' || !json.Valid(rawResponse) {
		return nil, observed, terminalResult{}, fmt.Errorf("openai: %s has no valid response object", s.Ident(typ))
	}
	sanitizedResponse := s.RedactJSON(rawResponse)
	responseFields, ok := provider.ExactObject(rawResponse)
	if !ok {
		return sanitizedResponse, observeUsage(observed, rawResponse), terminalResult{}, fmt.Errorf("openai: malformed %s response object", s.Ident(typ))
	}
	status := provider.Discriminator(responseFields, "status")
	want := map[string]string{"response.completed": "completed", "response.incomplete": "incomplete", "response.failed": "failed"}[typ]
	if status != want {
		return sanitizedResponse, observeUsage(observed, rawResponse), terminalResult{}, fmt.Errorf("openai: %s carries status %q, want %q", s.Ident(typ), s.Ident(status), want)
	}
	if err := validateResponse(responseFields); err != nil {
		return sanitizedResponse, observeUsage(observed, rawResponse), terminalResult{}, err
	}
	usage, _, usageErr := provider.Usage(rawResponse, false)
	if usageErr != nil {
		return sanitizedResponse, observed, terminalResult{}, usageErr
	}
	usage = mergeUsage(observed, usage)
	result := terminalResult{kind: want, model: s.RedactValue(provider.ExactString(responseFields, "model")), raw: bytes.Clone(rawResponse)}
	if want == "incomplete" {
		if details, ok := provider.ExactObject(provider.ExactField(responseFields, "incomplete_details")); ok {
			result.reason = s.Ident(provider.Discriminator(details, "reason"))
		}
	}
	if want == "failed" {
		if errorFields, ok := provider.ExactObject(provider.ExactField(responseFields, "error")); ok {
			result.hasError = true
			result.errorType = canonicalType(provider.Discriminator(errorFields, "type"))
			result.errorCode = canonicalCode(provider.Discriminator(errorFields, "code"))
			result.errorMessage = s.RedactValue(provider.ExactString(errorFields, "message"))
			if result.errorCode == "context_length_exceeded" && (s.Status == http.StatusOK || s.Status == http.StatusBadRequest) {
				return sanitizedResponse, usage, result, &harness.ContextLimitError{Status: s.Status, Type: result.errorType, Code: result.errorCode, RequestID: s.RedactValue(s.Header.Get("x-request-id"))}
			}
		}
	}
	return sanitizedResponse, usage, result, nil
}

// knownEvent lists the non-terminal events in the Responses streaming-events
// reference (https://platform.openai.com/docs/api-reference/responses-streaming).
// An event outside this list is refused rather than guessed at.
var responseEvents = []string{"response.created", "response.queued", "response.in_progress", "response.compaction.compacting", "response.shell_call_command.added", "response.shell_call_command.delta", "response.shell_call_command.done", "response.shell_call_output_content.delta", "response.shell_call_output_content.done", "response.output_item.added", "response.output_item.done", "response.content_part.added", "response.content_part.done", "response.output_text.delta", "response.output_text.done", "response.output_text.annotation.added", "response.refusal.delta", "response.refusal.done", "response.function_call_arguments.delta", "response.function_call_arguments.done", "response.custom_tool_call_input.delta", "response.custom_tool_call_input.done", "response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done", "response.reasoning_text.delta", "response.reasoning_text.done", "response.audio.delta", "response.audio.done", "response.audio.transcript.delta", "response.audio.transcript.done", "response.code_interpreter_call.in_progress", "response.code_interpreter_call.interpreting", "response.code_interpreter_call.completed", "response.code_interpreter_call_code.delta", "response.code_interpreter_call_code.done", "response.file_search_call.in_progress", "response.file_search_call.searching", "response.file_search_call.completed", "response.web_search_call.in_progress", "response.web_search_call.searching", "response.web_search_call.completed", "response.image_generation_call.in_progress", "response.image_generation_call.generating", "response.image_generation_call.completed", "response.image_generation_call.partial_image", "response.mcp_call.in_progress", "response.mcp_call.completed", "response.mcp_call.failed", "response.mcp_call_arguments.delta", "response.mcp_call_arguments.done", "response.mcp_list_tools.in_progress", "response.mcp_list_tools.completed", "response.mcp_list_tools.failed"}

func knownEvent(typ string) bool {
	return !terminal(typ) && protocolEvent(typ)
}

func streamError(rawData, body []byte, status int, header http.Header, redact func(string) string) error {
	raw := rawData
	if len(raw) == 0 {
		raw = body
	}
	fields, ok := provider.ExactObject(raw)
	if !ok || !provider.NoCaseAliases(fields, "type", "error", "code", "message") {
		return errors.New("openai: malformed stream error event")
	}
	source := fields
	if raw := provider.ExactField(fields, "error"); len(raw) > 0 && string(raw) != "null" {
		nested, ok := provider.ExactObject(raw)
		if !ok || !provider.NoCaseAliases(nested, "type", "code", "message", "param") {
			return errors.New("openai: malformed stream error event")
		}
		source = nested
	}
	rawType := provider.Discriminator(source, "type")
	rawCode := provider.Discriminator(source, "code")
	typ := canonicalType(rawType)
	code := canonicalCode(rawCode)
	message := redact(provider.ExactString(source, "message"))
	requestID := redact(header.Get("x-request-id"))
	if rawCode == "context_length_exceeded" && (status == http.StatusOK || status == http.StatusBadRequest) {
		return &harness.ContextLimitError{Status: status, Type: typ, Code: code, RequestID: requestID}
	}
	class := rawType
	if class == "" || class == "error" {
		class = rawCode
	}
	enum := code
	if rawCode == "" {
		enum = typ
	}
	text := fmt.Sprintf("openai: status %d", status)
	if enum != "" {
		text += " " + enum
	}
	if message != "" {
		text += ": " + message
	}
	if requestID != "" {
		text += " (request_id " + requestID + ")"
	}
	err := errors.New(text)
	switch class {
	case "invalid_request_error", "authentication_error", "invalid_api_key", "permission_error", "permission_denied", "not_found_error", "model_not_found":
		return provider.Unrecoverable(err)
	case "billing_error", "insufficient_quota":
		return &harness.StopError{Reason: core.StopBudgetExhausted, Err: err}
	default:
		return err
	}
}

func observeUsage(observed harness.Usage, response []byte) harness.Usage {
	usage, _, err := provider.Usage(response, false)
	if err != nil {
		return observed
	}
	return mergeUsage(observed, usage)
}

func mergeUsage(observed, next harness.Usage) harness.Usage {
	if next.InputReported {
		observed.InputTokens, observed.InputReported = next.InputTokens, true
	}
	if next.OutputReported {
		observed.OutputTokens, observed.OutputReported = next.OutputTokens, true
	}
	return observed
}
