// Package openai implements harness.Model for the streaming OpenAI Responses
// API, which other providers also serve. canti does not use the Chat
// Completions API.
package openai

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

const (
	protocol    = "openai-responses/v1"
	adapter     = "canti-openai-responses/3"
	toolAdapter = "canti-openai-responses/3-tool"
	// decisionFormat and decisionTool are strict, so a decision always
	// matches the schema. The tool choice is auto, as for every model API
	// canti supports, and a reply without the call is repaired like any
	// other reply that holds no decision.
	decisionFormat = `{"format":{"type":"json_schema","name":"` + harness.DecisionToolName + `","schema":` + harness.DecisionSchema + `,"strict":true}}`
	decisionTool   = `[{"type":"function","name":"` + harness.DecisionToolName + `","description":"` + harness.DecisionToolDescription +
		`","parameters":` + harness.DecisionSchema + `,"strict":true}]`
)

var (
	errorTypes = []string{"invalid_request_error", "authentication_error", "permission_error", "permission_denied", "not_found_error", "model_not_found", "billing_error", "insufficient_quota", "rate_limit_error", "rate_limit_exceeded", "api_error", "server_error", "invalid_api_key", "request_timeout", "conflict", "error"}
	errorCodes = []string{"context_length_exceeded", "insufficient_quota", "invalid_api_key", "model_not_found", "server_error", "rate_limit_exceeded", "invalid_value", "invalid_request_error", "parameter_missing", "request_timeout", "conflict", "permission_denied", "billing_error", "authentication_error", "not_found_error", "permission_error", "api_error"}
)

func canonicalType(value string) string { return provider.CanonicalEnum(value, errorTypes...) }
func canonicalCode(value string) string { return provider.CanonicalEnum(value, errorCodes...) }

type protocolCategory string

const (
	categoryEvent      protocolCategory = "event"
	categoryTerminal   protocolCategory = "terminal"
	categoryKind       protocolCategory = "kind"
	categoryEnum       protocolCategory = "enum"
	categoryKey        protocolCategory = "key"
	categoryDecision   protocolCategory = "decision"
	categoryClassifier protocolCategory = "classifier"
	categoryMarker     protocolCategory = "marker"
)

type protocolEntry struct {
	name     string
	category protocolCategory
}

var protocolTable = buildProtocolTable()

var protocolNames = tableNames(protocolTable)

var protocolEventSet = func() map[string]bool {
	set := map[string]bool{}
	for _, entry := range protocolTable {
		if entry.category == categoryEvent || entry.category == categoryTerminal {
			set[entry.name] = true
		}
	}
	return set
}()

func buildProtocolTable() []protocolEntry {
	var entries []protocolEntry
	add := func(category protocolCategory, names ...string) {
		for _, name := range names {
			entries = append(entries, protocolEntry{name: name, category: category})
		}
	}
	add(categoryEnum, errorTypes...)
	add(categoryEnum, errorCodes...)
	add(categoryEnum, "completed", "incomplete", "failed", "in_progress", "queued", "cancelled", "compacting", "interpreting", "generating", "searching", "tool", "null")
	for _, name := range responseEvents {
		add(categoryEvent, name)
	}
	add(categoryTerminal, "response.completed", "response.incomplete", "response.failed")
	add(categoryKind, "message", "function_call", "reasoning", "output_text", "refusal", "input_text")
	add(categoryKey,
		"response", "error", "code", "param", "request_id", "status", "output", "content", "arguments", "call_id", "name",
		"usage", "input_tokens", "output_tokens", "input_tokens_details", "output_tokens_details", "cached_tokens", "reasoning_tokens",
		"service_tier", "model", "id", "object", "incomplete_details", "reason", "type", "truncation", "store", "parallel_tool_calls",
		"tool_choice", "tools", "input", "instructions", "max_output_tokens", "reasoning", "text", "format", "json_schema", "strict",
		"schema", "description", "parameters", "additionalProperties", "required", "enum", "properties", "title", "default")
	add(categoryDecision, "exec", "return", "kind", "payload", "purpose")
	add(categoryClassifier, "credit balance", "prompt is too long", "prompt is too long:")
	add(categoryMarker, provider.RedactedMarker)
	return entries
}

func tableNames(table []protocolEntry) []string {
	out := make([]string, 0, len(table))
	for _, entry := range table {
		out = append(out, entry.name)
	}
	return out
}

// ProtocolNames returns a copy of the names the adapter's decoders recognize.
func ProtocolNames() []string { return append([]string(nil), protocolNames...) }

// ProtocolEvents returns the event and terminal names.
func ProtocolEvents() []string {
	var out []string
	for _, entry := range protocolTable {
		if entry.category == categoryEvent || entry.category == categoryTerminal {
			out = append(out, entry.name)
		}
	}
	return out
}

// ProtocolCategory reports the category of name, or the empty string.
func ProtocolCategory(name string) string {
	for _, entry := range protocolTable {
		if entry.name == name {
			return string(entry.category)
		}
	}
	return ""
}

func protocolEvent(name string) bool { return protocolEventSet[name] }

// ProtocolIsEvent reports whether name belongs to the event or terminal event category.
func ProtocolIsEvent(name string) bool { return protocolEvent(name) }

// DefaultBaseURL is the base URL of a Config that names none.
const DefaultBaseURL = "https://api.openai.com/v1"

// Efforts lists the reasoning effort levels a Config may name.
var Efforts = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}

// Config configures a Model. Zero values select the documented defaults.
type Config struct {
	BaseURL          string        // default "https://api.openai.com/v1"; the endpoint is BaseURL + "/responses"
	APIKey           string        // required; sent as Authorization: Bearer
	Model            string        // required; the adapter assumes no model
	MaxTokens        int           // max_output_tokens, default 16000
	Effort           string        // reasoning effort, one of Efforts; empty leaves the model's default
	StructuredOutput bool          // constrain text answers to the decision object schema
	DecisionFormat   string        // "text" (default) or "tool"
	IdleTimeout      time.Duration // longest wait between response bytes; default 10m
	MaxResponseBytes int64         // default 8 MiB; larger assembled responses and events are incomplete
	Client           *http.Client  // optional; redirects are never followed
}

// Model is a harness.Model backed by the Responses API.
type Model struct {
	cfg      Config
	endpoint *provider.Endpoint
}

var _ harness.Model = (*Model)(nil)

// New validates cfg and returns a Model.
func New(cfg Config) (*Model, error) {
	switch {
	case cfg.APIKey == "":
		return nil, errors.New("openai: APIKey is required")
	case cfg.Model == "":
		return nil, errors.New("openai: Model is required")
	case cfg.MaxTokens < 0:
		return nil, errors.New("openai: limits must not be negative")
	case cfg.Effort != "" && !slices.Contains(Efforts, cfg.Effort):
		return nil, fmt.Errorf("openai: unsupported effort %q", cfg.Effort)
	}
	if err := provider.CheckCredential("openai", cfg.APIKey, protocolNames); err != nil {
		return nil, err
	}
	cfg.DecisionFormat = cmp.Or(cfg.DecisionFormat, "text")
	if cfg.DecisionFormat != "text" && cfg.DecisionFormat != "tool" {
		return nil, fmt.Errorf("openai: unsupported decision format %q", cfg.DecisionFormat)
	}
	cfg.BaseURL = cmp.Or(cfg.BaseURL, DefaultBaseURL)
	cfg.MaxTokens = cmp.Or(cfg.MaxTokens, 16000)
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = provider.DefaultIdleTimeout
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = provider.DefaultMaxResponseBytes
	}
	endpoint, err := provider.New(provider.Config{Name: "openai", URL: provider.TrimBase(cfg.BaseURL, "/responses"),
		Secret: cfg.APIKey, IdleTimeout: cfg.IdleTimeout, MaxResponseBytes: cfg.MaxResponseBytes, Client: cfg.Client})
	if err != nil {
		return nil, err
	}
	return &Model{cfg: cfg, endpoint: endpoint}, nil
}

type inputText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type inputMessage struct {
	Role    string      `json:"role"`
	Content []inputText `json:"content"`
}

type reasoning struct {
	Effort string `json:"effort"`
}

// Freeze builds the deterministic request body for p: the system prompt as
// instructions and one user message with an input_text part per prompt
// block. The provider caches prompt prefixes without breakpoints, and the
// response is not stored, since the journal keeps it. It never includes
// credentials.
func (m *Model) Freeze(p harness.Prompt) (harness.Request, error) {
	if len(p.Blocks) == 0 || slices.Contains(p.Blocks, "") {
		return harness.Request{}, errors.New("openai: user prompt is empty or has an empty block")
	}
	if p.Stable < 0 || p.Stable > len(p.Blocks) {
		return harness.Request{}, fmt.Errorf("openai: %d stable blocks of %d", p.Stable, len(p.Blocks))
	}
	content := make([]inputText, len(p.Blocks))
	for i, text := range p.Blocks {
		content[i] = inputText{Type: "input_text", Text: text}
	}
	body := struct {
		Model             string          `json:"model"`
		Instructions      string          `json:"instructions,omitempty"`
		Input             []inputMessage  `json:"input"`
		MaxOutputTokens   int             `json:"max_output_tokens"`
		Stream            bool            `json:"stream"`
		Reasoning         *reasoning      `json:"reasoning,omitempty"`
		Text              json.RawMessage `json:"text,omitempty"`
		Tools             json.RawMessage `json:"tools,omitempty"`
		ToolChoice        string          `json:"tool_choice,omitempty"`
		ParallelToolCalls *bool           `json:"parallel_tool_calls,omitempty"`
		Truncation        string          `json:"truncation"`
		Store             bool            `json:"store"`
	}{Model: m.cfg.Model, Instructions: p.System, Input: []inputMessage{{"user", content}}, MaxOutputTokens: m.cfg.MaxTokens, Stream: true, Truncation: "disabled"}
	if m.cfg.Effort != "" {
		body.Reasoning = &reasoning{m.cfg.Effort}
	}
	requestAdapter := adapter
	if m.cfg.DecisionFormat == "tool" {
		parallel := false
		requestAdapter = toolAdapter
		body.Tools, body.ToolChoice, body.ParallelToolCalls = json.RawMessage(decisionTool), "auto", &parallel
	} else if m.cfg.StructuredOutput {
		body.Text = json.RawMessage(decisionFormat)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return harness.Request{}, fmt.Errorf("openai: %w", err)
	}
	return harness.Request{Body: raw, Model: m.cfg.Model, Protocol: protocol, Adapter: requestAdapter}, nil
}

// Complete sends exactly req.Body once. It returns a nil error exactly when
// the response is complete, a *harness.StopError when resending cannot help,
// and ctx's error once ctx is done.
func (m *Model) Complete(ctx context.Context, req harness.Request) (resp harness.Response, err error) {
	secret := m.cfg.APIKey
	defer func() {
		provider.SanitizeResult(secret, &resp)
		err = provider.RedactError(secret, err)
	}()
	if err := ctx.Err(); err != nil {
		return harness.Response{}, err
	}
	switch {
	case req.Adapter != adapter && req.Adapter != toolAdapter:
		return harness.Response{}, provider.Unrecoverable(fmt.Errorf("openai: unsupported adapter %q", req.Adapter))
	case req.Protocol != protocol:
		return harness.Response{}, provider.Unrecoverable(fmt.Errorf("openai: unsupported protocol %q", req.Protocol))
	}
	resp = harness.Response{Model: req.Model, Protocol: req.Protocol, Adapter: req.Adapter}
	stream, err := m.endpoint.Open(ctx, req.Body, provider.Header("authorization", "Bearer "+m.cfg.APIKey))
	if err != nil {
		return resp, err
	}
	defer stream.Close()
	if stream.Status/100 != 2 {
		body, readErr := stream.Body()
		raw := stream.RawBody()
		resp.Body = body
		usage, _, usageErr := provider.Usage(raw, false)
		resp.Usage = usage
		if readErr != nil {
			return resp, readErr
		}
		if usageErr != nil {
			return resp, usageErr
		}
		return resp, statusError(provider.Reply{Status: stream.Status, Header: stream.Header, Body: raw}, m.cfg.APIKey)
	}
	body, usage, terminal, streamErr := readResponseStream(stream)
	body, limitErr := provider.Bound("openai", body, m.cfg.MaxResponseBytes)
	resp.Body, resp.Usage = body, usage
	if limitErr != nil {
		return resp, limitErr
	}
	if streamErr != nil {
		return resp, streamErr
	}
	resp.Model = cmp.Or(terminal.model, resp.Model)
	switch terminal.kind {
	case "completed":
		resp.Complete = true
		extraction := m.extract(terminal.raw, body, req.Adapter, true)
		resp.Extraction = &extraction
		return resp, nil
	case "incomplete":
		if terminal.reason == "" {
			return resp, fmt.Errorf("openai: incomplete response: status %q", terminal.kind)
		}
		return resp, fmt.Errorf("openai: incomplete response: %s", terminal.reason)
	case "failed":
		if !terminal.hasError {
			return resp, fmt.Errorf("openai: failed response: status %q", terminal.kind)
		}
		return resp, fmt.Errorf("openai: failed response: %s: %s", terminal.errorCode, terminal.errorMessage)
	default:
		return resp, fmt.Errorf("openai: incomplete response: status %q", terminal.kind)
	}
}

// Decision returns the committed extraction of a saved response. Synthetic
// responses without one are read from their own body; provider responses
// always carry the extraction computed from raw bytes by Complete.
func (m *Model) Decision(resp harness.Response) []byte {
	if resp.Adapter != "" && resp.Adapter != adapter && resp.Adapter != toolAdapter {
		return nil
	}
	if resp.Adapter == toolAdapter && resp.Extraction != nil && !resp.Extraction.Found {
		return nil
	}
	if resp.Extraction != nil {
		return resp.Extraction.DecisionBody()
	}
	return nil
}

func sanitizeDecision(secret string, decision []byte) []byte {
	if !utf8.Valid(decision) {
		return []byte(provider.RedactValue(secret, string(decision)))
	}
	if !json.Valid(decision) {
		return []byte(provider.RedactValue(secret, string(decision)))
	}
	out := provider.RedactJSON(secret, decision)
	fields, ok := provider.ExactObject(out)
	if !ok {
		return out
	}
	safeFields := make(map[string]json.RawMessage, len(fields))
	changed := false
	for name, raw := range fields {
		safe := provider.Ident(secret, name)
		changed = changed || safe != name
		safeFields[safe] = raw
	}
	kind := provider.Discriminator(safeFields, "kind")
	safeKind := provider.Ident(secret, kind)
	if kind != "" && safeKind != kind {
		safeFields["kind"], _ = json.Marshal(safeKind)
		changed = true
	}
	if !changed {
		return out
	}
	encoded, err := json.Marshal(safeFields)
	if err != nil {
		return []byte(provider.RedactedMarker)
	}
	return encoded
}

func (m *Model) extract(rawBody, body []byte, adapterName string, complete bool) harness.Extraction {
	secret := m.cfg.APIKey
	input := rawBody
	if !utf8.Valid(rawBody) {
		input = body
	}
	var decision []byte
	switch adapterName {
	case toolAdapter:
		decision = toolDecision(harness.Response{Body: input, Complete: complete, Adapter: adapterName})
	case "", adapter:
		decision = textDecision(input)
	}
	if len(decision) == 0 {
		diagnostic := "no decision"
		if _, err := core.Decode(sanitizeDecision(secret, input)); err != nil {
			diagnostic = err.Error()
		}
		return harness.Extraction{Body: body, Diagnostic: provider.RedactValue(secret, diagnostic)}
	}
	decision = sanitizeDecision(secret, decision)
	decoded, err := core.Decode(decision)
	if err != nil {
		return harness.Extraction{Body: provider.RedactJSON(secret, decision), Found: true, Diagnostic: provider.RedactValue(secret, err.Error())}
	}
	decoded.Payload = provider.RedactValue(secret, decoded.Payload)
	decoded.Purpose = provider.RedactValue(secret, decoded.Purpose)
	return harness.Extraction{Decision: &decoded, Found: true}
}

func textDecision(body []byte) []byte {
	fields, ok := provider.ExactObject(body)
	var output []json.RawMessage
	if !ok || json.Unmarshal(provider.ExactField(fields, "output"), &output) != nil {
		return nil
	}
	var text []byte
	for _, raw := range output {
		item, ok := provider.ExactObject(raw)
		if !ok || provider.ExactString(item, "type") != "message" {
			continue
		}
		var content []json.RawMessage
		if json.Unmarshal(provider.ExactField(item, "content"), &content) != nil {
			continue
		}
		for _, partRaw := range content {
			part, ok := provider.ExactObject(partRaw)
			if ok && provider.ExactString(part, "type") == "output_text" {
				text = append(text, provider.ExactString(part, "text")...)
			}
		}
	}
	if len(text) == 0 {
		return nil
	}
	return text
}

// toolDecision returns the arguments of the single decision call of a
// complete response, or nil. Reasoning and message items may accompany it.
func toolDecision(resp harness.Response) []byte {
	if !resp.Complete {
		return nil
	}
	fields, ok := provider.ExactObject(resp.Body)
	var output []json.RawMessage
	if !ok || json.Unmarshal(fields["output"], &output) != nil {
		return nil
	}
	var arguments []byte
	for _, raw := range output {
		item, ok := provider.ExactObject(raw)
		if !ok {
			return nil
		}
		switch provider.ExactString(item, "type") {
		case "function_call":
			candidate := provider.ExactString(item, "arguments")
			if arguments != nil || provider.ExactString(item, "call_id") == "" || provider.ExactString(item, "name") != harness.DecisionToolName || candidate == "" {
				return nil
			}
			arguments = []byte(candidate)
		case "reasoning", "message":
		default:
			return nil
		}
	}
	return arguments
}

func validateResponse(fields map[string]json.RawMessage) error {
	if !provider.NoCaseAliases(fields, "id", "object", "model", "status", "incomplete_details", "error", "output", "usage") {
		return malformedResponse()
	}
	for _, name := range []string{"model", "status"} {
		if raw, ok := fields[name]; ok && (len(raw) == 0 || raw[0] != '"') {
			return malformedResponse()
		}
	}
	for _, name := range []string{"incomplete_details", "error"} {
		if raw, ok := fields[name]; ok && string(raw) != "null" {
			if _, ok := provider.ExactObject(raw); !ok {
				return malformedResponse()
			}
		}
	}
	if raw := provider.ExactField(fields, "output"); len(raw) > 0 {
		if err := validateOutput(raw); err != nil {
			return err
		}
	}
	return nil
}

func validateOutput(raw json.RawMessage) error {
	if raw[0] != '[' {
		return malformedResponse()
	}
	var output []json.RawMessage
	if json.Unmarshal(raw, &output) != nil {
		return malformedResponse()
	}
	for _, item := range output {
		itemFields, ok := provider.ExactObject(item)
		if !ok || !provider.NoCaseAliases(itemFields, "type") {
			return malformedResponse()
		}
		switch provider.Discriminator(itemFields, "type") {
		case "":
			return malformedResponse()
		case "message":
			if err := validateMessageContent(itemFields); err != nil {
				return err
			}
		case "function_call":
			if raw := provider.ExactField(itemFields, "arguments"); len(raw) == 0 || raw[0] != '"' {
				return malformedResponse()
			}
		}
	}
	return nil
}

func validateMessageContent(item map[string]json.RawMessage) error {
	raw := provider.ExactField(item, "content")
	if len(raw) == 0 || raw[0] != '[' {
		return malformedResponse()
	}
	var content []json.RawMessage
	if json.Unmarshal(raw, &content) != nil {
		return malformedResponse()
	}
	for _, part := range content {
		partFields, ok := provider.ExactObject(part)
		if !ok {
			return malformedResponse()
		}
		if err := validateContentPart(partFields); err != nil {
			return err
		}
	}
	return nil
}

func validateContentPart(part map[string]json.RawMessage) error {
	switch provider.Discriminator(part, "type") {
	case "output_text":
		if raw := provider.ExactField(part, "text"); len(raw) == 0 || raw[0] != '"' {
			return malformedResponse()
		}
	case "refusal":
		if raw := provider.ExactField(part, "refusal"); len(raw) == 0 || raw[0] != '"' {
			return malformedResponse()
		}
	case "":
		return malformedResponse()
	}
	return nil
}

func malformedResponse() error { return errors.New("openai: malformed response") }

func redactedContextLimit(secret string, limit harness.ContextLimitError) *harness.ContextLimitError {
	limit.RequestID = provider.RedactValue(secret, limit.RequestID)
	return &limit
}

func statusError(r provider.Reply, secret string) error {
	fields, exact := provider.ExactObject(r.Body)
	errorFields, nested := provider.ExactObject(provider.ExactField(fields, "error"))
	rawType, rawCode, message := "", "", ""
	if exact && nested {
		rawType, rawCode, message = provider.Discriminator(errorFields, "type"), provider.Discriminator(errorFields, "code"), provider.ExactString(errorFields, "message")
	}
	requestID := provider.RedactValue(secret, r.Header.Get("x-request-id"))
	if exact && nested && r.Status == http.StatusBadRequest && rawCode == "context_length_exceeded" {
		return &harness.ContextLimitError{Status: r.Status, Type: canonicalType(rawType), Code: canonicalCode(rawCode), RequestID: requestID}
	}
	enum := canonicalCode(rawCode)
	if rawCode == "" {
		enum = canonicalType(rawType)
	}
	return provider.StatusError("openai", r.Status, provider.Failure{
		Type: enum, Message: provider.RedactValue(secret, message), RequestID: requestID,
		Exhausted: rawCode == "insufficient_quota",
	})
}
