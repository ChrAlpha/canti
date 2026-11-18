// Package anthropic implements harness.Model for the streaming Anthropic
// Messages API.
package anthropic

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

const (
	version        = "2023-06-01"
	protocol       = "anthropic-messages/" + version
	fallbackBeta   = "server-side-fallback-2026-07-01"
	adapter        = "canti-anthropic/5"
	toolAdapter    = "canti-anthropic/5-tool"
	decisionFormat = `{"type":"json_schema","schema":` + harness.DecisionSchema + `}`
	// decisionTool is strict, so a tool call always matches the decision
	// schema. The tool choice is auto: current models reject a forced one,
	// and a reply without the call is repaired like any other reply that
	// holds no decision.
	decisionTool = `[{"name":"` + harness.DecisionToolName + `","description":"` + harness.DecisionToolDescription +
		`","strict":true,"input_schema":` + harness.DecisionSchema + `}]`
	decisionToolChoice = `{"type":"auto","disable_parallel_tool_use":true}`
)

var (
	errorTypes  = []string{"invalid_request_error", "authentication_error", "billing_error", "permission_error", "not_found_error", "request_too_large", "rate_limit_error", "api_error", "overloaded_error", "timeout_error", "conflict_error"}
	errorCodes  = []string{"prompt_too_long", "context_length_exceeded"}
	stopReasons = []string{"end_turn", "stop_sequence", "refusal", "tool_use", "max_tokens", "pause_turn", "model_context_window_exceeded"}
)

func canonicalType(value string) string       { return provider.CanonicalEnum(value, errorTypes...) }
func canonicalStopReason(value string) string { return provider.CanonicalEnum(value, stopReasons...) }

type protocolCategory string

const (
	categoryEvent      protocolCategory = "event"
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
		if entry.category == categoryEvent {
			set[entry.name] = true
		}
	}
	return set
}()

var protocolKindSet = func() map[string]bool {
	set := map[string]bool{}
	for _, entry := range protocolTable {
		if entry.category == categoryKind {
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
	add(categoryEnum, stopReasons...)
	add(categoryEnum, "tool", "null")
	add(categoryEvent, "message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "message_stop", "ping", "error")
	add(categoryKind, "text", "thinking", "tool_use", "redacted_thinking", "fallback", "text_delta", "thinking_delta", "signature_delta", "input_json_delta", "citations_delta")
	add(categoryKey,
		"type", "message", "content_block", "delta", "index", "signature", "partial_json", "citation", "citations", "input",
		"usage", "model", "stop_reason", "stop_sequence", "stop_details", "id", "role", "request_id", "to", "content", "stream",
		"input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "cache_creation", "ephemeral", "ttl", "cache_control",
		"service_tier", "context_management", "container", "metadata", "tools", "tool_choice", "tool_result", "system", "messages",
		"max_tokens", "temperature", "top_p", "top_k", "budget_tokens", "effort", "source", "title", "url", "document", "image", "text", "thinking", "tool_use", "error")
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

// ProtocolEvents returns the event names, including terminals when present.
func ProtocolEvents() []string {
	var out []string
	for _, entry := range protocolTable {
		if entry.category == categoryEvent {
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
func protocolKind(name string) bool  { return protocolKindSet[name] }

// ProtocolIsEvent reports whether name belongs to the event or terminal event category.
func ProtocolIsEvent(name string) bool { return protocolEvent(name) }

// DefaultBaseURL is the base URL of a Config that names none.
const DefaultBaseURL = "https://api.anthropic.com"

// Efforts lists the output_config effort levels a Config may name.
var Efforts = []string{"low", "medium", "high", "xhigh", "max"}

// Config configures a Model. Zero values select the documented defaults.
type Config struct {
	BaseURL          string        // default "https://api.anthropic.com"
	APIKey           string        // sent as x-api-key
	AuthToken        string        // sent as Authorization: Bearer; exactly one of APIKey/AuthToken is required
	Model            string        // required; the adapter assumes no model
	MaxTokens        int           // default 16000
	Effort           string        // output_config effort, one of Efforts; empty leaves the model's default
	StructuredOutput bool          // constrain text answers to the decision object schema
	DecisionFormat   string        // "text" (default) or "tool"
	Fallbacks        bool          // enable server-side refusal fallbacks
	IdleTimeout      time.Duration // longest wait between response bytes; default 10m
	MaxResponseBytes int64         // default 8 MiB; larger assembled responses and events are incomplete
	Client           *http.Client  // optional; redirects are never followed
}

// Model is a harness.Model backed by the Anthropic Messages API.
type Model struct {
	cfg      Config
	endpoint *provider.Endpoint
}

var _ harness.Model = (*Model)(nil)

// New validates cfg and returns a Model.
func New(cfg Config) (*Model, error) {
	if (cfg.APIKey == "") == (cfg.AuthToken == "") {
		return nil, errors.New("anthropic: exactly one of APIKey and AuthToken is required")
	}
	if cfg.Model == "" {
		return nil, errors.New("anthropic: Model is required")
	}
	if err := provider.CheckCredential("anthropic", cfg.APIKey, protocolNames); err != nil {
		return nil, err
	}
	if err := provider.CheckCredential("anthropic", cfg.AuthToken, protocolNames); err != nil {
		return nil, err
	}
	if cfg.MaxTokens < 0 {
		return nil, errors.New("anthropic: limits must not be negative")
	}
	if cfg.Effort != "" && !slices.Contains(Efforts, cfg.Effort) {
		return nil, fmt.Errorf("anthropic: unsupported effort %q", cfg.Effort)
	}
	if cfg.DecisionFormat == "" {
		cfg.DecisionFormat = "text"
	}
	if cfg.DecisionFormat != "text" && cfg.DecisionFormat != "tool" {
		return nil, fmt.Errorf("anthropic: unsupported decision format %q", cfg.DecisionFormat)
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = 16000
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = provider.DefaultIdleTimeout
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = provider.DefaultMaxResponseBytes
	}
	endpoint, err := provider.New(provider.Config{Name: "anthropic", URL: provider.TrimBase(cfg.BaseURL, "/v1/messages"),
		Secret: cfg.APIKey + cfg.AuthToken, IdleTimeout: cfg.IdleTimeout, MaxResponseBytes: cfg.MaxResponseBytes, Client: cfg.Client})
	if err != nil {
		return nil, err
	}
	return &Model{cfg: cfg, endpoint: endpoint}, nil
}

type userMessage struct {
	Role    string      `json:"role"`
	Content []textBlock `json:"content"`
}

type textBlock struct {
	Type         string          `json:"type"`
	Text         string          `json:"text"`
	CacheControl json.RawMessage `json:"cache_control,omitempty"`
}

type outputConfig struct {
	Effort string          `json:"effort,omitempty"`
	Format json.RawMessage `json:"format,omitempty"`
}

// Freeze builds the deterministic request body for p, one text block per
// prompt block, with cache breakpoints on the first and the last stable
// block, so the first block stays reusable after a step is dropped. It never
// includes credentials.
func (m *Model) Freeze(p harness.Prompt) (harness.Request, error) {
	if len(p.Blocks) == 0 || slices.Contains(p.Blocks, "") {
		return harness.Request{}, errors.New("anthropic: user prompt is empty or has an empty block")
	}
	if p.Stable < 0 || p.Stable > len(p.Blocks) {
		return harness.Request{}, fmt.Errorf("anthropic: %d stable blocks of %d", p.Stable, len(p.Blocks))
	}
	content := make([]textBlock, len(p.Blocks))
	for i, text := range p.Blocks {
		content[i] = textBlock{Type: "text", Text: text}
	}
	if p.Stable > 0 {
		content[0].CacheControl = json.RawMessage(`{"type":"ephemeral"}`)
		content[p.Stable-1].CacheControl = content[0].CacheControl
	}
	body := struct {
		Model        string          `json:"model"`
		MaxTokens    int             `json:"max_tokens"`
		Stream       bool            `json:"stream"`
		System       string          `json:"system,omitempty"`
		Messages     []userMessage   `json:"messages"`
		OutputConfig *outputConfig   `json:"output_config,omitempty"`
		Tools        json.RawMessage `json:"tools,omitempty"`
		ToolChoice   json.RawMessage `json:"tool_choice,omitempty"`
		Fallbacks    string          `json:"fallbacks,omitempty"`
	}{Model: m.cfg.Model, MaxTokens: m.cfg.MaxTokens, Stream: true, System: p.System, Messages: []userMessage{{"user", content}}}
	proto := protocol
	requestAdapter := adapter
	output := outputConfig{Effort: m.cfg.Effort}
	if m.cfg.DecisionFormat == "tool" {
		requestAdapter = toolAdapter
		body.Tools = json.RawMessage(decisionTool)
		body.ToolChoice = json.RawMessage(decisionToolChoice)
	} else if m.cfg.StructuredOutput {
		output.Format = json.RawMessage(decisionFormat)
	}
	if output.Effort != "" || output.Format != nil {
		body.OutputConfig = &output
	}
	if m.cfg.Fallbacks {
		body.Fallbacks, proto = "default", protocol+"+"+fallbackBeta
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return harness.Request{}, fmt.Errorf("anthropic: %w", err)
	}
	return harness.Request{Body: raw, Model: m.cfg.Model, Protocol: proto, Adapter: requestAdapter}, nil
}

// Complete sends exactly req.Body once. It returns a nil error exactly when
// the response is complete, a *harness.StopError when resending cannot help,
// and ctx's error once ctx is done.
func (m *Model) Complete(ctx context.Context, req harness.Request) (resp harness.Response, err error) {
	secret := m.cfg.APIKey + m.cfg.AuthToken
	defer func() {
		provider.SanitizeResult(secret, &resp)
		err = provider.RedactError(secret, err)
	}()
	if err := ctx.Err(); err != nil {
		return harness.Response{}, err
	}
	header := provider.Header("anthropic-version", version)
	switch {
	case req.Adapter != adapter && req.Adapter != toolAdapter:
		return harness.Response{}, provider.Unrecoverable(fmt.Errorf("anthropic: unsupported adapter %q", req.Adapter))
	case req.Protocol == protocol+"+"+fallbackBeta:
		header.Set("anthropic-beta", fallbackBeta)
	case req.Protocol != protocol:
		return harness.Response{}, provider.Unrecoverable(fmt.Errorf("anthropic: unsupported protocol %q", req.Protocol))
	}
	if m.cfg.APIKey != "" {
		header.Set("x-api-key", m.cfg.APIKey)
	} else {
		header.Set("authorization", "Bearer "+m.cfg.AuthToken)
	}
	resp = harness.Response{Model: req.Model, Protocol: req.Protocol, Adapter: req.Adapter}
	stream, err := m.endpoint.Open(ctx, req.Body, header)
	if err != nil {
		return resp, err
	}
	defer stream.Close()
	if stream.Status/100 != 2 {
		body, readErr := stream.Body()
		raw := stream.RawBody()
		resp.Body = body
		usage, _, usageErr := provider.Usage(raw, true)
		resp.Usage = usage
		if readErr != nil {
			return resp, readErr
		}
		if usageErr != nil {
			return resp, usageErr
		}
		return resp, statusError(provider.Reply{Status: stream.Status, Header: stream.Header, Body: raw}, m.cfg.APIKey+m.cfg.AuthToken)
	}
	body, rawBody, usage, invalidUTF8, streamErr := readMessageStream(stream, m.cfg.MaxResponseBytes)
	body, limitErr := provider.Bound("anthropic", body, m.cfg.MaxResponseBytes)
	resp.Body, resp.Usage = body, usage
	if limitErr != nil {
		return resp, limitErr
	}
	if streamErr != nil && !invalidUTF8 {
		return resp, streamErr
	}
	if invalidUTF8 {
		resp.Complete = true
		extraction := m.extract(body, body, req.Adapter, true)
		if extraction.Decision != nil {
			extraction.Body = extraction.DecisionBody()
			extraction.Decision = nil
		}
		if len(extraction.Body) == 0 {
			extraction.Body = body
		}
		extraction.Diagnostic = "core: invalid decision: invalid UTF-8"
		extraction.Found = true
		resp.Extraction = &extraction
		return resp, nil
	}

	fields, ok := provider.ExactObject(rawBody)
	if !ok || !provider.NoCaseAliases(fields, "id", "type", "role", "model", "content", "stop_reason", "stop_sequence", "usage") {
		return resp, errors.New("anthropic: malformed response envelope")
	}
	if model := provider.ExactString(fields, "model"); model != "" {
		resp.Model = provider.RedactValue(m.cfg.APIKey+m.cfg.AuthToken, model)
	}
	stopReason := provider.Discriminator(fields, "stop_reason")
	switch stopReason {
	case "model_context_window_exceeded":
		if stream.Status != http.StatusOK {
			return resp, errors.New("anthropic: context stop reason outside documented status")
		}
		return resp, redactedContextLimit(m.cfg.APIKey+m.cfg.AuthToken, harness.ContextLimitError{Status: stream.Status, StopReason: canonicalStopReason(stopReason), RequestID: stream.Header.Get("request-id")})
	case "end_turn", "stop_sequence", "refusal":
		resp.Complete = true
		extraction := m.extract(rawBody, body, req.Adapter, true)
		resp.Extraction = &extraction
		return resp, nil
	case "tool_use":
		if req.Adapter == toolAdapter {
			resp.Complete = true
			extraction := m.extract(rawBody, body, req.Adapter, true)
			resp.Extraction = &extraction
			return resp, nil
		}
	}
	return resp, fmt.Errorf("anthropic: incomplete response: stop_reason %q", provider.Ident(m.cfg.APIKey+m.cfg.AuthToken, stopReason))
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
	secret := m.cfg.APIKey + m.cfg.AuthToken
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
	var content []json.RawMessage
	if !ok || json.Unmarshal(provider.ExactField(fields, "content"), &content) != nil {
		return nil
	}
	var text []byte
	for _, raw := range content {
		block, ok := provider.ExactObject(raw)
		if !ok {
			return nil
		}
		if provider.ExactString(block, "type") != "text" {
			continue
		}
		text = append(text, provider.ExactString(block, "text")...)
	}
	if len(text) == 0 {
		return nil
	}
	return text
}

func redactedContextLimit(secret string, limit harness.ContextLimitError) *harness.ContextLimitError {
	limit.RequestID = provider.RedactValue(secret, limit.RequestID)
	return &limit
}

func statusError(r provider.Reply, secret string) error {
	fields, exact := provider.ExactObject(r.Body)
	errorFields, nested := provider.ExactObject(provider.ExactField(fields, "error"))
	rawType, message := "", ""
	if exact && nested {
		rawType, message = provider.Discriminator(errorFields, "type"), provider.ExactString(errorFields, "message")
	}
	requestID := provider.RedactValue(secret, cmp.Or(provider.ExactString(fields, "request_id"), r.Header.Get("request-id")))
	if exact && nested && r.Status == http.StatusBadRequest && rawType == "invalid_request_error" && strings.HasPrefix(message, "prompt is too long:") {
		return redactedContextLimit(secret, harness.ContextLimitError{Status: r.Status, Type: "invalid_request_error", Code: "prompt_too_long", RequestID: requestID})
	}
	return provider.StatusError("anthropic", r.Status, provider.Failure{
		Type: canonicalType(rawType), Message: provider.RedactValue(secret, message), RequestID: requestID,
		Exhausted: r.Status == http.StatusBadRequest && strings.Contains(strings.ToLower(message), "credit balance"),
	})
}
