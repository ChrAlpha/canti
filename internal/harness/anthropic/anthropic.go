// Package anthropic implements harness.Model for the non-streaming Anthropic
// Messages API.
//
// It uses net/http instead of the official SDK so that every attempt sends
// exactly the frozen request bytes, is counted by the harness without hidden
// retries, and keeps the raw response body.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const (
	version        = "2023-06-01"
	protocol       = "anthropic-messages/" + version
	fallbackBeta   = "server-side-fallback-2026-07-01"
	adapter        = "canti-anthropic/2"
	toolAdapter    = "canti-anthropic/3-tool"
	purposeSchema  = `"purpose":{"type":"string","description":"One short line: what this step is for and what you will do next with its result."}`
	decisionFormat = `{"format":{"type":"json_schema","schema":{"type":"object","properties":{` +
		`"kind":{"type":"string","enum":["exec","return"]},` + purposeSchema + `,"payload":{"type":"string"}},` +
		`"required":["kind","payload","purpose"],"additionalProperties":false}}}`
)

// Config configures a Model. Zero values select the documented defaults.
type Config struct {
	BaseURL          string        // default "https://api.anthropic.com"
	APIKey           string        // sent as x-api-key
	AuthToken        string        // sent as Authorization: Bearer; exactly one of APIKey/AuthToken is required
	Model            string        // required, e.g. "claude-opus-5"
	MaxTokens        int           // default 16000
	StructuredOutput bool          // constrain text answers to the decision object schema
	DecisionFormat   string        // "text" (default) or "tool"
	Fallbacks        bool          // enable server-side refusal fallbacks
	Timeout          time.Duration // per-attempt HTTP timeout; default 10m
	MaxResponseBytes int64         // default 8 MiB; larger bodies are incomplete
	Client           *http.Client  // optional; redirects are never followed
}

// Model is a harness.Model backed by the Anthropic Messages API.
type Model struct {
	cfg      Config
	client   *http.Client
	endpoint string
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
	if cfg.MaxTokens < 0 || cfg.Timeout < 0 || cfg.MaxResponseBytes < 0 {
		return nil, errors.New("anthropic: limits must not be negative")
	}
	if cfg.DecisionFormat == "" {
		cfg.DecisionFormat = "text"
	}
	if cfg.DecisionFormat != "text" && cfg.DecisionFormat != "tool" {
		return nil, fmt.Errorf("anthropic: unsupported decision format %q", cfg.DecisionFormat)
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.anthropic.com"
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("anthropic: BaseURL must be an absolute http(s) URL")
	}
	if ip := net.ParseIP(u.Hostname()); u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("anthropic: BaseURL must use https unless it is a loopback address")
	}
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = 16000
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Minute
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = 8 << 20
	}
	client := http.Client{}
	if cfg.Client != nil {
		client = *cfg.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Model{cfg: cfg, client: &client, endpoint: strings.TrimRight(cfg.BaseURL, "/") + "/v1/messages"}, nil
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
		System       string          `json:"system,omitempty"`
		Messages     []userMessage   `json:"messages"`
		OutputConfig json.RawMessage `json:"output_config,omitempty"`
		Tools        json.RawMessage `json:"tools,omitempty"`
		ToolChoice   json.RawMessage `json:"tool_choice,omitempty"`
		Fallbacks    string          `json:"fallbacks,omitempty"`
	}{Model: m.cfg.Model, MaxTokens: m.cfg.MaxTokens, System: p.System, Messages: []userMessage{{"user", content}}}
	proto := protocol
	requestAdapter := adapter
	if m.cfg.DecisionFormat == "tool" {
		requestAdapter = toolAdapter
		body.Tools = json.RawMessage(decisionTool)
		body.ToolChoice = json.RawMessage(decisionToolChoice)
	} else if m.cfg.StructuredOutput {
		body.OutputConfig = json.RawMessage(decisionFormat)
	}
	if m.cfg.Fallbacks {
		body.Fallbacks, proto = "default", protocol+"+"+fallbackBeta
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return harness.Request{}, fmt.Errorf("anthropic: %w", err)
	}
	return harness.Request{MaxOutputTokens: m.cfg.MaxTokens, Body: raw, Model: m.cfg.Model, Protocol: proto, Adapter: requestAdapter}, nil
}

type message struct {
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	Content    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Usage struct {
		InputTokens              int64 `json:"input_tokens"`
		OutputTokens             int64 `json:"output_tokens"`
		CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
		CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	} `json:"usage"`
}

// Complete sends exactly req.Body once. It returns a nil error exactly when
// the response is complete, a *harness.StopError when resending cannot help,
// and ctx's error once ctx is done.
func (m *Model) Complete(ctx context.Context, req harness.Request) (harness.Response, error) {
	if err := ctx.Err(); err != nil {
		return harness.Response{}, err
	}
	var beta string
	switch {
	case req.Adapter != adapter && req.Adapter != toolAdapter && req.Adapter != summaryAdapter:
		return harness.Response{}, unrecoverable(fmt.Errorf("anthropic: unsupported adapter %q", req.Adapter))
	case req.Protocol == protocol+"+"+fallbackBeta:
		beta = fallbackBeta
	case req.Protocol != protocol:
		return harness.Response{}, unrecoverable(fmt.Errorf("anthropic: unsupported protocol %q", req.Protocol))
	}
	resp := harness.Response{Model: req.Model, Protocol: req.Protocol, Adapter: req.Adapter}

	attempt, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	hr, err := http.NewRequestWithContext(attempt, http.MethodPost, m.endpoint, bytes.NewReader(req.Body))
	if err != nil {
		return resp, unrecoverable(fmt.Errorf("anthropic: %w", err))
	}
	hr.Header.Set("content-type", "application/json")
	hr.Header.Set("anthropic-version", version)
	if m.cfg.APIKey != "" {
		hr.Header.Set("x-api-key", m.cfg.APIKey)
	} else {
		hr.Header.Set("authorization", "Bearer "+m.cfg.AuthToken)
	}
	if beta != "" {
		hr.Header.Set("anthropic-beta", beta)
	}
	res, err := m.client.Do(hr)
	if err != nil {
		return resp, m.failure(ctx, attempt, err)
	}
	defer res.Body.Close()
	resp.Body, err = io.ReadAll(io.LimitReader(res.Body, m.cfg.MaxResponseBytes+1))
	resp.Body = bytes.ReplaceAll(resp.Body, []byte(m.cfg.APIKey+m.cfg.AuthToken), []byte("[redacted]"))
	if err != nil || ctx.Err() != nil {
		return resp, m.failure(ctx, attempt, err)
	}
	if int64(len(resp.Body)) > m.cfg.MaxResponseBytes {
		resp.Body = resp.Body[:m.cfg.MaxResponseBytes]
		return resp, fmt.Errorf("anthropic: response exceeds %d bytes", m.cfg.MaxResponseBytes)
	}
	if res.StatusCode/100 != 2 {
		return resp, statusError(res, resp.Body)
	}

	var msg message
	if err := json.Unmarshal(resp.Body, &msg); err != nil {
		return resp, fmt.Errorf("anthropic: malformed response: %w", err)
	}
	if msg.Model != "" {
		resp.Model = msg.Model
	}
	resp.Usage, resp.UsageKnown = reportedUsage(resp.Body)
	if req.Adapter == toolAdapter {
		fields, ok := exactObject(resp.Body)
		if !ok {
			return resp, errors.New("anthropic: malformed tool response")
		}
		msg.StopReason = exactString(fields, "stop_reason")
	}
	switch msg.StopReason {
	case "end_turn", "stop_sequence", "refusal":
		resp.Complete = true
		return resp, nil
	case "tool_use":
		if req.Adapter == toolAdapter {
			resp.Complete = true
			return resp, nil
		}
	}
	return resp, fmt.Errorf("anthropic: incomplete response: stop_reason %q", msg.StopReason)
}

// Decision concatenates the text blocks of a saved response. A body without
// text is returned unchanged so the strict decoder rejects it.
func (m *Model) Decision(resp harness.Response) []byte {
	if resp.Adapter == toolAdapter {
		return toolDecision(resp)
	}
	if resp.Adapter != "" && resp.Adapter != adapter {
		return nil
	}
	var msg message
	if json.Unmarshal(resp.Body, &msg) != nil {
		return resp.Body
	}
	var text []byte
	for _, block := range msg.Content {
		if block.Type == "text" {
			text = append(text, block.Text...)
		}
	}
	if len(text) == 0 {
		return resp.Body
	}
	return text
}

func (m *Model) failure(ctx, attempt context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("anthropic: %w", ctx.Err())
	case attempt.Err() != nil:
		return fmt.Errorf("anthropic: attempt timed out after %v", m.cfg.Timeout)
	}
	return fmt.Errorf("anthropic: %w", err)
}

func statusError(res *http.Response, body []byte) error {
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(body, &e)
	text := fmt.Sprintf("anthropic: status %d", res.StatusCode)
	if e.Error.Type != "" {
		text += " " + e.Error.Type
	}
	if e.Error.Message != "" {
		text += ": " + e.Error.Message
	}
	if e.RequestID == "" {
		e.RequestID = res.Header.Get("request-id")
	}
	if e.RequestID != "" {
		text += " (request_id " + e.RequestID + ")"
	}
	err := errors.New(text)
	code := res.StatusCode
	switch {
	case code == http.StatusPaymentRequired ||
		code == http.StatusBadRequest && strings.Contains(strings.ToLower(e.Error.Message), "credit balance"):
		return &harness.StopError{Reason: core.StopBudgetExhausted, Err: err}
	case code == http.StatusRequestTimeout || code == http.StatusConflict || code == http.StatusTooManyRequests || code >= 500:
		return err
	}
	return unrecoverable(err)
}

func unrecoverable(err error) error {
	return &harness.StopError{Reason: core.StopUnrecoverable, Err: err}
}
