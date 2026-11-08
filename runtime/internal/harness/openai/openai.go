// Package openai implements harness.Model for the non-streaming OpenAI
// Responses API, which other providers also serve. canti does not use the
// Chat Completions API.
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

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

const (
	protocol    = "openai-responses/v1"
	adapter     = "canti-openai-responses/2"
	toolAdapter = "canti-openai-responses/2-tool"
	// decisionFormat and decisionTool are strict, so a decision always
	// matches the schema. The tool choice is auto, as for every model API
	// canti supports, and a reply without the call is repaired like any
	// other reply that holds no decision.
	decisionFormat = `{"format":{"type":"json_schema","name":"` + harness.DecisionToolName + `","schema":` + harness.DecisionSchema + `,"strict":true}}`
	decisionTool   = `[{"type":"function","name":"` + harness.DecisionToolName + `","description":"` + harness.DecisionToolDescription +
		`","parameters":` + harness.DecisionSchema + `,"strict":true}]`
)

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
	Timeout          time.Duration // per-attempt HTTP timeout; default 10m
	MaxResponseBytes int64         // default 8 MiB; larger bodies are incomplete
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
	cfg.DecisionFormat = cmp.Or(cfg.DecisionFormat, "text")
	if cfg.DecisionFormat != "text" && cfg.DecisionFormat != "tool" {
		return nil, fmt.Errorf("openai: unsupported decision format %q", cfg.DecisionFormat)
	}
	cfg.BaseURL = cmp.Or(cfg.BaseURL, DefaultBaseURL)
	cfg.MaxTokens = cmp.Or(cfg.MaxTokens, 16000)
	endpoint, err := provider.New(provider.Config{Name: "openai", URL: provider.TrimBase(cfg.BaseURL, "/responses"),
		Secret: cfg.APIKey, Timeout: cfg.Timeout, MaxResponseBytes: cfg.MaxResponseBytes, Client: cfg.Client})
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
		Reasoning         *reasoning      `json:"reasoning,omitempty"`
		Text              json.RawMessage `json:"text,omitempty"`
		Tools             json.RawMessage `json:"tools,omitempty"`
		ToolChoice        string          `json:"tool_choice,omitempty"`
		ParallelToolCalls *bool           `json:"parallel_tool_calls,omitempty"`
		Truncation        string          `json:"truncation"`
		Store             bool            `json:"store"`
	}{Model: m.cfg.Model, Instructions: p.System, Input: []inputMessage{{"user", content}}, MaxOutputTokens: m.cfg.MaxTokens, Truncation: "disabled"}
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

type response struct {
	Model             string `json:"model"`
	Status            string `json:"status"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Output []struct {
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	} `json:"output"`
	Usage struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

// Complete sends exactly req.Body once. It returns a nil error exactly when
// the response is complete, a *harness.StopError when resending cannot help,
// and ctx's error once ctx is done.
func (m *Model) Complete(ctx context.Context, req harness.Request) (harness.Response, error) {
	if err := ctx.Err(); err != nil {
		return harness.Response{}, err
	}
	switch {
	case req.Adapter != adapter && req.Adapter != toolAdapter:
		return harness.Response{}, provider.Unrecoverable(fmt.Errorf("openai: unsupported adapter %q", req.Adapter))
	case req.Protocol != protocol:
		return harness.Response{}, provider.Unrecoverable(fmt.Errorf("openai: unsupported protocol %q", req.Protocol))
	}
	resp := harness.Response{Model: req.Model, Protocol: req.Protocol, Adapter: req.Adapter}
	reply, err := m.endpoint.Post(ctx, req.Body, provider.Header("authorization", "Bearer "+m.cfg.APIKey))
	resp.Body = reply.Body
	if err != nil {
		return resp, err
	}
	if reply.Status/100 != 2 {
		return resp, statusError(reply)
	}
	fields, ok := provider.ExactObject(resp.Body)
	var r response
	if !ok || json.Unmarshal(resp.Body, &r) != nil {
		return resp, errors.New("openai: malformed response")
	}
	resp.Model = cmp.Or(r.Model, resp.Model)
	resp.Usage = harness.Usage{InputTokens: r.Usage.InputTokens, OutputTokens: r.Usage.OutputTokens}
	switch status := provider.ExactString(fields, "status"); {
	case status == "completed":
		resp.Complete = true
		return resp, nil
	case status == "incomplete" && r.IncompleteDetails != nil:
		return resp, fmt.Errorf("openai: incomplete response: %s", r.IncompleteDetails.Reason)
	case status == "failed" && r.Error != nil:
		if r.Error.Code == "context_length_exceeded" {
			return resp, &harness.ContextLimitError{Status: reply.Status, Code: r.Error.Code, RequestID: reply.Header.Get("x-request-id")}
		}
		return resp, fmt.Errorf("openai: failed response: %s: %s", r.Error.Code, r.Error.Message)
	default:
		return resp, fmt.Errorf("openai: incomplete response: status %q", status)
	}
}

// Decision concatenates the output_text parts of the messages of a saved
// response, or for the tool transport returns the arguments of its single
// decision call. A body without text is returned unchanged so the strict
// decoder rejects it.
func (m *Model) Decision(resp harness.Response) []byte {
	switch resp.Adapter {
	case toolAdapter:
		return toolDecision(resp)
	case "", adapter:
	default:
		return nil
	}
	var r response
	if json.Unmarshal(resp.Body, &r) != nil {
		return resp.Body
	}
	var text []byte
	for _, item := range r.Output {
		if item.Type != "message" {
			continue
		}
		for _, part := range item.Content {
			if part.Type == "output_text" {
				text = append(text, part.Text...)
			}
		}
	}
	if len(text) == 0 {
		return resp.Body
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

func statusError(r provider.Reply) error {
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	fields, exact := provider.ExactObject(r.Body)
	_, nested := provider.ExactObject(fields["error"])
	decoded := json.Unmarshal(r.Body, &e) == nil
	if exact && nested && decoded && r.Status == http.StatusBadRequest && e.Error.Code == "context_length_exceeded" {
		return &harness.ContextLimitError{Status: r.Status, Type: e.Error.Type, Code: e.Error.Code, RequestID: r.Header.Get("x-request-id")}
	}
	return provider.StatusError("openai", r.Status, provider.Failure{
		Type: cmp.Or(e.Error.Code, e.Error.Type), Message: e.Error.Message, RequestID: r.Header.Get("x-request-id"),
		Exhausted: e.Error.Code == "insufficient_quota",
	})
}
