package anthropic

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/internal/harness"
)

const summaryAdapter = "canti-anthropic/4-summary"

var _ harness.SummaryModel = (*Model)(nil)

// FreezeSummary uses the same endpoint and model without decision tools,
// structured decision output, fallbacks, or cache writes.
func (m *Model) FreezeSummary(p harness.Prompt, maxTokens int) (harness.Request, error) {
	if maxTokens < 1 {
		return harness.Request{}, errors.New("anthropic: summary output limit must be positive")
	}
	copy := *m
	copy.cfg.DecisionFormat = "text"
	copy.cfg.StructuredOutput = false
	copy.cfg.Fallbacks = false
	copy.cfg.MaxTokens = min(maxTokens, m.cfg.MaxTokens)
	p.Stable = 0
	req, err := copy.Freeze(p)
	req.Adapter = summaryAdapter
	return req, err
}

func (m *Model) Summary(resp harness.Response) (string, error) {
	if !resp.Complete || resp.Adapter != summaryAdapter {
		return "", errors.New("anthropic: summary response is not complete")
	}
	fields, ok := exactObject(resp.Body)
	if !ok || !utf8.Valid(resp.Body) {
		return "", errors.New("anthropic: malformed summary envelope")
	}
	if stop := exactString(fields, "stop_reason"); stop != "end_turn" {
		return "", fmt.Errorf("anthropic: summary stopped with %q", stop)
	}
	var content []json.RawMessage
	if err := json.Unmarshal(fields["content"], &content); err != nil {
		return "", err
	}
	var text strings.Builder
	for _, raw := range content {
		block, ok := exactObject(raw)
		if !ok || exactString(block, "type") != "text" {
			return "", errors.New("anthropic: summary contains a non-text block")
		}
		var value string
		if err := json.Unmarshal(block["text"], &value); err != nil || string(block["text"]) == "null" {
			return "", errors.New("anthropic: summary text must be a string")
		}
		text.WriteString(value)
	}
	if strings.TrimSpace(text.String()) == "" {
		return "", errors.New("anthropic: empty summary")
	}
	return text.String(), nil
}

func reportedUsage(body []byte) (harness.Usage, bool) {
	var result harness.Usage
	fields, ok := exactObject(body)
	if !ok {
		return result, false
	}
	usage, ok := exactObject(fields["usage"])
	if !ok {
		return result, false
	}
	for _, name := range []string{"input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"} {
		raw, present := usage[name]
		if !present {
			if name == "input_tokens" || name == "output_tokens" {
				return harness.Usage{}, false
			}
			continue
		}
		var value int64
		if json.Unmarshal(raw, &value) != nil || value < 0 {
			return harness.Usage{}, false
		}
		if name == "output_tokens" {
			result.OutputTokens = value
			continue
		}
		if value > math.MaxInt64-result.InputTokens {
			return harness.Usage{}, false
		}
		result.InputTokens += value
	}
	if result.OutputTokens > math.MaxInt64-result.InputTokens {
		return harness.Usage{}, false
	}
	return result, true
}
