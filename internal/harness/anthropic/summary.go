package anthropic

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const summaryAdapter = "canti-anthropic/5-summary"
const summaryToolAdapter = "canti-anthropic/5-summary-tool"

var _ harness.SummaryModel = (*Model)(nil)

// FreezeSummary copies a complete prefix and its provider configuration. Only
// the output allowance and the final maintenance instruction change.
func (m *Model) FreezeSummary(input harness.SummaryInput, maxTokens int) (harness.Request, error) {
	if maxTokens < 1 || input.Blocks < 1 || strings.TrimSpace(input.Instruction) == "" {
		return harness.Request{}, errors.New("anthropic: invalid summary input")
	}
	var req harness.Request
	var err error
	if input.Parent != nil {
		req = *input.Parent
	} else {
		req, err = m.Freeze(input.Prompt)
	}
	if err != nil {
		return req, err
	}
	if req.Adapter != adapter && req.Adapter != toolAdapter {
		return harness.Request{}, errors.New("anthropic: unsupported summary parent")
	}
	fields, ok := exactObject(req.Body)
	var limit int
	if !ok || json.Unmarshal(fields["max_tokens"], &limit) != nil || limit < 1 {
		return harness.Request{}, errors.New("anthropic: malformed summary parent")
	}
	messages, err := summaryMessages(fields["messages"], input.Blocks, input.Instruction)
	if err != nil {
		return harness.Request{}, err
	}
	fields["messages"] = messages
	req.MaxOutputTokens = min(maxTokens, limit)
	fields["max_tokens"], _ = json.Marshal(req.MaxOutputTokens)
	req.Body, err = json.Marshal(fields)
	req.InputTextBytes = summaryTextBytes(fields)
	if req.Adapter == toolAdapter {
		req.Adapter = summaryToolAdapter
	} else {
		req.Adapter = summaryAdapter
	}
	req.Context, req.Window, req.CheckpointID = nil, nil, ""
	return req, err
}

func summaryTextBytes(fields map[string]json.RawMessage) int {
	var system string
	var messages []userMessage
	_ = json.Unmarshal(fields["system"], &system)
	_ = json.Unmarshal(fields["messages"], &messages)
	n := len(system)
	for _, message := range messages {
		for _, block := range message.Content {
			n += len(block.Text)
		}
	}
	return n
}

func summaryMessages(raw json.RawMessage, count int, instruction string) (json.RawMessage, error) {
	var messages []json.RawMessage
	if json.Unmarshal(raw, &messages) != nil || len(messages) != 1 {
		return nil, errors.New("anthropic: summary parent requires one user message")
	}
	message, ok := exactObject(messages[0])
	var blocks []json.RawMessage
	if !ok || exactString(message, "role") != "user" || json.Unmarshal(message["content"], &blocks) != nil || count > len(blocks) {
		return nil, errors.New("anthropic: invalid summary prefix boundary")
	}
	last, ok := exactObject(blocks[count-1])
	if !ok || exactString(last, "type") != "text" {
		return nil, errors.New("anthropic: summary prefix must end at a text block")
	}
	if _, present := last["cache_control"]; !present {
		last["cache_control"] = json.RawMessage(`{"type":"ephemeral"}`)
		blocks[count-1], _ = json.Marshal(last)
	}
	message["content"], _ = json.Marshal(blocks[:count])
	messages[0], _ = json.Marshal(message)
	next, _ := json.Marshal(userMessage{Role: "user", Content: []textBlock{{Type: "text", Text: instruction}}})
	return json.Marshal(append(messages, next))
}

func usesTool(adapterName string) bool {
	return adapterName == toolAdapter || adapterName == summaryToolAdapter
}

func (m *Model) Summary(resp harness.Response) (string, error) {
	if !resp.Complete || (resp.Adapter != summaryAdapter && resp.Adapter != summaryToolAdapter) {
		return "", errors.New("anthropic: summary response is not complete")
	}
	fields, ok := exactObject(resp.Body)
	if !ok || !utf8.Valid(resp.Body) {
		return "", errors.New("anthropic: malformed summary envelope")
	}
	stop := "end_turn"
	if resp.Adapter == summaryToolAdapter {
		resp.Adapter, stop = toolAdapter, "tool_use"
	} else {
		resp.Adapter = adapter
		if !summaryTextOnly(fields["content"]) {
			return "", errors.New("anthropic: summary contains a non-text block")
		}
	}
	if exactString(fields, "stop_reason") != stop {
		return "", fmt.Errorf("anthropic: summary stopped with %q", exactString(fields, "stop_reason"))
	}
	d, err := core.Decode(m.Decision(resp))
	if err != nil || d.Kind != core.Return || strings.TrimSpace(d.Payload) == "" {
		return "", errors.New("anthropic: summary must return a nonempty continuation payload")
	}
	return d.Payload, nil
}

func summaryTextOnly(raw json.RawMessage) bool {
	var blocks []json.RawMessage
	if json.Unmarshal(raw, &blocks) != nil || len(blocks) == 0 {
		return false
	}
	for _, raw := range blocks {
		block, ok := exactObject(raw)
		if !ok || exactString(block, "type") != "text" || exactString(block, "text") == "" {
			return false
		}
	}
	return true
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
		var count *int64
		if json.Unmarshal(raw, &count) != nil || count == nil || *count < 0 {
			return harness.Usage{}, false
		}
		value := *count
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
	if read, ok := usage["cache_read_input_tokens"]; ok {
		if write, ok := usage["cache_creation_input_tokens"]; ok {
			result.Cache = &harness.CacheUsage{}
			_ = json.Unmarshal(read, &result.Cache.ReadTokens)
			_ = json.Unmarshal(write, &result.Cache.WriteTokens)
		}
	}
	return result, true
}
