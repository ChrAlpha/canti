package anthropic

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/ChrAlpha/canti/internal/harness"
)

const decisionTool = `[{"name":"canti_decision","description":"Submit the next canti decision as tool input instead of replying with JSON text.","input_schema":{"type":"object","properties":{"kind":{"type":"string","enum":["exec","return"]},"payload":{"type":"string"}},"required":["kind","payload"],"additionalProperties":false}}]`
const decisionToolChoice = `{"type":"tool","name":"canti_decision","disable_parallel_tool_use":true}`

func toolDecision(resp harness.Response) []byte {
	if !resp.Complete {
		return nil
	}
	msg, ok := exactObject(resp.Body)
	if !ok || exactString(msg, "stop_reason") != "tool_use" {
		return nil
	}
	var content []json.RawMessage
	if json.Unmarshal(msg["content"], &content) != nil {
		return nil
	}
	var input json.RawMessage
	for _, raw := range content {
		block, ok := exactObject(raw)
		if !ok {
			return nil
		}
		switch exactString(block, "type") {
		case "tool_use":
			candidate := block["input"]
			if len(input) != 0 || exactString(block, "id") == "" || exactString(block, "name") != "canti_decision" || len(candidate) == 0 || string(candidate) == "null" {
				return nil
			}
			input = candidate
		case "text", "thinking", "redacted_thinking":
		default:
			return nil
		}
	}
	return input
}

func exactObject(raw []byte) (map[string]json.RawMessage, bool) {
	if !uniqueObjectKeys(raw) {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, false
	}
	return fields, true
}

func exactString(fields map[string]json.RawMessage, name string) string {
	var value string
	_ = json.Unmarshal(fields[name], &value)
	return value
}

func uniqueObjectKeys(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return false
		}
	}
	close, err := d.Token()
	if err != nil || close != json.Delim('}') {
		return false
	}
	_, err = d.Token()
	return err == io.EOF
}
