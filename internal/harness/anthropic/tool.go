package anthropic

import (
	"encoding/json"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/provider"
)

// toolDecision returns the input of the single decision tool call of a
// complete tool_use response, or nil.
func toolDecision(resp harness.Response) []byte {
	if !resp.Complete {
		return nil
	}
	msg, ok := provider.ExactObject(resp.Body)
	if !ok || provider.ExactString(msg, "stop_reason") != "tool_use" {
		return nil
	}
	var content []json.RawMessage
	if json.Unmarshal(msg["content"], &content) != nil {
		return nil
	}
	var input json.RawMessage
	for _, raw := range content {
		block, ok := provider.ExactObject(raw)
		if !ok {
			return nil
		}
		switch provider.ExactString(block, "type") {
		case "tool_use":
			candidate := block["input"]
			if len(input) != 0 || provider.ExactString(block, "id") == "" || provider.ExactString(block, "name") != harness.DecisionToolName ||
				len(candidate) == 0 || string(candidate) == "null" {
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
