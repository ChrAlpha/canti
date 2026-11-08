package provider

import (
	"encoding/json"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// Usage reads optional integer counters. For Messages, cache input counters
// add to input_tokens; for Responses input_tokens already includes the cache.
// The raw object is retained even when its numeric contract is invalid.
func Usage(body []byte, messages bool) (harness.Usage, json.RawMessage, error) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil {
		return harness.Usage{}, nil, nil
	}
	raw := envelope["usage"]
	if len(raw) == 0 || string(raw) == "null" {
		return harness.Usage{}, raw, nil
	}
	fields, ok := ExactObject(raw)
	if !ok {
		return harness.Usage{}, raw, Unrecoverable(fmt.Errorf("provider: usage is not an exact object"))
	}
	input, known, err := counter(fields, "input_tokens")
	if err != nil {
		return harness.Usage{}, raw, Unrecoverable(err)
	}
	output, outputKnown, err := counter(fields, "output_tokens")
	if err != nil {
		return harness.Usage{}, raw, Unrecoverable(err)
	}
	if messages {
		for _, name := range []string{"cache_creation_input_tokens", "cache_read_input_tokens"} {
			n, _, err := counter(fields, name)
			if err != nil {
				return harness.Usage{}, raw, Unrecoverable(err)
			}
			input, err = harness.CheckedAdd(input, n)
			if err != nil {
				return harness.Usage{}, raw, Unrecoverable(err)
			}
		}
	}
	usage := harness.Usage{InputTokens: input, OutputTokens: output, InputReported: known, OutputReported: outputKnown}
	if err := usage.Validate(); err != nil {
		return harness.Usage{}, raw, Unrecoverable(err)
	}
	return usage, raw, nil
}

func counter(fields map[string]json.RawMessage, name string) (int64, bool, error) {
	raw, ok := fields[name]
	if !ok || string(raw) == "null" {
		return 0, false, nil
	}
	var n int64
	if json.Unmarshal(raw, &n) != nil || n < 0 {
		return 0, false, fmt.Errorf("provider: invalid usage counter %s", name)
	}
	return n, true, nil
}
