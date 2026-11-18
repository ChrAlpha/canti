package provider_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

func TestUsagePresenceAndNumericContract(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		want        harness.Usage
		invalid     bool
	}{
		{"missing", `null`, harness.Usage{}, false},
		{"zero", `{"input_tokens":0,"output_tokens":0}`, harness.Usage{InputReported: true, OutputReported: true}, false},
		{"partial", `{"output_tokens":2}`, harness.Usage{OutputTokens: 2, OutputReported: true}, false},
		{"negative", `{"input_tokens":-1}`, harness.Usage{}, true},
		{"fractional", `{"output_tokens":0.5}`, harness.Usage{}, true},
		{"integer overflow", `{"input_tokens":9223372036854775808}`, harness.Usage{}, true},
		{"sum overflow", `{"input_tokens":9223372036854775807,"output_tokens":1}`, harness.Usage{}, true},
		{"duplicate", `{"input_tokens":1,"input_tokens":2}`, harness.Usage{}, true},
	} {
		for _, messages := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/messages=%t", tc.name, messages), func(t *testing.T) {
				assertUsage(t, `{"usage":`+tc.usage+`}`, messages, tc.want, tc.invalid)
			})
		}
	}
}

func TestUsageAddsCacheInputOnlyForMessages(t *testing.T) {
	cached := `{"usage":{"input_tokens":5,"cache_creation_input_tokens":2,"cache_read_input_tokens":3,"output_tokens":1}}`
	assertUsage(t, cached, true, harness.Usage{InputTokens: 10, OutputTokens: 1, InputReported: true, OutputReported: true}, false)
	assertUsage(t, cached, false, harness.Usage{InputTokens: 5, OutputTokens: 1, InputReported: true, OutputReported: true}, false)

	overflow := `{"usage":{"input_tokens":9223372036854775807,"cache_read_input_tokens":1}}`
	assertUsage(t, overflow, true, harness.Usage{}, true)
	assertUsage(t, overflow, false, harness.Usage{InputTokens: 9223372036854775807, InputReported: true}, false)
}

func assertUsage(t *testing.T, body string, messages bool, want harness.Usage, invalid bool) {
	t.Helper()
	usage, raw, err := provider.Usage([]byte(body), messages)
	var stop *harness.StopError
	if errors.As(err, &stop) != invalid || (err != nil) != invalid || usage != want {
		t.Fatalf("Usage(%s, %t) = %+v, %v; want %+v, invalid %t", body, messages, usage, err, want, invalid)
	}
	if want := body[len(`{"usage":`) : len(body)-1]; string(raw) != want {
		t.Fatalf("Usage(%s, %t) raw = %s, want %s", body, messages, raw, want)
	}
}
