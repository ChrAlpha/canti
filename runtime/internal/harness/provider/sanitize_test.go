package provider_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

func TestSanitizeResultVisitsEveryStringField(t *testing.T) {
	const secret = "sanitize-boundary-placeholder"
	response := &harness.Response{
		Body:  []byte(`{"echo":"` + secret + `"}`),
		Model: secret, Protocol: secret, Adapter: secret,
		Extraction: &harness.Extraction{
			Decision:   &core.Decision{Kind: core.Kind(secret), Purpose: secret, Payload: secret},
			Body:       []byte(`{"echo":"` + secret + `"}`),
			Diagnostic: secret,
		},
	}
	provider.SanitizeResult(secret, response)
	limit := &harness.ContextLimitError{Type: secret, Code: secret, StopReason: secret, RequestID: secret}
	provider.SanitizeResult(secret, limit)
	stop := &harness.StopError{Reason: core.StopReason(secret), Err: errors.New(secret)}
	provider.SanitizeResult(secret, stop)
	for name, value := range map[string]any{"response": response, "limit": limit, "stop": stop} {
		var found []string
		collectStrings(reflect.ValueOf(value), secret, &found)
		if len(found) != 0 {
			t.Errorf("%s retained a string field containing the credential: %q", name, found)
		}
	}
}

func collectStrings(value reflect.Value, secret string, found *[]string) {
	if !value.IsValid() {
		return
	}
	if value.CanInterface() {
		if err, ok := value.Interface().(error); ok && strings.Contains(err.Error(), secret) {
			*found = append(*found, err.Error())
		}
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !value.IsNil() {
			collectStrings(value.Elem(), secret, found)
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if value.Field(i).CanInterface() {
				collectStrings(value.Field(i), secret, found)
			}
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			if strings.Contains(string(value.Bytes()), secret) {
				*found = append(*found, string(value.Bytes()))
			}
			return
		}
		for i := 0; i < value.Len(); i++ {
			collectStrings(value.Index(i), secret, found)
		}
	case reflect.String:
		if strings.Contains(value.String(), secret) {
			*found = append(*found, value.String())
		}
	}
}
