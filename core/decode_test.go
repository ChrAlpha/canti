package core_test

import (
	"encoding/json"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
)

func TestDecodeValidDecisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		wantKind core.Kind
		wantBody string
	}{
		{name: "exec", raw: `{"kind":"exec","payload":"go test ./..."}`, wantKind: core.Exec, wantBody: "go test ./..."},
		{name: "return", raw: `{"kind":"return","payload":"done"}`, wantKind: core.Return, wantBody: "done"},
		{name: "empty return", raw: `{"kind":"return","payload":""}`, wantKind: core.Return},
		{name: "reordered fields", raw: ` { "payload": "one\ntwo", "kind": "exec" } `, wantKind: core.Exec, wantBody: "one\ntwo"},
		{name: "unicode escapes", raw: `{"kind":"return","payload":"\u4f60\u597d"}`, wantKind: core.Return, wantBody: "\u4f60\u597d"},
		{name: "escaped field names", raw: `{"\u006b\u0069nd":"exec","\u0070ayload":"x"}`, wantKind: core.Exec, wantBody: "x"},
		{name: "surrogate pair", raw: `{"kind":"return","payload":"\ud83d\ude00"}`, wantKind: core.Return, wantBody: "😀"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := core.Decode([]byte(tt.raw))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if got.Kind != tt.wantKind || got.Payload != tt.wantBody {
				t.Fatalf("Decode() = %#v, want kind %q payload %q", got, tt.wantKind, tt.wantBody)
			}
		})
	}
}

func TestDecodeRejectsInvalidProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ``},
		{name: "array", raw: `[]`},
		{name: "missing kind", raw: `{"payload":"x"}`},
		{name: "missing payload", raw: `{"kind":"exec"}`},
		{name: "unknown field", raw: `{"kind":"exec","payload":"x","extra":true}`},
		{name: "wrong kind type", raw: `{"kind":1,"payload":"x"}`},
		{name: "wrong payload type", raw: `{"kind":"exec","payload":1}`},
		{name: "null kind", raw: `{"kind":null,"payload":"x"}`},
		{name: "null payload", raw: `{"kind":"exec","payload":null}`},
		{name: "object payload", raw: `{"kind":"exec","payload":{}}`},
		{name: "array payload", raw: `{"kind":"exec","payload":[]}`},
		{name: "boolean payload", raw: `{"kind":"exec","payload":true}`},
		{name: "case sensitive fields", raw: `{"Kind":"exec","payload":"x"}`},
		{name: "unsupported kind", raw: `{"kind":"wait","payload":"x"}`},
		{name: "duplicate field", raw: `{"kind":"exec","kind":"return","payload":"x"}`},
		{name: "escaped duplicate field", raw: `{"kind":"exec","payload":"x","\u0070ayload":"y"}`},
		{name: "escaped duplicate kind", raw: `{"kind":"exec","\u006b\u0069nd":"return","payload":"x"}`},
		{name: "trailing value", raw: `{"kind":"exec","payload":"x"} {}`},
		{name: "trailing garbage", raw: `{"kind":"exec","payload":"x"} !`},
		{name: "truncated", raw: `{"kind":"exec","payload":"x"`},
		{name: "markdown wrapper", raw: "```json\n{\"kind\":\"exec\",\"payload\":\"x\"}\n```"},
		{name: "invalid UTF-8", raw: "{\"kind\":\"exec\",\"payload\":\"\xff\"}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := core.Decode([]byte(tt.raw)); err == nil {
				t.Fatalf("Decode(%q) succeeded, want error", tt.raw)
			}
		})
	}
}

func TestDecodeDiagnosticPriority(t *testing.T) {
	for _, tt := range []struct {
		name, raw, want string
	}{
		{"extra field before type errors", `{"kind":1,"payload":false,"extra":true}`, "core: invalid decision: expected kind and payload"},
		{"unknown field before type errors", `{"kind":1,"other":"x"}`, `core: invalid decision: unknown field "other"`},
		{"kind type before payload type", `{"kind":1,"payload":false}`, "core: invalid decision: kind must be a string"},
		{"payload type", `{"kind":"exec","payload":null}`, "core: invalid decision: payload must be a string"},
		{"escaped duplicate before type errors", `{"kind":1,"\u006b\u0069nd":"return","payload":false}`, `core: invalid decision: duplicate field "kind"`},
		{"malformed trailing value before type errors", `{"kind":1,"payload":false} !`, "core: invalid decision: invalid character '!' looking for beginning of value"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := core.Decode([]byte(tt.raw))
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Decode(%q) error = %v, want %q", tt.raw, err, tt.want)
			}
		})
	}
}

func TestDecodeIsSafeForConcurrentCalls(t *testing.T) {
	t.Parallel()

	const workers = 32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				got, err := core.Decode([]byte(`{"kind":"exec","payload":"printf ok"}`))
				if err != nil || got.Kind != core.Exec || got.Payload != "printf ok" {
					t.Errorf("Decode() = %#v, error = %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func FuzzDecodeAcceptedDecision(f *testing.F) {
	for _, raw := range [][]byte{
		[]byte(`{"kind":"exec","payload":"go test ./..."}`),
		[]byte(`{"\u006b\u0069nd":"return","payload":"\ud83d\ude00"}`),
		[]byte(`{"kind":"exec","kind":"return","payload":"x"}`),
		[]byte(`{"kind":null,"payload":"x"}`),
		[]byte(`{"kind":"exec","payload":"x"} {}`),
		[]byte{0xff},
	} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, err := core.Decode(raw)
		if err != nil {
			return
		}
		if !utf8.Valid(raw) || !json.Valid(raw) {
			t.Fatalf("Decode(%q) accepted invalid input", raw)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 2 {
			t.Fatalf("Decode(%q) accepted unexpected fields", raw)
		}
		var kind, payload *string
		if err := json.Unmarshal(fields["kind"], &kind); err != nil || kind == nil || string(got.Kind) != *kind {
			t.Fatalf("Decode(%q) produced invalid kind %q", raw, got.Kind)
		}
		if err := json.Unmarshal(fields["payload"], &payload); err != nil || payload == nil || got.Payload != *payload {
			t.Fatalf("Decode(%q) produced invalid payload %q", raw, got.Payload)
		}
		if got.Kind != core.Exec && got.Kind != core.Return {
			t.Fatalf("Decode(%q) accepted unsupported kind %q", raw, got.Kind)
		}
	})
}
