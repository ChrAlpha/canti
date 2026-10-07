package core_test

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
)

func TestDecodeValidDecisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		raw         string
		wantKind    core.Kind
		wantBody    string
		wantPurpose string
	}{
		{name: "exec", raw: `{"kind":"exec","payload":"go test ./..."}`, wantKind: core.Exec, wantBody: "go test ./..."},
		{name: "return", raw: `{"kind":"return","payload":"done"}`, wantKind: core.Return, wantBody: "done"},
		{name: "empty return", raw: `{"kind":"return","payload":""}`, wantKind: core.Return},
		{name: "reordered fields", raw: ` { "payload": "one\ntwo", "kind": "exec" } `, wantKind: core.Exec, wantBody: "one\ntwo"},
		{name: "unicode escapes", raw: `{"kind":"return","payload":"\u4f60\u597d"}`, wantKind: core.Return, wantBody: "\u4f60\u597d"},
		{name: "escaped field names", raw: `{"\u006b\u0069nd":"exec","\u0070ayload":"x"}`, wantKind: core.Exec, wantBody: "x"},
		{name: "surrogate pair", raw: `{"kind":"return","payload":"\ud83d\ude00"}`, wantKind: core.Return, wantBody: "😀"},
		{name: "purpose", raw: `{"kind":"exec","purpose":"list files, then open go.mod","payload":"ls"}`, wantKind: core.Exec, wantBody: "ls", wantPurpose: "list files, then open go.mod"},
		{name: "empty purpose", raw: `{"kind":"return","purpose":"","payload":"done"}`, wantKind: core.Return, wantBody: "done"},
		{name: "purpose last with line break", raw: `{"payload":"x","kind":"exec","purpose":"one\ntwo 雪"}`, wantKind: core.Exec, wantBody: "x", wantPurpose: "one\ntwo 雪"},
		{name: "escaped purpose name", raw: `{"kind":"exec","payload":"x","\u0070urpose":"p"}`, wantKind: core.Exec, wantBody: "x", wantPurpose: "p"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := core.Decode([]byte(tt.raw))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if got.Kind != tt.wantKind || got.Payload != tt.wantBody || got.Purpose != tt.wantPurpose {
				t.Fatalf("Decode() = %#v, want kind %q payload %q purpose %q", got, tt.wantKind, tt.wantBody, tt.wantPurpose)
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
		{name: "null purpose", raw: `{"kind":"exec","payload":"x","purpose":null}`},
		{name: "number purpose", raw: `{"kind":"exec","payload":"x","purpose":1}`},
		{name: "object purpose", raw: `{"kind":"exec","payload":"x","purpose":{}}`},
		{name: "array purpose", raw: `{"kind":"exec","payload":"x","purpose":["x"]}`},
		{name: "purpose without kind", raw: `{"purpose":"x","payload":"x"}`},
		{name: "purpose without payload", raw: `{"kind":"exec","purpose":"x"}`},
		{name: "purpose only", raw: `{"purpose":"x"}`},
		{name: "purpose and unknown field", raw: `{"kind":"exec","payload":"x","purpose":"p","extra":"y"}`},
		{name: "purpose replacing payload", raw: `{"kind":"exec","purpose":"p","other":"x"}`},
		{name: "duplicate purpose", raw: `{"kind":"exec","payload":"x","purpose":"a","purpose":"b"}`},
		{name: "escaped duplicate purpose", raw: `{"kind":"exec","payload":"x","purpose":"a","\u0070urpose":"b"}`},
		{name: "case sensitive purpose", raw: `{"kind":"exec","payload":"x","Purpose":"p"}`},
		{name: "invalid UTF-8 purpose", raw: "{\"kind\":\"exec\",\"payload\":\"x\",\"purpose\":\"\xff\"}"},
		{name: "trailing value after purpose", raw: `{"kind":"exec","payload":"x","purpose":"p"} {}`},
		{name: "empty exec", raw: `{"kind":"exec","payload":""}`},
		{name: "blank exec", raw: `{"kind":"exec","purpose":"p","payload":" \t\r\n\u00a0\u3000"}`},
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
		{"purpose type", `{"kind":"exec","payload":"x","purpose":null}`, "core: invalid decision: purpose must be a string"},
		{"payload type before purpose type", `{"kind":"exec","payload":1,"purpose":1}`, "core: invalid decision: payload must be a string"},
		{"purpose type before unsupported kind", `{"kind":"wait","payload":"x","purpose":1}`, "core: invalid decision: purpose must be a string"},
		{"purpose does not replace a required field", `{"kind":"exec","purpose":"x"}`, "core: invalid decision: expected kind and payload"},
		{"extra field beside purpose", `{"kind":"exec","payload":"x","purpose":"p","extra":true}`, "core: invalid decision: expected kind and payload"},
		{"unknown field beside purpose", `{"kind":"exec","purpose":"p","other":"x"}`, `core: invalid decision: unknown field "other"`},
		{"blank exec", `{"kind":"exec","payload":" \n"}`, "core: invalid decision: exec payload is blank"},
		{"unsupported kind before blank payload", `{"kind":"wait","payload":""}`, `core: invalid decision: unsupported kind "wait"`},
		{"first unknown field beside purpose", `{"purpose":"p","x":"a","y":"b"}`, `core: invalid decision: unknown field "x"`},
		{"first unknown field in document order", `{"purpose":"p","y":"a","x":"b"}`, `core: invalid decision: unknown field "y"`},
		{"first unknown field before kind", `{"z":"a","kind":"exec"}`, `core: invalid decision: unknown field "z"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for range 64 {
				_, err := core.Decode([]byte(tt.raw))
				if err == nil || err.Error() != tt.want {
					t.Fatalf("Decode(%q) error = %v, want %q", tt.raw, err, tt.want)
				}
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
		[]byte(`{"kind":"exec","purpose":"run the tests","payload":"go test ./..."}`),
		[]byte(`{"kind":"exec","purpose":null,"payload":"x"}`),
		[]byte(`{"\u006b\u0069nd":"return","payload":"\ud83d\ude00"}`),
		[]byte(`{"kind":"exec","kind":"return","payload":"x"}`),
		[]byte(`{"kind":null,"payload":"x"}`),
		[]byte(`{"kind":"exec","payload":"x"} {}`),
		[]byte(`{"kind":"exec","payload":" "}`),
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
		if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 2 && len(fields) != 3 {
			t.Fatalf("Decode(%q) accepted unexpected fields", raw)
		}
		var purpose *string
		if rawPurpose, ok := fields["purpose"]; ok {
			if err := json.Unmarshal(rawPurpose, &purpose); err != nil || purpose == nil || got.Purpose != *purpose {
				t.Fatalf("Decode(%q) produced invalid purpose %q", raw, got.Purpose)
			}
		} else if len(fields) != 2 || got.Purpose != "" {
			t.Fatalf("Decode(%q) accepted unexpected fields or purpose %q", raw, got.Purpose)
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
		if got.Kind == core.Exec && strings.TrimSpace(got.Payload) == "" {
			t.Fatalf("Decode(%q) accepted a blank exec payload", raw)
		}
	})
}
