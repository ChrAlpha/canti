package core_test

import (
	"sync"
	"testing"

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
