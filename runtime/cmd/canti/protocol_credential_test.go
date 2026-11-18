package main

import (
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/openai"
)

func TestCLIRefusesEveryProtocolCredential(t *testing.T) {
	type credential struct{ api, env, name string }
	var cases []credential
	seen := map[credential]bool{}
	for _, name := range anthropic.ProtocolNames() {
		if name != "" && !seen[credential{"anthropic-messages", "ANTHROPIC_API_KEY", name}] {
			seen[credential{"anthropic-messages", "ANTHROPIC_API_KEY", name}] = true
			cases = append(cases, credential{"anthropic-messages", "ANTHROPIC_API_KEY", name})
		}
	}
	for _, name := range openai.ProtocolNames() {
		if name != "" && !seen[credential{"openai-responses", "OPENAI_API_KEY", name}] {
			seen[credential{"openai-responses", "OPENAI_API_KEY", name}] = true
			cases = append(cases, credential{"openai-responses", "OPENAI_API_KEY", name})
		}
	}
	for _, value := range []string{"                ", "aaaaaaaaaaaaaaaa", "abc\x01defghijklmnop"} {
		for _, api := range []struct{ name, env string }{{"anthropic-messages", "ANTHROPIC_API_KEY"}, {"openai-responses", "OPENAI_API_KEY"}} {
			key := credential{api: api.name, env: api.env, name: value}
			if !seen[key] {
				seen[key] = true
				cases = append(cases, key)
			}
		}
	}
	f := newFixture(t, map[int]core.Decision{})
	for _, tc := range cases {
		t.Run(tc.api+"/"+tc.name, func(t *testing.T) {
			before := len(f.model.requests())
			run := f.cli([]string{tc.env + "=" + tc.name}, "run", "-api", tc.api, "-goal", "g")
			if run.code == exitOK {
				t.Fatalf("CLI accepted protocol credential %q", tc.name)
			}
			if strings.Contains(run.stdout+run.stderr, tc.name) {
				t.Fatalf("CLI echoed protocol credential %q: %+v", tc.name, run)
			}
			if got := len(f.model.requests()); got != before {
				t.Fatalf("CLI sent %d upstream requests with credential %q", got-before, tc.name)
			}
		})
	}
}
