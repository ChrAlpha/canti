package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/openai"
)

func TestRunNamesItsModelAPIAndModel(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		err  string
	}{
		{"no API", []string{"-model", "m"}, "-api must be anthropic-messages or openai-responses"},
		{"Chat Completions", []string{"-api", "openai-completions", "-model", "m"}, "-api openai-completions is not supported; use openai-responses"},
		{"unknown API", []string{"-api", "gemini", "-model", "m"}, "-api must be"},
		{"no model", []string{"-api", "anthropic-messages"}, "-model is required"},
		{"Messages effort", []string{"-api", "anthropic-messages", "-model", "m", "-effort", "minimal"}, "-effort of anthropic-messages must be empty or one of low, medium, high, xhigh, max"},
		{"Responses effort", []string{"-api", "openai-responses", "-model", "m", "-effort", "extreme"}, "-effort of openai-responses must be"},
		{"Responses fallbacks", []string{"-api", "openai-responses", "-model", "m", "-fallbacks"}, "-fallbacks needs -api anthropic-messages"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := parseSpec(t, tc.args...)
			s.config.Workspace = "/work"
			if _, _, err := newRun("goal", "", s, time.Time{}); err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("newRun = %v, want %q", err, tc.err)
			}
		})
	}
	for _, args := range [][]string{
		{"-api", "anthropic-messages", "-model", "m", "-effort", "max", "-fallbacks"},
		{"-api", "openai-responses", "-model", "m", "-effort", "none", "-decision-format", "tool"},
		{"-api", "openai-responses", "-model", "m"},
	} {
		s := parseSpec(t, args...)
		s.config.Workspace = "/work"
		run, _, err := newRun("goal", "", s, time.Time{})
		if err != nil {
			t.Fatalf("newRun(%q) = %v", args, err)
		}
		if got, err := decodeConfig(run, "/work"); err != nil || got != s.config {
			t.Fatalf("stored config = %+v, %v\nwant %+v", got, err, s.config)
		}
		if s.config.Effort == "" && bytes.Contains(run.Config, []byte(`"effort"`)) {
			t.Fatalf("an empty effort is stored: %s", run.Config)
		}
	}
}

func TestCredentialsReadAndRemoveEveryVariable(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "key")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "token")
	t.Setenv("OPENAI_API_KEY", "openai")
	if c, err := credentials(); err != nil || c != (credential{anthropicKey: "key", openaiKey: "openai"}) {
		t.Errorf("credentials = %+v, %v", c, err)
	}
	if _, err := credentials(); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") || !strings.Contains(err.Error(), "ANTHROPIC_API_KEY") {
		t.Errorf("a credential stayed in the environment: %v", err)
	}
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "token")
	if c, err := credentials(); err != nil || c != (credential{anthropicToken: "token"}) {
		t.Errorf("credentials = %+v, %v", c, err)
	}
}

func TestNewModelUsesTheCredentialAndBaseURLOfItsAPI(t *testing.T) {
	messages := config{API: apiMessages, Model: "m", MaxTokens: 100, Effort: "high", Structured: true, DecisionFormat: "tool", Fallbacks: true}
	responses := config{API: apiResponses, Model: "m", MaxTokens: 100, Effort: "low", Structured: true}
	if got := anthropicConfig(messages, "https://a.test", credential{anthropicKey: "k", openaiKey: "o"}); got != (anthropic.Config{BaseURL: "https://a.test", APIKey: "k",
		Model: "m", MaxTokens: 100, Effort: "high", StructuredOutput: true, DecisionFormat: "tool", Fallbacks: true}) {
		t.Errorf("anthropic config = %+v", got)
	}
	if got := openaiConfig(responses, "https://o.test/v1", credential{anthropicKey: "k", openaiKey: "o"}); got != (openai.Config{BaseURL: "https://o.test/v1", APIKey: "o",
		Model: "m", MaxTokens: 100, Effort: "low", StructuredOutput: true}) {
		t.Errorf("openai config = %+v", got)
	}
	for _, tc := range []struct {
		c    config
		cred credential
		err  string
	}{
		{messages, credential{openaiKey: "o"}, "set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN to call an anthropic-messages model"},
		{responses, credential{anthropicKey: "k"}, "set OPENAI_API_KEY to call an openai-responses model"},
		{config{API: apiCompletions, Model: "m"}, credential{openaiKey: "o"}, "not supported"},
	} {
		if _, err := newModel(tc.c, "https://model.test", tc.cred); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("newModel(%s) = %v, want %q", tc.c.API, err, tc.err)
		}
	}
	t.Setenv("OPENAI_BASE_URL", "ftp://not-a-model")
	if _, err := newModel(responses, "", credential{openaiKey: "o"}); err == nil || !strings.Contains(err.Error(), "BaseURL") {
		t.Errorf("OPENAI_BASE_URL was not used: %v", err)
	}
	if _, err := newModel(responses, "https://o.test/v1", credential{openaiKey: "o"}); err != nil {
		t.Errorf("-model-url did not override OPENAI_BASE_URL: %v", err)
	}
}
