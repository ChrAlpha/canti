package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/openai"
	"github.com/ChrAlpha/canti/runtime/internal/host"
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
			s := parseSpec(t, append([]string{"-ephemeral"}, tc.args...)...)
			s.Config.Workspace = "/work"
			s.Config.Provider = "http://127.0.0.1:1"
			if _, _, err := host.NewRun("goal", "", s, time.Time{}); err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("newRun = %v, want %q", err, tc.err)
			}
		})
	}
	for _, args := range [][]string{
		{"-api", "anthropic-messages", "-model", "m", "-effort", "max", "-fallbacks"},
		{"-api", "openai-responses", "-model", "m", "-effort", "none", "-decision-format", "tool"},
		{"-api", "openai-responses", "-model", "m"},
	} {
		s := parseSpec(t, append([]string{"-ephemeral", "-context-tokens", "200000"}, args...)...)
		s.Config.Workspace = "/work"
		s.Config.Provider = "http://127.0.0.1:1"
		s.Config.HelperDigest, s.Config.HelperProtocol, _ = docker.DefaultHelperIdentity()
		run, _, err := host.NewRun("goal", "", s, time.Time{})
		if err != nil {
			t.Fatalf("host.NewRun(%q) = %v", args, err)
		}
		if got, err := host.DecodeConfig(run, "/work"); err != nil || got != s.Config {
			t.Fatalf("stored config = %+v, %v\nwant %+v", got, err, s.Config)
		}
		if s.Config.Effort == "" && bytes.Contains(run.Config, []byte(`"effort"`)) {
			t.Fatalf("an empty effort is stored: %s", run.Config)
		}
	}
}

func TestCredentialsReadAndRemoveEveryVariable(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "anthropic-credential")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token-credential")
	t.Setenv("OPENAI_API_KEY", "openai-credential")
	if c, err := credentials(); err != nil || c != (host.Keys{AnthropicKey: "anthropic-credential", OpenAIKey: "openai-credential"}) {
		t.Errorf("credentials = %+v, %v", c, err)
	}
	if _, err := credentials(); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") || !strings.Contains(err.Error(), "ANTHROPIC_API_KEY") {
		t.Errorf("a credential stayed in the environment: %v", err)
	}
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "anthropic-token-credential")
	if c, err := credentials(); err != nil || c != (host.Keys{AnthropicToken: "anthropic-token-credential"}) {
		t.Errorf("credentials = %+v, %v", c, err)
	}
}

func TestNewModelUsesTheCredentialAndBaseURLOfItsAPI(t *testing.T) {
	messages := host.RunConfig{API: host.APIMessages, Model: "m", MaxTokens: 100, Effort: "high", Structured: true, DecisionFormat: "tool", Fallbacks: true}
	responses := host.RunConfig{API: host.APIResponses, Model: "m", MaxTokens: 100, Effort: "low", Structured: true}
	if got := host.AnthropicConfig(messages, "https://a.test", host.Keys{AnthropicKey: "test-credential-key", OpenAIKey: "test-credential-openai"}); got != (anthropic.Config{BaseURL: "https://a.test", APIKey: "test-credential-key",
		Model: "m", MaxTokens: 100, Effort: "high", StructuredOutput: true, DecisionFormat: "tool", Fallbacks: true}) {
		t.Errorf("anthropic config = %+v", got)
	}
	if got := host.OpenAIConfig(responses, "https://o.test/v1", host.Keys{AnthropicKey: "test-credential-key", OpenAIKey: "test-credential-openai"}); got != (openai.Config{BaseURL: "https://o.test/v1", APIKey: "test-credential-openai",
		Model: "m", MaxTokens: 100, Effort: "low", StructuredOutput: true}) {
		t.Errorf("openai config = %+v", got)
	}
	for _, tc := range []struct {
		c    host.RunConfig
		cred host.Keys
		err  string
	}{
		{messages, host.Keys{OpenAIKey: "test-credential-openai"}, "set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN to call an anthropic-messages model"},
		{responses, host.Keys{AnthropicKey: "test-credential-key"}, "set OPENAI_API_KEY to call an openai-responses model"},
		{host.RunConfig{API: host.APICompletions, Model: "m"}, host.Keys{OpenAIKey: "test-credential-openai"}, "not supported"},
	} {
		if _, err := newModel(tc.c, "https://model.test", tc.cred); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("newModel(%s) = %v, want %q", tc.c.API, err, tc.err)
		}
	}
	t.Setenv("OPENAI_BASE_URL", "ftp://not-a-model")
	if _, err := newModel(responses, "", host.Keys{OpenAIKey: "test-credential-openai"}); err == nil || !strings.Contains(err.Error(), "BaseURL") {
		t.Errorf("OPENAI_BASE_URL was not used: %v", err)
	}
	if _, err := newModel(responses, "https://o.test/v1", host.Keys{OpenAIKey: "test-credential-openai"}); err != nil {
		t.Errorf("-model-url did not override OPENAI_BASE_URL: %v", err)
	}
}

func TestNewModelRefusesDegenerateCredentialsWithoutEcho(t *testing.T) {
	responses := host.RunConfig{API: host.APIResponses, Model: "m", MaxTokens: 100}
	for _, key := range []string{"short", "context_length_exceeded", "response.created", "                ", "aaaaaaaaaaaaaaaa"} {
		_, err := newModel(responses, "https://o.test/v1", host.Keys{OpenAIKey: key})
		if err == nil || strings.Contains(err.Error(), key) {
			t.Fatalf("newModel(%d-byte credential) = %v; want refusal without an echo", len(key), err)
		}
	}
}

func TestCredentialsRefuseNonASCIICredentialsWithoutEcho(t *testing.T) {
	for _, key := range []string{"sk-astra-é-012345", "sk-astra-🧪-012345"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("ANTHROPIC_API_KEY", key)
			if _, err := credentials(); err == nil || strings.Contains(err.Error(), key) {
				t.Fatalf("credentials(%q) = %v; want refusal without an echo", key, err)
			}
		})
	}
}
