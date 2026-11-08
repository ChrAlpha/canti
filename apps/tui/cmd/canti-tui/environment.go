package main

import (
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
	"os"
	"strings"
)

var credentialVariables = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY"}

func credentials() hostrpc.Binding {
	var v []string
	for _, n := range credentialVariables {
		v = append(v, os.Getenv(n))
		os.Unsetenv(n)
	}
	b := hostrpc.Binding{AnthropicKey: v[0], AnthropicToken: v[1], OpenAIKey: v[2]}
	if b.AnthropicKey != "" {
		b.AnthropicToken = ""
	}
	return b
}

func binding(b hostrpc.Binding, api, url string) hostrpc.Binding {
	b.Provider = url
	switch api {
	case "anthropic-messages":
		b.Fallback = os.Getenv("ANTHROPIC_BASE_URL")
		b.OpenAIKey = ""
	case "openai-responses":
		b.Fallback = os.Getenv("OPENAI_BASE_URL")
		b.AnthropicKey = ""
		b.AnthropicToken = ""
	}
	return b
}

func runtimeEnvironment() []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !isCredential(name) {
			env = append(env, kv)
		}
	}
	return env
}

func isCredential(name string) bool {
	for _, n := range credentialVariables {
		if name == n {
			return true
		}
	}
	return false
}
