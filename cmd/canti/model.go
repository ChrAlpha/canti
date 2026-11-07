package main

import (
	"cmp"
	"os"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/host"
)

// credentialVariables are the environment variables of every model
// credential. Each command reads them once and removes them all before it
// starts any process, whichever API its run uses.
var credentialVariables = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY"}

// credentials reads the model credentials and removes them from the
// environment, so that no child process inherits one. It fails only when
// none is set; host.NewModel reports a missing credential of the run's API.
func credentials() (host.Keys, error) {
	var values []string
	for _, name := range credentialVariables {
		values = append(values, os.Getenv(name))
		os.Unsetenv(name)
	}
	c := host.Keys{AnthropicKey: values[0], AnthropicToken: values[1], OpenAIKey: values[2]}
	if c.AnthropicKey != "" {
		c.AnthropicToken = ""
	}
	if c == (host.Keys{}) {
		return c, host.ErrNoKeys
	}
	return c, nil
}

// newModel builds the adapter of c's model API with keys. An empty url
// selects the API's base URL variable, or else the adapter's default.
func newModel(c host.RunConfig, url string, keys host.Keys) (harness.Model, error) {
	variable := map[string]string{host.APIMessages: "ANTHROPIC_BASE_URL", host.APIResponses: "OPENAI_BASE_URL"}[c.API]
	if variable != "" {
		url = cmp.Or(url, os.Getenv(variable))
	}
	return host.NewModel(c, url, keys)
}
