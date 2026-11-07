package main

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/openai"
)

// The model APIs a run can name. canti supports them as equals and assumes
// neither a provider nor a model: a run names both, and -model-url or the
// API's base URL variable names the provider. The Chat Completions API is
// refused by name; OpenAI-compatible models are reached through Responses.
const (
	apiMessages    = "anthropic-messages"
	apiResponses   = "openai-responses"
	apiCompletions = "openai-completions"
)

var apiNames = []string{apiMessages, apiResponses}

// credentialVariables are the environment variables of every model
// credential. Each command reads them once and removes them all before it
// starts any process, whichever API its run uses.
var credentialVariables = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY"}

type credential struct{ anthropicKey, anthropicToken, openaiKey string }

// credentials reads the model credentials and removes them from the
// environment, so that no child process inherits one. It fails only when
// none is set; newModel reports a missing credential of the run's API.
func credentials() (credential, error) {
	var values []string
	for _, name := range credentialVariables {
		values = append(values, os.Getenv(name))
		os.Unsetenv(name)
	}
	c := credential{anthropicKey: values[0], anthropicToken: values[1], openaiKey: values[2]}
	if c.anthropicKey != "" {
		c.anthropicToken = ""
	}
	if c == (credential{}) {
		return c, errors.New("set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN for " + apiMessages + ", or OPENAI_API_KEY for " + apiResponses + ", to call the model")
	}
	return c, nil
}

// checkModel checks the model API, model, and API-specific flags of c.
func checkModel(c config) error {
	efforts := map[string][]string{apiMessages: anthropic.Efforts, apiResponses: openai.Efforts}[c.API]
	switch {
	case c.API == apiCompletions:
		return fmt.Errorf("-api %s is not supported; use %s", apiCompletions, apiResponses)
	case efforts == nil:
		return fmt.Errorf("-api must be %s; canti has no default model API", strings.Join(apiNames, " or "))
	case c.Model == "":
		return errors.New("-model is required; canti has no default model")
	case c.Effort != "" && !slices.Contains(efforts, c.Effort):
		return fmt.Errorf("-effort of %s must be empty or one of %s", c.API, strings.Join(efforts, ", "))
	case c.Fallbacks && c.API != apiMessages:
		return fmt.Errorf("-fallbacks needs -api %s", apiMessages)
	}
	return nil
}

// newModel builds the adapter of c's model API. An empty url selects the
// API's base URL variable, or else the adapter's default.
func newModel(c config, url string, cred credential) (harness.Model, error) {
	if err := checkModel(c); err != nil {
		return nil, err
	}
	switch c.API {
	case apiMessages:
		if cred.anthropicKey == "" && cred.anthropicToken == "" {
			return nil, errors.New("set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN to call an " + apiMessages + " model")
		}
		return anthropic.New(anthropicConfig(c, cmp.Or(url, os.Getenv("ANTHROPIC_BASE_URL")), cred))
	default:
		if cred.openaiKey == "" {
			return nil, errors.New("set OPENAI_API_KEY to call an " + apiResponses + " model")
		}
		return openai.New(openaiConfig(c, cmp.Or(url, os.Getenv("OPENAI_BASE_URL")), cred))
	}
}

func anthropicConfig(c config, url string, cred credential) anthropic.Config {
	return anthropic.Config{
		BaseURL: url, APIKey: cred.anthropicKey, AuthToken: cred.anthropicToken, Model: c.Model,
		MaxTokens: c.MaxTokens, Effort: c.Effort, StructuredOutput: c.Structured, DecisionFormat: c.DecisionFormat, Fallbacks: c.Fallbacks,
	}
}

func openaiConfig(c config, url string, cred credential) openai.Config {
	return openai.Config{
		BaseURL: url, APIKey: cred.openaiKey, Model: c.Model,
		MaxTokens: c.MaxTokens, Effort: c.Effort, StructuredOutput: c.Structured, DecisionFormat: c.DecisionFormat,
	}
}
