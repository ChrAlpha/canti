package host

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/openai"
)

// The model APIs a run can name. canti supports them as equals and assumes
// neither a provider nor a model: a run names both, and -model-url or the
// API's base URL variable names the provider. The Chat Completions API is
// refused by name; OpenAI-compatible models are reached through Responses.
const (
	APIMessages    = "anthropic-messages"
	APIResponses   = "openai-responses"
	APICompletions = "openai-completions"
)

// APINames are the model APIs a run can name.
var APINames = []string{APIMessages, APIResponses}

// CheckModel checks the model API, model, and API-specific flags of c.
func CheckModel(c RunConfig) error {
	efforts := map[string][]string{APIMessages: anthropic.Efforts, APIResponses: openai.Efforts}[c.API]
	switch {
	case c.API == APICompletions:
		return fmt.Errorf("-api %s is not supported; use %s", APICompletions, APIResponses)
	case efforts == nil:
		return fmt.Errorf("-api must be %s; canti has no default model API", strings.Join(APINames, " or "))
	case c.Model == "":
		return errors.New("-model is required; canti has no default model")
	case c.Effort != "" && !slices.Contains(efforts, c.Effort):
		return fmt.Errorf("-effort of %s must be empty or one of %s", c.API, strings.Join(efforts, ", "))
	case c.Fallbacks && c.API != APIMessages:
		return fmt.Errorf("-fallbacks needs -api %s", APIMessages)
	}
	return nil
}

// ErrNoKeys reports a process that holds the key of no model API.
var ErrNoKeys = errors.New("set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN for " + APIMessages + ", or OPENAI_API_KEY for " + APIResponses + ", to call the model")

// Keys are the model credentials of a process, one per model API. They
// live in memory only; String never shows them.
type Keys struct{ AnthropicKey, AnthropicToken, OpenAIKey string }

// String hides every secret of k.
func (k Keys) String() string { return "[redacted]" }

// GoString hides every secret of k.
func (k Keys) GoString() string { return "host.Keys{[redacted]}" }

// For returns the keys of k that model API api uses, and none of the others.
func (k Keys) For(api string) Keys {
	switch api {
	case APIMessages:
		return Keys{AnthropicKey: k.AnthropicKey, AnthropicToken: k.AnthropicToken}
	case APIResponses:
		return Keys{OpenAIKey: k.OpenAIKey}
	}
	return Keys{}
}

// LogValue hides every secret of k from structured logs.
func (k Keys) LogValue() slog.Value { return slog.StringValue("[redacted]") }

// MarshalJSON hides every secret of k: no encoding of host values carries a
// key, and the host protocol sends keys only in its own request fields.
func (k Keys) MarshalJSON() ([]byte, error) { return []byte(`"[redacted]"`), nil }

// NewModel builds the adapter of c's model API against the provider at url,
// with the key of that API. An empty url selects the adapter's default; the
// host reads no environment.
func NewModel(c RunConfig, url string, k Keys) (harness.Model, error) {
	if err := CheckModel(c); err != nil {
		return nil, err
	}
	switch c.API {
	case APIMessages:
		if k.AnthropicKey == "" && k.AnthropicToken == "" {
			return nil, errors.New("set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN to call an " + APIMessages + " model")
		}
		return anthropic.New(AnthropicConfig(c, url, k))
	default:
		if k.OpenAIKey == "" {
			return nil, errors.New("set OPENAI_API_KEY to call an " + APIResponses + " model")
		}
		return openai.New(OpenAIConfig(c, url, k))
	}
}

// AnthropicConfig is the Messages adapter configuration of a run of c.
func AnthropicConfig(c RunConfig, url string, k Keys) anthropic.Config {
	return anthropic.Config{
		BaseURL: url, APIKey: k.AnthropicKey, AuthToken: k.AnthropicToken, Model: c.Model,
		MaxTokens: c.MaxTokens, Effort: c.Effort, StructuredOutput: c.Structured, DecisionFormat: c.DecisionFormat, Fallbacks: c.Fallbacks,
	}
}

// OpenAIConfig is the Responses adapter configuration of a run of c.
func OpenAIConfig(c RunConfig, url string, k Keys) openai.Config {
	return openai.Config{
		BaseURL: url, APIKey: k.OpenAIKey, Model: c.Model,
		MaxTokens: c.MaxTokens, Effort: c.Effort, StructuredOutput: c.Structured, DecisionFormat: c.DecisionFormat,
	}
}
