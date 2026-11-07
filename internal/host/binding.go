package host

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"

	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/openai"
)

// Binding is what a driving loop calls the model of one run with: the
// provider a caller names, the provider it would use for a run that stores
// none, and the keys of the run's model API. A host holds a binding only
// while it drives the run it was given for, and never stores, logs, or
// returns its keys.
type Binding struct {
	Provider string // named explicitly, as by -model-url; "" names none
	Fallback string // for a run that stores no provider, as from the environment
	Keys     Keys
}

// String hides the keys of b, and describes its providers by DescribeURL.
func (b Binding) String() string {
	to := "the adapter default"
	if p := cmp.Or(b.Provider, b.Fallback); p != "" {
		to = DescribeURL(p)
	}
	return fmt.Sprintf("binding to %s (keys [redacted])", to)
}

// DescribeURL describes a URL that a caller gave, valid or not, without
// any part of it that may hold a secret: only its scheme, host, and path,
// never its user information, query, or fragment, and nothing at all of an
// input that does not parse as a URL.
func DescribeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "an unparseable URL"
	}
	s := (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
	if s == "" {
		return "an empty URL"
	}
	return s
}

// GoString hides the keys of b.
func (b Binding) GoString() string { return "host." + b.String() }

// LogValue hides the keys of b from structured logs.
func (b Binding) LogValue() slog.Value { return slog.StringValue(b.String()) }

// MarshalJSON encodes b as String does: no encoding of a host value carries
// a key or an unvalidated URL.
func (b Binding) MarshalJSON() ([]byte, error) { return json.Marshal(b.String()) }

// DefaultProvider is the base URL of api's adapter that names none.
func DefaultProvider(api string) string {
	return map[string]string{APIMessages: anthropic.DefaultBaseURL, APIResponses: openai.DefaultBaseURL}[api]
}

// CheckProvider checks that p names a provider without secrets: an absolute
// URL with neither user information, query, nor fragment, which the run
// stores.
func CheckProvider(p string) error {
	u, err := url.Parse(p)
	switch {
	case err != nil:
		return errors.New("the model URL does not parse as a URL")
	case !u.IsAbs() || u.Host == "":
		return fmt.Errorf("model URL %q must be absolute", DescribeURL(p))
	case u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(p, "#"):
		return fmt.Errorf("model URL %q must hold no user information, query, or fragment: canti stores it with the run", DescribeURL(p))
	}
	return nil
}

// newProvider is the provider a new run of api stores when it is created
// with b: the one b names, or else its fallback, or else the adapter's
// default, so that no later process, terminal, or host fills one in.
func newProvider(api string, b Binding) (string, error) {
	p := cmp.Or(b.Provider, b.Fallback, DefaultProvider(api))
	if p == "" {
		return "", nil
	}
	return p, CheckProvider(p)
}

// Pair names what a credential is bound to: a model API and the exact
// provider, its base URL, that the credential may be sent to.
type Pair struct{ API, Provider string }

func (p Pair) String() string { return p.API + " at " + p.Provider }

// ErrNoCredential reports a drive or a submission for whose model API and
// provider neither its caller nor the host holds a credential.
var ErrNoCredential = errors.New("no credential is given or bound")

// provider is the provider a run of c is driven with: the one it stores, or
// else, for a run created before runs stored one, the one the binding names
// or its fallback, and the adapter's default when both are empty. A drive
// checks it with CheckProvider before it builds a model: only a stored
// provider was checked when its run was admitted.
func provider(c RunConfig, b Binding) string {
	if c.Provider != "" {
		return c.Provider
	}
	return cmp.Or(b.Provider, b.Fallback, DefaultProvider(c.API))
}

// keysFor returns the keys that a drive or the submission of run id, of
// config c and provider p, calls the model with. A credential is bound to
// the exact API and provider its caller resolved: the keys of b, for c's API
// only, when the provider b resolves to, the one it names, else its
// fallback, else the adapter's default, is p, as it is for a run that stores
// no provider; otherwise the host's own credential for that API and p, when
// it holds one. A binding that names another provider is refused, also
// without keys, and its keys are never sent to p.
func (h *Host) keysFor(id string, c RunConfig, p string, b Binding) (Keys, error) {
	resolved := cmp.Or(b.Provider, b.Fallback, DefaultProvider(c.API))
	mismatch := c.Provider != "" && resolved != p
	if k := b.Keys.For(c.API); k != (Keys{}) {
		if mismatch {
			return Keys{}, fmt.Errorf("%s calls the model at %s, but the credential given is for %s at %s; "+
				"name %s with -model-url or the API's base URL variable to drive it with that credential", runName(id), p, c.API, DescribeURL(resolved), p)
		}
		return k, nil
	}
	if b.Provider != "" && mismatch {
		return Keys{}, fmt.Errorf("run %s calls the model at %s, not %s; resume it without -model-url", id, p, DescribeURL(b.Provider))
	}
	if k := h.opts.Credentials[Pair{c.API, p}].For(c.API); k != (Keys{}) {
		return k, nil
	}
	return Keys{}, fmt.Errorf("%s: %w for %s", runName(id), ErrNoCredential, Pair{c.API, p})
}

func runName(id string) string {
	if id == "" {
		return "the new run"
	}
	return "run " + id
}

// Bindings returns the pairs the host's own credentials are bound to, in
// order, without their keys.
func (h *Host) Bindings() []Pair {
	var out []Pair
	for p, k := range h.opts.Credentials {
		if k.For(p.API) != (Keys{}) {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b Pair) int { return cmp.Or(cmp.Compare(a.API, b.API), cmp.Compare(a.Provider, b.Provider)) })
	return out
}
