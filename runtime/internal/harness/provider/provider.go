// Package provider holds what the model adapters share: one streaming HTTP
// exchange of frozen request bytes, the classification of a failed status,
// bounded server-sent event parsing, and the strict reading of a JSON
// envelope.
//
// It uses net/http instead of a provider SDK so that every attempt sends
// exactly the frozen bytes once, is counted by the harness without hidden
// retries, and can record the assembled response the stream produced.
package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// Config configures an Endpoint. Zero limits select the documented defaults.
type Config struct {
	Name             string        // prefix of every error, e.g. "anthropic"
	URL              string        // absolute endpoint URL; https unless the host is a loopback address
	Secret           string        // credential replaced by "[redacted]" wherever a response echoes it
	IdleTimeout      time.Duration // longest wait between response bytes; default 10m
	MaxResponseBytes int64         // default 8 MiB; larger assembled responses and events are incomplete
	Client           *http.Client  // optional; redirects are never followed
}

// Endpoint posts frozen request bytes to one URL.
type Endpoint struct {
	cfg    Config
	client *http.Client
}

// Reply is the status, headers, and body of one exchange.
type Reply struct {
	Status int
	Header http.Header
	Body   []byte
}

// DefaultMaxResponseBytes is the response limit of an Endpoint whose Config
// names none: a longer assembled response or event is incomplete.
const DefaultMaxResponseBytes = 8 << 20

// DefaultIdleTimeout is the longest an Endpoint whose Config names none waits
// between response bytes. A streaming response that stays silent longer is a
// failed attempt; a generation that keeps sending bytes is not bounded by it.
const DefaultIdleTimeout = 10 * time.Minute

// New validates cfg and returns an Endpoint.
func New(cfg Config) (*Endpoint, error) {
	if cfg.IdleTimeout < 0 || cfg.MaxResponseBytes < 0 {
		return nil, fmt.Errorf("%s: limits must not be negative", cfg.Name)
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("%s: BaseURL must be an absolute http(s) URL", cfg.Name)
	}
	if ip := net.ParseIP(u.Hostname()); u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("%s: BaseURL must use https unless it is a loopback address", cfg.Name)
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = DefaultMaxResponseBytes
	}
	client := http.Client{}
	if cfg.Client != nil {
		client = *cfg.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Endpoint{cfg: cfg, client: &client}, nil
}

// Failure is what a provider's error body and headers say about a failed
// status.
type Failure struct {
	Type, Message, RequestID string
	// Exhausted reports the provider's signal that the account cannot pay
	// for more requests; status 402 always does.
	Exhausted bool
}

// StatusError describes a non-2xx status. Exhausted credit stops the run with
// StopBudgetExhausted; 408, 409, 429, and 5xx are retryable; any other status
// is StopUnrecoverable.
func StatusError(name string, status int, f Failure) error {
	text := fmt.Sprintf("%s: status %d", name, status)
	if f.Type != "" {
		text += " " + f.Type
	}
	if f.Message != "" {
		text += ": " + f.Message
	}
	if f.RequestID != "" {
		text += " (request_id " + f.RequestID + ")"
	}
	err := errors.New(text)
	switch {
	case f.Exhausted || status == http.StatusPaymentRequired:
		return &harness.StopError{Reason: core.StopBudgetExhausted, Err: err}
	case status == http.StatusRequestTimeout || status == http.StatusConflict || status == http.StatusTooManyRequests || status >= 500:
		return err
	}
	return Unrecoverable(err)
}

// MinCredentialBytes is the shortest credential an adapter accepts. Shorter
// values are refused rather than treated as a low-entropy secret.
const MinCredentialBytes = 16

// CheckCredential refuses a credential that is shorter than MinCredentialBytes
// or that occurs inside the adapter's protocol vocabulary. The error never
// echoes the value.
func CheckCredential(adapter, value string, vocabulary []string) error {
	if value == "" {
		return nil
	}
	if len(value) < MinCredentialBytes {
		return fmt.Errorf("%s: credential must be at least %d bytes", adapter, MinCredentialBytes)
	}
	if strings.Contains(value, redactedMarker) || markerEdge(value) {
		return fmt.Errorf("%s: credential overlaps the adapter protocol vocabulary", adapter)
	}
	if !validCredentialCharacters(value) {
		return fmt.Errorf("%s: credential must contain printable, non-repeating characters", adapter)
	}
	for _, token := range vocabulary {
		if strings.Contains(token, value) {
			return fmt.Errorf("%s: credential overlaps the adapter protocol vocabulary", adapter)
		}
	}
	return nil
}

func validCredentialCharacters(value string) bool {
	seen, mixed := false, false
	var first rune
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return false
		}
		if !seen {
			first, seen = r, true
		} else if r != first {
			mixed = true
		}
	}
	return seen && mixed
}

// markerEdge rejects credentials whose prefix is a proper suffix of the
// marker or whose suffix is a proper prefix of it. A single left-to-right
// replacement cannot then create a new occurrence at a marker boundary.
func markerEdge(value string) bool {
	for k := 1; k < len(redactedMarker) && k <= len(value); k++ {
		if value[:k] == redactedMarker[len(redactedMarker)-k:] || value[len(value)-k:] == redactedMarker[:k] {
			return true
		}
	}
	return false
}

// CanonicalEnum returns value when it is one of allowed, otherwise the fixed
// "unrecognized" marker. Typed error and signal enums never carry provider
// free text.
func CanonicalEnum(value string, allowed ...string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return "unrecognized"
}

// Unrecoverable wraps err as a failure no resend of the same bytes can fix.
func Unrecoverable(err error) error {
	return &harness.StopError{Reason: core.StopUnrecoverable, Err: err}
}

// ExactObject decodes raw as a JSON object whose keys are unique.
func ExactObject(raw []byte) (map[string]json.RawMessage, bool) {
	if !uniqueObjectKeys(raw) {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil, false
	}
	return fields, true
}

// ExactString is the string value of fields[name], or "" if it is not one.
func ExactString(fields map[string]json.RawMessage, name string) string {
	var value string
	_ = json.Unmarshal(fields[name], &value)
	return value
}

// ExactField reads a canonical wire key and refuses case-variant aliases of
// that same discriminator. Other, unrelated extension fields remain allowed.
func ExactField(fields map[string]json.RawMessage, name string) json.RawMessage {
	for key := range fields {
		if key != name && strings.EqualFold(key, name) {
			return nil
		}
	}
	return fields[name]
}

// NoCaseAliases reports whether fields holds no case-variant alias of any
// name. Unknown, differently named extension fields remain allowed.
func NoCaseAliases(fields map[string]json.RawMessage, names ...string) bool {
	for key := range fields {
		for _, name := range names {
			if key != name && strings.EqualFold(key, name) {
				return false
			}
		}
	}
	return true
}

// Discriminator reads an exact canonical string without case aliases.
func Discriminator(fields map[string]json.RawMessage, name string) string {
	var value string
	_ = json.Unmarshal(ExactField(fields, name), &value)
	return value
}

// Header returns the canonical headers of a JSON request with the extra
// name-value pairs.
func Header(pairs ...string) http.Header {
	h := http.Header{"Content-Type": {"application/json"}}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set(pairs[i], pairs[i+1])
	}
	return h
}

// TrimBase joins base, without trailing slashes, and path.
func TrimBase(base, path string) string { return strings.TrimRight(base, "/") + path }

func uniqueObjectKeys(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return false
		}
	}
	closing, err := d.Token()
	if err != nil || closing != json.Delim('}') {
		return false
	}
	_, err = d.Token()
	return err == io.EOF
}
