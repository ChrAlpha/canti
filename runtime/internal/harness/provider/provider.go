// Package provider holds what the model adapters share: one HTTP exchange
// of frozen request bytes, the classification of a failed status, and the
// strict reading of a JSON envelope.
//
// It uses net/http instead of a provider SDK so that every attempt sends
// exactly the frozen bytes once, is counted by the harness without hidden
// retries, and keeps the raw response body.
package provider

import (
	"bytes"
	"context"
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
	Timeout          time.Duration // per-attempt HTTP timeout; default 10m
	MaxResponseBytes int64         // default 8 MiB; larger bodies are incomplete
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
// names none: a longer body is incomplete.
const DefaultMaxResponseBytes = 8 << 20

// New validates cfg and returns an Endpoint.
func New(cfg Config) (*Endpoint, error) {
	if cfg.Timeout < 0 || cfg.MaxResponseBytes < 0 {
		return nil, fmt.Errorf("%s: limits must not be negative", cfg.Name)
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("%s: BaseURL must be an absolute http(s) URL", cfg.Name)
	}
	if ip := net.ParseIP(u.Hostname()); u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("%s: BaseURL must use https unless it is a loopback address", cfg.Name)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Minute
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

// Post sends body once with header under the per-attempt timeout. It
// returns a nil error when a whole body within the response limit arrived,
// whatever its status, with the credential redacted from it. Otherwise it
// returns what arrived and an error: ctx's error once ctx is done, a timeout,
// a transport failure, or a body over the limit, cut to the limit.
func (e *Endpoint) Post(ctx context.Context, body []byte, header http.Header) (Reply, error) {
	if err := ctx.Err(); err != nil {
		return Reply{}, err
	}
	attempt, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()
	hr, err := http.NewRequestWithContext(attempt, http.MethodPost, e.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return Reply{}, Unrecoverable(fmt.Errorf("%s: %w", e.cfg.Name, err))
	}
	hr.Header = header.Clone()
	res, err := e.client.Do(hr)
	if err != nil {
		return Reply{}, e.failure(ctx, attempt, err)
	}
	defer res.Body.Close()
	r := Reply{Status: res.StatusCode, Header: res.Header}
	r.Body, err = io.ReadAll(io.LimitReader(res.Body, e.cfg.MaxResponseBytes+1))
	if e.cfg.Secret != "" {
		r.Body = bytes.ReplaceAll(r.Body, []byte(e.cfg.Secret), []byte("[redacted]"))
	}
	if err != nil || ctx.Err() != nil {
		return r, e.failure(ctx, attempt, err)
	}
	if int64(len(r.Body)) > e.cfg.MaxResponseBytes {
		r.Body = r.Body[:e.cfg.MaxResponseBytes]
		return r, fmt.Errorf("%s: response exceeds %d bytes", e.cfg.Name, e.cfg.MaxResponseBytes)
	}
	return r, nil
}

func (e *Endpoint) failure(ctx, attempt context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("%s: %w", e.cfg.Name, ctx.Err())
	case attempt.Err() != nil:
		return fmt.Errorf("%s: attempt timed out after %v", e.cfg.Name, e.cfg.Timeout)
	}
	return fmt.Errorf("%s: %w", e.cfg.Name, err)
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
