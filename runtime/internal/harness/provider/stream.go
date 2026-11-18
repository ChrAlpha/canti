package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const unparseableMarker = "[truncated: unparseable remainder]"

// RedactedMarker is the replacement Redact writes. It is part of each
// adapter's protocol vocabulary so a credential containing it is refused.
const RedactedMarker = "[redacted]"

const redactedMarker = RedactedMarker

const (
	// streamEventOverhead is the framing allowance above MaxResponseBytes for
	// one event: a terminal event wraps the assembled response in a JSON
	// envelope. The assembled response itself stays bounded by
	// MaxResponseBytes.
	streamEventOverhead = 64 << 10
	// streamWireFactor and streamWireOverhead bound the whole wire stream.
	// Delta events repeat content the terminal event carries, so a legitimate
	// stream is larger than its assembled response by a small linear factor.
	streamWireFactor   = 4
	streamWireOverhead = 256 << 10
	maxEventNameBytes  = 128
)

var errIdleTimeout = errors.New("idle read timeout")

// Event is one dispatched server-sent event. Data is the event's joined data
// lines without a trailing newline.
type Event struct {
	Name    string
	RawName string
	Data    []byte
	RawData []byte
}

// Stream is one opened HTTP exchange. Status and Header are available before
// the body is read. For a 2xx response, Next yields server-sent events; for
// any other status, Body reads the bounded JSON error body.
type Stream struct {
	Status int
	Header http.Header

	cfg     Config
	ctx     context.Context
	attempt context.Context
	cancel  context.CancelCauseFunc
	body    io.ReadCloser
	reader  *bufio.Reader
	timer   *time.Timer
	maxLine int
	maxData int

	bomChecked bool
	pendingLF  bool
	bodyRead   bool
	bodyRaw    []byte
	bodyDiag   []byte
	closed     bool
}

// Open sends body once with header. It returns after the response headers
// arrive, so the caller can classify the status or start reading events. The
// idle timeout applies while waiting for headers and between every body byte.
// A non-nil error means no response is available; ctx's error wins once ctx
// is done.
func (e *Endpoint) Open(ctx context.Context, body []byte, header http.Header) (*Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	attempt, cancel := context.WithCancelCause(ctx)
	hr, err := http.NewRequestWithContext(attempt, http.MethodPost, e.cfg.URL, bytes.NewReader(body))
	if err != nil {
		cancel(nil)
		return nil, Unrecoverable(fmt.Errorf("%s: %w", e.cfg.Name, err))
	}
	hr.Header = header.Clone()
	if hr.Header.Get("Accept") == "" {
		hr.Header.Set("Accept", "text/event-stream")
	}
	timer := time.AfterFunc(e.cfg.IdleTimeout, func() { cancel(errIdleTimeout) })
	res, err := e.client.Do(hr)
	if err != nil {
		timer.Stop()
		cancel(nil)
		return nil, e.failure(ctx, attempt, err)
	}
	// Header bytes are response bytes: a slow header phase must not consume
	// the idle allowance that belongs between the headers and the body.
	timer.Reset(e.cfg.IdleTimeout)
	hdr := res.Header.Clone()
	if e.cfg.Secret != "" {
		for name, values := range hdr {
			for i, value := range values {
				hdr[name][i] = RedactValue(e.cfg.Secret, value)
			}
		}
	}
	maxStream := wireLimit(e.cfg.MaxResponseBytes)
	maxEvent := eventLimit(e.cfg.MaxResponseBytes)
	s := &Stream{
		Status: res.StatusCode, Header: hdr, cfg: e.cfg, ctx: ctx, attempt: attempt, cancel: cancel,
		body: res.Body, timer: timer, maxLine: maxEvent + 16, maxData: maxEvent,
	}
	s.reader = bufio.NewReader(&idleReader{Reader: &boundReader{Reader: res.Body, remaining: maxStream, limit: maxStream}, timer: timer, idle: e.cfg.IdleTimeout})
	return s, nil
}

// Next reads the next server-sent event. It returns io.EOF only when the
// stream ended between events; a cut line or event is an error. Comments,
// blank dispatches without data, and id/retry/unknown fields are ignored as
// the SSE format prescribes.
func (s *Stream) Next() (Event, error) {
	if err := s.ready(); err != nil {
		return Event{}, err
	}
	s.skipBOM()
	var name string
	var data []byte
	hasData := false
	for {
		line, err := s.readLine()
		switch {
		case errors.Is(err, io.EOF):
			if hasData {
				return Event{}, fmt.Errorf("%s: %w", s.cfg.Name, io.ErrUnexpectedEOF)
			}
			return Event{}, io.EOF
		case err != nil:
			return Event{}, err
		}
		if len(line) == 0 {
			if hasData {
				return Event{Name: s.RedactValue(name), RawName: name, Data: s.RedactJSON(data), RawData: bytes.Clone(data)}, nil
			}
			name, data, hasData = "", nil, false
			continue
		}
		if name, data, hasData, err = s.applyLine(line, name, data, hasData); err != nil {
			return Event{}, err
		}
	}
}

// RedactJSON redacts literal bytes first, then decoded JSON strings. A
// provider can echo a credential with a Unicode escape that no literal byte
// comparison sees; the decoded form must not reach a decision, a failure
// text, or a persisted fact.
func (s *Stream) RedactJSON(data []byte) []byte { return RedactJSON(s.cfg.Secret, data) }

// RedactValue redacts one decoded string from this exchange through the shared
// nested policy.
func (s *Stream) RedactValue(value string) string { return RedactValue(s.cfg.Secret, value) }

// redactString is the literal-only helper behind RedactValue;
// RedactValue.
func redactString(secret, value string) string {
	if secret == "" {
		return value
	}
	out := strings.ReplaceAll(value, secret, redactedMarker)
	if strings.Contains(out, secret) {
		return redactedMarker
	}
	return out
}

func redactValue(secret, value string) string { return redactValueDepth(secret, value, 0) }

func redactScalar(secret, value string) string { return redactString(secret, value) }

func nestedJSONValue(value string) bool {
	trimmed := strings.TrimSpace(value)
	return len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"') && json.Valid([]byte(value))
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)

// Ident sanitizes a provider-supplied identifier for diagnostics. It first
// applies RedactValue, then admits only a short protocol-shaped token; any
// escape, quote, brace or nesting becomes a fixed placeholder.
func Ident(secret, value string) string {
	value = RedactValue(secret, value)
	if !identifierPattern.MatchString(value) {
		return "<invalid identifier>"
	}
	return value
}

// Ident is Ident for this exchange.
func (s *Stream) Ident(value string) string { return Ident(s.cfg.Secret, value) }

// RedactValue applies the bounded nested-JSON and quoted-segment policy to a
// free-string field such as a decoded decision payload, purpose or diagnostic.
func RedactValue(secret, value string) string { return redactValueDepth(secret, value, 0) }

func redactValueDepth(secret, value string, depth int) string {
	if secret == "" {
		return value
	}
	if depth >= maxNestedJSON {
		return redactProse(secret, value)
	}
	if nestedJSONValue(value) {
		return string(redactJSONDepth(secret, []byte(value), depth))
	}
	if json.Valid([]byte(value)) {
		return redactScalar(secret, value)
	}
	return redactProse(secret, value)
}

// redactProse applies literal redaction, then decodes Go/JSON escape views
// without pairing quotes. It fails closed when the depth bound is reached.
func redactProse(secret, value string) string {
	original := redactString(secret, value)
	if strings.Contains(original, secret) {
		return redactedMarker
	}
	value = original
	for pass := 0; pass < 16; pass++ {
		next, changed := unescapeOnce(value)
		if !changed {
			return original
		}
		value = next
		if strings.Contains(value, secret) {
			return redactedMarker
		}
	}
	return redactedMarker
}

func unescapeOnce(value string) (string, bool) {
	var out strings.Builder
	changed := false
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			out.WriteByte(value[i])
			continue
		}
		decoded, consumed, ok := decodeEscape(value[i:])
		if !ok {
			out.WriteByte(value[i])
			continue
		}
		out.WriteString(decoded)
		changed = true
		i += consumed - 1
	}
	return out.String(), changed
}

func decodeEscape(value string) (string, int, bool) {
	if len(value) < 2 || value[0] != '\\' {
		return "", 0, false
	}
	switch value[1] {
	case 'n':
		return "\n", 2, true
	case 't':
		return "\t", 2, true
	case 'r':
		return "\r", 2, true
	case 'b':
		return "\b", 2, true
	case 'f':
		return "\f", 2, true
	case 'v':
		return "\v", 2, true
	case 'a':
		return "\a", 2, true
	case '"', '\\', '/', '\'':
		return value[1:2], 2, true
	case 'u':
		return decodeRuneEscape(value[2:], 4)
	case 'U':
		return decodeRuneEscape(value[2:], 8)
	case 'x':
		return decodeRuneEscape(value[2:], 2)
	}
	if value[1] >= '0' && value[1] <= '7' {
		return decodeOctal(value[1:])
	}
	return "", 0, false
}

func decodeRuneEscape(value string, width int) (string, int, bool) {
	runeValue, ok := parseRuneEscape(value, width, 16)
	if !ok || runeValue >= 0x80 {
		return "", 0, false
	}
	return string(runeValue), width + 2, true
}

func decodeOctal(value string) (string, int, bool) {
	if len(value) < 3 {
		return "", 0, false
	}
	var runeValue rune
	for i := 0; i < 3; i++ {
		digit := value[i]
		if digit < '0' || digit > '7' {
			return "", 0, false
		}
		runeValue = runeValue*8 + rune(digit-'0')
	}
	if runeValue >= 0x80 {
		return "", 0, false
	}
	return string(runeValue), 4, true
}

func parseRuneEscape(value string, width, base int) (rune, bool) {
	if len(value) < width {
		return 0, false
	}
	var result rune
	for i := 0; i < width; i++ {
		parsed, ok := hexDigit(value[i], base)
		if !ok {
			return 0, false
		}
		result = result*rune(base) + rune(parsed)
	}
	if !utf8.ValidRune(result) {
		return 0, false
	}
	return result, true
}

func hexDigit(digit byte, base int) (int, bool) {
	switch {
	case digit >= '0' && digit <= '9':
		return int(digit - '0'), true
	case base == 16 && digit >= 'a' && digit <= 'f':
		return int(digit-'a') + 10, true
	case base == 16 && digit >= 'A' && digit <= 'F':
		return int(digit-'A') + 10, true
	}
	return 0, false
}

// RedactJSON returns data with only those JSON string tokens (keys included)
// whose decoded value contains secret replaced. All other bytes, number
// lexemes, key order and duplicate keys stay exactly as received, so the
// structure validation sees is the structure the provider sent. The literal
// pass always covers every byte as well. If a string token cannot be closed,
// the unparseable remainder is replaced by a fixed marker so an escaped echo
// in it cannot be persisted.
func RedactJSON(secret string, data []byte) []byte {
	if secret == "" {
		return data
	}
	return redactJSONDepth(secret, data, 0)
}

const maxNestedJSON = 4

func redactJSONDepth(secret string, data []byte, depth int) []byte {
	if json.Valid(data) {
		if depth >= maxNestedJSON {
			return []byte(redactedMarker)
		}
		out, _, _ := rewriteJSONStrings(secret, data, depth)
		return out
	}
	if !looksLikeJSON(data) {
		return []byte(RedactValue(secret, string(data)))
	}
	out, tokenized, _ := rewriteJSONStrings(secret, data, depth)
	if tokenized {
		return []byte(RedactValue(secret, string(out)))
	}
	prefix := RedactValue(secret, string(out))
	return append([]byte(prefix), []byte(unparseableMarker)...)
}

func looksLikeJSON(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{', '[', '"':
			return true
		default:
			return false
		}
	}
	return false
}

func rewriteJSONStrings(secret string, data []byte, depth int) ([]byte, bool, int) {
	var out []byte
	last := 0
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		end, value, ok := scanJSONString(data, i)
		if !ok {
			out = append(out, data[last:i]...)
			return out, false, i
		}
		redacted := value
		if nestedJSONValue(value) {
			if depth >= maxNestedJSON {
				redacted = redactedMarker
			} else {
				redacted = string(redactJSONDepth(secret, []byte(value), depth+1))
			}
		} else if json.Valid([]byte(value)) {
			redacted = redactScalar(secret, value)
		} else {
			redacted = redactValueDepth(secret, redacted, depth+1)
		}
		if redacted != value {
			out = append(out, data[last:i]...)
			encoded, err := json.Marshal(redacted)
			if err != nil {
				out = append(out, data[i:end]...)
				last = end
				i = end - 1
				continue
			}
			out = append(out, encoded...)
			last = end
		}
		i = end - 1
	}
	out = append(out, data[last:]...)
	return out, true, len(data)
}

func scanJSONString(data []byte, start int) (int, string, bool) {
	for i := start + 1; i < len(data); i++ {
		switch data[i] {
		case '\\':
			i++
		case '"':
			var value string
			if json.Unmarshal(data[start:i+1], &value) != nil {
				return 0, "", false
			}
			return i + 1, value, true
		}
	}
	return 0, "", false
}

type sanitizedError struct {
	text    string
	cause   error
	matches []error
}

func (e *sanitizedError) Error() string { return e.text }
func (e *sanitizedError) Unwrap() error { return e.cause }

// Is keeps errors.Is true for the known sentinels a replaced leaf matched.
func (e *sanitizedError) Is(target error) bool { return sentinelMatch(e.matches, target) }

type sanitizedMultiError struct {
	text    string
	causes  []error
	matches []error
}

func (e *sanitizedMultiError) Error() string        { return e.text }
func (e *sanitizedMultiError) Unwrap() []error      { return e.causes }
func (e *sanitizedMultiError) Is(target error) bool { return sentinelMatch(e.matches, target) }

var knownSentinels = []error{context.Canceled, context.DeadlineExceeded, io.EOF, io.ErrUnexpectedEOF, os.ErrDeadlineExceeded, net.ErrClosed, os.ErrClosed}

func sentinelMatch(matches []error, target error) bool {
	for _, match := range matches {
		if match == target {
			return true
		}
	}
	return false
}

func sentinelMatches(err error) []error {
	var matches []error
	for _, sentinel := range knownSentinels {
		if errors.Is(err, sentinel) {
			matches = append(matches, sentinel)
		}
	}
	return matches
}

type sanitizedNetError struct {
	*sanitizedError
	timeout   bool
	temporary bool
}

func (e *sanitizedNetError) Timeout() bool   { return e.timeout }
func (e *sanitizedNetError) Temporary() bool { return e.temporary }

type sanitizedNetMultiError struct {
	*sanitizedMultiError
	timeout   bool
	temporary bool
}

func (e *sanitizedNetMultiError) Timeout() bool   { return e.timeout }
func (e *sanitizedNetMultiError) Temporary() bool { return e.temporary }

type sanitizedAddr struct {
	network string
	text    string
}

func (a sanitizedAddr) Network() string { return a.network }
func (a sanitizedAddr) String() string  { return a.text }

// RedactDecision applies the JSON token rewrite when the decision parses as
// JSON, and the literal pass otherwise.
func RedactDecision(secret string, decision []byte) []byte {
	if secret == "" {
		return decision
	}
	if json.Valid(decision) {
		return RedactJSON(secret, decision)
	}
	return []byte(RedactValue(secret, string(decision)))
}

// RedactError redacts exchange values from an error and every error reachable
// through Unwrap/errors.As. It keeps sentinel identity and net timeout
// behavior without exposing an unredacted cause.
func RedactError(secret string, err error) error {
	if err == nil || secret == "" {
		return err
	}
	return sanitizeError(secret, err)
}

func sanitizeError(secret string, err error) error {
	switch err {
	case nil:
		return nil
	case context.Canceled:
		return context.Canceled
	case context.DeadlineExceeded:
		return context.DeadlineExceeded
	case io.EOF:
		return io.EOF
	case io.ErrUnexpectedEOF:
		return io.ErrUnexpectedEOF
	}
	switch typed := err.(type) {
	case *harness.StopError:
		return &harness.StopError{Reason: typed.Reason, Err: sanitizeError(secret, typed.Err)}
	case *harness.ContextLimitError:
		limit := *typed
		limit.RequestID = RedactValue(secret, limit.RequestID)
		return &limit
	case *url.Error:
		return &url.Error{Op: RedactValue(secret, typed.Op), URL: RedactValue(secret, typed.URL), Err: sanitizeError(secret, typed.Err)}
	case *net.OpError:
		return &net.OpError{Op: RedactValue(secret, typed.Op), Net: RedactValue(secret, typed.Net), Source: sanitizeAddr(secret, typed.Source), Addr: sanitizeAddr(secret, typed.Addr), Err: sanitizeError(secret, typed.Err)}
	case *os.SyscallError:
		return &os.SyscallError{Syscall: RedactValue(secret, typed.Syscall), Err: sanitizeError(secret, typed.Err)}
	case *net.DNSError:
		copy := *typed
		copy.Name = RedactValue(secret, copy.Name)
		copy.Server = RedactValue(secret, copy.Server)
		copy.Err = RedactValue(secret, copy.Err)
		copy.UnwrapErr = sanitizeError(secret, copy.UnwrapErr)
		return &copy
	}
	text := RedactValue(secret, err.Error())
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		raw := multi.Unwrap()
		causes := make([]error, 0, len(raw))
		for _, cause := range raw {
			causes = append(causes, sanitizeError(secret, cause))
		}
		matches := sentinelMatches(err)
		if netErr, ok := err.(net.Error); ok {
			return &sanitizedNetMultiError{sanitizedMultiError: &sanitizedMultiError{text: text, causes: causes, matches: matches}, timeout: netErr.Timeout(), temporary: netErr.Temporary()}
		}
		return &sanitizedMultiError{text: text, causes: causes, matches: matches}
	}
	var cause error
	if unwrapper, ok := err.(interface{ Unwrap() error }); ok {
		cause = sanitizeError(secret, unwrapper.Unwrap())
	}
	matches := sentinelMatches(err)
	if netErr, ok := err.(net.Error); ok {
		return &sanitizedNetError{sanitizedError: &sanitizedError{text: text, cause: cause, matches: matches}, timeout: netErr.Timeout(), temporary: netErr.Temporary()}
	}
	return &sanitizedError{text: text, cause: cause, matches: matches}
}

func sanitizeAddr(secret string, addr net.Addr) net.Addr {
	if addr == nil {
		return nil
	}
	return sanitizedAddr{network: RedactValue(secret, addr.Network()), text: RedactValue(secret, addr.String())}
}

func (s *Stream) ready() error {
	if s.closed {
		return fmt.Errorf("%s: stream is closed", s.cfg.Name)
	}
	if s.Status/100 != 2 {
		return fmt.Errorf("%s: status %d is not an event stream", s.cfg.Name, s.Status)
	}
	mediaType, _, err := mime.ParseMediaType(s.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "text/event-stream") {
		return fmt.Errorf("%s: response is not text/event-stream", s.cfg.Name)
	}
	return nil
}

func (s *Stream) skipBOM() {
	if s.bomChecked {
		return
	}
	s.bomChecked = true
	if p, _ := s.reader.Peek(3); bytes.Equal(p, []byte{0xef, 0xbb, 0xbf}) {
		_, _ = s.reader.Discard(3)
	}
}

func (s *Stream) applyLine(line []byte, name string, data []byte, hasData bool) (string, []byte, bool, error) {
	if line[0] == ':' {
		return name, data, hasData, nil
	}
	field, value, ok := bytes.Cut(line, []byte(":"))
	if !ok {
		field, value = line, nil
	}
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	switch string(field) {
	case "data":
		if hasData {
			data = append(data, '\n')
		}
		if len(data)+len(value) > s.maxData {
			return name, data, hasData, fmt.Errorf("%s: event exceeds %d bytes", s.cfg.Name, s.maxData)
		}
		data = append(data, value...)
		hasData = true
	case "event":
		if len(value) > maxEventNameBytes {
			return name, data, hasData, fmt.Errorf("%s: event name exceeds %d bytes", s.cfg.Name, maxEventNameBytes)
		}
		name = string(value)
	default:
		// id, retry and unknown fields are ignored by the SSE format.
	}
	return name, data, hasData, nil
}

// Body reads the bounded body of a non-2xx response, with the credential
// redacted. It returns what arrived with an error when the body is over the
// limit or framing fails.
func (s *Stream) Body() ([]byte, error) {
	if s.bodyRead {
		return nil, fmt.Errorf("%s: response body was already read", s.cfg.Name)
	}
	s.bodyRead = true
	raw, err := io.ReadAll(io.LimitReader(s.reader, s.cfg.MaxResponseBytes+1))
	wireOverflow := int64(len(raw)) > s.cfg.MaxResponseBytes
	s.bodyRaw = raw
	s.bodyDiag = boundDiagnostic(s.RedactJSON(raw), s.cfg.MaxResponseBytes)
	if wireOverflow {
		return s.bodyDiag, fmt.Errorf("%s: response exceeds %d bytes", s.cfg.Name, s.cfg.MaxResponseBytes)
	}
	if err != nil {
		return s.bodyDiag, s.failure(err)
	}
	return s.bodyDiag, nil
}

// RawBody is the complete in-memory body read by Body, before redaction or
// diagnostic clipping. It is for classification and usage, never persistence.
func (s *Stream) RawBody() []byte { return s.bodyRaw }

func boundDiagnostic(body []byte, limit int64) []byte {
	if int64(len(body)) <= limit {
		return body
	}
	marker := []byte(unparseableMarker)
	if int64(len(marker)) <= limit && bytes.HasSuffix(body, marker) {
		keep := int(limit) - len(marker)
		return append(body[:keep:keep], marker...)
	}
	return body[:limit]
}

// Close stops the idle timer and releases the connection.
func (s *Stream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.timer.Stop()
	s.cancel(nil)
	return s.body.Close()
}

// readLine returns one SSE line without its terminator. CR, LF, and CRLF are
// accepted, and a line longer than the event allowance is refused without
// accumulating more bytes.
func (s *Stream) readLine() ([]byte, error) {
	if s.pendingLF {
		s.pendingLF = false
		b, err := s.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, s.failure(err)
		}
		if b != '\n' {
			if err := s.reader.UnreadByte(); err != nil {
				return nil, s.failure(err)
			}
		}
	}
	var line []byte
	for {
		b, err := s.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(line) == 0 {
					return nil, io.EOF
				}
				return nil, fmt.Errorf("%s: %w", s.cfg.Name, io.ErrUnexpectedEOF)
			}
			return nil, s.failure(err)
		}
		switch b {
		case '\n':
			return line, nil
		case '\r':
			// CR ends the line immediately. A following LF belongs to the
			// terminator and is consumed by the next read, without waiting
			// for bytes that may still be in flight.
			s.pendingLF = true
			return line, nil
		default:
			line = append(line, b)
			if len(line) > s.maxLine {
				return nil, fmt.Errorf("%s: line exceeds %d bytes", s.cfg.Name, s.maxLine)
			}
		}
	}
}

func (s *Stream) failure(err error) error {
	switch {
	case s.ctx.Err() != nil:
		return fmt.Errorf("%s: %w", s.cfg.Name, s.ctx.Err())
	case errors.Is(context.Cause(s.attempt), errIdleTimeout):
		return fmt.Errorf("%s: no response bytes for %v", s.cfg.Name, s.cfg.IdleTimeout)
	}
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), s.cfg.Name+":") {
		return err
	}
	return fmt.Errorf("%s: %w", s.cfg.Name, err)
}

func (e *Endpoint) failure(ctx, attempt context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("%s: %w", e.cfg.Name, ctx.Err())
	case errors.Is(context.Cause(attempt), errIdleTimeout):
		return fmt.Errorf("%s: no response bytes for %v", e.cfg.Name, e.cfg.IdleTimeout)
	}
	return fmt.Errorf("%s: %w", e.cfg.Name, err)
}

type idleReader struct {
	Reader io.Reader
	timer  *time.Timer
	idle   time.Duration
}

func (r *idleReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.timer.Reset(r.idle)
	}
	return n, err
}

// boundReader fails after limit bytes instead of reporting a clean EOF.
type boundReader struct {
	Reader    io.Reader
	remaining int64
	limit     int64
}

func (r *boundReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		var one [1]byte
		n, err := r.Reader.Read(one[:])
		if n > 0 {
			return 0, fmt.Errorf("stream exceeds %d bytes", r.limit)
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.Reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func eventLimit(maxResponse int64) int {
	if maxResponse > math.MaxInt64-streamEventOverhead {
		return math.MaxInt
	}
	return int(maxResponse + streamEventOverhead)
}

func wireLimit(maxResponse int64) int64 {
	if maxResponse > (math.MaxInt64-streamWireOverhead)/streamWireFactor {
		return math.MaxInt64 - 1
	}
	return maxResponse*streamWireFactor + streamWireOverhead
}

// Bound truncates an assembled response to limit and reports the overflow.
// The truncated bytes remain the diagnostic record of the attempt.
func Bound(name string, body []byte, limit int64) ([]byte, error) {
	if int64(len(body)) <= limit {
		return body, nil
	}
	return body[:limit], fmt.Errorf("%s: response exceeds %d bytes", name, limit)
}
