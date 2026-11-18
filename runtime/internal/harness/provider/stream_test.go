package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

func endpoint(t *testing.T, url string, edit func(*provider.Config)) *provider.Endpoint {
	t.Helper()
	cfg := provider.Config{Name: "test", URL: url, IdleTimeout: 5 * time.Second, MaxResponseBytes: 1 << 20}
	if edit != nil {
		edit(&cfg)
	}
	e, err := provider.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func open(t *testing.T, e *provider.Endpoint) *provider.Stream {
	t.Helper()
	s, err := e.Open(context.Background(), []byte(`{}`), provider.Header())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStreamReadsBoundedServerSentEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "\xef\xbb\xbf: keep-alive\r\n\r\nid: 7\r\nevent: first\r\ndata: one\r\ndata: two\r\n\r\n")
		_, _ = io.WriteString(w, "event: second\ndata: three\n\n")
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, nil))
	for _, want := range []provider.Event{{Name: "first", Data: []byte("one\ntwo")}, {Name: "second", Data: []byte("three")}} {
		event, err := s.Next()
		if err != nil || event.Name != want.Name || string(event.Data) != string(want.Data) {
			t.Fatalf("Next = %+v, %v; want %+v", event, err, want)
		}
	}
	if event, err := s.Next(); !errors.Is(err, io.EOF) || event.Name != "" {
		t.Fatalf("after the stream: %+v, %v; want EOF", event, err)
	}
}

func TestStreamRefusesAnOversizedEventAndLine(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		switch r.URL.Path {
		case "/event":
			_, _ = fmt.Fprintf(w, "event: big\ndata: %s\ndata: %s\n\n", strings.Repeat("a", 40<<10), strings.Repeat("b", 40<<10))
		case "/line":
			_, _ = fmt.Fprintf(w, "event: big\ndata: %s\n\n", strings.Repeat("a", 70<<10))
		}
	}))
	defer srv.Close()
	for path, want := range map[string]string{"/event": "event exceeds", "/line": "line exceeds"} {
		t.Run(path, func(t *testing.T) {
			s := open(t, endpoint(t, srv.URL+path, func(c *provider.Config) { c.MaxResponseBytes = 16 }))
			if _, err := s.Next(); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Next = %v, want %q", err, want)
			}
		})
	}
}

func TestStreamIdleTimeoutCutsAStallButReadsALiveStream(t *testing.T) {
	t.Run("stall", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "event: one\ndata: x\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		defer srv.Close()
		s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.IdleTimeout = 50 * time.Millisecond }))
		if event, err := s.Next(); err != nil || event.Name != "one" {
			t.Fatalf("first event = %+v, %v", event, err)
		}
		started := time.Now()
		if _, err := s.Next(); err == nil || !strings.Contains(err.Error(), "no response bytes") || time.Since(started) < 40*time.Millisecond {
			t.Fatalf("stalled Next = %v after %v", err, time.Since(started))
		}
	})

	t.Run("live", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			for i := range 6 {
				_, _ = fmt.Fprintf(w, "event: tick\ndata: %d\n\n", i)
				w.(http.Flusher).Flush()
				time.Sleep(20 * time.Millisecond)
			}
			_, _ = io.WriteString(w, "event: done\ndata: done\n\n")
		}))
		defer srv.Close()
		s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.IdleTimeout = 60 * time.Millisecond }))
		for i := range 7 {
			event, err := s.Next()
			if err != nil {
				t.Fatalf("event %d: %v", i, err)
			}
			if i < 6 && event.Name != "tick" || i == 6 && event.Name != "done" {
				t.Fatalf("event %d = %+v", i, event)
			}
		}
	})
}

func TestStreamBoundsTheWholeExchange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for range 40000 {
			_, _ = io.WriteString(w, ": keepalive\n")
		}
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.MaxResponseBytes = 1 }))
	if _, err := s.Next(); err == nil || !strings.Contains(err.Error(), "stream exceeds") {
		t.Fatalf("Next = %v, want the wire bound", err)
	}
}

func TestStreamReturnsTheCallerContextError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := endpoint(t, srv.URL, nil).Open(ctx, []byte(`{}`), provider.Header())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cancel()
	if _, err := s.Next(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next = %v, want context.Canceled", err)
	}
}

func TestStreamBodyIsBoundedAndRedacted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `SECRET-abcdefghijklmnopqrstuvwxyz`)
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.Secret, c.MaxResponseBytes = "SECRET", 20 }))
	body, err := s.Body()
	if err == nil || !strings.Contains(err.Error(), "response exceeds") || strings.Contains(string(body), "SECRET") || !strings.Contains(string(body), "[redacted]") || len(body) != 20 {
		t.Fatalf("Body = %q, %v", body, err)
	}
}

func TestStreamRefusesANonEventResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, nil))
	if _, err := s.Next(); err == nil || !strings.Contains(err.Error(), "not text/event-stream") {
		t.Fatalf("Next = %v, want a content-type refusal", err)
	}
}

func TestStreamResetsTheIdleClockWhenHeadersArrive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(150 * time.Millisecond)
		_, _ = io.WriteString(w, "event: one\ndata: x\n\n")
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.IdleTimeout = 250 * time.Millisecond }))
	if event, err := s.Next(); err != nil || event.Name != "one" {
		t.Fatalf("Next = %+v, %v; headers must reset the idle clock", event, err)
	}
}

func TestStreamFollowsWHATWGLineAndDataRules(t *testing.T) {
	t.Run("cr ends the line without waiting", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "event: one\r\ndata: x\r\r")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		defer srv.Close()
		s := open(t, endpoint(t, srv.URL, nil))
		if event, err := s.Next(); err != nil || event.Name != "one" || string(event.Data) != "x" {
			t.Fatalf("Next = %+v, %v", event, err)
		}
	})

	t.Run("empty data dispatches and contributes a line", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("content-type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "event: blank\ndata:\n\nevent: multi\ndata:\ndata:x\n\n")
		}))
		defer srv.Close()
		s := open(t, endpoint(t, srv.URL, nil))
		first, err := s.Next()
		if err != nil || first.Name != "blank" || len(first.Data) != 0 {
			t.Fatalf("blank event = %+v, %v", first, err)
		}
		second, err := s.Next()
		if err != nil || second.Name != "multi" || string(second.Data) != "\nx" {
			t.Fatalf("multi event = %+v, %v", second, err)
		}
	})
}

func TestStreamRedactsEventData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"SECRET\"}}\n\n")
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.Secret = "SECRET" }))
	event, err := s.Next()
	if err != nil || strings.Contains(string(event.Data), "SECRET") || !strings.Contains(string(event.Data), "[redacted]") {
		t.Fatalf("event = %q, %v", event.Data, err)
	}
	if !strings.Contains(string(event.RawData), "SECRET") {
		t.Fatalf("raw event = %q, want the unredacted wire data", event.RawData)
	}
}

func TestRedactJSONRewritesOnlyMatchingStringTokens(t *testing.T) {
	data := []byte(`{"literal":"sk-SECRET-key","escaped":"\u0073k-SECRET-key","n":9007199254740993,"keep":"sk-other"}`)
	out := provider.RedactJSON("sk-SECRET-key", data)
	text := string(out)
	if strings.Contains(text, "SECRET") || strings.Count(text, "[redacted]") != 2 || !strings.Contains(text, `"n":9007199254740993`) || !strings.Contains(text, `"keep":"sk-other"`) {
		t.Fatalf("RedactJSON = %s", text)
	}
}

func TestRedactJSONKeepsDuplicateStructure(t *testing.T) {
	data := []byte(`{"block":{"type":"text","text":"","text":"x"},"echo":"\u0073k-SECRET-key"}`)
	out := provider.RedactJSON("sk-SECRET-key", data)
	if strings.Count(string(out), `"text":`) != 2 {
		t.Fatalf("duplicate keys changed: %s", out)
	}
	fields, ok := provider.ExactObject(out)
	if !ok {
		t.Fatalf("top-level object changed: %s", out)
	}
	if _, ok := provider.ExactObject(fields["block"]); ok {
		t.Fatalf("duplicate block was normalized: %s", out)
	}
}

func TestRedactJSONTruncatesAnUnparseableRemainder(t *testing.T) {
	const secret = "sk-SECRET-key"
	for name, data := range map[string]string{
		"literal": `{"error":{"message":"bad key ` + secret,
		"escaped": `{"error":{"message":"bad key \u0073` + secret[1:],
	} {
		t.Run(name, func(t *testing.T) {
			out := provider.RedactJSON(secret, []byte(data))
			if strings.Contains(string(out), secret) || !strings.Contains(string(out), "[truncated: unparseable remainder]") {
				t.Fatalf("RedactJSON = %s", out)
			}
		})
	}
}

func TestRedactJSONLeavesUnrelatedNumericTokens(t *testing.T) {
	const secret = "9007199254740993"
	data := []byte(`{"metadata":{"echo":"` + secret + `"},"usage":{"input_tokens":9007199254740993}}`)
	out := provider.RedactJSON(secret, data)
	if !strings.Contains(string(out), `"echo":"[redacted]"`) || !strings.Contains(string(out), `"input_tokens":9007199254740993`) || !json.Valid(out) {
		t.Fatalf("RedactJSON = %s", out)
	}
}

func TestRedactJSONLeavesValidPrimitives(t *testing.T) {
	for _, data := range []string{"1234", "true", "null"} {
		if out := provider.RedactJSON("1234", []byte(data)); string(out) != data {
			t.Fatalf("primitive %s became %s", data, out)
		}
	}
	if out := provider.RedactJSON("1234", []byte(`"1234"`)); string(out) != `"[redacted]"` {
		t.Fatalf("top-level string = %s", out)
	}
}

func TestRawEventDataCarriesClassificationEnums(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("content-type", "text/event-stream")
		_, _ = io.WriteString(w, "event: error\ndata: {\"type\":\"error\",\"code\":\"insufficient_quota\",\"message\":\"quota\"}\n\n")
	}))
	defer srv.Close()
	s := open(t, endpoint(t, srv.URL, func(c *provider.Config) { c.Secret = "quota" }))
	event, err := s.Next()
	if err != nil || !strings.Contains(string(event.RawData), "insufficient_quota") || strings.Contains(string(event.Data), "insufficient_quota") {
		t.Fatalf("event raw=%q data=%q err=%v", event.RawData, event.Data, err)
	}
}

type secretAddr struct{ secret string }

func (a secretAddr) Network() string { return "net-" + a.secret }
func (a secretAddr) String() string  { return "addr-" + a.secret }

type multiError []error

func (e multiError) Error() string   { return "multi " + e[0].Error() }
func (e multiError) Unwrap() []error { return e }

func TestRedactErrorSanitizesMultiCauseTransport(t *testing.T) {
	const secret = "multi-secret-placeholder"
	addr := secretAddr{secret: secret}
	dns := &net.DNSError{Name: secret, Server: secret, Err: secret, UnwrapErr: errors.New(secret)}
	op := &net.OpError{Op: "dial-" + secret, Net: "tcp-" + secret, Source: addr, Addr: addr,
		Err: &os.SyscallError{Syscall: "connect-" + secret, Err: dns}}
	u := &url.Error{Op: "Get-" + secret, URL: "https://example.test/" + secret, Err: op}
	err := provider.RedactError(secret, multiError{
		fmt.Errorf("first %s: %w", secret, u),
		fmt.Errorf("diagnostic: %w", io.ErrUnexpectedEOF),
		u,
	})
	assertNoSecretInError(t, secret, err)
	var gotURL *url.Error
	if !errors.As(err, &gotURL) || gotURL.Op != "Get-[redacted]" || gotURL.URL != "https://example.test/[redacted]" {
		t.Fatalf("url.Error = %+v", gotURL)
	}
	var gotOp *net.OpError
	if !errors.As(err, &gotOp) || gotOp.Op != "dial-[redacted]" || gotOp.Net != "tcp-[redacted]" {
		t.Fatalf("net.OpError = %+v", gotOp)
	}
	for name, got := range map[string]net.Addr{"source": gotOp.Source, "addr": gotOp.Addr} {
		if got == nil || got.Network() != "net-[redacted]" || got.String() != "addr-[redacted]" {
			t.Fatalf("%s address = %v, %q", name, got, got)
		}
	}
	var gotDNS *net.DNSError
	if !errors.As(err, &gotDNS) || gotDNS.Name != "[redacted]" || gotDNS.Server != "[redacted]" || gotDNS.Err != "[redacted]" {
		t.Fatalf("DNSError = %+v", gotDNS)
	}
	if gotDNS.UnwrapErr == nil || strings.Contains(gotDNS.UnwrapErr.Error(), secret) {
		t.Fatalf("DNSError.UnwrapErr echoes: %v", gotDNS.UnwrapErr)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("multi-cause chain lost ErrUnexpectedEOF")
	}
}

func TestRedactErrorKeepsSentinelIdentityAndDiagnosticsThroughWrappers(t *testing.T) {
	const secret = "sentinel-secret-placeholder"
	cases := []struct {
		name string
		err  error
		want error
		text string
	}{
		{"canceled through url", &url.Error{Op: secret, URL: "https://example.test/" + secret, Err: fmt.Errorf("aborted: %w", context.Canceled)}, context.Canceled, "aborted"},
		{"deadline through op", &net.OpError{Op: secret, Net: "tcp", Err: fmt.Errorf("timeout: %w", context.DeadlineExceeded)}, context.DeadlineExceeded, "timeout"},
		{"unexpected eof diagnostic", fmt.Errorf("openai: stream ended before a terminal response event: %w", io.ErrUnexpectedEOF), io.ErrUnexpectedEOF, "stream ended before a terminal response event"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := provider.RedactError(secret, tc.err)
			assertNoSecretInError(t, secret, err)
			if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("sanitized %v loses %v or %q", err, tc.want, tc.text)
			}
		})
	}
}

func TestRedactErrorSanitizesAnUnknownNETErrorWithAddress(t *testing.T) {
	const secret = "unknown-net-secret-placeholder"
	err := provider.RedactError(secret, &unknownNetError{secret: secret, cause: fmt.Errorf("cause %s: %w", secret, context.Canceled)})
	assertNoSecretInError(t, secret, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("unknown wrapper lost context.Canceled")
	}
	netErr, ok := err.(net.Error)
	if !ok || !netErr.Timeout() || !netErr.Temporary() {
		t.Fatalf("unknown net.Error lost its behavior: %#v", err)
	}
}

type unknownNetError struct {
	secret string
	cause  error
}

func (e *unknownNetError) Error() string   { return "unknown " + e.secret }
func (e *unknownNetError) Unwrap() error   { return e.cause }
func (e *unknownNetError) Timeout() bool   { return true }
func (e *unknownNetError) Temporary() bool { return true }

func assertNoSecretInError(t *testing.T, secret string, err error) {
	t.Helper()
	seen := map[error]bool{}
	var walk func(error)
	walk = func(err error) {
		if err == nil || seen[err] {
			return
		}
		seen[err] = true
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error chain echoes the placeholder: %v", err)
		}
		if multi, ok := err.(interface{ Unwrap() []error }); ok {
			for _, cause := range multi.Unwrap() {
				walk(cause)
			}
		}
		if one, ok := err.(interface{ Unwrap() error }); ok {
			walk(one.Unwrap())
		}
	}
	walk(err)
}

func TestRedactDecisionKeepsOnlyTheProtocolKind(t *testing.T) {
	const secret = "decision-payload-placeholder"
	decision := `{"kind":"return","payload":"` + secret + `","purpose":"echo ` + secret + `"}`
	got := provider.RedactDecision(secret, []byte(decision))
	var decoded struct {
		Kind, Payload, Purpose string
	}
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != "return" || strings.Contains(decoded.Payload, secret) || strings.Contains(decoded.Purpose, secret) {
		t.Fatalf("decision = %s", got)
	}
}

type deadlineLeaf struct{}

func (deadlineLeaf) Error() string   { return "i/o timeout" }
func (deadlineLeaf) Timeout() bool   { return true }
func (deadlineLeaf) Temporary() bool { return true }
func (deadlineLeaf) Is(target error) bool {
	return target == context.DeadlineExceeded
}

func TestRedactErrorKeepsACustomLeafDeadlineMatch(t *testing.T) {
	const secret = "custom-leaf-deadline-placeholder"
	err := provider.RedactError(secret, &url.Error{Op: "Post", URL: "https://example.test/" + secret, Err: deadlineLeaf{}})
	assertNoSecretInError(t, secret, err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("custom Is match lost: %v", err)
	}
	var u *url.Error
	if !errors.As(err, &u) {
		t.Fatal("url.Error wrapper lost")
	}
	netErr, ok := err.(net.Error)
	if !ok || !netErr.Timeout() {
		t.Fatalf("timeout behavior lost: %#v", err)
	}
}

func TestMarkerEdgeCredentialsAreRefused(t *testing.T) {
	for _, secret := range []string{"ed]abcdefghijklm", "credential-x[red", "abcdefghijklm[redacted]x"} {
		if err := provider.CheckCredential("test", secret, []string{provider.RedactedMarker}); err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("CheckCredential(%q) = %v; want refusal without an echo", secret, err)
		}
	}
}

func TestRedactionRunsOnceForAnAcceptedCredential(t *testing.T) {
	const secret = "safe-credential-placeholder"
	input := "before" + secret + "after"
	out := provider.RedactValue(secret, input)
	if strings.Contains(string(out), secret) || !strings.Contains(string(out), provider.RedactedMarker) {
		t.Fatalf("literal redaction = %q", out)
	}
	encoded := `{"message":` + strconv.Quote(input) + `}`
	redacted := provider.RedactJSON(secret, []byte(encoded))
	if strings.Contains(string(redacted), secret) {
		t.Fatalf("JSON redaction = %q", redacted)
	}
}

func TestCredentialContainingTheRedactionMarkerIsRefused(t *testing.T) {
	for _, value := range []string{"sk-sol-" + provider.RedactedMarker, "prefix" + provider.RedactedMarker + "suffix"} {
		err := provider.CheckCredential("test", value, []string{provider.RedactedMarker})
		if err == nil || strings.Contains(err.Error(), value) {
			t.Fatalf("CheckCredential(%q) = %v; want refusal without an echo", value, err)
		}
	}
}

func TestRedactionRecursesIntoNestedJSONStrings(t *testing.T) {
	const secret = "nested-json-credential"
	inner := `{"kind":"return","purpose":"` + secret + `","payload":"done"}`
	outer := `{"text":` + strconv.Quote(inner) + `}`
	out := provider.RedactJSON(secret, []byte(outer))
	if strings.Contains(string(out), secret) {
		t.Fatalf("nested redaction = %s", out)
	}
	var envelope struct{ Text string }
	if err := json.Unmarshal(out, &envelope); err != nil {
		t.Fatal(err)
	}
	var decision map[string]string
	if err := json.Unmarshal([]byte(envelope.Text), &decision); err != nil {
		t.Fatal(err)
	}
	if decision["kind"] != "return" || strings.Contains(envelope.Text, secret) {
		t.Fatalf("decoded nested decision = %s", envelope.Text)
	}
	doubled := `{"text":` + strconv.Quote(envelope.Text) + `}`
	out = provider.RedactJSON(secret, []byte(doubled))
	if strings.Contains(string(out), secret) {
		t.Fatalf("double-nested redaction = %s", out)
	}
}

func TestNestedJSONNumberLexemeRemainsUnrelated(t *testing.T) {
	const secret = "9007199254740993"
	inner := `{"tokens":9007199254740993,"echo":"` + secret + `"}`
	outer := `{"text":` + strconv.Quote(inner) + `}`
	out := provider.RedactJSON(secret, []byte(outer))
	if strings.Contains(string(out), `"echo":"`+secret+`"`) {
		t.Fatalf("nested numeric string echo survived: %s", out)
	}
	var envelope struct{ Text string }
	if err := json.Unmarshal(out, &envelope); err != nil || !json.Valid([]byte(envelope.Text)) {
		t.Fatalf("nested JSON became invalid: %s (%v)", out, err)
	}
	var decoded struct {
		Tokens int64 `json:"tokens"`
	}
	if err := json.Unmarshal([]byte(envelope.Text), &decoded); err != nil || decoded.Tokens != 9007199254740993 {
		t.Fatalf("nested number changed: %+v %v", decoded, err)
	}
}

func TestRedactValueAppliesNestedPolicyToDecisionFields(t *testing.T) {
	const secret = "decision-field-placeholder"
	value := `{"echo":"` + secret + `","tokens":9007199254740993}`
	out := provider.RedactValue(secret, value)
	if strings.Contains(out, secret) || !json.Valid([]byte(out)) || !strings.Contains(out, `"tokens":9007199254740993`) {
		t.Fatalf("RedactValue = %s", out)
	}
	plain := "run " + secret
	if got := provider.RedactValue(secret, plain); strings.Contains(got, secret) || !strings.Contains(got, provider.RedactedMarker) {
		t.Fatalf("plain RedactValue = %s", got)
	}
}

func TestRedactValueDecodesQuotedProseSegments(t *testing.T) {
	const secret = "quoted-prose-placeholder"
	value := `{"kind":"return","payload":"done"} "` + secret + `" and "\u0071` + secret[1:] + `"`
	out := provider.RedactValue(secret, value)
	if strings.Contains(out, secret) || !strings.Contains(out, provider.RedactedMarker) {
		t.Fatalf("RedactValue = %s", out)
	}
}

func TestRedactValueFailsClosedAtTheQuotedDepthBound(t *testing.T) {
	const secret = "deep-quoted-prose-placeholder"
	for layers := 5; layers <= 6; layers++ {
		value := secret
		for i := 0; i < layers; i++ {
			value = "value " + strconv.Quote(value)
		}
		out := provider.RedactValue(secret, value)
		if strings.Contains(out, secret) {
			t.Fatalf("layers=%d retained the credential: %s", layers, out)
		}
	}
}

func TestRedactValueProseDoesNotPairQuotes(t *testing.T) {
	const secret = "astra-loopback-012345"
	cases := []string{
		`panel 6" reported value "` + secret + `"`,
		`panel 6" reported value "` + secret,
		`value sk-\u0061stra-loopback-012345`,
		`value sk-astra-loopback-012345`,
	}
	for _, value := range cases {
		if out := provider.RedactValue(secret, value); strings.Contains(out, secret) || !strings.Contains(out, provider.RedactedMarker) {
			t.Errorf("RedactValue(%q) = %q", value, out)
		}
	}
	const unrelated = `panel 6" says \"hello\" and\nmore`
	if out := provider.RedactValue(secret, unrelated); out != unrelated {
		t.Errorf("unrelated prose changed: %q", out)
	}
}

func TestRedactProseHasAnIndependentFixpointBound(t *testing.T) {
	const secret = "sk-astra-loopback-012345"
	for _, backslashes := range []int{16, 17} {
		value := strings.Repeat("\\", backslashes) + "u0073" + secret[1:]
		if out := provider.RedactValue(secret, value); !strings.Contains(out, provider.RedactedMarker) {
			t.Errorf("backslashes=%d value was not marked: %q", backslashes, out)
		}
	}
	for _, command := range []string{`sed 's/\\n/\\t/g'`, `printf "a\"b"`} {
		if out := provider.RedactValue(secret, command); out != command {
			t.Errorf("ordinary command changed: %q -> %q", command, out)
		}
	}
}

func TestASCIICredentialLeavesEscapedCommandsByteIdentical(t *testing.T) {
	const key = "sk-astra-ascii-012345"
	for _, command := range []string{`printf '%b' 'sk-astra-\xE9-012345'`, `sed 's/\\n/\\t/g'`, `printf "a\"b"`,
		`printf '%s\n' abcdef | sed -E 's/(a)(b)(c)(d)(e)(f)/sk-astra-\61-012345/'`,
		`sed -E 's/(a)(b)(c)(d)(e)/sk-sol-\57sed-escape-guard/'`,
		`printf '%s\n' 'sk-astra-\uİ41-012345'`} {
		if out := provider.RedactValue(key, command); out != command {
			t.Errorf("escaped command changed: %q -> %q", command, out)
		}
	}
}

func TestUnescapeFollowsGoAndJSONGrammar(t *testing.T) {
	if got := provider.RedactValue("A", `a\61b`); got != `a\61b` {
		t.Errorf("two-digit octal changed: %q", got)
	}
	if got := provider.RedactValue("A", `a\101b`); !strings.Contains(got, provider.RedactedMarker) {
		t.Errorf("three-digit octal not decoded: %q", got)
	}
	for _, value := range []string{`a\x6b`, `a\u006b`, `a\1b`, `a\9b`} {
		if got := provider.RedactValue("Z", value); got != value {
			t.Errorf("invalid escape changed: %q -> %q", value, got)
		}
	}
}

func TestEscapeSyntaxRejectsNonASCIILookalikes(t *testing.T) {
	const key = "sk-astra-A-012345"
	for _, command := range []string{
		`sk-astra-\uİ41-012345`,
		`sk-astra-\xＦＦ-012345`,
		`sk-astra-\１２３-012345`,
		`sk-astra-\١٢٣-012345`,
	} {
		if out := provider.RedactValue(key, command); out != command {
			t.Errorf("non-ASCII escape changed: %q -> %q", command, out)
		}
	}
}

func TestOctalEscapesAtOrAbove0x80StayAsWritten(t *testing.T) {
	for _, value := range []string{`a\200b`, `a\377b`} {
		if got := provider.RedactValue("\xe9", value); got != value {
			t.Errorf("high octal changed: %q -> %q", value, got)
		}
	}
	if got := provider.RedactValue("\x7f", `a\177b`); !strings.Contains(got, provider.RedactedMarker) {
		t.Errorf("\\177 was not decoded: %q", got)
	}
}
