package hostrpc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/provider"
	"github.com/ChrAlpha/canti/internal/host"
)

var recordWire = flag.Bool("record-wire", false, "record the encoding of the wire messages of this protocol version")

// wireMessages are one message of every shape the protocol defines, with
// every field set. Their version numbers are fixed sample values, never the
// host's constants, so that a change of the context or semantics version
// moves no record.
func wireMessages() map[string]any {
	code := 3
	spec := Spec{Workspace: "/w", Image: "debian:bookworm-slim", Network: "none", Memory: "2g", CPUs: "1", Pids: 512, OutputLimit: 8 << 20,
		API: "anthropic-messages", Model: "m", MaxTokens: 1000, Effort: "low", Structured: true, DecisionFormat: "tool", Fallbacks: true,
		ContextBytes: 262144, ContextVersion: 13, HistoryImports: "/i", WindowSteps: 2, WindowBytes: 3, HandoverBytes: 2048,
		MaxSteps: 20, MaxAttempts: 3, WallTime: "30m0s", TokenBudget: 7, OutputBudget: 8, JobTimeout: "10m0s"}
	run := Run{RunID: "run_1", SessionID: "s", SessionSeq: 2, InputID: "in_1", Goal: "g", Constraints: "c", Status: StatusStopped, Phase: "stopped",
		Step: 4, StopReason: "cancelled", Result: "r", EffectID: "effect_1", Cursor: "run_1:9", ContextVersion: 13, API: "anthropic-messages",
		Provider: "https://p.test", Driver: DriverIdle, Drivable: false, Why: "w", DriveError: "e", Totals: Totals{1, 2, 3, 4}, Budget: Budget{"30m0s", 5, 6, "10m0s"}}
	return map[string]any{
		"request": Request{Protocol: Protocol, Op: OpSubmit, RunID: "run_1", SessionID: "s", Cursor: "run_1:5", Once: true, Limit: 6, TimeoutMS: 7, CleanupMS: 8,
			Submit:  &Submission{SessionID: "s", InputID: "in_1", Goal: "g", Constraints: "c", ExplicitConstraints: true, Spec: spec},
			Binding: &Binding{Provider: "https://p.test", Fallback: "https://f.test", AnthropicKey: "test-key-1", AnthropicToken: "test-key-2", OpenAIKey: "test-key-3"},
			Resolve: &Resolution{Job: true, Execution: "exited", ExitCode: &code, Note: "n"}},
		"response": Response{Protocol: Protocol, Error: &Error{Code: CodeFailed, Message: "m"},
			Hello: &Hello{Protocol: Protocol, Version: "dev", ContextVersion: 13, SemanticsVersion: 4, PID: 1, Bindings: []Pair{{API: "anthropic-messages", Provider: "https://p.test"}}},
			Ack:   &Ack{Replayed: true, Differs: []string{"model", "limits.max_steps"}, Run: run}, Run: &run,
			Page: &Page{Next: "run_1:9", More: true, Halted: true, Items: []Item{{Seq: 2, Step: 1, Kind: "decided",
				Command: `canti:handover-carry {"text":"t","carry":[{"step":1}]}`, Purpose: "p", Action: "handover_carry"}, {Seq: 3, Step: 1, Kind: "completed", Command: "x", Purpose: "p", Action: "retrieve", Result: "r",
				Local: true, Resolved: true, Execution: "exited", ExitCode: &code, Preview: "pv", Detail: "d",
				Outcome: &Outcome{Status: "done", Detail: "d", Retrieval: true, RetrievalError: "re", Sources: 1, Entries: 2, Handover: true},
				Window:  5, StopReason: "cancelled", Failure: "f", Truncated: true}}},
			Wait: &Wait{Cursor: "run_1:9", Halted: true, Driver: DriverDriving}, Constraints: new(string)},
	}
}

// earlierRecords are the sha256 digests of the records of every earlier
// protocol version, which the versions after them keep as they are.
var earlierRecords = map[string]string{
	"v1/request.json":  "6935e54e3438d5e48bd05f316a41342079ccff8b6e72fd450552b64af47b1731",
	"v1/response.json": "b1aa8ac298869cabfab8b5a146fe77049cc17b47e4f62646fc65a41bbd256f82",
	"v2/request.json":  "80155d48c7eb40b81d4f6eb23ea95041645b7a862831ac633fb84cae19801662",
	"v2/response.json": "233776096192839e2d9dd65e6a5395daf950bd3ea3dede87a7e68495e3c214ff",
}

// TestWireMessagesKeepTheirEncoding requires that every message encodes as
// testdata/v<Protocol> records it. -record-wire records a message only when
// its record is missing: a changed message needs the next Protocol and its
// own record, and a record is never rewritten.
func TestWireMessagesKeepTheirEncoding(t *testing.T) {
	dir := filepath.Join("testdata", fmt.Sprintf("v%d", Protocol))
	for name, msg := range wireMessages() {
		got, err := json.MarshalIndent(msg, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := checkRecord(filepath.Join(dir, name+".json"), append(got, '\n'), *recordWire); err != nil {
			t.Error(err)
		}
	}
}

// checkRecord compares got with the record at path, and with record writes
// it there only when there is none.
func checkRecord(path string, got []byte, record bool) error {
	want, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist) && record:
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		return os.WriteFile(path, got, 0o644)
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%s is not recorded; record a new protocol version with -record-wire", path)
	case err != nil:
		return err
	case !bytes.Equal(got, want):
		return fmt.Errorf("%s encodes as\n%s\nwant\n%s\na changed message needs the next Protocol and its own record, never a rewritten one", path, got, want)
	}
	return nil
}

// TestRecordingNeverRewritesARecord requires that recording leaves an
// existing record that differs as it is and fails.
func TestRecordingNeverRewritesARecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v9", "request.json")
	if err := checkRecord(path, []byte("first\n"), true); err != nil {
		t.Fatal(err)
	}
	if err := checkRecord(path, []byte("second\n"), true); err == nil {
		t.Fatal("recording a changed message succeeded")
	}
	if data, _ := os.ReadFile(path); string(data) != "first\n" {
		t.Fatalf("the record became %q", data)
	}
}

// TestEarlierProtocolsKeepTheirRecord requires that the records of every
// earlier protocol version stay as they were recorded.
func TestEarlierProtocolsKeepTheirRecord(t *testing.T) {
	for v := 1; v < Protocol; v++ {
		files, err := filepath.Glob(filepath.Join("testdata", fmt.Sprintf("v%d", v), "*"))
		if err != nil || len(files) == 0 {
			t.Fatalf("protocol %d has no record: %v", v, err)
		}
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			name, _ := filepath.Rel("testdata", f)
			if sum := fmt.Sprintf("%x", sha256.Sum256(data)); earlierRecords[name] != sum {
				t.Errorf("%s has sha256 %s, want the recorded %s", name, sum, earlierRecords[name])
			}
		}
	}
	for name := range earlierRecords {
		if _, err := os.Stat(filepath.Join("testdata", name)); err != nil {
			t.Errorf("the record %s is gone: %v", name, err)
		}
	}
}

// TestEveryValueHasAWord requires that each table of protocol words names
// every value of its enumeration that the host's sources declare, with
// distinct words, and that the words and values map back to each other.
func TestEveryValueHasAWord(t *testing.T) {
	declared := declaredValues(t, map[string]string{
		"../../core": "Phase StopReason ExecutionState",
		"../harness": "OutcomeStatus ActionName",
		"../host":    "ItemKind",
	})
	check := func(name string, words map[string]string) {
		seen := map[string]bool{}
		for v, w := range words {
			if w == "" || seen[w] {
				t.Errorf("%s %q has the word %q, empty or not distinct", name, v, w)
			}
			seen[w] = true
		}
		for _, v := range declared[name] {
			if _, ok := words[v]; !ok {
				t.Errorf("%s %q has no word of the protocol", name, v)
			}
		}
		if len(words) != len(declared[name]) {
			t.Errorf("%s has %d words for %d declared values", name, len(words), len(declared[name]))
		}
	}
	check("Phase", stringKeys(phaseWords))
	check("StopReason", stringKeys(reasonWords))
	check("ExecutionState", stringKeys(executionWords))
	check("OutcomeStatus", stringKeys(outcomeWords))
	check("ActionName", stringKeys(actionWords))
	check("ItemKind", stringKeys(itemWords))
	if value(phaseWords, "awaiting_observation") != core.PhaseAwaitingObservation || word(reasonWords, core.StopUnresolved) != "unresolved" || value(reasonWords, "") != "" {
		t.Fatal("words do not map back to their values")
	}
}

func stringKeys[K ~string](m map[K]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[string(k)] = v
	}
	return out
}

// declaredValues returns, by type name, the values of the string constants
// of each type that the packages in dirs declare outside their tests.
func declaredValues(t *testing.T, dirs map[string]string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for dir, types := range dirs {
		want := map[string]bool{}
		for _, name := range strings.Fields(types) {
			want[name] = true
		}
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range f.Decls {
				g, ok := d.(*ast.GenDecl)
				if !ok || g.Tok != token.CONST {
					continue
				}
				for _, spec := range g.Specs {
					vs := spec.(*ast.ValueSpec)
					id, ok := vs.Type.(*ast.Ident)
					if !ok || !want[id.Name] {
						continue
					}
					for _, v := range vs.Values {
						if lit, ok := v.(*ast.BasicLit); ok && lit.Kind == token.STRING {
							value, _ := strconv.Unquote(lit.Value)
							out[id.Name] = append(out[id.Name], value)
						}
					}
				}
			}
		}
	}
	return out
}

// TestACursorRoundTripsAndRefusesWhatIsNone requires that the wire cursor
// carries a run and a position and that a string that is no cursor is
// refused as host.ErrCursor.
func TestACursorRoundTripsAndRefusesWhatIsNone(t *testing.T) {
	for _, c := range []host.Cursor{{RunID: "run_1", Seq: 0}, {RunID: "run:with:colons", Seq: 42}} {
		if got, err := ParseCursor(CursorOf(c)); err != nil || got != c {
			t.Errorf("cursor %+v round-trips as %+v, %v", c, got, err)
		}
	}
	for _, s := range []string{"", "run_1", ":5", "run_1:", "run_1:-1", "run_1:+1", "run_1:05", "run_1:x"} {
		if _, err := ParseCursor(s); !errors.Is(err, host.ErrCursor) {
			t.Errorf("ParseCursor(%q) = %v", s, err)
		}
	}
}

func TestABindingNeverShowsItsKeys(t *testing.T) {
	b := Binding{Provider: "https://p.test", AnthropicKey: "sk-secret-1", OpenAIKey: "sk-secret-2"}
	for _, s := range []string{fmt.Sprint(b), fmt.Sprintf("%v %+v %#v", b, b, b), fmt.Sprintf("%v", &b)} {
		if strings.Contains(s, "sk-secret") {
			t.Errorf("a binding printed as %q", s)
		}
	}
	if w := BindingOf(host.APIResponses, host.Binding{Keys: host.Keys{AnthropicKey: "a", OpenAIKey: "o"}}); w.AnthropicKey != "" || w.OpenAIKey != "o" {
		t.Errorf("the binding of a Responses run carries %+v", w)
	}
}

func serving(t *testing.T) (*host.Host, *Client) {
	t.Helper()
	h, _, c, _ := servingWith(t, nil)
	return h, c
}

// servingWith serves a new host and returns it with its server, a client,
// and the result of Serve.
func servingWith(t *testing.T, logger *slog.Logger) (*host.Host, *Server, *Client, <-chan error) {
	t.Helper()
	root, err := os.MkdirTemp("", "hostrpc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	opts := host.Options{Workspace: filepath.Join(root, "w"), State: filepath.Join(root, "s")}
	for _, d := range []string{opts.Workspace, opts.State} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	h, err := host.Open(context.Background(), opts, true)
	if err != nil {
		t.Fatal(err)
	}
	path, err := SocketPath(opts.State)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Host: h, Version: "test", Logger: logger}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), ln) }()
	t.Cleanup(func() {
		srv.Close()
		h.Shutdown(context.Background())
	})
	return h, srv, &Client{Path: path}, done
}

func raw(t *testing.T, path, request string) Response {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.WriteString(conn, request+"\n")
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestAServerRefusesAnotherProtocolAndUnknownFields(t *testing.T) {
	h, c := serving(t)
	if r := raw(t, c.Path, fmt.Sprintf(`{"protocol":%d,"op":"hello"}`, Protocol+1)); r.Error == nil || r.Error.Code != CodeProtocol ||
		!strings.Contains(r.Error.Message, fmt.Sprintf("protocol %d", Protocol+1)) || r.Hello != nil {
		t.Fatalf("a request of the next protocol = %+v", r)
	}
	if r := raw(t, c.Path, fmt.Sprintf(`{"protocol":%d,"op":"hello","extra":1}`, Protocol)); r.Error == nil || r.Hello != nil {
		t.Fatalf("a request with an unknown field = %+v", r)
	}
	hello, err := c.Hello(context.Background())
	if err != nil || hello.Protocol != Protocol || hello.ContextVersion != host.ContextVersion || hello.SemanticsVersion != core.SemanticsVersion ||
		hello.PID != os.Getpid() || hello.Bindings == nil || len(hello.Bindings) != len(h.Bindings()) {
		t.Fatalf("hello = %+v, %v", hello, err)
	}
}

// TestARequestOfAnEarlierProtocolIsRefusedBeforeItActs requires that the
// recorded submission of every earlier version, keys included, sent to this
// host is refused naming both versions, the host's process, and a remedy that
// works, and that it admits no run and shows no key in the answer or the
// host's log.
func TestARequestOfAnEarlierProtocolIsRefusedBeforeItActs(t *testing.T) {
	for v := 1; v < Protocol; v++ {
		t.Run(fmt.Sprintf("v%d", v), func(t *testing.T) {
			var log syncBuffer
			h, _, c, _ := servingWith(t, slog.New(slog.NewTextHandler(&log, nil)))
			request, err := os.ReadFile(filepath.Join("testdata", fmt.Sprintf("v%d", v), "request.json"))
			if err != nil {
				t.Fatal(err)
			}
			conn, err := net.Dial("unix", c.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.Write(request)
			answer, _ := io.ReadAll(conn)
			var r Response
			if err := json.Unmarshal(answer, &r); err != nil || r.Error == nil || r.Error.Code != CodeProtocol || r.Ack != nil ||
				!strings.Contains(r.Error.Message, fmt.Sprintf("the client speaks host protocol %d and this host, pid %d, speaks %d", v, os.Getpid(), Protocol)) ||
				!strings.Contains(r.Error.Message, fmt.Sprintf("kill -TERM %d", os.Getpid())) {
				t.Fatalf("a version-%d submission was answered %s, %v", v, answer, err)
			}
			if strings.Contains(string(answer)+log.String(), "test-key") {
				t.Fatal("a key shows in the answer or the log")
			}
			if v, err := h.View(context.Background(), ""); err == nil {
				t.Fatalf("a run was admitted: %+v", v.Run)
			}
		})
	}
}

// TestAClientRefusesAHostOfAnEarlierProtocol requires that a client facing a
// host that answers in any earlier protocol reports both versions, the host's
// process, and a remedy that works, before it sends anything that carries a
// key.
func TestAClientRefusesAHostOfAnEarlierProtocol(t *testing.T) {
	for v := 1; v < Protocol; v++ {
		t.Run(fmt.Sprintf("v%d", v), func(t *testing.T) {
			dir, err := os.MkdirTemp("", "hostrpc")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			path := filepath.Join(dir, SocketName)
			ln, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			requests := make(chan string, 8)
			answer := fmt.Sprintf(`{"protocol":%d,"error":{"code":"protocol","message":"the client speaks host protocol %d and this host, pid 4242, speaks %d; stop it with \"canti host -stop\" once it drives nothing, or use the canti that started it"}}`+"\n", v, Protocol, v)
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					line, _ := bufio.NewReader(conn).ReadString('\n')
					requests <- line
					io.WriteString(conn, answer)
					conn.Close()
				}
			}()
			_, err = (&Client{Path: path}).Hello(context.Background())
			if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), fmt.Sprintf("process 4242, speaks host protocol %d and this canti speaks %d", v, Protocol)) ||
				!strings.Contains(err.Error(), `"kill -TERM 4242"`) || !strings.Contains(err.Error(), "never replaced") {
				t.Fatalf("hello to a version-%d host = %v", v, err)
			}
			if got := <-requests; strings.Contains(got, "key") || !strings.Contains(got, `"op":"hello"`) {
				t.Fatalf("the client sent %s", got)
			}
		})
	}
}

func TestStalledClientsDelayNoOther(t *testing.T) {
	_, c := serving(t)
	for range 64 {
		conn, err := net.Dial("unix", c.Path)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		io.WriteString(conn, `{"protocol":1,`)
	}
	start := time.Now()
	for range 10 {
		if _, err := c.Hello(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("10 calls beside 64 stalled connections took %v", d)
	}
}

func TestAClientTellsANoHostFromALostAnswer(t *testing.T) {
	dir, err := os.MkdirTemp("", "hostrpc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, SocketName)
	if _, err := (&Client{Path: path}).Hello(context.Background()); !Unavailable(err) || Lost(err) {
		t.Fatalf("no socket: %v", err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			bufio.NewReader(conn).ReadString('\n')
			conn.Close()
		}
	}()
	if _, err := (&Client{Path: path}).Hello(context.Background()); !Lost(err) || Unavailable(err) {
		t.Fatalf("a host that closed before answering: %v", err)
	}
	ln.Close()
	os.WriteFile(path, nil, 0o600)
	if _, err := (&Client{Path: path}).Hello(context.Background()); !Unavailable(err) {
		t.Fatalf("a stale socket file: %v", err)
	}
}

func TestSubmitRefusesASpecOfAnotherContextVersionOrWorkspace(t *testing.T) {
	h, c := serving(t)
	spec := SpecOf(host.Spec{Config: host.RunConfig{Workspace: h.Workspace(), ContextVersion: host.ContextVersion + 1}})
	if _, err := c.Submit(context.Background(), Submission{Goal: "g", Spec: spec}, Binding{}, false); !strings.Contains(fmt.Sprint(err), "context version") {
		t.Fatalf("a spec of another context version: %v", err)
	}
	spec.ContextVersion, spec.Workspace = host.ContextVersion, "/elsewhere"
	if _, err := c.Submit(context.Background(), Submission{Goal: "g", Spec: spec}, Binding{}, false); !strings.Contains(fmt.Sprint(err), "workspace") {
		t.Fatalf("a spec of another workspace: %v", err)
	}
}

// TestClosingTheServerEndsItsConnectionsBeforeTheStateIsReleased requires
// that closing a server removes its socket, ends a connection that never
// sends its request, and lets Serve return, and that the host's shutdown then
// releases the state directory, within the shutdown's deadline.
func TestClosingTheServerEndsItsConnectionsBeforeTheStateIsReleased(t *testing.T) {
	h, srv, c, done := servingWith(t, nil)
	conn, err := net.Dial("unix", c.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.WriteString(conn, `{"protocol":`)
	if _, err := c.Hello(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	srv.Close()
	if _, err := os.Stat(c.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the socket remains after Close: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("the stalled connection did not end: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("closing the server and the host took %v", d)
	}
}

// TestAClientReadsAtMostOneBoundedAnswer requires that a client stops
// reading an answer past maxResponse, so that no host can make it hold more.
func TestAClientReadsAtMostOneBoundedAnswer(t *testing.T) {
	dir, err := os.MkdirTemp("", "hostrpc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, SocketName)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(conn, `{"protocol":`+fmt.Sprint(Protocol)+`,"error":{"code":"failed","message":"`)
		io.Copy(conn, io.LimitReader(zeros{}, maxResponse+1<<20))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := (&Client{Path: path}).Hello(ctx); err == nil || ctx.Err() != nil {
		t.Fatalf("an answer over the bound gave %v", err)
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// syncBuffer is a buffer that a logger writes and a test reads at once.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

// TestTheLargestValidRunFitsOneAnswer requires that a run whose result is
// the longest payload a complete model response carries, in the bytes JSON
// writes longest, with goal and constraints of a whole request, encodes as
// one answer, which a client reads whole, and that HTML bytes stay one byte.
func TestTheLargestValidRunFitsOneAnswer(t *testing.T) {
	for _, fill := range []string{"\x01", "<", " "} {
		run := Run{RunID: "run_1", Cursor: "run_1:1", Result: strings.Repeat(fill, provider.DefaultMaxResponseBytes/len(fill)),
			Goal: strings.Repeat(fill, maxRequest/2/len(fill)), Constraints: strings.Repeat(fill, maxRequest/2/len(fill))}
		body, err := encodeResponse(Response{Protocol: Protocol, Run: &run})
		if err != nil {
			t.Fatalf("a run of %q: %v", fill, err)
		}
		if fill == "<" && len(body) > 2*provider.DefaultMaxResponseBytes {
			t.Fatalf("a result of '<' encodes in %d bytes: HTML escaping is on", len(body))
		}
		dir, err := os.MkdirTemp("", "hostrpc")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		path := filepath.Join(dir, SocketName)
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			bufio.NewReader(conn).ReadString('\n')
			conn.Write(body)
		}()
		got, err := (&Client{Path: path}).View(context.Background(), "run_1")
		ln.Close()
		if err != nil || got.Result != run.Result || got.Goal != run.Goal {
			t.Fatalf("a client read a run of %q as %d bytes, %v", fill, len(got.Result), err)
		}
	}
}

// TestAResolutionTravelsAsTheProtocolsWords requires that a resolution's
// execution state is sent as the protocol's word for it and that a word the
// protocol lacks is refused as a spec error, recording nothing.
func TestAResolutionTravelsAsTheProtocolsWords(t *testing.T) {
	for state, w := range executionWords {
		if r := ResolutionOf(false, harness.Observation{Execution: state}); r.Execution != w || r.Job {
			t.Errorf("a resolution of %q travels as %+v", state, r)
		}
	}
	if r := ResolutionOf(true, harness.Observation{Execution: core.ExecutionExited}); !r.Job || r.Execution != "" {
		t.Errorf("a job resolution travels as %+v", r)
	}
	_, c := serving(t)
	for _, w := range []string{"lost", "Exited", ""} {
		if _, err := c.Resolve(context.Background(), "run_none", Resolution{Execution: w}); !errors.As(err, new(host.SpecError)) {
			t.Errorf("resolving with %q = %v, want a spec error", w, err)
		}
	}
}
