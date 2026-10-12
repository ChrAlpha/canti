package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMain runs the test binary as the docker CLI's "system dial-stdio"
// when CANTI_TEST_DIAL names the daemon to reach, or "down".
func TestMain(m *testing.M) {
	if addr := os.Getenv("CANTI_TEST_DIAL"); addr != "" {
		os.Exit(dialFake(addr))
	}
	os.Exit(m.Run())
}

func dialFake(addr string) int {
	if !slices.Equal(os.Args[1:], []string{"system", "dial-stdio"}) {
		fmt.Fprintf(os.Stderr, "unexpected arguments %q\n", os.Args[1:])
		return 2
	}
	if addr == "down" {
		fmt.Fprintln(os.Stderr, "Cannot connect to the Docker daemon at unix:///nowhere.sock. Is the docker daemon running?")
		return 1
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	go func() {
		io.Copy(conn, os.Stdin)
		conn.(*net.TCPConn).CloseWrite()
	}()
	io.Copy(os.Stdout, conn)
	return 0
}

// daemon is a fake daemon that reports API versions max and min and answers
// every other request with answer, recording each request's path and the
// connection it came on.
type daemon struct {
	mu       sync.Mutex
	versions int
	paths    []string
	conns    []string
}

func newDaemon(t *testing.T, max, min string, answer func(w http.ResponseWriter, r *http.Request, nth int)) (*daemon, *httptest.Server) {
	d := &daemon{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		d.mu.Lock()
		d.conns = append(d.conns, r.RemoteAddr)
		nth := 0
		for _, c := range d.conns {
			if c == r.RemoteAddr {
				nth++
			}
		}
		if r.URL.Path == "/version" {
			d.versions++
			d.mu.Unlock()
			fmt.Fprintf(w, `{"ApiVersion":%q,"MinAPIVersion":%q}`, max, min)
			return
		}
		d.paths = append(d.paths, r.Method+" "+r.URL.RequestURI()+" "+strings.Join(r.Header["Idempotency-Key"], ","))
		d.mu.Unlock()
		answer(w, r, nth)
	}))
	t.Cleanup(srv.Close)
	return d, srv
}

func noContent(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(http.StatusNoContent) }

func (d *daemon) sent() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.paths)
}

func testEngineExecutor(e *engine) *Executor {
	return &Executor{timeouts: timeouts{5 * time.Second, 5 * time.Second}, engine: e}
}

func TestEngineSpeaksTheNearestVersionTheDaemonSupports(t *testing.T) {
	for _, tt := range []struct{ max, min, want string }{
		{"1.56", "1.40", apiMax},
		{apiMax, "1.24", apiMax},
		{"1.43", "1.12", "1.43"},
		{"1.30", "1.12", "1.30"},
		{"1.60", "1.50", "1.50"},
		{"1.29", "1.12", ""},
		{"", "", ""},
		{"2", "1.12", ""},
	} {
		d, srv := newDaemon(t, tt.max, tt.min, noContent)
		x := testEngineExecutor(dialing(srv))
		for range 2 {
			err := x.api(t.Context(), "rm", http.MethodDelete, "/containers/c?v=1", nil, nil)
			if tt.want == "" {
				if err == nil || !strings.Contains(err.Error(), "at least "+apiMin+" is required") {
					t.Fatalf("daemon of API %q: %v, want refused", tt.max, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("daemon of API %q..%q: %v", tt.min, tt.max, err)
			}
		}
		want, versions := []string{"DELETE /v" + tt.want + "/containers/c?v=1 ", "DELETE /v" + tt.want + "/containers/c?v=1 "}, 1
		if tt.want == "" {
			want, versions = nil, 2
		}
		if got := d.sent(); !slices.Equal(got, want) || d.versions != versions {
			t.Fatalf("daemon of API %q..%q was sent %q after %d version requests, want %q after %d", tt.min, tt.max, got, d.versions, want, versions)
		}
	}
}

func TestEngineReportsTheDaemonsMessage(t *testing.T) {
	for body, want := range map[string]string{
		`{"message":"No such container: c"}`: "docker inspect: No such container: c",
		"Bad Gateway\n":                      "docker inspect: Bad Gateway",
		strings.Repeat("x", 100<<10):         "docker inspect: " + strings.Repeat("x", 4<<10),
	} {
		_, srv := newDaemon(t, apiMax, "1.24", func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, body)
		})
		err := testEngineExecutor(dialing(srv)).api(t.Context(), "inspect", http.MethodGet, "/containers/c/json", nil, nil)
		if err == nil || err.Error() != want || status(err) != http.StatusNotFound {
			t.Fatalf("error = %.80v, want %.80q", err, want)
		}
		if missing := strings.Contains(body, "No such container"); errors.Is(err, errMissing) != missing {
			t.Fatalf("errors.Is(%.80v, errMissing) = %v", err, !missing)
		}
	}
}

func TestOnlyAStartIsNeverSentTwice(t *testing.T) {
	for _, tt := range []struct {
		op, method, path string
		replay           bool
	}{
		{"inspect", http.MethodGet, "/containers/c/json", true},
		{"create", http.MethodPost, "/containers/create?name=c", true},
		{"stop", http.MethodPost, "/containers/c/stop?t=1", true},
		{"rm", http.MethodDelete, "/containers/c?v=1", true},
		{"start", http.MethodPost, "/containers/c/start", false},
	} {
		d, srv := newDaemon(t, apiMax, "1.24", func(w http.ResponseWriter, r *http.Request, nth int) {
			if nth > 1 {
				if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
					conn.Close()
				}
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		e := dialing(srv)
		e.client.Transport.(*http.Transport).MaxConnsPerHost = 1
		var body any
		if tt.method == http.MethodPost {
			body = map[string]string{}
		}
		err := testEngineExecutor(e).api(t.Context(), tt.op, tt.method, tt.path, body, nil)
		want := []string{tt.method + " /v" + apiMax + tt.path + " "}
		if tt.replay {
			want = append(want, want[0])
		}
		if got := d.sent(); (err == nil) != tt.replay || !slices.Equal(got, want) {
			t.Fatalf("%s whose connection broke = %v after %q, want %q", tt.op, err, got, want)
		}
	}
}

func TestEngineReachesTheDaemonThroughTheDockerCLI(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	d, srv := newDaemon(t, apiMax, "1.24", func(w http.ResponseWriter, r *http.Request, _ int) {
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"Id":"c-e","Warnings":[]}`)
		case http.MethodGet:
			io.WriteString(w, `{"Id":"c-e"}`)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	t.Setenv("CANTI_TEST_DIAL", srv.Listener.Addr().String())
	e := newEngine(func(ctx context.Context) (net.Conn, error) { return dialStdio(ctx, exe) })
	t.Cleanup(e.client.CloseIdleConnections)
	x := testEngineExecutor(e)
	for range 2 {
		var c container
		if err := x.api(t.Context(), "create", http.MethodPost, "/containers/create?name=canti-e", map[string]string{}, nil); err != nil {
			t.Fatalf("create through the CLI = %v", err)
		}
		if err := x.api(t.Context(), "inspect", http.MethodGet, "/containers/canti-e/json", nil, decode(&c)); err != nil || c.ID != "c-e" {
			t.Fatalf("inspect through the CLI = %+v, %v", c, err)
		}
		if err := x.api(t.Context(), "rm", http.MethodDelete, "/containers/c-e?v=1", nil, nil); err != nil {
			t.Fatalf("rm through the CLI = %v", err)
		}
	}
	d.mu.Lock()
	conns := slices.Compact(slices.Clone(d.conns))
	d.mu.Unlock()
	if len(conns) != 1 {
		t.Fatalf("connections = %q, want every call through one docker CLI process", conns)
	}

	t.Setenv("CANTI_TEST_DIAL", "down")
	down := newEngine(func(ctx context.Context) (net.Conn, error) { return dialStdio(ctx, exe) })
	err = testEngineExecutor(down).api(t.Context(), "inspect", http.MethodGet, "/containers/canti-e/json", nil, nil)
	if err == nil || !strings.HasPrefix(err.Error(), "docker inspect: ") || !strings.Contains(err.Error(), "Cannot connect to the Docker daemon") {
		t.Fatalf("inspect with the daemon down = %v, want the CLI's reason", err)
	}
}

func TestDialStdioEndsItsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	cli := dir + "/docker"
	script := fmt.Sprintf("#!/bin/sh\nsleep 300 &\necho $! > %q/child\nexec cat\n", dir)
	if err := os.WriteFile(cli, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	conn, err := dialStdio(t.Context(), cli)
	if err != nil {
		t.Fatal(err)
	}
	var child []byte
	for deadline := time.Now().Add(5 * time.Second); len(child) == 0 || child[len(child)-1] != '\n'; {
		if time.Now().After(deadline) {
			t.Fatal("the CLI never started")
		}
		time.Sleep(10 * time.Millisecond)
		child, _ = os.ReadFile(dir + "/child")
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if b := make([]byte, 4); func() bool { _, err := io.ReadFull(conn, b); return err != nil || string(b) != "ping" }() {
		t.Fatal("the connection does not carry the CLI's standard input and output")
	}
	done := make(chan struct{})
	go func() { conn.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() waits for a child of the CLI")
	}
	pid := strings.TrimSpace(string(child))
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		out, _ := exec.Command("ps", "-o", "stat=", "-p", pid).Output()
		if s := strings.TrimSpace(string(out)); s == "" || strings.HasPrefix(s, "Z") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %s of the CLI outlived the connection", pid)
		}
	}
}
