package docker

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
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The executor speaks the Docker Engine API over connections that the
// docker CLI's "system dial-stdio" command opens, so the CLI resolves the
// daemon as it does for its own commands (DOCKER_HOST, the current context,
// TLS, ssh), and a CLI process starts per pooled connection instead of per
// call.
const (
	apiMax = "1.47"
	apiMin = "1.30"
)

// engine sends Engine API requests and negotiates the API version once.
type engine struct {
	client  *http.Client
	gate    chan struct{}
	version string
	single  bool
}

var engines sync.Map

// cliEngine returns the engine shared by every executor of the docker CLI
// at path.
func cliEngine(path string) *engine {
	if e, ok := engines.Load(path); ok {
		return e.(*engine)
	}
	e, _ := engines.LoadOrStore(path, newEngine(func(ctx context.Context) (net.Conn, error) { return dialStdio(ctx, path) }))
	return e.(*engine)
}

// endpointEngine never follows a later ambient Docker context change.
func endpointEngine(path, endpoint string) *engine {
	key := path + "\x00" + endpoint
	if e, ok := engines.Load(key); ok {
		return e.(*engine)
	}
	candidate := newEngine(func(ctx context.Context) (net.Conn, error) { return dialDocker(ctx, path, endpoint) })
	candidate.single = true
	e, _ := engines.LoadOrStore(key, candidate)
	return e.(*engine)
}

func newEngine(dial func(context.Context) (net.Conn, error)) *engine {
	return &engine{gate: make(chan struct{}, 1), client: &http.Client{Transport: &http.Transport{
		DialContext:         func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) },
		MaxIdleConnsPerHost: 2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}}}
}

// apiError is an error response of the daemon.
type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string { return e.message }

// Is reports a missing container as errMissing.
func (e *apiError) Is(target error) bool {
	return target == errMissing && e.status == http.StatusNotFound && strings.Contains(strings.ToLower(e.message), "no such container")
}

func status(err error) int {
	var e *apiError
	if errors.As(err, &e) {
		return e.status
	}
	return 0
}

// api sends one request of operation op under the operation's own deadline,
// and handles a successful response, if handle is set, within it. Once ctx is
// done it returns ctx's error; a call past only its own deadline fails with an
// ordinary error, never mistaken for the caller's shutdown. Reads and deterministic
// create/remove requests may be retried on a fresh connection. Start and stop
// are never replayed: a delayed duplicate could disturb a later start epoch.
func (x *Executor) api(ctx context.Context, op, method, path string, body any, handle func(*http.Response) error) error {
	limit := x.timeouts.call
	switch op {
	case "create", "logs", "wait":
		limit = x.timeouts.transfer
	case "stop":
		limit += x.cfg.StopGrace
	}
	call, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	err := x.engine.do(call, method, path, body, !x.engine.single && op != "start" && op != "stop", handle)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case call.Err() != nil:
		return fmt.Errorf("docker %s: no response within %v", op, limit)
	}
	if u, ok := err.(*url.Error); ok {
		err = u.Err
	}
	return fmt.Errorf("docker %s: %w", op, err)
}

func (e *engine) do(ctx context.Context, method, path string, body any, replay bool, handle func(*http.Response) error) error {
	version, err := e.negotiate(ctx)
	if err != nil {
		return err
	}
	res, err := e.send(ctx, method, "/v"+version+path, body, replay)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		data, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
		var msg struct{ Message string }
		if json.Unmarshal(data, &msg) != nil || msg.Message == "" {
			msg.Message = strings.TrimSpace(string(data))
		}
		return &apiError{res.StatusCode, msg.Message}
	}
	if handle != nil && res.StatusCode != http.StatusNotModified {
		if err := handle(res); err != nil {
			return err
		}
	}
	io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))
	return nil
}

func (e *engine) send(ctx context.Context, method, path string, body any, replay bool) (*http.Response, error) {
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if replay {
		req.Header["Idempotency-Key"] = nil
	}
	return e.client.Do(req)
}

// negotiate returns the API version the daemon is spoken to in, which it
// asks for once: apiMax, or the nearest version the daemon supports.
func (e *engine) negotiate(ctx context.Context) (string, error) {
	select {
	case e.gate <- struct{}{}:
		defer func() { <-e.gate }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if e.version != "" {
		return e.version, nil
	}
	res, err := e.send(ctx, http.MethodGet, "/version", nil, !e.single)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	var v struct{ ApiVersion, MinAPIVersion string }
	if res.StatusCode == http.StatusOK {
		err = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&v)
	}
	switch _, _, ok := version(v.ApiVersion); {
	case res.StatusCode != http.StatusOK || err != nil || !ok || older(v.ApiVersion, apiMin):
		return "", fmt.Errorf("the daemon reports API version %q with status %d; at least %s is required", v.ApiVersion, res.StatusCode, apiMin)
	case older(v.ApiVersion, apiMax):
		e.version = v.ApiVersion
	case older(apiMax, v.MinAPIVersion):
		e.version = v.MinAPIVersion
	default:
		e.version = apiMax
	}
	return e.version, nil
}

// older reports whether API version a precedes b.
func older(a, b string) bool {
	am, an, aok := version(a)
	bm, bn, bok := version(b)
	return aok && bok && (am < bm || am == bm && an < bn)
}

func version(v string) (int, int, bool) {
	major, minor, ok := strings.Cut(v, ".")
	m, err1 := strconv.Atoi(major)
	n, err2 := strconv.Atoi(minor)
	return m, n, ok && err1 == nil && err2 == nil
}

// stdioConn is a connection to the daemon through one "docker system
// dial-stdio" process, in its own process group so that a terminal's
// interrupt reaches only canti. Closing it ends the group.
type stdioConn struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    io.ReadCloser
	stderr *tail
	once   sync.Once
}

func dialStdio(ctx context.Context, docker string) (net.Conn, error) {
	return dialDocker(ctx, docker, "")
}

func dialDocker(ctx context.Context, docker, endpoint string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := []string{"system", "dial-stdio"}
	if endpoint != "" {
		args = append([]string{"--host", endpoint}, args...)
	}
	cmd := exec.Command(docker, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	c := &stdioConn{cmd: cmd, stderr: &tail{}}
	cmd.Stderr = c.stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	c.in, c.out = in, out
	return c, nil
}

// Read and Write report the end of the process with what it wrote to
// standard error, such as why it could not reach the daemon.
func (c *stdioConn) Read(p []byte) (int, error) {
	n, err := c.out.Read(p)
	if err == io.EOF {
		err = c.fail(err)
	}
	return n, err
}

func (c *stdioConn) Write(p []byte) (int, error) {
	n, err := c.in.Write(p)
	if err != nil {
		err = c.fail(err)
	}
	return n, err
}

func (c *stdioConn) fail(err error) error {
	c.Close()
	if s := c.stderr.String(); s != "" {
		return errors.New(s)
	}
	return err
}

func (c *stdioConn) Close() error {
	c.once.Do(func() {
		c.in.Close()
		syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
		c.cmd.Wait()
	})
	return nil
}

func (c *stdioConn) LocalAddr() net.Addr              { return stdioAddr{} }
func (c *stdioConn) RemoteAddr() net.Addr             { return stdioAddr{} }
func (c *stdioConn) SetDeadline(time.Time) error      { return nil }
func (c *stdioConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stdioConn) SetWriteDeadline(time.Time) error { return nil }

type stdioAddr struct{}

func (stdioAddr) Network() string { return "stdio" }
func (stdioAddr) String() string  { return "docker system dial-stdio" }

// tail keeps the last 4 KiB written to it.
type tail struct {
	mu sync.Mutex
	b  []byte
}

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b = append(t.b, p...)
	if over := len(t.b) - 4<<10; over > 0 {
		t.b = t.b[over:]
	}
	return len(p), nil
}

func (t *tail) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.b))
}
