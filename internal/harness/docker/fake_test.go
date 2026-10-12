package docker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// fake is the Docker daemon of the executor under test. It answers each
// operation, after running the operation's hook, with the reply set for it
// or a default success, and records the call. Its docker CLI only pulls.
type fake struct {
	t       *testing.T
	ctl     string
	x       *Executor
	stop    chan struct{}
	mu      sync.Mutex
	replies map[string]reply
	hooks   map[string]func(*http.Request)
	log     []string
	signals map[string]chan struct{}
}

// reply answers an operation with body, or, with an error status, with body
// as the daemon's message; down drops the connection without an answer.
type reply struct {
	status int
	body   string
	down   bool
}

const fakeCLI = `#!/bin/sh
echo "$*" >> %[1]q/pulls
[ -e %[1]q/pull.hang ] && exec sleep 30
exit "$(cat %[1]q/pull.exit 2>/dev/null || echo 0)"
`

func newFake(t *testing.T) *fake {
	f := &fake{t: t, ctl: t.TempDir(), stop: make(chan struct{}), replies: map[string]reply{},
		hooks: map[string]func(*http.Request){}, signals: map[string]chan struct{}{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(f.stop) })
	cli := filepath.Join(f.ctl, "docker")
	if err := os.WriteFile(cli, fmt.Appendf(nil, fakeCLI, f.ctl), 0o755); err != nil {
		t.Fatal(err)
	}
	f.x = newTestExecutor(t, Config{Docker: cli})
	f.x.engine = dialing(srv)
	return f
}

// dialing is an engine that reaches srv over TCP.
func dialing(srv *httptest.Server) *engine {
	addr := srv.Listener.Addr().String()
	return newEngine(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "tcp", addr) })
}

func (f *fake) serve(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body)
	if r.URL.Path == "/version" {
		io.WriteString(w, `{"ApiVersion":"1.47","MinAPIVersion":"1.24"}`)
		return
	}
	op, call := route(r)
	f.mu.Lock()
	f.log = append(f.log, call)
	hook := f.hooks[op]
	f.mu.Unlock()
	if hook != nil {
		hook(r)
	}
	if r.Context().Err() != nil {
		return
	}
	f.mu.Lock()
	re, set := f.replies[op]
	f.mu.Unlock()
	switch {
	case re.down:
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	case re.status >= 400:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(re.status)
		json.NewEncoder(w).Encode(map[string]string{"message": re.body})
	case set:
		io.WriteString(w, re.body)
	case op == "ps":
		io.WriteString(w, "[]")
	case op == "create":
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"Id":"","Warnings":[]}`)
	case op == "start", op == "stop", op == "rm":
		w.WriteHeader(http.StatusNoContent)
	}
}

// route names the operation of r and the call it records: the operation and
// the container it names, the grace of a stop, the path an archive reads, and
// what a listing includes.
func route(r *http.Request) (string, string) {
	_, path, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	parts := strings.Split(path, "/")
	q := r.URL.Query()
	switch {
	case path == "containers/json":
		return "ps", "ps all=" + q.Get("all") + " " + q.Get("filters")
	case path == "containers/create":
		return "create", "create " + q.Get("name")
	case len(parts) == 2 && r.Method == http.MethodDelete:
		return "rm", "rm " + parts[1] + " v=" + q.Get("v")
	case len(parts) != 3:
		return "unknown", "unknown " + r.Method + " " + path
	}
	op := map[string]string{"json": "inspect", "archive": "cp"}[parts[2]]
	if op == "" {
		op = parts[2]
	}
	call := op + " " + parts[1]
	switch op {
	case "stop":
		call += " " + q.Get("t")
	case "cp":
		call += " " + q.Get("path")
	case "logs":
		call += " stdout=" + q.Get("stdout") + " stderr=" + q.Get("stderr")
	}
	return op, call
}

// frame is body as one frame of stream in a multiplexed log.
func frame(stream byte, body string) string {
	if body == "" {
		return ""
	}
	h := []byte{stream, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(h[4:], uint32(len(body)))
	return string(h) + body
}

func (f *fake) reply(op, body string) { f.answer(op, reply{body: body}) }

// output makes the job's log text, written to its standard output.
func (f *fake) output(text string) { f.reply("logs", frame(1, text)) }

func (f *fake) fail(op string, status int, message string) {
	f.answer(op, reply{status: status, body: message})
}

// down makes op fail as a daemon that drops the connection does.
func (f *fake) down(op string) { f.answer(op, reply{down: true}) }

func (f *fake) answer(op string, re reply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies[op] = re
}

// clear restores the default reply of each op.
func (f *fake) clear(ops ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range ops {
		delete(f.replies, op)
	}
}

// hook runs fn before op answers; nil removes the hook.
func (f *fake) hook(op string, fn func(*http.Request)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if fn == nil {
		delete(f.hooks, op)
		return
	}
	f.hooks[op] = fn
}

// hang makes op answer nothing until its caller gives up.
func (f *fake) hang(op string) { f.hook(op, f.wait) }

func (f *fake) wait(r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-f.stop:
	}
}

func (f *fake) missing() {
	f.fail("inspect", http.StatusNotFound, "No such container: canti-e")
}

func (f *fake) container(status string, exit int) string {
	return f.containerOf("e", "true", status, exit)
}

// containerOf is the inspect output of the container Bind creates for id.
func (f *fake) containerOf(id, command, status string, exit int) string {
	return f.inspected(id, command, status, exit, map[string]string{exitLabel: exitFile})
}

// legacy is f.container as an earlier revision created it, without the exit
// label.
func (f *fake) legacy(status string, exit int) string {
	return f.inspected("e", "true", status, exit, nil)
}

func (f *fake) inspected(id, command, status string, exit int, labels map[string]string) string {
	var c container
	c.ID = "c-" + id
	c.Config.Labels = map[string]string{"canti.effect": id, "canti.state": f.x.cfg.State, "canti.spec": specHash(f.x.runArgs(command))}
	maps.Copy(c.Config.Labels, labels)
	c.State.Status, c.State.ExitCode = status, exit
	c.State.StartedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.State.FinishedAt = c.State.StartedAt.Add(time.Second)
	b, err := json.Marshal(c)
	if err != nil {
		f.t.Fatal(err)
	}
	return string(b)
}

// listing is the daemon's listing of containers, each an ID followed by the
// effect it is labelled with.
func listing(pairs ...string) string {
	var cs []map[string]any
	for i := 0; i < len(pairs); i += 2 {
		cs = append(cs, map[string]any{"Id": pairs[i], "Labels": map[string]string{"canti.effect": pairs[i+1]}})
	}
	b, _ := json.Marshal(cs)
	return string(b)
}

// exited makes the job of effect e exit with code, which its shell recorded.
func (f *fake) exited(code int) {
	f.reply("inspect", f.container("exited", code))
	f.reply("cp", exitArchive(f.t, fmt.Sprintf("%d\n", code)))
}

// calls returns the recorded calls of op, in order.
func (f *fake) calls(op string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var lines []string
	for _, line := range f.log {
		if strings.HasPrefix(line, op+" ") {
			lines = append(lines, line)
		}
	}
	return lines
}

// pulls returns the arguments of every pull of the fake's docker CLI.
func (f *fake) pulls() []string {
	b, err := os.ReadFile(filepath.Join(f.ctl, "pulls"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		f.t.Fatal(err)
	}
	var lines []string
	for line := range strings.Lines(string(b)) {
		lines = append(lines, strings.TrimSpace(line))
	}
	return lines
}

func (f *fake) latch(value string) {
	if err := os.MkdirAll(f.x.dir("e"), 0o700); err != nil {
		f.t.Fatal(err)
	}
	if err := setLatch(f.x.dir("e"), value); err != nil {
		f.t.Fatal(err)
	}
}

// signal reports that name happened.
func (f *fake) signal(name string) {
	ch := f.signalled(name)
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (f *fake) signalled(name string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.signals[name] == nil {
		f.signals[name] = make(chan struct{})
	}
	return f.signals[name]
}

func (f *fake) happened(name string) bool {
	select {
	case <-f.signalled(name):
		return true
	default:
		return false
	}
}

// cancelAt returns a context that is cancelled once the fake signals name.
func (f *fake) cancelAt(name string) context.Context {
	ctx, cancel := context.WithCancel(f.t.Context())
	f.t.Cleanup(cancel)
	ch := f.signalled(name)
	go func() {
		defer cancel()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}()
	return ctx
}

var effect = harness.Effect{ID: "e", Command: "true"}

func TestInspectReportsMissingOnlyWhenDockerSaysSo(t *testing.T) {
	f := newFake(t)
	f.missing()
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || err != nil {
		t.Fatalf("Inspect(no such container) = %v, %v; want not found", found, err)
	}
	for name, fail := range map[string]func(){
		"not found otherwise": func() { f.fail("inspect", http.StatusNotFound, "page not found") },
		"daemon error":        func() { f.fail("inspect", http.StatusInternalServerError, "No such container: canti-e") },
		"daemon down":         func() { f.down("inspect") },
	} {
		fail()
		if _, found, err := f.x.Inspect(t.Context(), "e"); found || err == nil {
			t.Fatalf("Inspect(%s) = %v, %v; want an error", name, found, err)
		}
	}
	for _, out := range []string{"", "{}", "[]", "not json", f.container("created", 0) + "{}",
		strings.ReplaceAll(f.container("created", 0), `"canti.effect":"e"`, `"canti.effect":"x"`)} {
		f.reply("inspect", out)
		if _, found, err := f.x.Inspect(t.Context(), "e"); found || err == nil {
			t.Fatalf("Inspect(%s) = %v, %v; want an error", out, found, err)
		}
	}
}

func TestBindCreatesNothingOnceContextIsDone(t *testing.T) {
	f := newFake(t)
	f.missing()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.x.Bind(ctx, effect); !errors.Is(err, context.Canceled) {
		t.Fatalf("Bind(done) = %v", err)
	}
	if calls := f.calls("inspect"); len(calls) != 0 {
		t.Fatalf("Bind(done) called docker: %q", calls)
	}
}

func TestBindPullsAMissingImageOnce(t *testing.T) {
	f := newFake(t)
	f.missing()
	created := f.container("created", 0)
	f.fail("create", http.StatusNotFound, "No such image: alpine:3.23")
	f.hook("create", func(*http.Request) {
		if len(f.pulls()) > 0 {
			f.clear("create")
			f.reply("inspect", created)
		}
	})
	if job, err := f.x.Bind(t.Context(), effect); err != nil || job.ID != "c-e" || job.Status != harness.JobPending {
		t.Fatalf("Bind(image missing) = %+v, %v; want the job created after a pull", job, err)
	}
	if pulls, creates := f.pulls(), f.calls("create"); !slices.Equal(pulls, []string{"pull --quiet alpine:3.23"}) || len(creates) != 2 {
		t.Fatalf("pulls %q, creates %q; want one pull between two creates", pulls, creates)
	}

	g := newFake(t)
	g.missing()
	g.fail("create", http.StatusNotFound, "No such image: alpine:3.23")
	if err := os.WriteFile(filepath.Join(g.ctl, "pull.exit"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := g.x.Bind(t.Context(), effect); err == nil || !strings.Contains(err.Error(), "docker pull") || len(g.calls("create")) != 1 {
		t.Fatalf("Bind(pull fails) = %v, creates %q; want the pull's error and no second create", err, g.calls("create"))
	}
	g.fail("create", http.StatusNotFound, "page not found")
	if _, err := g.x.Bind(t.Context(), effect); err == nil || len(g.pulls()) != 1 {
		t.Fatalf("Bind(create not found otherwise) = %v, pulls %q; want an error without a pull", err, g.pulls())
	}

	h := newFake(t)
	h.x.timeouts.transfer = 100 * time.Millisecond
	h.missing()
	h.fail("create", http.StatusNotFound, "No such image: alpine:3.23")
	if err := os.WriteFile(filepath.Join(h.ctl, "pull.hang"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if _, err := h.x.Bind(t.Context(), effect); err == nil || err.Error() != "docker pull: no response within 100ms" || time.Since(began) > 5*time.Second {
		t.Fatalf("Bind(pull hung) = %v after %v; want the pull's own deadline", err, time.Since(began))
	}
}

func TestBindAdoptsOnlyItsOwnContainerOfALostCreateResponse(t *testing.T) {
	conflict := `Conflict. The container name "/canti-e" is already in use by container "c-e".`
	f := newFake(t)
	created := f.container("created", 0)
	for name, held := range map[string]string{
		"another state": strings.ReplaceAll(created, f.x.cfg.State, "/other"),
		"another spec":  f.containerOf("e", "false", "created", 0),
		"own":           created,
	} {
		f.missing()
		f.hook("create", func(*http.Request) { f.reply("inspect", held) })
		f.fail("create", http.StatusConflict, conflict)
		job, err := f.x.Bind(t.Context(), effect)
		if own := name == "own"; own != (err == nil) || own && (job.ID != "c-e" || job.Status != harness.JobPending) {
			t.Fatalf("Bind(name held by %s) = %+v, %v", name, job, err)
		}
	}
	if creates := f.calls("create"); len(creates) != 3 {
		t.Fatalf("creates = %q, want one for each Bind", creates)
	}
}

func TestStartNeverRestartsAnUnlatchedContainer(t *testing.T) {
	f := newFake(t)
	for _, status := range []string{"exited", "running"} {
		f.reply("inspect", f.container(status, 0))
		if _, err := f.x.Start(t.Context(), "e"); err == nil {
			t.Fatalf("Start(%s without latch) succeeded", status)
		}
	}
	if latch, err := readLatch(f.x.dir("e")); latch != "" || err != nil || len(f.calls("start")) != 0 {
		t.Fatalf("latch %q, %v, starts %q", latch, err, f.calls("start"))
	}
}

func TestFailedStartIsNeverReissued(t *testing.T) {
	for name, fail := range map[string]func(*fake){
		"rejected":    func(f *fake) { f.fail("start", http.StatusInternalServerError, "boom") },
		"answer lost": func(f *fake) { f.down("start") },
		"no answer":   func(f *fake) { f.x.timeouts.call = 50 * time.Millisecond; f.hang("start") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.reply("inspect", f.container("created", 0))
			fail(f)
			if _, err := f.x.Start(t.Context(), "e"); err == nil {
				t.Fatal("Start() succeeded")
			}
			f.x.timeouts.call = callTimeout
			for range 2 {
				if job, err := f.x.Start(t.Context(), "e"); err != nil || job.Status != harness.JobUnknown {
					t.Fatalf("Start() again = %+v, %v; want unknown", job, err)
				}
			}
			if err := f.x.Cancel(t.Context(), "e"); err != nil {
				t.Fatal(err)
			}
			if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || job.Status != harness.JobUnknown {
				t.Fatalf("Inspect() = %+v, %v; want unknown", job, err)
			}
			if starts, stops := f.calls("start"), f.calls("stop"); len(starts) != 1 || len(stops) != 0 {
				t.Fatalf("starts %q, stops %q", starts, stops)
			}
		})
	}
}

// rejecting makes the fake daemon reject a start as it does for an unknown
// user, calling before first.
func (f *fake) rejecting(before func()) {
	f.reply("inspect", f.container("created", 0))
	var c container
	if err := json.Unmarshal([]byte(f.container("created", 128)), &c); err != nil {
		f.t.Fatal(err)
	}
	c.State.Error, c.State.StartedAt, c.State.FinishedAt = "unable to find user nosuchuser", time.Time{}, time.Time{}
	rejected, _ := json.Marshal(c)
	f.hook("start", func(*http.Request) {
		if before != nil {
			before()
		}
		f.reply("inspect", string(rejected))
	})
	f.fail("start", http.StatusInternalServerError, "unable to find user nosuchuser")
}

func TestRejectedStartFailsToStart(t *testing.T) {
	f := newFake(t)
	f.rejecting(nil)
	want, err := f.x.Start(t.Context(), "e")
	out := harness.ArtifactRef{Path: "e/output", Available: true, Detail: "job never started: unable to find user nosuchuser"}
	if err != nil || want.ID != "c-e" || want.Status != harness.JobFailedToStart || want.ExitCode != nil || !want.StartedAt.IsZero() || want.Output != out {
		t.Fatalf("Start(rejected) = %+v, %v; want the job failed to start with output %+v", want, err, out)
	}
	if _, err := os.Stat(f.x.record("e")); err != nil {
		t.Fatalf("the failed start was not recorded: %v", err)
	}
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm c-e v=1" {
		t.Fatalf("rms = %q, want the created container released", rms)
	}
	f.missing()
	if job, err := f.x.Start(t.Context(), "e"); err != nil || !same(job, want) {
		t.Fatalf("Start() again = %+v, %v; want %+v", job, err, want)
	}
	if err := f.x.Cancel(t.Context(), "e"); err != nil {
		t.Fatal(err)
	}
	if job, found, err := f.x.Inspect(t.Context(), "e"); err != nil || !found || !same(job, want) {
		t.Fatalf("Inspect() = %+v, %v, %v; want %+v", job, found, err, want)
	}
	if starts, stops := f.calls("start"), f.calls("stop"); len(starts) != 1 || len(stops) != 0 {
		t.Fatalf("starts %q, stops %q", starts, stops)
	}
}

func TestRejectedStartKeepsItsReason(t *testing.T) {
	f := newFake(t)
	f.rejecting(nil)
	f.fail("logs", http.StatusInternalServerError, "boom")
	job, err := f.x.Start(t.Context(), "e")
	if want := "job never started: unable to find user nosuchuser; logs unavailable: docker logs: boom"; err != nil || job.Status != harness.JobFailedToStart || job.Output.Detail != want {
		t.Fatalf("Start(rejected, capture fails) = %+v, %v; want detail %q", job, err, want)
	}
	w := newFake(t)
	w.rejecting(nil)
	if err := os.WriteFile(filepath.Join(w.x.artifacts(), "e"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	job, err = w.x.Start(t.Context(), "e")
	if err != nil || !strings.HasPrefix(job.Output.Detail, "job never started: unable to find user nosuchuser; artifact write failed: ") {
		t.Fatalf("Start(rejected, artifact unwritable) = %+v, %v; want the reason kept", job, err)
	}
	g := newFake(t)
	g.rejecting(nil)
	if _, err := g.x.Start(t.Context(), "e"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(g.x.artifacts(), "e", "output")); err != nil {
		t.Fatal(err)
	}
	job, _, err = g.x.Inspect(t.Context(), "e")
	if err != nil || !strings.HasPrefix(job.Output.Detail, "job never started: unable to find user nosuchuser; artifact missing: ") {
		t.Fatalf("Inspect(artifact removed) = %+v, %v; want the reason kept", job, err)
	}
}

func TestRejectedStartDuringShutdownIsRecordedLater(t *testing.T) {
	f := newFake(t)
	f.rejecting(func() { f.signal("began"); time.Sleep(300 * time.Millisecond) })
	if _, err := f.x.Start(f.cancelAt("began"), "e"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start(rejected during shutdown) = %v, want cancellation", err)
	}
	if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 {
		t.Fatalf("a report after shutdown was recorded: %v, rms %q", err, f.calls("rm"))
	}
	job, err := f.x.Start(t.Context(), "e")
	if err != nil || job.Status != harness.JobFailedToStart || len(f.calls("start")) != 1 || len(f.calls("rm")) != 1 {
		t.Fatalf("Start() again = %+v, %v, starts %q; want the failed start recorded and released", job, err, f.calls("start"))
	}
}

func TestLaunchOutlivesShutdown(t *testing.T) {
	f := newFake(t)
	f.reply("inspect", f.container("created", 0))
	running := f.container("running", 0)
	f.hook("start", func(*http.Request) {
		f.signal("began")
		time.Sleep(300 * time.Millisecond)
		f.reply("inspect", running)
	})
	if job, err := f.x.Start(f.cancelAt("began"), "e"); err != nil || job.Status != harness.JobRunning {
		t.Fatalf("Start() interrupted after the latch = %+v, %v; want running", job, err)
	}
	if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || job.Status != harness.JobRunning {
		t.Fatalf("Inspect() = %+v, %v; want running", job, err)
	}
}

func TestHungCallsFailAtTheirOwnDeadlines(t *testing.T) {
	f := newFake(t)
	f.x.timeouts = timeouts{call: 50 * time.Millisecond, transfer: 150 * time.Millisecond}
	f.x.cfg.StopGrace = 100 * time.Millisecond
	for _, tt := range []struct {
		op, method, path string
		limit            time.Duration
	}{
		{"inspect", http.MethodGet, "/containers/canti-e/json", 50 * time.Millisecond},
		{"create", http.MethodPost, "/containers/create?name=canti-e", 50 * time.Millisecond},
		{"start", http.MethodPost, "/containers/c-e/start", 50 * time.Millisecond},
		{"stop", http.MethodPost, "/containers/c-e/stop?t=1", 150 * time.Millisecond},
		{"rm", http.MethodDelete, "/containers/c-e?v=1", 50 * time.Millisecond},
		{"ps", http.MethodGet, "/containers/json?all=1", 50 * time.Millisecond},
		{"cp", http.MethodGet, "/containers/c-e/archive?path=%2Ftmp%2F.canti-exit", 50 * time.Millisecond},
		{"logs", http.MethodGet, "/containers/c-e/logs?stdout=1&stderr=1", 150 * time.Millisecond},
	} {
		f.hang(tt.op)
		began := time.Now()
		err := f.x.api(t.Context(), tt.op, tt.method, tt.path, nil, nil)
		took := time.Since(began)
		if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			err.Error() != fmt.Sprintf("docker %s: no response within %v", tt.op, tt.limit) || took < tt.limit || took > tt.limit+5*time.Second {
			t.Fatalf("docker %s (hung) = %v after %v; want an ordinary error at %v", tt.op, err, took, tt.limit)
		}
		if calls := f.calls(tt.op); len(calls) != 1 {
			t.Fatalf("%s calls = %q, want one", tt.op, calls)
		}
	}
	f.x.timeouts.call = time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := f.x.api(ctx, "ps", http.MethodGet, "/containers/json?all=1", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("docker ps (hung, caller's deadline first) = %v, want the caller's error", err)
	}
	f.latch(latchStart)
	f.hook("inspect", nil)
	f.hook("cp", nil)
	f.hook("rm", nil)
	f.exited(0)
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobSucceeded || job.Output.Available || job.Output.Detail != "logs unavailable: docker logs: no response within 150ms" {
		t.Fatalf("Inspect(capture hung) = %+v, %v; want the job recorded without output", job, err)
	}
}

func TestVanishedStartedJobIsNotReportedMissing(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.missing()
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || !errors.Is(err, harness.ErrJobLost) {
		t.Fatalf("Inspect() = %v, %v; want ErrJobLost", found, err)
	}
	if _, err := f.x.Bind(t.Context(), effect); !errors.Is(err, harness.ErrJobLost) || len(f.calls("create")) != 0 {
		t.Fatalf("Bind() = %v, creates %q; want ErrJobLost and no replacement", err, f.calls("create"))
	}
	if _, err := f.x.Start(t.Context(), "e"); !errors.Is(err, harness.ErrJobLost) || len(f.calls("start")) != 0 {
		t.Fatalf("Start() = %v, starts %q; want ErrJobLost", err, f.calls("start"))
	}
	if err := f.x.Cancel(t.Context(), "e"); err != nil {
		t.Fatalf("Cancel() = %v", err)
	}
}

func TestBindAfterFenceRecordsCancelledJobWithoutAContainer(t *testing.T) {
	f := newFake(t)
	f.latch(latchCancel)
	f.missing()
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || err != nil {
		t.Fatalf("Inspect(fenced) = %v, %v; want not found", found, err)
	}
	want := harness.Job{ID: "fenced:e", Status: harness.JobCancelled, Output: harness.ArtifactRef{Path: "e/output", Available: true, Detail: "job was cancelled; output is partial"}}
	if job, err := f.x.Bind(t.Context(), effect); err != nil || !same(job, want) {
		t.Fatalf("Bind(fenced) = %+v, %v; want %+v", job, err, want)
	}
	if fi, err := os.Stat(filepath.Join(f.x.artifacts(), "e", "output")); err != nil || fi.Size() != 0 || fi.Mode().Perm() != 0o644 {
		t.Fatalf("artifact: %v, %v; want an empty one", fi, err)
	}
	for range 2 {
		if job, err := f.x.Bind(t.Context(), effect); err != nil || !same(job, want) {
			t.Fatalf("Bind(recorded) = %+v, %v; want %+v", job, err, want)
		}
		if job, err := f.x.Start(t.Context(), "e"); err != nil || !same(job, want) {
			t.Fatalf("Start(recorded) = %+v, %v; want %+v", job, err, want)
		}
		if job, found, err := f.x.Inspect(t.Context(), "e"); err != nil || !found || !same(job, want) {
			t.Fatalf("Inspect(recorded) = %+v, %v, %v; want %+v", job, found, err, want)
		}
		if err := f.x.Cancel(t.Context(), "e"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.x.Bind(t.Context(), harness.Effect{ID: "e", Command: "false"}); err == nil {
		t.Fatal("Bind(changed) succeeded")
	}
	if inspects, ps := f.calls("inspect"), f.calls("ps"); len(inspects) != 2 || len(ps) != 4 {
		t.Fatalf("inspects %q, ps %q; want only the lookups that found no container and each Bind's sweep", inspects, ps)
	}
	for _, op := range []string{"create", "start", "stop", "logs", "cp", "rm"} {
		if calls := f.calls(op); len(calls) != 0 {
			t.Fatalf("%s calls = %q, want none for a job that never had a container", op, calls)
		}
	}
}

func TestBindAfterFenceKeepsTheJobBoundBeforeIt(t *testing.T) {
	f := newFake(t)
	f.missing()
	created := f.container("created", 0)
	f.hook("create", func(*http.Request) { f.reply("inspect", created) })
	if job, err := f.x.Bind(t.Context(), effect); err != nil || job.ID != "c-e" || job.Status != harness.JobPending {
		t.Fatalf("Bind() = %+v, %v; want the created job pending", job, err)
	}
	if err := f.x.Cancel(t.Context(), "e"); err != nil {
		t.Fatal(err)
	}
	want, err := f.x.Bind(t.Context(), effect)
	if err != nil || want.ID != "c-e" || want.Status != harness.JobCancelled {
		t.Fatalf("Bind() after the fence = %+v, %v; want the job bound before it, cancelled", want, err)
	}
	if _, err := os.Stat(f.x.record("e")); err != nil {
		t.Fatalf("the cancelled job was not recorded: %v", err)
	}
	f.missing()
	if job, err := f.x.Bind(t.Context(), effect); err != nil || !same(job, want) {
		t.Fatalf("Bind() after the container was removed = %+v, %v; want %+v", job, err, want)
	}
	if job, found, err := f.x.Inspect(t.Context(), "e"); err != nil || !found || !same(job, want) {
		t.Fatalf("Inspect() = %+v, %v, %v; want %+v", job, found, err, want)
	}
	if creates, rms := f.calls("create"), f.calls("rm"); len(creates) != 1 || creates[0] != "create canti-e" || len(rms) != 3 || rms[0] != "rm c-e v=1" {
		t.Fatalf("creates %q, rms %q; want one container, recorded and released", creates, rms)
	}
}

func same(a, b harness.Job) bool {
	x, errx := json.Marshal(a)
	y, erry := json.Marshal(b)
	return errx == nil && erry == nil && string(x) == string(y)
}

func TestFinishedJobIsRecordedThenReleased(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(3)
	f.output("hello\n")
	f.hook("rm", func(*http.Request) {
		if fi, err := os.Stat(f.x.record("e")); err != nil || fi.Size() == 0 {
			f.signal("early")
		}
	})
	stale := filepath.Join(f.x.artifacts(), "e", ".tmp-crashed")
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	want, found, err := f.x.Inspect(t.Context(), "e")
	if err != nil || !found || want.ID != "c-e" || want.Status != harness.JobFailed || want.ExitCode == nil || *want.ExitCode != 3 ||
		want.Preview != "hello\n" || !want.Output.Complete || want.StartedAt.IsZero() || !want.FinishedAt.After(want.StartedAt) {
		t.Fatalf("Inspect() = %+v, %v, %v", want, found, err)
	}
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm c-e v=1" {
		t.Fatalf("rms = %q, want the finished container released", rms)
	}
	if logs := f.calls("logs"); len(logs) != 1 || logs[0] != "logs c-e stdout=1 stderr=1" {
		t.Fatalf("logs = %q, want both streams of the finished container", logs)
	}
	if f.happened("early") {
		t.Fatal("the container was removed before its record was durable")
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale capture file survived: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(f.x.artifacts(), "e", "output")); err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("artifact mode: %v, %v", fi, err)
	}
	foreign := strings.ReplaceAll(strings.ReplaceAll(f.container("created", 0), `"c-e"`, `"c-other"`), f.x.cfg.State, "/other")
	for name, gone := range map[string]func(){
		"removed": f.missing,
		"renamed": func() { f.reply("inspect", foreign) },
	} {
		gone()
		if job, found, err := f.x.Inspect(t.Context(), "e"); err != nil || !found || !same(job, want) {
			t.Fatalf("%s: Inspect() = %+v, %v, %v; want %+v", name, job, found, err, want)
		}
		if job, err := f.x.Bind(t.Context(), effect); err != nil || !same(job, want) {
			t.Fatalf("%s: Bind() = %+v, %v; want %+v", name, job, err, want)
		}
		if _, err := f.x.Bind(t.Context(), harness.Effect{ID: "e", Command: "false"}); err == nil {
			t.Fatalf("%s: Bind(changed) succeeded", name)
		}
		if job, err := f.x.Start(t.Context(), "e"); err != nil || !same(job, want) {
			t.Fatalf("%s: Start() = %+v, %v; want %+v", name, job, err, want)
		}
		if err := f.x.Cancel(t.Context(), "e"); err != nil {
			t.Fatalf("%s: Cancel() = %v", name, err)
		}
	}
	for _, op := range []string{"create", "start", "stop"} {
		if calls := f.calls(op); len(calls) != 0 {
			t.Fatalf("%s calls = %q", op, calls)
		}
	}
	if logs := f.calls("logs"); len(logs) != 1 {
		t.Fatalf("logs = %q, want one capture", logs)
	}
}

func TestFailedReleaseIsRetriedByTheNextReport(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.output("hello\n")
	f.fail("rm", http.StatusInternalServerError, "boom")
	want, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || want.Status != harness.JobSucceeded || want.Preview != "hello\n" {
		t.Fatalf("Inspect(rm fails) = %+v, %v", want, err)
	}
	for i := range 2 {
		if i == 1 {
			f.clear("rm")
		}
		if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || !same(job, want) {
			t.Fatalf("Inspect() again = %+v, %v; want %+v", job, err, want)
		}
	}
	if rms, logs := f.calls("rm"), f.calls("logs"); len(rms) != 3 || rms[2] != "rm c-e v=1" || len(logs) != 1 {
		t.Fatalf("rms %q, logs %q; want the removal retried on every report and one capture", rms, logs)
	}
}

func TestBindReleasesRecordedJobsOfOtherEffects(t *testing.T) {
	f := newFake(t)
	for id, job := range map[string]string{"old": "c-old", "moved": "c-moved"} {
		if err := os.MkdirAll(f.x.dir(id), 0o700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(record{Job: harness.Job{ID: job, Status: harness.JobSucceeded}, Spec: "spec"})
		if err := publish(f.x.record(id), data, false); err != nil {
			t.Fatal(err)
		}
	}
	f.reply("ps", listing("c-old", "old", "c-live", "live", "c-other", "moved"))
	f.missing()
	created := f.container("created", 0)
	f.hook("create", func(*http.Request) { f.reply("inspect", created) })
	if job, err := f.x.Bind(t.Context(), effect); err != nil || job.ID != "c-e" || job.Status != harness.JobPending || len(f.calls("create")) != 1 {
		t.Fatalf("Bind() = %+v, %v, creates %q; want the created job pending", job, err, f.calls("create"))
	}
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm c-old v=1" {
		t.Fatalf("rms = %q, want only the recorded job of another effect released", rms)
	}
	filters, _ := json.Marshal(map[string][]string{"label": {"canti.state=" + f.x.cfg.State}})
	if ps := f.calls("ps"); len(ps) != 1 || ps[0] != "ps all=1 "+string(filters) {
		t.Fatalf("ps calls = %q, want one listing of every container of this state, running or not", ps)
	}
	if _, err := f.x.Bind(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	if _, err := f.x.Start(t.Context(), "e"); err != nil {
		t.Fatal(err)
	}
	f.exited(0)
	if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || job.Status != harness.JobSucceeded {
		t.Fatalf("Inspect() = %+v, %v; want the job recorded", job, err)
	}
	if ps, rms := f.calls("ps"), f.calls("rm"); len(ps) != 2 || !slices.Equal(rms, []string{"rm c-old v=1", "rm c-old v=1", "rm c-e v=1"}) {
		t.Fatalf("ps %q, rms %q; want each Bind, and neither Start nor Inspect, to sweep", ps, rms)
	}
}

func TestBindOfARecordedEffectReleasesOtherRecordedJobs(t *testing.T) {
	f := newFake(t)
	for id, job := range map[string]string{"a": "c-a", "e": "c-e"} {
		if err := os.MkdirAll(f.x.dir(id), 0o700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(record{Job: harness.Job{ID: job, Status: harness.JobSucceeded}, Spec: specHash(f.x.runArgs("true"))})
		if err := publish(f.x.record(id), data, false); err != nil {
			t.Fatal(err)
		}
	}
	f.reply("ps", listing("c-a", "a", "c-e", "e"))
	if job, err := f.x.Bind(t.Context(), effect); err != nil || job.ID != "c-e" {
		t.Fatalf("Bind() = %+v, %v; want the recorded job", job, err)
	}
	if rms := f.calls("rm"); !slices.Contains(rms, "rm c-a v=1") {
		t.Fatalf("rms = %q, want the recorded job of another effect released too", rms)
	}
}

func TestBindReleasesOtherJobsBeforeItsOwnHungRelease(t *testing.T) {
	f := newFake(t)
	for id, job := range map[string]string{"a": "c-a", "e": "c-e"} {
		if err := os.MkdirAll(f.x.dir(id), 0o700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(record{Job: harness.Job{ID: job, Status: harness.JobSucceeded}, Spec: specHash(f.x.runArgs("true"))})
		if err := publish(f.x.record(id), data, false); err != nil {
			t.Fatal(err)
		}
	}
	f.reply("ps", listing("c-a", "a", "c-e", "e"))
	f.hook("rm", func(r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/c-e") {
			f.wait(r)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if job, err := f.x.Bind(ctx, effect); err != nil || job.ID != "c-e" {
		t.Fatalf("Bind() = %+v, %v; want the recorded job", job, err)
	}
	if rms := f.calls("rm"); !slices.Contains(rms, "rm c-a v=1") {
		t.Fatalf("rms = %q, want the recorded job of another effect released before the effect's own hung removal", rms)
	}
}

func TestRebindingRetriesAFailedRelease(t *testing.T) {
	f := newFake(t)
	if err := os.MkdirAll(f.x.dir("a"), 0o700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(record{Job: harness.Job{ID: "c-a", Status: harness.JobSucceeded}, Spec: "spec"})
	if err := publish(f.x.record("a"), data, false); err != nil {
		t.Fatal(err)
	}
	f.reply("ps", listing("c-a", "a"))
	f.fail("rm", http.StatusInternalServerError, "boom")
	f.missing()
	created := f.container("created", 0)
	f.hook("create", func(*http.Request) { f.reply("inspect", created) })
	if _, err := f.x.Bind(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	f.clear("rm")
	failed := len(f.calls("rm"))
	if _, err := f.x.Bind(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	if _, err := f.x.Start(t.Context(), "e"); err != nil {
		t.Fatal(err)
	}
	f.exited(0)
	if _, _, err := f.x.Inspect(t.Context(), "e"); err != nil {
		t.Fatal(err)
	}
	if rms := f.calls("rm")[failed:]; !slices.Contains(rms, "rm c-a v=1") {
		t.Fatalf("rms after the daemon recovered = %q, want the recorded job of another effect released", rms)
	}
}

func TestFailedCaptureIsRecordedAndReleased(t *testing.T) {
	for _, tt := range []struct {
		name, detail string
		fail         func(*fake)
	}{
		{"logs unavailable", "logs unavailable: docker logs: boom", func(f *fake) {
			f.fail("logs", http.StatusInternalServerError, "boom")
		}},
		{"daemon down", "logs unavailable: docker logs: ", func(f *fake) { f.down("logs") }},
		{"log cut short", "logs unavailable: docker logs: unexpected EOF", func(f *fake) {
			f.reply("logs", frame(1, "hello\n")[:12])
		}},
		{"log stream failed", "logs unavailable: docker logs: daemon failed", func(f *fake) {
			f.reply("logs", frame(1, "hello\n")+frame(3, "daemon failed"))
		}},
		{"artifact unwritable", "artifact write failed: ", func(f *fake) {
			f.output("hello\n")
			if err := os.WriteFile(filepath.Join(f.x.artifacts(), "e"), nil, 0o644); err != nil {
				f.t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.exited(0)
			tt.fail(f)
			want, found, err := f.x.Inspect(t.Context(), "e")
			if err != nil || !found || want.Status != harness.JobSucceeded || want.Output.Path != "e/output" ||
				want.Output.Available || want.Output.Complete || !strings.HasPrefix(want.Output.Detail, tt.detail) {
				t.Fatalf("Inspect(capture fails) = %+v, %v, %v", want, found, err)
			}
			data, err := os.ReadFile(f.x.record("e"))
			var rec record
			if err == nil {
				err = json.Unmarshal(data, &rec)
			}
			if err != nil || !same(rec.Job, want) || rec.Spec != specHash(f.x.runArgs("true")) {
				t.Fatalf("record = %+v, %v; want %+v", rec, err, want)
			}
			if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm c-e v=1" {
				t.Fatalf("rms = %q, want the container and its log released", rms)
			}
			captures := len(f.calls("logs"))
			f.output("late\n")
			if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || !same(job, want) {
				t.Fatalf("Inspect() again = %+v, %v; want the recorded job", job, err)
			}
			if logs := f.calls("logs"); len(logs) != captures {
				t.Fatalf("logs = %q, want the final report never captured again", logs)
			}
		})
	}
}

func TestInterruptedCaptureIsNotRecorded(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.hook("logs", func(r *http.Request) { f.signal("began"); f.wait(r) })
	if _, _, err := f.x.Inspect(f.cancelAt("began"), "e"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Inspect(shutdown during capture) = %v, want cancellation", err)
	}
	if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 {
		t.Fatalf("an interrupted capture was recorded: %v, rms %q", err, f.calls("rm"))
	}
	f.hook("logs", nil)
	f.output("hello\n")
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Preview != "hello\n" || !job.Output.Complete || len(f.calls("logs")) != 2 || len(f.calls("rm")) != 1 {
		t.Fatalf("Inspect() = %+v, %v, logs %q; want the next report to capture again", job, err, f.calls("logs"))
	}
}

func TestUnpublishedRecordIsNotFinal(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.output("hello\n")
	f.hook("logs", func(*http.Request) {
		if err := os.MkdirAll(filepath.Join(f.x.record("e"), "blocked"), 0o755); err != nil {
			f.t.Error(err)
		}
	})
	if job, _, err := f.x.Inspect(t.Context(), "e"); err == nil || len(f.calls("rm")) != 0 {
		t.Fatalf("Inspect(record unpublishable) = %+v, %v, rms %q; want an error and the container kept", job, err, f.calls("rm"))
	}
	if err := os.RemoveAll(f.x.record("e")); err != nil {
		t.Fatal(err)
	}
	f.hook("logs", nil)
	if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || job.Status != harness.JobSucceeded || !job.Output.Complete || len(f.calls("rm")) != 1 {
		t.Fatalf("Inspect() = %+v, %v; want the job recorded and released", job, err)
	}
}

func TestMalformedRecordIsAnError(t *testing.T) {
	valid := `{"ID":"c-e","Status":"succeeded","Spec":"SPEC"}`
	for name, data := range map[string]string{
		"valid":         valid,
		"old format":    `{"preview":"hello\n","output":{"Path":"e/output","Bytes":6,"Complete":true,"Available":true}}`,
		"unknown field": `{"ID":"c-e","Status":"succeeded","Spec":"SPEC","Retry":true}`,
		"no ID":         `{"Status":"succeeded","Spec":"SPEC"}`,
		"no status":     `{"ID":"c-e","Spec":"SPEC"}`,
		"no spec":       `{"ID":"c-e","Status":"succeeded"}`,
		"not final":     `{"ID":"c-e","Status":"running","Spec":"SPEC"}`,
		"trailing data": valid + ` {}`,
		"truncated":     valid[:20],
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.reply("inspect", f.container("running", 0))
			f.reply("ps", listing("c-e", "e"))
			data = strings.ReplaceAll(data, "SPEC", specHash(f.x.runArgs("true")))
			if err := os.WriteFile(f.x.record("e"), []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			_, found, inspectErr := f.x.Inspect(t.Context(), "e")
			_, bindErr := f.x.Bind(t.Context(), effect)
			_, startErr := f.x.Start(t.Context(), "e")
			cancelErr := f.x.Cancel(t.Context(), "e")
			if name == "valid" {
				if !found || errors.Join(inspectErr, bindErr, startErr, cancelErr) != nil {
					t.Fatalf("valid record: found %v, errors %v", found, errors.Join(inspectErr, bindErr, startErr, cancelErr))
				}
				return
			}
			if found || inspectErr == nil || bindErr == nil || startErr == nil || cancelErr != nil {
				t.Fatalf("found %v, errors %v, %v, %v, %v; want every report to fail and nothing to stop", found, inspectErr, bindErr, startErr, cancelErr)
			}
			f.x.sweep(t.Context())
			for _, op := range []string{"rm", "logs", "stop", "start", "create"} {
				if calls := f.calls(op); len(calls) != 0 {
					t.Fatalf("%s calls = %q", op, calls)
				}
			}
		})
	}
}

func TestCancelStopsOnlyARunningJob(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.reply("inspect", f.container("running", 0))
	stopped := f.container("exited", 143)
	f.hook("stop", func(*http.Request) { f.reply("inspect", stopped) })
	f.output("partial\n")
	for range 2 {
		if err := f.x.Cancel(t.Context(), "e"); err != nil {
			t.Fatal(err)
		}
	}
	if stops := f.calls("stop"); len(stops) != 1 || stops[0] != "stop c-e 5" {
		t.Fatalf("stops = %q", stops)
	}
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobCancelled || *job.ExitCode != 143 || job.Output.Complete || job.Preview != "partial\n" {
		t.Fatalf("Inspect() = %+v, %v", job, err)
	}
}
