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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
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
echo "$*${DOCKER_DEFAULT_PLATFORM:+ for $DOCKER_DEFAULT_PLATFORM}" >> %[1]q/pulls
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
	case op == "wait":
		io.WriteString(w, `{"StatusCode":0}`)
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
	case path == "containers/create" && q.Has("platform"):
		return "create", "create " + q.Get("name") + " for " + q.Get("platform")
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
	case "wait":
		call += " " + q.Get("condition")
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
	c.Config.Labels = map[string]string{"canti.effect": id, "canti.state": f.x.cfg.State, "canti.spec": f.x.Specification(command)}
	maps.Copy(c.Config.Labels, labels)
	c.State.Status, c.State.ExitCode = status, exit
	c.State.StartedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.State.FinishedAt = c.State.StartedAt.Add(time.Second)
	if status == "created" {
		c.State.StartedAt, c.State.FinishedAt = time.Time{}, time.Time{}
	}
	if status == "running" {
		c.State.FinishedAt = time.Time{}
	}
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
	e := f.effect()
	if err := os.MkdirAll(f.x.dir(e.ID), 0700); err != nil {
		f.t.Fatal(err)
	}
	if err := f.x.saveOwnership("binding.json", ownerOf(e, "c-e")); err != nil {
		f.t.Fatal(err)
	}
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

func (f *fake) effect() harness.Effect {
	return harness.Effect{ID: "e", Command: "true", Spec: f.x.Specification("true")}
}
func (f *fake) ready() {
	f.reply("inspect", f.container("created", 0))
	running := f.container("running", 0)
	f.hook("start", func(*http.Request) { f.reply("inspect", running) })
}
func freshExecutor(f *fake) *Executor { x := *f.x; return &x }

func TestInspectReportsAbsenceOnlyWhenDockerSaysSo(t *testing.T) {
	f := newFake(t)
	f.missing()
	e := f.effect()
	j, err := f.x.Inspect(t.Context(), e)
	if err != nil || j.Status != harness.JobAbsent {
		t.Fatalf("absence: %+v %v", j, err)
	}
	for _, bad := range []string{"", "{}", "[]", "not json", f.container("created", 0) + "{}", strings.ReplaceAll(f.container("created", 0), `"canti.effect":"e"`, `"canti.effect":"foreign"`)} {
		f.reply("inspect", bad)
		if _, err := f.x.Inspect(t.Context(), e); err == nil {
			t.Fatalf("accepted invalid lookup %q", bad)
		}
	}
	f.fail("inspect", 500, "No such container: canti-e")
	if _, err := f.x.Inspect(t.Context(), e); err == nil {
		t.Fatal("daemon error treated as absence")
	}
	if len(f.calls("create"))+len(f.calls("start")) != 0 {
		t.Fatal("Inspect created or started")
	}
}
func TestDriveCreatesNothingOnceContextIsDone(t *testing.T) {
	f := newFake(t)
	f.missing()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.x.Drive(ctx, f.effect()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(f.calls("inspect"))+len(f.calls("create"))+len(f.calls("start")) != 0 {
		t.Fatal("cancelled call reached backend")
	}
}
func TestDrivePublishesBindingBeforeLaunch(t *testing.T) {
	f := newFake(t)
	f.missing()
	e := f.effect()
	created := f.container("created", 0)
	running := f.container("running", 0)
	f.hook("create", func(*http.Request) { f.reply("inspect", created) })
	f.hook("start", func(*http.Request) {
		saved, found, err := f.x.ownership(e, "binding.json")
		if err != nil || !found || saved.ID != "c-e" {
			t.Errorf("binding at launch: %+v %v %v", saved, found, err)
		}
		f.reply("inspect", running)
	})
	j, err := f.x.Drive(t.Context(), e)
	if err != nil || j.Status != harness.JobRunning {
		t.Fatalf("Drive: %+v %v", j, err)
	}
	if len(f.calls("create")) != 1 || len(f.calls("start")) != 1 {
		t.Fatal(f.log)
	}
}
func TestDriveAdoptsOnlyMatchingOwnerAfterLostCreateAnswer(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint(foreign), func(t *testing.T) {
			f := newFake(t)
			f.missing()
			created := f.container("created", 0)
			if foreign {
				created = f.containerOf("e", "false", "created", 0)
			}
			f.down("create")
			f.hook("create", func(*http.Request) { f.reply("inspect", created) })
			f.hook("start", func(*http.Request) { f.reply("inspect", f.container("running", 0)) })
			j, err := f.x.Drive(t.Context(), f.effect())
			if foreign {
				if err == nil || len(f.calls("start")) != 0 {
					t.Fatalf("foreign owner: %+v %v", j, err)
				}
			} else if err != nil || j.Status != harness.JobRunning || len(f.calls("start")) != 1 {
				t.Fatalf("lost create: %+v %v", j, err)
			}
		})
	}
}
func TestRecordedPendingOwnerIsNeverReplaced(t *testing.T) {
	f := newFake(t)
	e := f.effect()
	if err := os.MkdirAll(f.x.dir(e.ID), 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.x.saveOwnership("binding.json", ownerOf(e, "original")); err != nil {
		t.Fatal(err)
	}
	for _, missing := range []bool{true, false} {
		if missing {
			f.missing()
		} else {
			f.reply("inspect", f.container("created", 0))
		}
		for _, op := range []func(context.Context, harness.Effect) (harness.Job, error){freshExecutor(f).Drive, freshExecutor(f).Inspect, freshExecutor(f).Cancel} {
			if _, err := op(t.Context(), e); !errors.Is(err, harness.ErrJobLost) {
				t.Fatalf("lost pending owner: %v", err)
			}
		}
	}
	if len(f.calls("create"))+len(f.calls("start"))+len(f.calls("stop")) != 0 {
		t.Fatal("replaced or stopped unowned container")
	}
}

type bindingCancellation struct {
	context.Context
	cancel context.CancelFunc
	path   string
}

func (c bindingCancellation) Err() error {
	if _, err := os.Stat(c.path); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}
func TestDriveRechecksDeadlineAfterBindingBeforeLatch(t *testing.T) {
	f := newFake(t)
	f.ready()
	e := f.effect()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bound := bindingCancellation{Context: ctx, cancel: cancel, path: filepath.Join(f.x.dir(e.ID), "binding.json")}
	if _, err := freshExecutor(f).Drive(bound, e); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired binding authorization: %v", err)
	}
	if _, found, err := f.x.ownership(e, "binding.json"); err != nil || !found {
		t.Fatalf("binding not durable: %v", err)
	}
	if latch, err := readLatch(f.x.dir(e.ID)); err != nil || latch != "" || len(f.calls("start")) != 0 {
		t.Fatalf("latch %q %v, starts %v", latch, err, f.calls("start"))
	}
}

func TestUncertainLaunchIsNeverIssuedAgain(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			f := newFake(t)
			f.reply("inspect", f.container("created", 0))
			if lost {
				f.down("start")
			} else {
				f.fail("start", 500, "boom")
			}
			e := f.effect()
			for range 3 {
				j, err := freshExecutor(f).Drive(t.Context(), e)
				if err != nil || j.Status != harness.JobUnknown {
					t.Fatalf("uncertain launch: %+v %v", j, err)
				}
			}
			if len(f.calls("start")) != 1 {
				t.Fatal(f.calls("start"))
			}
		})
	}
}
func TestUnlatchedLiveOwnerStaysUnknown(t *testing.T) {
	for _, state := range []string{"running", "exited"} {
		t.Run(state, func(t *testing.T) {
			f := newFake(t)
			f.reply("inspect", f.container(state, 0))
			j, err := f.x.Drive(t.Context(), f.effect())
			if err != nil || j.Status != harness.JobUnknown || len(f.calls("start")) != 0 {
				t.Fatalf("unlatched owner: %+v %v", j, err)
			}
		})
	}
}
func TestCancellationBeforeBindingFencesLateDriveWithoutAnOwner(t *testing.T) {
	f := newFake(t)
	f.missing()
	e := f.effect()
	want, err := f.x.Cancel(t.Context(), e)
	if err != nil || want.Status != harness.JobCancelled || want.ID != "" || !want.Fenced {
		t.Fatalf("fence: %+v %v", want, err)
	}
	for _, op := range []func(context.Context, harness.Effect) (harness.Job, error){freshExecutor(f).Drive, freshExecutor(f).Inspect, freshExecutor(f).Cancel} {
		j, err := op(t.Context(), e)
		if err != nil || !same(j, want) {
			t.Fatalf("fenced replay: %+v %v", j, err)
		}
	}
	if len(f.calls("create"))+len(f.calls("start"))+len(f.calls("stop")) != 0 {
		t.Fatal("late Drive reached execution")
	}
}
func TestCancellationFenceDoesNotTurnAmbiguousLookupIntoKnownAbsence(t *testing.T) {
	f := newFake(t)
	f.down("inspect")
	e := f.effect()
	if _, err := f.x.Cancel(t.Context(), e); err == nil {
		t.Fatal("ambiguous lookup became cancelled")
	}
	if _, found, err := f.x.ownership(e, "cancel"); err != nil || !found {
		t.Fatalf("fence not retained: %v", err)
	}
	if _, err := os.Stat(f.x.record(e.ID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("false terminal receipt: %v", err)
	}
	f.ready()
	j, err := freshExecutor(f).Drive(t.Context(), e)
	if err != nil || j.Status != harness.JobCancelled || len(f.calls("start")) != 0 {
		t.Fatalf("late fenced owner: %+v %v", j, err)
	}
}
func TestWrongSpecificationCannotDriveInspectOrCancel(t *testing.T) {
	f := newFake(t)
	f.ready()
	e := f.effect()
	e.Spec = strings.Repeat("0", 64)
	for _, op := range []func(context.Context, harness.Effect) (harness.Job, error){f.x.Drive, f.x.Inspect, f.x.Cancel} {
		if _, err := op(t.Context(), e); err == nil {
			t.Fatal("wrong spec accepted")
		}
	}
	if len(f.log) != 0 {
		t.Fatalf("wrong spec reached backend: %v", f.log)
	}
}
func TestFinishedReceiptSurvivesReleaseAndLaterCancel(t *testing.T) {
	for _, code := range []int{0, 3} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.exited(code)
			f.output("hello\n")
			e := f.effect()
			want, err := f.x.Inspect(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			f.missing()
			for _, op := range []func(context.Context, harness.Effect) (harness.Job, error){freshExecutor(f).Cancel, freshExecutor(f).Drive, freshExecutor(f).Inspect} {
				j, err := op(t.Context(), e)
				if err != nil || !same(j, want) {
					t.Fatalf("retained result changed: %+v %v, want %+v", j, err, want)
				}
			}
			if len(f.calls("start"))+len(f.calls("stop")) != 0 {
				t.Fatal("terminal replay reached execution")
			}
		})
	}
}
func TestFirstCancelPreservesAnAlreadyExitedResult(t *testing.T) {
	for _, code := range []int{0, 3} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			f := newFake(t)
			f.latch(latchStart)
			f.exited(code)
			e := f.effect()
			j, err := f.x.Cancel(t.Context(), e)
			if err != nil || j.ExitCode == nil || *j.ExitCode != code || j.Status == harness.JobCancelled || !j.Fenced {
				t.Fatalf("natural exit: %+v %v", j, err)
			}
			if len(f.calls("stop")) != 0 {
				t.Fatal("stopped exited owner")
			}
		})
	}
}
func TestFailedCaptureRetainsOwnerUntilPublicationCanRetry(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.fail("logs", 500, "capture failed")
	e := f.effect()
	if _, err := f.x.Inspect(t.Context(), e); err == nil {
		t.Fatal("capture failure became terminal")
	}
	if len(f.calls("rm")) != 0 {
		t.Fatal("released uncaptured owner")
	}
	if _, err := os.Stat(f.x.record(e.ID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("published failed capture")
	}
	f.output("recovered")
	j, err := freshExecutor(f).Inspect(t.Context(), e)
	if err != nil || j.Preview != "recovered" || len(f.calls("rm")) != 1 {
		t.Fatalf("capture retry: %+v %v", j, err)
	}
}
func TestMalformedBindingAndTerminalRecordsFailClosed(t *testing.T) {
	for _, name := range []string{"binding.json", "cancel", "output.json"} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.ready()
			e := f.effect()
			if err := os.MkdirAll(f.x.dir(e.ID), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.x.dir(e.ID), name), []byte(`{"Format":1}`), 0600); err != nil {
				t.Fatal(err)
			}
			for _, op := range []func(context.Context, harness.Effect) (harness.Job, error){f.x.Drive, f.x.Inspect, f.x.Cancel} {
				if _, err := op(t.Context(), e); err == nil {
					t.Fatalf("accepted malformed %s", name)
				}
			}
			if len(f.calls("create"))+len(f.calls("start"))+len(f.calls("stop")) != 0 {
				t.Fatal("corrupt evidence reached execution")
			}
		})
	}
}

func same(a, b harness.Job) bool { return reflect.DeepEqual(a, b) }

func TestRetainedReceiptRejectsMissingLaunchWitness(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	e := f.effect()
	if _, err := f.x.Inspect(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	before := len(f.calls("rm"))
	if err := os.Remove(filepath.Join(f.x.dir(e.ID), "latch")); err != nil {
		t.Fatal(err)
	}
	if _, err := freshExecutor(f).Inspect(t.Context(), e); err == nil {
		t.Fatal("known result accepted without launch witness")
	}
	if len(f.calls("rm")) != before {
		t.Fatal("invalid receipt authorized release")
	}
}
func TestRetainedReceiptStaysImmutableAfterArtifactLoss(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.output("retained")
	e := f.effect()
	want, err := f.x.Inspect(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.x.cfg.State, "artifacts", want.Output.Path)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	f.missing()
	j, err := freshExecutor(f).Inspect(t.Context(), e)
	if err != nil || !same(j, want) {
		t.Fatalf("receipt changed after artifact deletion: %+v %v", j, err)
	}
	read, err := (archive.Reader{Artifacts: filepath.Join(f.x.cfg.State, "artifacts")}).Retrieve(t.Context(), archive.RetrievalQuery{Operation: "read", Path: "/canti/artifacts/" + want.Output.Path, Reason: "verify retained bytes"})
	if err != nil || read.Error == "" {
		t.Fatalf("missing artifact retrieval: %+v %v", read, err)
	}
}

func TestBindingPublicationFailureCannotConsumeLaunchLatch(t *testing.T) {
	f := newFake(t)
	f.missing()
	e := f.effect()
	created := f.container("created", 0)
	blocker := filepath.Join(f.x.dir(e.ID), "binding.json")
	f.hook("create", func(*http.Request) {
		if err := os.MkdirAll(blocker, 0700); err != nil {
			t.Error(err)
		}
		f.reply("inspect", created)
	})
	if _, err := f.x.Drive(t.Context(), e); err == nil {
		t.Fatal("unpublished binding launched")
	}
	if latch, err := readLatch(f.x.dir(e.ID)); err != nil || latch != "" || len(f.calls("start")) != 0 {
		t.Fatalf("unpublished launch: %q %v %v", latch, err, f.calls("start"))
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	f.hook("start", func(*http.Request) { f.reply("inspect", f.container("running", 0)) })
	j, err := freshExecutor(f).Drive(t.Context(), e)
	if err != nil || j.ID != "c-e" || len(f.calls("create")) != 1 || len(f.calls("start")) != 1 {
		t.Fatalf("publication recovery: %+v %v", j, err)
	}
}

func TestRetainedStartedCancellationRequiresItsStopWitness(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.reply("inspect", f.container("running", 0))
	e := f.effect()
	f.hook("stop", func(*http.Request) { f.reply("inspect", f.container("exited", 143)) })
	if j, err := f.x.Cancel(t.Context(), e); err != nil || j.Status != harness.JobCancelled {
		t.Fatalf("cancel: %+v %v", j, err)
	}
	before := len(f.calls("rm"))
	if err := os.Remove(filepath.Join(f.x.dir(e.ID), "stop")); err != nil {
		t.Fatal(err)
	}
	if _, err := freshExecutor(f).Inspect(t.Context(), e); err == nil {
		t.Fatal("accepted started cancellation without stop witness")
	}
	if len(f.calls("rm")) != before {
		t.Fatal("invalid cancellation authorized release")
	}
}

func TestEffectLockWaitHasAnIndependentBound(t *testing.T) {
	f := newFake(t)
	f.x.timeouts.call = 30 * time.Millisecond
	unlock, err := f.x.lock(t.Context(), "e")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	began := time.Now()
	_, err = f.x.Inspect(t.Context(), f.effect())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(began) > time.Second {
		t.Fatalf("lock wait: %v after %v", err, time.Since(began))
	}
	if len(f.log) != 0 {
		t.Fatal("blocked operation reached backend")
	}
}

func TestTerminalPublicationFailureRetainsTheOwnerForRetry(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.output("retained output")
	e := f.effect()
	blocker := f.x.record(e.ID)
	f.hook("logs", func(*http.Request) {
		if err := os.Mkdir(blocker, 0700); err != nil {
			t.Error(err)
		}
	})
	if _, err := f.x.Inspect(t.Context(), e); err == nil {
		t.Fatal("failed publication became a terminal receipt")
	}
	if len(f.calls("rm")) != 0 {
		t.Fatal("owner released before terminal publication")
	}
	f.hook("logs", nil)
	f.output("retained output")
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	j, err := freshExecutor(f).Inspect(t.Context(), e)
	if err != nil || j.ID != "c-e" || j.Status != harness.JobSucceeded || j.Preview != "retained output" || len(f.calls("rm")) != 1 {
		t.Fatalf("publication retry: %+v %v", j, err)
	}
}

func TestDriveOwnsBoundedLaunchAfterCallerCancellation(t *testing.T) {
	f := newFake(t)
	f.ready()
	f.x.timeouts.call = 200 * time.Millisecond
	e := f.effect()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	f.hook("start", func(r *http.Request) {
		close(started)
		cancel()
		select {
		case <-release:
		case <-f.stop:
			return
		}
		if r.Context().Err() != nil {
			t.Error("post-latch launch inherited caller cancellation")
		}
		f.reply("inspect", f.container("running", 0))
	})
	type result struct {
		job harness.Job
		err error
	}
	done := make(chan result, 1)
	go func() { j, err := f.x.Drive(ctx, e); done <- result{j, err} }()
	<-started
	if latch, err := readLatch(f.x.dir(e.ID)); err != nil || latch != latchStart {
		t.Fatalf("start lacked latch: %q %v", latch, err)
	}
	select {
	case r := <-done:
		t.Fatalf("Drive detached its launch: %+v", r)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case r := <-done:
		if r.err != nil || r.job.Status != harness.JobRunning || !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("bounded completion: %+v %v", r, ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("launch did not finish within its own bounds")
	}
	if _, err := freshExecutor(f).Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if len(f.calls("start")) != 1 {
		t.Fatal("launch was repeated")
	}
}

func TestAmbientPlatformCannotChangeAcceptedExecution(t *testing.T) {
	f := newFake(t)
	f.missing()
	e := f.effect()
	t.Setenv("DOCKER_DEFAULT_PLATFORM", "invalid/ambient-platform")
	if got := f.x.Specification(e.Command); got != e.Spec {
		t.Fatal("ambient platform changed pure specification")
	}
	f.fail("create", http.StatusNotFound, "No such image: alpine:3.23")
	created := f.container("created", 0)
	f.hook("create", func(*http.Request) {
		if len(f.pulls()) > 0 {
			f.clear("create")
			f.reply("inspect", created)
		}
	})
	f.hook("start", func(*http.Request) { f.reply("inspect", f.container("running", 0)) })
	j, err := f.x.Drive(t.Context(), e)
	if err != nil || j.Status != harness.JobRunning {
		t.Fatalf("ambient-independent Drive: %+v %v", j, err)
	}
	creates, pulls := f.calls("create"), f.pulls()
	if len(creates) != 2 || creates[0] != "create canti-e" || creates[1] != "create canti-e" || len(pulls) != 1 || strings.Contains(pulls[0], "ambient-platform") {
		t.Fatalf("ambient platform reached create/pull: %v %v", creates, pulls)
	}
}
