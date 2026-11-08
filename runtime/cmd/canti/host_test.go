package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

var listening = regexp.MustCompile(`canti host: serving workspace .* as process (\d+)`)

// hostPIDs returns the PIDs of the hosts that listened for the fixture's
// state directory, in order, from its host log.
func (f *fixture) hostPIDs() []int {
	f.t.Helper()
	data, _ := os.ReadFile(filepath.Join(f.state, "host.log"))
	var pids []int
	for _, m := range listening.FindAllStringSubmatch(string(data), -1) {
		pid, _ := strconv.Atoi(m[1])
		pids = append(pids, pid)
	}
	return pids
}

// hostProcesses finds this test binary's host processes for the fixture,
// including startup contenders that have not logged a listening socket yet.
// Every client has finished spawning them before fixture cleanup begins.
func (f *fixture) hostProcesses() []int {
	f.t.Helper()
	exe, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		f.t.Fatal(err)
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		process := filepath.Join("/proc", entry.Name())
		if target, err := os.Readlink(filepath.Join(process, "exe")); err != nil || target != exe {
			continue
		}
		data, err := os.ReadFile(filepath.Join(process, "cmdline"))
		args := strings.Split(string(data), "\x00")
		if err == nil && len(args) > 1 && args[1] == "host" && slicesContain(args, f.state) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// autoHost makes the fixture's state directory served by host processes that
// its commands start, and ends every one of them when the test ends.
func (f *fixture) autoHost() {
	f.t.Cleanup(func() {
		pids := f.hostProcesses()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGTERM)
		}
		for _, pid := range pids {
			for deadline := time.Now().Add(20 * time.Second); syscall.Kill(pid, 0) == nil; time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					syscall.Kill(pid, syscall.SIGKILL)
					f.t.Errorf("host %d did not exit after SIGTERM", pid)
					break
				}
			}
		}
	})
}

func (f *fixture) socket() string { return filepath.Join(f.state, hostrpc.SocketName) }

func TestAnAutoHostDrivesTheRunAndKeepsServing(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "canti:retrieve {\"operation\":\"list\",\"path\":\"/canti/runs\",\"reason\":\"r\"}"}, 2: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	r := f.cli(nil, "run", "-host", "auto", "-goal", "g")
	if r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "submitting input in_") {
		t.Fatalf("run: %+v", r)
	}
	if pids := f.hostPIDs(); len(pids) != 1 || syscall.Kill(pids[0], 0) != nil {
		t.Fatalf("the host that drove the run is gone: %v", pids)
	}
	if s := f.status(); s.Phase != core.PhaseReturned || !strings.HasPrefix(s.Run.InputID, "in_") {
		t.Fatalf("status = %+v", s)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "g2", "-input", "next"); r.code != exitOK || len(f.hostPIDs()) != 1 {
		t.Fatalf("a second run did not reach the same host: %+v, hosts %v", r, f.hostPIDs())
	}
	if r := f.cli(nil, "host", "-stop"); r.code != exitOK {
		t.Fatalf("host -stop: %+v", r)
	}
	waitFor(t, func() bool { _, err := os.Stat(f.socket()); return os.IsNotExist(err) })
}

func TestADetachedRunKeepsBeingDrivenByTheHost(t *testing.T) {
	f := newFixture(t, nil)
	gate := make(chan struct{})
	f.model.decideWith(func(step int, _ string) (core.Decision, error) {
		if step == 1 {
			<-gate
		}
		return core.Decision{Kind: core.Return, Payload: "done"}, nil
	})
	f.autoHost()
	cmd, wait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "detached")
	waitFor(t, func() bool { return len(f.model.requests()) == 1 })
	cmd.Process.Kill()
	wait()
	retry := make(chan result, 1)
	go func() { retry <- f.cli(nil, "run", "-host", "auto", "-goal", "g", "-input", "detached") }()
	time.Sleep(100 * time.Millisecond)
	close(gate)
	if r := <-retry; r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("a retry of the input the host drives did not follow it to its end: %+v", r)
	}
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume of the returned run: %+v", r)
	}
	for _, e := range f.history() {
		if e.Kind == "stop_requested" {
			t.Fatalf("a client that went away stopped its run: %v", kinds(f.history()))
		}
	}
	if len(f.model.requests()) != 1 {
		t.Fatalf("the model was called %d times", len(f.model.requests()))
	}
}

func TestALostSubmissionAnswerNamesTheAdmittedRun(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	marker := filepath.Join(t.TempDir(), "admitted")
	cmd, wait := f.start([]string{"CANTI_TEST_SETUP=point:submit.committed", "CANTI_TEST_BLOCKED=" + marker}, "run", "-host", "auto", "-goal", "g", "-input", "kept")
	waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
	first := f.hostPIDs()
	if len(first) != 1 {
		t.Fatalf("hosts %v", first)
	}
	syscall.Kill(first[0], syscall.SIGKILL)
	r := wait()
	_ = cmd
	if r.code != exitOK || r.stdout != "done\n" || len(f.model.requests()) != 1 {
		t.Fatalf("the client whose host died after admitting its submission: %+v, %d model requests", r, len(f.model.requests()))
	}
	if pids := f.hostPIDs(); len(pids) != 2 {
		t.Fatalf("the client did not start a new host on the stale socket: %v", pids)
	}
	s := f.status()
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "g", "-input", "kept"); r.code != exitOK || r.stdout != "done\n" || f.status().Run.ID != s.Run.ID || len(f.model.requests()) != 1 {
		t.Fatalf("a deliberate retry: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "other", "-input", "kept"); r.code != exitError || !strings.Contains(r.stderr, "different content") {
		t.Fatalf("a conflicting retry: %+v", r)
	}
}

func TestConcurrentClientsStartOneHost(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	var wg sync.WaitGroup
	results := make([]result, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = f.cli(nil, "run", "-host", "auto", "-goal", "g", "-input", "i"+strconv.Itoa(i))
		}()
	}
	wg.Wait()
	admitted := 0
	for _, r := range results {
		switch {
		case r.code == exitOK && r.stdout == "done\n":
			admitted++
		case r.code == exitError && strings.Contains(r.stderr, "it first"):
		default:
			t.Errorf("client: %+v", r)
		}
	}
	if pids := f.hostPIDs(); admitted == 0 || len(pids) != 1 {
		t.Fatalf("%d runs admitted by hosts %v", admitted, pids)
	}
	if len(f.model.requests()) != admitted {
		t.Fatalf("%d admitted runs made %d model calls", admitted, len(f.model.requests()))
	}
}

func TestAClientWaitsForAHostThatIsStartingAndRefusesAnEmbeddedOwner(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	hold := f.model.holdRequests()
	embedded, wait := f.start(nil, "run", "-goal", "g")
	<-hold
	if r := f.cli(nil, "run", "-host", "auto", "-host-wait", "300ms", "-goal", "g2"); r.code != exitLocked || !strings.Contains(r.stderr, "no host serves it") {
		t.Fatalf("auto beside an embedded owner: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.state, "host.log")); !os.IsNotExist(err) {
		t.Fatalf("a refused auto request started a host contender: %v", err)
	}
	embedded.Process.Signal(syscall.SIGTERM)
	if r := wait(); r.code != exitStopped {
		t.Fatalf("embedded run: %+v", r)
	}
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	if r := f.cli(nil, "run", "-goal", "open", "-once"); r.code != exitActive {
		t.Fatalf("open run: %+v", r)
	}

	f.autoHost()
	marker := filepath.Join(t.TempDir(), "locked")
	h, hwait := f.start([]string{"CANTI_TEST_SETUP=point:host.locked", "CANTI_TEST_BLOCKED=" + marker}, "host")
	waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
	stopped := make(chan result, 1)
	go func() { stopped <- f.cli(nil, "stop", "-host", "auto", "-host-wait", "30s") }()
	time.Sleep(300 * time.Millisecond)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if r := <-stopped; r.code != exitStopped {
		t.Fatalf("a client during a host's start: %+v", r)
	}
	if hello, err := (&hostrpc.Client{Path: f.socket()}).Hello(context.Background()); err != nil || hello.PID != h.Process.Pid {
		t.Fatalf("the state directory is served by %+v, %v, not the starting host %d", hello, err, h.Process.Pid)
	}
	h.Process.Signal(syscall.SIGTERM)
	if r := hwait(); r.code != exitOK {
		t.Fatalf("host: %+v", r)
	}
}

func TestHostShutdownCommitsNothingAndReleasesTheState(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	hold := f.model.holdRequests()
	h, wait := f.start(nil, "host")
	waitFor(t, func() bool {
		_, err := (&hostrpc.Client{Path: f.socket()}).Hello(context.Background())
		return err == nil
	})
	client, cwait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "held")
	<-hold
	before := kinds(f.history())
	if err := h.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if r := wait(); r.code != exitOK {
		t.Fatalf("host: %+v", r)
	}
	client.Process.Kill()
	cwait()
	if after := kinds(f.history()); strings.Join(after, " ") != strings.Join(before, " ") || slicesContain(after, "stop_requested") {
		t.Fatalf("shutdown changed the journal from %q to %q", before, after)
	}
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("an embedded resume after the host shut down: %+v", r)
	}
}

func slicesContain(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestAHostWithoutCredentialsStopsARun(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	hold := f.model.holdRequests()
	client, wait := f.start(nil, "run", "-host", "auto", "-goal", "g", "-input", "stopped")
	<-hold
	pids := f.hostPIDs()
	syscall.Kill(pids[0], syscall.SIGKILL)
	client.Process.Kill()
	wait()
	r := runCLI(t, nil, "stop", "-host", "auto", "-workspace", f.workspace, "-state", f.state)
	if r.code != exitStopped || len(f.hostPIDs()) != 2 {
		t.Fatalf("stop on a host without credentials: %+v, hosts %v", r, f.hostPIDs())
	}
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
		t.Fatalf("status = %+v", s)
	}
}

func TestATooLongSocketPathIsRefused(t *testing.T) {
	f := newFixture(t, nil)
	f.state = filepath.Join(t.TempDir(), strings.Repeat("s", 120))
	r := f.cli(nil, "run", "-host", "auto", "-goal", "g")
	if r.code != exitError || !strings.Contains(r.stderr, "a Unix socket allows") {
		t.Fatalf("run: %+v", r)
	}
	if r := f.cli(nil, "host"); r.code != exitError || !strings.Contains(r.stderr, "a Unix socket allows") {
		t.Fatalf("host: %+v", r)
	}
}

func TestAHostOfAnotherProtocolIsRefused(t *testing.T) {
	f := newFixture(t, nil)
	if err := os.MkdirAll(f.state, 0o700); err != nil {
		t.Fatal(err)
	}
	stop := fakeHost(t, f.socket(), `{"protocol":2,"error":{"code":"protocol","message":"the client speaks host protocol 1 and this host, pid 1, speaks 2"}}`)
	defer stop()
	r := f.cli(nil, "run", "-host", "auto", "-goal", "g")
	if r.code != exitError || !strings.Contains(r.stderr, fmt.Sprintf("speaks host protocol 2 and this canti speaks %d", hostrpc.Protocol)) {
		t.Fatalf("run against a host of another protocol: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.state, "canti.db")); !os.IsNotExist(err) {
		t.Fatalf("a refused client created a journal: %v", err)
	}
}

// fakeHost serves answer to every request on the socket at path until stop.
func fakeHost(t *testing.T, path, answer string) (stop func()) {
	t.Helper()
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
			io.WriteString(conn, answer+"\n")
			conn.Close()
		}
	}()
	return func() { ln.Close() }
}

// TestAForegroundHostBindsItsCredentialAndContinuesTheOpenRun requires that
// a foreground canti host binds the key of its own environment to the
// provider its base URL variable names, reports the pair without the key,
// and continues an open run of that pair when it starts, with no client.
func TestAForegroundHostBindsItsCredentialAndContinuesTheOpenRun(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "g", "-once"); r.code != exitActive {
		t.Fatalf("run -once: %+v", r)
	}
	cmd, wait := f.start([]string{"ANTHROPIC_BASE_URL=" + f.model.URL}, "host")
	stopped := false
	stop := func() result {
		stopped = true
		cmd.Process.Signal(syscall.SIGTERM)
		return wait()
	}
	t.Cleanup(func() {
		if !stopped {
			stop()
		}
	})
	waitFor(t, func() bool { s := f.status(); return s.Phase == core.PhaseReturned })
	keyless := []string{"-workspace", f.workspace, "-state", f.state, "-host", "auto"}
	if r := runCLI(t, nil, append(append([]string{"run"}, keyless...), append(fixtureModel, "-model-url", f.model.URL, "-goal", "second")...)...); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("a keyless client of the foreground host's pair: %+v", r)
	}
	other := newScriptedModel(t, f.key, map[int]core.Decision{1: {Kind: core.Return, Payload: "elsewhere"}})
	if r := runCLI(t, nil, append(append([]string{"run"}, keyless...), append(fixtureModel, "-model-url", other.URL, "-goal", "third")...)...); r.code != exitError ||
		!strings.Contains(r.stderr, "ANTHROPIC_API_KEY") || len(other.requests()) != 0 {
		t.Fatalf("a keyless client of another pair: %+v", r)
	}
	r := stop()
	if r.code != exitOK || !strings.Contains(r.stderr, "canti host: binds its anthropic-messages credential to "+f.model.URL+"\n") || len(f.model.requests()) != 2 {
		t.Fatalf("foreground host: %+v, %d requests", r, len(f.model.requests()))
	}
}

// TestAClientArrivingWhileTheHostExitsStartsTheNextOne requires that a client
// that arrives after a host removed its socket but while it still holds the
// state directory, as it does while it shuts down or exits when idle, waits
// for it to exit and then starts the next host, instead of failing once the
// host it started lost the lock.
func TestAClientArrivingWhileTheHostExitsStartsTheNextOne(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	marker := filepath.Join(t.TempDir(), "closing")
	if r := f.cli([]string{"CANTI_TEST_SETUP=point:shutdown.linearized", "CANTI_TEST_BLOCKED=" + marker}, "run", "-host", "auto", "-goal", "first"); r.code != exitOK {
		t.Fatalf("first run: %+v", r)
	}
	if r := f.cli(nil, "host", "-stop"); r.code != exitOK {
		t.Fatalf("host -stop: %+v", r)
	}
	waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
	if _, err := os.Stat(f.socket()); !os.IsNotExist(err) {
		t.Fatalf("the closing host still has its socket: %v", err)
	}
	waiting := filepath.Join(t.TempDir(), "waiting")
	_, wait := f.start([]string{"CANTI_TEST_SETUP=point:host.waiting", "CANTI_TEST_BLOCKED=" + waiting}, "run", "-host", "auto", "-goal", "second")
	waitFor(t, func() bool { _, err := os.Stat(waiting); return err == nil })
	os.Remove(marker)
	os.Remove(waiting)
	if r := wait(); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("the client that arrived while the host exited: %+v\nhost log:\n%s", r, readFile(t, filepath.Join(f.state, "host.log")))
	}
	if pids := f.hostPIDs(); len(pids) != 2 {
		t.Fatalf("serving hosts %v, want the first and the next", pids)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, _ := os.ReadFile(path)
	return string(data)
}

// TestTheDefaultHostIsTheHostProcess requires that run, resume, stop, and
// resolve without -host reach the host process of the state directory,
// starting it when none serves it, and that it keeps the run's progress
// across clients, while -host embedded drives in the command's own process.
func TestTheDefaultHostIsTheHostProcess(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	if r := f.cli(nil, "run", unpinned, "-goal", "g"); r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "submitting input in_") {
		t.Fatalf("run without -host: %+v", r)
	}
	if pids := f.hostPIDs(); len(pids) != 1 {
		t.Fatalf("run without -host started hosts %v, want one", pids)
	}
	for _, args := range [][]string{{"resume", unpinned}, {"stop", unpinned}} {
		if r := f.cli(nil, args[0], args[1:]...); r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("%s without -host: %+v", args[0], r)
		}
	}
	if r := f.cli(nil, "resolve", unpinned, "-execution", "exited", "-exit-code", "0", "-note", "verified", "-owner", "asserted-owner"); r.code != exitError || !strings.Contains(r.stderr, "no unknown outcome") {
		t.Fatalf("resolve without -host: %+v", r)
	}
	if len(f.hostPIDs()) != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("%d hosts and %d model requests, want one each", len(f.hostPIDs()), len(f.model.requests()))
	}
	if r := f.cli(nil, "run", "-host", "embedded", "-goal", "beside"); r.code != exitLocked || !strings.Contains(r.stderr, "locked by another canti process") {
		t.Fatalf("an embedded run beside the host process: %+v", r)
	}
}

// TestARefusedModelURLNeverShowsWhatMayHoldASecret requires that a model URL
// refused for its query, user information, or fragment, named by -model-url
// or by the base URL variable, embedded, through the host process, or at a
// foreground host's start, shows none of those parts on stderr.
func TestARefusedModelURLNeverShowsWhatMayHoldASecret(t *testing.T) {
	const marker = "DUMMYMARKER"
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	for _, tc := range []struct {
		name string
		env  []string
		args []string
	}{
		{"embedded -model-url", nil, []string{"run", "-model-url", "https://u" + marker + ":p@gw.test/v1", "-goal", "g"}},
		{"embedded variable", []string{"ANTHROPIC_BASE_URL=https://gw.test/v1?key=" + marker}, []string{"run", "-model-url", "", "-goal", "g"}},
		{"host process", []string{"ANTHROPIC_BASE_URL=https://gw.test/v1#" + marker}, []string{"run", "-host", "auto", "-model-url", "", "-goal", "g"}},
		{"foreground host", []string{"ANTHROPIC_BASE_URL=https://gw.test/v1?key=" + marker}, []string{"host"}},
	} {
		r := f.cli(tc.env, tc.args[0], tc.args[1:]...)
		if r.code == exitOK || strings.Contains(r.stdout+r.stderr, marker) || !strings.Contains(r.stderr, "https://gw.test/v1") {
			t.Errorf("%s: %+v", tc.name, r)
		}
	}
}

// TestHistoryImportsAreResolvedByTheClientThatNamesThem requires that a
// relative -history-imports names the directory of the client that names it,
// never of the host process another client started from elsewhere, and that
// retrying the same input from there names no difference.
func TestHistoryImportsAreResolvedByTheClientThatNamesThem(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	first, second := t.TempDir(), t.TempDir()
	for _, dir := range []string{first, second} {
		if err := os.Mkdir(filepath.Join(dir, "imports"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	args := func(goal, input string) []string {
		return append(append([]string{"run", "-workspace", f.workspace, "-state", f.state, "-host", "auto", "-model-url", f.model.URL}, fixtureModel...),
			"-history-imports", "imports", "-goal", goal, "-input", input)
	}
	env := []string{"ANTHROPIC_API_KEY=" + f.key}
	for _, tc := range []struct{ dir, goal, input string }{{first, "first", "in-1"}, {second, "second", "in-2"}} {
		_, wait := startCLIIn(t, tc.dir, nil, nil, env, args(tc.goal, tc.input)...)
		if r := wait(); r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("run from %s: %+v", tc.dir, r)
		}
		want, _ := filepath.EvalSymlinks(filepath.Join(tc.dir, "imports"))
		if got := storedConfig(t, f).HistoryImports; got != want {
			t.Fatalf("a run submitted from %s stores imports %q, want %q", tc.dir, got, want)
		}
	}
	_, wait := startCLIIn(t, second, nil, nil, env, args("second", "in-2")...)
	if r := wait(); r.code != exitOK || r.stdout != "done\n" || strings.Contains(r.stderr, "differs") {
		t.Fatalf("an identical retry: %+v", r)
	}
}

// storedConfig is the stored config of the fixture's latest run.
func storedConfig(t *testing.T, f *fixture) host.RunConfig {
	t.Helper()
	store, err := sqlite.OpenReadOnly(context.Background(), filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, c, err := host.Find(context.Background(), store, f.workspace, "")
	if err != nil || run.ID == "" {
		t.Fatal(err)
	}
	return c
}

// TestARetryShowsWhatItChangesAndWhyTheHostWaits requires that a deliberate
// retry of an input with other settings names the settings the admitted run
// keeps instead, and that reporting an active run the host cannot continue
// by itself says why, naming the credential a client must give.
func TestARetryShowsWhatItChangesAndWhyTheHostWaits(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	if r := f.cli(nil, "run", "-host", "auto", "-once", "-input", "k", "-goal", "g"); r.code != exitActive {
		t.Fatalf("run -once: %+v", r)
	}
	r := f.cli(nil, "run", "-host", "auto", "-once", "-input", "k", "-goal", "g", "-max-tokens", "2000")
	if r.code != exitActive || !strings.Contains(r.stderr, "which keeps its own settings; this submission differs in max_tokens\n") ||
		!strings.Contains(r.stderr, "the host cannot continue run "+f.status().Run.ID+" by itself: a client must give a credential for anthropic-messages at "+f.model.URL) {
		t.Fatalf("a changed retry of an idle run: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-once", "-input", "k", "-goal", "g", "-model", "other-model", "-max-steps", "7"); r.code != exitActive ||
		!strings.Contains(r.stderr, "this submission differs in model, limits.max_steps\n") {
		t.Fatalf("a retry with another model and step limit: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-once", "-input", "k", "-goal", "g"); strings.Contains(r.stderr, "differs") {
		t.Fatalf("an identical retry names a difference: %+v", r)
	}
}

// TestACommandOnNoRunStartsNoHost requires that resume, stop, and resolve on
// the default -host, for a state directory that holds no journal, refuse as
// the embedded commands do, and create no journal, socket, log, or host.
func TestACommandOnNoRunStartsNoHost(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	for _, args := range [][]string{{"resume", unpinned}, {"stop", unpinned}, {"resolve", unpinned, "-job"}} {
		if r := f.cli(nil, args[0], args[1:]...); r.code != exitError || !strings.Contains(r.stderr, "no runs in workspace") {
			t.Fatalf("%s without a journal: %+v", args[0], r)
		}
	}
	for _, name := range []string{"canti.db", hostrpc.SocketName, "host.log", "lock"} {
		if _, err := os.Stat(filepath.Join(f.state, name)); !os.IsNotExist(err) {
			t.Errorf("%s exists after commands on no run: %v", name, err)
		}
	}
	if pids := f.hostPIDs(); len(pids) != 0 {
		t.Fatalf("hosts %v were started", pids)
	}
}

// TestASubmissionFollowsFromWhatItHasNotShown requires that a client follows
// a run its own submission admitted, also one whose answer was lost and
// replayed, from the start, and a run an earlier invocation admitted from
// its reported state.
func TestASubmissionFollowsFromWhatItHasNotShown(t *testing.T) {
	ack := hostrpc.Ack{Run: hostrpc.Run{RunID: "run_1", Cursor: "run_1:7"}}
	for _, tc := range []struct {
		replayed, lost bool
		want           string
	}{{false, false, ""}, {true, true, ""}, {true, false, "run_1:7"}} {
		ack.Replayed = tc.replayed
		if got := followFrom(ack, tc.lost); got != tc.want {
			t.Errorf("followFrom(replayed %v, lost %v) = %+v; want %+v", tc.replayed, tc.lost, got, tc.want)
		}
	}
}

// TestAResumeRefusesASecretBearingURLWithoutChangingItsStoredProvider requires that
// resuming a current run, embedded or through the host process, with a
// credential bound to another endpoint is refused before any model request, without
// committing anything, and with no part of the URL's secret on stderr, in
// the host's log, or in any file of the state directory, while a valid
// provider still resumes it.
func TestAResumeRefusesASecretBearingURLWithoutChangingItsStoredProvider(t *testing.T) {
	const marker = "DUMMYMARKER"
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
		t.Fatalf("run -once: %+v", r)
	}
	before := f.history()
	bad := f.model.URL + "/v?key=" + marker
	for _, tc := range []struct {
		name string
		env  []string
		args []string
	}{
		{"embedded -model-url", nil, []string{"resume", "-host", "embedded", "-once", "-model-url", bad}},
		{"embedded variable", []string{"ANTHROPIC_BASE_URL=" + bad}, []string{"resume", "-host", "embedded", "-once", "-model-url", ""}},
		{"host -model-url", nil, []string{"resume", "-host", "auto", "-once", "-model-url", bad}},
		{"host variable", []string{"ANTHROPIC_BASE_URL=" + bad}, []string{"resume", "-host", "auto", "-once", "-model-url", ""}},
	} {
		r := f.cli(tc.env, tc.args[0], tc.args[1:]...)
		if r.code != exitError || strings.Contains(r.stdout+r.stderr, marker) || !strings.Contains(r.stderr, "credential given is for") {
			t.Errorf("%s: %+v", tc.name, r)
		}
	}
	if h := f.history(); !reflect.DeepEqual(h, before) || len(f.model.requests()) != 0 {
		t.Fatalf("refused resumes appended %v and sent %d requests", kinds(h[len(before):]), len(f.model.requests()))
	}
	filepath.WalkDir(f.state, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && strings.Contains(readFile(t, path), marker) {
			t.Errorf("%s holds the marker", path)
		}
		return nil
	})
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "done\n" || len(f.model.requests()) != 1 {
		t.Fatalf("a valid resume: %+v, %d requests", r, len(f.model.requests()))
	}
}

// TestARetryNamesItsRunAfterItsImportsAreGone requires that a deliberate
// retry of an input whose relative -history-imports directory has since been
// removed still names the run first admitted for it, without a difference,
// that one of another goal still conflicts, and that a new input naming the
// missing directory, with -input or without, is refused and admits nothing.
func TestARetryNamesItsRunAfterItsImportsAreGone(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "imports"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(goal string, extra ...string) result {
		_, wait := startCLIIn(t, dir, nil, nil, []string{"ANTHROPIC_API_KEY=" + f.key}, append(append(append([]string{"run", "-workspace", f.workspace,
			"-state", f.state, "-host", "auto", "-model-url", f.model.URL}, fixtureModel...), "-history-imports", "imports", "-goal", goal), extra...)...)
		return wait()
	}
	if r := run("g", "-input", "k"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("first run: %+v", r)
	}
	first := f.status().Run.ID
	if err := os.Remove(filepath.Join(dir, "imports")); err != nil {
		t.Fatal(err)
	}
	if r := run("g", "-input", "k"); r.code != exitOK || r.stdout != "done\n" || strings.Contains(r.stderr, "differs") || f.status().Run.ID != first {
		t.Fatalf("a retry after the imports were removed: %+v", r)
	}
	if r := run("other", "-input", "k"); r.code != exitError || !strings.Contains(r.stderr, "different content") {
		t.Fatalf("a retry of another goal: %+v", r)
	}
	for _, extra := range [][]string{{"-input", "k2"}, nil} {
		if r := run("new", extra...); r.code != exitUsage || !strings.Contains(r.stderr, "history imports") {
			t.Fatalf("a new input naming the missing directory (%q): %+v", extra, r)
		}
	}
	if s := f.status(); s.Run.ID != first || len(f.model.requests()) != 1 {
		t.Fatalf("the latest run is %s after %d requests; want only the first", s.Run.ID, len(f.model.requests()))
	}
}

// TestAReplayNamesTheSettingsItIgnoresInEveryMode requires that a retry of
// an input that names another history imports directory than its admitted
// run says, on stderr, that the run keeps its own settings and which ones
// the retry named otherwise, embedded and through the host process, alone
// and in a session, with the run's result on stdout, and that an identical
// retry names none.
func TestAReplayNamesTheSettingsItIgnoresInEveryMode(t *testing.T) {
	for _, mode := range []string{"embedded", "auto"} {
		for _, session := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s session %v", mode, session), func(t *testing.T) {
				f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
				f.autoHost()
				first, other := t.TempDir(), t.TempDir()
				submit := func(dir string) result {
					args := []string{"-host", mode, "-input", "k", "-goal", "g", "-history-imports", dir}
					if session {
						args = append(args, "-session", "s")
					}
					return f.cli(nil, "run", args...)
				}
				if r := submit(first); r.code != exitOK || r.stdout != "done\n" || strings.Contains(r.stderr, "differs") {
					t.Fatalf("first submission: %+v", r)
				}
				run := f.status().Run.ID
				if r := submit(other); r.code != exitOK || r.stdout != "done\n" ||
					!strings.Contains(r.stderr, "canti: input k names run "+run+", which keeps its own settings; this submission differs in history_imports\n") {
					t.Fatalf("a retry naming another directory: %+v", r)
				}
				if r := submit(first); r.code != exitOK || r.stdout != "done\n" || strings.Contains(r.stderr, "differs") {
					t.Fatalf("an identical retry: %+v", r)
				}
				if f.status().Run.ID != run || len(f.model.requests()) != 1 {
					t.Fatalf("the retries made run %s and %d requests", f.status().Run.ID, len(f.model.requests()))
				}
			})
		}
	}
}
