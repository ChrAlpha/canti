package docker

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

type attachedFake struct {
	t                                   *testing.T
	mu                                  sync.Mutex
	dir, workspace, endpoint, container string
	running, exists                     bool
	epoch                               time.Time
	next, starts, stops                 int
	execs                               map[string]execMetadata
	requests                            map[string]execRequest
	stopEntered, stopRelease            chan struct{}
	stopOnce                            sync.Once
	dropStop                            bool
	engine                              *engine
	address                             string
	startEntered, startRelease          chan struct{}
	startOnce                           sync.Once
	labelIdentity, extraMount           string
	extraTarget                         string
}

func newAttachedFake(t *testing.T) *attachedFake {
	f := &attachedFake{t: t, dir: t.TempDir(), workspace: t.TempDir(), running: true, exists: true, epoch: time.Now().UTC(), container: strings.Repeat("c", 64), execs: map[string]execMetadata{}, requests: map[string]execRequest{}}
	f.endpoint = "unix://" + filepath.Join(f.dir, "engine.sock")
	helper := filepath.Join(t.TempDir(), "helper")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\necho 1\n"), 0555); err != nil {
		t.Fatal(err)
	}
	if err := InitEnvironment(f.dir, helper); err != nil {
		t.Fatal(err)
	}
	f.labelIdentity, _ = EnvironmentIdentity(f.dir)
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	f.address = server.Listener.Addr().String()
	t.Cleanup(server.Close)
	f.engine = dialing(server)
	f.engine.single = true
	key := "docker\x00" + f.endpoint
	engines.Store(key, f.engine)
	t.Cleanup(func() { engines.Delete(key); f.engine.client.CloseIdleConnections() })
	if _, err := AttachEnvironment(t.Context(), f.dir, f.endpoint, "dev", ""); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *attachedFake) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/version" {
		fmt.Fprint(w, `{"ApiVersion":"1.52","MinAPIVersion":"1.24"}`)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v"+apiMax)
	if path == "/info" {
		json.NewEncoder(w).Encode(map[string]string{"ID": "daemon", "OSType": "linux", "Architecture": runtime.GOARCH})
		return
	}
	if strings.HasSuffix(path, "/stop") {
		f.mu.Lock()
		entered, release, drop := f.stopEntered, f.stopRelease, f.dropStop
		f.stops++
		f.mu.Unlock()
		if entered != nil {
			f.stopOnce.Do(func() { close(entered) })
			<-release
		}
		f.mu.Lock()
		target := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/stop")
		if !f.exists || target != f.container {
			f.mu.Unlock()
			w.WriteHeader(404)
			fmt.Fprint(w, `{"message":"No such container"}`)
			return
		}
		f.running = false
		for id, m := range f.execs {
			if m.Running {
				m.Running = false
				m.ExitCode = nil
				f.execs[id] = m
			}
		}
		f.mu.Unlock()
		if drop {
			conn, _, _ := w.(http.Hijacker).Hijack()
			if conn != nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if path == "/containers/json" {
		if f.exists {
			json.NewEncoder(w).Encode([]map[string]string{{"Id": f.container}})
		} else {
			fmt.Fprint(w, "[]")
		}
		return
	}
	if strings.HasPrefix(path, "/containers/") && strings.HasSuffix(path, "/json") {
		selected := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/json")
		if !f.exists || selected != "dev" && selected != f.container {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"message":"No such container"}`)
			return
		}
		var c preparedContainer
		c.ID = f.container
		c.Config.User = "1000:1000"
		c.Config.Env = []string{"HOME=/home/dev", "PATH=/usr/local/bin:/usr/bin:/bin"}
		c.Config.Labels = map[string]string{controlLabel: f.dir, authorityLabel: f.labelIdentity}
		c.State.Running = f.running
		c.State.StartedAt = f.epoch
		c.Mounts = append(c.Mounts, struct {
			Type, Source, Destination string
			RW                        bool
		}{"bind", filepath.Join(f.dir, "public"), "/canti", false}, struct {
			Type, Source, Destination string
			RW                        bool
		}{"bind", filepath.Join(f.dir, "spool"), spoolRoot, true}, struct {
			Type, Source, Destination string
			RW                        bool
		}{"bind", f.workspace, "/workspace", true})
		if f.extraMount != "" {
			target := f.extraTarget
			if target == "" {
				target = "/extra"
			}
			c.Mounts = append(c.Mounts, struct {
				Type, Source, Destination string
				RW                        bool
			}{"bind", f.extraMount, target, false})
		}
		json.NewEncoder(w).Encode(c)
		return
	}
	if strings.HasSuffix(path, "/archive") {
		name := strings.TrimPrefix(r.URL.Query().Get("path"), "/canti/")
		data, err := os.ReadFile(filepath.Join(f.dir, "public", name))
		if err != nil {
			w.WriteHeader(404)
			return
		}
		tw := tar.NewWriter(w)
		tw.WriteHeader(&tar.Header{Name: filepath.Base(name), Mode: 0444, Size: int64(len(data)), Typeflag: tar.TypeReg})
		tw.Write(data)
		tw.Close()
		return
	}
	if strings.HasPrefix(path, "/containers/") && strings.HasSuffix(path, "/exec") {
		var req execRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(400)
			return
		}
		f.next++
		id := fmt.Sprintf("%064x", f.next)
		f.execs[id] = execMetadata{ID: id, ContainerID: f.container}
		f.requests[id] = req
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"Id": id})
		return
	}
	if strings.HasPrefix(path, "/exec/") {
		id := strings.Split(strings.TrimPrefix(path, "/exec/"), "/")[0]
		m, ok := f.execs[id]
		if !ok {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"message":"No such exec instance"}`)
			return
		}
		if strings.HasSuffix(path, "/start") {
			f.starts++
			if f.startEntered != nil {
				f.startOnce.Do(func() { close(f.startEntered) })
				release := f.startRelease
				f.mu.Unlock()
				<-release
				f.mu.Lock()
			}
			if !f.running || m.ContainerID != f.container {
				w.WriteHeader(404)
				fmt.Fprint(w, `{"message":"No such exec instance"}`)
				return
			}
			m.Running = true
			m.Pid = 42
			f.execs[id] = m
			w.WriteHeader(http.StatusOK)
			return
		}
		json.NewEncoder(w).Encode(m)
		return
	}
	if strings.HasSuffix(path, "/start") {
		f.running = true
		f.epoch = time.Now().UTC()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(404)
}
func (f *attachedFake) executor(t *testing.T) *Attached {
	t.Helper()
	a, err := ReadAttachment(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	cwd, err := EnvironmentWorkdir(t.Context(), a, f.workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewAttached(AttachedConfig{Attachment: a, Workspace: f.workspace, State: state, Workdir: cwd, OutputLimit: 1024, PreviewLimit: 128})
	if err != nil {
		t.Fatal(err)
	}
	return x
}
func effectFor(x *Attached, id, command string) harness.Effect {
	return harness.Effect{ID: id, Command: command, Spec: x.Specification(command)}
}
func (f *attachedFake) complete(t *testing.T, x *Attached, e harness.Effect, code int, text string) {
	t.Helper()
	r, found, err := x.readJob(e)
	if err != nil || !found {
		t.Fatalf("command=%+v: %v", r, err)
	}
	rec := jobrunner.Record{Protocol: jobrunner.Protocol, EffectID: e.ID, Spec: e.Spec, Generation: r.Generation, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), ExitCode: &code, Bytes: int64(len(text)), Stored: int64(len(text)), Digest: harness.ConfigDigest([]byte(text)), Preview: text}
	data, _ := json.Marshal(rec)
	for name, body := range map[string][]byte{"output": []byte(text), "completion.json": data} {
		if err = os.WriteFile(filepath.Join(f.dir, "spool", e.ID, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	m := f.execs[r.ExecID]
	m.Running = false
	m.ExitCode = &code
	f.execs[r.ExecID] = m
	f.mu.Unlock()
}
func TestAttachedCommandsRetainIndependentBindingsAndReceipts(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	for i := 1; i <= 2; i++ {
		e := effectFor(x, "effect_"+strconv.Itoa(i), "printf useful")
		j, err := x.Drive(t.Context(), e)
		if err != nil || j.Status != harness.JobRunning {
			t.Fatalf("drive=%+v: %v", j, err)
		}
		f.complete(t, x, e, 0, "useful")
		j, err = x.Inspect(t.Context(), e)
		if err != nil || j.Status != harness.JobSucceeded || j.Binding.Generation != uint64(i) {
			t.Fatalf("complete=%+v: %v", j, err)
		}
		f.mu.Lock()
		delete(f.execs, j.ID)
		f.mu.Unlock()
		again, err := x.Cancel(t.Context(), e)
		if err != nil || again.ID != j.ID || again.Status != harness.JobSucceeded {
			t.Fatalf("retained=%+v: %v", again, err)
		}
	}
	if f.starts != 2 {
		t.Fatalf("starts=%d", f.starts)
	}
}
func TestAttachedCancellationConfirmsCessationWithoutPID1Exit(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_cancel", "sleep 30")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	j, err := x.Cancel(t.Context(), e)
	if err != nil || j.Status != harness.JobCancelled || j.ExitCode != nil || !j.StartedAt.IsZero() || !j.FinishedAt.IsZero() || j.Cessation == nil {
		t.Fatalf("cancel=%+v: %v", j, err)
	}
	status, err := ControlEnvironment(t.Context(), f.dir, "start")
	if err != nil || status.Gate != "open" {
		t.Fatalf("restart=%+v: %v", status, err)
	}
	second := effectFor(x, "effect_next", "true")
	if _, err = x.Drive(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	stops := f.stops
	if _, err = x.Cancel(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if f.stops != stops {
		t.Fatal("old cancellation stopped the newer command")
	}
}
func TestAttachedCompetingStatesCannotDispatchBesideSurvivingCommand(t *testing.T) {
	f := newAttachedFake(t)
	first, second := f.executor(t), f.executor(t)
	e := effectFor(first, "effect_first", "sleep 30")
	if _, err := first.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	next := effectFor(second, "effect_second", "true")
	if _, err := second.Drive(t.Context(), next); err == nil {
		t.Fatal("second state dispatched beside live orphan")
	}
	if f.starts != 1 {
		t.Fatal("duplicated foreground work")
	}
	f.complete(t, first, e, 0, "done")
	if _, err := second.Drive(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if _, done, err := first.cached(e); err != nil || !done {
		t.Fatalf("takeover did not publish prior owner's receipt: %v", err)
	}
}

func TestAttachedDelayedStopKeepsSameIDQuarantinedAcrossTakeover(t *testing.T) {
	f := newAttachedFake(t)
	first, second := f.executor(t), f.executor(t)
	e := effectFor(first, "effect_old", "sleep 30")
	if _, err := first.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	f.stopEntered, f.stopRelease = make(chan struct{}), make(chan struct{})
	f.dropStop = true
	release := sync.OnceFunc(func() { close(f.stopRelease) })
	defer release()
	first.backend.timeouts.call = 30 * time.Millisecond
	first.backend.cfg.StopGrace = time.Millisecond
	done := make(chan error, 1)
	go func() { _, err := first.Cancel(t.Context(), e); done <- err }()
	<-f.stopEntered
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bounded cancellation did not return")
	}
	// A later stopped sample still does not acknowledge the held old request.
	f.mu.Lock()
	f.running = false
	f.mu.Unlock()
	if _, err := ControlEnvironment(t.Context(), f.dir, "start"); err == nil {
		t.Fatal("reopened same ID with an old stop in flight")
	}
	next := effectFor(second, "effect_new", "true")
	if _, err := second.Drive(t.Context(), next); err == nil {
		t.Fatal("a second state dispatched under quarantine")
	}
	release()
	if _, err := ControlEnvironment(t.Context(), f.dir, "start"); err == nil {
		t.Fatal("late stop delivery manufactured an acknowledgement")
	}
	f.mu.Lock()
	starts := f.starts
	f.mu.Unlock()
	if starts != 1 {
		t.Fatalf("starts=%d", starts)
	}
}
func TestAttachedRetirementRequiresOldIDAbsenceAndKeepsOldReceipts(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	doneEffect := effectFor(x, "effect_done", "true")
	if _, err := x.Drive(t.Context(), doneEffect); err != nil {
		t.Fatal(err)
	}
	f.complete(t, x, doneEffect, 0, "done")
	if _, err := x.Inspect(t.Context(), doneEffect); err != nil {
		t.Fatal(err)
	}
	if _, err := ControlEnvironment(t.Context(), f.dir, "retire"); err == nil {
		t.Fatal("retired a present old container")
	}
	f.mu.Lock()
	old := f.container
	f.exists = false
	f.mu.Unlock()
	status, err := ControlEnvironment(t.Context(), f.dir, "retire")
	if err != nil || status.Gate != "retired" {
		t.Fatalf("retire=%+v: %v", status, err)
	}
	f.mu.Lock()
	f.exists = true
	f.container = strings.Repeat("d", 64)
	f.running = true
	f.epoch = time.Now().UTC()
	f.mu.Unlock()
	a, err := AttachEnvironment(t.Context(), f.dir, f.endpoint, "dev", "")
	if err != nil || a.ContainerID == old || a.Incarnation != 2 {
		t.Fatalf("replacement=%+v: %v", a, err)
	}
	j, err := x.Cancel(t.Context(), doneEffect)
	if err != nil || j.Status != harness.JobSucceeded {
		t.Fatalf("old receipt=%+v: %v", j, err)
	}
	oldEffect := effectFor(x, "effect_old_pending", "true")
	if _, err = x.Drive(t.Context(), oldEffect); !errors.Is(err, harness.ErrJobLost) {
		t.Fatalf("old run mutated replacement: %v", err)
	}
}
func TestAttachedMissingExecMetadataNeverReplaysAcceptedWork(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_missing", "touch marker")
	j, err := x.Drive(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	delete(f.execs, j.ID)
	f.mu.Unlock()
	j, err = x.Drive(t.Context(), e)
	if err != nil || j.Status != harness.JobUnknown {
		t.Fatalf("missing=%+v: %v", j, err)
	}
	if f.starts != 1 {
		t.Fatal("replayed command after metadata loss")
	}
	j, err = x.Cancel(t.Context(), e)
	if err != nil || j.Status != harness.JobCancelled || j.ExitCode != nil {
		t.Fatalf("reconcile=%+v: %v", j, err)
	}
	if _, err = ControlEnvironment(t.Context(), f.dir, "start"); err != nil {
		t.Fatal(err)
	}
}
func TestAttachedUnconfirmedLaunchCannotBecomePendingOrSuccessful(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_unconfirmed", "true")
	j, err := x.Drive(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := x.readJob(e)
	if err != nil {
		t.Fatal(err)
	}
	// Remove acknowledgement to model controller death before its publication,
	// and restore the exact API1.47 unstarted/null metadata shape.
	dir, _, err := x.loadRequest(r.Launch)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, "ack.json")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.execs[j.ID] = execMetadata{ID: j.ID, ContainerID: f.container}
	f.mu.Unlock()
	j, err = x.Drive(t.Context(), e)
	if err != nil || j.Status != harness.JobUnknown || j.DispatchedAt.IsZero() {
		t.Fatalf("unconfirmed=%+v: %v", j, err)
	}
	if f.starts != 1 {
		t.Fatal("sent ExecStart again")
	}
	j, err = x.Cancel(t.Context(), e)
	if err != nil || j.Cessation == nil || j.Cessation.Kind != harness.CessationQuarantined {
		t.Fatalf("quarantined cessation=%+v: %v", j, err)
	}
	if _, err = ControlEnvironment(t.Context(), f.dir, "start"); err == nil {
		t.Fatal("restarted environment with an unconsumed old launch")
	}
}

func TestAttachedReceiptSurvivesAuthorityAndContainerDeletion(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_retained", "true")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	f.complete(t, x, e, 0, "retained")
	before, err := x.Inspect(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(filepath.Join(f.dir, "authority")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.exists = false
	f.mu.Unlock()
	rebuilt, err := NewAttached(x.cfg)
	if err != nil {
		t.Fatal(err)
	}
	after, err := rebuilt.Cancel(t.Context(), e)
	if err != nil || after.ID != before.ID || after.Status != harness.JobSucceeded || after.Preview != "retained" {
		t.Fatalf("replayed=%+v: %v", after, err)
	}
}
func TestAttachedRejectsForgedCompletionWhileCommandIsLive(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_forged", "sleep 30")
	j, err := x.Drive(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	f.complete(t, x, e, 0, "forged")
	f.mu.Lock()
	m := f.execs[j.ID]
	m.Running = true
	m.ExitCode = nil
	f.execs[j.ID] = m
	f.mu.Unlock()
	j, err = x.Inspect(t.Context(), e)
	if err != nil || j.Status != harness.JobRunning {
		t.Fatalf("forged completion settled live command: %+v %v", j, err)
	}
	_, s, err := readEnvironment(f.dir)
	if err != nil || s.Active != e.ID {
		t.Fatal("forged helper evidence released foreground ownership")
	}
}
func TestAttachedRejectsFutureHelperTimestamps(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_clock", "true")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	f.complete(t, x, e, 0, "done")
	path := filepath.Join(f.dir, "spool", e.ID, "completion.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rec jobrunner.Record
	json.Unmarshal(data, &rec)
	rec.StartedAt = time.Now().Add(time.Hour)
	rec.FinishedAt = rec.StartedAt
	data, _ = json.Marshal(rec)
	os.WriteFile(path, data, 0600)
	j, err := x.Inspect(t.Context(), e)
	if err != nil || j.Status != harness.JobUnknown {
		t.Fatalf("future helper clock became trusted time: %+v %v", j, err)
	}
}

// attachedWorker is a real separate controller process used to exercise flock
// release and durable request ownership after SIGKILL, over the fake CLI's HTTP.
func attachedWorker(path string) int {
	var task struct {
		Config    AttachedConfig
		Effect    harness.Effect
		Operation string
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return 1
	}
	if err = json.Unmarshal(data, &task); err != nil {
		return 1
	}
	x, err := NewAttached(task.Config)
	if err != nil {
		return 1
	}
	if task.Operation == "cancel" {
		_, err = x.Cancel(context.Background(), task.Effect)
	} else {
		_, err = x.Drive(context.Background(), task.Effect)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
func (f *attachedFake) worker(t *testing.T, x *Attached, e harness.Effect, op string) *exec.Cmd {
	t.Helper()
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cli := filepath.Join(dir, "docker")
	script := fmt.Sprintf("#!/bin/sh\nCANTI_TEST_DIAL=%q exec %q \"$@\"\n", f.address, binary)
	if err = os.WriteFile(cli, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	task := struct {
		Config    AttachedConfig
		Effect    harness.Effect
		Operation string
	}{x.cfg, e, op}
	data, _ := json.Marshal(task)
	path := filepath.Join(dir, "task.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^$")
	command.Env = []string{"PATH=" + dir + ":/usr/bin:/bin", "CANTI_ATTACHED_WORKER=" + path}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill() })
	return command
}
func TestAttachedControllerSIGKILLRetainsDelayedStopFenceUntilNewID(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_killed", "sleep 30")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	f.stopEntered, f.stopRelease = make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(f.stopRelease) })
	defer release()
	worker := f.worker(t, x, e, "cancel")
	select {
	case <-f.stopEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never sent its durable stop request")
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err == nil {
		t.Fatal("controller was not killed")
	}
	f.mu.Lock()
	f.running = false
	f.mu.Unlock()
	if _, err := ControlEnvironment(t.Context(), f.dir, "start"); err == nil {
		t.Fatal("SIGKILL plus stopped sample reopened same ID")
	}
	f.mu.Lock()
	f.exists = false
	f.mu.Unlock()
	if _, err := ControlEnvironment(t.Context(), f.dir, "retire"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.exists = true
	f.running = true
	f.container = strings.Repeat("d", 64)
	f.epoch = time.Now().UTC()
	f.mu.Unlock()
	if _, err := AttachEnvironment(t.Context(), f.dir, f.endpoint, "dev", ""); err != nil {
		t.Fatal(err)
	}
	newer := f.executor(t)
	next := effectFor(newer, "effect_replacement", "true")
	if _, err := newer.Drive(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	release()
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	running := f.running
	f.mu.Unlock()
	if !running {
		t.Fatal("delayed old stop killed replacement environment")
	}
}
func TestAttachedControllerSIGKILLNeverReplaysDelayedLaunch(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_delayed_launch", "touch marker")
	f.startEntered, f.startRelease = make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(f.startRelease) })
	defer release()
	worker := f.worker(t, x, e, "drive")
	select {
	case <-f.startEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never sent its launch")
	}
	if err := worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	worker.Wait()
	j, err := x.Cancel(t.Context(), e)
	if err != nil || j.Cessation == nil || j.Cessation.Kind != harness.CessationQuarantined {
		t.Fatalf("delayed launch cessation=%+v: %v", j, err)
	}
	if _, err = ControlEnvironment(t.Context(), f.dir, "start"); err == nil {
		t.Fatal("restarted beside unconsumed launch")
	}
	release()
	f.mu.Lock()
	starts := f.starts
	f.mu.Unlock()
	if starts != 1 {
		t.Fatal("accepted command was replayed")
	}
}

func TestAttachedStoredOriginIsValidatedBeforeAnyDirectoryCreation(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_origin", "true")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	r, _, err := x.readJob(e)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "must-not-be-created")
	r.Config.State = outside
	if err = saveJSON(filepath.Join(x.jobDir(e.ID), "command.json"), r, false); err != nil {
		t.Fatal(err)
	}
	if _, err = x.Inspect(t.Context(), e); err == nil {
		t.Fatal("accepted a mismatched stored origin")
	}
	if _, err = os.Stat(outside); !os.IsNotExist(err) {
		t.Fatal("corrupt origin created host directories")
	}
}
func TestAttachedStateExclusionIsPureAndCheckedAgainAtDispatch(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	cfg := x.cfg
	cfg.State = filepath.Join(f.dir, "spool", "journal")
	if _, err := NewAttached(cfg); err == nil {
		t.Fatal("accepted state inside writable environment spool")
	}
	broad := t.TempDir()
	cfg = x.cfg
	cfg.State = filepath.Join(broad, "journal")
	f.mu.Lock()
	f.extraMount = broad
	f.mu.Unlock()
	rebuilt, err := NewAttached(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := effectFor(rebuilt, "effect_exposed_state", "true")
	if _, err = rebuilt.Drive(t.Context(), e); err == nil {
		t.Fatal("dispatch exposed a journal through an additional mount")
	}
	if _, err = os.Stat(cfg.State); !os.IsNotExist(err) {
		t.Fatal("mount rejection created exposed state")
	}
}
func TestAttachedFreshAuthorityCannotReopenTheOldPhysicalContainer(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_old_authority", "sleep 30")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	oldID := x.cfg.Attachment.AuthorityID
	// Preserve the mounted directory inodes while losing all initialization data;
	// a new host-only token would not fence delayed requests against this old ID.
	for _, name := range []string{"authority", "public/bin"} {
		if err := os.RemoveAll(filepath.Join(f.dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(filepath.Join(f.dir, "spool"))
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(f.dir, "spool", entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	helper := filepath.Join(t.TempDir(), "canti-job")
	os.WriteFile(helper, []byte("#!/bin/sh\necho 1\n"), 0555)
	if err := InitEnvironment(f.dir, helper); err != nil {
		t.Fatal(err)
	}
	fresh, err := EnvironmentIdentity(f.dir)
	if err != nil || fresh == oldID {
		t.Fatalf("new identity=%q: %v", fresh, err)
	}
	if _, err = AttachEnvironment(t.Context(), f.dir, f.endpoint, "dev", ""); err == nil {
		t.Fatal("fresh authority reopened old immutable container metadata")
	}
	if _, err = x.Drive(t.Context(), e); !errors.Is(err, harness.ErrJobLost) {
		t.Fatalf("old run accepted replacement authority: %v", err)
	}
	if f.starts != 1 {
		t.Fatal("authority loss caused a duplicate dispatch")
	}
}

func TestAttachedLostAuthorityRecordNeverAuthorizesReplacementLaunch(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_record_lost", "sleep 30")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(x.jobDir(e.ID), "command.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Drive(t.Context(), e); !errors.Is(err, harness.ErrJobLost) {
		t.Fatalf("lost authority=%v", err)
	}
	if f.starts != 1 {
		t.Fatal("lost command authority caused another launch")
	}
}
func TestAttachedLocalLatchRejectsAResetAuthorityLatch(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	e := effectFor(x, "effect_latch_lost", "sleep 30")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	r, _, err := x.readJob(e)
	if err != nil {
		t.Fatal(err)
	}
	r.DispatchedAt = time.Time{}
	r.Launch = ""
	if err = saveJSON(filepath.Join(x.jobDir(e.ID), "command.json"), r, false); err != nil {
		t.Fatal(err)
	}
	if _, err = x.Drive(t.Context(), e); err == nil {
		t.Fatal("accepted reset launch latch")
	}
	if f.starts != 1 {
		t.Fatal("reset latch authorized replay")
	}
}
func TestAttachedNeverBoundCancellationDoesNotDisturbOtherState(t *testing.T) {
	f := newAttachedFake(t)
	first, second := f.executor(t), f.executor(t)
	active := effectFor(first, "effect_live", "sleep 30")
	if _, err := first.Drive(t.Context(), active); err != nil {
		t.Fatal(err)
	}
	absent := effectFor(second, "effect_never_bound", "true")
	j, err := second.Cancel(t.Context(), absent)
	if err != nil || j.Status != harness.JobCancelled || j.ID != "" {
		t.Fatalf("fence=%+v: %v", j, err)
	}
	if f.starts != 1 || f.stops != 0 {
		t.Fatal("unbound cancellation disturbed live foreground work")
	}
}

func TestAttachedReservedMountsCannotBeShadowedAtAttachmentOrRestart(t *testing.T) {
	for _, target := range []string{"/canti/bin", "/canti/bin/version/canti-job", spoolRoot + "/effect", "/var/lib/canti", "/"} {
		t.Run(target, func(t *testing.T) {
			f := newAttachedFake(t)
			x := f.executor(t)
			f.mu.Lock()
			f.extraMount, f.extraTarget = t.TempDir(), target
			f.mu.Unlock()
			if _, err := AttachEnvironment(t.Context(), f.dir, f.endpoint, "dev", ""); err == nil {
				t.Fatal("attached with a reserved path shadow")
			}
			if _, err := x.Drive(t.Context(), effectFor(x, "shadow", "true")); err == nil {
				t.Fatal("dispatched with a reserved path shadow")
			}
			c := x.cfg
			c.Attachment.Mounts = append(c.Attachment.Mounts, EnvironmentMount{Type: "bind", Source: f.extraMount, Target: target})
			if _, err := NewAttached(c); err == nil {
				t.Fatal("reconstructed a stored reserved path shadow")
			}
			if f.starts != 0 {
				t.Fatal("started a command despite shadowing")
			}
		})
	}
}

func TestAttachedLifecycleRejectsInvalidTerminalBeforeMirroringOrReleasingSlot(t *testing.T) {
	for _, invalid := range []string{"shape", "binding", "nonterminal"} {
		t.Run(invalid, func(t *testing.T) {
			f := newAttachedFake(t)
			x := f.executor(t)
			e := effectFor(x, "invalid_terminal", "sleep 60")
			if _, err := x.Drive(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			r, found, err := x.readJob(e)
			if err != nil || !found {
				t.Fatalf("record: %v", err)
			}
			j := x.baseJob(r)
			j.Status = harness.JobSucceeded
			code := 0
			j.ExitCode, j.StartedAt, j.FinishedAt = &code, time.Now().UTC(), time.Now().UTC()
			switch invalid {
			case "shape":
				j.ExitCode = nil
			case "binding":
				j.Binding.Generation++
			case "nonterminal":
				j.Status, j.ExitCode, j.StartedAt, j.FinishedAt = harness.JobRunning, nil, time.Time{}, time.Time{}
			}
			r.Terminal = &j
			if err = x.saveJob(r); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(x.jobDir(e.ID), "command.json"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ControlEnvironment(t.Context(), f.dir, "stop"); err == nil {
				t.Fatal("accepted invalid terminal")
			}
			if _, err = os.Stat(x.cachePath(e.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("mirrored invalid terminal: %v", err)
			}
			_, state, err := readEnvironment(f.dir)
			if err != nil || state.Active != e.ID {
				t.Fatalf("released slot: %+v: %v", state, err)
			}
			after, err := os.ReadFile(filepath.Join(x.jobDir(e.ID), "command.json"))
			if err != nil || string(after) != string(before) {
				t.Fatalf("mutated command record: %v", err)
			}
			if f.stops != 0 || f.starts != 1 {
				t.Fatalf("mutated Docker: starts=%d stops=%d", f.starts, f.stops)
			}
		})
	}
}

func TestAttachedCompleteOutputStaysCompleteAfterRestart(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	if _, err := ControlEnvironment(t.Context(), f.dir, "stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := ControlEnvironment(t.Context(), f.dir, "start"); err != nil {
		t.Fatal(err)
	}
	e := effectFor(x, "after_restart", "printf retained")
	if _, err := x.Drive(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	f.complete(t, x, e, 0, "retained")
	j, err := x.Inspect(t.Context(), e)
	if err != nil || j.Status != harness.JobSucceeded || !j.Output.Complete || !strings.Contains(j.Output.Detail, "stored attachment observation") || strings.HasPrefix(j.Output.Detail, ";") {
		t.Fatalf("post-restart output=%+v: %v", j, err)
	}
}

func TestAttachedRejectsUnixEndpointUserinfo(t *testing.T) {
	f := newAttachedFake(t)
	x := f.executor(t)
	endpoint := "unix://fixture-user:fixture-password@/synthetic-engine.sock"
	if _, err := AttachEnvironment(t.Context(), f.dir, endpoint, "dev", ""); err == nil {
		t.Fatal("attachment accepted URL userinfo")
	}
	c := x.cfg
	c.Attachment.Endpoint = endpoint
	if _, err := NewAttached(c); err == nil {
		t.Fatal("stored attachment accepted URL userinfo")
	}
}
