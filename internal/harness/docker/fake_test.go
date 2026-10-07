package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

const fakeDocker = `#!/bin/sh
ctl=%q
cmd=$1
[ "$cmd" = container ] && cmd=$2
echo "$cmd $*" | paste -sd ' ' - >> "$ctl/calls"
[ -f "$ctl/$cmd.sh" ] && . "$ctl/$cmd.sh"
[ -f "$ctl/$cmd.out" ] && cat "$ctl/$cmd.out"
[ -f "$ctl/$cmd.err" ] && cat "$ctl/$cmd.err" >&2
exit "$(cat "$ctl/$cmd.exit" 2>/dev/null || echo 0)"
`

type fake struct {
	t   *testing.T
	ctl string
	x   *Executor
}

func newFake(t *testing.T) *fake {
	ctl := t.TempDir()
	bin := filepath.Join(ctl, "docker")
	if err := os.WriteFile(bin, fmt.Appendf(nil, fakeDocker, ctl), 0o755); err != nil {
		t.Fatal(err)
	}
	return &fake{t: t, ctl: ctl, x: newTestExecutor(t, Config{Docker: bin})}
}

func (f *fake) set(name, content string) {
	if err := os.WriteFile(filepath.Join(f.ctl, name), []byte(content), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fake) unset(names ...string) {
	for _, name := range names {
		if err := os.Remove(filepath.Join(f.ctl, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			f.t.Fatal(err)
		}
	}
}

func (f *fake) missing() {
	f.unset("inspect.out")
	f.set("inspect.err", "Error response from daemon: No such container: canti-e\n")
	f.set("inspect.exit", "1")
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
	b, err := json.Marshal([]container{c})
	if err != nil {
		f.t.Fatal(err)
	}
	return string(b)
}

// exited makes the job of effect e exit with code, which its shell recorded.
func (f *fake) exited(code int) {
	f.set("inspect.out", f.container("exited", code))
	f.set("cp.out", exitArchive(f.t, fmt.Sprintf("%d\n", code)))
}

func (f *fake) calls(cmd string) []string {
	b, err := os.ReadFile(filepath.Join(f.ctl, "calls"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		f.t.Fatal(err)
	}
	var lines []string
	for line := range strings.Lines(string(b)) {
		if strings.HasPrefix(line, cmd+" ") {
			lines = append(lines, strings.TrimSpace(line))
		}
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

// cancelAt returns a context that is cancelled once the fake creates name.
func (f *fake) cancelAt(name string) context.Context {
	ctx, cancel := context.WithCancel(f.t.Context())
	f.t.Cleanup(cancel)
	go func() {
		defer cancel()
		for ctx.Err() == nil {
			if _, err := os.Stat(filepath.Join(f.ctl, name)); err == nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
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
	f.set("inspect.err", "Error: No such object: canti-e\n")
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || err != nil {
		t.Fatalf("Inspect(no such object) = %v, %v; want not found", found, err)
	}
	f.set("inspect.err", "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?\n")
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || err == nil {
		t.Fatalf("Inspect(daemon down) = %v, %v; want an error", found, err)
	}
	f.unset("inspect.err", "inspect.exit")
	for _, out := range []string{"[]", "not json", strings.ReplaceAll(f.container("created", 0), `"canti.effect":"e"`, `"canti.effect":"x"`)} {
		f.set("inspect.out", out)
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

func TestStartNeverRestartsAnUnlatchedContainer(t *testing.T) {
	f := newFake(t)
	for _, status := range []string{"exited", "running"} {
		f.set("inspect.out", f.container(status, 0))
		if _, err := f.x.Start(t.Context(), "e"); err == nil {
			t.Fatalf("Start(%s without latch) succeeded", status)
		}
	}
	if latch, err := readLatch(f.x.dir("e")); latch != "" || err != nil || len(f.calls("start")) != 0 {
		t.Fatalf("latch %q, %v, starts %q", latch, err, f.calls("start"))
	}
}

func TestFailedStartIsNeverReissued(t *testing.T) {
	f := newFake(t)
	f.set("inspect.out", f.container("created", 0))
	f.set("start.err", "Error response from daemon: boom\n")
	f.set("start.exit", "1")
	if _, err := f.x.Start(t.Context(), "e"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Start() = %v, want the docker error", err)
	}
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
}

// rejecting makes the fake's docker start fail as the daemon does for an
// unknown user, running script first.
func (f *fake) rejecting(script string) {
	f.set("inspect.out", f.container("created", 0))
	var c []container
	if err := json.Unmarshal([]byte(f.container("created", 128)), &c); err != nil {
		f.t.Fatal(err)
	}
	c[0].State.Error, c[0].State.StartedAt, c[0].State.FinishedAt = "unable to find user nosuchuser", time.Time{}, time.Time{}
	rejected, _ := json.Marshal(c)
	f.set("rejected.json", string(rejected))
	f.set("start.sh", script+`cp "$ctl/rejected.json" "$ctl/inspect.out"`+"\n")
	f.set("start.err", "Error response from daemon: unable to find user nosuchuser\n")
	f.set("start.exit", "1")
}

func TestRejectedStartFailsToStart(t *testing.T) {
	f := newFake(t)
	f.rejecting("")
	want, err := f.x.Start(t.Context(), "e")
	out := harness.ArtifactRef{Path: "e/output", Available: true, Detail: "job never started: unable to find user nosuchuser"}
	if err != nil || want.ID != "c-e" || want.Status != harness.JobFailedToStart || want.ExitCode != nil || !want.StartedAt.IsZero() || want.Output != out {
		t.Fatalf("Start(rejected) = %+v, %v; want the job failed to start with output %+v", want, err, out)
	}
	if _, err := os.Stat(f.x.record("e")); err != nil {
		t.Fatalf("the failed start was not recorded: %v", err)
	}
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm rm -v c-e" {
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
	f.rejecting("")
	f.set("logs.exit", "1")
	job, err := f.x.Start(t.Context(), "e")
	if want := "job never started: unable to find user nosuchuser; logs unavailable: exit status 1"; err != nil || job.Status != harness.JobFailedToStart || job.Output.Detail != want {
		t.Fatalf("Start(rejected, capture fails) = %+v, %v; want detail %q", job, err, want)
	}
	w := newFake(t)
	w.rejecting("")
	if err := os.WriteFile(filepath.Join(w.x.artifacts(), "e"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	job, err = w.x.Start(t.Context(), "e")
	if err != nil || !strings.HasPrefix(job.Output.Detail, "job never started: unable to find user nosuchuser; artifact write failed: ") {
		t.Fatalf("Start(rejected, artifact unwritable) = %+v, %v; want the reason kept", job, err)
	}
	g := newFake(t)
	g.rejecting("")
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
	f.rejecting(`touch "$ctl/began"; sleep 0.3; `)
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
	f.set("inspect.out", f.container("created", 0))
	f.set("running.json", f.container("running", 0))
	f.set("start.sh", `touch "$ctl/began"; sleep 0.3; cp "$ctl/running.json" "$ctl/inspect.out"`+"\n")
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
	for _, cmd := range []string{"inspect", "create", "start", "stop", "rm", "ps", "logs"} {
		f.set(cmd+".sh", "exec sleep 30\n")
	}
	for _, tt := range []struct {
		args  []string
		limit time.Duration
	}{
		{[]string{"container", "inspect", "canti-e"}, 50 * time.Millisecond},
		{[]string{"create", "--name", "canti-e"}, 150 * time.Millisecond},
		{[]string{"start", "c-e"}, 50 * time.Millisecond},
		{[]string{"stop", "-t", "1", "c-e"}, 150 * time.Millisecond},
		{[]string{"rm", "-v", "c-e"}, 50 * time.Millisecond},
		{[]string{"ps", "-aq"}, 50 * time.Millisecond},
	} {
		began := time.Now()
		_, err := f.x.docker(t.Context(), tt.args...)
		took := time.Since(began)
		if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			!strings.Contains(err.Error(), fmt.Sprintf("no response within %v", tt.limit)) || took < tt.limit || took > tt.limit+5*time.Second {
			t.Fatalf("docker %s (hung) = %v after %v; want an ordinary error at %v", tt.args[0], err, took, tt.limit)
		}
	}
	f.x.timeouts.call = time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := f.x.docker(ctx, "ps", "-aq"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("docker ps (hung, caller's deadline first) = %v, want the caller's error", err)
	}
	f.latch(latchStart)
	f.unset("inspect.sh", "rm.sh")
	f.exited(0)
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobSucceeded || job.Output.Available || job.Output.Detail != "logs unavailable: no response within 150ms" {
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

func TestBindAfterFenceCreatesCancelledJob(t *testing.T) {
	f := newFake(t)
	f.latch(latchCancel)
	f.missing()
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || err != nil {
		t.Fatalf("Inspect(fenced) = %v, %v; want not found", found, err)
	}
	f.set("created.json", f.container("created", 0))
	f.set("create.sh", `cp "$ctl/created.json" "$ctl/inspect.out"; rm -f "$ctl/inspect.err" "$ctl/inspect.exit"`+"\n")
	if job, err := f.x.Bind(t.Context(), effect); err != nil || job.Status != harness.JobCancelled || job.ID != "c-e" {
		t.Fatalf("Bind(fenced) = %+v, %v", job, err)
	}
	if creates := f.calls("create"); len(creates) != 1 || !strings.Contains(creates[0], "--name canti-e") {
		t.Fatalf("creates = %q", creates)
	}
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm rm -v c-e" {
		t.Fatalf("rms = %q, want the fenced container released", rms)
	}
	f.missing()
	if job, err := f.x.Bind(t.Context(), effect); err != nil || job.Status != harness.JobCancelled || job.ID != "c-e" || len(f.calls("create")) != 1 {
		t.Fatalf("Bind(released) = %+v, %v, creates %q; want the recorded job", job, err, f.calls("create"))
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
	f.set("logs.out", "hello\n")
	f.set("rm.sh", fmt.Sprintf("test -s %q || touch \"$ctl/early\"\n", filepath.Join(f.x.dir("e"), "output.json")))
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
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm rm -v c-e" {
		t.Fatalf("rms = %q, want the finished container released", rms)
	}
	if _, err := os.Stat(filepath.Join(f.ctl, "early")); !errors.Is(err, fs.ErrNotExist) {
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
		"renamed": func() { f.unset("inspect.err", "inspect.exit"); f.set("inspect.out", foreign) },
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
	for _, cmd := range []string{"create", "start", "stop"} {
		if calls := f.calls(cmd); len(calls) != 0 {
			t.Fatalf("%s calls = %q", cmd, calls)
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
	f.set("logs.out", "hello\n")
	f.set("rm.err", "Error response from daemon: boom\n")
	f.set("rm.exit", "1")
	want, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || want.Status != harness.JobSucceeded || want.Preview != "hello\n" {
		t.Fatalf("Inspect(rm fails) = %+v, %v", want, err)
	}
	for i := range 2 {
		if i == 1 {
			f.unset("rm.err", "rm.exit")
		}
		if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || !same(job, want) {
			t.Fatalf("Inspect() again = %+v, %v; want %+v", job, err, want)
		}
	}
	if rms, logs := f.calls("rm"), f.calls("logs"); len(rms) != 3 || rms[2] != "rm rm -v c-e" || len(logs) != 1 {
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
	f.set("ps.out", "c-old old\nc-live live\nc-other moved\n")
	f.missing()
	f.set("create.sh", "rm -f \"$ctl/inspect.err\" \"$ctl/inspect.exit\"\n")
	f.set("inspect.sh", fmt.Sprintf("[ -f \"$ctl/inspect.exit\" ] || printf '%%s' %q > \"$ctl/inspect.out\"\n", f.container("created", 0)))
	if _, err := f.x.Bind(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm rm -v c-old" {
		t.Fatalf("rms = %q, want only the recorded job of another effect released", rms)
	}
	if ps := f.calls("ps"); len(ps) != 1 || strings.Contains(ps[0], " -q") || strings.Contains(ps[0], " -aq") {
		t.Fatalf("ps calls = %q, want one listing whose --format is not overridden by --quiet", ps)
	}
}

func TestFailedCaptureIsRecordedAndReleased(t *testing.T) {
	for _, tt := range []struct {
		name, detail string
		fail         func(*fake)
	}{
		{"logs unavailable", "logs unavailable: ", func(f *fake) {
			f.set("logs.err", "Error response from daemon: boom\n")
			f.set("logs.exit", "1")
		}},
		{"artifact unwritable", "artifact write failed: ", func(f *fake) {
			f.set("logs.out", "hello\n")
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
			if rms := f.calls("rm"); len(rms) != 1 || rms[0] != "rm rm -v c-e" {
				t.Fatalf("rms = %q, want the container and its log released", rms)
			}
			f.unset("logs.err", "logs.exit")
			f.set("logs.out", "late\n")
			if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || !same(job, want) {
				t.Fatalf("Inspect() again = %+v, %v; want the recorded job", job, err)
			}
			if logs := f.calls("logs"); len(logs) != 1 {
				t.Fatalf("logs = %q, want the final report never captured again", logs)
			}
		})
	}
}

func TestInterruptedCaptureIsNotRecorded(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.set("logs.sh", `touch "$ctl/began"; exec sleep 5`+"\n")
	if _, _, err := f.x.Inspect(f.cancelAt("began"), "e"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Inspect(shutdown during capture) = %v, want cancellation", err)
	}
	if _, err := os.Stat(f.x.record("e")); !errors.Is(err, fs.ErrNotExist) || len(f.calls("rm")) != 0 {
		t.Fatalf("an interrupted capture was recorded: %v, rms %q", err, f.calls("rm"))
	}
	f.unset("logs.sh")
	f.set("logs.out", "hello\n")
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Preview != "hello\n" || !job.Output.Complete || len(f.calls("logs")) != 2 || len(f.calls("rm")) != 1 {
		t.Fatalf("Inspect() = %+v, %v, logs %q; want the next report to capture again", job, err, f.calls("logs"))
	}
}

func TestUnpublishedRecordIsNotFinal(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.exited(0)
	f.set("logs.out", "hello\n")
	f.set("logs.sh", fmt.Sprintf("mkdir -p %q\n", filepath.Join(f.x.record("e"), "blocked")))
	if job, _, err := f.x.Inspect(t.Context(), "e"); err == nil || len(f.calls("rm")) != 0 {
		t.Fatalf("Inspect(record unpublishable) = %+v, %v, rms %q; want an error and the container kept", job, err, f.calls("rm"))
	}
	if err := os.RemoveAll(f.x.record("e")); err != nil {
		t.Fatal(err)
	}
	f.unset("logs.sh")
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
			f.set("inspect.out", f.container("running", 0))
			f.set("ps.out", "c-e e\n")
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
			f.x.Bind(t.Context(), harness.Effect{ID: "other", Command: "true"})
			for _, cmd := range []string{"rm", "logs", "stop", "start", "create"} {
				if calls := f.calls(cmd); len(calls) != 0 {
					t.Fatalf("%s calls = %q", cmd, calls)
				}
			}
		})
	}
}

func TestCancelStopsOnlyARunningJob(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.set("inspect.out", f.container("running", 0))
	f.set("stopped.json", f.container("exited", 143))
	f.set("stop.sh", `cp "$ctl/stopped.json" "$ctl/inspect.out"`+"\n")
	f.set("logs.out", "partial\n")
	for range 2 {
		if err := f.x.Cancel(t.Context(), "e"); err != nil {
			t.Fatal(err)
		}
	}
	if stops := f.calls("stop"); len(stops) != 1 || stops[0] != "stop stop -t 5 c-e" {
		t.Fatalf("stops = %q", stops)
	}
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobCancelled || *job.ExitCode != 143 || job.Output.Complete || job.Preview != "partial\n" {
		t.Fatalf("Inspect() = %+v, %v", job, err)
	}
}
