package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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
echo "$cmd $*" >> "$ctl/calls"
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
	var c container
	c.ID = "c-e"
	c.Config.Labels = map[string]string{"canti.effect": "e", "canti.state": f.x.cfg.State, "canti.spec": specHash(f.x.runArgs("true"))}
	c.State.Status, c.State.ExitCode = status, exit
	b, err := json.Marshal([]container{c})
	if err != nil {
		f.t.Fatal(err)
	}
	return string(b)
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

func TestLaunchOutlivesShutdown(t *testing.T) {
	f := newFake(t)
	f.set("inspect.out", f.container("created", 0))
	f.set("running.json", f.container("running", 0))
	f.set("start.sh", `touch "$ctl/began"; sleep 0.3; cp "$ctl/running.json" "$ctl/inspect.out"`+"\n")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		defer cancel()
		for ctx.Err() == nil {
			if _, err := os.Stat(filepath.Join(f.ctl, "began")); err == nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if job, err := f.x.Start(ctx, "e"); err != nil || job.Status != harness.JobRunning {
		t.Fatalf("Start() interrupted after the latch = %+v, %v; want running", job, err)
	}
	if job, _, err := f.x.Inspect(t.Context(), "e"); err != nil || job.Status != harness.JobRunning {
		t.Fatalf("Inspect() = %+v, %v; want running", job, err)
	}
}

func TestVanishedStartedJobIsNotReportedMissing(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.missing()
	if _, found, err := f.x.Inspect(t.Context(), "e"); found || !errors.Is(err, ErrJobLost) {
		t.Fatalf("Inspect() = %v, %v; want ErrJobLost", found, err)
	}
	if _, err := f.x.Bind(t.Context(), effect); !errors.Is(err, ErrJobLost) || len(f.calls("create")) != 0 {
		t.Fatalf("Bind() = %v, creates %q; want ErrJobLost and no replacement", err, f.calls("create"))
	}
	if _, err := f.x.Start(t.Context(), "e"); !errors.Is(err, ErrJobLost) || len(f.calls("start")) != 0 {
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
}

func TestFailedCaptureIsRetriedAndSuccessKept(t *testing.T) {
	f := newFake(t)
	f.latch(latchStart)
	f.set("inspect.out", f.container("exited", 0))
	f.set("logs.err", "Error response from daemon: boom\n")
	f.set("logs.exit", "1")
	job, _, err := f.x.Inspect(t.Context(), "e")
	if err != nil || job.Status != harness.JobSucceeded || job.Output.Available || !strings.HasPrefix(job.Output.Detail, "logs unavailable") {
		t.Fatalf("Inspect(logs fail) = %+v, %v", job, err)
	}
	if _, err := os.Stat(filepath.Join(f.x.dir("e"), "output.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a failed capture was recorded: %v", err)
	}
	f.unset("logs.err", "logs.exit")
	f.set("logs.out", "hello\n")
	stale := filepath.Join(f.x.artifacts(), "e", ".tmp-crashed")
	if err := os.WriteFile(stale, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := harness.ArtifactRef{Path: "e/output", Bytes: 6, Complete: true, Available: true}
	for range 2 {
		job, _, err = f.x.Inspect(t.Context(), "e")
		if err != nil || job.Preview != "hello\n" || job.Output != want {
			t.Fatalf("Inspect() = %+v, %v", job, err)
		}
		f.set("logs.out", "changed\n")
	}
	if logs := f.calls("logs"); len(logs) != 2 {
		t.Fatalf("logs = %q, want one failed and one kept capture", logs)
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale capture file survived: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(f.x.artifacts(), "e", "output")); err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("artifact mode: %v, %v", fi, err)
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
