//go:build integration

package docker_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/docker"
)

func appear(ctx context.Context, t *testing.T, path string) {
	t.Helper()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", path, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

const noRecord = "no exit record at /tmp/.canti-exit: the process was killed before it could record, or the record could not be written"

// kill kills the container of job from outside canti and waits until Docker
// reports it exited.
func kill(ctx context.Context, t *testing.T, job string) {
	t.Helper()
	if out, err := exec.Command("docker", "kill", job).CombinedOutput(); err != nil {
		t.Fatalf("docker kill: %v: %s", err, out)
	}
	var state struct{ State struct{ Status string } }
	for dockerInspect(t, job, &state); state.State.Status != "exited"; dockerInspect(t, job, &state) {
		if ctx.Err() != nil {
			t.Fatalf("killed container is %s", state.State.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOnlyARecordedExitIsFinal(t *testing.T) {
	cfg, id := setup(t, docker.Config{Memory: "512m", PidsLimit: 128})
	ctx := testContext(t, 2*time.Minute)
	x := open(t, cfg)
	for _, tt := range []struct {
		name, command, preview string
		code                   int
	}{
		{"true", "true", "", 0},
		{"exit", "echo before; exit 3", "before\n", 3},
		{"exec", "echo a\nexec sh -c 'exit 4'", "a\n", 4},
		{"own-signal", "echo b; kill -9 $$", "b\n", 137},
		{"syntax-error", "echo c\nif true; then echo d", "", 2},
	} {
		job := run(ctx, t, x, harness.Effect{ID: id(tt.name), Command: tt.command})
		want := harness.JobFailed
		if tt.code == 0 {
			want = harness.JobSucceeded
		}
		preview := job.Preview == tt.preview
		if tt.name == "syntax-error" {
			preview = strings.HasPrefix(job.Preview, "c\n") && strings.Contains(strings.ToLower(job.Preview), "syntax error")
		}
		if job.Status != want || job.ExitCode == nil || *job.ExitCode != tt.code || !preview {
			t.Errorf("%s: job = %+v, want %s with exit status %d and preview %q", tt.name, job, want, tt.code, tt.preview)
		}
	}

	killed := harness.Effect{ID: id("killed"), Command: "echo started; touch /workspace/began; sleep 300"}
	bound, err := x.Bind(ctx, killed)
	if err == nil {
		_, err = x.Start(ctx, killed.ID)
	}
	if err != nil {
		t.Fatal(err)
	}
	appear(ctx, t, filepath.Join(cfg.Workspace, "began"))
	kill(ctx, t, bound.ID)
	unknown := func(what string, job harness.Job) {
		t.Helper()
		if job.ID != bound.ID || job.Status != harness.JobUnknown || job.ExitCode != nil || job.Output.Detail != noRecord {
			t.Fatalf("%s = %+v, want job %s unknown with detail %q", what, job, bound.ID, noRecord)
		}
	}
	unknown("Inspect()", inspect(ctx, t, x, killed.ID))
	restarted := open(t, cfg)
	unknown("restarted Inspect()", inspect(ctx, t, restarted, killed.ID))
	if err := restarted.Cancel(ctx, killed.ID); err != nil {
		t.Fatal(err)
	}
	if job, err := restarted.Start(ctx, killed.ID); err != nil || job.ID != bound.ID || job.Status != harness.JobUnknown || job.Output != (harness.ArtifactRef{}) {
		t.Fatalf("Start() = %+v, %v; want job %s unknown without reading its exit record", job, err, bound.ID)
	}
	unknown("Inspect() after Cancel", inspect(ctx, t, open(t, cfg), killed.ID))
	if _, err := os.Stat(filepath.Join(cfg.State, "jobs", killed.ID, "output.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the unknown job was recorded: %v", err)
	}
	if ids := containers(t, killed.ID); !slices.Equal(ids, []string{bound.ID[:12]}) {
		t.Fatalf("containers of the killed job = %q, want %s kept for inspection", ids, bound.ID[:12])
	}

	stopped := harness.Effect{ID: id("stopped"), Command: "trap 'echo got TERM; exit 7' TERM; touch /workspace/stopping; sleep 300 & wait"}
	if bound, err = x.Bind(ctx, stopped); err == nil {
		_, err = x.Start(ctx, stopped.ID)
	}
	if err != nil {
		t.Fatal(err)
	}
	appear(ctx, t, filepath.Join(cfg.Workspace, "stopping"))
	began := time.Now()
	if out, err := exec.Command("docker", "stop", "-t", "30", bound.ID).CombinedOutput(); err != nil {
		t.Fatalf("docker stop: %v: %s", err, out)
	}
	took := time.Since(began)
	if job := await(ctx, t, x, stopped.ID, finished); job.Status != harness.JobFailed || job.ExitCode == nil || *job.ExitCode != 7 || job.Preview != "got TERM\n" || took > 10*time.Second {
		t.Fatalf("job stopped outside canti = %+v after %v; want its own handling of SIGTERM recorded as exit status 7", job, took)
	}
}

func TestAnOutOfMemoryKillIsFinalOnlyWhenRecorded(t *testing.T) {
	cfg, id := setup(t, docker.Config{Memory: "32m", PidsLimit: 128})
	ctx := testContext(t, 2*time.Minute)
	x := open(t, cfg)
	const hog = `awk 'BEGIN { s = "x"; while (1) s = s s }'`
	job := run(ctx, t, x, harness.Effect{ID: id("oom"), Command: "echo start; " + hog})
	if job.Status != harness.JobFailed || job.ExitCode == nil || *job.ExitCode != 137 || job.Preview != "start\n" {
		t.Fatalf("job killed out of memory = %+v, want failed with the recorded exit status 137", job)
	}
	survivor := harness.Effect{ID: id("survivor"), Command: hog + "; echo after $?; touch /workspace/survived; sleep 300"}
	bound, err := x.Bind(ctx, survivor)
	if err == nil {
		_, err = x.Start(ctx, survivor.ID)
	}
	if err != nil {
		t.Fatal(err)
	}
	appear(ctx, t, filepath.Join(cfg.Workspace, "survived"))
	kill(ctx, t, bound.ID)
	var state struct{ State struct{ OOMKilled bool } }
	if dockerInspect(t, bound.ID, &state); !state.State.OOMKilled {
		t.Fatal("Docker does not report the earlier out-of-memory kill")
	}
	const detail = noRecord + "; Docker observed an out-of-memory kill in the container"
	if job := inspect(ctx, t, open(t, cfg), survivor.ID); job.ID != bound.ID || job.Status != harness.JobUnknown || job.ExitCode != nil || job.Output.Detail != detail {
		t.Fatalf("job killed after surviving an out-of-memory kill = %+v, want unknown with detail %q", job, detail)
	}
}
