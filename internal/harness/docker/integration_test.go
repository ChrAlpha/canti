//go:build integration

package docker_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/executortest"
)

func setup(t *testing.T, cfg docker.Config) (docker.Config, func(name string) string) {
	t.Helper()
	cfg.Image = cmp.Or(os.Getenv("CANTI_TEST_IMAGE"), "alpine:3.23")
	cfg.Workspace, cfg.State = t.TempDir(), t.TempDir()
	prefix := fmt.Sprintf("it%d", time.Now().UnixNano())
	t.Cleanup(func() {
		out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=canti.effect", "--filter", "name=canti-"+prefix).Output()
		if err != nil {
			t.Errorf("list test containers: %v", err)
			return
		}
		if ids := strings.Fields(string(out)); len(ids) > 0 {
			if out, err := exec.Command("docker", append([]string{"rm", "-f"}, ids...)...).CombinedOutput(); err != nil {
				t.Errorf("remove test containers: %v: %s", err, out)
			}
		}
	})
	return cfg, func(name string) string { return prefix + "-" + name }
}

func open(t *testing.T, cfg docker.Config) *docker.Executor {
	t.Helper()
	x, err := docker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func testContext(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

func inspect(ctx context.Context, t *testing.T, x harness.Executor, id string) harness.Job {
	t.Helper()
	job, found, err := x.Inspect(ctx, id)
	if err != nil || !found {
		t.Fatalf("Inspect(%s) = %+v, %v, %v", id, job, found, err)
	}
	return job
}

func await(ctx context.Context, t *testing.T, x harness.Executor, id string, done func(harness.Job) bool) harness.Job {
	t.Helper()
	for {
		if job := inspect(ctx, t, x, id); done(job) {
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", id, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func finished(job harness.Job) bool {
	return job.Status == harness.JobSucceeded || job.Status == harness.JobFailed || job.Status == harness.JobCancelled
}

func run(ctx context.Context, t *testing.T, x harness.Executor, e harness.Effect) harness.Job {
	t.Helper()
	if _, err := x.Bind(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Start(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	return await(ctx, t, x, e.ID, finished)
}

func dockerInspect(t *testing.T, id string, v any) {
	t.Helper()
	out, err := exec.Command("docker", "container", "inspect", id).Output()
	if err != nil {
		t.Fatal(err)
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(out, &raw); err != nil || len(raw) != 1 {
		t.Fatalf("docker inspect %s: %v", id, err)
	}
	if err := json.Unmarshal(raw[0], v); err != nil {
		t.Fatal(err)
	}
}

func containers(t *testing.T, effectID string) []string {
	t.Helper()
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=canti.effect="+effectID).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(string(out))
}

func TestExecutorPassesStartLatchCheck(t *testing.T) {
	cfg, id := setup(t, docker.Config{})
	f := executortest.Fixture{
		Open: func(context.Context) (harness.Executor, error) { return docker.New(cfg) },
		Effect: func(name string) harness.Effect {
			return harness.Effect{ID: id(name), Command: fmt.Sprintf("echo run >> %s/%s.runs; sleep 1", docker.WorkDir, name)}
		},
		Runs: func(_ context.Context, effectID string) (int, error) {
			b, err := os.ReadFile(filepath.Join(cfg.Workspace, strings.TrimPrefix(effectID, id(""))+".runs"))
			if errors.Is(err, fs.ErrNotExist) {
				return 0, nil
			}
			return strings.Count(string(b), "run\n"), err
		},
	}
	if err := executortest.Check(testContext(t, 3*time.Minute), f); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("docker", "ps", "-a", "--filter", "name=canti-"+id(""), "--format", "{{.Names}}").Output()
	if names := strings.Fields(string(out)); err != nil || !slices.Equal(slices.Sorted(slices.Values(names)), []string{"canti-" + id("bind"), "canti-" + id("changed-command")}) {
		t.Fatalf("containers after the check = %q, %v; want only the unstarted ones", names, err)
	}
}

func TestRestartedExecutorCollectsOriginalJob(t *testing.T) {
	cfg, id := setup(t, docker.Config{})
	ctx := testContext(t, time.Minute)
	e := harness.Effect{ID: id("late"), Command: "sleep 2; echo late"}
	x := open(t, cfg)
	bound, err := x.Bind(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Start(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	restarted := open(t, cfg)
	if job := inspect(ctx, t, restarted, e.ID); job.ID != bound.ID || job.Status != harness.JobRunning {
		t.Fatalf("restarted Inspect() = %+v, want running %s", job, bound.ID)
	}
	job := await(ctx, t, restarted, e.ID, finished)
	if job.ID != bound.ID || job.Status != harness.JobSucceeded || job.ExitCode == nil || *job.ExitCode != 0 || job.Preview != "late\n" {
		t.Fatalf("finished job = %+v", job)
	}
	if !job.FinishedAt.After(job.StartedAt) || job.StartedAt.IsZero() {
		t.Fatalf("job times %v..%v", job.StartedAt, job.FinishedAt)
	}
	want := harness.ArtifactRef{Path: e.ID + "/output", Bytes: 5, Complete: true, Available: true}
	if job.Output != want {
		t.Fatalf("output = %+v, want %+v", job.Output, want)
	}
	if b, err := os.ReadFile(filepath.Join(cfg.State, "artifacts", job.Output.Path)); err != nil || string(b) != "late\n" {
		t.Fatalf("artifact = %q, %v", b, err)
	}
	if ids := containers(t, e.ID); len(ids) != 0 {
		t.Fatalf("containers of the finished job = %q, want none", ids)
	}
	rebound, err := open(t, cfg).Bind(ctx, e)
	if err != nil || rebound.ID != job.ID || rebound.Status != job.Status || rebound.Preview != job.Preview || rebound.Output != want || len(containers(t, e.ID)) != 0 {
		t.Fatalf("Bind() after release = %+v, %v; want the recorded job and no container", rebound, err)
	}
	if got := inspect(ctx, t, open(t, cfg), e.ID); got.ID != job.ID || got.Status != job.Status || got.ExitCode == nil || *got.ExitCode != 0 ||
		got.Preview != job.Preview || got.Output != want || !got.StartedAt.Equal(job.StartedAt) || !got.FinishedAt.Equal(job.FinishedAt) {
		t.Fatalf("Inspect() after release = %+v, want %+v", got, job)
	}
	again, err := open(t, cfg).Start(ctx, e.ID)
	if err != nil || again.Status != harness.JobSucceeded || !again.StartedAt.Equal(job.StartedAt) {
		t.Fatalf("Start() after finish = %+v, %v; want the finished job unchanged", again, err)
	}
}

func TestRejectedStartIsRecordedAndReleased(t *testing.T) {
	cfg, id := setup(t, docker.Config{User: "nosuchuser"})
	ctx := testContext(t, time.Minute)
	e := harness.Effect{ID: id("rejected"), Command: "touch /workspace/ran"}
	x := open(t, cfg)
	bound, err := x.Bind(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	job, err := x.Start(ctx, e.ID)
	if err != nil || job.ID != bound.ID || job.Status != harness.JobFailedToStart || job.ExitCode != nil || !job.StartedAt.IsZero() ||
		!strings.HasPrefix(job.Output.Detail, "job never started: ") || !strings.Contains(job.Output.Detail, "nosuchuser") {
		t.Fatalf("Start(rejected) = %+v, %v; want the bound job failed to start with the daemon's reason", job, err)
	}
	if _, err := os.Stat(filepath.Join(cfg.State, "jobs", e.ID, "output.json")); err != nil {
		t.Fatalf("record of the failed start: %v", err)
	}
	if ids := containers(t, e.ID); len(ids) != 0 {
		t.Fatalf("containers of the failed start = %q, want none", ids)
	}
	if again, err := open(t, cfg).Start(ctx, e.ID); err != nil || again.ID != job.ID || again.Status != harness.JobFailedToStart {
		t.Fatalf("Start() again = %+v, %v; want the recorded job", again, err)
	}
	if got := inspect(ctx, t, open(t, cfg), e.ID); got.ID != job.ID || got.Status != harness.JobFailedToStart || got.Output != job.Output {
		t.Fatalf("Inspect() = %+v, want %+v", got, job)
	}
	if _, err := os.Stat(filepath.Join(cfg.Workspace, "ran")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the rejected job ran: %v", err)
	}
}

func TestBindReleasesAnEarlierJobWhoseRemovalFailed(t *testing.T) {
	cfg, name := setup(t, docker.Config{})
	real, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	failing := filepath.Join(t.TempDir(), "fail-rm")
	cfg.Docker = filepath.Join(t.TempDir(), "docker")
	shim := fmt.Sprintf("#!/bin/sh\n[ \"$1\" = rm ] && [ -e %q ] && exit 1\nexec %q \"$@\"\n", failing, real)
	if err := os.WriteFile(cfg.Docker, []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(failing, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	x := open(t, cfg)
	ctx := testContext(t, time.Minute)
	first := harness.Effect{ID: name("first"), Command: "echo first"}
	if job := run(ctx, t, x, first); job.Status != harness.JobSucceeded {
		t.Fatalf("first job = %+v", job)
	}
	if got := containers(t, first.ID); len(got) != 1 {
		t.Fatalf("containers of the first effect = %q, want the one whose removal failed", got)
	}
	if err := os.Remove(failing); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Bind(ctx, harness.Effect{ID: name("second"), Command: "echo second"}); err != nil {
		t.Fatal(err)
	}
	if got := containers(t, first.ID); len(got) != 0 {
		t.Fatalf("containers of the first effect = %q after the next bind, want none", got)
	}
}

func TestForeignStateCannotAdoptJob(t *testing.T) {
	cfg, id := setup(t, docker.Config{})
	ctx := testContext(t, time.Minute)
	e := harness.Effect{ID: id("foreign"), Command: "echo run >> /workspace/foreign.runs; until [ -e /workspace/release ]; do sleep 0.05; done"}
	x := open(t, cfg)
	if _, err := x.Bind(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Start(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	other := cfg
	other.State = t.TempDir()
	y := open(t, other)
	if _, err := y.Start(ctx, e.ID); err == nil {
		t.Fatal("Start() under another state succeeded")
	}
	if _, found, err := y.Inspect(ctx, e.ID); err == nil {
		t.Fatalf("Inspect() under another state = found %v, want an error", found)
	}
	if _, err := y.Bind(ctx, e); err == nil {
		t.Fatal("Bind() under another state succeeded")
	}
	if err := y.Cancel(ctx, e.ID); err != nil {
		t.Fatalf("Cancel() under another state = %v, want a fence", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Workspace, "release"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if job := await(ctx, t, open(t, cfg), e.ID, finished); job.Status != harness.JobSucceeded {
		t.Fatalf("original job = %+v, want it unchanged", job)
	}
	if b, err := os.ReadFile(filepath.Join(cfg.Workspace, "foreign.runs")); err != nil || string(b) != "run\n" {
		t.Fatalf("runs = %q, %v; want one", b, err)
	}
}

func TestBindNeverExecutes(t *testing.T) {
	cfg, id := setup(t, docker.Config{})
	ctx := testContext(t, time.Minute)
	e := harness.Effect{ID: id("bound"), Command: "touch /workspace/marker"}
	x := open(t, cfg)
	if job, err := x.Bind(ctx, e); err != nil || job.Status != harness.JobPending {
		t.Fatalf("Bind() = %+v, %v", job, err)
	}
	time.Sleep(time.Second)
	if _, err := os.Stat(filepath.Join(cfg.Workspace, "marker")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("bound job ran: %v", err)
	}
	if job := inspect(ctx, t, open(t, cfg), e.ID); job.Status != harness.JobPending {
		t.Fatalf("Inspect() = %+v, want pending", job)
	}
}

func TestOutputIsBoundedAndVerified(t *testing.T) {
	cfg, id := setup(t, docker.Config{OutputLimit: 1 << 20})
	ctx := testContext(t, 2*time.Minute)
	x := open(t, cfg)
	big := harness.Effect{ID: id("big"), Command: "echo START; yes 0123456789abcdefghijklmnopqrstuvwxyz | head -c 20971520; echo; echo END-TAIL"}
	total := len("START\n") + 20971520 + len("\n") + len("END-TAIL\n")
	bound, err := x.Bind(ctx, big)
	if err != nil {
		t.Fatal(err)
	}
	var info struct {
		HostConfig struct {
			LogConfig struct {
				Type   string
				Config map[string]string
			}
		}
	}
	dockerInspect(t, bound.ID, &info)
	if lc := info.HostConfig.LogConfig; lc.Type != "json-file" || !maps.Equal(lc.Config, map[string]string{"max-size": "1p", "mode": "blocking"}) {
		t.Fatalf("log config = %+v", lc)
	}
	job := run(ctx, t, x, big)
	if ids := containers(t, big.ID); len(ids) != 0 {
		t.Fatalf("containers of the flood job = %q, want its log released", ids)
	}
	if again := inspect(ctx, t, open(t, cfg), big.ID); again.ID != job.ID || again.Output != job.Output || again.Preview != job.Preview {
		t.Fatalf("Inspect() after release = %+v, want %+v", again, job)
	}
	out := job.Output
	if job.Status != harness.JobSucceeded || !out.Available || out.Complete || out.Bytes != 1<<20 || out.Detail != fmt.Sprintf("kept %d of %d bytes", 1<<20, total) {
		t.Fatalf("big job = %s %+v", job.Status, out)
	}
	b, err := os.ReadFile(filepath.Join(cfg.State, "artifacts", out.Path))
	if err != nil || len(b) != 1<<20 || !strings.HasPrefix(string(b), "START\n0123456789") {
		t.Fatalf("artifact has %d bytes: %v", len(b), err)
	}
	if p := job.Preview; len(p) > 4<<10 || !strings.HasPrefix(p, "START\n") || !strings.HasSuffix(p, "\nEND-TAIL\n") || !strings.Contains(p, " bytes omitted ...\n") {
		t.Fatalf("preview (%d bytes) = %q", len(p), p)
	}

	small := harness.Effect{ID: id("small"), Command: "echo small; echo err >&2; echo after; exit 3"}
	job = run(ctx, t, x, small)
	want := harness.ArtifactRef{Path: small.ID + "/output", Bytes: 16, Complete: true, Available: true}
	if job.Status != harness.JobFailed || job.ExitCode == nil || *job.ExitCode != 3 || job.Preview != "small\nerr\nafter\n" || job.Output != want {
		t.Fatalf("small job = %+v", job)
	}
	if err := os.Remove(filepath.Join(cfg.State, "artifacts", job.Output.Path)); err != nil {
		t.Fatal(err)
	}
	job = inspect(ctx, t, open(t, cfg), small.ID)
	if job.Status != harness.JobFailed || job.Output.Available || job.Output.Complete || !strings.Contains(job.Output.Detail, "artifact missing") || job.Preview != "small\nerr\nafter\n" {
		t.Fatalf("job with a deleted artifact = %+v", job)
	}
}

func TestCancelStopsProcessTree(t *testing.T) {
	const grace = 2 * time.Second
	for _, tt := range []struct {
		name, command string
		least         time.Duration
	}{
		{"terminates", "sleep 300 & sleep 300 & touch /workspace/began; wait", 0},
		{"ignores TERM", "trap '' TERM; sleep 300 & sleep 300 & touch /workspace/began; wait", grace},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, id := setup(t, docker.Config{StopGrace: grace})
			ctx := testContext(t, time.Minute)
			e := harness.Effect{ID: id("cancel"), Command: tt.command}
			x := open(t, cfg)
			if _, err := x.Bind(ctx, e); err != nil {
				t.Fatal(err)
			}
			if _, err := x.Start(ctx, e.ID); err != nil {
				t.Fatal(err)
			}
			for {
				if _, err := os.Stat(filepath.Join(cfg.Workspace, "began")); err == nil {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("job never began")
				}
				time.Sleep(20 * time.Millisecond)
			}
			began := time.Now()
			if err := open(t, cfg).Cancel(ctx, e.ID); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(began); took < tt.least || took > grace+5*time.Second {
				t.Fatalf("Cancel() took %v", took)
			}
			job := inspect(ctx, t, open(t, cfg), e.ID)
			if ids := containers(t, e.ID); len(ids) != 0 || job.Status != harness.JobCancelled || job.Output.Complete || !strings.Contains(job.Output.Detail, "cancelled") {
				t.Fatalf("cancelled job = %+v, containers %q", job, ids)
			}
			if err := open(t, cfg).Cancel(ctx, e.ID); err != nil {
				t.Fatalf("second Cancel() = %v", err)
			}
			if again, err := open(t, cfg).Start(ctx, e.ID); err != nil || again.Status != harness.JobCancelled || !again.StartedAt.Equal(job.StartedAt) {
				t.Fatalf("Start() after Cancel = %+v, %v", again, err)
			}
		})
	}
}

func TestTrustBoundary(t *testing.T) {
	t.Setenv("CANTI_TRUST_PROBE", "secret")
	config := t.TempDir()
	proxies := `{"proxies":{"default":{"httpProxy":"http://user:proxy-secret@proxy:3128","noProxy":"proxy-secret"}}}`
	if err := os.WriteFile(filepath.Join(config, "config.json"), []byte(proxies), 0o600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("DOCKER_HOST") == "" {
		host, err := exec.Command("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("DOCKER_HOST", strings.TrimSpace(string(host)))
	}
	t.Setenv("DOCKER_CONFIG", config)
	records := t.TempDir()
	if err := os.WriteFile(filepath.Join(records, "meta"), []byte("step: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, id := setup(t, docker.Config{Mounts: []docker.Mount{{Source: records, Target: "/canti/run"}}})
	ctx := testContext(t, time.Minute)
	script := fmt.Sprintf(`env
echo "uid=$(id -u):$(id -g)"
echo "net=$(ls /sys/class/net)"
grep -E '^(CapEff|NoNewPrivs):' /proc/self/status
touch %s/x 2>/dev/null && echo artifacts-writable
test -e %s && echo state-visible
cat /canti/run/meta
touch /canti/run/x 2>/dev/null && echo records-writable
echo ok > %s/trust && echo workspace-writable`, docker.ArtifactDir, cfg.State, docker.WorkDir)
	job := run(ctx, t, open(t, cfg), harness.Effect{ID: id("trust"), Command: script})
	b, err := os.ReadFile(filepath.Join(cfg.State, "artifacts", job.Output.Path))
	if err != nil || job.Status != harness.JobSucceeded || !job.Output.Complete {
		t.Fatalf("trust job = %+v, %v", job, err)
	}
	out := string(b)
	for _, want := range []string{fmt.Sprintf("uid=%d:%d\n", os.Getuid(), os.Getgid()), "net=lo\n", "CapEff:\t0000000000000000\n", "NoNewPrivs:\t1\n", "step: 1\n", "workspace-writable\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	for _, leak := range []string{"CANTI_TRUST_PROBE", "secret", "proxy:3128", "artifacts-writable", "records-writable", "state-visible"} {
		if strings.Contains(out, leak) {
			t.Errorf("output contains %q:\n%s", leak, out)
		}
	}
	if b, err := os.ReadFile(filepath.Join(cfg.Workspace, "trust")); err != nil || string(b) != "ok\n" {
		t.Errorf("workspace write = %q, %v", b, err)
	}
	for _, dir := range []string{filepath.Join(cfg.State, "artifacts"), records} {
		if _, err := os.Stat(filepath.Join(dir, "x")); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("job wrote the read-only mount of %s: %v", dir, err)
		}
	}
}
