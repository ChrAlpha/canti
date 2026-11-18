//go:build integration

package docker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

func prepareAttachedDocker(t *testing.T, user string) (context.Context, string, string, string, string, string) {
	t.Helper()
	return prepareAttachedDockerMounts(t, user, nil)
}

func prepareAttachedDockerMounts(t *testing.T, user string, extra []string) (context.Context, string, string, string, string, string) {
	t.Helper()
	return prepareAttachedDockerMountsIn(t, user, t.TempDir(), extra)
}

func prepareAttachedDockerMountsIn(t *testing.T, user, workspace string, extra []string) (context.Context, string, string, string, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	control, state := t.TempDir(), t.TempDir()
	helper := filepath.Join(t.TempDir(), "canti-job")
	build := exec.CommandContext(ctx, "go", "build", "-o", helper, "github.com/ChrAlpha/canti/runtime/cmd/canti-job")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build capture helper: %v %s", err, out)
	}
	if err := docker.InitEnvironment(control, helper); err != nil {
		t.Fatal(err)
	}
	authorityID, err := docker.EnvironmentIdentity(control)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" {
		out, err := exec.CommandContext(ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
		if err != nil {
			t.Fatal(err)
		}
		endpoint = strings.TrimSpace(string(out))
	}
	if !strings.HasPrefix(endpoint, "unix://") {
		t.Skip("attached execution requires a local Unix Docker endpoint")
	}
	name := fmt.Sprintf("canti-attached-it-%d", time.Now().UnixNano())
	args := []string{"--host", endpoint, "run", "-d", "--name", name, "--label", "io.canti.control=" + control, "--label", "io.canti.authority=" + authorityID, "--label", "io.canti.test=persistent-environment", "--mount", "type=bind,src=" + filepath.Join(control, "public") + ",dst=/canti,readonly", "--mount", "type=bind,src=" + filepath.Join(control, "spool") + ",dst=/var/lib/canti/jobs", "--mount", "type=bind,src=" + workspace + ",dst=/workspace"}
	args = append(args, extra...)
	args = append(args, "--user", user, "--env", "HOME=/root", "--env", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "--init", "alpine:3.23", "/bin/sh", "-c", "trap 'exit 0' TERM INT; while :; do sleep 1; done")
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("prepare task environment: %v %s", err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() {
		if out, err := exec.Command("docker", "--host", endpoint, "rm", "-f", id).CombinedOutput(); err != nil {
			t.Errorf("remove task environment: %v %s", err, out)
		}
	})
	return ctx, control, workspace, state, endpoint, id
}

func TestAttachedDockerRetainsEnvironmentAcrossCommandsAndRestart(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runCommand := func(effect, command string) harness.Job {
		e := harness.Effect{ID: effect, Command: command, Spec: x.Specification(command)}
		if _, err := x.Drive(ctx, e); err != nil {
			t.Fatal(err)
		}
		for {
			j, err := x.Drive(ctx, e)
			if err != nil {
				t.Fatal(err)
			}
			switch j.Status {
			case harness.JobSucceeded, harness.JobFailed:
				return j
			case harness.JobUnknown:
				t.Fatalf("unknown command: %+v", j)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	first := runCommand("install", `printf '#!/bin/sh\nprintf persistent-tool' >/usr/local/bin/canti-test-tool; chmod +x /usr/local/bin/canti-test-tool; mkdir -p /root/.config/canti; printf private-config >/root/.config/canti/value; (while :; do printf tick >>/root/service.log; sleep 1; done) </dev/null >/root/service.stdout 2>&1 & echo $! >/root/service.pid; printf installed`)
	if first.Status != harness.JobSucceeded || first.Preview != "installed" {
		t.Fatalf("install=%+v", first)
	}
	second := runCommand("reuse", `kill -0 "$(cat /root/service.pid)" && canti-test-tool && cat /root/.config/canti/value`)
	if second.Preview != "persistent-toolprivate-config" {
		t.Fatalf("reuse=%+v", second)
	}
	before, err := os.ReadFile(filepath.Join(state, "artifacts", "install", "output"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != "installed" {
		t.Fatalf("background service changed published output: %q", before)
	}
	sleeping := harness.Effect{ID: "cancel", Command: "sleep 60"}
	sleeping.Spec = x.Specification(sleeping.Command)
	if _, err = x.Drive(ctx, sleeping); err != nil {
		t.Fatal(err)
	}
	stopped, err := x.Cancel(ctx, sleeping)
	if err != nil || stopped.Status != harness.JobCancelled {
		t.Fatalf("stop=%+v: %v", stopped, err)
	}
	if stopped.ExitCode == nil && stopped.Cessation == nil {
		t.Fatal("missing exit code without confirmed cessation")
	}
	if _, err = docker.ControlEnvironment(ctx, control, "start"); err != nil {
		t.Fatal(err)
	}
	after := runCommand("after_restart", `canti-test-tool; cat /root/.config/canti/value`)
	if after.Preview != "persistent-toolprivate-config" || !after.Output.Complete {
		t.Fatalf("restart lost filesystem: %+v", after)
	}
}

func TestAttachedDockerRejectsSymlinkBindSources(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" {
		out, err := exec.CommandContext(ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
		if err != nil {
			t.Fatal(err)
		}
		endpoint = strings.TrimSpace(string(out))
	}
	if !strings.HasPrefix(endpoint, "unix://") {
		t.Skip("attached execution requires a local Unix Docker endpoint")
	}
	for _, hidden := range []string{"authority", "state"} {
		t.Run(hidden, func(t *testing.T) {
			control, workspace, state := t.TempDir(), t.TempDir(), t.TempDir()
			helper := filepath.Join(t.TempDir(), "protocol-fixture")
			if err := os.WriteFile(helper, []byte(fmt.Sprintf("#!/bin/sh\necho %d\n", jobrunner.Protocol)), 0555); err != nil {
				t.Fatal(err)
			}
			if err := docker.InitEnvironment(control, helper); err != nil {
				t.Fatal(err)
			}
			authority, err := docker.EnvironmentIdentity(control)
			if err != nil {
				t.Fatal(err)
			}
			target := state
			if hidden == "authority" {
				target = filepath.Join(control, "authority")
			}
			alias := filepath.Join(t.TempDir(), "friendly-bind")
			if err = os.Symlink(target, alias); err != nil {
				t.Fatal(err)
			}
			args := []string{"--host", endpoint, "run", "-d", "--label", "io.canti.test=persistent-environment", "--label", "io.canti.control=" + control, "--label", "io.canti.authority=" + authority, "--mount", "type=bind,src=" + filepath.Join(control, "public") + ",dst=/canti,readonly", "--mount", "type=bind,src=" + filepath.Join(control, "spool") + ",dst=/var/lib/canti/jobs", "--mount", "type=bind,src=" + workspace + ",dst=/workspace", "--mount", "type=bind,src=" + alias + ",dst=/leak,readonly", "--user", "0:0", "--env", "HOME=/root", "--env", "PATH=/usr/bin:/bin", "alpine:3.23", "sleep", "60"}
			out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("prepare symlink bind: %v %s", err, out)
			}
			id := strings.TrimSpace(string(out))
			t.Cleanup(func() {
				if out, err := exec.Command("docker", "--host", endpoint, "rm", "-f", id).CombinedOutput(); err != nil {
					t.Errorf("remove symlink bind fixture: %v %s", err, out)
				}
			})
			if _, err = docker.AttachEnvironment(ctx, control, endpoint, id, ""); err == nil || !strings.Contains(err.Error(), "canonical host paths") {
				t.Fatalf("accepted actual Docker symlink bind: %v", err)
			}
			if _, err = os.Stat(filepath.Join(control, "authority", "environment.json")); !os.IsNotExist(err) {
				t.Fatalf("published unsafe attachment: %v", err)
			}
		})
	}
}

func waitAttachedDockerJob(t *testing.T, ctx context.Context, x *docker.Attached, e harness.Effect) harness.Job {
	t.Helper()
	for {
		j, err := x.Drive(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		switch j.Status {
		case harness.JobSucceeded, harness.JobFailed, harness.JobCancelled:
			return j
		case harness.JobUnknown:
			t.Fatalf("command became uncertain: %+v", j)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestAttachedDockerMissingWorkdirCompletesAndReleasesEnvironment(t *testing.T) {
	for _, missing := range []string{"requested-cwd", "workspace-after-admission"} {
		t.Run(missing, func(t *testing.T) {
			ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
			a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
			if err != nil {
				t.Fatal(err)
			}
			project := workspace
			if missing == "workspace-after-admission" {
				project = filepath.Join(workspace, "missing")
				if err := os.Mkdir(project, 0700); err != nil {
					t.Fatal(err)
				}
			}
			cwd, err := docker.EnvironmentWorkdir(ctx, a, project, state, "/workspace/missing")
			if err != nil {
				t.Fatal(err)
			}
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: project, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if missing == "workspace-after-admission" {
				if err := os.Remove(project); err != nil {
					t.Fatal(err)
				}
			}
			e := harness.Effect{ID: "missing_cwd", Command: "printf should-not-run"}
			e.Spec = x.Specification(e.Command)
			j := waitAttachedDockerJob(t, ctx, x, e)
			if j.Status != harness.JobFailed || j.ExitCode == nil || *j.ExitCode == 0 || !j.Output.Complete || !strings.Contains(j.Preview, "cd") || !strings.Contains(j.Preview, cwd) || strings.Contains(j.Preview, "should-not-run") {
				t.Fatalf("cwd failure lost ordinary completion/stderr: %+v", j)
			}
			status, err := docker.ControlEnvironment(ctx, control, "inspect")
			if err != nil || status.Active != "" || status.Gate != "open" || status.Unsettled != 0 {
				t.Fatalf("cwd failure retained ownership: %+v: %v", status, err)
			}
			good, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: "/workspace", OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			next := harness.Effect{ID: "after_missing_cwd", Command: "printf reused"}
			next.Spec = good.Specification(next.Command)
			if got := waitAttachedDockerJob(t, ctx, good, next); got.Status != harness.JobSucceeded || got.Preview != "reused" {
				t.Fatalf("environment unusable after cwd failure: %+v", got)
			}
		})
	}
}

func TestAttachedDockerNaturalExit255Reports255(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	e := harness.Effect{ID: "exit_255", Command: "exit 255"}
	e.Spec = x.Specification(e.Command)
	j := waitAttachedDockerJob(t, ctx, x, e)
	if j.Status != harness.JobFailed || j.ExitCode == nil || *j.ExitCode != 255 || j.Cessation != nil {
		t.Fatalf("natural exit 255 = %+v", j)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Active != "" {
		t.Fatalf("environment after exit tests = %+v: %v", status, err)
	}
}

func TestAttachedDockerAmbiguousNaturalExitIsUnverified(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	e := harness.Effect{ID: "exit_254", Command: "exit 254"}
	e.Spec = x.Specification(e.Command)
	var job harness.Job
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		job, err = x.Drive(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status != harness.JobRunning && job.Status != harness.JobPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ambiguous exit did not settle")
		}
	}
	if job.Status != harness.JobUnknown || job.ExitCode != nil || !strings.Contains(job.Output.Detail, "unverified") {
		t.Fatalf("ambiguous natural exit = %+v", job)
	}
}

func TestAttachedDockerStopsAfterCommandRemovesWorkspace(t *testing.T) {
	for _, operation := range []string{"cancel", "stop"} {
		t.Run(operation, func(t *testing.T) {
			ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
			project := filepath.Join(workspace, "project")
			if err := os.Mkdir(project, 0700); err != nil {
				t.Fatal(err)
			}
			a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
			if err != nil {
				t.Fatal(err)
			}
			cwd, err := docker.EnvironmentWorkdir(ctx, a, project, state, "")
			if err != nil {
				t.Fatal(err)
			}
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: project, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			e := harness.Effect{ID: "remove_workspace", Command: "rm -rf /workspace/project; printf ready > /workspace/removed; sleep 60"}
			e.Spec = x.Specification(e.Command)
			if _, err = x.Drive(ctx, e); err != nil {
				t.Fatal(err)
			}
			for {
				if _, err := os.Stat(filepath.Join(workspace, "removed")); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
			for _, observe := range []func(context.Context, harness.Effect) (harness.Job, error){x.Drive, x.Inspect} {
				if j, err := observe(ctx, e); err != nil || j.Status != harness.JobRunning {
					t.Fatalf("observe removed workspace: %+v: %v", j, err)
				}
			}
			if operation == "cancel" {
				_, err = x.Cancel(ctx, e)
			} else {
				_, err = docker.ControlEnvironment(ctx, control, "stop")
			}
			if err != nil {
				t.Fatal(err)
			}
			wantRunning, wantGate := "true", "open"
			if operation == "stop" {
				wantRunning, wantGate = "false", "stopped"
			}
			out, err := exec.CommandContext(ctx, "docker", "--host", endpoint, "inspect", "--format", "{{.State.Running}}", id).CombinedOutput()
			if err != nil || strings.TrimSpace(string(out)) != wantRunning {
				t.Fatalf("Docker container running = %q, want %q: %v", out, wantRunning, err)
			}
			j, err := x.Inspect(ctx, e)
			if err != nil || j.Status != harness.JobCancelled {
				t.Fatalf("cancel receipt after removal: %+v: %v", j, err)
			}
			if j.Cessation == nil {
				t.Fatalf("cessation after %s = %+v", operation, j.Cessation)
			}
			wantKind := harness.CessationJobTerminated
			if operation == "stop" {
				wantKind = harness.CessationStopped
			}
			if j.Cessation.Kind != wantKind {
				t.Fatalf("%s cessation kind = %q, want %q", operation, j.Cessation.Kind, wantKind)
			}
			status, err := docker.ControlEnvironment(ctx, control, "inspect")
			if err != nil || status.Active != "" || status.Unsettled != 0 || status.Gate != wantGate {
				t.Fatalf("operation retained slot/fence: %+v: %v", status, err)
			}
		})
	}
}

func TestAttachedDockerRejectsUserUnableToWritePrivateSpool(t *testing.T) {
	user := fmt.Sprintf("%d:%d", os.Getuid()+1, os.Getgid())
	ctx, control, _, _, endpoint, id := prepareAttachedDocker(t, user)
	// Establish actual DAC failure independently of the admission result.
	out, err := exec.CommandContext(ctx, "docker", "--host", endpoint, "exec", id, "/bin/sh", "-c", ": > /var/lib/canti/jobs/uid-probe").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "Permission denied") {
		t.Fatalf("UID mismatch fixture did not deny spool access: %q: %v", out, err)
	}
	if _, err := docker.AttachEnvironment(ctx, control, endpoint, id, ""); err == nil {
		t.Fatalf("admitted host UID %d/container %s despite demonstrated spool write denial", os.Getuid(), user)
	}
	if _, err := os.Stat(filepath.Join(control, "authority", "environment.json")); !os.IsNotExist(err) {
		t.Fatalf("published unusable attachment: %v", err)
	}
}

func TestAttachedDockerSupportedUserWritesCommandEvidence(t *testing.T) {
	for _, user := range []string{"0:0", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())} {
		t.Run(user, func(t *testing.T) {
			ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, user)
			a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
			if err != nil {
				t.Fatal(err)
			}
			cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
			if err != nil {
				t.Fatal(err)
			}
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			e := harness.Effect{ID: "uid_evidence", Command: "id -u"}
			e.Spec = x.Specification(e.Command)
			j := waitAttachedDockerJob(t, ctx, x, e)
			if j.Status != harness.JobSucceeded || strings.TrimSpace(j.Preview) != strings.Split(user, ":")[0] || !j.Output.Complete {
				t.Fatalf("supported UID lost helper evidence: %+v", j)
			}
			for _, name := range []string{"output", "completion.json"} {
				if data, err := os.ReadFile(filepath.Join(control, "spool", e.ID, name)); err != nil || len(data) == 0 {
					t.Fatalf("unreadable helper %s: %v", name, err)
				}
			}
		})
	}
}

func TestPersistentProjectBindingReadsANestedMount(t *testing.T) {
	overlay := t.TempDir()
	if err := os.WriteFile(filepath.Join(overlay, "AGENTS.md"), []byte("overlay rules\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "shadow"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "shadow", "AGENTS.md"), []byte("underlay rules\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "AGENTS.md"), []byte("root rules\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, control, _, state, endpoint, id := prepareAttachedDockerMountsIn(t, "0:0", workspace, []string{"--mount", "type=bind,src=" + overlay + ",dst=/workspace/shadow,readonly"})
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunConfig{Environment: control, Workspace: workspace, Attachment: &a, EnvironmentWorkdir: "/workspace/shadow", OutputLimit: 8 << 20}
	p, err := host.BindProject(ctx, state, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Instructions) != 2 || p.Instructions[0].Content != "root rules\n" || p.Instructions[1].Content != "overlay rules\n" {
		t.Fatalf("instructions = %+v", p.Instructions)
	}
}

func TestPersistentProjectBindingSkipsAResolvedSkillPathWithAControlCharacter(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "notes\nfile.md")
	if err := os.WriteFile(target, []byte("---\nname: linked\ndescription: d\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(workspace, ".agents/skills/linked")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(dir, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, filepath.Join(dir, "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	ctx, control, _, state, endpoint, id := prepareAttachedDockerMountsIn(t, "0:0", workspace, nil)
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunConfig{Environment: control, Workspace: workspace, Attachment: &a, EnvironmentWorkdir: "/workspace", OutputLimit: 8 << 20}
	p, err := host.BindProject(ctx, state, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Skills) != 0 || len(p.Skipped) != 1 || !strings.Contains(p.Skipped[0].Reason, "control character") {
		t.Fatalf("skills = %+v skipped = %+v", p.Skills, p.Skipped)
	}
}

func TestPersistentProjectReadRefusesAStoppedEnvironment(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDockerMounts(t, "0:0", nil)
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 8 << 20, PreviewLimit: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docker.ControlEnvironment(ctx, control, "stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := x.ReadProject(ctx, "/workspace", "/workspace"); err == nil {
		t.Fatal("read project context from a stopped environment")
	}
}

func TestPersistentProjectBindingReadsARegularFileBind(t *testing.T) {
	overlay := filepath.Join(t.TempDir(), "AGENTS.md")
	if err := os.WriteFile(overlay, []byte("file bind rules\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "AGENTS.md"), []byte("underlay rules\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, control, _, state, endpoint, id := prepareAttachedDockerMountsIn(t, "0:0", workspace, []string{"--mount", "type=bind,src=" + overlay + ",dst=/workspace/AGENTS.md,readonly"})
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunConfig{Environment: control, Workspace: workspace, Attachment: &a, EnvironmentWorkdir: "/workspace", OutputLimit: 8 << 20}
	p, err := host.BindProject(ctx, state, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Instructions) != 1 || p.Instructions[0].Content != "file bind rules\n" {
		t.Fatalf("instructions = %+v", p.Instructions)
	}
}

func TestPersistentProjectBindingFollowsALinkIntoANestedMount(t *testing.T) {
	overlay := t.TempDir()
	if err := os.WriteFile(filepath.Join(overlay, "AGENTS.md"), []byte("overlay through link\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "shadow"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "shadow", "AGENTS.md"), []byte("underlay through link\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("shadow/AGENTS.md", filepath.Join(workspace, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	ctx, control, _, state, endpoint, id := prepareAttachedDockerMountsIn(t, "0:0", workspace, []string{"--mount", "type=bind,src=" + overlay + ",dst=/workspace/shadow,readonly"})
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunConfig{Environment: control, Workspace: workspace, Attachment: &a, EnvironmentWorkdir: "/workspace", OutputLimit: 8 << 20}
	p, err := host.BindProject(ctx, state, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Instructions) != 1 || p.Instructions[0].Content != "overlay through link\n" {
		t.Fatalf("instructions = %+v", p.Instructions)
	}
}

func TestPersistentProjectBindingRefusesAnInstructionUnderATmpfsMount(t *testing.T) {
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "vol"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "vol", "AGENTS.md"), []byte("underlay\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, control, _, state, endpoint, id := prepareAttachedDockerMountsIn(t, "0:0", workspace, []string{"--mount", "type=tmpfs,dst=/workspace/vol"})
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunConfig{Environment: control, Workspace: workspace, Attachment: &a, EnvironmentWorkdir: "/workspace/vol", OutputLimit: 8 << 20}
	if _, err := host.BindProject(ctx, state, c); err == nil || !strings.Contains(err.Error(), "unreadable mount") {
		t.Fatalf("tmpfs project binding = %v", err)
	}
}

func TestPersistentProjectBindingBoundsLongPathPages(t *testing.T) {
	workspace := t.TempDir()
	workdir := filepath.Join(workspace, strings.Repeat("d", 200))
	for i := 0; i < 14; i++ {
		workdir = filepath.Join(workdir, strings.Repeat(string(rune('a'+i%26)), 200))
	}
	skills := filepath.Join(workdir, ".agents", "skills")
	for i := 0; i < 64; i++ {
		valid := filepath.Join(skills, fmt.Sprintf("v%02d", i))
		if err := os.MkdirAll(valid, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(valid, "SKILL.md"), []byte(fmt.Sprintf("---\nname: v%02d\ndescription: valid\n---\n", i)), 0o644); err != nil {
			t.Fatal(err)
		}
		invalid := filepath.Join(skills, fmt.Sprintf("i%02d", i))
		if err := os.MkdirAll(invalid, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(invalid, "SKILL.md"), []byte("---\nname: invalid\n---\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	visible := "/workspace" + strings.TrimPrefix(workdir, workspace)
	ctx, control, _, state, endpoint, id := prepareAttachedDockerMountsIn(t, "0:0", workspace, nil)
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	c := host.RunConfig{Environment: control, Workspace: workspace, Attachment: &a, EnvironmentWorkdir: visible, OutputLimit: 8 << 20}
	p, err := host.BindProject(ctx, state, c)
	if err != nil {
		t.Fatal(err)
	}
	if p.SkillsTotal != 64 || p.SkippedTotal != 64 || len(p.Skills) != 64 || len(p.Skipped) != 64 {
		t.Fatalf("pages = skills %d/%d skipped %d/%d", len(p.Skills), p.SkillsTotal, len(p.Skipped), p.SkippedTotal)
	}
	data, err := json.Marshal(p)
	if err != nil || len(data) > 4<<20 {
		t.Fatalf("encoded snapshot = %d, %v", len(data), err)
	}
}

const attachedServicePid = "/tmp/canti-service.pid"

func startAttachedService(t *testing.T, ctx context.Context, x *docker.Attached) {
	t.Helper()
	e := harness.Effect{ID: "service", Command: `setsid sh -c 'echo $$ > ` + attachedServicePid + `; exec sleep 600' </dev/null >/tmp/canti-service.log 2>&1 & printf started`}
	e.Spec = x.Specification(e.Command)
	if j := waitAttachedDockerJob(t, ctx, x, e); j.Status != harness.JobSucceeded {
		t.Fatalf("service launch = %+v", j)
	}
}

func attachedDockerExec(ctx context.Context, endpoint, id, command string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "--host", endpoint, "exec", id, "/bin/sh", "-c", command).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func awaitAttachedPids(t *testing.T, ctx context.Context, endpoint, id, path string, count int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, err := attachedDockerExec(ctx, endpoint, id, "wc -l < "+path)
		if err == nil {
			var lines int
			if _, scanErr := fmt.Sscan(out, &lines); scanErr == nil && lines >= count {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("job pid file %s has fewer than %d entries: %q: %v", path, count, out, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertAttachedJobGoneAndServiceAlive(t *testing.T, ctx context.Context, endpoint, id, pids, service string) {
	t.Helper()
	command := `for p in $(cat ` + pids + `); do if kill -0 "$p" 2>/dev/null; then echo "job process $p is alive"; exit 1; fi; done; kill -0 "$(cat ` + service + `)"`
	if out, err := attachedDockerExec(ctx, endpoint, id, command); err != nil {
		t.Fatalf("job cleanup/service retention failed: %q: %v", out, err)
	}
}

func attachedEpoch(t *testing.T, ctx context.Context, control string) string {
	t.Helper()
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil {
		t.Fatal(err)
	}
	return status.Epoch
}

// prepareAttachedDocker runs with --init, so tini reaps reparented children.
// The non-reaping PID 1 case, where a zombie of the job group remains, is
// covered by jobrunner.TestTerminationCountsZombiesAsGone.
func TestAttachedDockerCancellationKeepsServicesAndKillsTheJobGroup(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	epoch := attachedEpoch(t, ctx, control)
	e := harness.Effect{ID: "long_job", Command: "echo $$ >/tmp/canti-job.pids; sleep 300 & echo $! >>/tmp/canti-job.pids; sleep 300 & echo $! >>/tmp/canti-job.pids; wait"}
	e.Spec = x.Specification(e.Command)
	if _, err = x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-job.pids", 3)
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.ExitCode != nil || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated || !strings.Contains(j.Output.Detail, "environment") {
		t.Fatalf("cancel = %+v: %v", j, err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Gate != "open" || status.Active != "" {
		t.Fatalf("environment after cancellation = %+v: %v", status, err)
	}
	assertAttachedJobGoneAndServiceAlive(t, ctx, endpoint, id, "/tmp/canti-job.pids", attachedServicePid)
}

func TestAttachedDockerJobTimeoutKeepsServicesAndKillsTheJobGroup(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	epoch := attachedEpoch(t, ctx, control)
	journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	run := harness.Run{ID: "run_attached_timeout", Goal: "run one command", Budget: harness.Budget{JobTimeout: 2 * time.Second}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
	initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	model := &attachedScriptedModel{exec: "echo $$ >/tmp/canti-job.pids; sleep 300 & echo $! >>/tmp/canti-job.pids; sleep 300 & echo $! >>/tmp/canti-job.pids; wait"}
	loop, err := harness.NewLoop(journal, x, model, harness.LoopConfig{Environment: "attached", History: t.TempDir(), Inputs: t.TempDir(), Poll: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = loop.Run(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	steps, err := journal.Recent(ctx, run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].Step != 1 || steps[0].Observation.Execution != core.ExecutionTimedOut || steps[0].Observation.ExitCode != nil {
		t.Fatalf("timeout observation = %+v", steps)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Gate != "open" || status.Active != "" {
		t.Fatalf("environment after timeout = %+v: %v", status, err)
	}
	assertAttachedJobGoneAndServiceAlive(t, ctx, endpoint, id, "/tmp/canti-job.pids", attachedServicePid)
}

func TestAttachedDockerCancellationEscalatesAfterIgnoredTERM(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	epoch := attachedEpoch(t, ctx, control)
	e := harness.Effect{ID: "ignored_term", Command: "trap '' TERM; echo $$ >/tmp/canti-job.pids; while :; do sleep 1; done"}
	e.Spec = x.Specification(e.Command)
	if _, err = x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-job.pids", 1)
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.ExitCode != nil || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated {
		t.Fatalf("ignored-TERM cancel = %+v: %v", j, err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	if out, err := attachedDockerExec(ctx, endpoint, id, `for p in $(cat /tmp/canti-job.pids); do if kill -0 "$p" 2>/dev/null; then echo "job process $p is alive"; exit 1; fi; done`); err != nil {
		t.Fatalf("ignored-TERM job survived: %q: %v", out, err)
	}
}

func TestAttachedDockerCancellationFallsBackWhenTheHelperIsKilled(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	e := harness.Effect{ID: "helper_killed", Command: `printf '%s\n%s\n' "$PPID" "$$" >/tmp/canti-helper; sleep 300 & wait`}
	e.Spec = x.Specification(e.Command)
	if _, err = x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-helper", 2)
	if out, err := attachedDockerExec(ctx, endpoint, id, `kill -KILL "$(head -1 /tmp/canti-helper)" && kill -0 "$(tail -1 /tmp/canti-helper)"`); err != nil {
		t.Fatalf("kill capture helper: %q: %v", out, err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Cessation == nil || j.ExitCode != nil {
		t.Fatalf("helper-killed fallback = %+v: %v", j, err)
	}
	if out, err := exec.CommandContext(ctx, "docker", "--host", endpoint, "inspect", "--format", "{{.State.Running}}", id).CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "false" {
		t.Fatalf("fallback did not stop the environment: %q: %v", out, err)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || status.Running || status.Gate != "stopped" {
		t.Fatalf("environment after helper death = %+v: %v", status, err)
	}
}

type attachedScriptedModel struct {
	calls atomic.Int32
	exec  string
}

func (m *attachedScriptedModel) Freeze(harness.Prompt) (harness.Request, error) {
	return harness.Request{Body: []byte("scripted"), Model: "scripted", Protocol: "scripted/v1", Adapter: "scripted"}, nil
}

func (m *attachedScriptedModel) Complete(context.Context, harness.Request) (harness.Response, error) {
	decision := core.Decision{Kind: core.Return, Payload: "done"}
	if m.calls.Add(1) == 1 {
		decision = core.Decision{Kind: core.Exec, Payload: m.exec}
	}
	body, err := json.Marshal(decision)
	if err != nil {
		return harness.Response{}, err
	}
	return harness.Response{Body: body, Complete: true, Model: "scripted", Protocol: "scripted/v1", Adapter: "scripted"}, nil
}

func (m *attachedScriptedModel) Decision(response harness.Response) []byte { return response.Body }
