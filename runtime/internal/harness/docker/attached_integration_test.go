//go:build integration

package docker_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
)

func prepareAttachedDocker(t *testing.T, user string) (context.Context, string, string, string, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	control, workspace, state := t.TempDir(), t.TempDir(), t.TempDir()
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
	args := []string{"--host", endpoint, "run", "-d", "--name", name, "--label", "io.canti.control=" + control, "--label", "io.canti.authority=" + authorityID, "--label", "io.canti.test=persistent-environment", "--mount", "type=bind,src=" + filepath.Join(control, "public") + ",dst=/canti,readonly", "--mount", "type=bind,src=" + filepath.Join(control, "spool") + ",dst=/var/lib/canti/jobs", "--mount", "type=bind,src=" + workspace + ",dst=/workspace", "--user", user, "--env", "HOME=/root", "--env", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "--init", "alpine:3.23", "/bin/sh", "-c", "trap 'exit 0' TERM INT; while :; do sleep 1; done"}
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
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 1024, StopGrace: time.Second})
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
			if err := os.WriteFile(helper, []byte("#!/bin/sh\necho 1\n"), 0555); err != nil {
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
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: project, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 1024, StopGrace: time.Second})
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
			good, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: "/workspace", OutputLimit: 4096, PreviewLimit: 1024, StopGrace: time.Second})
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
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: project, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 1024, StopGrace: time.Second})
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
			out, err := exec.CommandContext(ctx, "docker", "--host", endpoint, "inspect", "--format", "{{.State.Running}}", id).CombinedOutput()
			if err != nil || strings.TrimSpace(string(out)) != "false" {
				t.Fatalf("Docker container was not stopped: %q: %v", out, err)
			}
			j, err := x.Inspect(ctx, e)
			if err != nil || j.Status != harness.JobCancelled {
				t.Fatalf("cancel receipt after removal: %+v: %v", j, err)
			}
			status, err := docker.ControlEnvironment(ctx, control, "inspect")
			if err != nil || status.Active != "" || status.Unsettled != 0 || status.Gate != "stopped" {
				t.Fatalf("stop retained slot/fence: %+v: %v", status, err)
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
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 1024, StopGrace: time.Second})
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
