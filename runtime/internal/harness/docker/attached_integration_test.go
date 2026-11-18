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
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
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
