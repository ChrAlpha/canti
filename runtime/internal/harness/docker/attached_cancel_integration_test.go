//go:build integration

package docker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

type attachedCommandRecord struct {
	ExecID, Epoch, Launch string
	Generation            uint64
	Fenced                bool
}

func readAttachedCommandRecord(t *testing.T, control, effect string) attachedCommandRecord {
	t.Helper()
	var record attachedCommandRecord
	data, err := os.ReadFile(filepath.Join(control, "authority", "jobs", effect, "command.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func runAttachedControl(ctx context.Context, endpoint, id, helper, effect, spec string, generation uint64, epoch string, grace time.Duration, helperPID int) error {
	args := []string{"--host", endpoint, "exec", id, helper, "-terminate", "-effect", effect, "-spec", spec, "-generation", fmt.Sprint(generation), "-epoch", epoch, "-grace", grace.String(), "-helper-pid", fmt.Sprint(helperPID)}
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("control: %v: %s", err, out)
	}
	return nil
}

func TestAttachedDockerLiveWorkerBehindZombieLeaderIsKilled(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exec.LookPath("cc"); err != nil {
		t.Skip("cc is required for the live-thread fixture")
	}
	source := filepath.Join(t.TempDir(), "worker.c")
	if err := os.WriteFile(source, []byte(`
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
static void *worker(void *ignored) { for (;;) pause(); return 0; }
int main(void) {
	pthread_t child;
	signal(SIGTERM, SIG_IGN);
	if (pthread_create(&child, 0, worker, 0)) return 1;
	pthread_exit(0);
}
`), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.CommandContext(ctx, "cc", "-static", "-pthread", "-o", filepath.Join(workspace, "worker"), source).CombinedOutput(); err != nil {
		t.Fatalf("build worker: %s: %v", out, err)
	}
	startAttachedService(t, ctx, x)
	epoch := attachedEpoch(t, ctx, control)
	e := harness.Effect{ID: "worker_behind_zombie", Command: `./worker </dev/null >/dev/null 2>&1 & echo $! >/tmp/canti-worker; wait`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	command := `p=$(cat /tmp/canti-worker) && awk '{print $1,$3}' /proc/$p/task/*/stat`
	var before string
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		before, err = attachedDockerExec(ctx, endpoint, id, command)
		if err == nil && strings.Contains(before, " Z") && strings.Contains(before, " S") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(before, " Z") || !strings.Contains(before, " S") {
		t.Fatalf("fixture did not establish zombie leader and live thread: %q: %v", before, err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated || j.ExitCode != nil {
		t.Fatalf("cancel=%+v: %v", j, err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	after, err := attachedDockerExec(ctx, endpoint, id, command)
	if err == nil && strings.Contains(after, " S") {
		t.Fatalf("live worker survived the cancelled receipt: before=%q after=%q", before, after)
	}
}

func TestAttachedDockerCancelledJobKeepsItsPreview(t *testing.T) {
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
	e := harness.Effect{ID: "kept_preview", Command: `printf 'kept-preview'; sleep 300`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || !strings.Contains(j.Preview, "kept-preview") || !strings.Contains(j.Preview, "command was cancelled") || j.Output.Complete || !j.Output.Available || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated {
		t.Fatalf("cancelled preview = %+v: %v", j, err)
	}
}

func TestAttachedDockerCancelledLargeOutputIsPartial(t *testing.T) {
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
	e := harness.Effect{ID: "cancel_large", Command: `seq 3000; sleep 300`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || !j.Output.Available || j.Output.Complete || j.Output.Bytes != 4096 || j.Preview == "" || !strings.Contains(j.Preview, "command was cancelled") || !strings.Contains(j.Preview, "output is partial") {
		t.Fatalf("cancelled large output = %+v: %v", j, err)
	}
}

func TestAttachedDockerCancelledEmptyOutputHasEmptyPreview(t *testing.T) {
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
	e := harness.Effect{ID: "cancel_empty", Command: `sleep 300`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Preview != "" || !j.Output.Available || j.Output.Complete || j.Output.Bytes != 0 {
		t.Fatalf("cancelled empty output = %+v: %v", j, err)
	}
}

func TestAttachedDockerArtifactLossAfterProofKeepsTheCancelledReceipt(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	epoch := attachedEpoch(t, ctx, control)
	e := harness.Effect{ID: "missing_artifact", Command: `printf 'diagnostic'; echo $$ >/tmp/canti-artifact; sleep 300 & wait`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-artifact", 1)
	if err := os.Remove(filepath.Join(control, "spool", e.ID, "output")); err != nil {
		t.Fatal(err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated || j.ExitCode != nil || j.Output.Available || j.Preview != "" {
		t.Fatalf("artifact loss after proof = %+v: %v", j, err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	assertAttachedJobGoneAndServiceAlive(t, ctx, endpoint, id, "/tmp/canti-artifact", attachedServicePid)
}

func TestAttachedDockerTwoControlsConvergeAndALateControlHasNoTarget(t *testing.T) {
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
	e := harness.Effect{ID: "two_controls", Command: `trap '' TERM; echo $$ >/tmp/canti-two; while :; do sleep 0.05; done`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-two", 1)
	record := readAttachedCommandRecord(t, control, e.ID)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- runAttachedControl(ctx, endpoint, id, a.Helper, e.ID, e.Spec, record.Generation, record.Epoch, time.Second, 0)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Logf("a racing control lost the signal race: %v", err)
		}
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated {
		t.Fatalf("receipt after racing controls = %+v: %v", j, err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Gate != "open" || status.Active != "" {
		t.Fatalf("environment after racing controls = %+v: %v", status, err)
	}
	assertAttachedJobGoneAndServiceAlive(t, ctx, endpoint, id, "/tmp/canti-two", attachedServicePid)
	// A late control of the same generation has no live helper.
	if err := runAttachedControl(ctx, endpoint, id, a.Helper, e.ID, e.Spec, record.Generation, record.Epoch, time.Second, 0); err == nil {
		t.Fatal("late control succeeded without a live helper")
	}
	if out, err := attachedDockerExec(ctx, endpoint, id, `kill -0 "$(cat `+attachedServicePid+`)"`); err != nil {
		t.Fatalf("late control killed the service: %q: %v", out, err)
	}
}

func TestAttachedDockerForgedSpoolIdentityCannotKillAService(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	effect := "forged_identity"
	command := `p=$(cat ` + attachedServicePid + `); printf '{"Protocol":2,"PID":%s,"PGID":%s}' "$p" "$p" >/var/lib/canti/jobs/` + effect + `/process.json; printf '{"Protocol":2,"Outcome":"gone"}' >/var/lib/canti/jobs/` + effect + `/termination.json; sleep 300`
	e := harness.Effect{ID: effect, Command: command}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated {
		t.Fatalf("forged spool cancel = %+v: %v", j, err)
	}
	if out, err := attachedDockerExec(ctx, endpoint, id, `kill -0 "$(cat `+attachedServicePid+`)"`); err != nil {
		t.Fatalf("forged spool killed the service: %q: %v", out, err)
	}
}

func TestAttachedDockerCancellationKillsAProcessGroupThatForks(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	e := harness.Effect{ID: "forking_group", Command: `trap '' TERM; echo $$ >/tmp/canti-forking; while :; do sleep 30 & sleep 0.05; done`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-forking", 1)
	j, err := x.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled {
		t.Fatalf("forking-group cancellation = %+v: %v", j, err)
	}
	if j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated {
		t.Fatalf("forking-group cancellation = %+v", j.Cessation)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Gate != "open" {
		t.Fatalf("environment after forking-group cancellation = %+v: %v", status, err)
	}
	command := `g=$(cat /tmp/canti-forking); ps -eo pid,pgid,stat | awk -v g="$g" 'NR>1 && $2==g && $3 !~ /^Z/ {print; found=1} END {exit found?1:0}'`
	if out, err := attachedDockerExec(ctx, endpoint, id, command); err != nil {
		t.Fatalf("in-group process survived: %q: %v", out, err)
	}
}

type attachedCancelTask struct {
	Config docker.AttachedConfig
	Effect harness.Effect
}

func TestAttachedDockerCancelControllerChild(t *testing.T) {
	path := os.Getenv("CANTI_CANCEL_TASK")
	if path == "" {
		t.Skip("cancel controller child only")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var task attachedCancelTask
	if err := json.Unmarshal(data, &task); err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(task.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Cancel(context.Background(), task.Effect); err != nil {
		t.Fatal(err)
	}
}

func TestAttachedDockerFencedAmbiguousExitStaysUnknownWithoutStop(t *testing.T) {
	for _, code := range []string{"253", "254"} {
		t.Run(code, func(t *testing.T) {
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
			e := harness.Effect{ID: "ambiguous_fenced_" + code, Command: `trap '' TERM; while [ ! -e /workspace/canti-release ]; do sleep 0.02; done; exit ` + code}
			e.Spec = x.Specification(e.Command)
			if _, err := x.Drive(ctx, e); err != nil {
				t.Fatal(err)
			}
			type result struct {
				job harness.Job
				err error
			}
			finished := make(chan result, 1)
			go func() {
				job, err := x.Cancel(ctx, e)
				finished <- result{job, err}
			}()
			deadline := time.Now().Add(10 * time.Second)
			for {
				record := readAttachedCommandRecord(t, control, e.ID)
				if record.Fenced {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("cancellation never fenced")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := os.WriteFile(filepath.Join(workspace, "canti-release"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			got := <-finished
			if got.err != nil || got.job.Status != harness.JobUnknown || got.job.ExitCode != nil || got.job.Cessation != nil {
				t.Fatalf("fenced ambiguous %s = %+v: %v", code, got.job, got.err)
			}
			if got := attachedEpoch(t, ctx, control); got != epoch {
				t.Fatalf("environment epoch changed from %s to %s", epoch, got)
			}
			status, err := docker.ControlEnvironment(ctx, control, "inspect")
			if err != nil || !status.Running || status.Gate != "open" {
				t.Fatalf("environment after ambiguous exit = %+v: %v", status, err)
			}
			// An explicit environment stop must reconcile the ambiguous active
			// slot instead of leaving the gate quarantined.
			if _, err = docker.ControlEnvironment(ctx, control, "stop"); err != nil {
				t.Fatal(err)
			}
			started, err := docker.ControlEnvironment(ctx, control, "start")
			if err != nil || !started.Running || started.Gate != "open" || started.Active != "" {
				t.Fatalf("same-ID start after explicit stop = %+v: %v", started, err)
			}
		})
	}
}

func TestAttachedDockerNaturalExitAfterFenceWinsWithoutEnvironmentStop(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	epoch := attachedEpoch(t, ctx, control)
	e := harness.Effect{ID: "natural_after_fence", Command: `trap '' TERM; while [ ! -e /workspace/canti-release ]; do sleep 0.02; done; exit 42`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	type result struct {
		job harness.Job
		err error
	}
	finished := make(chan result, 1)
	go func() {
		job, err := x.Cancel(ctx, e)
		finished <- result{job, err}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		record := readAttachedCommandRecord(t, control, e.ID)
		if record.Fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancellation never fenced")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(filepath.Join(workspace, "canti-release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	got := <-finished
	if got.err != nil || got.job.Status != harness.JobFailed || got.job.Cessation != nil || got.job.ExitCode == nil || *got.job.ExitCode != 42 {
		t.Fatalf("natural exit after fence = %+v: %v", got.job, got.err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Gate != "open" || status.Active != "" {
		t.Fatalf("environment after natural exit = %+v: %v", status, err)
	}
	if out, err := attachedDockerExec(ctx, endpoint, id, `kill -0 "$(cat `+attachedServicePid+`)"`); err != nil {
		t.Fatalf("natural exit killed the service: %q: %v", out, err)
	}
}

func TestAttachedDockerControllerDeathDuringGraceConverges(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	cfg := docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 3 * time.Second}
	x, err := docker.NewAttached(cfg)
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	epoch := attachedEpoch(t, ctx, control)
	e := harness.Effect{ID: "controller_death", Command: `trap 'touch /workspace/canti-term' TERM; echo $$ >/tmp/canti-death; while :; do sleep 0.1; done`}
	e.Spec = x.Specification(e.Command)
	if _, err := x.Drive(ctx, e); err != nil {
		t.Fatal(err)
	}
	awaitAttachedPids(t, ctx, endpoint, id, "/tmp/canti-death", 1)
	task := filepath.Join(t.TempDir(), "task.json")
	raw, _ := json.Marshal(attachedCancelTask{cfg, e})
	if err := os.WriteFile(task, raw, 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttachedDockerCancelControllerChild$")
	child.Env = append(os.Environ(), "CANTI_CANCEL_TASK="+task)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(workspace, "canti-term")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("TERM was never delivered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	rebuilt, err := docker.NewAttached(cfg)
	if err != nil {
		t.Fatal(err)
	}
	j, err := rebuilt.Cancel(ctx, e)
	if err != nil || j.Status != harness.JobCancelled || j.Cessation == nil || j.Cessation.Kind != harness.CessationJobTerminated {
		t.Fatalf("recovered cancel = %+v: %v", j, err)
	}
	if got := attachedEpoch(t, ctx, control); got != epoch {
		t.Fatalf("environment epoch changed from %s to %s", epoch, got)
	}
	status, err := docker.ControlEnvironment(ctx, control, "inspect")
	if err != nil || !status.Running || status.Gate != "open" || status.Active != "" {
		t.Fatalf("environment after recovery = %+v: %v", status, err)
	}
	assertAttachedJobGoneAndServiceAlive(t, ctx, endpoint, id, "/tmp/canti-death", attachedServicePid)
}

// attachedCancelProxy forwards the Docker endpoint through a Unix socket so a
// test can hold one engine response while the daemon has already made it
// durable.
func attachedCancelProxy(t *testing.T, endpoint string, before func(*http.Request), after func(*http.Response) error) string {
	t.Helper()
	base := t.TempDir()
	sock := filepath.Join(base, "engine.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(endpoint, "unix://"))
	}}
	target, _ := url.Parse("http://docker")
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = transport
	proxy.ModifyResponse = after
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if before != nil {
			before(r)
		}
		proxy.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		transport.CloseIdleConnections()
	})
	return "unix://" + sock
}

func startAttachedCancelController(t *testing.T, ctx context.Context, cfg docker.AttachedConfig, e harness.Effect) *exec.Cmd {
	t.Helper()
	task := filepath.Join(t.TempDir(), "task.json")
	raw, err := json.Marshal(attachedCancelTask{cfg, e})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(task, raw, 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttachedDockerCancelControllerChild$")
	child.Env = append(os.Environ(), "CANTI_CANCEL_TASK="+task)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	return child
}

// TestAttachedDockerRecoveryAfterProofBeforeCommitKeepsCancellation models a
// cancellation controller dying after the daemon proved termination but before
// the host committed the receipt. The proof arrives before the job deadline;
// the restarted loop observes it only after the deadline. The durable request
// time, not the recovery time, must keep this a cancellation.
func TestAttachedDockerRecoveryAfterProofBeforeCommitKeepsCancellation(t *testing.T) {
	ctx, control, workspace, state, engine, id := prepareAttachedDocker(t, "0:0")
	var original atomic.Value
	var gate atomic.Bool
	seen := make(chan time.Time, 1)
	release := make(chan struct{})
	defer close(release)
	endpoint := attachedCancelProxy(t, engine, nil, func(res *http.Response) error {
		if !gate.Load() || !strings.HasSuffix(res.Request.URL.Path, "/exec/"+original.Load().(string)+"/json") {
			return nil
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		_ = res.Body.Close()
		res.Body = io.NopCloser(bytes.NewReader(body))
		var reply struct {
			Running  bool
			ExitCode *int
		}
		if json.Unmarshal(body, &reply) == nil && !reply.Running && reply.ExitCode != nil && *reply.ExitCode == 255 && gate.CompareAndSwap(true, false) {
			seen <- time.Now()
			<-release
			return res.Request.Context().Err()
		}
		return nil
	})
	a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	cfg := docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 300 * time.Millisecond}
	x, err := docker.NewAttached(cfg)
	if err != nil {
		t.Fatal(err)
	}
	startAttachedService(t, ctx, x)
	journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	const timeout = 4 * time.Second
	run := harness.Run{ID: "run_recovery_before_proof", Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
	initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	command := "echo $$ >/tmp/canti-recovery.pids; sleep 300 & wait"
	model := &attachedScriptedModel{exec: command}
	loopConfig := harness.LoopConfig{Environment: "attached", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond}
	loop, err := harness.NewLoop(journal, x, model, loopConfig)
	if err != nil {
		t.Fatal(err)
	}
	var effect harness.Effect
	for range 5 {
		snap, err := loop.Step(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase == core.PhaseEffectSaved {
			effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
			break
		}
	}
	if effect.ID == "" {
		t.Fatal("job was never accepted")
	}
	job, err := x.Drive(ctx, effect)
	if err != nil || job.DispatchedAt.IsZero() {
		t.Fatalf("job not launched: %+v %v", job, err)
	}
	deadline := job.DispatchedAt.Add(timeout)
	awaitAttachedPids(t, ctx, engine, id, "/tmp/canti-recovery.pids", 1)
	original.Store(readAttachedCommandRecord(t, control, effect.ID).ExecID)
	gate.Store(true)
	controller := startAttachedCancelController(t, ctx, cfg, effect)
	var observed time.Time
	select {
	case observed = <-seen:
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not report exit 255")
	}
	if !observed.Before(deadline) {
		t.Fatalf("fixture finished too slowly: observed at %v, budget deadline %v", observed, deadline)
	}
	if err := controller.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = controller.Wait()
	time.Sleep(time.Until(deadline) + 100*time.Millisecond)
	rebuilt, err := docker.NewAttached(cfg)
	if err != nil {
		t.Fatal(err)
	}
	loop, err = harness.NewLoop(journal, rebuilt, model, loopConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Step(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	steps, err := journal.Recent(ctx, run.ID, 0, 10)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	if got := steps[0].Observation.Execution; got != core.ExecutionCancelled {
		t.Fatalf("daemon confirmed exit 255 %v before the job deadline, controller was killed before accepting proof, restart after deadline recorded %s", deadline.Sub(observed), got)
	}
}

// TestAttachedDockerNaturalExitAtStopFallbackKeepsTheEnvironment reproduces
// Sol's fallback race: all controls fail, the fallback pauses after the third
// sample, and the original exec exits naturally before the stop. A final
// sample must settle the natural result instead of stopping the environment.
func TestAttachedDockerNaturalExitAtStopFallbackKeepsTheEnvironment(t *testing.T) {
	for _, code := range []string{"253", "0"} {
		t.Run(code, func(t *testing.T) {
			ctx, control, workspace, state, engine, id := prepareAttachedDocker(t, "0:0")
			var armed, hit, created atomic.Bool
			var original atomic.Value
			var failures, stops atomic.Int32
			reached := make(chan struct{})
			unblock := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(unblock) }) }
			defer release()
			endpoint := attachedCancelProxy(t, engine, func(r *http.Request) {
				if !armed.Load() {
					return
				}
				if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/containers/"+id+"/stop") {
					stops.Add(1)
				}
				if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/containers/"+id+"/json") && failures.Load() == 3 && !hit.Swap(true) {
					close(reached)
					<-unblock
				}
			}, func(res *http.Response) error {
				if !armed.Load() {
					return nil
				}
				if res.Request.Method == "POST" && strings.HasSuffix(res.Request.URL.Path, "/containers/"+id+"/exec") {
					created.Store(true)
					_ = res.Body.Close()
					res.StatusCode, res.Status = http.StatusInternalServerError, "500 Internal Server Error"
					res.Body = io.NopCloser(strings.NewReader("{}"))
					res.ContentLength = 2
					res.Header.Set("Content-Length", "2")
				}
				if strings.HasSuffix(res.Request.URL.Path, "/exec/"+original.Load().(string)+"/json") && created.Swap(false) {
					failures.Add(1)
				}
				return nil
			})
			a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
			if err != nil {
				t.Fatal(err)
			}
			cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
			if err != nil {
				t.Fatal(err)
			}
			x, err := docker.NewAttached(docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 300 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			startAttachedService(t, ctx, x)
			e := harness.Effect{ID: "exit_at_fallback_" + code, Command: `echo $$ >/tmp/canti-fallback.pids; while [ ! -e /workspace/canti-release ]; do sleep 0.02; done; exit ` + code}
			e.Spec = x.Specification(e.Command)
			if _, err = x.Drive(ctx, e); err != nil {
				t.Fatal(err)
			}
			awaitAttachedPids(t, ctx, engine, id, "/tmp/canti-fallback.pids", 1)
			original.Store(readAttachedCommandRecord(t, control, e.ID).ExecID)
			armed.Store(true)
			type outcome struct {
				job harness.Job
				err error
			}
			finished := make(chan outcome, 1)
			go func() {
				job, err := x.Cancel(ctx, e)
				finished <- outcome{job, err}
			}()
			select {
			case <-reached:
			case <-time.After(10 * time.Second):
				t.Fatal("the fallback never reached its final sample")
			}
			if err := os.WriteFile(filepath.Join(workspace, "canti-release"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			// The direct engine endpoint bypasses the paused proxy.
			tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(engine, "unix://"))
			}}
			client := &http.Client{Transport: tr, Timeout: 2 * time.Second}
			observed := false
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				resp, err := client.Get("http://docker/v1.47/exec/" + original.Load().(string) + "/json")
				if err != nil {
					t.Fatal(err)
				}
				var m struct {
					Running  bool
					ExitCode *int
				}
				err = json.NewDecoder(resp.Body).Decode(&m)
				_ = resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if !m.Running && m.ExitCode != nil && fmt.Sprint(*m.ExitCode) == code {
					observed = true
					break
				}
			}
			tr.CloseIdleConnections()
			if !observed {
				t.Fatal("engine never reported the natural exit")
			}
			release()
			got := <-finished
			if got.err != nil || stops.Load() != 0 {
				t.Fatalf("fallback stopped the environment: stops=%d job=%+v err=%v", stops.Load(), got.job, got.err)
			}
			if code == "0" {
				if got.job.Status != harness.JobSucceeded || got.job.ExitCode == nil || *got.job.ExitCode != 0 || got.job.Cessation != nil {
					t.Fatalf("natural zero = %+v", got.job)
				}
			} else if got.job.Status != harness.JobUnknown || got.job.Cessation != nil || !strings.Contains(got.job.Output.Detail, "unverified") {
				t.Fatalf("ambiguous natural exit = %+v", got.job)
			}
			status, err := docker.ControlEnvironment(ctx, control, "inspect")
			if err != nil || !status.Running || status.Gate != "open" {
				t.Fatalf("environment = %+v: %v", status, err)
			}
			if out, err := attachedDockerExec(ctx, engine, id, `kill -0 "$(cat `+attachedServicePid+`)"`); err != nil {
				t.Fatalf("service was stopped: %q: %v", out, err)
			}
		})
	}
}

// TestAttachedDockerCancelEffectUsesDurableRequestTime drives the real
// CancelEffect entry point on a real Docker job and SQLite journal. A request
// before the job deadline is a cancellation; one after it is a timeout, even
// though the caller chose to cancel. The recovery case must reach the same
// classification after the controller dies.
func TestAttachedDockerCancelEffectUsesDurableRequestTime(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after bool
		child bool
		want  core.ExecutionState
	}{
		{"before deadline", false, false, core.ExecutionCancelled},
		{"after deadline", true, false, core.ExecutionTimedOut},
		{"recovery after deadline", true, true, core.ExecutionTimedOut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, control, workspace, state, engine, id := prepareAttachedDocker(t, "0:0")
			a, err := docker.AttachEnvironment(ctx, control, engine, id, "")
			if err != nil {
				t.Fatal(err)
			}
			cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
			if err != nil {
				t.Fatal(err)
			}
			cfg := docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 3 * time.Second}
			x, err := docker.NewAttached(cfg)
			if err != nil {
				t.Fatal(err)
			}
			startAttachedService(t, ctx, x)
			journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			const timeout = 2 * time.Second
			run := harness.Run{ID: "run_cancel_classification", Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
			initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.CreateRun(ctx, run, initial); err != nil {
				t.Fatal(err)
			}
			command := "echo $$ >/tmp/canti-classify.pids; trap '' TERM; sleep 300 & wait"
			model := &attachedScriptedModel{exec: command}
			loopConfig := harness.LoopConfig{Environment: "attached", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond}
			loop, err := harness.NewLoop(journal, x, model, loopConfig)
			if err != nil {
				t.Fatal(err)
			}
			var effect harness.Effect
			for range 5 {
				snap, err := loop.Step(ctx, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if snap.State.Phase == core.PhaseEffectSaved {
					effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
					break
				}
			}
			if effect.ID == "" {
				t.Fatal("job was never accepted")
			}
			job, err := x.Drive(ctx, effect)
			if err != nil || job.DispatchedAt.IsZero() {
				t.Fatalf("job not launched: %+v %v", job, err)
			}
			awaitAttachedPids(t, ctx, engine, id, "/tmp/canti-classify.pids", 1)
			if tc.after {
				time.Sleep(time.Until(job.DispatchedAt.Add(timeout)) + 250*time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
			if tc.child {
				controller := startAttachedCancelController(t, ctx, cfg, effect)
				for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
					if readAttachedCommandRecord(t, control, effect.ID).Fenced {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("cancellation did not fence")
					}
				}
				if err := controller.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				_ = controller.Wait()
				rebuilt, err := docker.NewAttached(cfg)
				if err != nil {
					t.Fatal(err)
				}
				loop, err = harness.NewLoop(journal, rebuilt, model, loopConfig)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := loop.CancelEffect(ctx, run.ID); err != nil {
				t.Fatal(err)
			}
			steps, err := journal.Recent(ctx, run.ID, 0, 10)
			if err != nil || len(steps) != 1 {
				t.Fatalf("steps = %+v, %v", steps, err)
			}
			if got := steps[0].Observation.Execution; got != tc.want {
				t.Fatalf("cancel after deadline=%t recorded %s, want %s", tc.after, got, tc.want)
			}
			status, err := docker.ControlEnvironment(ctx, control, "inspect")
			if err != nil || !status.Running || status.Gate != "open" {
				t.Fatalf("environment = %+v: %v", status, err)
			}
			if out, err := attachedDockerExec(ctx, engine, id, `kill -0 "$(cat `+attachedServicePid+`)"`); err != nil {
				t.Fatalf("service was stopped: %q: %v", out, err)
			}
		})
	}
}

// TestEphemeralCancelEffectUsesDurableRequestTime proves the shared decision
// also covers an ephemeral receipt, which has no Cessation: the admission
// request time, not the later stop-grace completion, decides the outcome.
func TestEphemeralCancelEffectUsesDurableRequestTime(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after bool
		want  core.ExecutionState
	}{
		{"before deadline", false, core.ExecutionCancelled},
		{"after deadline", true, core.ExecutionTimedOut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t, 5*time.Minute)
			cfg, _ := setup(t, docker.Config{})
			x := open(t, cfg)
			journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			const timeout = 2 * time.Second
			run := harness.Run{ID: fmt.Sprintf("run_ephemeral_cancel_%t", tc.after), Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
			initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.CreateRun(ctx, run, initial); err != nil {
				t.Fatal(err)
			}
			command := "sleep 300"
			model := &attachedScriptedModel{exec: command}
			loopConfig := harness.LoopConfig{Environment: "ephemeral", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond}
			loop, err := harness.NewLoop(journal, x, model, loopConfig)
			if err != nil {
				t.Fatal(err)
			}
			var effect harness.Effect
			for range 5 {
				snap, err := loop.Step(ctx, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if snap.State.Phase == core.PhaseEffectSaved {
					effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
					break
				}
			}
			if effect.ID == "" {
				t.Fatal("job was never accepted")
			}
			job, err := x.Drive(ctx, effect)
			if err != nil || job.Status != harness.JobRunning {
				t.Fatalf("job not launched: %+v %v", job, err)
			}
			if tc.after {
				time.Sleep(timeout + 250*time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
			if _, err := loop.CancelEffect(ctx, run.ID); err != nil {
				t.Fatal(err)
			}
			steps, err := journal.Recent(ctx, run.ID, 0, 10)
			if err != nil || len(steps) != 1 {
				t.Fatalf("steps = %+v, %v", steps, err)
			}
			if got := steps[0].Observation.Execution; got != tc.want {
				t.Fatalf("cancel after deadline=%t recorded %s, want %s", tc.after, got, tc.want)
			}
		})
	}
}

func TestEphemeralQueuedCancelKeepsItsAdmissionTime(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	cfg, _ := setup(t, docker.Config{})
	x := open(t, cfg)
	journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	const timeout = 2 * time.Second
	run := harness.Run{ID: "run_queued_cancel", Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
	initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	command := "sleep 300"
	model := &attachedScriptedModel{exec: command}
	loop, err := harness.NewLoop(journal, delayedWait{Executor: x, delay: 1500 * time.Millisecond}, model, harness.LoopConfig{Environment: "ephemeral", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var effect harness.Effect
	for range 5 {
		snap, err := loop.Step(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase == core.PhaseEffectSaved {
			effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
			break
		}
	}
	if effect.ID == "" {
		t.Fatal("job was never accepted")
	}
	if job, err := x.Drive(ctx, effect); err != nil || job.Status != harness.JobRunning {
		t.Fatalf("job not launched: %+v %v", job, err)
	}
	type result struct {
		snap harness.Snapshot
		err  error
	}
	requested := make(chan result, 1)
	go func() {
		snap, err := loop.RequestCancelEffect(ctx, run.ID)
		requested <- result{snap, err}
	}()
	time.Sleep(timeout + 250*time.Millisecond)
	if _, err := loop.Step(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	got := <-requested
	if got.err != nil {
		t.Fatal(got.err)
	}
	steps, err := journal.Recent(ctx, run.ID, 0, 10)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	if got := steps[0].Observation.Execution; got != core.ExecutionCancelled {
		t.Fatalf("queued pre-deadline cancel recorded %s, want cancelled", got)
	}
}

type dockerCancelTask struct {
	Config   docker.Config
	Helper   string
	CancelAt time.Time
	Effect   harness.Effect
}

func TestDockerCancelControllerChild(t *testing.T) {
	path := os.Getenv("CANTI_DOCKER_CANCEL_TASK")
	if path == "" {
		t.Skip("docker cancel controller child only")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var task dockerCancelTask
	if err := json.Unmarshal(data, &task); err != nil {
		t.Fatal(err)
	}
	task.Config.Helper = task.Helper
	task.Effect.CancelRequestedAt = task.CancelAt
	x, err := docker.New(task.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Cancel(context.Background(), task.Effect); err != nil {
		t.Fatal(err)
	}
}

func TestEphemeralCrashDuringGraceKeepsAdmissionTime(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	cfg, _ := setup(t, docker.Config{StopGrace: 5 * time.Second})
	x := open(t, cfg)
	journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	const timeout = 2 * time.Second
	run := harness.Run{ID: "run_crash_during_grace", Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
	initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	command := `trap 'touch /workspace/canti-crash-term' TERM; while :; do sleep 0.05; done`
	model := &attachedScriptedModel{exec: command}
	loopConfig := harness.LoopConfig{Environment: "ephemeral", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond}
	loop, err := harness.NewLoop(journal, x, model, loopConfig)
	if err != nil {
		t.Fatal(err)
	}
	var effect harness.Effect
	for range 5 {
		snap, err := loop.Step(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase == core.PhaseEffectSaved {
			effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
			break
		}
	}
	if effect.ID == "" {
		t.Fatal("job was never accepted")
	}
	job, err := x.Drive(ctx, effect)
	if err != nil || job.Status != harness.JobRunning {
		t.Fatalf("job not launched: %+v %v", job, err)
	}
	deadline := job.StartedAt.Add(timeout)
	effect.CancelRequestedAt = job.StartedAt.Add(100 * time.Millisecond)
	if !effect.CancelRequestedAt.Before(deadline) {
		t.Fatalf("fixture request %v is not before deadline %v", effect.CancelRequestedAt, deadline)
	}
	task := filepath.Join(t.TempDir(), "task.json")
	raw, err := json.Marshal(dockerCancelTask{Config: cfg, Helper: cfg.Helper, CancelAt: effect.CancelRequestedAt, Effect: effect})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(task, raw, 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDockerCancelControllerChild$")
	child.Env = append(os.Environ(), "CANTI_DOCKER_CANCEL_TASK="+task)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	marker := filepath.Join(cfg.Workspace, "canti-crash-term")
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(end) {
			t.Fatal("TERM was never delivered")
		}
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	time.Sleep(time.Until(deadline) + 500*time.Millisecond)
	rebuilt, err := docker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	loop, err = harness.NewLoop(journal, rebuilt, model, loopConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Step(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	steps, err := journal.Recent(ctx, run.ID, 0, 10)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	if got := steps[0].Observation.Execution; got != core.ExecutionCancelled {
		t.Fatalf("crash during grace recorded %s, want cancelled", got)
	}
}

type delayedWait struct {
	harness.Executor
	delay time.Duration
}

func (d delayedWait) Wait(ctx context.Context, e harness.Effect) error {
	err := d.Executor.(harness.Waiter).Wait(ctx, e)
	time.Sleep(d.delay)
	return err
}

func TestEphemeralAwaitAdmissionKeepsRequestTime(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	cfg, _ := setup(t, docker.Config{})
	x := open(t, cfg)
	journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	const timeout = 1 * time.Second
	run := harness.Run{ID: "run_await_cancel", Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
	initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	command := "sleep 300"
	model := &attachedScriptedModel{exec: command}
	loop, err := harness.NewLoop(journal, delayedWait{Executor: x, delay: 1500 * time.Millisecond}, model, harness.LoopConfig{Environment: "ephemeral", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var effect harness.Effect
	for range 5 {
		snap, err := loop.Step(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase == core.PhaseEffectSaved {
			effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
			break
		}
	}
	if effect.ID == "" {
		t.Fatal("job was never accepted")
	}
	stepDone := make(chan error, 1)
	go func() { _, err := loop.Step(ctx, run.ID); stepDone <- err }()
	var started time.Time
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		job, err := x.Inspect(ctx, effect)
		if err == nil && job.Status == harness.JobRunning && !job.StartedAt.IsZero() {
			started = job.StartedAt
			break
		}
	}
	if started.IsZero() {
		t.Fatal("job did not start")
	}
	if time.Since(started) >= timeout {
		t.Fatal("job started too late for the pre-deadline fixture")
	}
	requested := make(chan error, 1)
	go func() {
		_, err := loop.RequestCancelEffect(ctx, run.ID)
		requested <- err
	}()
	for range 2 {
		select {
		case err := <-stepDone:
			if err != nil {
				t.Fatal(err)
			}
			stepDone = nil
		case err := <-requested:
			if err != nil {
				t.Fatal(err)
			}
			requested = nil
		case <-time.After(20 * time.Second):
			t.Fatal("cancel/step did not finish")
		}
	}
	steps, err := journal.Recent(ctx, run.ID, 0, 10)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	if got := steps[0].Observation.Execution; got != core.ExecutionCancelled {
		t.Fatalf("await pre-deadline cancel recorded %s, want cancelled", got)
	}
}

func TestAttachedQueuedCancelKeepsItsAdmissionTime(t *testing.T) {
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
	journal, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	const timeout = 2 * time.Second
	run := harness.Run{ID: "run_attached_queued_cancel", Goal: "run one command", Budget: harness.Budget{JobTimeout: timeout}, Created: time.Now().UTC(), Config: []byte(`{"scripted":true}`)}
	initial, err := core.NewState(run.ID, core.Limits{MaxSteps: 5, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	command := "echo $$ >/tmp/canti-queued.pids; sleep 300 & wait"
	model := &attachedScriptedModel{exec: command}
	loop, err := harness.NewLoop(journal, x, model, harness.LoopConfig{Environment: "attached", History: t.TempDir(), Inputs: t.TempDir(), Poll: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var effect harness.Effect
	for range 5 {
		snap, err := loop.Step(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.State.Phase == core.PhaseEffectSaved {
			effect = harness.Effect{ID: snap.State.EffectID, Command: command, Spec: x.Specification(command)}
			break
		}
	}
	if effect.ID == "" {
		t.Fatal("job was never accepted")
	}
	if job, err := x.Drive(ctx, effect); err != nil || job.Status != harness.JobRunning {
		t.Fatalf("job not launched: %+v %v", job, err)
	}
	requested := make(chan error, 1)
	go func() {
		_, err := loop.RequestCancelEffect(ctx, run.ID)
		requested <- err
	}()
	time.Sleep(timeout + 250*time.Millisecond)
	if _, err := loop.Step(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := <-requested; err != nil {
		t.Fatal(err)
	}
	steps, err := journal.Recent(ctx, run.ID, 0, 10)
	if err != nil || len(steps) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	if got := steps[0].Observation.Execution; got != core.ExecutionCancelled {
		t.Fatalf("attached queued pre-deadline cancel recorded %s, want cancelled", got)
	}
}

func TestAttachedDockerDestinationObstructionKeepsCaptureForRetry(t *testing.T) {
	for _, mode := range []string{"file", "read-only"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "read-only" && os.Geteuid() == 0 {
				t.Skip("root can create files in a read-only directory")
			}
			ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
			a, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
			if err != nil {
				t.Fatal(err)
			}
			cwd, err := docker.EnvironmentWorkdir(ctx, a, workspace, state, "")
			if err != nil {
				t.Fatal(err)
			}
			cfg := docker.AttachedConfig{Attachment: a, Workspace: workspace, State: state, Workdir: cwd, OutputLimit: 4096, PreviewLimit: 4096, StopGrace: 300 * time.Millisecond}
			x, err := docker.NewAttached(cfg)
			if err != nil {
				t.Fatal(err)
			}
			command := `printf 'recoverable synthetic output'; trap '' TERM; sleep 300`
			e := harness.Effect{ID: "destination_obstruction", Command: command}
			e.Spec = x.Specification(command)
			if _, err := x.Drive(ctx, e); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(state, "artifacts", e.ID)
			if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
				t.Fatal(err)
			}
			if mode == "file" {
				if err := os.WriteFile(destination, []byte("obstruction"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.MkdirAll(destination, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(destination, 0500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(destination, 0755) })
			}
			if _, err := x.Cancel(ctx, e); err == nil {
				t.Fatal("obstructed destination did not fail")
			}
			if mode == "file" {
				if err := os.Remove(destination); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Chmod(destination, 0755); err != nil {
				t.Fatal(err)
			}
			rebuilt, err := docker.NewAttached(cfg)
			if err != nil {
				t.Fatal(err)
			}
			job, err := rebuilt.Cancel(ctx, e)
			if err != nil || job.Status != harness.JobCancelled || !job.Output.Available || job.Output.Bytes != int64(len("recoverable synthetic output")) {
				t.Fatalf("retry after obstruction = %+v, %v", job, err)
			}
		})
	}
}
