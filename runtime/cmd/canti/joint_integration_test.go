//go:build integration

package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// TestJointOneExternalEffectAfterAHostCrashWithTwoClients requires that a
// job whose host is killed right after it started is observed once when two
// clients resume the run at once through the host process: one host serves
// them, one drives and the other attaches, both report the result, no bind
// or start is committed after the crash, and the job that started before the
// crash, in the one container it ran in, is the only one recorded.
func TestJointOneExternalEffectAfterAHostCrashWithTwoClients(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 2; echo once >> count"}, 2: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	if r := f.cli([]string{"CANTI_TEST_EXECUTOR_CRASH=running"}, "run", "-host", "auto", "-goal", "g", "-image", image); r.code != exitError {
		t.Fatalf("a client whose host was killed: %+v", r)
	}
	pre := f.history()
	job := effectOwner(t, f, pre[len(pre)-1].State.EffectID)
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != job || cs[0].state != "running" {
		t.Fatalf("after the crash the containers are %+v, want job %s running", cs, job)
	}
	_, first := f.start(nil, "resume", "-host", "auto")
	_, second := f.start(nil, "resume", "-host", "auto")
	for i, r := range []result{first(), second()} {
		if r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("resume %d: %+v", i+1, r)
		}
	}
	if pids := f.hostPIDs(); len(pids) != 2 {
		t.Fatalf("hosts %v: want the killed one and one serving both clients", pids)
	}
	h := f.history()
	if !extends(h, pre) || !slices.Equal(kinds(h[len(pre):]), slices.Concat([]string{"observed"}, stepKinds[:4])) {
		t.Fatalf("recovery must observe the original acceptance once: %q", kinds(h))
	}
	checkFile(t, filepath.Join(f.workspace, "count"), "once\n")
	if recs := checkReleased(t, f, h); len(recs) != 1 || recs[job].ID != job {
		t.Fatalf("records %+v, want only that of job %s", recs, job)
	}
	checkReplay(t, f.status().Run.ID, h)
}

// TestJointAnUnknownResultSurvivesCredentialedHostStarts requires that a
// foreground host holding the credential of a run's API and provider, which
// it continues by itself when it starts, records as unknown, exactly once and
// with no new bind, start, or container, the result of a job lost while its
// host was down, and that the next such host commits nothing to the run,
// reports it not drivable until it is resolved, and that the run then stays
// unresolved through a credential-free stop until an explicit resolution
// settles it.
func TestJointAnUnknownResultSurvivesCredentialedHostStarts(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 60"}, 2: {Kind: core.Return, Payload: "done"}})
	f.autoHost()
	if r := f.cli([]string{"CANTI_TEST_EXECUTOR_CRASH=running"}, "run", "-host", "auto", "-goal", "g", "-image", image); r.code != exitError {
		t.Fatalf("a client whose host was killed: %+v", r)
	}
	pre := f.history()
	job := effectOwner(t, f, pre[len(pre)-1].State.EffectID)
	if out, err := exec.Command("docker", "rm", "-f", job).CombinedOutput(); err != nil {
		t.Fatalf("docker rm: %v: %s", err, out)
	}
	foreground := func() func() result {
		cmd, wait := f.start([]string{"ANTHROPIC_BASE_URL=" + f.model.URL}, "host")
		return func() result { cmd.Process.Signal(syscall.SIGTERM); return wait() }
	}
	stop := foreground()
	waitFor(t, func() bool { return f.status().Phase == core.PhaseNeedsResolution })
	if r := stop(); r.code != exitOK || !strings.Contains(r.stderr, "binds its anthropic-messages credential to "+f.model.URL) {
		t.Fatalf("the first credentialed host: %+v", r)
	}
	h := f.history()
	added := h[len(pre):]
	if len(added) != 1 || added[0].Kind != "outcome_unknown" || added[0].To != core.PhaseNeedsResolution ||
		!strings.Contains(added[0].Facts.Failure, harness.ErrJobLost.Error()) {
		t.Fatalf("the first credentialed host committed %q: %+v", kinds(added), added)
	}
	if cs := effectContainers(t, f.state); len(cs) != 0 {
		t.Fatalf("containers %+v after the lost job, want none", cs)
	}

	stop = foreground()
	c := &hostrpc.Client{Path: f.socket()}
	waitFor(t, func() bool { _, err := c.Hello(context.Background()); return err == nil })
	time.Sleep(time.Second)
	v, err := c.View(context.Background(), "")
	if err != nil || v.Drivable || v.Driver != hostrpc.DriverIdle || !strings.Contains(v.Why, "resolve it") || v.Status != hostrpc.StatusNeedsResolution {
		t.Fatalf("the second credentialed host reports %+v, %v", v, err)
	}
	if r := stop(); r.code != exitOK {
		t.Fatalf("the second credentialed host: %+v", r)
	}
	if again := f.history(); !reflect.DeepEqual(again, h) {
		t.Fatalf("the second credentialed host committed %q", kinds(again[len(h):]))
	}

	if r := f.cli(nil, "run", "-host", "auto", "-goal", "other", "-image", image); r.code != exitError || !strings.Contains(r.stderr, "needs resolution") {
		t.Fatalf("a new input beside the unresolved run: %+v", r)
	}
	r := runCLI(t, nil, "stop", "-host", "auto", "-workspace", f.workspace, "-state", f.state)
	if r.code != exitResolution {
		t.Fatalf("a credential-free stop: %+v", r)
	}
	if after := f.history(); len(after) != len(h)+1 || after[len(h)].Kind != "stop_requested" || after[len(after)-1].To != core.PhaseNeedsResolution {
		t.Fatalf("the stop committed %q", kinds(after[len(h):]))
	}
	if r := f.cli(nil, "resolve", "-host", "auto", "-job"); r.code != exitError || !strings.Contains(r.stderr, harness.ErrJobLost.Error()) {
		t.Fatalf("resolve -job of a lost job: %+v", r)
	}
	if r := f.cli(nil, "resolve", "-host", "auto", "-execution", "cancelled", "-note", "verified cancellation"); r.code != exitStopped {
		t.Fatalf("resolve -execution: %+v", r)
	}
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
		t.Fatalf("after the resolution the run is %s %s", s.Phase, s.Reason)
	}
}
