//go:build integration

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestGateUnknownOutcomeOfAJobKilledOutsideCanti(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo x >> count; echo started; sleep 300"},
		2: {Kind: core.Return, Payload: "done"},
	})
	crashRun(t, f, "executor:running", "run", "-goal", "survive", "-image", image, "-memory", "512m", "-pids", "128")
	pre := f.history()
	s := pre[len(pre)-1].State
	if !slices.Equal(kinds(pre), stepKinds[:4]) || s.Phase != core.PhaseEffectSaved {
		t.Fatalf("history after the crash = %q", kinds(pre))
	}
	waitFor(t, func() bool {
		data, err := os.ReadFile(filepath.Join(f.workspace, "count"))
		return err == nil && string(data) == "x\n"
	})
	if out, err := exec.Command("docker", "kill", effectOwner(t, f, s.EffectID)).CombinedOutput(); err != nil {
		t.Fatalf("docker kill: %v: %s", err, out)
	}
	waitFor(t, func() bool {
		cs := effectContainers(t, f.state)
		return len(cs) == 1 && cs[0].state == "exited"
	})

	const detail = "no exit record at /tmp/.canti-exit: the process was killed before it could record, or the record could not be written"
	// unknown checks that a resume of f halts in needs_resolution with the
	// detail, after committing exactly one outcome_unknown, and keeps the
	// killed job without a record, a second job, or a second request.
	unknown := func(what string) []entry {
		t.Helper()
		before := f.history()
		if r := f.cli(nil, "resume"); r.code != exitResolution || !strings.Contains(r.stderr, "needs resolution") {
			t.Fatalf("%s: resume = %d\nstdout: %s\nstderr: %s", what, r.code, r.stdout, r.stderr)
		}
		h := f.history()
		last := h[len(h)-1]
		if !extends(h, before) || len(h) != len(before)+1 || last.Kind != "outcome_unknown" || last.To != core.PhaseNeedsResolution ||
			last.Facts.Observation != nil || last.Facts.Receipt == nil || last.Facts.Receipt.ID != effectOwner(t, f, s.EffectID) || last.Facts.Receipt.Status != harness.JobUnknown || !strings.Contains(last.Facts.Failure, detail) {
			t.Fatalf("%s: history = %q, last %+v", what, kinds(h), last)
		}
		if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != effectOwner(t, f, s.EffectID) || cs[0].state != "exited" {
			t.Errorf("%s: containers = %+v, want the killed job %s kept", what, cs, effectOwner(t, f, s.EffectID))
		}
		if recs, n := records(t, f), len(f.model.requests()); len(recs) != 0 || n != 1 {
			t.Errorf("%s: records %q, %d model requests; want none and one", what, recs, n)
		}
		if again := f.cli(nil, "resume"); again.code != exitResolution {
			t.Errorf("%s: second resume = %d\nstderr: %s", what, again.code, again.stderr)
		}
		if now := f.history(); !extends(now, h) || len(now) != len(h) {
			t.Errorf("%s: a second resume committed %q", what, kinds(now[len(h):]))
		}
		return h
	}
	h := unknown("after the kill")
	if r := f.cli(nil, "resolve", "-job"); r.code != exitError || !strings.Contains(r.stderr, s.EffectID) ||
		!strings.Contains(r.stderr, `"canti resolve -run `+s.RunID+` -execution STATE"`) {
		t.Fatalf("resolve -job of the killed job = %d\nstderr: %s", r.code, r.stderr)
	}
	if now := f.history(); !extends(now, h) || len(now) != len(h) {
		t.Fatalf("resolve -job of the killed job committed %q", kinds(now[len(h):]))
	}
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != effectOwner(t, f, s.EffectID) || cs[0].state != "exited" {
		t.Errorf("containers after resolve -job = %+v, want the killed job %s kept", cs, effectOwner(t, f, s.EffectID))
	}
	if r := f.cli(nil, "resolve", "-execution", "cancelled", "-note", "killed outside canti"); r.code != exitOK {
		t.Fatalf("resolve -execution = %d\nstderr: %s", r.code, r.stderr)
	}
	h = f.history()
	last := h[len(h)-1]
	want := harness.Observation{Execution: core.ExecutionCancelled, Preview: "killed outside canti", Output: harness.ArtifactRef{Detail: "resolved by operator"}}
	if last.Kind != "resolved" || last.To != core.PhaseIdle || last.State.Step != 2 || !reflect.DeepEqual(last.Facts.Observation, &want) {
		t.Fatalf("resolution = %+v", last)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume after the resolution = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	final := f.history()
	if !extends(final, h) || !slices.Equal(kinds(final), slices.Concat(stepKinds[:4],
		[]string{"outcome_unknown", "resolved"}, stepKinds[:4])) {
		t.Fatalf("history = %q", kinds(final))
	}
	if jobs := boundJobs(t, final); len(jobs) != 1 || jobs[s.EffectID] != effectOwner(t, f, s.EffectID) {
		t.Errorf("bound jobs = %v, want only %s=%s", jobs, s.EffectID, effectOwner(t, f, s.EffectID))
	}
	if p := f.model.prompts(2); len(p) != 1 || !strings.Contains(p[0], "set by explicit resolution") || !strings.Contains(p[0], "killed outside canti") {
		t.Errorf("step 2 prompts = %q", p)
	}
	if n, step1 := len(f.model.requests()), len(f.model.prompts(1)); n != 2 || step1 != 1 {
		t.Errorf("%d model requests, %d for step 1", n, step1)
	}
	if recs := records(t, f); len(recs) != 0 {
		t.Errorf("records = %q, want the unknown job never recorded", recs)
	}
	checkFile(t, filepath.Join(f.workspace, "count"), "x\n")
	checkReplay(t, s.RunID, final)
	evidence(t, "unknown outcomes", "executor:running, then docker kill of job "+effectOwner(t, f, s.EffectID), image, f, final)
}
