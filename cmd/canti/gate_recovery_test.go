//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/host"
)

var stepKinds = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "observed"}

// released is a command that waits until the test creates the file release
// in the workspace.
const released = "until [ -e release ]; do sleep 0.1; done; "

func TestGateColdRestartAfterEveryStep(t *testing.T) {
	script := map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo x >> count; wc -l < count"},
		2: {Kind: core.Exec, Payload: "echo x >> count; wc -l < count"},
		3: {Kind: core.Return, Payload: "done"},
	}
	cold, image := recoveryFixture(t, script)
	intent := map[core.Phase][]string{
		core.PhaseIdle:          {"request_frozen"},
		core.PhaseRequestFrozen: {"attempt_started", "response_saved"},
		core.PhaseResponseSaved: {"response_loaded"},
		core.PhaseEffectSaved:   {"observed"},
	}
	var h []entry
	phase, processes := core.PhaseIdle, 0
	for r := cold.cli(nil, "run", "-once", "-goal", "count", "-image", image); ; r = cold.cli(nil, "resume", "-once") {
		processes++
		next := cold.history()
		added := next[min(len(h), len(next)):]
		if !extends(next, h) || len(added) == 0 || !slices.Equal(kinds(added), intent[phase]) || added[0].From != phase {
			t.Fatalf("process %d from %s committed %q\nstderr: %s", processes, phase, kinds(added), r.stderr)
		}
		h, phase = next, next[len(next)-1].To
		if r.code != exitActive {
			if r.code != exitOK || r.stdout != "done\n" || phase != core.PhaseReturned {
				t.Fatalf("process %d = %d in %s\nstdout: %s\nstderr: %s", processes, r.code, phase, r.stdout, r.stderr)
			}
			break
		}
	}
	if processes != 11 {
		t.Errorf("%d processes drove the run, want one per intent: 11", processes)
	}
	checkReplay(t, cold.status().Run.ID, h)

	warm, _ := recoveryFixture(t, script)
	if r := warm.cli(nil, "run", "-goal", "count", "-image", image); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("uninterrupted run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	w := warm.history()
	if len(w) != len(h) {
		t.Fatalf("uninterrupted history %q\ncold-restart history %q", kinds(w), kinds(h))
	}
	anonymous := func(s core.State) core.State {
		for _, id := range []*string{&s.RunID, &s.EffectID} {
			if *id != "" {
				*id = "set"
			}
		}
		return s
	}
	for i := range h {
		if h[i].Kind != w[i].Kind || h[i].From != w[i].From || h[i].To != w[i].To || anonymous(h[i].State) != anonymous(w[i].State) {
			t.Errorf("seq %d: cold restart %s %s->%s %+v\nuninterrupted %s %s->%s %+v",
				i+1, h[i].Kind, h[i].From, h[i].To, h[i].State, w[i].Kind, w[i].From, w[i].To, w[i].State)
		}
	}
	// exchanged returns the frozen request bytes of h, the bodies the model
	// received, and those bodies with the run ID and every effect ID replaced
	// by a placeholder and its step.
	exchanged := func(f *fixture, h []entry) (frozen, received, anonymous []string) {
		ids := []string{h[0].State.RunID, "run"}
		for _, e := range h {
			switch e.Kind {
			case "request_frozen":
				frozen = append(frozen, string(e.Facts.Request.Body))
			case "response_loaded":
				ids = append(ids, e.State.EffectID, fmt.Sprintf("effect-%d", e.State.Step))
			}
		}
		for _, body := range f.model.requests() {
			received = append(received, string(body))
			anonymous = append(anonymous, strings.NewReplacer(ids...).Replace(string(body)))
		}
		return frozen, received, anonymous
	}
	frozen, received, coldSent := exchanged(cold, h)
	_, _, warmSent := exchanged(warm, w)
	if len(frozen) != 3 || !slices.Equal(frozen, received) {
		t.Errorf("the model received other bytes than were frozen: %d frozen, %d received", len(frozen), len(received))
	}
	if !slices.Equal(coldSent, warmSent) {
		t.Errorf("cold-restart requests differ from the uninterrupted run's\ncold: %q\nwarm: %q", coldSent, warmSent)
	}
	if r := cold.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" || len(cold.model.requests()) != len(received) {
		t.Errorf("resume of the returned run = %d, %d requests\nstdout: %s\nstderr: %s", r.code, len(cold.model.requests()), r.stdout, r.stderr)
	}
	for _, f := range []*fixture{cold, warm} {
		checkFile(t, filepath.Join(f.workspace, "count"), "x\nx\n")
		checkReleased(t, f, f.history())
		checkRecords(t, f, "count", f.history())
	}
	evidence(t, "cold restart after every step", fmt.Sprintf("process exit after each of %d intents", processes), image, cold, h)
}

func TestGateCrashAtCommitBoundaries(t *testing.T) {
	script := map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo x >> count-1; echo one"},
		2: {Kind: core.Exec, Payload: "echo x >> count-2; sleep 1; echo two"},
		3: {Kind: core.Return, Payload: "done"},
	}
	// recovered resumes the crashed run of f and checks that it returns along
	// want without changing pre, sending the step 1 request sends times and
	// every other request once, and running and releasing each job once.
	recovered := func(t *testing.T, f *fixture, pre []entry, want []string, sends int) []entry {
		t.Helper()
		if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		h := f.history()
		if !extends(h, pre) {
			t.Errorf("confirmed transitions changed\nbefore: %+v\nafter: %+v", pre, h[:min(len(pre), len(h))])
		}
		if !slices.Equal(kinds(h), want) {
			t.Fatalf("history = %q\nwant %q", kinds(h), want)
		}
		requests := f.model.requests()
		if len(f.model.prompts(1)) != sends || len(requests) != sends+2 || !bytes.Equal(requests[0], requests[sends-1]) {
			t.Errorf("%d model requests, %d for step 1, want %d resending the frozen bytes", len(requests), len(f.model.prompts(1)), sends)
		}
		checkFile(t, filepath.Join(f.workspace, "count-1"), "x\n")
		checkFile(t, filepath.Join(f.workspace, "count-2"), "x\n")
		checkReleased(t, f, h)
		checkRecords(t, f, "count", h)
		checkReplay(t, f.status().Run.ID, h)
		return h
	}
	for k, kind := range stepKinds {
		for _, when := range []string{"before", "after"} {
			crash := when + ":" + kind
			t.Run(crash, func(t *testing.T) {
				t.Parallel()
				f, image := recoveryFixture(t, script)
				crashRun(t, f, crash, "run", "-goal", "count", "-image", image)
				pre := f.history()
				cut := k
				if when == "after" {
					cut++
				}
				// A committed attempt whose response was never saved is
				// counted as lost on resume and sent again.
				recounted := crash == "after:attempt_started" || crash == "before:response_saved"
				want := slices.Concat(stepKinds, stepKinds, stepKinds[:4])
				if recounted {
					want = slices.Insert(want, 2, "attempt_started")
				}
				if !slices.Equal(kinds(pre), want[:cut]) {
					t.Fatalf("history after the crash = %q, want %q", kinds(pre), want[:cut])
				}
				// The process killed before saving a response was answered.
				sends := 1
				if crash == "before:response_saved" {
					sends = 2
				}
				h := recovered(t, f, pre, want, sends)
				if when == "before" {
					at := cut
					if recounted {
						at++
					}
					step1 := kinds(h[:slices.Index(kinds(h), "observed")+1])
					n := 0
					for _, k := range step1 {
						if k == kind {
							n++
						}
					}
					if h[at].Kind != kind || n != 1 {
						t.Errorf("step 1 commits %s %d times, want once at seq %d: %q", kind, n, at+1, step1)
					}
				}
				evidence(t, "crash at commit boundaries", crash, image, f, h)
			})
		}
	}
	// A step is published after its observation is committed and before the
	// next request is frozen; a crash on either side of the publication
	// leaves it to the next process.
	t.Run("after:observed,before:request_frozen", func(t *testing.T) {
		t.Parallel()
		f, image := recoveryFixture(t, script)
		crashRun(t, f, "after:observed", "run", "-goal", "count", "-image", image)
		pre := f.history()
		if !slices.Equal(kinds(pre), stepKinds) {
			t.Fatalf("history after the crash = %q", kinds(pre))
		}
		one := filepath.Join(f.state, "history", pre[0].State.RunID, "steps", "00000001")
		if _, err := os.Stat(one); !os.IsNotExist(err) {
			t.Fatalf("step 1 records before the next request was prepared: %v", err)
		}
		crashRun(t, f, "before:request_frozen", "resume")
		if h := f.history(); !extends(h, pre) || len(h) != len(pre) {
			t.Fatalf("the killed resume committed %q", kinds(h[len(pre):]))
		}
		checkFile(t, filepath.Join(one, "command"), script[1].Payload)
		checkFile(t, filepath.Join(one, "preview"), pre[4].Facts.Observation.Preview)
		h := recovered(t, f, pre, slices.Concat(stepKinds, stepKinds, stepKinds[:4]), 1)
		evidence(t, "crash at commit boundaries", "after:observed, then before:request_frozen of step 2", image, f, h)
	})
	// The executor records a finished job before it releases the job's
	// container; a crash in between leaves both for the next report.
	t.Run("before:release", func(t *testing.T) {
		t.Parallel()
		f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: released + script[1].Payload}, 2: script[2], 3: script[3]})
		var pid atomic.Int64
		path := dockerProxy(t, func(_ http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodDelete {
				return false
			}
			if p := pid.Load(); p > 0 {
				syscall.Kill(int(p), syscall.SIGKILL)
			}
			<-r.Context().Done()
			return true
		})
		cmd, wait := f.start(path, "run", "-goal", "count", "-image", image)
		pid.Store(int64(cmd.Process.Pid))
		awaitRunningEffect(t, f)
		writeFile(t, filepath.Join(f.workspace, "release"))
		if r := wait(); !killed(cmd) {
			t.Fatalf("run = %d, not killed\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		pre := f.history()
		s := pre[len(pre)-1].State
		if !slices.Equal(kinds(pre), stepKinds[:4]) {
			t.Fatalf("history after the crash = %q", kinds(pre))
		}
		if rec, ok := readRecord(t, f, s.EffectID); !ok || rec.ID != effectOwner(t, f, s.EffectID) || rec.Status != harness.JobSucceeded || rec.Preview != "one\n" {
			t.Fatalf("record after the crash = %+v, %v; want job %s succeeded", rec, ok, effectOwner(t, f, s.EffectID))
		}
		if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != effectOwner(t, f, s.EffectID) || cs[0].state != "exited" {
			t.Fatalf("containers after the crash = %+v, want the recorded job %s exited", cs, effectOwner(t, f, s.EffectID))
		}
		h := recovered(t, f, pre, slices.Concat(stepKinds, stepKinds, stepKinds[:4]), 1)
		evidence(t, "crash at commit boundaries", "SIGKILL in the removal of the recorded step 1 job "+effectOwner(t, f, s.EffectID), image, f, h)
	})
}

func TestGateInFlightJobRecovery(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: released + "echo x >> count-1; echo late-output"},
		2: {Kind: core.Return, Payload: "done"},
	})
	crashRun(t, f, "executor:running", "run", "-goal", "wait", "-image", image)
	pre := f.history()
	s := pre[len(pre)-1].State
	if !slices.Equal(kinds(pre), stepKinds[:4]) || s.Phase != core.PhaseEffectSaved {
		t.Fatalf("history after the crash = %q", kinds(pre))
	}
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != effectOwner(t, f, s.EffectID) || cs[0].state != "running" {
		t.Fatalf("containers after the controller died = %+v, want job %s running", cs, effectOwner(t, f, s.EffectID))
	}
	_, wait := f.start(nil, "resume")
	writeFile(t, filepath.Join(f.workspace, "release"))
	if r := wait(); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	h := f.history()
	if !extends(h, pre) || !slices.Equal(kinds(h), slices.Concat(stepKinds, stepKinds[:4])) {
		t.Fatalf("history = %q", kinds(h))
	}
	obs := h[len(pre)].Facts.Observation
	if obs == nil || obs.Execution != core.ExecutionExited || obs.ExitCode == nil || *obs.ExitCode != 0 ||
		!strings.Contains(obs.Preview, "late-output") || obs.Output.Path != s.EffectID+"/output" || !obs.Output.Complete {
		t.Errorf("step 1 observation = %+v", obs)
	}
	if jobs := boundJobs(t, h); len(jobs) != 1 || jobs[s.EffectID] != effectOwner(t, f, s.EffectID) {
		t.Errorf("bound jobs = %v, want %s=%s", jobs, s.EffectID, effectOwner(t, f, s.EffectID))
	}
	if n := len(f.model.prompts(1)); n != 1 || len(f.model.requests()) != 2 {
		t.Errorf("%d model requests, %d for step 1", len(f.model.requests()), n)
	}
	checkFile(t, filepath.Join(f.workspace, "count-1"), "x\n")
	checkReleased(t, f, h)
	checkReplay(t, s.RunID, h)
	evidence(t, "in-flight job recovery", "executor:running while the job runs", image, f, h)
}

func TestGateUnknownOutcomeOfALostJob(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "sleep 60"},
		2: {Kind: core.Return, Payload: "done"},
	})
	crashRun(t, f, "executor:running", "run", "-goal", "lose", "-image", image)
	pre := f.history()
	s := pre[len(pre)-1].State
	if out, err := exec.Command("docker", "rm", "-f", effectOwner(t, f, s.EffectID)).CombinedOutput(); err != nil {
		t.Fatalf("docker rm: %v: %s", err, out)
	}
	if r := f.cli(nil, "resume"); r.code != exitResolution || !strings.Contains(r.stderr, "needs resolution") {
		t.Fatalf("resume = %d\nstderr: %s", r.code, r.stderr)
	}
	h := f.history()
	last := h[len(h)-1]
	if !extends(h, pre) || len(h) != len(pre)+1 || last.Kind != "outcome_unknown" || last.To != core.PhaseNeedsResolution ||
		!strings.Contains(last.Facts.Failure, harness.ErrJobLost.Error()) || !strings.Contains(last.Facts.Failure, s.EffectID) {
		t.Fatalf("history = %q, last %+v", kinds(h), last)
	}
	unchanged := func(what string) {
		t.Helper()
		if now := f.history(); !extends(now, h) || len(now) != len(h) {
			t.Errorf("%s changed the history: %q", what, kinds(now))
		}
		if cs, recs := effectContainers(t, f.state), records(t, f); len(cs) != 0 || len(recs) != 0 {
			t.Errorf("%s left containers %+v, records %q", what, cs, recs)
		}
		if n := len(f.model.requests()); n != 1 {
			t.Errorf("%s: %d model requests", what, n)
		}
	}
	if r := f.cli(nil, "resume"); r.code != exitResolution {
		t.Errorf("second resume = %d\nstderr: %s", r.code, r.stderr)
	}
	unchanged("a second resume")
	if r := f.cli(nil, "resolve", "-job"); r.code != exitError || !strings.Contains(r.stderr, harness.ErrJobLost.Error()) {
		t.Errorf("resolve -job = %d\nstderr: %s", r.code, r.stderr)
	}
	unchanged("resolve -job")

	for _, args := range [][]string{
		{"-execution", "exited", "-exit-code", "0", "-note", "verified"},
		{"-execution", "exited", "-exit-code", "0", "-owner", effectOwner(t, f, s.EffectID)},
	} {
		if r := f.cli(nil, "resolve", args...); r.code != exitUsage {
			t.Fatalf("missing operator provenance %q: %+v", args, r)
		}
		unchanged("rejected operator provenance")
	}
	if r := f.cli(nil, "resolve", "-execution", "exited", "-exit-code", "0", "-note", "verified", "-owner", effectOwner(t, f, s.EffectID)); r.code != exitOK {
		t.Fatalf("resolve -execution = %d\nstderr: %s", r.code, r.stderr)
	}
	h = f.history()
	last, zero := h[len(h)-1], 0
	want := harness.Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: "verified", Output: harness.ArtifactRef{Detail: "resolved by operator"}}
	if last.Kind != "resolved" || last.From != core.PhaseNeedsResolution || last.To != core.PhaseIdle || last.State.Step != 2 ||
		!reflect.DeepEqual(last.Facts.Observation, &want) || last.Facts.Resolution == nil || last.Facts.Resolution.Source != harness.ResolutionOperator || last.Facts.Resolution.OwnerID != effectOwner(t, f, s.EffectID) {
		t.Fatalf("resolution = %+v", last)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume after the resolution = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	final := f.history()
	if !extends(final, h) || !slices.Equal(kinds(final), slices.Concat(stepKinds[:4], []string{"outcome_unknown", "resolved"}, stepKinds[:4])) {
		t.Errorf("history = %q", kinds(final))
	}
	if p := f.model.prompts(2); len(p) != 1 || !strings.Contains(p[0], "set by explicit resolution") || !strings.Contains(p[0], "verified") {
		t.Errorf("step 2 prompts = %q", p)
	}
	if n := len(f.model.requests()); n != 2 {
		t.Errorf("%d model requests", n)
	}
	if cs, recs := effectContainers(t, f.state), records(t, f); len(cs) != 0 || len(recs) != 0 {
		t.Errorf("containers = %+v, records %q", cs, recs)
	}
	checkReplay(t, s.RunID, final)
	evidence(t, "unknown outcomes", "executor:running, then docker rm -f of job "+effectOwner(t, f, s.EffectID), image, f, final)
}

func TestGateUnknownOutcomeOfAnUnavailableStatus(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: released + "echo x >> count-1; echo original"},
		2: {Kind: core.Return, Payload: "done"},
	})
	unavailable := filepath.Join(t.TempDir(), "unavailable")
	path := dockerProxy(t, unavailableInspection(unavailable))

	_, wait := f.start(path, "run", "-goal", "wait", "-image", image)
	awaitRunningEffect(t, f)
	writeFile(t, unavailable)
	if r := wait(); r.code != exitResolution {
		t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
	}
	h := f.history()
	last := h[len(h)-1]
	if !slices.Equal(kinds(h), slices.Concat(stepKinds[:4], []string{"outcome_unknown"})) || last.To != core.PhaseNeedsResolution ||
		!strings.Contains(last.Facts.Failure, "docker inspect") || !strings.Contains(last.Facts.Failure, "job status unavailable") {
		t.Fatalf("history = %q, last %+v", kinds(h), last)
	}
	s := h[len(h)-2].State
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != effectOwner(t, f, s.EffectID) || cs[0].state != "running" {
		t.Fatalf("containers in needs_resolution = %+v, want job %s running", cs, effectOwner(t, f, s.EffectID))
	}
	if err := os.Remove(unavailable); err != nil {
		t.Fatal(err)
	}
	if r := f.cli(nil, "resolve", "-job"); r.code != exitOK {
		t.Fatalf("resolve -job = %d\nstderr: %s", r.code, r.stderr)
	}
	resolved := f.history()
	last = resolved[len(resolved)-1]
	if !extends(resolved, h) || len(resolved) != len(h)+1 || last.Kind != "effect_recovered" || last.From != core.PhaseNeedsResolution ||
		last.To != core.PhaseEffectSaved || last.Facts.Receipt == nil || last.Facts.Receipt.ID != effectOwner(t, f, s.EffectID) {
		t.Fatalf("resolution = %+v", last)
	}
	writeFile(t, filepath.Join(f.workspace, "release"))
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	final := f.history()
	if !extends(final, resolved) || !slices.Equal(kinds(final), slices.Concat(stepKinds[:4], []string{"outcome_unknown", "effect_recovered", "observed"}, stepKinds[:4])) {
		t.Fatalf("history = %q", kinds(final))
	}
	if obs := final[len(resolved)].Facts.Observation; obs == nil || obs.Execution != core.ExecutionExited || obs.Preview != "original\n" {
		t.Errorf("observation = %+v", obs)
	}
	checkFile(t, filepath.Join(f.workspace, "count-1"), "x\n")
	if n := len(f.model.requests()); n != 2 {
		t.Errorf("%d model requests", n)
	}
	checkReleased(t, f, final)
	checkReplay(t, s.RunID, final)
	evidence(t, "unknown outcomes", "container inspect unavailable in effect_saved", image, f, final)
}

func TestGateSingleWorkspaceWriterAndTakeover(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: released + "echo x >> count-1"},
		2: {Kind: core.Return, Payload: "done"},
	})
	writer, wait := f.start(nil, "run", "-goal", "write", "-image", image)
	awaitRunningEffect(t, f)
	h := f.history()
	unchanged := func(what string) {
		t.Helper()
		if now := f.history(); !extends(now, h) || len(now) != len(h) {
			t.Errorf("%s changed the history: %q", what, kinds(now))
		}
		if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].state != "running" {
			t.Errorf("containers after %s = %+v", what, cs)
		}
		if n := len(f.model.requests()); n != 1 {
			t.Errorf("%s: %d model requests", what, n)
		}
	}
	_, waitResume := f.start(nil, "resume")
	_, waitStop := f.start(nil, "stop")
	for c, r := range map[string]result{"resume": waitResume(), "stop": waitStop()} {
		if r.code != exitLocked || !strings.Contains(r.stderr, host.ErrLocked.Error()) {
			t.Errorf("concurrent %s = %d\nstderr: %s", c, r.code, r.stderr)
		}
	}
	unchanged("the refused writers")
	if err := writer.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if wait(); !killed(writer) {
		t.Fatal("the writer was not killed")
	}
	unchanged("killing the writer")
	_, wait = f.start(nil, "resume")
	writeFile(t, filepath.Join(f.workspace, "release"))
	if r := wait(); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("takeover = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	final := f.history()
	if !extends(final, h) || !slices.Equal(kinds(final), slices.Concat(stepKinds, stepKinds[:4])) || final[len(h)].From != core.PhaseEffectSaved {
		t.Fatalf("history = %q", kinds(final))
	}
	if n := len(f.model.prompts(1)); n != 1 || len(f.model.requests()) != 2 {
		t.Errorf("%d model requests, %d for step 1", len(f.model.requests()), n)
	}
	checkFile(t, filepath.Join(f.workspace, "count-1"), "x\n")
	checkReleased(t, f, final)
	checkReplay(t, h[0].State.RunID, final)
	evidence(t, "single workspace writer", "concurrent resume and stop, then SIGKILL of the writer in effect_saved", image, f, final)
}

// localSteps returns the transitions that perform a local action in h.
func localSteps(h []entry) []entry {
	return slices.DeleteFunc(slices.Clone(h), func(e entry) bool { return e.Kind != "performed" })
}

func TestGateCrashAtLocalActionCommits(t *testing.T) {
	list := core.Decision{Kind: core.Exec, Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"look"}`}
	handover := core.Decision{Kind: core.Exec, Payload: harness.HandoverPrefix + "Counted once in count. Next: report done."}
	count := core.Decision{Kind: core.Exec, Payload: "echo x >> count; echo one"}
	done := core.Decision{Kind: core.Return, Payload: "done"}
	local := []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "performed"}
	for _, tc := range []struct {
		name   string
		script map[int]core.Decision
		flags  []string
		want   []string
		kinds  []string
	}{
		{"retrieval", map[int]core.Decision{1: list, 2: done}, nil, slices.Concat(local, stepKinds[:4]), []string{"response_loaded", "performed"}},
		{"handover", map[int]core.Decision{1: count, 2: handover, 3: done}, []string{"-window-steps", "1"},
			slices.Concat(stepKinds, local, stepKinds[:4]), []string{"performed"}},
	} {
		for _, kind := range tc.kinds {
			for _, when := range []string{"before", "after"} {
				crash := when + ":" + kind
				t.Run(tc.name+"/"+crash, func(t *testing.T) {
					t.Parallel()
					f, image := recoveryFixture(t, tc.script)
					crashRun(t, f, crash, "run", append([]string{"-goal", "count", "-image", image}, tc.flags...)...)
					pre := f.history()
					// The crash hits the first commit of its kind.
					cut := slices.Index(tc.want, kind)
					if when == "after" {
						cut++
					}
					if !slices.Equal(kinds(pre), tc.want[:cut]) {
						t.Fatalf("history after the crash = %q, want %q", kinds(pre), tc.want[:cut])
					}
					if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
						t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
					}
					h := f.history()
					if !extends(h, pre) || !slices.Equal(kinds(h), tc.want) {
						t.Fatalf("history = %q\nwant %q extending the confirmed %q", kinds(h), tc.want, kinds(pre))
					}
					performed := localSteps(h)
					if len(performed) != 1 || performed[0].Facts.Outcome == nil || performed[0].Facts.Outcome.Status != harness.OutcomeDone {
						t.Fatalf("performed %+v, want one done local action", performed)
					}
					if tc.name == "handover" {
						if w := performed[0].Facts.Window; w == nil || w.FirstStep != 3 || w.SourceStep != 2 {
							t.Fatalf("the handover committed the window %+v", w)
						}
						if last := h[len(h)-4]; last.Facts.Request == nil || last.Facts.Request.Window == nil || !last.Facts.Request.Window.Equal(*performed[0].Facts.Window) {
							t.Fatalf("the next request renders %+v, not the committed window", last.Facts.Request)
						}
					}
					if requests := f.model.requests(); len(requests) != len(tc.script) {
						t.Errorf("%d model requests, want one per step", len(requests))
					}
					checkReleased(t, f, h)
					checkRecords(t, f, "count", h)
					checkReplay(t, f.status().Run.ID, h)
					evidence(t, "crash at local action commits", tc.name+" "+crash, image, f, h)
				})
			}
		}
	}
}

func TestGateColdRestartAcrossLocalActions(t *testing.T) {
	script := map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo x >> count; wc -l < count"},
		2: {Kind: core.Exec, Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"look"}`},
		3: {Kind: core.Exec, Payload: harness.HandoverPrefix + "Counted once in count. Next: report done."},
		4: {Kind: core.Return, Payload: "done"},
	}
	flags := []string{"-goal", "count", "-window-steps", "2"}
	cold, image := recoveryFixture(t, script)
	processes := 0
	for r := cold.cli(nil, "run", append([]string{"-once", "-image", image}, flags...)...); ; r = cold.cli(nil, "resume", "-once") {
		processes++
		if r.code == exitOK {
			if r.stdout != "done\n" {
				t.Fatalf("process %d stdout: %s", processes, r.stdout)
			}
			break
		}
		if r.code != exitActive || processes > 40 {
			t.Fatalf("process %d = %d\nstderr: %s", processes, r.code, r.stderr)
		}
	}
	if processes != 15 {
		t.Errorf("%d processes drove the run, want one per intent: 15", processes)
	}
	if r := cold.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume of the returned run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	h := cold.history()
	warm, _ := recoveryFixture(t, script)
	if r := warm.cli(nil, "run", append([]string{"-image", image}, flags...)...); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("uninterrupted run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	w := warm.history()
	anonymous := func(e entry) string {
		run := e.State.RunID
		s, _ := json.Marshal([]any{e.Kind, e.From, e.To, e.Facts.Action, e.Facts.Outcome, e.Facts.Window})
		return strings.ReplaceAll(string(s), run, "run")
	}
	if len(h) != len(w) || !slices.Equal(kinds(h), kinds(w)) {
		t.Fatalf("cold-restart history %q\nuninterrupted %q", kinds(h), kinds(w))
	}
	for i := range h {
		if anonymous(h[i]) != anonymous(w[i]) {
			t.Errorf("seq %d: cold restart %s\nuninterrupted %s", i+1, anonymous(h[i]), anonymous(w[i]))
		}
	}
	for _, f := range []*fixture{cold, warm} {
		jobs := checkReleased(t, f, f.history())
		if len(jobs) != 1 || len(localSteps(f.history())) != 2 {
			t.Errorf("%d jobs and %d performed steps, want one job for the command and two local steps", len(jobs), len(localSteps(f.history())))
		}
		checkFile(t, filepath.Join(f.workspace, "count"), "x\n")
		checkRecords(t, f, "count", f.history())
		checkReplay(t, f.status().Run.ID, f.history())
	}
	evidence(t, "cold restart across local actions", fmt.Sprintf("process exit after each of %d intents", processes), image, cold, h)
}
