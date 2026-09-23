//go:build integration

package main

import (
	"bytes"
	"fmt"
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

var stepKinds = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "job_bound", "job_started", "observed"}

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
		core.PhaseIdle:                {"request_frozen"},
		core.PhaseRequestFrozen:       {"attempt_started", "response_saved"},
		core.PhaseResponseSaved:       {"response_loaded"},
		core.PhaseEffectSaved:         {"job_bound"},
		core.PhaseJobBound:            {"job_started"},
		core.PhaseAwaitingObservation: {"observed"},
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
	if processes != 15 {
		t.Errorf("%d processes drove the run, want one per intent: 15", processes)
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
		for _, id := range []*string{&s.RunID, &s.EffectID, &s.JobID} {
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
	// received, and those bodies with every effect ID replaced by its step.
	exchanged := func(f *fixture, h []entry) (frozen, received, anonymous []string) {
		var ids []string
		for _, e := range h {
			switch e.Kind {
			case "request_frozen":
				frozen = append(frozen, string(e.Facts.Request.Body))
			case "job_bound":
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
	for _, f := range []*fixture{cold, warm} {
		checkFile(t, filepath.Join(f.workspace, "count"), "x\nx\n")
		checkReleased(t, f, f.history())
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
	// The executor records a finished job before it releases the job's
	// container; a crash in between leaves both for the next report.
	t.Run("before:release", func(t *testing.T) {
		t.Parallel()
		f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: released + script[1].Payload}, 2: script[2], 3: script[3]})
		shim := dockerShim(t, "if [ \"$1\" = rm ]; then\n\tkill -KILL \"$PPID\"\n\twhile kill -0 \"$PPID\" 2>/dev/null; do sleep 0.05; done\n\texit 1\nfi")
		cmd, wait := f.start(shim, "run", "-goal", "count", "-image", image)
		awaitPhase(t, f, core.PhaseAwaitingObservation)
		writeFile(t, filepath.Join(f.workspace, "release"))
		if r := wait(); !killed(cmd) {
			t.Fatalf("run = %d, not killed\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		pre := f.history()
		s := pre[len(pre)-1].State
		if !slices.Equal(kinds(pre), stepKinds[:6]) {
			t.Fatalf("history after the crash = %q", kinds(pre))
		}
		if rec, ok := readRecord(t, f, s.EffectID); !ok || rec.ID != s.JobID || rec.Status != harness.JobSucceeded || rec.Preview != "one\n" {
			t.Fatalf("record after the crash = %+v, %v; want job %s succeeded", rec, ok, s.JobID)
		}
		if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != s.JobID || cs[0].state != "exited" {
			t.Fatalf("containers after the crash = %+v, want the recorded job %s exited", cs, s.JobID)
		}
		h := recovered(t, f, pre, slices.Concat(stepKinds, stepKinds, stepKinds[:4]), 1)
		evidence(t, "crash at commit boundaries", "SIGKILL in docker rm of the recorded step 1 job "+s.JobID, image, f, h)
	})
}

func TestGateInFlightJobRecovery(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: released + "echo x >> count-1; echo late-output"},
		2: {Kind: core.Return, Payload: "done"},
	})
	crashRun(t, f, "after:job_started", "run", "-goal", "wait", "-image", image)
	pre := f.history()
	s := pre[len(pre)-1].State
	if !slices.Equal(kinds(pre), stepKinds[:6]) || s.Phase != core.PhaseAwaitingObservation {
		t.Fatalf("history after the crash = %q", kinds(pre))
	}
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != s.JobID || cs[0].state != "running" {
		t.Fatalf("containers after the controller died = %+v, want job %s running", cs, s.JobID)
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
	if jobs := boundJobs(t, h); len(jobs) != 1 || jobs[s.EffectID] != s.JobID {
		t.Errorf("bound jobs = %v, want %s=%s", jobs, s.EffectID, s.JobID)
	}
	if n := len(f.model.prompts(1)); n != 1 || len(f.model.requests()) != 2 {
		t.Errorf("%d model requests, %d for step 1", len(f.model.requests()), n)
	}
	checkFile(t, filepath.Join(f.workspace, "count-1"), "x\n")
	checkReleased(t, f, h)
	checkReplay(t, s.RunID, h)
	evidence(t, "in-flight job recovery", "after:job_started while the job runs", image, f, h)
}

func TestGateUnknownOutcomeOfALostJob(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "sleep 60"},
		2: {Kind: core.Return, Payload: "done"},
	})
	crashRun(t, f, "after:job_started", "run", "-goal", "lose", "-image", image)
	pre := f.history()
	s := pre[len(pre)-1].State
	if out, err := exec.Command("docker", "rm", "-f", s.JobID).CombinedOutput(); err != nil {
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
	if r := f.cli(nil, "resolve", "-execution", "exited", "-exit-code", "0", "-note", "verified"); r.code != exitOK {
		t.Fatalf("resolve -execution = %d\nstderr: %s", r.code, r.stderr)
	}
	h = f.history()
	last, zero := h[len(h)-1], 0
	want := harness.Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: "verified", Output: harness.ArtifactRef{Detail: "resolved by operator"}}
	if last.Kind != "resolved" || last.From != core.PhaseNeedsResolution || last.To != core.PhaseIdle || last.State.Step != 2 ||
		!reflect.DeepEqual(last.Facts.Observation, &want) {
		t.Fatalf("resolution = %+v", last)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume after the resolution = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	final := f.history()
	if !extends(final, h) || !slices.Equal(kinds(final), slices.Concat(stepKinds[:6], []string{"outcome_unknown", "resolved"}, stepKinds[:4])) {
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
	evidence(t, "unknown outcomes", "after:job_started, then docker rm -f of job "+s.JobID, image, f, final)
}

func TestGateUnknownOutcomeOfAnUnavailableStatus(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: released + "echo x >> count-1; echo original"},
		2: {Kind: core.Return, Payload: "done"},
	})
	unavailable := filepath.Join(t.TempDir(), "unavailable")
	path := dockerShim(t, fmt.Sprintf("if [ \"$1 $2\" = 'container inspect' ] && [ -e '%s' ]; then\n\techo 'job status unavailable' >&2\n\texit 1\nfi", unavailable))

	_, wait := f.start(path, "run", "-goal", "wait", "-image", image)
	awaitPhase(t, f, core.PhaseAwaitingObservation)
	writeFile(t, unavailable)
	if r := wait(); r.code != exitResolution {
		t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
	}
	h := f.history()
	last := h[len(h)-1]
	if !slices.Equal(kinds(h), slices.Concat(stepKinds[:6], []string{"outcome_unknown"})) || last.To != core.PhaseNeedsResolution ||
		!strings.Contains(last.Facts.Failure, "inspect failed") || !strings.Contains(last.Facts.Failure, "job status unavailable") {
		t.Fatalf("history = %q, last %+v", kinds(h), last)
	}
	s := h[len(h)-2].State
	if cs := effectContainers(t, f.state); len(cs) != 1 || cs[0].id != s.JobID || cs[0].state != "running" {
		t.Fatalf("containers in needs_resolution = %+v, want job %s running", cs, s.JobID)
	}
	if err := os.Remove(unavailable); err != nil {
		t.Fatal(err)
	}
	if r := f.cli(nil, "resolve", "-job"); r.code != exitOK {
		t.Fatalf("resolve -job = %d\nstderr: %s", r.code, r.stderr)
	}
	resolved := f.history()
	last = resolved[len(resolved)-1]
	if !extends(resolved, h) || len(resolved) != len(h)+1 || last.Kind != "job_bound" || last.From != core.PhaseNeedsResolution ||
		last.To != core.PhaseAwaitingObservation || last.State.JobID != s.JobID || !last.State.Started {
		t.Fatalf("resolution = %+v", last)
	}
	writeFile(t, filepath.Join(f.workspace, "release"))
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	final := f.history()
	if !extends(final, resolved) || !slices.Equal(kinds(final), slices.Concat(stepKinds[:6], []string{"outcome_unknown", "job_bound", "observed"}, stepKinds[:4])) {
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
	evidence(t, "unknown outcomes", "container inspect unavailable in awaiting_observation", image, f, final)
}

func TestGateSingleWorkspaceWriterAndTakeover(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: released + "echo x >> count-1"},
		2: {Kind: core.Return, Payload: "done"},
	})
	writer, wait := f.start(nil, "run", "-goal", "write", "-image", image)
	awaitPhase(t, f, core.PhaseAwaitingObservation)
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
		if r.code != exitLocked || !strings.Contains(r.stderr, errLocked.Error()) {
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
	if !extends(final, h) || !slices.Equal(kinds(final), slices.Concat(stepKinds, stepKinds[:4])) || final[len(h)].From != core.PhaseAwaitingObservation {
		t.Fatalf("history = %q", kinds(final))
	}
	if n := len(f.model.prompts(1)); n != 1 || len(f.model.requests()) != 2 {
		t.Errorf("%d model requests, %d for step 1", len(f.model.requests()), n)
	}
	checkFile(t, filepath.Join(f.workspace, "count-1"), "x\n")
	checkReleased(t, f, final)
	checkReplay(t, h[0].State.RunID, final)
	evidence(t, "single workspace writer", "concurrent resume and stop, then SIGKILL of the writer in awaiting_observation", image, f, final)
}
