//go:build integration

package main

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// recoveryFixture is a dockerFixture whose test also fails if a container of
// its state directory is still running when the test ends.
func recoveryFixture(t *testing.T, script map[int]core.Decision) (*fixture, string) {
	t.Helper()
	f, image := dockerFixture(t, script)
	t.Cleanup(func() {
		for _, c := range effectContainers(t, f.state) {
			if c.state == "running" {
				t.Errorf("container %s of effect %s is still running", c.id, c.effect)
			}
		}
	})
	return f, image
}

type effectContainer struct{ id, effect, state string }

// effectContainers lists every container labelled with the state directory.
func effectContainers(t *testing.T, state string) []effectContainer {
	t.Helper()
	state, err := filepath.EvalSymlinks(state)
	if os.IsNotExist(err) {
		return nil
	}
	var out []byte
	if err == nil {
		out, err = exec.Command("docker", "ps", "-a", "--no-trunc", "--filter", "label=canti.state="+state,
			"--format", `{{.ID}} {{.Label "canti.effect"}} {{.State}}`).Output()
	}
	if err != nil {
		t.Errorf("list containers: %v", err)
		return nil
	}
	var cs []effectContainer
	for line := range strings.Lines(string(out)) {
		var c effectContainer
		if _, err := fmt.Sscan(line, &c.id, &c.effect, &c.state); err != nil {
			t.Errorf("container line %q: %v", line, err)
		}
		cs = append(cs, c)
	}
	return cs
}

// boundJobs maps every effect of h to the one job its job_bound transitions
// name.
func boundJobs(t *testing.T, h []entry) map[string]string {
	t.Helper()
	jobs := map[string]string{}
	for _, e := range h {
		if e.Kind != "job_bound" {
			continue
		}
		if job, ok := jobs[e.State.EffectID]; ok && job != e.State.JobID {
			t.Errorf("effect %s is bound to jobs %s and %s", e.State.EffectID, job, e.State.JobID)
		}
		jobs[e.State.EffectID] = e.State.JobID
	}
	return jobs
}

// jobRecord is the final record the executor publishes for the finished job
// of an effect before it releases the job's container.
type jobRecord struct {
	harness.Job
	Spec string
}

// readRecord returns the final record of effect, if it has one.
func readRecord(t *testing.T, f *fixture, effect string) (jobRecord, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.state, "jobs", effect, "output.json"))
	if os.IsNotExist(err) {
		return jobRecord{}, false
	}
	var rec jobRecord
	if err == nil {
		err = strict(string(data), &rec)
	}
	if err != nil {
		t.Fatalf("record of effect %s: %v", effect, err)
	}
	return rec, true
}

// records lists the effects of f that have a final record.
func records(t *testing.T, f *fixture) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.state, "jobs", "*", "output.json"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, p := range paths {
		out = append(out, filepath.Base(filepath.Dir(p)))
	}
	return out
}

// checkReleased checks that the host holds no container of f and that every
// effect bound in h, and no other, has a final record, which names the job
// committed for it and reports its committed observation. It returns the
// records by job ID.
func checkReleased(t *testing.T, f *fixture, h []entry) map[string]jobRecord {
	t.Helper()
	if cs := effectContainers(t, f.state); len(cs) != 0 {
		t.Errorf("containers left = %+v, want every finished job released", cs)
	}
	jobs, observed := boundJobs(t, h), map[string]*harness.Observation{}
	for _, e := range h {
		switch e.Kind {
		case "job_started":
			if s := evEvent[core.JobStarted](t, e); s.JobID != jobs[s.EffectID] {
				t.Errorf("effect %s started job %s, bound %s", s.EffectID, s.JobID, jobs[s.EffectID])
			}
		case "observed":
			observed[evEvent[core.Observed](t, e).EffectID] = e.Facts.Observation
		}
	}
	if got, want := records(t, f), slices.Sorted(maps.Keys(jobs)); !slices.Equal(got, want) {
		t.Errorf("effects with a final record = %q, want the bound %q", got, want)
	}
	out := map[string]jobRecord{}
	for effect, job := range jobs {
		rec, ok := readRecord(t, f, effect)
		obs := observed[effect]
		if !ok || obs == nil {
			t.Errorf("effect %s: record found %v, observation %+v", effect, ok, obs)
			continue
		}
		status := harness.JobCancelled
		if obs.Execution == core.ExecutionExited {
			status = harness.JobFailed
			if rec.ExitCode != nil && *rec.ExitCode == 0 {
				status = harness.JobSucceeded
			}
		}
		want := harness.Observation{Execution: obs.Execution, ExitCode: rec.ExitCode, Preview: rec.Preview, Output: rec.Output}
		if rec.ID != job || rec.Status != status || rec.ExitCode == nil || rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt) ||
			!reflect.DeepEqual(*obs, want) {
			t.Errorf("record of effect %s = %+v\nwant the finished job %s as observed: %+v", effect, rec, job, *obs)
		}
		out[job] = rec
	}
	return out
}

func decodeEvent[E core.Event](raw string) (core.Event, error) {
	var e E
	err := strict(raw, &e)
	return e, err
}

var eventDecoders = map[string]func(string) (core.Event, error){
	"request_frozen": decodeEvent[core.RequestFrozen], "attempt_started": decodeEvent[core.AttemptStarted],
	"response_saved": decodeEvent[core.ResponseSaved], "response_incomplete": decodeEvent[core.ResponseIncomplete],
	"response_loaded": decodeEvent[core.ResponseLoaded], "job_bound": decodeEvent[core.JobBound],
	"job_started": decodeEvent[core.JobStarted], "observed": decodeEvent[core.Observed],
	"outcome_unknown": decodeEvent[core.OutcomeUnknown], "stop_requested": decodeEvent[core.StopRequested],
	"resolved": decodeEvent[core.Resolved],
}

// checkReplay folds core.State.Next over the recorded events of h from the
// fresh state of run and fails unless it reproduces every recorded
// transition in an unbroken sequence.
func checkReplay(t *testing.T, runID string, h []entry) {
	t.Helper()
	s, err := core.NewState(runID, parseSpec(t).limits)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range h {
		decode, ok := eventDecoders[e.Kind]
		if !ok {
			t.Fatalf("seq %d has unknown kind %q", e.Seq, e.Kind)
		}
		event, err := decode(string(e.Event))
		if err != nil || event.Kind() != e.Kind {
			t.Fatalf("seq %d event %s: %v", e.Seq, e.Event, err)
		}
		next, err := s.Next(event)
		if e.Seq != int64(i+1) || e.From != s.Phase || err != nil || next != e.State {
			t.Fatalf("replay diverges at seq %d (%s from %s): got %+v, %v\nrecorded %+v", e.Seq, e.Kind, s.Phase, next, err, e.State)
		}
		s = next
	}
}

// extends reports whether h begins with every entry of prefix unchanged.
func extends(h, prefix []entry) bool {
	return len(h) >= len(prefix) && slices.EqualFunc(h[:len(prefix)], prefix, func(a, b entry) bool { return reflect.DeepEqual(a, b) })
}

// crashRun runs the CLI with CANTI_TEST_CRASH=crash and fails unless the
// process died by SIGKILL.
func crashRun(t *testing.T, f *fixture, crash, command string, args ...string) {
	t.Helper()
	cmd, wait := f.start([]string{"CANTI_TEST_CRASH=" + crash}, command, args...)
	if r := wait(); !killed(cmd) {
		t.Fatalf("canti %s with %s = %d, not killed\nstdout: %s\nstderr: %s", command, crash, r.code, r.stdout, r.stderr)
	}
}

func killed(cmd *exec.Cmd) bool {
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}

// dockerShim returns the PATH setting of a docker CLI that runs the shell
// script with the CLI's arguments before it runs the real one.
func dockerShim(t *testing.T, script string) []string {
	t.Helper()
	bin, err := exec.LookPath("docker")
	if err == nil {
		bin, err = filepath.Abs(bin)
	}
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	shim := fmt.Sprintf("#!/bin/sh\n%s\nexec '%s' \"$@\"\n", script, bin)
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	return []string{"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH")}
}

// awaitPhase waits until the latest run of f reports phase.
func awaitPhase(t *testing.T, f *fixture, phase core.Phase) {
	t.Helper()
	waitFor(t, func() bool {
		r := f.cli(nil, "status")
		var s status
		return r.code == exitOK && strict(r.stdout, &s) == nil && s.Phase == phase
	})
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func checkFile(t *testing.T, path, want string) {
	t.Helper()
	if data, err := os.ReadFile(path); err != nil || string(data) != want {
		t.Errorf("%s = %q, %v; want %q", filepath.Base(path), data, err, want)
	}
}

// evidence logs the one-line evidence record of a gate case.
func evidence(t *testing.T, gate, injected, image string, f *fixture, h []entry) {
	t.Helper()
	var jobs []string
	for _, e := range h {
		if e.Kind == "job_bound" {
			jobs = append(jobs, fmt.Sprintf("step%d:%s=%s", e.State.Step, e.State.EffectID, e.State.JobID))
		}
	}
	last := h[len(h)-1]
	t.Logf("evidence gate=%q label=%q injected=%q platform=%s/%s go=%s image=%s run=%s final=%s seqs=1..%d requests=%d jobs=%v",
		gate, "concrete backend integration with a scripted provider", injected, runtime.GOOS, runtime.GOARCH, runtime.Version(),
		image, last.State.RunID, last.To, last.Seq, len(f.model.requests()), jobs)
}
