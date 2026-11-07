//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
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
	"github.com/ChrAlpha/canti/internal/harness/docker/dockertest"
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

// checkRecords checks that the step records of the run of h hold goal, the
// command and preview of every completed step and no other, a performed
// step's preview as context version 13 presents its outcome, and the run's
// result once it is closed.
func checkRecords(t *testing.T, f *fixture, goal string, h []entry) {
	t.Helper()
	last := h[len(h)-1].State
	dir := filepath.Join(f.state, "history", last.RunID)
	checkFile(t, filepath.Join(dir, "goal"), goal)
	var want, got []string
	for i, e := range h {
		if e.Kind != "observed" && e.Kind != "resolved" && e.Kind != "performed" {
			continue
		}
		s := h[i-1].State
		step := filepath.Join(dir, "steps", fmt.Sprintf("%08d", s.Step))
		checkFile(t, filepath.Join(step, "command"), s.Decision.Payload)
		if e.Facts.Outcome != nil {
			checkFile(t, filepath.Join(step, "preview"), outcomePreview(t, *e.Facts.Outcome))
		} else {
			checkFile(t, filepath.Join(step, "preview"), e.Facts.Observation.Preview)
		}
		want = append(want, filepath.Base(step))
	}
	entries, err := os.ReadDir(filepath.Join(dir, "steps"))
	for _, e := range entries {
		got = append(got, e.Name())
	}
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("step records %q, %v; want %q", got, err, want)
	}
	result := ""
	switch last.Phase {
	case core.PhaseReturned:
		result = "phase: returned\n\n" + last.Decision.Payload
	case core.PhaseStopped:
		result = "phase: stopped\nreason: " + string(last.Reason) + "\n"
	}
	if data, err := os.ReadFile(filepath.Join(dir, "result")); string(data) != result || (result == "") != os.IsNotExist(err) {
		t.Errorf("result = %q, %v; want %q", data, err, result)
	}
}

// outcomePreview is the preview that context version 13 publishes for a
// step that the harness performed with outcome o, as version 12 did.
func outcomePreview(t *testing.T, o harness.Outcome) string {
	t.Helper()
	switch {
	case o.Detail != "":
		return "Context action failed: " + o.Detail
	case o.Retrieval != nil:
		refs := *o.Retrieval
		refs.Evidence = nil
		data, err := json.Marshal(refs)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	return "Handover recorded; the next request starts a new context window."
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
	"job_started": decodeEvent[core.JobStarted], "observed": decodeEvent[core.Observed], "performed": decodeEvent[core.Performed],
	"outcome_unknown": decodeEvent[core.OutcomeUnknown], "stop_requested": decodeEvent[core.StopRequested],
	"resolved": decodeEvent[core.Resolved],
}

// checkReplay folds core.State.Next over the recorded events of h from the
// fresh state of run and fails unless it reproduces every recorded
// transition in an unbroken sequence.
func checkReplay(t *testing.T, runID string, h []entry) {
	t.Helper()
	if len(h) == 0 {
		t.Fatal("no transitions to replay")
	}
	s, err := core.NewState(runID, h[0].State.Limits)
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

// dockerProxy returns the PATH setting of a docker CLI that reaches the
// daemon through a proxy, where intercept answers a request when it reports
// true.
func dockerProxy(t *testing.T, intercept func(http.ResponseWriter, *http.Request) bool) []string {
	t.Helper()
	return []string{"PATH=" + dockertest.Proxy(t, intercept) + string(os.PathListSeparator) + os.Getenv("PATH")}
}

// unavailableInspection fails every container inspection while the file
// unavailable exists.
func unavailableInspection(unavailable string) func(http.ResponseWriter, *http.Request) bool {
	return func(w http.ResponseWriter, r *http.Request) bool {
		_, err := os.Stat(unavailable)
		return err == nil && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/json") && !strings.HasSuffix(r.URL.Path, "/containers/json") &&
			dockertest.Reply(w, http.StatusInternalServerError, `{"message":"job status unavailable"}`)
	}
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

// latestOutcome returns the outcome of the latest step of h that the harness
// performed itself, failing unless that step is the latest completed one and
// its action was done.
func latestOutcome(t *testing.T, h []entry) harness.Outcome {
	t.Helper()
	for _, e := range slices.Backward(h) {
		switch e.Kind {
		case "performed":
			if o := e.Facts.Outcome; o != nil && o.Status == harness.OutcomeDone {
				return *o
			}
			t.Fatalf("the latest local action did not complete: %+v", e.Facts.Outcome)
		case "observed", "resolved":
			t.Fatalf("the latest completed step ran a command: %+v", e.Facts.Observation)
		}
	}
	t.Fatal("no completed step")
	return harness.Outcome{}
}
