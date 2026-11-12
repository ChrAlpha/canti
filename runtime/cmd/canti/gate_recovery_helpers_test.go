//go:build integration

package main

import (
	"context"
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
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker/dockertest"
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

// boundJobs maps receipt evidence to the single physical owner of each effect.
func boundJobs(t *testing.T, h []entry) map[string]string {
	t.Helper()
	jobs := map[string]string{}
	for _, e := range h {
		r := e.Facts.Receipt
		if r == nil {
			continue
		}
		if old := jobs[r.EffectID]; old != "" && old != r.ID {
			t.Errorf("effect %s changed owner from %s to %s", r.EffectID, old, r.ID)
		}
		jobs[r.EffectID] = r.ID
	}
	return jobs
}

// effectOwner reads the executor's durable binding, independently of Run state.
func effectOwner(t *testing.T, f *fixture, effect string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.state, "jobs", effect, "binding.json"))
	var binding struct {
		Format                          int
		EffectID, Spec, CommandHash, ID string
	}
	if err == nil {
		err = strict(string(data), &binding)
	}
	if err != nil || binding.Format != 2 || binding.EffectID != effect || binding.ID == "" {
		t.Fatalf("binding for %s: %+v, %v", effect, binding, err)
	}
	return binding.ID
}

// init injects a crash only after Docker confirms the bound owner is running.
// The observer lives in the actual controller process, including an auto host.
func init() {
	if os.Getenv("CANTI_TEST_MAIN") != "1" || os.Getenv("CANTI_TEST_EXECUTOR_CRASH") != "running" {
		return
	}
	// Auto clients observe the host; only the process owning the drive dies.
	if len(os.Args) > 1 && os.Args[1] != "host" {
		for i, arg := range os.Args {
			if arg == "-host" && i+1 < len(os.Args) && os.Args[i+1] == "auto" {
				return
			}
		}
	}
	state := ""
	for i, arg := range os.Args {
		if arg == "-state" && i+1 < len(os.Args) {
			state = os.Args[i+1]
		}
	}
	if state == "" {
		return
	}
	go func() {
		for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			paths, _ := filepath.Glob(filepath.Join(state, "jobs", "*", "binding.json"))
			for _, path := range paths {
				data, err := os.ReadFile(path)
				var binding struct{ ID string }
				if err != nil || json.Unmarshal(data, &binding) != nil || binding.ID == "" {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Running}}", binding.ID).Output()
				cancel()
				if err == nil && strings.TrimSpace(string(out)) == "true" {
					kill()
				}
			}
		}
	}()
}

// jobRecord is the final record the executor publishes for the finished job
// of an effect before it releases the job's container.
type jobRecord struct {
	harness.Job
	Format int
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
	receipts := map[string]*harness.Job{}
	for _, e := range h {
		switch e.Kind {
		case "observed":
			effect := evEvent[core.Observed](t, e).EffectID
			observed[effect] = e.Facts.Observation
			receipts[effect] = e.Facts.Receipt
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
		if rec.Format != 2 || rec.EffectID != effect || !reflect.DeepEqual(&rec.Job, receipts[effect]) || rec.ID != job || rec.Status != status || job != "" && (rec.ExitCode == nil || rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt)) ||
			!reflect.DeepEqual(*obs, want) {
			t.Errorf("record of effect %s = %+v\nwant the finished job %s as observed: %+v", effect, rec, job, *obs)
		}
		out[job] = rec
	}
	return out
}

// checkRecords checks that the step records of the run of h hold goal, the
// command and preview of every completed step and no other, a performed
// step's preview as the current context presents its outcome, and the run's
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

// outcomePreview is the published preview of the committed local outcome.
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
	if o.Focus == nil {
		t.Fatal("successful local outcome has neither retrieval nor focus")
	}
	return fmt.Sprintf("Focus replaced with %d frozen source ranges.", len(o.Focus.Sources))
}

func decodeEvent[E core.Event](raw string) (core.Event, error) {
	var e E
	err := strict(raw, &e)
	return e, err
}

var eventDecoders = map[string]func(string) (core.Event, error){
	"request_frozen": decodeEvent[core.RequestFrozen], "request_rejected": decodeEvent[core.RequestRejected], "attempt_started": decodeEvent[core.AttemptStarted],
	"response_saved": decodeEvent[core.ResponseSaved], "response_incomplete": decodeEvent[core.ResponseIncomplete],
	"response_loaded": decodeEvent[core.ResponseLoaded], "effect_recovered": decodeEvent[core.EffectRecovered], "observed": decodeEvent[core.Observed], "performed": decodeEvent[core.Performed],
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
	contextHead := harness.InitialContext()
	var frozen *harness.Request
	var rebuild *core.State
	seenEvents := map[string]bool{}
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
		transition := harness.Transition{Seq: e.Seq, EventID: e.EventID, Semantics: e.Semantics, From: s, To: next, Event: event, Facts: e.Facts}
		if seenEvents[e.EventID] || e.Semantics != core.SemanticsVersion {
			t.Fatalf("seq %d duplicate event identity or wrong semantics", e.Seq)
		}
		seenEvents[e.EventID] = true
		if err := harness.ValidateTransition(transition); err != nil {
			t.Fatalf("seq %d invalid attached facts: %v", e.Seq, err)
		}
		if err := harness.CheckContextMutation(transition, contextHead); err != nil {
			t.Fatalf("seq %d invalid atomic context mutation: %v", e.Seq, err)
		}
		if request := e.Facts.Request; request != nil {
			if rebuild != nil && (next.Step != rebuild.Step || next.RequestRev != rebuild.RequestRev+1 || request.Context.Preparation != "request_rejected") {
				t.Fatalf("seq %d rejection did not rebuild the same unfinished step/new revision", e.Seq)
			}
			frozen, rebuild = request, nil
		}
		if rejected, ok := event.(core.RequestRejected); ok {
			r, response := e.Facts.Rejection, e.Facts.Response
			if rejected.Step != s.Step || rejected.Attempt != s.Attempt || frozen == nil || r == nil || response == nil || len(response.Body) == 0 ||
				!r.Signal.Confirmed() || !r.Previous.Equal(contextHead) || frozen.ContextState == nil || !frozen.ContextState.Equal(r.Previous) ||
				response.Adapter != frozen.Adapter || response.Protocol != frozen.Protocol || response.Usage.Validate() != nil {
				t.Fatalf("seq %d rejection lost sent-request/typed-signal/response/usage provenance", e.Seq)
			}
			if next.Phase == core.PhaseIdle {
				want := harness.ContextState{AnchorSeq: e.Seq, FirstStep: s.Step}
				if next.Step != s.Step || next.Attempt != s.Attempt+1 || next.RequestRev != s.RequestRev || e.Facts.ContextState == nil || !e.Facts.ContextState.Equal(want) {
					t.Fatalf("seq %d rejection did not consume one attempt and atomically reset focus", e.Seq)
				}
				prior := s
				rebuild = &prior
			} else if next.Phase != core.PhaseStopped || next.Reason != core.StopAttemptsExhausted || e.Facts.ContextState != nil || next.Step != s.Step {
				t.Fatalf("seq %d exhausted rejection invented context or progress", e.Seq)
			}
		}
		if e.Facts.Outcome != nil && e.Facts.Outcome.Focus != nil {
			if e.Facts.Outcome.Status != harness.OutcomeDone || e.Facts.ContextState == nil ||
				!slices.Equal(e.Facts.ContextState.Focus, e.Facts.Outcome.Focus.Sources) || next.Step != s.Step+1 {
				t.Fatalf("seq %d focus selection/release and progress were not committed together", e.Seq)
			}
		}
		if e.Facts.ContextState != nil {
			contextHead = *e.Facts.ContextState
		}
		s = next
	}
}

// archiveText follows the same read surface available to native retrievals,
// paging exact bytes rather than reintroducing automatic previous-run projection.
func archiveText(t *testing.T, f *fixture, path string) string {
	t.Helper()
	reader := archive.Reader{History: filepath.Join(f.state, "history"), Artifacts: filepath.Join(f.state, "artifacts")}
	query := archive.RetrievalQuery{Operation: "read", Path: path, Reason: "Verify original committed historical evidence.", Limit: 8192}
	var body strings.Builder
	for page := 0; page < 128; page++ {
		result, err := reader.Retrieve(context.Background(), query)
		if err != nil || result.Error != "" || result.Incomplete || len(result.Sources) != len(result.Evidence) {
			t.Fatalf("archive read %s: %+v %v", path, result, err)
		}
		for _, data := range result.Evidence {
			body.WriteString(data)
		}
		if !result.More {
			return body.String()
		}
		if result.NextOffset <= query.Offset {
			t.Fatal("archive read did not advance")
		}
		query.Offset = result.NextOffset
	}
	t.Fatal("archive read exceeded 1MiB bound")
	return ""
}

func checkLineLink(t *testing.T, f *fixture, prompt, current, previous, goal string) {
	t.Helper()
	if !strings.Contains(prompt, "## Goal\n"+goal+"\n") || !strings.Contains(prompt, "/canti/runs/"+current+"/") ||
		!strings.Contains(prompt, "## Previous runs") || !strings.Contains(prompt, "- "+previous+":") ||
		!strings.Contains(prompt, "/canti/runs/"+previous+"/") {
		t.Fatalf("missing current task or previous-run navigation: %s", prompt)
	}
	previousFile, err := os.ReadFile(filepath.Join(f.state, "history", current, "previous"))
	if err != nil || string(previousFile) != previous {
		t.Fatalf("previous run record = %q, %v; want %q", previousFile, err, previous)
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
	env := []string{"CANTI_TEST_CRASH=" + crash}
	if crash == "executor:running" {
		env = []string{"CANTI_TEST_EXECUTOR_CRASH=running"}
	}
	cmd, wait := f.start(env, command, args...)
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

// awaitRunningEffect waits for both accepted Run state and a running physical owner.
func awaitRunningEffect(t *testing.T, f *fixture) {
	t.Helper()
	waitFor(t, func() bool {
		r := f.cli(nil, "status")
		var s status
		if r.code != exitOK || strict(r.stdout, &s) != nil || s.Phase != core.PhaseEffectSaved {
			return false
		}
		for _, c := range effectContainers(t, f.state) {
			if c.effect == s.EffectID && c.state == "running" {
				return true
			}
		}
		return false
	})
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

// evidence logs the one-line evidence record of a gate case.
func evidence(t *testing.T, gate, injected, image string, f *fixture, h []entry) {
	t.Helper()
	var jobs []string
	for _, e := range h {
		if r := e.Facts.Receipt; r != nil {
			jobs = append(jobs, fmt.Sprintf("step%d:%s=%s", e.State.Step, r.EffectID, r.ID))
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

// sourceHandles derives explicit focus handles only from the completed retrieval.
type sourceHandle struct {
	Seq    int64 `json:"seq"`
	Source int   `json:"source"`
}

func sourceHandles(t *testing.T, h []entry) []sourceHandle {
	t.Helper()
	for _, e := range slices.Backward(h) {
		if e.Kind == "performed" && e.Facts.Outcome != nil && e.Facts.Outcome.Retrieval != nil {
			var handles []sourceHandle
			for index := range e.Facts.Outcome.Retrieval.Sources {
				handles = append(handles, sourceHandle{e.Seq, index})
			}
			if len(handles) == 0 {
				t.Fatal("completed retrieval has no focusable sources")
			}
			return handles
		}
	}
	t.Fatal("no completed retrieval for focus")
	return nil
}
func checkSelectedRequest(t *testing.T, f *fixture, step int, selected []sourceHandle) {
	t.Helper()
	for _, e := range f.history() {
		if e.Kind != "request_frozen" || e.State.Step != step {
			continue
		}
		request := e.Facts.Request
		if request == nil || request.ContextState == nil || len(request.ContextState.Focus) != len(selected) {
			t.Fatalf("selected request lost handles: %+v", request)
		}
		for index, ref := range request.ContextState.Focus {
			if ref.SourceSeq != selected[index].Seq || ref.Source != selected[index].Source {
				t.Fatalf("selected handles changed: %+v", request.ContextState.Focus)
			}
		}
		return
	}
	t.Fatalf("no frozen request for selected step%d", step)
}
