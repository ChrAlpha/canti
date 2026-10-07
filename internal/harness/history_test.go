package harness_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// seed commits one step of d through r. An exec step completes with obs:
// observed without a job when it failed to start, otherwise observed or,
// when resolved is set, resolved after an unknown outcome of its job.
func seed(t *testing.T, f *fixture, r harness.Runner, d core.Decision, obs harness.Observation, resolved bool) {
	t.Helper()
	ctx := context.Background()
	snap, err := f.store.Latest(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	commit := func(e core.Event, facts harness.Facts) {
		t.Helper()
		if snap, err = r.Commit(ctx, snap, e, facts); err != nil {
			t.Fatalf("commit %s: %v", e.Kind(), err)
		}
	}
	body, _ := json.Marshal(d)
	step := snap.State.Step
	commit(core.RequestFrozen{}, harness.Facts{Request: &harness.Request{Body: []byte("frozen"), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}})
	commit(core.AttemptStarted{Step: step, Attempt: snap.State.Attempt}, harness.Facts{})
	commit(core.ResponseSaved{Step: step, Attempt: snap.State.Attempt}, harness.Facts{Response: &harness.Response{Body: body, Complete: true, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}})
	commit(core.ResponseLoaded{Step: step, Attempt: snap.State.Attempt, Body: body}, harness.Facts{})
	if d.Kind == core.Return {
		return
	}
	effect := snap.State.EffectID
	if obs.Execution != core.ExecutionFailedToStart || resolved {
		commit(core.JobBound{EffectID: effect, JobID: jobID(effect)}, harness.Facts{})
		commit(core.JobStarted{EffectID: effect, JobID: jobID(effect)}, harness.Facts{})
	}
	if resolved {
		commit(core.OutcomeUnknown{EffectID: effect}, harness.Facts{Failure: "job lost"})
		commit(core.Resolved{EffectID: effect, Execution: obs.Execution}, harness.Facts{Observation: &obs})
		return
	}
	commit(core.Observed{EffectID: effect, Execution: obs.Execution}, harness.Facts{Observation: &obs})
}

func runner(t *testing.T, f *fixture) harness.Runner {
	t.Helper()
	r, err := harness.NewRunner(f.store, f.exec)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func publish(t *testing.T, f *fixture, j harness.Journal, id string) {
	t.Helper()
	if err := harness.PublishHistory(context.Background(), j, f.cfg.History, id); err != nil {
		t.Fatalf("PublishHistory(%s) = %v", id, err)
	}
}

// tree maps every file under dir to its content and every directory to "/".
func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == dir {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			out[rel+"/"] = "/"
			return nil
		}
		data, err := os.ReadFile(p)
		out[rel] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func entries(t *testing.T, dir string) map[string]fs.FileInfo {
	t.Helper()
	out := map[string]fs.FileInfo{}
	for p := range tree(t, dir) {
		info, err := os.Lstat(filepath.Join(dir, p))
		if err != nil {
			t.Fatal(err)
		}
		out[p] = info
	}
	return out
}

func wantTree(t *testing.T, dir string, want map[string]string) {
	t.Helper()
	if got := tree(t, dir); !reflect.DeepEqual(got, want) {
		t.Fatalf("records under %s =\n%q\nwant\n%q", dir, got, want)
	}
}

func execDecision(command string) core.Decision {
	return core.Decision{Kind: core.Exec, Payload: command}
}

func TestPublishHistoryLaysOutTheCommittedSteps(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	r, dir := runner(t, f), filepath.Join(f.cfg.History, runID)
	want := map[string]string{"goal": "Count the Go files.", "constraints": "Do not modify files.", "steps/": "/"}
	publish(t, f, f.store, runID)
	wantTree(t, dir, want)

	ok, killed := 0, 143
	steps := []struct {
		command, meta string
		obs           harness.Observation
		resolved      bool
	}{
		{"printf '%s\\n' \"日本語\"\r\necho two", "step: 1\nexecution: exited\nexit_code: 0\nresolved: false\noutput: /canti/artifacts/e1/output\noutput_bytes: 14\noutput_complete: true\n",
			harness.Observation{Execution: core.ExecutionExited, ExitCode: &ok, Preview: "日本語\ntwo\n", Output: harness.ArtifactRef{Path: "e1/output", Bytes: 14, Complete: true, Available: true}}, false},
		{"true", "step: 2\nexecution: failed_to_start\nresolved: false\noutput: unavailable\noutput_bytes: 0\noutput_complete: false\noutput_detail: job never started: unable to find user nosuchuser\n",
			harness.Observation{Execution: core.ExecutionFailedToStart, Output: harness.ArtifactRef{Path: "e2/output", Detail: "job never started: unable to find user nosuchuser"}}, false},
		{"sleep 600", "step: 3\nexecution: timed_out\nexit_code: 143\nresolved: false\noutput: /canti/artifacts/e3/output\noutput_bytes: 8\noutput_complete: false\noutput_detail: kept 8 of 9 bytes; job was  cancelled\n",
			harness.Observation{Execution: core.ExecutionTimedOut, ExitCode: &killed, Preview: "partial\n", Output: harness.ArtifactRef{Path: "e3/output", Bytes: 8, Available: true, Detail: "kept 8 of 9 bytes;\njob was\r\ncancelled"}}, false},
		{"make test", "step: 4\nexecution: exited\nexit_code: 0\nresolved: true\noutput: unavailable\noutput_bytes: 0\noutput_complete: false\noutput_detail: resolved by operator\n",
			harness.Observation{Execution: core.ExecutionExited, ExitCode: &ok, Preview: "checked by hand", Output: harness.ArtifactRef{Detail: "resolved by operator"}}, true},
	}
	for i, s := range steps {
		seed(t, f, r, execDecision(s.command), s.obs, s.resolved)
		publish(t, f, f.store, runID)
		name := fmt.Sprintf("steps/%08d/", i+1)
		want[name], want[name+"meta"], want[name+"command"], want[name+"preview"] = "/", s.meta, s.command, s.obs.Preview
		wantTree(t, dir, want)
	}

	before := entries(t, dir)
	publish(t, f, f.store, runID)
	wantTree(t, dir, want)
	for p, info := range entries(t, dir) {
		if !os.SameFile(info, before[p]) || !info.ModTime().Equal(before[p].ModTime()) {
			t.Errorf("a second publication rewrote %s", p)
		}
	}

	seed(t, f, r, core.Decision{Kind: core.Return, Payload: "There are 3 Go files.\nDone."}, harness.Observation{}, false)
	publish(t, f, f.store, runID)
	want["result"] = "phase: returned\n\nThere are 3 Go files.\nDone."
	wantTree(t, dir, want)
}

func TestPublishHistoryRecordsAStoppedRunWithoutConstraints(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	ctx := context.Background()
	state, err := core.NewState("run-2", limits)
	if err == nil {
		err = f.store.CreateRun(ctx, harness.Run{ID: "run-2", Goal: " Stop at once. ", Created: created}, state)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner(t, f).Commit(ctx, harness.Snapshot{State: state}, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
		t.Fatal(err)
	}
	publish(t, f, f.store, "run-2")
	publish(t, f, f.store, runID)
	wantTree(t, f.cfg.History, map[string]string{
		"run-2/": "/", "run-2/goal": " Stop at once. ", "run-2/steps/": "/", "run-2/result": "phase: stopped\nreason: cancelled\n",
		runID + "/": "/", runID + "/goal": "Count the Go files.", runID + "/constraints": "Do not modify files.", runID + "/steps/": "/",
	})
}

func TestPublishHistoryRebuildsALostProjection(t *testing.T) {
	const n = historyPage + 6
	f := newFixture(t, core.Limits{MaxSteps: n + 1, MaxAttempts: 1}, harness.Budget{})
	r, dir := runner(t, f), filepath.Join(f.cfg.History, runID)
	for i := 1; i <= n; i++ {
		code := i % 3
		seed(t, f, r, execDecision(fmt.Sprintf("echo %d", i)), harness.Observation{Execution: core.ExecutionExited, ExitCode: &code, Preview: fmt.Sprintf("%d\n", i),
			Output: harness.ArtifactRef{Path: fmt.Sprintf("e%d/output", i), Bytes: int64(len(fmt.Sprint(i)) + 1), Complete: true, Available: true}}, i%5 == 0)
		publish(t, f, f.store, runID)
	}
	want := tree(t, dir)
	if len(want) != 3+4*n {
		t.Fatalf("%d records for %d steps", len(want), n)
	}
	for _, lose := range []string{dir, filepath.Join(dir, "steps")} {
		if err := os.RemoveAll(lose); err != nil {
			t.Fatal(err)
		}
		p := &probe{Store: f.store}
		publish(t, f, p, runID)
		wantTree(t, dir, want)
		if p.pages != 3 || p.records != n+1 {
			t.Fatalf("rebuilt from %d records in %d pages, want the latest and then %d in two pages", p.records, p.pages, n)
		}
	}

	if err := os.WriteFile(filepath.Join(dir, ".tmp-123"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".tmp-456", "00000001"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp-456", "00000001", "meta"), []byte("forged"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "steps")); err != nil {
		t.Fatal(err)
	}
	publish(t, f, f.store, runID)
	wantTree(t, dir, want)
}

func TestPublishHistoryAddsOnlyTheLatestStep(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{})
	r, dir := runner(t, f), filepath.Join(f.cfg.History, runID)
	for i := 1; i <= 3; i++ {
		seed(t, f, r, execDecision(fmt.Sprintf("echo %d", i)), harness.Observation{Execution: core.ExecutionCancelled, Preview: "stopped"}, false)
	}
	publish(t, f, f.store, runID)
	want := tree(t, dir)
	for _, name := range []string{"goal", "constraints", "steps/00000002", "steps/00000003"} {
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	publish(t, f, f.store, runID)
	for p := range want {
		if strings.HasPrefix(p, "steps/00000002/") {
			delete(want, p)
		}
	}
	wantTree(t, dir, want)
}

func TestPublishHistoryReportsFailures(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, limits, harness.Budget{})
	ok := 0
	seed(t, f, runner(t, f), execDecision("true"), harness.Observation{Execution: core.ExecutionExited, ExitCode: &ok}, false)
	root, dir := f.cfg.History, filepath.Join(f.cfg.History, runID)

	if err := os.WriteFile(root, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := harness.PublishHistory(ctx, f.store, root, runID); err == nil {
		t.Fatal("PublishHistory() into a file succeeded")
	}
	if data, err := os.ReadFile(root); err != nil || string(data) != "not a directory" {
		t.Fatalf("history root = %q, %v", data, err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}

	if err := harness.PublishHistory(ctx, &failingRecent{Store: f.store}, root, runID); !errors.Is(err, errRecent) {
		t.Fatalf("PublishHistory() with a failing read = %v, want %v", err, errRecent)
	}
	for p := range tree(t, dir) {
		if p == "steps/" || strings.HasPrefix(p, ".tmp-") {
			t.Fatalf("a failed rebuild left %s", p)
		}
	}
	publish(t, f, f.store, runID)
	if got := tree(t, dir); got["steps/00000001/command"] != "true" {
		t.Fatalf("records after the failure = %q", got)
	}

	stale := filepath.Join(dir, ".tmp-stale", "held")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Dir(stale), 0o755); os.Chmod(dir, 0o755) })
	if err := os.Chmod(filepath.Dir(stale), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := harness.PublishHistory(ctx, f.store, root, runID); err == nil {
		t.Fatal("PublishHistory() succeeded without removing a stale temporary")
	}
	if err := os.Chmod(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	publish(t, f, f.store, runID)
	if _, err := os.Stat(filepath.Dir(stale)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale temporary after a publication = %v", err)
	}
	if err := os.Chmod(dir, 0o311); err != nil {
		t.Fatal(err)
	}
	if err := harness.PublishHistory(ctx, f.store, root, runID); err == nil {
		t.Fatal("PublishHistory() succeeded without listing the run directory")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := harness.PublishHistory(ctx, f.store, root, "run-missing"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("PublishHistory(run-missing) = %v, want %v", err, harness.ErrNotFound)
	}
	for _, id := range []string{"..", "a/b"} {
		state, err := core.NewState(id, limits)
		if err == nil {
			err = f.store.CreateRun(ctx, harness.Run{ID: id, Goal: "escape", Created: created}, state)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := harness.PublishHistory(ctx, f.store, root, id); err == nil {
			t.Fatalf("PublishHistory(%q) succeeded", id)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "a")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a run ID with a separator created %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "goal")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a run ID of .. escaped the root: %v", err)
	}
}

func TestPublishHistoryRefusesRecordsOfAnotherType(t *testing.T) {
	ok := 0
	for _, tc := range []struct{ entry, plant string }{
		{".", "link"}, {"goal", "link"}, {"constraints", "link"}, {"result", "link"}, {"steps", "link"},
		{"steps/00000001", "link"}, {"steps/00000001/meta", "link"}, {"steps/00000001/command", "link"}, {"steps/00000001/preview", "link"},
		{"goal", "dir"}, {"result", "dir"}, {"steps/00000001/preview", "dir"},
		{".", "file"}, {"steps", "file"}, {"steps/00000001", "file"},
	} {
		t.Run(tc.plant+" "+tc.entry, func(t *testing.T) {
			f := newFixture(t, limits, harness.Budget{})
			r := runner(t, f)
			seed(t, f, r, execDecision("true"), harness.Observation{Execution: core.ExecutionExited, ExitCode: &ok}, false)
			seed(t, f, r, core.Decision{Kind: core.Return, Payload: "done"}, harness.Observation{}, false)
			publish(t, f, f.store, runID)
			outside := t.TempDir()
			if err := os.WriteFile(filepath.Join(outside, "file"), []byte("outside"), 0o644); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(f.cfg.History, runID, tc.entry)
			info, err := os.Lstat(p)
			if err == nil {
				err = os.RemoveAll(p)
			}
			target := outside
			if !info.IsDir() {
				target = filepath.Join(outside, "file")
			}
			switch tc.plant {
			case "link":
				err = errors.Join(err, os.Symlink(target, p))
			case "dir":
				err = errors.Join(err, os.Mkdir(p, 0o755))
			case "file":
				err = errors.Join(err, os.WriteFile(p, nil, 0o644))
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := harness.PublishHistory(context.Background(), f.store, f.cfg.History, runID); err == nil {
				t.Fatalf("PublishHistory() accepted a %s for %s", tc.plant, tc.entry)
			}
			wantTree(t, outside, map[string]string{"file": "outside"})
		})
	}
}

// watching is a fakeModel that calls freeze before it freezes a prompt.
type watching struct {
	*fakeModel
	freeze func(harness.Prompt)
}

func (m watching) Freeze(p harness.Prompt) (harness.Request, error) {
	m.freeze(p)
	return m.fakeModel.Freeze(p)
}

func stepNames(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(filepath.Join(dir, "steps"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}

func TestLoopPublishesEveryStepBeforeTheNextRequest(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, twoSteps()...)
	dir := filepath.Join(f.cfg.History, runID)
	freezes := 0
	m := watching{f.model, func(p harness.Prompt) {
		freezes++
		var want []string
		for i := 1; i < freezes; i++ {
			want = append(want, fmt.Sprintf("%08d", i))
		}
		records := tree(t, dir)
		if got := stepNames(t, dir); !slices.Equal(got, want) || records["goal"] != "Count the Go files." || records["result"] != "" {
			t.Errorf("request %d frozen over steps %v and records %q, want steps %v and no result", freezes, got, records, want)
		}
	}}
	l, err := harness.NewLoop(f.store, f.exec, m, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	wantState(t, f.run(l), core.PhaseReturned, "")
	records := tree(t, dir)
	for i, command := range []string{"ls -la /workspace", "find . -name '*.go' | wc -l"} {
		name := fmt.Sprintf("steps/%08d/", i+1)
		if records[name+"command"] != command || records[name+"preview"] != "ran: "+command || !strings.HasPrefix(records[name+"meta"], fmt.Sprintf("step: %d\nexecution: exited\nexit_code: 0\n", i+1)) {
			t.Errorf("step %d records = %q", i+1, records)
		}
	}
	if freezes != 3 || records["result"] != "phase: returned\n\nThere are 3 Go files." {
		t.Fatalf("%d requests frozen; result %q", freezes, records["result"])
	}
}

// block replaces the history root with a file, so nothing can be published
// under it, and returns the function that restores it.
func block(t *testing.T, root string) func() {
	t.Helper()
	aside := root + ".aside"
	if err := os.Rename(root, aside); err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if err := os.Remove(root); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(aside, root); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

func TestLoopFreezesNothingWhileStepRecordsCannotBePublished(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, twoSteps()...)
	l := f.loop()
	for _, done := range []int{0, len(cycle)} {
		restore := block(t, f.cfg.History)
		prompts, _ := f.model.calls()
		snap, err := l.Step(f.bounded(), runID)
		if after, _ := f.model.calls(); err == nil || snap.State.Phase != core.PhaseIdle || len(f.transitions()) != done || after != prompts {
			t.Fatalf("Step() without step records = %+v, %v after %d prompts and %d transitions; want an error and nothing frozen", snap.State, err, after, len(f.transitions()))
		}
		restore()
		wantState(t, f.steps(l, 1), core.PhaseRequestFrozen, "")
		if names := stepNames(t, filepath.Join(f.cfg.History, runID)); len(names) != done/len(cycle) {
			t.Fatalf("steps %v published before request %d", names, done/len(cycle)+1)
		}
		f.steps(l, 5)
	}
}

func TestLoopRunPublishesAHaltedRun(t *testing.T) {
	t.Run("final step", func(t *testing.T) {
		f := newFixture(t, core.Limits{MaxSteps: 1, MaxAttempts: 1}, harness.Budget{}, execReply("true"))
		wantState(t, f.run(f.loop()), core.PhaseStopped, core.StopStepsExhausted)
		records := tree(t, filepath.Join(f.cfg.History, runID))
		if records["steps/00000001/command"] != "true" || records["result"] != "phase: stopped\nreason: steps_exhausted\n" {
			t.Fatalf("records = %q", records)
		}
	})
	t.Run("failure", func(t *testing.T) {
		h := &logs{}
		f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
		f.cfg.Logger = slog.New(h)
		var restore func()
		l := f.loopOn(hookJournal{Store: f.store, after: func(tr harness.Transition) {
			if tr.Event.Kind() == "response_loaded" {
				restore = block(t, f.cfg.History)
			}
		}})
		if snap, err := l.Run(f.bounded(), runID); err != nil || snap.State.Phase != core.PhaseReturned {
			t.Fatalf("Run() with a failing publication = %+v, %v; want the returned run", snap.State, err)
		}
		if w := h.find("step records not published"); len(w) != 1 || w[0]["level"] != "WARN" || w[0]["run_id"] != runID || !strings.Contains(w[0]["error"], "not a directory") {
			t.Fatalf("warnings = %v, want one for the failed publication", w)
		}
		restore()
		f.run(f.fresh())
		if got := tree(t, filepath.Join(f.cfg.History, runID))["result"]; got != "phase: returned\n\ndone" {
			t.Fatalf("result after a later Run = %q", got)
		}
	})
}
