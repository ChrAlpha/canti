//go:build integration

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// evFixture is a dockerFixture that fails the test when a container of its
// state directory is still running at the end.
func evFixture(t *testing.T, script map[int]core.Decision) (*fixture, string) {
	t.Helper()
	f, image := dockerFixture(t, script)
	t.Cleanup(func() {
		if ids := evContainers(t, f.state); len(ids) > 0 {
			t.Errorf("containers of %s still running: %q", f.state, ids)
		}
	})
	return f, image
}

// evContainers lists the running containers labelled with state.
func evContainers(t *testing.T, state string) []string {
	t.Helper()
	if s, err := filepath.EvalSymlinks(state); err == nil {
		state = s
	}
	out, err := exec.Command("docker", "ps", "-q", "--filter", "label=canti.state="+state).Output()
	if err != nil {
		t.Errorf("docker ps: %v", err)
	}
	return strings.Fields(string(out))
}

type evContainer struct {
	State      struct{ Running bool }
	Config     struct{ User string }
	HostConfig struct {
		NetworkMode string
		CapDrop     []string
	}
}

// evInspect inspects the container id.
func evInspect(t *testing.T, id string) evContainer {
	t.Helper()
	out, err := exec.Command("docker", "container", "inspect", id).Output()
	var cs []evContainer
	if err == nil {
		err = json.Unmarshal(out, &cs)
	}
	if err != nil || len(cs) != 1 {
		t.Fatalf("docker inspect %s: %v", id, err)
	}
	return cs[0]
}

func evEvent[T any](t *testing.T, e entry) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(e.Event, &v); err != nil {
		t.Fatalf("event %s: %v", e.Kind, err)
	}
	return v
}

func evJob(t *testing.T, f *fixture, h []entry) string {
	t.Helper()
	for _, e := range h {
		if r := e.Facts.Receipt; r != nil && r.ID != "" {
			return r.ID
		}
	}
	// Running work has no journal receipt: locate its durable binding in the
	// state directory supplied to the test binary's fixture.
	for _, e := range slices.Backward(h) {
		if e.State.EffectID != "" {
			return effectOwner(t, f, e.State.EffectID)
		}
	}
	t.Fatal("no accepted effect")
	return ""
}

func evRequestSum(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:8])
}

// evAwaitRunning waits for an accepted effect whose physical owner is running.
func evAwaitRunning(t *testing.T, f *fixture) []entry {
	t.Helper()
	awaitRunningEffect(t, f)
	return f.history()
}

var evPlatform = sync.OnceValue(func() string {
	out, _ := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output()
	return fmt.Sprintf("%s %s/%s docker %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, strings.TrimSpace(string(out)))
})

// evLog logs the one-line evidence record of a gate case.
func evLog(t *testing.T, image, gate, injected, observed, measured string) {
	t.Helper()
	t.Logf("evidence gate=%q label=%q env=%q image=%s injected=%q observed=%q measured=%q",
		gate, "concrete backend integration, scripted provider", evPlatform(), image, injected, observed, measured)
}

var (
	evStep    = []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "observed"}
	evStarted = evStep[:4]
	evReturn  = evStep[:4]
)

func TestGateFrozenRequestReplay(t *testing.T) {
	for _, tc := range []struct {
		name, failure string
		first         reply
		paid          int64
	}{
		{"overloaded", "status 529 overloaded_error", reply{status: 529, body: `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`}, 2},
		{"max_tokens", `stop_reason "max_tokens"`, reply{stopReason: "max_tokens", text: `{"kind":"exec","payload":"echo truncated > trunc.txt"}`}, 3},
		{"cut", "unexpected EOF", reply{cut: true}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "echo ok >> ok.txt"}, 2: {Kind: core.Return, Payload: "done"}})
			f.model.answer(1, 1, tc.first)
			begin := time.Now()
			r := f.cli(nil, "run", "-goal", "g", "-image", image)
			elapsed := time.Since(begin)
			if r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			h := f.history()
			want := slices.Concat([]string{"request_frozen", "attempt_started", "response_incomplete"}, evStep[1:], evReturn)
			if !slices.Equal(kinds(h), want) {
				t.Fatalf("history = %q", kinds(h))
			}
			frozen, sent := h[0].Facts.Request, f.model.requests()
			if frozen == nil || len(sent) != 3 || !bytes.Equal(sent[0], frozen.Body) || !bytes.Equal(sent[1], frozen.Body) ||
				!bytes.Equal(sent[2], h[7].Facts.Request.Body) {
				t.Fatalf("%d requests do not replay the frozen bytes %+v", len(sent), frozen)
			}
			if frozen.Model != "fixture-model" || frozen.Adapter != "canti-anthropic/2" || !strings.HasPrefix(frozen.Protocol, "anthropic-messages/2023-06-01") {
				t.Errorf("request provenance = %+v", frozen)
			}
			for i, a := range map[int]core.AttemptStarted{1: {Step: 1, Attempt: 1}, 3: {Step: 1, Attempt: 2}} {
				if got := evEvent[core.AttemptStarted](t, h[i]); got != a {
					t.Errorf("history[%d] = %+v, want %+v", i, got, a)
				}
			}
			lost := h[2]
			if got := evEvent[core.ResponseIncomplete](t, lost); got != (core.ResponseIncomplete{Step: 1, Attempt: 1}) ||
				lost.To != core.PhaseRequestFrozen || lost.State.Attempt != 2 || lost.State.RequestRev != 1 {
				t.Errorf("incomplete response = %+v", lost)
			}
			partial := lost.Facts.Response
			if !strings.Contains(lost.Facts.Failure, tc.failure) || partial == nil || partial.Complete || len(partial.Body) == 0 {
				t.Fatalf("incomplete response facts = %+v", lost.Facts)
			}
			switch tc.name {
			case "max_tokens":
				if !bytes.Contains(partial.Body, []byte("echo truncated")) {
					t.Errorf("partial response = %s", partial.Body)
				}
			case "cut":
				if json.Valid(partial.Body) {
					t.Errorf("cut response is whole: %s", partial.Body)
				}
			}
			if got := evEvent[core.ResponseSaved](t, h[4]); got != (core.ResponseSaved{Step: 1, Attempt: 2}) || h[4].Facts.Response.Model != "claude-test" {
				t.Errorf("saved response = %+v", h[4])
			}
			for _, e := range h {
				if e.Kind == "response_loaded" {
					l := evEvent[core.ResponseLoaded](t, e)
					if d, err := core.Decode(l.Body); l.Step == 1 && (l.Attempt != 2 || err != nil || d.Payload != "echo ok >> ok.txt") {
						t.Errorf("step 1 loaded attempt %d: %s", l.Attempt, l.Body)
					}
				}
			}
			if _, err := os.Stat(filepath.Join(f.workspace, "trunc.txt")); !os.IsNotExist(err) {
				t.Errorf("the truncated decision ran: %v", err)
			}
			if data, err := os.ReadFile(filepath.Join(f.workspace, "ok.txt")); err != nil || string(data) != "ok\n" {
				t.Errorf("ok.txt = %q, %v; want one run", data, err)
			}
			if recs := checkReleased(t, f, h); len(recs) != 1 {
				t.Errorf("records %+v, want the one job of attempt 2", recs)
			}
			if s := f.status(); s.Totals.InputTokens != 100*tc.paid || s.Totals.OutputTokens != 10*tc.paid {
				t.Errorf("totals = %+v, want the usage of %d responses", s.Totals, tc.paid)
			}
			evLog(t, image, "frozen request replay", "step 1 attempt 1: "+tc.name,
				fmt.Sprintf("response_incomplete %q, attempt 2 saved and dispatched once, returned", lost.Facts.Failure),
				fmt.Sprintf("request sha256/8 %s, %d bytes sent twice byte-identical, partial %d bytes kept, run %v",
					evRequestSum(frozen.Body), len(frozen.Body), len(partial.Body), elapsed.Round(time.Millisecond)))
		})
	}
}

const evLine = 1000

// evFlood prints lines lines of evLine bytes each and then the line tail-marker.
func evFlood(lines int) string {
	return fmt.Sprintf(`yes "$(printf '%%0%dd' 0)" | head -n %d; echo tail-marker`, evLine-1, lines)
}

var evOmitted = regexp.MustCompile(`\n\.\.\. \d+ bytes omitted \.\.\.\n`)

// evCheckFlood checks the observation of an evFlood of total bytes kept to limit.
func evCheckFlood(t *testing.T, f *fixture, obs *harness.Observation, total, limit int64) {
	t.Helper()
	if obs == nil || obs.Execution != core.ExecutionExited || obs.ExitCode == nil || *obs.ExitCode != 0 {
		t.Fatalf("observation = %+v", obs)
	}
	o := obs.Output
	if o.Complete || !o.Available || o.Bytes != limit || o.Detail != fmt.Sprintf("kept %d of %d bytes", limit, total) || path.Base(o.Path) != "output" {
		t.Errorf("output = %+v", o)
	}
	data, err := os.ReadFile(filepath.Join(f.state, "artifacts", o.Path))
	line := strings.Repeat("0", evLine-1) + "\n"
	if err != nil || int64(len(data)) != limit || string(data) != strings.Repeat(line, int(limit)/evLine+1)[:limit] {
		t.Errorf("artifact holds %d bytes, %v; want the first %d bytes of the output", len(data), err, limit)
	}
	marker := evOmitted.FindString(obs.Preview)
	if marker == "" || len(obs.Preview) > 4<<10 || !strings.HasPrefix(obs.Preview, line) || !strings.HasSuffix(obs.Preview, line+"tail-marker\n") {
		t.Errorf("preview of %d bytes, marker %q: %q...%q", len(obs.Preview), marker, obs.Preview[:min(64, len(obs.Preview))], obs.Preview[max(0, len(obs.Preview)-64):])
	}
}

func TestGateLargeOutput(t *testing.T) {
	const limit = 1 << 20
	t.Run("retained", func(t *testing.T) {
		const lines = 20000
		total := int64(lines*evLine + len("tail-marker\n"))
		f, image := evFixture(t, map[int]core.Decision{
			1: {Kind: core.Exec, Payload: evFlood(lines)},
			2: {Kind: core.Exec, Payload: `for f in ` + archive.ArtifactsRoot + `/*/output; do echo "$f $(wc -c < "$f")"; done`},
			3: {Kind: core.Return, Payload: "done"},
		})
		begin := time.Now()
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-output-limit", fmt.Sprint(limit))
		elapsed := time.Since(begin)
		if r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), slices.Concat(evStep, evStep, evReturn)) {
			t.Fatalf("history = %q", kinds(h))
		}
		obs := h[4].Facts.Observation
		evCheckFlood(t, f, obs, total, limit)
		where := path.Join(archive.ArtifactsRoot, obs.Output.Path)
		sent := f.model.requests()
		if len(sent) != 3 {
			t.Fatalf("%d requests", len(sent))
		}
		step, prompt, err := promptOf(sent[1])
		ref := fmt.Sprintf("Output: %d bytes, incomplete, at %s (kept %d of %d bytes)", limit, where, limit, total)
		if err != nil || step != 2 || !strings.Contains(prompt, ref) || !strings.Contains(prompt, "tail-marker") || len(sent[1]) > 64<<10 {
			t.Errorf("step 2 request of %d bytes lacks %q: %v\n%s", len(sent[1]), ref, err, prompt)
		}
		if read := h[9].Facts.Observation; read == nil || read.Preview != fmt.Sprintf("%s %d\n", where, limit) {
			t.Fatalf("reading the artifact in the job = %+v", read)
		}
		if s := f.status(); s.Totals.OutputBytes != limit+h[9].Facts.Observation.Output.Bytes {
			t.Errorf("status totals = %+v", s.Totals)
		}
		rec := checkReleased(t, f, h)[evJob(t, f, h)]
		evLog(t, image, "large output", fmt.Sprintf("step 1 writes %d bytes, -output-limit %d", total, limit),
			fmt.Sprintf("exited 0, %q, artifact %s readable by the step 2 job, container and its log released, returned", obs.Output.Detail, where),
			fmt.Sprintf("artifact %d bytes, preview %d bytes, step 2 request %d bytes, job %v, run %v",
				limit, len(obs.Preview), len(sent[1]), rec.FinishedAt.Sub(rec.StartedAt).Round(time.Millisecond), elapsed.Round(time.Millisecond)))
	})
	t.Run("disk_budget", func(t *testing.T) {
		const lines = 2000
		total := int64(lines*evLine + len("tail-marker\n"))
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: evFlood(lines)}, 2: {Kind: core.Return, Payload: "unreachable"}})
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-output-limit", fmt.Sprint(limit), "-output-budget", fmt.Sprint(limit))
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: budget_exhausted") {
			t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), append(slices.Clone(evStep), "stop_requested")) {
			t.Fatalf("history = %q", kinds(h))
		}
		evCheckFlood(t, f, h[4].Facts.Observation, total, limit)
		stop := h[5]
		why := fmt.Sprintf("%d output bytes reached the %d-byte budget", limit, limit)
		if evEvent[core.StopRequested](t, stop).Reason != core.StopBudgetExhausted || stop.Facts.Failure != why ||
			stop.From != core.PhaseIdle || stop.To != core.PhaseStopped {
			t.Errorf("stop = %+v", stop)
		}
		if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopBudgetExhausted || s.Step != 2 || s.Totals.OutputBytes != limit || s.Totals.Steps != 1 {
			t.Errorf("status = %+v", s)
		}
		if n := len(f.model.requests()); n != 1 {
			t.Errorf("%d requests after the budget was spent", n)
		}
		checkReleased(t, f, h)
		evLog(t, image, "large output / finite runs (disk)", fmt.Sprintf("step 1 writes %d bytes, -output-limit %d -output-budget %d", total, limit, limit),
			fmt.Sprintf("stopped %s: %q, step 1 observation retained", core.StopBudgetExhausted, stop.Facts.Failure),
			fmt.Sprintf("retained %d bytes, exit %d, 1 model request", limit, r.code))
	})
}

func TestGateCancellationAndTimeout(t *testing.T) {
	const grace, slack = 5 * time.Second, 5 * time.Second
	t.Run("job_timeout", func(t *testing.T) {
		const timeout = 2 * time.Second
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 300 & sleep 300 & wait"}, 2: {Kind: core.Return, Payload: "done"}})
		begin := time.Now()
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-job-timeout", timeout.String())
		elapsed := time.Since(begin)
		if r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), slices.Concat(evStep, evReturn)) {
			t.Fatalf("history = %q", kinds(h))
		}
		obs := h[4].Facts.Observation
		if obs == nil || obs.Execution != core.ExecutionTimedOut || obs.ExitCode == nil || obs.Output.Complete || !strings.Contains(obs.Output.Detail, "cancelled") {
			t.Fatalf("observation = %+v", obs)
		}
		rec := checkReleased(t, f, h)[evJob(t, f, h)]
		ran := rec.FinishedAt.Sub(rec.StartedAt)
		if ran < timeout || ran > timeout+grace+slack {
			t.Errorf("job ran %v, want %v to %v", ran, timeout, timeout+grace+slack)
		}
		if p := f.model.prompts(2); len(p) != 1 || !strings.Contains(p[0], "Execution: timed_out") {
			t.Errorf("step 2 prompt = %q", p)
		}
		evLog(t, image, "cancellation and timeout", "-job-timeout 2s on sleep 300 & sleep 300 & wait",
			fmt.Sprintf("observed %s exit %d, recorded cancelled, container stopped and released, step 2 returned", obs.Execution, *obs.ExitCode),
			fmt.Sprintf("job ran %v (bound %v), run %v", ran.Round(time.Millisecond), timeout+grace+slack, elapsed.Round(time.Millisecond)))
	})
	t.Run("stop_without_controller", func(t *testing.T) {
		const cleanup = 20 * time.Second
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 300"}})
		if r := f.cli([]string{"CANTI_TEST_EXECUTOR_CRASH=running"}, "run", "-goal", "g", "-image", image); r.code != -1 {
			t.Fatalf("crashing run = %d\nstderr: %s", r.code, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), evStarted) {
			t.Fatalf("history after the crash = %q", kinds(h))
		}
		job := evJob(t, f, h)
		if ids := evContainers(t, f.state); len(ids) != 1 || !strings.HasPrefix(job, ids[0]) {
			t.Fatalf("running containers %q, want job %s", ids, job)
		}
		begin := time.Now()
		r := f.cli(nil, "stop", "-cleanup", cleanup.String())
		elapsed := time.Since(begin)
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: cancelled") {
			t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
		}
		obs := evCheckCancelled(t, f, core.StopCancelled)
		if elapsed > cleanup {
			t.Errorf("stop took %v, beyond its %v cleanup", elapsed, cleanup)
		}
		rec := checkReleased(t, f, f.history())[job]
		evLog(t, image, "cancellation and timeout", "SIGKILL after Docker reports the bound owner running, then stop -cleanup 20s",
			fmt.Sprintf("stop_requested cancelled -> observed %s exit %d -> stopped cancelled, container stopped and released", obs.Execution, *obs.ExitCode),
			fmt.Sprintf("stop took %v (bound %v), job ran %v", elapsed.Round(time.Millisecond), cleanup, rec.FinishedAt.Sub(rec.StartedAt).Round(time.Millisecond)))
	})
	t.Run("signal", func(t *testing.T) {
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 300"}})
		cmd, wait := f.start(nil, "run", "-goal", "g", "-image", image)
		job := evJob(t, f, evAwaitRunning(t, f))
		begin := time.Now()
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		r := wait()
		elapsed := time.Since(begin)
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: cancelled") {
			t.Fatalf("interrupted run = %d\nstderr: %s", r.code, r.stderr)
		}
		obs := evCheckCancelled(t, f, core.StopCancelled)
		if elapsed > stopCleanup {
			t.Errorf("the interrupted run took %v to exit, beyond its %v cleanup", elapsed, stopCleanup)
		}
		rec := checkReleased(t, f, f.history())[job]
		evLog(t, image, "cancellation and timeout", "SIGINT to run while job sleep 300 runs",
			fmt.Sprintf("exit %d, stopped cancelled, observed %s exit %d, recorded %s, container released", r.code, obs.Execution, *obs.ExitCode, rec.Status),
			fmt.Sprintf("signal to exit %v (bound %v)", elapsed.Round(time.Millisecond), stopCleanup))
	})
}

// evCheckCancelled checks that the run stopped for reason after its running
// step 1 job was cancelled and its true result recorded, and returns that
// result.
func evCheckCancelled(t *testing.T, f *fixture, reason core.StopReason) *harness.Observation {
	t.Helper()
	h := f.history()
	if !slices.Equal(kinds(h), append(slices.Clone(evStarted), "stop_requested", "observed")) {
		t.Fatalf("history = %q", kinds(h))
	}
	stop, last := h[4], h[5]
	if evEvent[core.StopRequested](t, stop).Reason != reason || stop.From != core.PhaseEffectSaved || stop.To != core.PhaseCancelling {
		t.Errorf("stop = %+v", stop)
	}
	obs := last.Facts.Observation
	if evEvent[core.Observed](t, last).Execution != core.ExecutionCancelled || obs == nil || obs.Execution != core.ExecutionCancelled || obs.ExitCode == nil ||
		obs.Output.Complete || !obs.Output.Available || last.To != core.PhaseStopped || last.State.Reason != reason {
		t.Fatalf("result = %+v", last)
	}
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != reason || s.Totals.Steps != 1 {
		t.Errorf("status = %+v", s)
	}
	return obs
}

func TestGateFiniteRuns(t *testing.T) {
	t.Run("steps", func(t *testing.T) {
		f, image := evFixture(t, map[int]core.Decision{
			1: {Kind: core.Exec, Payload: "echo one"}, 2: {Kind: core.Exec, Payload: "echo two"}, 3: {Kind: core.Return, Payload: "unreachable"},
		})
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-max-steps", "2")
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: steps_exhausted") {
			t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), slices.Concat(evStep, evStep)) || h[9].To != core.PhaseStopped || h[9].State.Reason != core.StopStepsExhausted {
			t.Fatalf("history = %q", kinds(h))
		}
		for i, want := range map[int]string{4: "one\n", 9: "two\n"} {
			if o := h[i].Facts.Observation; o == nil || o.Execution != core.ExecutionExited || o.Preview != want {
				t.Errorf("observation %d = %+v", i, o)
			}
		}
		if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopStepsExhausted || s.Totals.Steps != 2 {
			t.Errorf("status = %+v", s)
		}
		if n := len(f.model.requests()); n != 2 {
			t.Errorf("%d requests", n)
		}
		checkReleased(t, f, h)
		evLog(t, image, "finite runs (steps)", "-max-steps 2 with exec at steps 1 and 2",
			"stopped steps_exhausted after the step 2 observation", fmt.Sprintf("2 observations retained, 2 model requests, exit %d", r.code))
	})
	t.Run("attempts", func(t *testing.T) {
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "echo never > never.txt"}})
		failed := reply{status: 500, body: `{"type":"error","error":{"type":"api_error","message":"Internal server error"}}`}
		f.model.answer(1, 1, failed)
		f.model.answer(1, 2, failed)
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-max-attempts", "2")
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: attempts_exhausted") {
			t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), []string{"request_frozen", "attempt_started", "response_incomplete", "attempt_started", "response_incomplete"}) ||
			h[4].To != core.PhaseStopped || h[4].State.Reason != core.StopAttemptsExhausted {
			t.Fatalf("history = %q", kinds(h))
		}
		for _, i := range []int{2, 4} {
			if !strings.Contains(h[i].Facts.Failure, "status 500 api_error") {
				t.Errorf("failure %d = %q", i, h[i].Facts.Failure)
			}
		}
		sent := f.model.requests()
		if len(sent) != 2 || !bytes.Equal(sent[0], h[0].Facts.Request.Body) || !bytes.Equal(sent[1], sent[0]) {
			t.Errorf("%d requests do not replay the frozen bytes", len(sent))
		}
		if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopAttemptsExhausted || s.Attempt != 2 {
			t.Errorf("status = %+v", s)
		}
		if _, err := os.Stat(filepath.Join(f.workspace, "never.txt")); !os.IsNotExist(err) {
			t.Errorf("something ran: %v", err)
		}
		checkReleased(t, f, h)
		evLog(t, image, "finite runs (attempts)", "HTTP 500 on every attempt, -max-attempts 2",
			"stopped attempts_exhausted with both failures recorded, nothing dispatched", fmt.Sprintf("2 byte-identical requests, exit %d", r.code))
	})
	t.Run("wall_time", func(t *testing.T) {
		const wall, grace, slack = 5 * time.Second, 5 * time.Second, 5 * time.Second
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "sleep 60"}})
		begin := time.Now()
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-wall-time", wall.String())
		elapsed := time.Since(begin)
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: timed_out") {
			t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
		}
		obs := evCheckCancelled(t, f, core.StopTimedOut)
		if why := f.history()[4].Facts.Failure; !strings.Contains(why, "wall-time budget") {
			t.Errorf("stop failure = %q", why)
		}
		if elapsed > wall+grace+slack {
			t.Errorf("run took %v, want at most %v", elapsed, wall+grace+slack)
		}
		h := f.history()
		rec := checkReleased(t, f, h)[evJob(t, f, h)]
		evLog(t, image, "finite runs (wall time)", "-wall-time 5s on sleep 60",
			fmt.Sprintf("stopped timed_out, job observed %s exit %d, container stopped and released", obs.Execution, *obs.ExitCode),
			fmt.Sprintf("run %v (bound %v), job ran %v", elapsed.Round(time.Millisecond), wall+grace+slack, rec.FinishedAt.Sub(rec.StartedAt).Round(time.Millisecond)))
	})
	t.Run("tokens", func(t *testing.T) {
		f, image := evFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "echo spent > spent.txt"}})
		r := f.cli(nil, "run", "-goal", "g", "-image", image, "-token-budget", "100")
		if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: budget_exhausted") {
			t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
		}
		h := f.history()
		if !slices.Equal(kinds(h), slices.Concat(evReturn, []string{"stop_requested", "observed"})) {
			t.Fatalf("history = %q", kinds(h))
		}
		stop, last := h[4], h[5]
		if evEvent[core.StopRequested](t, stop).Reason != core.StopBudgetExhausted || stop.Facts.Failure != "110 tokens reached the 100-token budget" ||
			stop.From != core.PhaseEffectSaved || stop.To != core.PhaseCancelling {
			t.Errorf("stop = %+v", stop)
		}
		if o := last.Facts.Observation; o == nil || o.Execution != core.ExecutionCancelled || o.Output != (harness.ArtifactRef{}) || last.Facts.Receipt == nil || !last.Facts.Receipt.Fenced || last.Facts.Receipt.ID != "" ||
			last.To != core.PhaseStopped || last.State.Reason != core.StopBudgetExhausted {
			t.Errorf("result = %+v", last)
		}
		if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopBudgetExhausted || s.Totals.InputTokens != 100 || s.Totals.OutputTokens != 10 {
			t.Errorf("status = %+v", s)
		}
		if _, err := os.Stat(filepath.Join(f.workspace, "spent.txt")); !os.IsNotExist(err) {
			t.Errorf("the effect ran after the budget was spent: %v", err)
		}
		checkReleased(t, f, h)
		evLog(t, image, "finite runs (tokens)", "-token-budget 100 below one response's 110 tokens",
			fmt.Sprintf("stopped budget_exhausted: %q, effect fenced before bind", stop.Facts.Failure), fmt.Sprintf("110 tokens, no container, exit %d", r.code))
	})
}
