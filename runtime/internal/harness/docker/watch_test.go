package docker

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

type execModel struct{}

func (execModel) Freeze(p harness.Prompt) (harness.Request, error) {
	return harness.Request{Body: []byte(p.User()), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, nil
}

func (execModel) Complete(context.Context, harness.Request) (harness.Response, error) {
	return harness.Response{Body: []byte(`{"kind":"exec","payload":"true"}`), Complete: true, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, nil
}

func (execModel) Decision(resp harness.Response) []byte { return resp.Body }

type loopFixture struct {
	*fake
	t      *testing.T
	ctx    context.Context
	store  *sqlite.Store
	cfg    harness.LoopConfig
	loop   *harness.Loop
	effect string
}

// newLoop runs a loop on f until its job runs, so its effect is awaiting
// observation.
func newLoop(t *testing.T, f *fake) *loopFixture {
	t.Helper()
	lf := saveEffect(t, f)
	f.reply("inspect", f.containerOf(lf.effect, "true", "created", 0))
	running := f.containerOf(lf.effect, "true", "running", 0)
	f.hook("start", func(*http.Request) { f.reply("inspect", running) })
	if _, err := f.x.Drive(lf.ctx, harness.Effect{ID: lf.effect, Command: "true", Spec: f.x.Specification("true")}); err != nil {
		t.Fatal(err)
	}
	return lf
}

// saveEffect runs a loop on f until its effect is saved, so it binds next.
func saveEffect(t *testing.T, f *fake) *loopFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	state, err := core.NewState("run-1", core.Limits{MaxSteps: 2, MaxAttempts: 1})
	if err == nil {
		err = store.CreateRun(ctx, harness.Run{ID: "run-1", Goal: "Run true.", Created: time.Now()}, state)
	}
	if err != nil {
		t.Fatal(err)
	}
	cfg := harness.LoopConfig{
		Environment: "fake",
		History:     t.TempDir(),
		Poll:        time.Millisecond, InspectRetries: 2,
	}
	l, err := harness.NewLoop(store, f.x, execModel{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	lf := &loopFixture{fake: f, t: t, ctx: ctx, store: store, cfg: cfg, loop: l}
	lf.effect = lf.step(3, core.PhaseEffectSaved).State.EffectID
	return lf
}

func (lf *loopFixture) step(n int, want core.Phase) harness.Snapshot {
	lf.t.Helper()
	var snap harness.Snapshot
	var err error
	for range n {
		if snap, err = lf.loop.Step(lf.ctx, "run-1"); err != nil {
			lf.t.Fatalf("Step() = %v at %+v", err, snap.State)
		}
	}
	if snap.State.Phase != want {
		lf.t.Fatalf("phase = %q, want %q", snap.State.Phase, want)
	}
	return snap
}

func (lf *loopFixture) wantUnknown(failure string) {
	lf.t.Helper()
	ts, err := lf.store.Transitions(lf.ctx, "run-1", 0, 100)
	if err != nil {
		lf.t.Fatal(err)
	}
	last := ts[len(ts)-1]
	if _, ok := last.Event.(core.OutcomeUnknown); !ok ||
		!strings.Contains(last.Facts.Failure, failure) {
		lf.t.Fatalf("last transition = %+v, want an unknown outcome after the inspect retries", last)
	}
}

func TestLoopGivesUpOnAHungDaemon(t *testing.T) {
	lf := newLoop(t, newFake(t))
	lf.x.timeouts.call = 50 * time.Millisecond
	lf.hang("inspect")
	before := len(lf.calls("inspect"))
	lf.step(1, core.PhaseNeedsResolution)
	lf.wantUnknown("no response within 50ms")
	if n := len(lf.calls("inspect")) - before; n != 3 {
		t.Fatalf("inspected %d times, want 3", n)
	}
}

func TestLoopStopLeavesAnUnreadableRecordToResolution(t *testing.T) {
	lf := newLoop(t, newFake(t))
	if err := os.WriteFile(lf.x.record(lf.effect), []byte(`{"preview":"hello\n","output":{"Path":"x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	snap, err := lf.loop.Stop(lf.ctx, "run-1", core.StopCancelled)
	if err != nil || snap.State.Phase != core.PhaseNeedsResolution || snap.State.Reason != core.StopCancelled {
		t.Fatalf("Stop() = %+v, %v; want the run to await resolution", snap.State, err)
	}
	lf.wantUnknown("record of effect")
	if stops := lf.calls("stop"); len(stops) != 0 {
		t.Fatalf("stops = %q, want a recorded job never stopped", stops)
	}
}

func TestLoopObservesAnEffectFencedBeforeDrive(t *testing.T) {
	lf := saveEffect(t, newFake(t))
	lf.fail("inspect", 404, "No such container: canti-"+lf.effect)
	e := harness.Effect{ID: lf.effect, Command: "true", Spec: lf.x.Specification("true")}
	if _, err := lf.x.Cancel(lf.ctx, e); err != nil {
		t.Fatal(err)
	}
	lf.step(1, core.PhaseIdle)
	rows, err := lf.store.Transitions(lf.ctx, "run-1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	last := rows[len(rows)-1]
	if last.Facts.Receipt == nil || last.Facts.Receipt.ID != "" || last.Facts.Observation.Execution != core.ExecutionCancelled {
		t.Fatalf("fenced completion: %+v", last)
	}
	for _, op := range []string{"create", "start", "stop", "logs", "cp", "rm"} {
		if len(lf.calls(op)) != 0 {
			t.Fatal(op, lf.calls(op))
		}
	}
}

func TestLoopStopIsNotHeldByAHungListing(t *testing.T) {
	lf := newLoop(t, newFake(t))
	lf.hang("ps")
	stopped := lf.containerOf(lf.effect, "true", "exited", 143)
	lf.hook("stop", func(*http.Request) { lf.reply("inspect", stopped) })
	ctx, cancel := context.WithTimeout(lf.ctx, 2*time.Second)
	defer cancel()
	snap, err := lf.loop.Stop(ctx, "run-1", core.StopCancelled)
	if err != nil || snap.State.Phase != core.PhaseStopped {
		t.Fatalf("Stop() = %+v, %v; want the cancelled job observed within the cleanup deadline", snap.State, err)
	}
	ts, err := lf.store.Transitions(lf.ctx, "run-1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := ts[len(ts)-1].Event.(core.Observed); !ok || e.Execution != core.ExecutionCancelled {
		t.Fatalf("last transition = %+v, want the cancelled job observed", ts[len(ts)-1])
	}
}

func TestLoopObservesTheExitOfAWaitedJobAtOnce(t *testing.T) {
	lf := newLoop(t, newFake(t))
	lf.cfg.Poll = time.Hour
	l, err := harness.NewLoop(lf.store, lf.x, execModel{}, lf.cfg)
	if err != nil {
		t.Fatal(err)
	}
	exited := lf.containerOf(lf.effect, "true", "exited", 0)
	lf.hook("wait", func(*http.Request) {
		time.Sleep(100 * time.Millisecond)
		lf.reply("inspect", exited)
		lf.reply("cp", exitArchive(t, "0\n"))
	})
	lf.output("done\n")
	began := time.Now()
	snap, err := l.Step(lf.ctx, "run-1")
	if err != nil || snap.State.Phase != core.PhaseIdle || time.Since(began) > 5*time.Second {
		t.Fatalf("Step() = %+v, %v after %v; want the exit observed at once despite a poll interval of an hour", snap.State, err, time.Since(began))
	}
	if waits := lf.calls("wait"); len(waits) != 1 || waits[0] != "wait c-"+lf.effect+" not-running" {
		t.Fatalf("waits = %q, want one for the effect's container to stop running", waits)
	}
}
