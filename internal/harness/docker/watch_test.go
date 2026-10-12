package docker

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
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
	lf.step(2, core.PhaseAwaitingObservation)
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
	l, err := harness.NewLoop(store, f.x, execModel{}, harness.LoopConfig{
		Environment: "fake",
		History:     t.TempDir(),
		Poll:        time.Millisecond, InspectRetries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	lf := &loopFixture{fake: f, t: t, ctx: ctx, store: store, loop: l}
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
	if _, ok := last.Event.(core.OutcomeUnknown); !ok || !strings.Contains(last.Facts.Failure, "inspect failed 3 consecutive times") ||
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

func TestLoopObservesAnEffectFencedBeforeItsBind(t *testing.T) {
	lf := saveEffect(t, newFake(t))
	if err := os.MkdirAll(lf.x.dir(lf.effect), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := setLatch(lf.x.dir(lf.effect), latchCancel); err != nil {
		t.Fatal(err)
	}
	lf.fail("inspect", http.StatusNotFound, "No such container: canti-"+lf.effect)
	lf.step(3, core.PhaseIdle)
	ts, err := lf.store.Transitions(lf.ctx, "run-1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var bound core.JobBound
	var obs *harness.Observation
	for _, tr := range ts {
		switch e := tr.Event.(type) {
		case core.JobBound:
			bound = e
		case core.Observed:
			obs = tr.Facts.Observation
		}
	}
	want := harness.Observation{Execution: core.ExecutionCancelled, Output: harness.ArtifactRef{Path: lf.effect + "/output", Available: true, Detail: "job was cancelled; output is partial"}}
	if bound.JobID != "fenced:"+lf.effect || obs == nil || !reflect.DeepEqual(*obs, want) {
		t.Fatalf("bound job %q, observation %+v; want the fenced job observed as %+v", bound.JobID, obs, want)
	}
	for _, op := range []string{"create", "start", "stop", "logs", "cp", "rm"} {
		if calls := lf.calls(op); len(calls) != 0 {
			t.Fatalf("%s calls = %q, want none for an effect fenced before its bind", op, calls)
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
