package docker

import (
	"context"
	"os"
	"path/filepath"
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
	f.set("inspect.out", f.containerOf(lf.effect, "true", "created", 0))
	f.set("running.json", f.containerOf(lf.effect, "true", "running", 0))
	f.set("start.sh", `cp "$ctl/running.json" "$ctl/inspect.out"`+"\n")
	lf.step(2, core.PhaseAwaitingObservation)
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
	lf.set("inspect.sh", "exec sleep 30\n")
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
