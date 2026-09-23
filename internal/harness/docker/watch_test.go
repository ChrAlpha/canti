package docker

import (
	"context"
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
	return harness.Request{Body: []byte(p.User), Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, nil
}

func (execModel) Complete(context.Context, harness.Request) (harness.Response, error) {
	return harness.Response{Body: []byte(`{"kind":"exec","payload":"true"}`), Complete: true, Model: "fake-1", Protocol: "fake/v1", Adapter: "fake"}, nil
}

func (execModel) Decision(resp harness.Response) []byte { return resp.Body }

func TestLoopGivesUpOnAHungDaemon(t *testing.T) {
	f := newFake(t)
	f.x.timeouts.call = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state, err := core.NewState("run-1", core.Limits{MaxSteps: 2, MaxAttempts: 1})
	if err == nil {
		err = store.CreateRun(ctx, harness.Run{ID: "run-1", Goal: "Run true.", Created: time.Now()}, state)
	}
	if err != nil {
		t.Fatal(err)
	}
	l, err := harness.NewLoop(store, f.x, execModel{}, harness.LoopConfig{
		Environment: "fake", ArtifactDir: ArtifactDir, Workspace: t.TempDir(), NotesFile: "notes.md", NotesPath: WorkDir + "/notes.md",
		Poll: time.Millisecond, InspectRetries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	step := func(n int, want core.Phase) harness.Snapshot {
		t.Helper()
		var snap harness.Snapshot
		for range n {
			if snap, err = l.Step(ctx, "run-1"); err != nil {
				t.Fatalf("Step() = %v at %+v", err, snap.State)
			}
		}
		if snap.State.Phase != want {
			t.Fatalf("phase = %q, want %q", snap.State.Phase, want)
		}
		return snap
	}
	id := step(3, core.PhaseEffectSaved).State.EffectID
	f.set("inspect.out", f.containerOf(id, "true", "created", 0))
	f.set("running.json", f.containerOf(id, "true", "running", 0))
	f.set("start.sh", `cp "$ctl/running.json" "$ctl/inspect.out"`+"\n")
	step(2, core.PhaseAwaitingObservation)
	f.set("inspect.sh", "exec sleep 5\n")
	before := len(f.calls("inspect"))
	step(1, core.PhaseNeedsResolution)
	ts, err := store.Transitions(ctx, "run-1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	last := ts[len(ts)-1]
	if _, ok := last.Event.(core.OutcomeUnknown); !ok || !strings.Contains(last.Facts.Failure, "inspect failed 3 consecutive times") ||
		!strings.Contains(last.Facts.Failure, "no response within") {
		t.Fatalf("last transition = %+v, want an unknown outcome after the inspect retries", last)
	}
	if n := len(f.calls("inspect")) - before; n != 3 {
		t.Fatalf("inspected %d times, want 3", n)
	}
}
