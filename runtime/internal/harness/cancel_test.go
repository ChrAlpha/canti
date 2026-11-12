package harness_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestRequestCancelEffectAcknowledgesTheCommittedObservation(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("sleep 30"))
	l := f.loop()
	snap := f.steps(l, 3)
	if snap.State.Phase != core.PhaseEffectSaved {
		t.Fatalf("running effect = %+v", snap.State)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := l.RequestCancelEffect(ctx, runID)
		done <- err
	}()
	// The request is in the loop's channel before the effect watch starts.
	time.Sleep(20 * time.Millisecond)
	next, err := l.Step(context.Background(), runID)
	if err != nil || next.State.Phase != core.PhaseIdle {
		t.Fatalf("Step after cancel = %+v, %v", next.State, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancel acknowledgment = %v, want the committed observation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel caller was not acknowledged")
	}
	if n := count(f.exec.log(), "cancel"); n != 1 {
		t.Fatalf("executor cancels = %d, want once", n)
	}
}

type cancelKey struct{}
type cancelLatestGate struct {
	harness.Journal
	entered, release chan struct{}
	once             sync.Once
}

func (j *cancelLatestGate) Latest(ctx context.Context, id string) (harness.Snapshot, error) {
	snap, err := j.Journal.Latest(ctx, id)
	if ctx.Value(cancelKey{}) != nil {
		j.once.Do(func() { close(j.entered); <-j.release })
	}
	return snap, err
}

func TestCancelDoesNotReachTheNextEffect(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("first"), execReply("second"))
	gate := &cancelLatestGate{Journal: f.store, entered: make(chan struct{}), release: make(chan struct{})}
	l := f.loopOn(gate)
	first := f.steps(l, 3)
	if first.State.Phase != core.PhaseEffectSaved {
		t.Fatalf("first effect = %+v", first.State)
	}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), cancelKey{}, true), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := l.RequestCancelEffect(ctx, runID)
		done <- err
	}()
	<-gate.entered
	// The inspected effect completes and a second effect is accepted before
	// the request reaches the loop.
	second := f.steps(l, 4)
	if second.State.Phase != core.PhaseEffectSaved || second.State.Step != 2 {
		t.Fatalf("second effect = %+v", second.State)
	}
	close(gate.release)
	time.Sleep(30 * time.Millisecond)
	if _, err := l.Step(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	<-done
	if f.exec.fenced[second.State.EffectID] {
		t.Fatalf("a cancellation of effect %s fenced later effect %s", first.State.EffectID, second.State.EffectID)
	}
}

func TestStaleCancelDuringNextEffectWait(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("first"), execReply("second"))
	gate := &cancelLatestGate{Journal: f.store, entered: make(chan struct{}), release: make(chan struct{})}
	waitingEntered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	x := func(ctx context.Context, id string) error {
		once.Do(func() { close(waitingEntered) })
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l, err := harness.NewLoop(gate, waiting{f.exec, x}, f.model, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := f.steps(l, 3)
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), cancelKey{}, true), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := l.RequestCancelEffect(ctx, runID)
		done <- err
	}()
	<-gate.entered
	second := f.steps(l, 4)
	if second.State.Phase != core.PhaseEffectSaved || second.State.Step != 2 {
		t.Fatal(second.State)
	}
	f.exec.hold = true
	stepDone := make(chan error, 1)
	go func() {
		_, err := l.Step(f.bounded(), runID)
		stepDone <- err
	}()
	<-waitingEntered
	close(gate.release)
	cancelErr := <-done
	f.exec.set(second.State.EffectID, succeed)
	close(finish)
	if err := <-stepDone; err != nil {
		t.Fatal(err)
	}
	f.exec.mu.Lock()
	fenced := f.exec.fenced[second.State.EffectID]
	f.exec.mu.Unlock()
	if fenced {
		t.Fatalf("stale cancellation of %s cancelled later waiting effect %s; reply=%v", first.State.EffectID, second.State.EffectID, cancelErr)
	}
}
