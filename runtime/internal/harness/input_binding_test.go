package harness_test

import (
	"context"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

// lateSteer admits another steer between the loop's read of the run's steers
// and its InputsBound commit, as a concurrent principal submission does.
type lateSteer struct {
	harness.Journal
	store    *sqlite.Store
	injected bool
}

func (j *lateSteer) Steers(ctx context.Context, runID string) ([]harness.Input, error) {
	rows, err := j.Journal.Steers(ctx, runID)
	if !j.injected {
		j.injected = true
		_, err = j.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal,
			Delivery: harness.DeliverySteer, Text: "second", Now: created})
	}
	return rows, err
}

func TestSteerBetweenReadAndBindKeepsTheDrive(t *testing.T) {
	f := newFixture(t, limits, harness.Budget{}, execReply("true"))
	if _, err := f.store.Admit(context.Background(), harness.InputSubmission{Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliverySteer, Text: "first", Now: created}); err != nil {
		t.Fatal(err)
	}
	l := f.loopOn(&lateSteer{Journal: f.store, store: f.store})
	snap, err := l.Step(f.bounded(), runID)
	if err != nil {
		t.Fatalf("prepare with a steer admitted between read and bind = %v", err)
	}
	if snap.State.Phase != core.PhaseRequestFrozen || snap.State.Inputs != 1 {
		t.Fatalf("prefix request = %+v", snap.State)
	}
	// The second steer remains unbound until the next binding point, where
	// the frozen decision is superseded and the next request sees both.
	snap, err = l.Step(f.bounded(), runID)
	if err != nil || snap.State.Phase != core.PhaseResponseSaved {
		t.Fatalf("send = %+v, %v", snap.State, err)
	}
	snap, err = l.Step(f.bounded(), runID)
	if err != nil {
		t.Fatalf("decode = %v", err)
	}
	if snap.State.Phase != core.PhaseIdle || snap.State.Step != 2 || snap.State.Inputs != 2 {
		t.Fatalf("late steer was not bound at the next point: %+v", snap.State)
	}
}
