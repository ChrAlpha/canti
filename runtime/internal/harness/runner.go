package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
)

// ErrFacts reports facts that are missing, inconsistent, or not permitted for
// the committed event.
var ErrFacts = errors.New("harness: facts do not match event")

// Runner derives and commits core transitions with their permitted facts.
// Physical execution belongs to the executor called by Loop.
type Runner struct {
	store Store
}

// NewRunner wires the durable store.
func NewRunner(store Store) (Runner, error) {
	if store == nil {
		return Runner{}, errors.New("harness: store is required")
	}
	return Runner{store: store}, nil
}

// Commit derives the next state for event and records it at the next
// sequence together with the facts the event permits. On an error the
// returned snapshot is unchanged; core errors keep their sentinels and fact
// errors wrap ErrFacts. A rejected transition writes nothing, but a store
// error can leave the commit outcome unknown: retrying the same snapshot and
// event reproduces the same event ID, which the store confirms without
// recording twice. Commit writes nothing once ctx is done, so facts learned
// after the harness's own context was cancelled need a separate context.
func (r Runner) Commit(ctx context.Context, snap Snapshot, event core.Event, facts Facts) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return snap, err
	}
	if err := snap.Validate(); err != nil {
		return snap, err
	}
	next, err := snap.State.Next(event)
	if err != nil {
		return snap, err
	}
	if err := facts.check(event, next); err != nil {
		return snap, err
	}
	if err := facts.checkWindows(snap.State.Step); err != nil {
		return snap, fmt.Errorf("%w: %v", ErrFacts, err)
	}
	id, err := core.EventID(snap.State.RunID, snap.Seq+1, event)
	if err != nil {
		return snap, err
	}
	transition := Transition{
		Seq:       snap.Seq + 1,
		EventID:   id,
		From:      snap.State,
		Event:     event,
		To:        next,
		Facts:     facts,
		Semantics: core.SemanticsVersion,
	}
	if err := r.store.Commit(ctx, transition); err != nil {
		return snap, fmt.Errorf("harness: commit %T: %w", event, err)
	}
	return Snapshot{Seq: transition.Seq, State: next}, nil
}

// check reports whether f holds exactly the records event permits, which
// leads to next.
func (f Facts) check(event core.Event, next core.State) error {
	var allowed Facts
	switch e := event.(type) {
	case core.RequestFrozen:
		if f.Request == nil || len(f.Request.Body) == 0 {
			return fmt.Errorf("%w: frozen request bytes are required", ErrFacts)
		}
		allowed.Request = f.Request
	case core.ResponseSaved:
		if f.Response == nil || !f.Response.Complete || len(f.Response.Body) == 0 {
			return fmt.Errorf("%w: complete response with a body is required", ErrFacts)
		}
		allowed.Response = f.Response
	case core.ResponseIncomplete:
		if f.Response != nil && f.Response.Complete {
			return fmt.Errorf("%w: incomplete response cannot be marked complete", ErrFacts)
		}
		allowed.Response, allowed.Failure = f.Response, f.Failure
	case core.ResponseLoaded:
		if next.Phase == core.PhaseEffectSaved {
			if _, err := Acceptance(core.SemanticsVersion, f.Action, f.Execution); err != nil {
				return fmt.Errorf("%w: %v", ErrFacts, err)
			}
			allowed.Action, allowed.Execution = f.Action, f.Execution
		}
	case core.Performed:
		if f.Outcome == nil {
			return fmt.Errorf("%w: a performed effect requires its outcome", ErrFacts)
		}
		if err := f.Outcome.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrFacts, err)
		}
		allowed.Outcome, allowed.Window = f.Outcome, f.Window
	case core.Observed:
		if err := f.checkObservation(e.Execution); err != nil {
			return err
		}
		allowed.Observation, allowed.Receipt = f.Observation, f.Receipt
	case core.Resolved:
		if err := f.checkObservation(e.Execution); err != nil {
			return err
		}
		if f.Resolution == nil {
			return fmt.Errorf("%w: resolution provenance is required", ErrFacts)
		}
		allowed.Observation, allowed.Receipt, allowed.Resolution = f.Observation, f.Receipt, f.Resolution
	case core.EffectRecovered:
		if f.Receipt == nil {
			return fmt.Errorf("%w: recovery receipt is required", ErrFacts)
		}
		allowed.Receipt = f.Receipt
	case core.OutcomeUnknown:
		allowed.Failure, allowed.Receipt = f.Failure, f.Receipt
	case core.StopRequested:
		allowed.Failure = f.Failure
	}
	if f != allowed {
		return fmt.Errorf("%w: records do not belong to %T", ErrFacts, event)
	}
	return nil
}

func (f Facts) checkObservation(execution core.ExecutionState) error {
	if f.Observation == nil {
		return fmt.Errorf("%w: observation record is required", ErrFacts)
	}
	if f.Observation.Execution != execution {
		return fmt.Errorf("%w: observation %q does not match event %q", ErrFacts, f.Observation.Execution, execution)
	}
	if f.Observation.Retrieval != nil || f.Observation.Handover != nil {
		return fmt.Errorf("%w: an observation holds no retrieval or handover; a performed outcome does", ErrFacts)
	}
	return nil
}

// checkWindows validates the window a request renders at step and the
// window a performed step commits for the step after it.
func (f Facts) checkWindows(step int) error {
	if f.Request != nil && f.Request.Window != nil {
		if err := f.Request.Window.Validate(step); err != nil {
			return err
		}
	}
	if f.Window != nil {
		return f.Window.Validate(step + 1)
	}
	return nil
}
