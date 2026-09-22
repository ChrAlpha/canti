package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
)

// Runner interprets core intents against the durable store and the effect
// executor. Every I/O happens after the phase that authorizes it is committed.
type Runner struct {
	store    Store
	executor Executor
}

func NewRunner(store Store, executor Executor) (Runner, error) {
	if store == nil {
		return Runner{}, errors.New("harness: store is required")
	}
	if executor == nil {
		return Runner{}, errors.New("harness: executor is required")
	}
	return Runner{store: store, executor: executor}, nil
}

// Commit derives the next state for event and records it together with the
// facts the event requires. On any error the returned state is unchanged.
func (r Runner) Commit(ctx context.Context, state core.State, event core.Event, facts Facts) (core.State, error) {
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if err := facts.check(event); err != nil {
		return state, err
	}
	next, err := state.Next(event)
	if err != nil {
		return state, err
	}
	if err := r.store.Commit(ctx, Transition{From: state, Event: event, To: next, Facts: facts}); err != nil {
		return state, fmt.Errorf("harness: commit %T: %w", event, err)
	}
	return next, nil
}

// Dispatch hands the committed effect to the executor and commits what it
// learned. Executor failure or an unknown status becomes an unknown outcome;
// cancellation leaves the effect committed so a resume can find its job.
func (r Runner) Dispatch(ctx context.Context, state core.State) (core.State, error) {
	if state.Intent() != core.IntentDispatch {
		return state, fmt.Errorf("harness: phase %q does not dispatch", state.Phase)
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	job, err := r.executor.StartOrResume(ctx, Effect{ID: state.EffectID, Command: state.Decision.Payload})
	if err != nil {
		if ctx.Err() != nil {
			return state, ctx.Err()
		}
		return r.Commit(ctx, state, core.OutcomeUnknown{}, Facts{Failure: err.Error()})
	}
	if job.Status == JobUnknown {
		return r.Commit(ctx, state, core.OutcomeUnknown{}, Facts{})
	}
	return r.Commit(ctx, state, core.JobStarted{JobID: job.ID}, Facts{})
}

func (f Facts) check(event core.Event) error {
	switch e := event.(type) {
	case core.RequestFrozen:
		if f.Request == nil || len(f.Request.Body) == 0 {
			return errors.New("harness: frozen request bytes are required")
		}
	case core.ResponseSaved:
		if f.Response == nil || !f.Response.Complete {
			return errors.New("harness: complete response is required")
		}
	case core.ResponseIncomplete:
		if f.Response != nil && f.Response.Complete {
			return errors.New("harness: incomplete response cannot be marked complete")
		}
	case core.Observed:
		return f.checkObservation(e.Execution)
	case core.Resolved:
		return f.checkObservation(e.Execution)
	}
	return nil
}

func (f Facts) checkObservation(execution core.ExecutionState) error {
	if f.Observation == nil {
		return errors.New("harness: observation record is required")
	}
	if f.Observation.Execution != execution {
		return fmt.Errorf("harness: observation %q does not match event %q", f.Observation.Execution, execution)
	}
	return nil
}
