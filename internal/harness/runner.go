package harness

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
)

// ErrFacts reports facts that are missing, inconsistent, or not permitted for
// the committed event.
var ErrFacts = errors.New("harness: facts do not match event")

// Runner interprets core intents against the durable store and the effect
// executor. Every I/O happens after the phase that authorizes it is committed.
type Runner struct {
	store    Store
	executor Executor
}

// NewRunner wires the durable store and the effect executor.
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
// facts the event permits. On any error the returned state is unchanged and
// nothing is written; core errors keep their sentinels and fact errors wrap
// ErrFacts. Commit writes nothing once ctx is done, so facts learned after the
// harness's own context was cancelled need a separate context.
func (r Runner) Commit(ctx context.Context, state core.State, event core.Event, facts Facts) (core.State, error) {
	if err := ctx.Err(); err != nil {
		return state, err
	}
	if err := state.Validate(); err != nil {
		return state, err
	}
	next, err := state.Next(event)
	if err != nil {
		return state, err
	}
	if err := facts.check(event); err != nil {
		return state, err
	}
	if err := r.store.Commit(ctx, Transition{From: state, Event: event, To: next, Facts: facts}); err != nil {
		return state, fmt.Errorf("harness: commit %T: %w", event, err)
	}
	return next, nil
}

// Bind hands the committed effect to the executor and commits the job that
// owns it. A returned job ID becomes JobBound whatever its status snapshot;
// an executor error or a job without an ID becomes OutcomeUnknown with the
// failure text. Cancellation observed around the call returns without
// committing, so a resume finds the job by effect ID.
func (r Runner) Bind(ctx context.Context, state core.State) (core.State, error) {
	if state.Intent() != core.IntentBind {
		return state, fmt.Errorf("harness: phase %q does not bind", state.Phase)
	}
	if err := state.Validate(); err != nil {
		return state, err
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	job, err := r.executor.Bind(ctx, Effect{ID: state.EffectID, Command: state.Decision.Payload})
	if err == nil && job.ID == "" {
		err = fmt.Errorf("executor returned no job ID with status %q", job.Status)
	}
	if err != nil {
		return r.keepUnknown(ctx, state, err)
	}
	return r.Commit(ctx, state, core.JobBound{EffectID: state.EffectID, JobID: job.ID}, Facts{})
}

// Start issues the start of the bound job and commits that it was issued. An
// executor error or a job other than the bound one becomes OutcomeUnknown
// with the failure text, because the start may or may not have happened.
// Cancellation observed around the call returns without committing.
func (r Runner) Start(ctx context.Context, state core.State) (core.State, error) {
	if state.Intent() != core.IntentStart {
		return state, fmt.Errorf("harness: phase %q does not start", state.Phase)
	}
	if err := state.Validate(); err != nil {
		return state, err
	}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	job, err := r.executor.Start(ctx, state.EffectID)
	if err == nil && job.ID != state.JobID {
		err = fmt.Errorf("executor started job %q instead of bound job %q", job.ID, state.JobID)
	}
	if err != nil {
		return r.keepUnknown(ctx, state, err)
	}
	return r.Commit(ctx, state, core.JobStarted{EffectID: state.EffectID, JobID: state.JobID}, Facts{})
}

func (r Runner) keepUnknown(ctx context.Context, state core.State, cause error) (core.State, error) {
	if ctx.Err() != nil {
		return state, ctx.Err()
	}
	next, commitErr := r.Commit(ctx, state, core.OutcomeUnknown{EffectID: state.EffectID}, Facts{Failure: cause.Error()})
	if commitErr != nil {
		return state, fmt.Errorf("%w; executor: %w", commitErr, cause)
	}
	return next, nil
}

func (f Facts) check(event core.Event) error {
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
	case core.Observed:
		if err := f.checkObservation(e.Execution); err != nil {
			return err
		}
		allowed.Observation = f.Observation
	case core.Resolved:
		if err := f.checkObservation(e.Execution); err != nil {
			return err
		}
		allowed.Observation = f.Observation
	case core.OutcomeUnknown:
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
	return nil
}
