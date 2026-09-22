package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	if err := facts.check(event); err != nil {
		return snap, err
	}
	id, err := eventID(snap.State.RunID, snap.Seq+1, event)
	if err != nil {
		return snap, err
	}
	transition := Transition{
		Seq:     snap.Seq + 1,
		EventID: id,
		From:    snap.State,
		Event:   event,
		To:      next,
		Facts:   facts,
	}
	if err := r.store.Commit(ctx, transition); err != nil {
		return snap, fmt.Errorf("harness: commit %T: %w", event, err)
	}
	return Snapshot{Seq: transition.Seq, State: next}, nil
}

func eventID(runID string, seq int64, event core.Event) (string, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("harness: encode %s event: %w", event.Kind(), err)
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s:%d:%s:%s", len(runID), runID, seq, event.Kind(), payload))
	return "event_" + hex.EncodeToString(sum[:]), nil
}

// Bind hands the committed effect to the executor and commits the job that
// owns it. A returned job ID becomes JobBound whatever its status snapshot;
// an executor error or a job without an ID becomes OutcomeUnknown with the
// failure text. Cancellation observed around the call returns without
// committing, so a resume finds the job by effect ID.
func (r Runner) Bind(ctx context.Context, snap Snapshot) (Snapshot, error) {
	state := snap.State
	if state.Intent() != core.IntentBind {
		return snap, fmt.Errorf("harness: phase %q does not bind", state.Phase)
	}
	if err := snap.Validate(); err != nil {
		return snap, err
	}
	if err := ctx.Err(); err != nil {
		return snap, err
	}
	job, err := r.executor.Bind(ctx, Effect{ID: state.EffectID, Command: state.Decision.Payload})
	if err == nil && job.ID == "" {
		err = fmt.Errorf("executor returned no job ID with status %q", job.Status)
	}
	if err != nil {
		return r.keepUnknown(ctx, snap, err)
	}
	return r.Commit(ctx, snap, core.JobBound{EffectID: state.EffectID, JobID: job.ID}, Facts{})
}

// Start issues the start of the bound job and commits that it was issued. An
// executor error or a job other than the bound one becomes OutcomeUnknown
// with the failure text, because the start may or may not have happened.
// Cancellation observed around the call returns without committing.
func (r Runner) Start(ctx context.Context, snap Snapshot) (Snapshot, error) {
	state := snap.State
	if state.Intent() != core.IntentStart {
		return snap, fmt.Errorf("harness: phase %q does not start", state.Phase)
	}
	if err := snap.Validate(); err != nil {
		return snap, err
	}
	if err := ctx.Err(); err != nil {
		return snap, err
	}
	job, err := r.executor.Start(ctx, state.EffectID)
	if err == nil && job.ID != state.JobID {
		err = fmt.Errorf("executor started job %q instead of bound job %q", job.ID, state.JobID)
	}
	if err != nil {
		return r.keepUnknown(ctx, snap, err)
	}
	return r.Commit(ctx, snap, core.JobStarted{EffectID: state.EffectID, JobID: state.JobID}, Facts{})
}

func (r Runner) keepUnknown(ctx context.Context, snap Snapshot, cause error) (Snapshot, error) {
	if ctx.Err() != nil {
		return snap, ctx.Err()
	}
	next, commitErr := r.Commit(ctx, snap, core.OutcomeUnknown{EffectID: snap.State.EffectID}, Facts{Failure: cause.Error()})
	if commitErr != nil {
		return snap, fmt.Errorf("%w; executor: %w", commitErr, cause)
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
