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
	if err := ValidateTransition(transition); err != nil {
		return snap, err
	}
	if err := r.store.Commit(ctx, transition); err != nil {
		return snap, fmt.Errorf("harness: commit %T: %w", event, err)
	}
	return Snapshot{Seq: transition.Seq, State: next}, nil
}

// check reports whether f holds exactly the records event permits, which
// leads to next.
func (f Facts) check(event core.Event, next core.State) error {
	if err := f.checkModelFacts(); err != nil {
		return err
	}
	allowed, err := f.allowed(event, next)
	if err != nil {
		return err
	}
	if f != allowed {
		return fmt.Errorf("%w: records do not belong to %T", ErrFacts, event)
	}
	return nil
}

// allowed returns the facts event permits.
func (f Facts) allowed(event core.Event, next core.State) (Facts, error) {
	switch e := event.(type) {
	case core.RequestFrozen:
		return f.requestFrozen()
	case core.ResponseSaved:
		return f.responseSaved()
	case core.ResponseIncomplete:
		return f.responseIncomplete()
	case core.RequestRejected:
		return f.requestRejected()
	case core.ResponseLoaded:
		return f.responseLoaded(next)
	case core.Performed:
		return f.performed()
	case core.Observed:
		return f.observed(e.Execution)
	case core.Resolved:
		return f.resolved(e.Execution)
	case core.EffectRecovered:
		return f.recovered()
	case core.OutcomeUnknown:
		return Facts{Failure: f.Failure, Receipt: f.Receipt}, nil
	case core.StopRequested:
		return Facts{Failure: f.Failure, Response: f.Response, Rejection: f.Rejection}, nil
	}
	return Facts{}, nil
}

func (f Facts) requestFrozen() (Facts, error) {
	if f.Request == nil || len(f.Request.Body) == 0 || f.Request.ContextState == nil {
		return Facts{}, fmt.Errorf("%w: frozen request bytes and context state are required", ErrFacts)
	}
	return Facts{Request: f.Request, Delivered: f.Delivered}, nil
}

func (f Facts) responseSaved() (Facts, error) {
	if f.Response == nil || !f.Response.Complete || len(f.Response.Body) == 0 {
		return Facts{}, fmt.Errorf("%w: complete response with a body is required", ErrFacts)
	}
	return Facts{Response: f.Response}, nil
}

func (f Facts) responseIncomplete() (Facts, error) {
	if f.Response != nil && f.Response.Complete {
		return Facts{}, fmt.Errorf("%w: incomplete response cannot be marked complete", ErrFacts)
	}
	return Facts{Response: f.Response, Failure: f.Failure}, nil
}

func (f Facts) requestRejected() (Facts, error) {
	if f.Response == nil || f.Failure == "" || f.Rejection == nil || !f.Rejection.Signal.Confirmed() {
		return Facts{}, fmt.Errorf("%w: rejected response and reason required", ErrFacts)
	}
	return Facts{Response: f.Response, Failure: f.Failure, ContextState: f.ContextState, Rejection: f.Rejection}, nil
}

func (f Facts) responseLoaded(next core.State) (Facts, error) {
	switch {
	case f.Supersede != nil:
		if next.Phase == core.PhaseEffectSaved || next.Phase == core.PhaseReturned {
			return Facts{}, fmt.Errorf("%w: a superseded decision cannot be accepted or returned", ErrFacts)
		}
		return Facts{Supersede: f.Supersede}, nil
	case next.Phase == core.PhaseEffectSaved:
		if _, err := Acceptance(core.SemanticsVersion, f.Action, f.Execution); err != nil {
			return Facts{}, fmt.Errorf("%w: %v", ErrFacts, err)
		}
		return Facts{Action: f.Action, Execution: f.Execution}, nil
	}
	return Facts{}, nil
}

func (f Facts) performed() (Facts, error) {
	if err := f.checkPerformed(); err != nil {
		return Facts{}, err
	}
	return Facts{Outcome: f.Outcome, ContextState: f.ContextState}, nil
}

func (f Facts) observed(execution core.ExecutionState) (Facts, error) {
	if err := f.checkObservation(execution); err != nil {
		return Facts{}, err
	}
	return Facts{Observation: f.Observation, Receipt: f.Receipt}, nil
}

func (f Facts) resolved(execution core.ExecutionState) (Facts, error) {
	if err := f.checkObservation(execution); err != nil {
		return Facts{}, err
	}
	if f.Resolution == nil {
		return Facts{}, fmt.Errorf("%w: resolution provenance is required", ErrFacts)
	}
	return Facts{Observation: f.Observation, Receipt: f.Receipt, Resolution: f.Resolution}, nil
}

func (f Facts) recovered() (Facts, error) {
	if f.Receipt == nil {
		return Facts{}, fmt.Errorf("%w: recovery receipt is required", ErrFacts)
	}
	return Facts{Receipt: f.Receipt}, nil
}

func (f Facts) checkModelFacts() error {
	if f.Response != nil {
		if err := f.Response.Usage.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrFacts, err)
		}
	}
	if f.Rejection != nil && !f.Rejection.Signal.Confirmed() {
		return fmt.Errorf("%w: confirmed rejection signal required", ErrFacts)
	}
	return nil
}

func (f Facts) checkPerformed() error {
	if f.Outcome == nil {
		return fmt.Errorf("%w: a performed effect requires its outcome", ErrFacts)
	}
	if err := f.Outcome.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrFacts, err)
	}
	if (f.ContextState != nil) != (f.Outcome.Handover != nil) {
		return fmt.Errorf("%w: a handover outcome and its context commit together", ErrFacts)
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
	if f.Observation.Retrieval != nil {
		return fmt.Errorf("%w: an observation holds no retrieval or handover; a performed outcome does", ErrFacts)
	}
	return nil
}

// ValidateTransition checks the same transition and fact contract at a concrete
// store boundary, including callers that did not construct it through Runner.
func ValidateTransition(t Transition) error {
	next, err := t.From.Next(t.Event)
	if err != nil {
		return err
	}
	if next != t.To {
		return fmt.Errorf("%w: next state differs from core transition", ErrFacts)
	}
	if err := t.Facts.check(t.Event, t.To); err != nil {
		return err
	}

	if r := t.Facts.Request; r != nil {
		if r.ContextState.AnchorSeq >= t.Seq {
			return fmt.Errorf("%w: frozen anchor must precede its request", ErrFacts)
		}
		if err := r.ContextState.Validate(t.To.Step, t.Seq); err != nil {
			return fmt.Errorf("%w: %v", ErrFacts, err)
		}
	}

	if s := t.Facts.Supersede; s != nil {
		if _, ok := t.Event.(core.ResponseLoaded); !ok || t.From.Inputs <= t.From.Seen || s.Instruction != t.From.Seen+1 || s.Kind != core.Exec && s.Kind != core.Return {
			return fmt.Errorf("%w: superseded decision of instruction %d in phase %q", ErrFacts, s.Instruction, t.From.Phase)
		}
	}
	if _, stop := t.Event.(core.StopRequested); stop && (t.Facts.Response != nil || t.Facts.Rejection != nil) && t.From.Phase != core.PhaseRequestSent {
		return fmt.Errorf("%w: a stopped model result must settle its sent attempt", ErrFacts)
	}
	return nil
}
