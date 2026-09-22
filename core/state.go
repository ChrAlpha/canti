package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// Phase is the last durable boundary a run has reached.
type Phase string

const (
	PhaseIdle                Phase = "idle"
	PhaseRequestFrozen       Phase = "request_frozen"
	PhaseRequestSent         Phase = "request_sent"
	PhaseResponseSaved       Phase = "response_saved"
	PhaseEffectSaved         Phase = "effect_saved"
	PhaseAwaitingObservation Phase = "awaiting_observation"
	PhaseCancelling          Phase = "cancelling"
	PhaseReturned            Phase = "returned"
	PhaseNeedsResolution     Phase = "needs_resolution"
	PhaseStopped             Phase = "stopped"
)

// Intent is the single action the harness may take from a committed State.
type Intent string

const (
	IntentPrepareRequest Intent = "prepare_request"
	IntentSendRequest    Intent = "send_request"
	IntentDecode         Intent = "decode"
	IntentDispatch       Intent = "dispatch"
	IntentInspect        Intent = "inspect"
	IntentCancel         Intent = "cancel"
	IntentHalt           Intent = "halt"
)

// Limits are the finite budgets the core can count by itself.
type Limits struct {
	MaxSteps    int
	MaxAttempts int
}

// State is the complete projection needed to continue a run. It holds no
// history, bodies, or artifacts; those are facts the harness stores.
type State struct {
	RunID    string
	Step     int
	Attempt  int
	Phase    Phase
	Limits   Limits
	Decision Decision
	EffectID string
	JobID    string
	Reason   StopReason
}

var (
	// ErrIllegalTransition reports a valid event that the current phase does
	// not accept.
	ErrIllegalTransition = errors.New("core: illegal transition")
	// ErrInvalidEvent reports an event whose content is unusable in any phase.
	ErrInvalidEvent = errors.New("core: invalid event")
)

// NewState starts a run at its first step.
func NewState(runID string, limits Limits) (State, error) {
	if runID == "" {
		return State{}, errors.New("core: run ID is required")
	}
	if limits.MaxSteps < 1 || limits.MaxAttempts < 1 {
		return State{}, errors.New("core: limits must be positive")
	}
	return State{RunID: runID, Step: 1, Attempt: 1, Phase: PhaseIdle, Limits: limits}, nil
}

// Intent reports what the harness should do next. Unknown and closed phases
// halt so that no run continues automatically without a known rule.
func (s State) Intent() Intent {
	switch s.Phase {
	case PhaseIdle:
		return IntentPrepareRequest
	case PhaseRequestFrozen, PhaseRequestSent:
		return IntentSendRequest
	case PhaseResponseSaved:
		return IntentDecode
	case PhaseEffectSaved:
		return IntentDispatch
	case PhaseAwaitingObservation:
		return IntentInspect
	case PhaseCancelling:
		return IntentCancel
	default:
		return IntentHalt
	}
}

// Next applies one event. It returns the unchanged state with an error for
// events that are invalid or not legal in the current phase.
func (s State) Next(event Event) (State, error) {
	if err := s.validate(event); err != nil {
		return s, err
	}
	switch e := event.(type) {
	case RequestFrozen:
		if s.Phase == PhaseIdle {
			s.Phase = PhaseRequestFrozen
			return s, nil
		}
	case AttemptStarted:
		switch s.Phase {
		case PhaseRequestFrozen:
			s.Phase = PhaseRequestSent
			return s, nil
		case PhaseRequestSent:
			return s.retry(StopAttemptsExhausted, PhaseRequestSent), nil
		}
	case ResponseSaved:
		if s.Phase == PhaseRequestSent {
			s.Phase = PhaseResponseSaved
			return s, nil
		}
	case ResponseIncomplete:
		if s.Phase == PhaseRequestSent {
			return s.retry(StopAttemptsExhausted, PhaseRequestFrozen), nil
		}
	case ResponseLoaded:
		if s.Phase == PhaseResponseSaved {
			return s.decide(e.Body), nil
		}
	case JobStarted:
		switch s.Phase {
		case PhaseEffectSaved:
			s.Phase = PhaseAwaitingObservation
			s.JobID = e.JobID
			return s, nil
		case PhaseCancelling:
			s.JobID = e.JobID
			return s, nil
		}
	case Observed:
		switch s.Phase {
		case PhaseEffectSaved, PhaseAwaitingObservation:
			return s.advance(), nil
		case PhaseCancelling:
			s.Phase = PhaseStopped
			return s, nil
		}
	case OutcomeUnknown:
		switch s.Phase {
		case PhaseEffectSaved, PhaseAwaitingObservation, PhaseCancelling:
			s.Phase = PhaseNeedsResolution
			return s, nil
		}
	case StopRequested:
		switch s.Phase {
		case PhaseIdle, PhaseRequestFrozen, PhaseRequestSent, PhaseResponseSaved:
			s.Phase = PhaseStopped
			s.Reason = e.Reason
			return s, nil
		case PhaseEffectSaved, PhaseAwaitingObservation:
			s.Phase = PhaseCancelling
			s.Reason = e.Reason
			return s, nil
		case PhaseNeedsResolution:
			if s.Reason == "" {
				s.Reason = e.Reason
			}
			return s, nil
		}
	case Resolved:
		if s.Phase == PhaseNeedsResolution {
			if s.Reason != "" {
				s.Phase = PhaseStopped
				return s, nil
			}
			return s.advance(), nil
		}
	}
	return s, fmt.Errorf("%w: %T in phase %q", ErrIllegalTransition, event, s.Phase)
}

func (s State) validate(event Event) error {
	switch e := event.(type) {
	case RequestFrozen, AttemptStarted, ResponseSaved, ResponseIncomplete, OutcomeUnknown:
		return nil
	case ResponseLoaded:
		if len(e.Body) == 0 {
			return fmt.Errorf("%w: response body is required", ErrInvalidEvent)
		}
	case JobStarted:
		if e.JobID == "" {
			return fmt.Errorf("%w: job ID is required", ErrInvalidEvent)
		}
		if s.JobID != "" && s.JobID != e.JobID {
			return fmt.Errorf("%w: job %q does not match known job %q", ErrInvalidEvent, e.JobID, s.JobID)
		}
	case Observed:
		if s.Phase == PhaseEffectSaved && e.Execution != ExecutionFailedToStart {
			return fmt.Errorf("%w: only failed_to_start can be observed without a job", ErrInvalidEvent)
		}
		return knownExecution(e.Execution)
	case Resolved:
		return knownExecution(e.Execution)
	case StopRequested:
		if e.Reason == "" {
			return fmt.Errorf("%w: stop reason is required", ErrInvalidEvent)
		}
	default:
		return fmt.Errorf("%w: unsupported event %T", ErrInvalidEvent, event)
	}
	return nil
}

func knownExecution(execution ExecutionState) error {
	switch execution {
	case ExecutionExited, ExecutionTimedOut, ExecutionCancelled, ExecutionFailedToStart:
		return nil
	}
	return fmt.Errorf("%w: execution state %q is not a known outcome", ErrInvalidEvent, execution)
}

func (s State) retry(exhausted StopReason, resume Phase) State {
	if s.Attempt < s.Limits.MaxAttempts {
		s.Attempt++
		s.Phase = resume
		return s
	}
	s.Phase = PhaseStopped
	s.Reason = exhausted
	return s
}

func (s State) decide(body []byte) State {
	decision, err := Decode(body)
	if err != nil {
		return s.retry(StopProtocolFailure, PhaseRequestFrozen)
	}
	s.Decision = decision
	if decision.Kind == Return {
		s.Phase = PhaseReturned
		return s
	}
	s.Phase = PhaseEffectSaved
	s.EffectID = effectID(s.RunID, s.Step)
	return s
}

func (s State) advance() State {
	if s.Step >= s.Limits.MaxSteps {
		s.Phase = PhaseStopped
		s.Reason = StopStepsExhausted
		return s
	}
	s.Step++
	s.Attempt = 1
	s.Phase = PhaseIdle
	s.Decision = Decision{}
	s.EffectID = ""
	s.JobID = ""
	return s
}

func effectID(runID string, step int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%d", len(runID), runID, step)))
	return "effect_" + hex.EncodeToString(sum[:])
}
