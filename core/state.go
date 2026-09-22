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
	PhaseJobBound            Phase = "job_bound"
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
	IntentBind           Intent = "bind"
	IntentStart          Intent = "start"
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
	RunID      string
	Step       int
	Attempt    int
	RequestRev int
	Phase      Phase
	Limits     Limits
	Decision   Decision
	EffectID   string
	JobID      string
	Reason     StopReason
}

// SemanticsVersion identifies the transition rules of this package. It must
// be bumped whenever a legal transition, an intent, or the meaning of an
// event changes, so a stored run can refuse to continue under semantics it
// was not committed with.
const SemanticsVersion = 1

var (
	// ErrIllegalTransition reports a valid event that the current phase does
	// not accept.
	ErrIllegalTransition = errors.New("core: illegal transition")
	// ErrInvalidEvent reports an event whose content is malformed or
	// contradicts the identities the state has recorded.
	ErrInvalidEvent = errors.New("core: invalid event")
	// ErrInvalidState reports a state projection that no legal run reaches.
	// Hosts must validate at load boundaries and refuse I/O on failure.
	ErrInvalidState = errors.New("core: invalid state")
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
		return IntentBind
	case PhaseJobBound:
		return IntentStart
	case PhaseAwaitingObservation:
		return IntentInspect
	case PhaseCancelling:
		return IntentCancel
	default:
		return IntentHalt
	}
}

// Validate checks the invariants every reachable state satisfies. States
// loaded from storage or built by another host must pass before any intent
// is acted on; Next itself stays total over state values.
func (s State) Validate() error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalidState, fmt.Sprintf(format, args...))
	}
	if s.RunID == "" {
		return fail("run ID is required")
	}
	if s.Limits.MaxSteps < 1 || s.Limits.MaxAttempts < 1 {
		return fail("limits must be positive")
	}
	if s.Step < 1 || s.Step > s.Limits.MaxSteps {
		return fail("step %d is outside the budget", s.Step)
	}
	if s.Attempt < 1 || s.Attempt > s.Limits.MaxAttempts {
		return fail("attempt %d is outside the budget", s.Attempt)
	}
	if s.RequestRev < 0 {
		return fail("request revision %d is negative", s.RequestRev)
	}
	hasDecision := s.Decision != Decision{}
	switch s.Phase {
	case PhaseIdle, PhaseRequestFrozen, PhaseRequestSent, PhaseResponseSaved:
		if s.Phase != PhaseIdle && s.RequestRev < 1 {
			return fail("a frozen request is required in phase %q", s.Phase)
		}
		if hasDecision || s.EffectID != "" || s.JobID != "" || s.Reason != "" {
			return fail("no decision, effect, job, or reason is allowed in phase %q", s.Phase)
		}
	case PhaseEffectSaved, PhaseJobBound, PhaseAwaitingObservation, PhaseCancelling, PhaseNeedsResolution:
		if s.RequestRev < 1 {
			return fail("a frozen request is required in phase %q", s.Phase)
		}
		if s.Decision.Kind != Exec {
			return fail("an exec decision is required in phase %q", s.Phase)
		}
		if s.EffectID == "" {
			return fail("an effect ID is required in phase %q", s.Phase)
		}
		switch s.Phase {
		case PhaseEffectSaved:
			if s.JobID != "" {
				return fail("no job is allowed before binding")
			}
			if s.Reason != "" {
				return fail("no stop reason is allowed in phase %q", s.Phase)
			}
		case PhaseJobBound, PhaseAwaitingObservation:
			if s.JobID == "" {
				return fail("a bound job is required in phase %q", s.Phase)
			}
			if s.Reason != "" {
				return fail("no stop reason is allowed in phase %q", s.Phase)
			}
		case PhaseCancelling:
			if s.Reason == "" {
				return fail("a stop reason is required in phase %q", s.Phase)
			}
		}
	case PhaseReturned:
		if s.RequestRev < 1 {
			return fail("a frozen request is required in phase %q", s.Phase)
		}
		if s.Decision.Kind != Return {
			return fail("a return decision is required in phase %q", s.Phase)
		}
		if s.EffectID != "" || s.JobID != "" || s.Reason != "" {
			return fail("no effect, job, or reason is allowed in phase %q", s.Phase)
		}
	case PhaseStopped:
		if s.Reason == "" {
			return fail("a stop reason is required in phase %q", s.Phase)
		}
		if s.Decision.Kind == Return {
			return fail("a return decision contradicts a stop")
		}
	default:
		return fail("unknown phase %q", s.Phase)
	}
	return nil
}

// Next applies one event. It returns the unchanged state with an error for
// events that are invalid or not legal in the current phase. A legal event
// may leave the state equal to its input; the harness still records it.
func (s State) Next(event Event) (State, error) {
	if err := s.validate(event); err != nil {
		return s, err
	}
	switch e := event.(type) {
	case RequestFrozen:
		if s.Phase == PhaseIdle {
			s.Phase = PhaseRequestFrozen
			s.RequestRev++
			return s, nil
		}
	case AttemptStarted:
		switch s.Phase {
		case PhaseRequestFrozen:
			if e.Attempt != s.Attempt {
				return s, fmt.Errorf("%w: attempt %d does not match current attempt %d", ErrInvalidEvent, e.Attempt, s.Attempt)
			}
			s.Phase = PhaseRequestSent
			return s, nil
		case PhaseRequestSent:
			if e.Attempt != s.Attempt+1 {
				return s, fmt.Errorf("%w: attempt %d does not follow lost attempt %d", ErrInvalidEvent, e.Attempt, s.Attempt)
			}
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
	case JobBound:
		switch s.Phase {
		case PhaseEffectSaved:
			s.Phase = PhaseJobBound
			s.JobID = e.JobID
			return s, nil
		case PhaseCancelling:
			s.JobID = e.JobID
			return s, nil
		case PhaseNeedsResolution:
			s.JobID = e.JobID
			if s.Reason != "" {
				s.Phase = PhaseCancelling
				return s, nil
			}
			s.Phase = PhaseJobBound
			return s, nil
		}
	case JobStarted:
		if s.Phase == PhaseJobBound {
			s.Phase = PhaseAwaitingObservation
			return s, nil
		}
	case Observed:
		switch s.Phase {
		case PhaseEffectSaved, PhaseJobBound, PhaseAwaitingObservation:
			return s.advance(), nil
		case PhaseCancelling:
			s.Phase = PhaseStopped
			return s, nil
		}
	case OutcomeUnknown:
		switch s.Phase {
		case PhaseEffectSaved, PhaseJobBound, PhaseAwaitingObservation, PhaseCancelling:
			s.Phase = PhaseNeedsResolution
			return s, nil
		}
	case StopRequested:
		switch s.Phase {
		case PhaseIdle, PhaseRequestFrozen, PhaseRequestSent, PhaseResponseSaved:
			s.Phase = PhaseStopped
			s.Reason = e.Reason
			return s, nil
		case PhaseEffectSaved, PhaseJobBound, PhaseAwaitingObservation:
			s.Phase = PhaseCancelling
			s.Reason = e.Reason
			return s, nil
		case PhaseCancelling, PhaseNeedsResolution:
			if s.Reason == "" {
				s.Reason = e.Reason
			}
			return s, nil
		}
	case Resolved:
		if s.Phase == PhaseNeedsResolution {
			if e.Execution == ExecutionUnknown {
				s.Phase = PhaseStopped
				if s.Reason == "" {
					s.Reason = StopUnresolved
				}
				return s, nil
			}
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
	case RequestFrozen:
		return nil
	case AttemptStarted:
		if e.Attempt < 1 {
			return fmt.Errorf("%w: attempt number is required", ErrInvalidEvent)
		}
		return s.claimStep(e.Step)
	case ResponseSaved:
		return s.claimAttempt(e.Step, e.Attempt)
	case ResponseIncomplete:
		return s.claimAttempt(e.Step, e.Attempt)
	case ResponseLoaded:
		if err := s.claimAttempt(e.Step, e.Attempt); err != nil {
			return err
		}
		if len(e.Body) == 0 {
			return fmt.Errorf("%w: response body is required", ErrInvalidEvent)
		}
	case JobBound:
		if err := s.claimEffect(e.EffectID); err != nil {
			return err
		}
		if e.JobID == "" {
			return fmt.Errorf("%w: job ID is required", ErrInvalidEvent)
		}
		if s.JobID != "" && s.JobID != e.JobID {
			return fmt.Errorf("%w: job %q does not match known job %q", ErrInvalidEvent, e.JobID, s.JobID)
		}
	case JobStarted:
		if err := s.claimEffect(e.EffectID); err != nil {
			return err
		}
		if e.JobID == "" {
			return fmt.Errorf("%w: job ID is required", ErrInvalidEvent)
		}
		if s.JobID != e.JobID {
			return fmt.Errorf("%w: job %q is not the bound job %q", ErrInvalidEvent, e.JobID, s.JobID)
		}
	case Observed:
		if err := s.claimEffect(e.EffectID); err != nil {
			return err
		}
		if err := knownExecution(e.Execution); err != nil {
			return err
		}
		if s.JobID == "" {
			if want, ok := joblessObservation[s.Phase]; ok && e.Execution != want {
				return fmt.Errorf("%w: only %s can be observed without a job in phase %q", ErrInvalidEvent, want, s.Phase)
			}
		}
	case OutcomeUnknown:
		return s.claimEffect(e.EffectID)
	case Resolved:
		if err := s.claimEffect(e.EffectID); err != nil {
			return err
		}
		if e.Execution == ExecutionUnknown {
			return nil
		}
		if err := knownExecution(e.Execution); err != nil {
			return err
		}
		if s.JobID == "" && (e.Execution == ExecutionExited || e.Execution == ExecutionTimedOut) {
			return fmt.Errorf("%w: %s cannot be resolved without a started job", ErrInvalidEvent, e.Execution)
		}
	case StopRequested:
		if e.Reason == "" {
			return fmt.Errorf("%w: stop reason is required", ErrInvalidEvent)
		}
	default:
		return fmt.Errorf("%w: unsupported event %T", ErrInvalidEvent, event)
	}
	return nil
}

func (s State) claimStep(step int) error {
	if step != s.Step {
		return fmt.Errorf("%w: step %d does not match current step %d", ErrInvalidEvent, step, s.Step)
	}
	return nil
}

func (s State) claimAttempt(step, attempt int) error {
	if err := s.claimStep(step); err != nil {
		return err
	}
	if attempt != s.Attempt {
		return fmt.Errorf("%w: attempt %d does not match current attempt %d", ErrInvalidEvent, attempt, s.Attempt)
	}
	return nil
}

func (s State) claimEffect(effectID string) error {
	if effectID == "" {
		return fmt.Errorf("%w: effect ID is required", ErrInvalidEvent)
	}
	if s.EffectID != "" && s.EffectID != effectID {
		return fmt.Errorf("%w: effect %q does not match current effect %q", ErrInvalidEvent, effectID, s.EffectID)
	}
	return nil
}

var joblessObservation = map[Phase]ExecutionState{
	PhaseEffectSaved: ExecutionFailedToStart,
	PhaseCancelling:  ExecutionCancelled,
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
		return s.retry(StopProtocolFailure, PhaseIdle)
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
	s.RequestRev = 0
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
