package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
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

// Limits are the budgets the core can count by itself. MaxSteps zero is unlimited.
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
	Started    bool
	Reason     StopReason
}

// SemanticsVersion identifies the transition rules of this package. It must
// be bumped whenever a legal transition, an intent, or the meaning of an
// event changes, so a stored run can refuse to continue under semantics it
// was not committed with.
const SemanticsVersion = 3

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

// KnownSemantics reports whether this package reads records of semantics
// version: versions 1 to SemanticsVersion.
func KnownSemantics(version int) error {
	if version < 1 || version > SemanticsVersion {
		return fmt.Errorf("core: semantics version %d, want 1 to %d", version, SemanticsVersion)
	}
	return nil
}

// Compatible reports whether s, committed under semantics version, keeps
// its meaning under this package: version 1 had no unlimited steps and
// versions before 3 no decision purpose.
func Compatible(version int, s State) error {
	if err := KnownSemantics(version); err != nil {
		return err
	}
	switch {
	case version == 1 && s.Limits.MaxSteps <= 0:
		return errors.New("core: semantics version 1 requires positive max steps")
	case version < 3 && s.Decision.Purpose != "":
		return fmt.Errorf("core: semantics version %d has no decision purpose", version)
	}
	return nil
}

// NewState starts a run at its first step.
func NewState(runID string, limits Limits) (State, error) {
	if runID == "" {
		return State{}, errors.New("core: run ID is required")
	}
	if limits.MaxSteps < 0 || limits.MaxAttempts < 1 {
		return State{}, errors.New("core: max steps must be nonnegative and max attempts must be positive")
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
	if s.Limits.MaxSteps < 0 || s.Limits.MaxAttempts < 1 {
		return fail("max steps must be nonnegative and max attempts must be positive")
	}
	if s.Step < 1 || s.Limits.MaxSteps > 0 && s.Step > s.Limits.MaxSteps {
		return fail("step %d is outside the budget", s.Step)
	}
	if s.Attempt < 1 || s.Attempt > s.Limits.MaxAttempts {
		return fail("attempt %d is outside the budget", s.Attempt)
	}
	if s.RequestRev < 0 {
		return fail("request revision %d is negative", s.RequestRev)
	}
	switch s.Reason {
	case "", StopCancelled, StopTimedOut, StopBudgetExhausted, StopUnrecoverable, StopStepsExhausted, StopAttemptsExhausted, StopProtocolFailure, StopUnresolved:
	default:
		return fail("unknown stop reason %q", s.Reason)
	}
	switch s.Decision.Kind {
	case "", Exec, Return:
	default:
		return fail("unknown decision kind %q", s.Decision.Kind)
	}
	hasDecision := s.Decision != Decision{}
	switch s.Phase {
	case PhaseIdle, PhaseRequestFrozen, PhaseRequestSent, PhaseResponseSaved:
		if s.Phase != PhaseIdle && s.RequestRev < 1 {
			return fail("a frozen request is required in phase %q", s.Phase)
		}
		if hasDecision || s.EffectID != "" || s.JobID != "" || s.Started || s.Reason != "" {
			return fail("no decision, effect, job, start, or reason is allowed in phase %q", s.Phase)
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
			if s.Started {
				return fail("no start is allowed before binding")
			}
			if s.Reason != "" {
				return fail("no stop reason is allowed in phase %q", s.Phase)
			}
		case PhaseJobBound, PhaseAwaitingObservation:
			if s.JobID == "" {
				return fail("a bound job is required in phase %q", s.Phase)
			}
			if s.Started != (s.Phase == PhaseAwaitingObservation) {
				return fail("phase %q contradicts the committed start", s.Phase)
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
		if s.EffectID != "" || s.JobID != "" || s.Started || s.Reason != "" {
			return fail("no effect, job, start, or reason is allowed in phase %q", s.Phase)
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

// Next applies one event. It returns the unchanged state with
// ErrInvalidEvent for malformed content, ErrIllegalTransition for a pair the
// current phase does not accept whatever its identity claims, and
// ErrInvalidEvent for a claim that contradicts the current position of a
// legal pair. A legal event may leave the state equal to its input; the
// harness still records it.
func (s State) Next(event Event) (State, error) {
	if err := validateEvent(event); err != nil {
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
			if err := s.claimStep(e.Step); err != nil {
				return s, err
			}
			if e.Attempt != s.Attempt {
				return s, fmt.Errorf("%w: attempt %d does not match current attempt %d", ErrInvalidEvent, e.Attempt, s.Attempt)
			}
			s.Phase = PhaseRequestSent
			return s, nil
		case PhaseRequestSent:
			if err := s.claimStep(e.Step); err != nil {
				return s, err
			}
			if e.Attempt != s.Attempt+1 {
				return s, fmt.Errorf("%w: attempt %d does not follow lost attempt %d", ErrInvalidEvent, e.Attempt, s.Attempt)
			}
			return s.retry(StopAttemptsExhausted, PhaseRequestSent), nil
		}
	case ResponseSaved:
		if s.Phase == PhaseRequestSent {
			if err := s.claimAttempt(e.Step, e.Attempt); err != nil {
				return s, err
			}
			s.Phase = PhaseResponseSaved
			return s, nil
		}
	case ResponseIncomplete:
		if s.Phase == PhaseRequestSent {
			if err := s.claimAttempt(e.Step, e.Attempt); err != nil {
				return s, err
			}
			return s.retry(StopAttemptsExhausted, PhaseRequestFrozen), nil
		}
	case ResponseLoaded:
		if s.Phase == PhaseResponseSaved {
			if err := s.claimAttempt(e.Step, e.Attempt); err != nil {
				return s, err
			}
			return s.decide(e.Body), nil
		}
	case JobBound:
		switch s.Phase {
		case PhaseEffectSaved, PhaseCancelling, PhaseNeedsResolution:
			if err := s.claimJob(e.EffectID, e.JobID); err != nil {
				return s, err
			}
			s.JobID = e.JobID
			switch s.Phase {
			case PhaseEffectSaved:
				s.Phase = PhaseJobBound
			case PhaseNeedsResolution:
				switch {
				case s.Reason != "":
					s.Phase = PhaseCancelling
				case s.Started:
					s.Phase = PhaseAwaitingObservation
				default:
					s.Phase = PhaseJobBound
				}
			}
			return s, nil
		}
	case JobStarted:
		if s.Phase == PhaseJobBound {
			if err := s.claimEffect(e.EffectID); err != nil {
				return s, err
			}
			if e.JobID != s.JobID {
				return s, fmt.Errorf("%w: job %q is not the bound job %q", ErrInvalidEvent, e.JobID, s.JobID)
			}
			s.Phase = PhaseAwaitingObservation
			s.Started = true
			return s, nil
		}
	case Observed:
		switch s.Phase {
		case PhaseEffectSaved, PhaseJobBound, PhaseAwaitingObservation, PhaseCancelling:
			if err := s.claimObservation(e.EffectID, e.Execution); err != nil {
				return s, err
			}
			if s.Phase == PhaseCancelling {
				s.Phase = PhaseStopped
				return s, nil
			}
			return s.advance(), nil
		}
	case OutcomeUnknown:
		switch s.Phase {
		case PhaseEffectSaved, PhaseJobBound, PhaseAwaitingObservation, PhaseCancelling:
			if err := s.claimEffect(e.EffectID); err != nil {
				return s, err
			}
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
			if err := s.claimEffect(e.EffectID); err != nil {
				return s, err
			}
			if e.Execution == ExecutionUnknown {
				s.Phase = PhaseStopped
				if s.Reason == "" {
					s.Reason = StopUnresolved
				}
				return s, nil
			}
			if s.JobID == "" && (e.Execution == ExecutionExited || e.Execution == ExecutionTimedOut) {
				return s, fmt.Errorf("%w: %s cannot be resolved without a bound job", ErrInvalidEvent, e.Execution)
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

func validateEvent(event Event) error {
	switch e := event.(type) {
	case RequestFrozen:
	case AttemptStarted:
		if e.Attempt < 1 {
			return fmt.Errorf("%w: attempt number is required", ErrInvalidEvent)
		}
	case ResponseSaved:
	case ResponseIncomplete:
	case ResponseLoaded:
		if len(e.Body) == 0 {
			return fmt.Errorf("%w: response body is required", ErrInvalidEvent)
		}
	case JobBound:
		return requireJobIdentity(e.EffectID, e.JobID)
	case JobStarted:
		return requireJobIdentity(e.EffectID, e.JobID)
	case Observed:
		if e.EffectID == "" {
			return fmt.Errorf("%w: effect ID is required", ErrInvalidEvent)
		}
		return knownExecution(e.Execution)
	case OutcomeUnknown:
		if e.EffectID == "" {
			return fmt.Errorf("%w: effect ID is required", ErrInvalidEvent)
		}
	case Resolved:
		if e.EffectID == "" {
			return fmt.Errorf("%w: effect ID is required", ErrInvalidEvent)
		}
		if e.Execution == ExecutionUnknown {
			return nil
		}
		return knownExecution(e.Execution)
	case StopRequested:
		if e.Reason == "" {
			return fmt.Errorf("%w: stop reason is required", ErrInvalidEvent)
		}
		switch e.Reason {
		case StopCancelled, StopTimedOut, StopBudgetExhausted, StopUnrecoverable:
		default:
			return fmt.Errorf("%w: %q is not a reportable stop reason", ErrInvalidEvent, e.Reason)
		}
	default:
		return fmt.Errorf("%w: unsupported event %T", ErrInvalidEvent, event)
	}
	return nil
}

func requireJobIdentity(effectID, jobID string) error {
	if effectID == "" {
		return fmt.Errorf("%w: effect ID is required", ErrInvalidEvent)
	}
	if jobID == "" {
		return fmt.Errorf("%w: job ID is required", ErrInvalidEvent)
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
	if s.EffectID != "" && s.EffectID != effectID {
		return fmt.Errorf("%w: effect %q does not match current effect %q", ErrInvalidEvent, effectID, s.EffectID)
	}
	return nil
}

func (s State) claimJob(effectID, jobID string) error {
	if err := s.claimEffect(effectID); err != nil {
		return err
	}
	if s.JobID != "" && s.JobID != jobID {
		return fmt.Errorf("%w: job %q does not match known job %q", ErrInvalidEvent, jobID, s.JobID)
	}
	return nil
}

func (s State) claimObservation(effectID string, execution ExecutionState) error {
	if err := s.claimEffect(effectID); err != nil {
		return err
	}
	if s.JobID == "" {
		if want, ok := joblessObservation[s.Phase]; ok && execution != want {
			return fmt.Errorf("%w: only %s can be observed without a job in phase %q", ErrInvalidEvent, want, s.Phase)
		}
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
	if s.Limits.MaxSteps > 0 && s.Step >= s.Limits.MaxSteps {
		s.Phase = PhaseStopped
		s.Reason = StopStepsExhausted
		return s
	}
	if s.Step == math.MaxInt {
		s.Phase = PhaseStopped
		s.Reason = StopUnrecoverable
		return s
	}
	s.Step++
	s.Attempt = 1
	s.RequestRev = 0
	s.Phase = PhaseIdle
	s.Decision = Decision{}
	s.EffectID = ""
	s.JobID = ""
	s.Started = false
	return s
}

func effectID(runID string, step int) string {
	var input [40]byte
	buf := strconv.AppendInt(input[:0], int64(len(runID)), 10)
	buf = append(buf, ':')
	buf = append(buf, runID...)
	buf = strconv.AppendInt(buf, int64(step), 10)
	sum := sha256.Sum256(buf)
	var id [71]byte
	copy(id[:], "effect_")
	hex.Encode(id[7:], sum[:])
	return string(id[:])
}
