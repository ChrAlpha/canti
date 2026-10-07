package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ExecutionState is the process-level outcome of an effect. It says nothing
// about business success or the absence of side effects.
type ExecutionState string

const (
	ExecutionExited        ExecutionState = "exited"
	ExecutionTimedOut      ExecutionState = "timed_out"
	ExecutionCancelled     ExecutionState = "cancelled"
	ExecutionFailedToStart ExecutionState = "failed_to_start"
	ExecutionUnknown       ExecutionState = "unknown"
)

// StopReason records why a run stopped without a return decision. The first
// four are reportable through StopRequested; the rest only the core's own
// rules may set.
type StopReason string

const (
	StopCancelled         StopReason = "cancelled"
	StopTimedOut          StopReason = "timed_out"
	StopBudgetExhausted   StopReason = "budget_exhausted"
	StopUnrecoverable     StopReason = "unrecoverable"
	StopStepsExhausted    StopReason = "steps_exhausted"
	StopAttemptsExhausted StopReason = "attempts_exhausted"
	StopProtocolFailure   StopReason = "protocol_failure"
	StopUnresolved        StopReason = "unresolved"
)

// Event is one fact the harness reports after acting on an Intent. Only the
// value types in this package are accepted. Kind is the stable protocol name
// for persisted records; Go type names are not part of the protocol.
type Event interface {
	event()
	Kind() string
}

// RequestFrozen reports that the request bytes and provenance are recorded
// under the next request revision of the current step.
type RequestFrozen struct{}

// AttemptStarted reports that the named attempt of the named step is about to
// be sent. In request_frozen it names the current attempt; in request_sent it
// names the next attempt and reports that the previous send was lost.
type AttemptStarted struct {
	Step    int
	Attempt int
}

// ResponseSaved reports that a complete raw response to the named attempt of
// the named step is recorded. A response from an earlier position is invalid,
// not late.
type ResponseSaved struct {
	Step    int
	Attempt int
}

// ResponseIncomplete reports a failed or truncated send of the named attempt.
type ResponseIncomplete struct {
	Step    int
	Attempt int
}

// ResponseLoaded carries the saved response body of the named attempt for
// decoding.
type ResponseLoaded struct {
	Step    int
	Attempt int
	Body    []byte
}

// JobBound reports the backend job that owns the named effect. Binding
// records identity only; it does not report or authorize execution.
type JobBound struct {
	EffectID string
	JobID    string
}

// JobStarted reports that a start was issued for the bound job of the named
// effect. From here on, side effects may exist.
type JobStarted struct {
	EffectID string
	JobID    string
}

// Observed reports a known execution outcome of the named effect. Unknown
// outcomes must use OutcomeUnknown so they cannot be mistaken for a completed
// step.
type Observed struct {
	EffectID  string
	Execution ExecutionState
}

// OutcomeUnknown reports that the named effect's fate could not be determined.
type OutcomeUnknown struct{ EffectID string }

// StopRequested asks the run to stop for a reason the harness observed.
type StopRequested struct{ Reason StopReason }

// Resolved records an explicit human resolution of the named effect's unknown
// outcome. Resolving to ExecutionUnknown abandons the run and preserves the
// uncertainty instead of fabricating an outcome.
type Resolved struct {
	EffectID  string
	Execution ExecutionState
}

func (RequestFrozen) event()      {}
func (AttemptStarted) event()     {}
func (ResponseSaved) event()      {}
func (ResponseIncomplete) event() {}
func (ResponseLoaded) event()     {}
func (JobBound) event()           {}
func (JobStarted) event()         {}
func (Observed) event()           {}
func (OutcomeUnknown) event()     {}
func (StopRequested) event()      {}
func (Resolved) event()           {}

func (RequestFrozen) Kind() string      { return "request_frozen" }
func (AttemptStarted) Kind() string     { return "attempt_started" }
func (ResponseSaved) Kind() string      { return "response_saved" }
func (ResponseIncomplete) Kind() string { return "response_incomplete" }
func (ResponseLoaded) Kind() string     { return "response_loaded" }
func (JobBound) Kind() string           { return "job_bound" }
func (JobStarted) Kind() string         { return "job_started" }
func (Observed) Kind() string           { return "observed" }
func (OutcomeUnknown) Kind() string     { return "outcome_unknown" }
func (StopRequested) Kind() string      { return "stop_requested" }
func (Resolved) Kind() string           { return "resolved" }

// Events returns a zero value of every event kind, in protocol order.
func Events() []Event {
	return []Event{
		RequestFrozen{}, AttemptStarted{}, ResponseSaved{}, ResponseIncomplete{},
		ResponseLoaded{}, JobBound{}, JobStarted{}, Observed{},
		OutcomeUnknown{}, StopRequested{}, Resolved{},
	}
}

// EventID names the event committed as sequence seq of run runID. It is
// part of the persisted protocol: changing it changes the semantics version.
func EventID(runID string, seq int64, event Event) (string, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("core: encode %s event: %w", event.Kind(), err)
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s:%d:%s:%s", len(runID), runID, seq, event.Kind(), payload))
	return "event_" + hex.EncodeToString(sum[:]), nil
}
