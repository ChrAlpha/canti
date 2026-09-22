package core

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

// StopReason records why a run stopped without a return decision.
type StopReason string

const (
	StopCancelled         StopReason = "cancelled"
	StopTimedOut          StopReason = "timed_out"
	StopBudgetExhausted   StopReason = "budget_exhausted"
	StopStepsExhausted    StopReason = "steps_exhausted"
	StopAttemptsExhausted StopReason = "attempts_exhausted"
	StopProtocolFailure   StopReason = "protocol_failure"
)

// Event is one fact the harness reports after acting on an Intent. Only the
// value types in this package are accepted.
type Event interface{ event() }

// RequestFrozen reports that the request bytes and provenance are recorded.
type RequestFrozen struct{}

// AttemptStarted reports that one model attempt is about to be sent. In
// request_sent it also reports that the previous attempt was lost.
type AttemptStarted struct{}

// ResponseSaved reports that a complete raw response is recorded.
type ResponseSaved struct{}

// ResponseIncomplete reports a failed or truncated model attempt.
type ResponseIncomplete struct{}

// ResponseLoaded carries the saved response body for decoding.
type ResponseLoaded struct{ Body []byte }

// JobStarted reports the backend job that owns the committed effect.
type JobStarted struct{ JobID string }

// Observed reports a known execution outcome. Unknown outcomes must use
// OutcomeUnknown so they cannot be mistaken for a completed step.
type Observed struct{ Execution ExecutionState }

// OutcomeUnknown reports that the effect's fate could not be determined.
type OutcomeUnknown struct{}

// StopRequested asks the run to stop for a reason the harness observed.
type StopRequested struct{ Reason StopReason }

// Resolved records an explicit human resolution of an unknown outcome.
type Resolved struct{ Execution ExecutionState }

func (RequestFrozen) event()      {}
func (AttemptStarted) event()     {}
func (ResponseSaved) event()      {}
func (ResponseIncomplete) event() {}
func (ResponseLoaded) event()     {}
func (JobStarted) event()         {}
func (Observed) event()           {}
func (OutcomeUnknown) event()     {}
func (StopRequested) event()      {}
func (Resolved) event()           {}
