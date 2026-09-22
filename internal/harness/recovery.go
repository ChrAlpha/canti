package harness

// Phase is the last durable boundary reached by a run.
type Phase string

const (
	PhaseIdle                Phase = "idle"
	PhaseRequestFrozen       Phase = "request_frozen"
	PhaseResponseSaved       Phase = "response_saved"
	PhaseDecisionSaved       Phase = "decision_saved"
	PhaseEffectSaved         Phase = "effect_saved"
	PhaseDispatching         Phase = "dispatching"
	PhaseAwaitingObservation Phase = "awaiting_observation"
	PhaseObservationSaved    Phase = "observation_saved"
	PhaseReturned            Phase = "returned"
	PhaseNeedsResolution     Phase = "needs_resolution"
)

// RunState is the small recovery projection needed to resume a run.
type RunState struct {
	RunID      string
	StepID     string
	Phase      Phase
	RequestID  string
	EffectID   string
	JobID      string
	NeedsInput bool
}

// Resumable reports whether a known, active phase permits recovery. It does
// not validate persisted facts or authorize dispatch without job inspection.
func (s RunState) Resumable() bool {
	if s.NeedsInput {
		return false
	}
	switch s.Phase {
	case PhaseIdle, PhaseRequestFrozen, PhaseResponseSaved, PhaseDecisionSaved,
		PhaseEffectSaved, PhaseDispatching, PhaseAwaitingObservation, PhaseObservationSaved:
		return true
	default:
		return false
	}
}
