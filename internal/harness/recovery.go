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

// Resumable reports whether the state has an unambiguous automatic path.
func (s RunState) Resumable() bool {
	return s.Phase != PhaseNeedsResolution && !s.NeedsInput
}
