package harness

// ExecutionState describes process-level execution, not business success or
// the absence of side effects.
type ExecutionState string

const (
	ExecutionExited        ExecutionState = "exited"
	ExecutionTimedOut      ExecutionState = "timed_out"
	ExecutionCancelled     ExecutionState = "cancelled"
	ExecutionFailedToStart ExecutionState = "failed_to_start"
	ExecutionUnknown       ExecutionState = "unknown"
)

// ArtifactRef points to output stored outside the bounded observation preview.
type ArtifactRef struct {
	Path      string
	Bytes     int64
	Complete  bool
	Available bool
}

// Observation is the durable result submitted after an effect is inspected.
type Observation struct {
	Execution ExecutionState
	ExitCode  *int
	Preview   string
	Output    ArtifactRef
}
