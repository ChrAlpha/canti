package harness

import "context"

// Executor owns external effects, keyed by the stable effect ID. Bind must
// create or return the job that owns an effect without executing anything;
// only Start may cause execution, and it must be idempotent for an
// already-started job and start nothing but the bound job. Bind must honor
// cancellation before creating a job and reject a changed command under an
// existing identity. Inspect and Cancel must never create or start a job.
// Inspect may report found == false only when the backend positively
// determined that no job owns the effect; an indeterminate lookup must return
// an error instead. Cancel must be idempotent and must prevent any later
// start of an unstarted bind, including one that lands after Cancel. An
// unknown outcome must never trigger a retry without inspection; an effect ID
// does not make commands idempotent.
type Executor interface {
	Bind(ctx context.Context, effect Effect) (Job, error)
	Start(ctx context.Context, effectID string) (Job, error)
	Inspect(ctx context.Context, effectID string) (job Job, found bool, err error)
	Cancel(ctx context.Context, effectID string) error
}

// Effect is the durable intent to perform one external action. A concrete
// executor must bind the effect ID to the full execution specification it
// runs under, not only the command text.
type Effect struct {
	ID      string
	Command string
}

// JobStatus describes the execution backend's observation of a job.
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
	JobUnknown   JobStatus = "unknown"
)

// Job is the backend's handle for an effect. Its ID is recorded as a fact;
// its status is a snapshot that Inspect must confirm before an observation.
type Job struct {
	ID       string
	Status   JobStatus
	ExitCode *int
	Output   ArtifactRef
}
