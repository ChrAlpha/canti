package harness

import "context"

// Executor owns external effects, keyed by the stable effect ID. StartOrResume
// must return the job that already owns an effect instead of starting another,
// must honor cancellation before creating a job, and must reject a changed
// command under an existing identity. Inspect and Cancel must never create a
// job; Cancel must be idempotent. An unknown outcome must never trigger a
// retry without inspection; an effect ID does not make commands idempotent.
type Executor interface {
	StartOrResume(ctx context.Context, effect Effect) (Job, error)
	Inspect(ctx context.Context, effectID string) (job Job, found bool, err error)
	Cancel(ctx context.Context, effectID string) error
}

// Effect is the durable intent to perform one external action.
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
