package harness

import "context"

// Executor owns external effects. StartOrResume must use Effect.ID to find an
// existing job after a harness restart instead of blindly starting a duplicate.
// It must honor cancellation before creating a job and reject changed commands
// under an existing identity. An unknown outcome must never trigger a retry
// without inspection; an effect ID does not make arbitrary commands idempotent.
type Executor interface {
	StartOrResume(context.Context, Effect) (Job, error)
	Inspect(context.Context, string) (Job, error)
	Cancel(context.Context, string) error
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

// Job identifies a backend task whose result can be queried after restart.
type Job struct {
	ID       string
	Status   JobStatus
	ExitCode *int
	Output   ArtifactRef
}
