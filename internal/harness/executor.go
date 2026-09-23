package harness

import "context"

// Executor owns external effects, keyed by the stable effect ID. Each effect
// has one durable start latch, set at most once and never reset: by Start,
// which runs the bound job, or by Cancel, after which no job of the effect
// can start. A job executes at most once, and only through Start.
//
// Bind must create or return the one job that owns an effect without
// executing anything, never replace it, create nothing once its context is
// done, and reject a changed command under an existing identity. A job
// reports JobPending exactly until its latch is set; a latched job whose
// launch is uncertain reports JobUnknown. A job reported succeeded, failed,
// or cancelled keeps that status and never begins running afterwards.
//
// Start must fail for an unbound effect and start nothing but the bound job;
// once the latch is set it is a no-op reporting the job as-is, even for a
// finished job — a naive backend restart, such as docker start on an exited
// container, does not satisfy this, so the executor must gate on its own
// record. Cancel is idempotent in every job state and safe to retry after an
// error. It stops a running job, replaces JobUnknown only with a result it
// established, and from its return an unstarted job it fences, including one
// bound after it, reports JobCancelled. Inspect and Cancel must never create
// or start a job. Inspect reports found == false exactly when the backend
// positively determined that no job owns the effect; an indeterminate lookup
// returns an error.
//
// An unknown outcome must never trigger a retry without inspection; an effect
// ID does not make commands idempotent. executortest.Check exercises the
// latch.
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
	JobCancelled JobStatus = "cancelled"
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
