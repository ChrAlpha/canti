package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ErrJobLost means a durably bound physical owner is missing. Absence never
// authorizes replacement or another launch.
var ErrJobLost = errors.New("harness: the bound owner of an effect is lost")

// Executor owns bounded, resumable execution of accepted external effects.
// Specification is pure and covers the effective execution configuration.
// Drive may create and consume the one durable launch latch; Inspect and
// Cancel never create or start work. Every operation checks the accepted Spec
// and returns the same authoritative receipt representation. Known terminal
// receipts are retained before releasing their owner and never change on Cancel.
// Cancel durably fences even never-bound effects. An error is not an outcome.
// A pre-latch context cancellation forbids launch; post-latch completion is
// synchronous and bounded, so a Host cannot release ownership while it acts.
type Executor interface {
	Specification(command string) string
	Drive(context.Context, Effect) (Job, error)
	Inspect(context.Context, Effect) (Job, error)
	Cancel(context.Context, Effect) (Job, error)
}

// Waiter provides a bounded advisory wake-up without blocking cancellation.
// Callers sample authoritative evidence afterwards.
type Waiter interface {
	Wait(context.Context, Effect) error
}

// Effect is reconstructed only from a committed external acceptance.
type Effect struct{ ID, Command, Spec string }

// ExecutionIdentity is committed with acceptance before executor I/O. Spec
// comes from the trusted adapter's pure Specification; ConfigDigest binds it
// to the exact immutable Run.Config whose adapter produced it.
type ExecutionIdentity struct{ Spec, ConfigDigest string }

// ConfigDigest hashes exact configuration bytes without interpreting them.
func ConfigDigest(config []byte) string {
	sum := sha256.Sum256(config)
	return hex.EncodeToString(sum[:])
}

func digest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

// Validate checks the current identity encoding.
func (e ExecutionIdentity) Validate() error {
	if !digest(e.Spec) || !digest(e.ConfigDigest) {
		return errors.New("harness: execution identity requires canonical SHA-256 digests")
	}
	return nil
}

// JobStatus describes authoritative executor evidence, never Run phases.
type JobStatus string

const (
	JobAbsent        JobStatus = "absent"
	JobPending       JobStatus = "pending"
	JobRunning       JobStatus = "running"
	JobSucceeded     JobStatus = "succeeded"
	JobFailed        JobStatus = "failed"
	JobCancelled     JobStatus = "cancelled"
	JobFailedToStart JobStatus = "failed_to_start"
	JobUnknown       JobStatus = "unknown"
)

// EnvironmentBinding identifies a command within a reusable execution environment.
// Generation is the authority's command slot generation, not an OS process ID.
type EnvironmentBinding struct {
	BackendID, EnvironmentID, StartEpoch string
	Incarnation, Generation              uint64
}

// Cessation proves that a bound command incarnation cannot execute anymore.
// ConfirmedAt is an observation time; it is not the command's unknown exit time.
type Cessation struct {
	EnvironmentID, StartEpoch string
	Generation                uint64
	Kind                      string
	RequestedAt, ConfirmedAt  time.Time
}

const (
	CessationStopped    = "environment_stopped"
	CessationEpochEnded = "start_epoch_ended"
)

// Job is a receipt for the accepted effect and specification. ID names a real
// physical owner. Fenced witnesses durable cancellation; a fenced cancellation
// may have no owner, but never a virtual ID. Process times come from backend evidence; dispatch and cessation times are
// controller observations.
// Terminal output metadata describes capture-time availability and never mutates
// after publication; artifact readers independently detect later data loss.
type Job struct {
	EffectID   string
	Spec       string
	ID         string
	Fenced     bool
	Status     JobStatus
	ExitCode   *int
	StartedAt  time.Time
	FinishedAt time.Time
	Preview    string
	Output     ArtifactRef
	// Dispatch time starts an attached command's runtime budget, independently
	// of whether its actual process start/exit timestamps are available.
	DispatchedAt time.Time           `json:",omitzero"`
	Binding      *EnvironmentBinding `json:",omitempty"`
	Cessation    *Cessation          `json:",omitempty"`
}

// Validate checks identity and evidence shape; the adapter additionally checks
// its durable binding witness. This is trusted evidence, not proof of arbitrary
// external effects.
func (j Job) Validate(e Effect) error {
	if e.ID == "" || !digest(e.Spec) || j.EffectID != e.ID || j.Spec != e.Spec {
		return errors.New("harness: receipt does not identify the accepted effect and specification")
	}
	if j.Output.Bytes < 0 || j.Output.Complete && !j.Output.Available {
		return errors.New("harness: invalid receipt artifact")
	}
	if !j.StartedAt.IsZero() && !j.FinishedAt.IsZero() && j.FinishedAt.Before(j.StartedAt) {
		return errors.New("harness: receipt finishes before it starts")
	}
	if err := j.environmentShape(); err != nil {
		return err
	}
	if !j.statusShape() {
		return fmt.Errorf("harness: receipt evidence contradicts status %q", j.Status)
	}
	return nil
}

func (j Job) statusShape() bool {
	switch j.Status {
	case JobAbsent:
		return j.ID == "" && !j.Fenced && j.noExecution() && j.noOutput()
	case JobPending:
		return j.ID != "" && !j.Fenced && j.noExecution() && j.noOutput()
	case JobRunning:
		return j.ID != "" && j.ExitCode == nil && !j.budgetOrigin().IsZero() && j.FinishedAt.IsZero() && j.noOutput()
	case JobSucceeded, JobFailed:
		return j.ID != "" && j.ExitCode != nil && (*j.ExitCode == 0) == (j.Status == JobSucceeded) && !j.StartedAt.IsZero() && !j.FinishedAt.IsZero()
	case JobFailedToStart:
		return j.ID != "" && j.ExitCode == nil && j.StartedAt.IsZero()
	case JobCancelled:
		return j.cancelledShape()
	case JobUnknown:
		return j.ID != "" || j.noExecution() && j.diagnosticOnly()
	}
	return false
}

func (j Job) noExecution() bool {
	return j.ExitCode == nil && j.StartedAt.IsZero() && j.FinishedAt.IsZero()
}
func (j Job) noOutput() bool { return j.Preview == "" && j.Output == (ArtifactRef{}) }
func (j Job) diagnosticOnly() bool {
	return j.Preview == "" && j.Output == (ArtifactRef{Detail: j.Output.Detail})
}

func (j Job) cancelledShape() bool {
	if !j.Fenced {
		return false
	}
	if j.ID == "" {
		return j.noExecution() && j.diagnosticOnly()
	}
	if j.Cessation != nil {
		return j.ExitCode == nil && j.FinishedAt.IsZero()
	}
	if j.StartedAt.IsZero() {
		return j.ExitCode == nil && j.FinishedAt.IsZero() && j.DispatchedAt.IsZero()
	}
	return !j.FinishedAt.IsZero() && j.ExitCode != nil
}

// environmentShape prevents an absent exit code or dispatch timestamp from
// being mistaken for physical termination evidence.
func (j Job) environmentShape() error {
	if j.Binding == nil {
		if j.Cessation != nil || !j.DispatchedAt.IsZero() {
			return errors.New("harness: environment evidence needs its command binding")
		}
		return nil
	}
	b := j.Binding
	if j.ID == "" || b.BackendID == "" || b.EnvironmentID == "" || b.StartEpoch == "" || b.Incarnation == 0 || b.Generation == 0 {
		return errors.New("harness: incomplete environment command binding")
	}
	if w := j.Cessation; w != nil {
		if j.Status != JobCancelled || !j.Fenced || j.ExitCode != nil || !j.FinishedAt.IsZero() || w.EnvironmentID != b.EnvironmentID || w.StartEpoch != b.StartEpoch || w.Generation != b.Generation || w.RequestedAt.IsZero() || w.ConfirmedAt.Before(w.RequestedAt) || w.Kind != CessationStopped && w.Kind != CessationEpochEnded {
			return errors.New("harness: cessation does not establish this command's termination")
		}
	}
	return nil
}

func (j Job) budgetOrigin() time.Time {
	if !j.DispatchedAt.IsZero() {
		return j.DispatchedAt
	}
	return j.StartedAt
}
func (j Job) budgetEnd() time.Time {
	if j.Cessation != nil {
		return j.Cessation.ConfirmedAt
	}
	return j.FinishedAt
}

// Resolution records who supplied the settlement evidence. An operator owner
// is an explicit assertion, never represented as an inspected receipt.
type Resolution struct{ Source, OwnerID string }

const (
	ResolutionInspection = "inspection"
	ResolutionOperator   = "operator"
)
