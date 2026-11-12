package hostrpc

import (
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

var jobWords = map[harness.JobStatus]string{
	harness.JobAbsent: "absent", harness.JobPending: "pending", harness.JobRunning: "running",
	harness.JobSucceeded: "succeeded", harness.JobFailed: "failed", harness.JobCancelled: "cancelled",
	harness.JobFailedToStart: "failed_to_start", harness.JobUnknown: "unknown",
}

// InspectionOf is the wire representation of a physical sample.
func InspectionOf(s host.Inspection) Inspection {
	out := Inspection{RunID: s.RunID, Cursor: CursorOf(host.Cursor(s.Pos)), EffectID: s.EffectID, SampledAt: s.SampledAt.Format("2006-01-02T15:04:05.999999999Z07:00"), Unavailable: s.Unavailable}
	if j := s.Receipt; j != nil {
		r := &Receipt{EffectID: j.EffectID, Spec: j.Spec, OwnerID: j.ID, Status: word(jobWords, j.Status), Fenced: j.Fenced, ExitCode: j.ExitCode, Preview: j.Preview, OutputPath: j.Output.Path, OutputBytes: j.Output.Bytes, OutputAvailable: j.Output.Available, OutputComplete: j.Output.Complete, Detail: j.Output.Detail}
		if !j.DispatchedAt.IsZero() {
			r.DispatchedAt = j.DispatchedAt.Format("2006-01-02T15:04:05.999999999Z07:00")
		}
		if b := j.Binding; b != nil {
			r.Binding = &EnvironmentBinding{BackendID: b.BackendID, EnvironmentID: b.EnvironmentID, StartEpoch: b.StartEpoch, AuthorityID: b.AuthorityID, Incarnation: b.Incarnation, Generation: b.Generation}
		}
		if c := j.Cessation; c != nil {
			r.Cessation = &Cessation{Binding: EnvironmentBinding{BackendID: c.BackendID, EnvironmentID: c.EnvironmentID, StartEpoch: c.StartEpoch, AuthorityID: c.AuthorityID, Incarnation: c.Incarnation, Generation: c.Generation}, EffectID: c.EffectID, Spec: c.Spec, OwnerID: c.OwnerID, StopIntent: c.StopIntent, Kind: c.Kind, RequestedAt: c.RequestedAt.Format("2006-01-02T15:04:05.999999999Z07:00"), ConfirmedAt: c.ConfirmedAt.Format("2006-01-02T15:04:05.999999999Z07:00")}
		}
		if !j.StartedAt.IsZero() {
			r.StartedAt = j.StartedAt.Format("2006-01-02T15:04:05.999999999Z07:00")
		}
		if !j.FinishedAt.IsZero() {
			r.FinishedAt = j.FinishedAt.Format("2006-01-02T15:04:05.999999999Z07:00")
		}
		out.Receipt = r
	}
	return out
}
