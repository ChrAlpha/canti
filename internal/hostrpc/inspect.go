package hostrpc

import (
	"context"
	"errors"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/host"
)

// Inspection is an explicitly sampled physical status, separate from the
// journal-only Run projection. The timestamp is RFC3339 with nanoseconds.
type Inspection struct {
	RunID       string   `json:"run_id"`
	Cursor      string   `json:"cursor"`
	EffectID    string   `json:"effect_id,omitempty"`
	SampledAt   string   `json:"sampled_at"`
	Receipt     *Receipt `json:"receipt,omitempty"`
	Unavailable string   `json:"unavailable,omitempty"`
}

// Receipt is executor evidence; OwnerID names the physical owner, when one
// exists. It embeds no harness or journal type.
type Receipt struct {
	EffectID        string `json:"effect_id"`
	Spec            string `json:"spec"`
	OwnerID         string `json:"owner_id,omitempty"`
	Status          string `json:"status"`
	Fenced          bool   `json:"fenced"`
	ExitCode        *int   `json:"exit_code,omitempty"`
	StartedAt       string `json:"started_at,omitempty"`
	FinishedAt      string `json:"finished_at,omitempty"`
	Preview         string `json:"preview,omitempty"`
	OutputPath      string `json:"output_path,omitempty"`
	OutputBytes     int64  `json:"output_bytes"`
	OutputAvailable bool   `json:"output_available"`
	OutputComplete  bool   `json:"output_complete"`
	Detail          string `json:"detail,omitempty"`
}

var jobWords = map[harness.JobStatus]string{
	harness.JobAbsent: "absent", harness.JobPending: "pending", harness.JobRunning: "running",
	harness.JobSucceeded: "succeeded", harness.JobFailed: "failed", harness.JobCancelled: "cancelled",
	harness.JobFailedToStart: "failed_to_start", harness.JobUnknown: "unknown",
}

// InspectionOf is the wire representation of a physical sample.
func InspectionOf(s host.Inspection) Inspection {
	out := Inspection{RunID: s.RunID, Cursor: CursorOf(host.Cursor{RunID: s.RunID, Seq: s.Seq}), EffectID: s.EffectID, SampledAt: s.SampledAt.Format("2006-01-02T15:04:05.999999999Z07:00"), Unavailable: s.Unavailable}
	if j := s.Receipt; j != nil {
		r := &Receipt{EffectID: j.EffectID, Spec: j.Spec, OwnerID: j.ID, Status: word(jobWords, j.Status), Fenced: j.Fenced, ExitCode: j.ExitCode, Preview: j.Preview, OutputPath: j.Output.Path, OutputBytes: j.Output.Bytes, OutputAvailable: j.Output.Available, OutputComplete: j.Output.Complete, Detail: j.Output.Detail}
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

// Inspect requests one bounded non-starting sample.
func (c *Client) Inspect(ctx context.Context, id string) (Inspection, error) {
	resp, err := c.Call(ctx, Request{Op: OpInspect, RunID: id})
	if err != nil {
		return Inspection{}, err
	}
	if resp.Inspection == nil {
		return Inspection{}, errors.New("hostrpc: inspection without a sample")
	}
	return *resp.Inspection, nil
}
