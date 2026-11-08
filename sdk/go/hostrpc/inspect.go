package hostrpc

import (
	"context"
	"errors"
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
