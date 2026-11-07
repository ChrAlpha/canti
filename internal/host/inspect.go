package host

import (
	"context"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// Inspection is a journal position and a separately timed physical sample.
// Unavailable explains why no receipt could be established. This operation
// commits no Run progress and never authorizes creation or launch. Terminal
// artifact metadata is capture-time evidence; archive reads check current data.
type Inspection struct {
	RunID       string
	Seq         int64
	EffectID    string
	SampledAt   time.Time
	Receipt     *harness.Job
	Unavailable string
}

// Inspect samples the current accepted external effect with a bounded,
// non-starting operation accounted to the Host lifetime. It may finalize and
// retain output. It does not interrupt or acquire the controller's Run lock.
func (h *Host) Inspect(ctx context.Context, id string) (Inspection, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return Inspection{}, err
	}
	defer leave()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	run, c, err := h.Find(call, id)
	if err != nil {
		return Inspection{}, err
	}
	l, err := h.loop(run, c, NoModel)
	if err != nil {
		return Inspection{}, err
	}
	snap, job, err := l.Inspect(call, run.ID)
	sample := Inspection{RunID: run.ID, Seq: snap.Seq, EffectID: snap.State.EffectID, SampledAt: time.Now().UTC()}
	if err != nil {
		sample.Unavailable = err.Error()
	} else {
		sample.Receipt = &job
	}
	return sample, nil
}
