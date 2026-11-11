package host

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// CancelEffect cancels the accepted effect of run id, or of the latest run,
// once and records its result, without stopping the run: it continues to its
// next step with a cancelled observation. When the host drives the run, the
// request goes to the drive's effect watch; otherwise a loop without a model
// performs the same cancellation. It refuses a run that holds no accepted
// effect. An acknowledged cancellation of an attached environment stops the
// environment, as a timeout does, and its receipt reports that.
func (h *Host) CancelEffect(ctx context.Context, id string) (harness.Snapshot, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return harness.Snapshot{}, err
	}
	defer leave()
	run, c, err := h.Find(ctx, id)
	if err != nil {
		return harness.Snapshot{}, err
	}
	id = run.ID
	unlock, err := h.lockRun(ctx, id)
	if err != nil {
		return harness.Snapshot{}, err
	}
	defer unlock()
	snap, err := h.store.Latest(ctx, id)
	if err != nil {
		return snap, err
	}
	if snap.State.Phase != core.PhaseEffectSaved {
		return snap, fmt.Errorf("run %s in phase %q has no accepted effect to cancel", id, snap.State.Phase)
	}
	h.mu.Lock()
	d := h.drivers[id]
	h.mu.Unlock()
	if d != nil && d.loop != nil {
		call, stop := context.WithCancel(ctx)
		defer stop()
		go func() {
			select {
			case <-d.done:
				stop()
			case <-call.Done():
			}
		}()
		next, err := d.loop.RequestCancelEffect(call, id)
		if errors.Is(err, context.Canceled) && ctx.Err() == nil {
			// The drive ended before the watch took the request. Its own
			// final observation, if any, is the answer.
			return h.store.Latest(ctx, id)
		}
		return next, err
	}
	l, err := h.loop(run, c, NoModel)
	if err != nil {
		return snap, err
	}
	return l.CancelEffect(ctx, id)
}

// ErrNoEffect reports a cancel of a run that holds no accepted effect.
var ErrNoEffect = errors.New("the run holds no accepted effect to cancel")
