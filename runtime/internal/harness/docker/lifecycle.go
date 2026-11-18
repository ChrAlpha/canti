package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

type observation struct {
	latch  string
	marked bool
	fenced bool
	c      container
}

// observe checks every durable identity before contacting the backend. A
// binding remains authoritative even when its container never started.
func (x *Executor) observe(ctx context.Context, effect harness.Effect) (o observation, found bool, err error) {
	witness, err := x.readWitnesses(effect)
	if err != nil {
		return o, false, err
	}
	o.latch, o.fenced, o.marked = witness.latch, witness.fenced, witness.stopping
	if o.fenced && o.latch == "" {
		o.latch = latchCancel
	}
	o.c, found, err = x.inspect(ctx, effect.ID)
	if err != nil {
		return o, false, err
	}
	if witness.bound && (!found || witness.owner.ID != o.c.ID) {
		return o, false, fmt.Errorf("%w: effect %s owner %s is missing or replaced", harness.ErrJobLost, effect.ID, witness.owner.ID)
	}
	if found {
		labels := o.c.Config.Labels
		if labels["canti.spec"] != effect.Spec {
			return o, false, fmt.Errorf("docker: effect %s container has a different specification or execution protocol", effect.ID)
		}
		if err := x.binding(effect, o.c); err != nil {
			return o, false, err
		}
	}
	return o, found, nil
}

func (o observation) exitRecord() bool {
	return o.latch == latchStart && !o.marked && (o.c.State.Status == "exited" || o.c.State.Status == "dead")
}

func (x *Executor) finalize(ctx context.Context, effect harness.Effect, o observation) (harness.Job, error) {
	var recorded *int
	if o.exitRecord() {
		if !x.captureComplete(effect.ID) {
			job := derive(o.latch, o.c, o.marked, nil)
			job.EffectID, job.Spec, job.Fenced = effect.ID, effect.Spec, o.fenced
			job.Output.Detail = unrecorded
			if o.c.State.OOMKilled {
				job.Output.Detail += "; Docker observed an out-of-memory kill in the container"
			}
			return job, nil
		}
		code := o.c.State.ExitCode
		recorded = &code
	}
	job := derive(o.latch, o.c, o.marked, recorded)
	job.EffectID, job.Spec, job.Fenced = effect.ID, effect.Spec, o.fenced
	if job.CancelRequestedAt.IsZero() {
		job.CancelRequestedAt = x.admissionTime(effect)
	}
	if terminal(job.Status) {
		return x.finish(ctx, effect, job)
	}
	return job, job.Validate(effect)
}

// admissionTime reads the first durable cancellation admission. The effect
// value is used when it carries the request; otherwise the fence is the
// durable owner after a restart.
func (x *Executor) admissionTime(effect harness.Effect) time.Time {
	w, err := x.readWitnesses(effect)
	if err == nil && w.fenced && !w.cancelAt.IsZero() {
		return w.cancelAt
	}
	return effect.CancelRequestedAt
}

func (x *Executor) captureComplete(id string) bool {
	fi, err := os.Lstat(filepath.Join(x.capture(id), "completion.json"))
	return err == nil && fi.Mode().IsRegular()
}

func (x *Executor) absent(ctx context.Context, effect harness.Effect, fenced bool) (harness.Job, error) {
	job := harness.Job{EffectID: effect.ID, Spec: effect.Spec, Status: harness.JobAbsent, Fenced: fenced}
	if fenced {
		job.Status = harness.JobCancelled
		job.CancelRequestedAt = x.admissionTime(effect)
		return x.finish(ctx, effect, job)
	}
	return job, nil
}

// Drive recovers a matching created owner, publishes its binding and consumes
// its start latch at most once. A caller deadline is checked again after all
// binding work, immediately before the latch. The bounded launch completion
// after the latch is synchronous and remains owned by the calling controller.
func (x *Executor) Drive(ctx context.Context, effect harness.Effect) (harness.Job, error) {
	if err := x.checkEffect(effect); err != nil {
		return harness.Job{}, err
	}
	unlock, err := x.lock(ctx, effect.ID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	if rec, done, err := x.recorded(ctx, effect); err != nil || done {
		return rec.Job, err
	}
	o, found, err := x.observe(ctx, effect)
	if err != nil {
		return harness.Job{}, err
	}
	if !found && o.fenced {
		return x.absent(ctx, effect, true)
	}
	if !found {
		if o, err = x.createOwner(ctx, effect); err != nil {
			return harness.Job{}, err
		}
	}
	if o.latch == "" && o.c.State.Status == "created" {
		return x.launch(ctx, effect, o)
	}
	return x.finalize(ctx, effect, o)
}

func (x *Executor) createOwner(ctx context.Context, effect harness.Effect) (observation, error) {
	if err := ctx.Err(); err != nil {
		return observation{}, err
	}
	if err := x.checkHelper(ctx); err != nil {
		return observation{}, err
	}
	created := x.create(ctx, effect, effect.Spec)
	// A lost acknowledgement can still leave a matching owner. Lookup must
	// positively identify it before publication; lookup failure is not absence.
	o, found, err := x.observe(ctx, effect)
	if err != nil {
		return o, errors.Join(created, err)
	}
	if !found {
		return o, errors.Join(created, fmt.Errorf("docker: creation left no observable owner for effect %s", effect.ID))
	}
	return o, nil
}

func (x *Executor) launch(ctx context.Context, effect harness.Effect, o observation) (harness.Job, error) {
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	if err := x.checkHelper(ctx); err != nil {
		return harness.Job{}, err
	}
	if err := setLatch(x.dir(effect.ID), latchStart); err != nil {
		return harness.Job{}, err
	}
	launch := context.WithoutCancel(ctx)
	_, started := x.start(launch, "/containers/"+o.c.ID+"/start", nil)
	observed, found, err := x.observe(launch, effect)
	if err != nil {
		return harness.Job{}, errors.Join(started, err)
	}
	if !found {
		return harness.Job{}, fmt.Errorf("%w: effect %s", harness.ErrJobLost, effect.ID)
	}
	// Positive observed state is authoritative even if the acknowledgement was
	// lost; a created container without rejection evidence remains unknown.
	return x.finalize(launch, effect, observed)
}

// Inspect samples and may finalize the accepted owner, but never creates or
// starts work. An absent result requires a successful backend absence lookup.
func (x *Executor) Inspect(ctx context.Context, effect harness.Effect) (harness.Job, error) {
	if err := x.checkEffect(effect); err != nil {
		return harness.Job{}, err
	}
	unlock, err := x.lock(ctx, effect.ID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	if rec, done, err := x.recorded(ctx, effect); err != nil || done {
		return rec.Job, err
	}
	o, found, err := x.observe(ctx, effect)
	if err != nil {
		return harness.Job{}, err
	}
	if !found {
		return x.absent(ctx, effect, o.fenced)
	}
	return x.finalize(ctx, effect, o)
}

// Cancel commits its identity and fence before looking up an unbound owner.
// Existing terminal receipts are immutable. A fence plus failed lookup is
// uncertainty, never a no-owner cancellation receipt.
func (x *Executor) Cancel(ctx context.Context, effect harness.Effect) (harness.Job, error) {
	if err := x.checkEffect(effect); err != nil {
		return harness.Job{}, err
	}
	unlock, err := x.lock(ctx, effect.ID)
	if err != nil {
		return harness.Job{}, err
	}
	defer unlock()
	if rec, done, err := x.recorded(ctx, effect); err != nil || done {
		return rec.Job, err
	}
	if _, _, err := x.ownership(effect, "binding.json"); err != nil {
		return harness.Job{}, err
	}
	witness, err := x.fence(effect)
	if err != nil {
		return harness.Job{}, err
	}
	if !witness.CancelAt.IsZero() {
		effect.CancelRequestedAt = witness.CancelAt
	}
	o, found, err := x.observe(ctx, effect)
	if err != nil {
		return harness.Job{}, err
	}
	if !found {
		return x.absent(ctx, effect, true)
	}
	if running(o.c) {
		if err := x.stopRequested(effect, o.c.ID); err != nil {
			return harness.Job{}, err
		}
		if _, err := x.stop(ctx, o.c.ID, x.cfg.StopGrace); err != nil {
			return harness.Job{}, err
		}
		if o, _, err = x.observe(ctx, effect); err != nil {
			return harness.Job{}, err
		}
	}
	return x.finalize(ctx, effect, o)
}

// Wait has no lock and changes no state, so it cannot hold up cancellation.
// It checks the descriptor and binding before waiting on the physical ID.
func (x *Executor) Wait(ctx context.Context, effect harness.Effect) error {
	if err := x.checkEffect(effect); err != nil {
		return err
	}
	witness, err := x.readWitnesses(effect)
	if err != nil || !witness.bound {
		return err
	}
	return x.api(ctx, "wait", http.MethodPost, "/containers/"+witness.owner.ID+"/wait?condition=not-running", nil, func(r *http.Response) error {
		_, err := io.Copy(io.Discard, io.LimitReader(r.Body, 4<<10))
		return err
	})
}
