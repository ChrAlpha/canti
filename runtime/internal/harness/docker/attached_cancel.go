package docker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func (x *Attached) cancel(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	if r.Terminal != nil {
		return *r.Terminal, x.releaseSlot(s, *r)
	}
	observed, err := x.sample(ctx, s, r)
	if err != nil {
		return harness.Job{}, err
	}
	if terminal(observed.Status) {
		return observed, nil
	}
	r.Fenced = true
	if r.CancelAt.IsZero() {
		r.CancelAt = time.Now().UTC()
	}
	if err = x.saveJob(*r); err != nil {
		return harness.Job{}, err
	}
	if r.DispatchedAt.IsZero() {
		j := x.baseJob(*r)
		j.Status = harness.JobCancelled
		return x.finishAttached(s, r, j)
	}
	c, err := x.backend.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return x.cancelOldEpoch(s, r)
	}
	if c.State.Running {
		if s.Pending != "" {
			return x.unknown(*r, "previous lifecycle request is unacknowledged; environment remains quarantined; retire after explicit old-ID removal")
		}
		stopErr := x.lifecycleRequest(ctx, s, "environment_stop", r.Generation)
		r.CancelIntent = s.LastStop
		if err = x.saveJob(*r); err != nil {
			return harness.Job{}, err
		}
		c, err = x.backend.inspectPrepared(context.WithoutCancel(ctx), x.cfg.Attachment.ContainerID)
		if err != nil {
			return harness.Job{}, errors.Join(stopErr, err)
		}
		if c.State.Running {
			return x.unknown(*r, "environment stop is not confirmed; retain its active command and quarantine")
		}
	}
	// Prefer a real shell exit if it survived stop and its exec metadata is
	// still available. Otherwise cessation keeps process timestamps/exit unknown.
	if finished, sampleErr := x.sample(context.WithoutCancel(ctx), s, r); sampleErr == nil && terminal(finished.Status) {
		return finished, nil
	}
	if x.hasUnsettledStart(*s) {
		return x.unknown(*r, "unacknowledged environment start prevents a cessation assertion")
	}
	if err = x.pruneRequests(s); err != nil {
		return harness.Job{}, err
	}
	kind := harness.CessationStopped
	if len(s.Requests) > 0 || s.Pending != "" {
		s.Gate = "quarantined"
		kind = harness.CessationQuarantined
	} else {
		s.Gate = "stopped"
	}
	if err = saveEnvironmentState(x.cfg.Attachment.Directory, *s); err != nil {
		return harness.Job{}, err
	}
	return x.publishCessation(s, r, kind)
}
func (x *Attached) hasUnsettledStart(s environmentState) bool {
	for _, ref := range s.Requests {
		_, r, err := x.loadRequest(ref)
		if err != nil || r.Kind == "environment_start" {
			return true
		}
	}
	return false
}
func (x *Attached) cancelOldEpoch(s *environmentState, r *attachedRecord) (harness.Job, error) {
	dir, request, err := x.loadRequest(r.Launch)
	if err != nil {
		return harness.Job{}, err
	}
	settled, err := requestSettled(dir, request)
	if err != nil {
		return harness.Job{}, err
	}
	if !settled || x.hasUnsettledStart(*s) {
		s.Gate = "quarantined"
		if err = saveEnvironmentState(x.cfg.Attachment.Directory, *s); err != nil {
			return harness.Job{}, err
		}
		return x.unknown(*r, "old epoch ended but launch remains ambiguous; no mutation of the current epoch is permitted")
	}
	return x.publishCessation(s, r, harness.CessationEpochEnded)
}
func (x *Attached) publishCessation(s *environmentState, r *attachedRecord, kind string) (harness.Job, error) {
	a := x.cfg.Attachment
	// The command record is itself a durable cancellation intent when an already
	// stopped/ended environment needs no new external stop request.
	intent, _ := marshal(struct {
		Effect      harness.Effect
		Exec, Epoch string
		Generation  uint64
		At          time.Time
	}{r.Effect, r.ExecID, r.Epoch, r.Generation, r.CancelAt})
	digest := harness.ConfigDigest(intent)
	if r.CancelIntent != "" {
		dir, request, err := x.loadRequest(r.CancelIntent)
		if err != nil {
			return harness.Job{}, err
		}
		if request.Kind != "environment_stop" || request.Generation != r.Generation {
			return harness.Job{}, errors.New("docker: cessation has another command's stop intent")
		}
		digest, err = requestIdentity(dir, request)
		if err != nil {
			return harness.Job{}, err
		}
	}
	j := x.baseJob(*r)
	j.Status = harness.JobCancelled
	j.Cessation = &harness.Cessation{AuthorityID: a.AuthorityID, BackendID: a.DaemonID, EnvironmentID: a.ContainerID, StartEpoch: r.Epoch, EffectID: r.Effect.ID, Spec: r.Effect.Spec, OwnerID: r.ExecID, StopIntent: digest, Incarnation: a.Incarnation, Generation: r.Generation, Kind: kind, RequestedAt: r.CancelAt, ConfirmedAt: time.Now().UTC()}
	j.Output.Detail = "environment stopped; command exit code unavailable; background services also stopped"
	data, err := spoolEvidence(a.Directory, r.Effect.ID, "output", x.cfg.OutputLimit)
	if err == nil {
		path := filepath.Join(x.cfg.State, "artifacts", r.Effect.ID, "output")
		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return harness.Job{}, err
		}
		if err = publish(path, data, false); err != nil {
			return harness.Job{}, err
		}
		if err = syncDir(filepath.Dir(filepath.Dir(path))); err != nil {
			return harness.Job{}, err
		}
		tap := &tap{w: io.Discard, limit: x.cfg.OutputLimit, previewLimit: x.cfg.PreviewLimit}
		_, _ = tap.Write(data)
		j.Preview = tap.preview()
		j.Output.Path, j.Output.Bytes, j.Output.Available = r.Effect.ID+"/output", int64(len(data)), true
	}
	if kind == harness.CessationQuarantined {
		j.Output.Detail += "; environment is quarantined until explicit retirement and replacement"
	}
	return x.finishAttached(s, r, j)
}
