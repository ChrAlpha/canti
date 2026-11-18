package docker

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

func (x *Attached) cancel(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	return x.cancelMode(ctx, s, r, true)
}

// cancelForEnvironmentStop runs the pre-existing environment-wide path even
// when the foreground job's own process group could be terminated first: the
// operator asked for the whole environment to stop, not only its current job.
func (x *Attached) cancelForEnvironmentStop(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	if r.CancelAt.IsZero() {
		r.CancelAt = time.Now().UTC()
	}
	return x.cancelMode(ctx, s, r, false)
}

func (x *Attached) cancelMode(ctx context.Context, s *environmentState, r *attachedRecord, fast bool) (harness.Job, error) {
	if r.Terminal != nil {
		if err := x.releaseSlot(s, *r); err != nil {
			return *r.Terminal, errors.Join(errSlotRelease, err)
		}
		return *r.Terminal, nil
	}
	observed, err := x.sample(ctx, s, r)
	if err != nil {
		return harness.Job{}, err
	}
	if fast && observed.Status == harness.JobUnknown && r.exitUnverified {
		return observed, nil
	}
	if terminal(observed.Status) {
		return observed, nil
	}
	if err = x.fenceCancellation(r); err != nil {
		return harness.Job{}, err
	}
	if r.DispatchedAt.IsZero() {
		j := x.baseJob(*r)
		j.Status = harness.JobCancelled
		return x.finishAttached(s, r, j)
	}
	if fast {
		job, done, err := x.fastCancellation(ctx, s, r)
		if err != nil {
			return harness.Job{}, err
		}
		if done {
			return job, nil
		}
		return x.finishAfterFailedAwait(ctx, s, r)
	}
	return x.stopForCancellation(ctx, s, r, false)
}

// finishAfterFailedAwait settles a proof whose original exec has ended but
// whose capture never appeared. A still-running helper is not proven done, so
// that case keeps the environment-stop fallback.
func (x *Attached) finishAfterFailedAwait(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	if r.Termination == nil {
		return x.stopForCancellation(ctx, s, r, true)
	}
	c, err := x.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil || c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return x.stopForCancellation(ctx, s, r, true)
	}
	m, err := x.inspectExec(ctx, *r)
	if err != nil || m.Running || m.Pid <= 0 || m.ExitCode == nil {
		return x.stopForCancellation(ctx, s, r, true)
	}
	return x.terminatedWithoutCompletion(ctx, s, r, c.State.Running)
}

func (x *Attached) fenceCancellation(r *attachedRecord) error {
	if r.Fenced && !r.CancelAt.IsZero() {
		return nil
	}
	next := *r
	next.Fenced = true
	if err := x.saveJob(next); err != nil {
		return err
	}
	*r = next
	return nil
}

func (x *Attached) fastCancellation(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, bool, error) {
	if r.ExecID == "" || s.Pending != "" || len(s.Requests) != 0 {
		return harness.Job{}, false, nil
	}
	if r.Termination != nil {
		return x.awaitTerminated(ctx, s, r)
	}
	for attempt := 0; attempt < 3; attempt++ {
		if x.terminateGroup(ctx, r) {
			// Control exit 0 proves only that the control found and waited for
			// the live helper; the bound original exec's status 255 is the proof.
			return x.awaitTerminated(ctx, s, r)
		}
		natural, done, err := x.naturalAfterFailedControl(ctx, s, r, true)
		if err != nil || done {
			return natural, done, err
		}
		select {
		case <-ctx.Done():
			return harness.Job{}, false, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return harness.Job{}, false, nil
}

func (x *Attached) naturalAfterFailedControl(ctx context.Context, s *environmentState, r *attachedRecord, natural bool) (harness.Job, bool, error) {
	job, err := x.sampleMode(ctx, s, r, natural)
	if err != nil {
		return harness.Job{}, false, err
	}
	if job.Status == harness.JobUnknown && r.exitUnverified {
		return job, true, nil
	}
	if terminal(job.Status) {
		return job, true, nil
	}
	return harness.Job{}, false, nil
}

// terminateGroup runs the versioned control exec. The control finds the live
// helper of this effect, spec, generation and epoch through PPid 0 and argv,
// pins it with pidfd, signals it, and exits 0 only after the helper exited and
// its child group has no live task. The host never reads a signal target from
// the spool.
func (x *Attached) terminateGroup(ctx context.Context, r *attachedRecord) bool {
	c, err := x.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil || !c.ready() || c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return false
	}
	dir, request, err := x.loadRequest(r.Launch)
	if err != nil {
		return false
	}
	settled, err := requestSettled(dir, request)
	if err != nil || !settled {
		return false
	}
	m, err := x.inspectExec(ctx, *r)
	if err != nil || m.Pid <= 0 {
		return false
	}
	expected := containerPID(m.Pid)
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*x.cfg.StopGrace+5*time.Second)
	defer cancel()
	code, err := x.runTerminateExec(call, *r, expected)
	if (err != nil || code != 0) && expected != 0 {
		code, err = x.runTerminateExec(call, *r, 0)
	}
	if err != nil || code != 0 {
		return false
	}
	c, err = x.inspectPrepared(call, x.cfg.Attachment.ContainerID)
	ok := err == nil && c.ready() && c.State.StartedAt.Format(time.RFC3339Nano) == r.Epoch
	return ok
}

// containerPID maps a daemon host PID to the container PID visible in its
// /proc/<pid>/status NSpid list. The cross-check is optional: a remote or
// rootless daemon may be unreadable, in which case PPid 0 + argv remain the
// identity and the control receives 0.
func containerPID(hostPID int) int {
	return containerPIDAt("/proc", hostPID)
}

func containerPIDAt(root string, hostPID int) int {
	data, err := os.ReadFile(filepath.Join(root, strconv.Itoa(hostPID), "status"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "NSpid:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "NSpid:"))
		if len(fields) == 0 {
			return 0
		}
		pid, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil || pid <= 0 {
			return 0
		}
		return pid
	}
	return 0
}

func (x *Attached) runTerminateExec(ctx context.Context, r attachedRecord, expectedPID int) (int, error) {
	a := x.cfg.Attachment
	request := execRequest{User: a.User, WorkingDir: "/", Cmd: []string{
		a.Helper, "-terminate",
		"-effect", r.Effect.ID,
		"-spec", r.Effect.Spec,
		"-generation", strconv.FormatUint(r.Generation, 10),
		"-epoch", r.Epoch,
		"-grace", x.cfg.StopGrace.String(),
		"-helper-pid", strconv.Itoa(expectedPID),
	}, Env: commandEnv(a)}
	var created struct{ ID string }
	if err := x.api(ctx, "exec-create", http.MethodPost, "/containers/"+a.ContainerID+"/exec", request, decodeResponse(&created)); err != nil {
		return 0, err
	}
	if len(created.ID) != 64 {
		return 0, errors.New("docker: termination exec creation returned an invalid physical ID")
	}
	if _, err := x.start(ctx, "/exec/"+created.ID+"/start", struct{ Detach, Tty bool }{Detach: true}); err != nil {
		return 0, err
	}
	for {
		m, err := x.inspectExecID(ctx, created.ID)
		if err != nil {
			return 0, err
		}
		if !m.Running {
			if m.Pid <= 0 || m.ExitCode == nil {
				return 0, errors.New("docker: termination exec has no started-process evidence")
			}
			return *m.ExitCode, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (x *Attached) awaitTerminated(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, bool, error) {
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), x.cfg.StopGrace+5*time.Second)
	defer cancel()
	for {
		job, err := x.sample(call, s, r)
		if err != nil {
			if errors.Is(err, errAttachedDestination) || errors.Is(err, errTerminalWrite) || errors.Is(err, errSlotRelease) {
				return harness.Job{}, false, err
			}
		} else {
			if job.Status == harness.JobUnknown && r.exitUnverified {
				return job, true, nil
			}
			if terminal(job.Status) {
				return job, true, nil
			}
		}
		select {
		case <-call.Done():
			return harness.Job{}, false, nil
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (x *Attached) stopForCancellation(ctx context.Context, s *environmentState, r *attachedRecord, preserve bool) (harness.Job, error) {
	c, err := x.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return x.cancelOldEpoch(s, r)
	}
	// A natural exit may have raced the failed controls. Before an ordinary
	// cancellation falls back to Docker, sample the original exec: a settled
	// result keeps the services running, and only a still-running exec may stop.
	if preserve {
		final, sampleErr := x.sample(ctx, s, r)
		if sampleErr != nil {
			return harness.Job{}, sampleErr
		}
		if terminal(final.Status) || final.Status == harness.JobUnknown && r.exitUnverified {
			return final, nil
		}
	}
	if c.State.Running {
		if s.Pending != "" {
			return x.unknown(*r, "previous lifecycle request is unacknowledged; environment remains quarantined; retire after explicit old-ID removal")
		}
		stopErr := x.lifecycleRequest(ctx, s, "environment_stop", r.Generation)
		next := *r
		next.CancelIntent = s.LastStop
		next.StopProof = x.acknowledgedStopProof(next.CancelIntent, r.Generation)
		if err = x.saveJob(next); err != nil {
			return harness.Job{}, err
		}
		*r = next
		c, err = x.inspectPrepared(context.WithoutCancel(ctx), x.cfg.Attachment.ContainerID)
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

// acknowledgedStopProof returns the request identity digest only after the
// environment-stop request is durably acknowledged. An intent alone is never
// accepted as proof when a cached receipt outlives its authority directory.
func (x *Attached) acknowledgedStopProof(ref string, generation uint64) string {
	if ref == "" {
		return ""
	}
	dir, request, err := x.loadRequest(ref)
	if err != nil || request.Kind != "environment_stop" || request.Generation != generation {
		return ""
	}
	settled, err := requestSettled(dir, request)
	if err != nil || !settled {
		return ""
	}
	digest, err := requestIdentity(dir, request)
	if err != nil {
		return ""
	}
	return digest
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
	spool, err := os.OpenRoot(filepath.Join(a.Directory, "spool"))
	if err == nil {
		defer spool.Close()
		command, openErr := spool.OpenRoot(r.Effect.ID)
		if openErr == nil {
			defer command.Close()
			n, copyErr := publishOutputEvidence(command, filepath.Join(x.cfg.State, "artifacts", r.Effect.ID, "output"), x.cfg.OutputLimit)
			if copyErr == nil && n > 0 {
				j.Output.Path, j.Output.Bytes, j.Output.Available = r.Effect.ID+"/output", n, true
				if preview, previewErr := jobrunner.PreviewEvidence(command, "output", x.cfg.OutputLimit, x.cfg.PreviewLimit, spoolOutput(r.Effect.ID), x.cfg.OutputLimit, true); previewErr == nil {
					j.Preview = preview
				}
			}
			if copyErr != nil && !(errors.Is(copyErr, jobrunner.ErrEvidenceSource) && errors.Is(copyErr, os.ErrNotExist)) {
				return harness.Job{}, copyErr
			}
		}
	}
	if j.Preview == "" {
		j.Preview = partialUnavailablePreview(spoolOutput(r.Effect.ID))
	}
	if kind == harness.CessationQuarantined {
		j.Output.Detail += "; environment is quarantined until explicit retirement and replacement"
	}
	return x.finishAttached(s, r, j)
}
