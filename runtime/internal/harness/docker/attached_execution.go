package docker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

var (
	errAttachedEvidence    = errors.New("docker: unsafe or unavailable command capture evidence")
	errAttachedDestination = errors.New("docker: command capture destination failed")
	errTerminalWrite       = errors.New("docker: terminal receipt write failed")
	errSlotRelease         = errors.New("docker: command slot release failed")
)

type execMetadata struct {
	ID, ContainerID string
	Running         bool
	Pid             int
	ExitCode        *int
}
type execRequest struct {
	User, WorkingDir                             string
	Env, Cmd                                     []string
	AttachStdin, AttachStdout, AttachStderr, Tty bool
}

func (x *Attached) execRequest(r attachedRecord) execRequest {
	a := x.cfg.Attachment
	command := []string{a.Helper, "-directory", spoolRoot + "/" + r.Effect.ID, "-artifact", spoolOutput(r.Effect.ID), "-effect", r.Effect.ID, "-spec", r.Effect.Spec, "-epoch", r.Epoch, "-remap-exit", "-generation", strconv.FormatUint(r.Generation, 10), "-grace", x.cfg.StopGrace.String(), "-workdir", x.cfg.Workdir, "-output-limit", strconv.FormatInt(x.cfg.OutputLimit, 10), "-preview-limit", strconv.Itoa(x.cfg.PreviewLimit)}
	if a.Init != "" {
		command = append(command, "-init", a.Init)
	}
	command = append(command, "--", r.Effect.Command)
	// The helper launches from the root directory and its shell enters the
	// workdir: a removed workdir fails the command, not the exec's creation.
	return execRequest{User: a.User, WorkingDir: "/", Cmd: command, Env: commandEnv(a)}
}

// commandEnv is the environment of every process the controller starts in the
// prepared container: its pinned HOME and PATH, with model credentials blanked.
func commandEnv(a Attachment) []string {
	return []string{"HOME=" + a.Home, "PATH=" + a.Path, "ANTHROPIC_API_KEY=", "ANTHROPIC_AUTH_TOKEN=", "OPENAI_API_KEY="}
}
func (x *Attached) dispatch(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	if s.Gate != "open" || len(s.Requests) > 0 {
		return harness.Job{}, gateError(s.Gate)
	}
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	a := x.cfg.Attachment
	c, err := x.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if !c.ready() || c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return harness.Job{}, errors.New("docker: environment start epoch changed before dispatch; cancel this accepted command")
	}
	if err = x.mirrorReference(*r); err != nil {
		return harness.Job{}, err
	}
	if r.ExecID == "" {
		if err = os.MkdirAll(filepath.Join(a.Directory, "spool", r.Effect.ID), 0700); err != nil {
			return harness.Job{}, err
		}
		if err = syncDir(filepath.Join(a.Directory, "spool")); err != nil {
			return harness.Job{}, err
		}
		var created struct{ ID string }
		err = x.api(ctx, "exec-create", http.MethodPost, "/containers/"+a.ContainerID+"/exec", x.execRequest(*r), decodeResponse(&created))
		if err != nil {
			return harness.Job{}, err
		}
		if len(created.ID) != 64 {
			return harness.Job{}, errors.New("docker: exec creation returned an invalid physical ID")
		}
		r.ExecID = created.ID
		if err = x.saveJob(*r); err != nil {
			return harness.Job{}, err
		}
		if err = x.mirrorReference(*r); err != nil {
			return harness.Job{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	c, err = x.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if !c.State.Running || c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return harness.Job{}, errors.New("docker: environment changed before launch latch")
	}
	request := requestFence{Format: 1, Kind: "exec_start", AuthorityID: a.AuthorityID, BackendID: a.DaemonID, EnvironmentID: a.ContainerID, TargetID: r.ExecID, Incarnation: a.Incarnation, Generation: r.Generation}
	dir, err := beginRequest(x.authority(), request)
	if err != nil {
		return harness.Job{}, err
	}
	r.Launch, r.DispatchedAt = filepath.Base(dir), time.Now().UTC()
	if err = x.saveJob(*r); err != nil {
		return harness.Job{}, err
	}
	if err = x.mirrorLaunch(*r); err != nil {
		return harness.Job{}, err
	}
	s.Requests = append(s.Requests, r.Launch)
	if err = saveEnvironmentState(a.Directory, *s); err != nil {
		return harness.Job{}, err
	}
	call := context.WithoutCancel(ctx)
	response, started := x.start(call, "/exec/"+r.ExecID+"/start", struct{ Detach, Tty bool }{Detach: true})
	if started == nil {
		started = acknowledgeRequest(dir, request, response, false)
	}
	job, observed := x.sample(call, s, r)
	if observed != nil {
		return harness.Job{}, errors.Join(started, observed)
	}
	// A positive exact-exec sample can supersede a lost acknowledgement. An
	// unknown sample remains unknown; it never permits another start request.
	return job, nil
}
func (x *Attached) inspectExec(ctx context.Context, r attachedRecord) (execMetadata, error) {
	m, err := x.inspectExecID(ctx, r.ExecID)
	if err != nil {
		return m, err
	}
	if m.ID != r.ExecID || m.ContainerID != x.cfg.Attachment.ContainerID {
		return m, errors.New("docker: exec metadata does not identify the bound command")
	}
	return m, nil
}
func spoolOutput(effectID string) string { return spoolRoot + "/" + effectID + "/output" }

func (x *Attached) inspectExecID(ctx context.Context, id string) (execMetadata, error) {
	var m execMetadata
	err := x.api(ctx, "exec-inspect", http.MethodGet, "/exec/"+id+"/json", nil, decodeResponse(&m))
	if err != nil {
		return m, err
	}
	if m.ID != id || m.ContainerID != x.cfg.Attachment.ContainerID {
		return m, errors.New("docker: exec metadata does not identify the requested command")
	}
	return m, nil
}
func (x *Attached) baseJob(r attachedRecord) harness.Job {
	j := harness.Job{EffectID: r.Effect.ID, Spec: r.Effect.Spec, ID: r.ExecID, Fenced: r.Fenced, DispatchedAt: r.DispatchedAt}
	if r.ExecID != "" {
		a := x.cfg.Attachment
		j.Binding = &harness.EnvironmentBinding{AuthorityID: a.AuthorityID, BackendID: a.DaemonID, EnvironmentID: a.ContainerID, StartEpoch: r.Epoch, Incarnation: a.Incarnation, Generation: r.Generation}
	}
	return j
}
func (x *Attached) unknown(r attachedRecord, detail string) (harness.Job, error) {
	j := x.baseJob(r)
	j.Status = harness.JobUnknown
	j.Output.Detail = detail
	return j, j.Validate(r.Effect)
}
func (x *Attached) sample(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	return x.sampleMode(ctx, s, r, false)
}

// sampleMode samples the bound exec. natural allows a verified ordinary command
// completion to settle a fenced effect after a targetless control attempt, as
// the shell did exit on its own.
func (x *Attached) sampleMode(ctx context.Context, s *environmentState, r *attachedRecord, natural bool) (harness.Job, error) {
	if r.Terminal != nil {
		if err := x.releaseSlot(s, *r); err != nil {
			return *r.Terminal, errors.Join(errSlotRelease, err)
		}
		return *r.Terminal, nil
	}
	if r.ExecID == "" {
		j := harness.Job{EffectID: r.Effect.ID, Spec: r.Effect.Spec, Status: harness.JobAbsent}
		return j, nil
	}
	if r.DispatchedAt.IsZero() {
		if r.Fenced {
			return x.unknown(*r, "fenced command was never dispatched; cancel settles it without a process proof")
		}
		j := x.baseJob(*r)
		j.Status = harness.JobPending
		return j, j.Validate(r.Effect)
	}
	r.exitUnverified = false
	if r.Termination != nil {
		return x.sampleProof(ctx, s, r)
	}
	if err := x.ensureLaunchReference(s, *r); err != nil {
		return harness.Job{}, err
	}
	c, err := x.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return x.unknown(*r, "environment start epoch changed; stop or inspect the old effect without replay")
	}
	m, err := x.inspectExec(ctx, *r)
	if err != nil {
		if status(err) == http.StatusNotFound {
			if r.Termination != nil {
				return x.terminatedWithoutCompletion(ctx, s, r, c.State.Running)
			}
			return x.unknown(*r, "bound exec metadata is missing; helper files alone cannot establish an exit; stop to reconcile")
		}
		return harness.Job{}, err
	}
	if m.Running || m.Pid > 0 && m.ExitCode != nil {
		if err = x.acknowledgeSample(s, *r, c); err != nil {
			return harness.Job{}, err
		}
	}

	if m.Running && !c.State.Running {
		return x.unknown(*r, "environment is stopped while exec metadata remains running; confirm cessation instead of inventing an exit")
	}
	if m.Running {
		j := x.baseJob(*r)
		j.Status = harness.JobRunning
		return j, j.Validate(r.Effect)
	}
	if m.Pid == 0 || m.ExitCode == nil {
		return x.unknown(*r, "launch is unconfirmed; unstarted exec defaults never establish successful completion")
	}
	return x.completed(ctx, s, r, m, natural, c.State.Running)
}

func (x *Attached) sampleProof(ctx context.Context, s *environmentState, r *attachedRecord) (harness.Job, error) {
	c, inspectErr := x.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if inspectErr != nil || c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch || !c.State.Running {
		return x.terminatedFromCapture(ctx, s, r, false)
	}
	m, execErr := x.inspectExec(ctx, *r)
	if execErr != nil || m.Running || m.Pid <= 0 || m.ExitCode == nil {
		return x.terminatedFromCapture(ctx, s, r, c.State.Running)
	}
	return x.completed(ctx, s, r, m, false, c.State.Running)
}

func (x *Attached) terminatedFromCapture(ctx context.Context, s *environmentState, r *attachedRecord, environmentRunning bool) (harness.Job, error) {
	rec, haveRecord := x.completionEvidence(*r)
	return x.terminatedReceipt(ctx, s, r, rec, haveRecord, time.Now().UTC(), environmentRunning)
}

func (x *Attached) ensureLaunchReference(s *environmentState, r attachedRecord) error {
	for _, ref := range s.Requests {
		if ref == r.Launch {
			return nil
		}
	}
	dir, request, err := x.loadRequest(r.Launch)
	if err != nil {
		return err
	}
	settled, err := requestSettled(dir, request)
	if err != nil || settled {
		return err
	}
	s.Requests = append(s.Requests, r.Launch)
	return saveEnvironmentState(x.cfg.Attachment.Directory, *s)
}
func (x *Attached) completed(ctx context.Context, s *environmentState, r *attachedRecord, m execMetadata, natural, environmentRunning bool) (harness.Job, error) {
	rec, haveRecord := x.completionEvidence(*r)
	now := time.Now().UTC()
	statusProof := *m.ExitCode == jobrunner.TerminationStatus
	if statusProof && !r.Fenced {
		return x.unknown(*r, "helper termination status without a cancellation fence")
	}
	if r.Termination != nil || statusProof {
		if statusProof && r.Termination == nil {
			next := *r
			next.Termination = &terminationProof{Protocol: jobrunner.Protocol, EffectID: r.Effect.ID, Spec: r.Effect.Spec, OwnerID: r.ExecID, Generation: r.Generation, ConfirmedAt: now}
			if err := x.saveJob(next); err != nil {
				return harness.Job{}, err
			}
			*r = next
		}
		return x.terminatedReceipt(ctx, s, r, rec, haveRecord, now, environmentRunning)
	}
	return x.normalReceipt(ctx, s, r, rec, haveRecord, *m.ExitCode, now, natural, environmentRunning)
}

func (x *Attached) completionEvidence(r attachedRecord) (jobrunner.Record, bool) {
	data, err := spoolEvidence(x.cfg.Attachment.Directory, r.Effect.ID, "completion.json", jobrunner.MetadataLimit)
	if err != nil {
		return jobrunner.Record{}, false
	}
	var rec jobrunner.Record
	if json.Unmarshal(data, &rec) != nil {
		return jobrunner.Record{}, false
	}
	return rec, true
}

func (x *Attached) terminatedReceipt(ctx context.Context, s *environmentState, r *attachedRecord, rec jobrunner.Record, haveRecord bool, now time.Time, environmentRunning bool) (harness.Job, error) {
	if !r.Fenced {
		return x.unknown(*r, "termination evidence without a cancellation fence")
	}
	requested := r.CancelAt
	if requested.IsZero() {
		requested = r.DispatchedAt
	}
	confirmed := now
	if r.Termination != nil && !r.Termination.ConfirmedAt.IsZero() {
		confirmed = r.Termination.ConfirmedAt
	}
	intent, _ := marshal(struct {
		Effect, Exec, Epoch string
		Generation          uint64
		At                  time.Time
	}{r.Effect.ID, r.ExecID, r.Epoch, r.Generation, requested})
	digest := harness.ConfigDigest(intent)
	j := x.baseJob(*r)
	j.Status = harness.JobCancelled
	j.Cessation = &harness.Cessation{AuthorityID: x.cfg.Attachment.AuthorityID, BackendID: x.cfg.Attachment.DaemonID, EnvironmentID: x.cfg.Attachment.ContainerID, StartEpoch: r.Epoch, EffectID: r.Effect.ID, Spec: r.Effect.Spec, OwnerID: r.ExecID, StopIntent: digest, Incarnation: x.cfg.Attachment.Incarnation, Generation: r.Generation, Kind: harness.CessationJobTerminated, RequestedAt: requested, ConfirmedAt: confirmed}
	j.Output.Detail = "job process group terminated; command exit unavailable"
	if haveRecord && validCompletionShape(*r, rec, x.cfg.OutputLimit, x.cfg.PreviewLimit, now) {
		if collected, err := x.collectAttached(*r, j, rec); err == nil {
			j = collected
			preview, previewErr := x.verifiedPreview(*r, true)
			if previewErr != nil {
				j.Preview = unavailablePreview(spoolOutput(r.Effect.ID))
			} else {
				j.Preview = preview
			}
			j.Output.Detail = strings.TrimPrefix(j.Output.Detail+"; command exit unavailable; output captured", "; ")
		} else if !errors.Is(err, errAttachedEvidence) {
			return harness.Job{}, err
		} else {
			j.Output = harness.ArtifactRef{Detail: "command output unavailable: " + err.Error()}
		}
	} else {
		j.Output.Detail += "; command output unavailable"
	}
	if haveRecord && rec.ExitCode != nil {
		j.Output.Detail = strings.TrimPrefix(j.Output.Detail+fmt.Sprintf("; observed exit %d is unverified", *rec.ExitCode), "; ")
	}
	x.epochDetail(&j, *r)
	if r.CancelIntent == "" && environmentRunning {
		j.Output.Detail = strings.TrimPrefix(j.Output.Detail+"; the environment and its services remain running", "; ")
	}
	return x.finishAttachedWith(s, r, j, func(next *attachedRecord) {
		if next.Termination == nil {
			next.Termination = &terminationProof{Protocol: jobrunner.Protocol, EffectID: r.Effect.ID, Spec: r.Effect.Spec, OwnerID: r.ExecID, Generation: r.Generation, ConfirmedAt: now}
		}
	})
}

func (x *Attached) normalReceipt(ctx context.Context, s *environmentState, r *attachedRecord, rec jobrunner.Record, haveRecord bool, execExit int, now time.Time, natural, environmentRunning bool) (harness.Job, error) {
	if execExit == jobrunner.RemappedCommandExit254 {
		r.exitUnverified = true
		return x.unknown(*r, "command exit 253 or 254 is unverified: helper status 253 cannot distinguish the natural 254 remap")
	}
	if !haveRecord || !validNormalCompletion(*r, rec, execExit, x.cfg.OutputLimit, x.cfg.PreviewLimit, now) {
		return x.unknown(*r, "command completion contradicts bound process evidence or capture limits")
	}
	preview, previewErr := x.previewEvidence(r.Effect.ID, rec)
	j := x.baseJob(*r)
	j.ExitCode = rec.ExitCode
	j.StartedAt, j.FinishedAt = rec.StartedAt, rec.FinishedAt
	j.Preview = preview
	if previewErr != nil {
		j.Preview = unavailablePreview(spoolOutput(r.Effect.ID))
	}
	j.Status = harness.JobSucceeded
	if *j.ExitCode != 0 {
		j.Status = harness.JobFailed
	}
	// A natural exit after a fence is a natural result. Only an acknowledged
	// environment stop makes an ordinary exit a cancellation.
	if r.Fenced && r.StopProof != "" {
		j.Status = harness.JobCancelled
		if previewErr == nil {
			j.Preview = cancelledProjection(preview, rec, spoolOutput(r.Effect.ID), x.cfg.PreviewLimit)
		} else {
			j.Preview = partialUnavailablePreview(spoolOutput(r.Effect.ID))
		}
	}
	collected, err := x.collectAttached(*r, j, rec)
	if err != nil {
		if errors.Is(err, errAttachedEvidence) {
			return x.unknown(*r, err.Error())
		}
		return harness.Job{}, err
	}
	j = collected
	x.epochDetail(&j, *r)
	return x.finishAttachedWith(s, r, j, func(next *attachedRecord) {
		if !r.Fenced {
			return
		}
		if j.Status != harness.JobCancelled {
			next.NaturalExit = true
		}
		if !environmentRunning {
			next.StoppedAtExit = true
		}
	})
}

func (x *Attached) epochDetail(j *harness.Job, r attachedRecord) {
	if x.cfg.Attachment.AdmissionEpoch != r.Epoch {
		j.Output.Detail = strings.TrimPrefix(j.Output.Detail+"; environment restarted since the stored attachment observation; inspect background services", "; ")
	}
}

func validNormalCompletion(r attachedRecord, rec jobrunner.Record, execExit int, outputLimit int64, previewLimit int, now time.Time) bool {
	if rec.Terminated || !validCompletionShape(r, rec, outputLimit, previewLimit, now) || rec.ExitCode == nil {
		return false
	}
	switch execExit {
	case jobrunner.TerminationStatus:
		return false
	case jobrunner.RemappedCommandExit:
		return *rec.ExitCode == jobrunner.TerminationStatus
	case jobrunner.RemappedCommandExit254:
		return false
	default:
		return *rec.ExitCode == execExit
	}
}

func validCompletionShape(r attachedRecord, rec jobrunner.Record, outputLimit int64, previewLimit int, now time.Time) bool {
	return rec.Protocol == jobrunner.Protocol && rec.EffectID == r.Effect.ID && rec.Spec == r.Effect.Spec && rec.Generation == r.Generation && !rec.StartedAt.IsZero() && !rec.FinishedAt.Before(rec.StartedAt) && !rec.StartedAt.Before(r.DispatchedAt.Add(-time.Second)) && !rec.FinishedAt.After(now.Add(time.Second)) && rec.Stored >= 0 && rec.Bytes >= rec.Stored && rec.Stored <= outputLimit && len(rec.Preview) <= previewLimit
}

// terminatedWithoutCompletion records a cancelled effect whose process group is
// proven gone but whose capture is missing or invalid. It is not an
// environment cessation and never stops the environment.
func (x *Attached) terminatedWithoutCompletion(ctx context.Context, s *environmentState, r *attachedRecord, environmentRunning bool) (harness.Job, error) {
	now := time.Now().UTC()
	requested := r.CancelAt
	if requested.IsZero() {
		requested = r.DispatchedAt
	}
	confirmed := now
	if r.Termination != nil && !r.Termination.ConfirmedAt.IsZero() {
		confirmed = r.Termination.ConfirmedAt
	}
	intent, _ := marshal(struct {
		Effect, Exec, Epoch string
		Generation          uint64
		At                  time.Time
	}{r.Effect.ID, r.ExecID, r.Epoch, r.Generation, requested})
	digest := harness.ConfigDigest(intent)
	j := x.baseJob(*r)
	j.Status = harness.JobCancelled
	j.Cessation = &harness.Cessation{AuthorityID: x.cfg.Attachment.AuthorityID, BackendID: x.cfg.Attachment.DaemonID, EnvironmentID: x.cfg.Attachment.ContainerID, StartEpoch: r.Epoch, EffectID: r.Effect.ID, Spec: r.Effect.Spec, OwnerID: r.ExecID, StopIntent: digest, Incarnation: x.cfg.Attachment.Incarnation, Generation: r.Generation, Kind: harness.CessationJobTerminated, RequestedAt: requested, ConfirmedAt: confirmed}
	j.Output.Detail = "job process group terminated; command exit and output are unavailable"
	if r.CancelIntent == "" && environmentRunning {
		j.Output.Detail += "; the environment and its services remain running"
	}
	return x.finishAttachedWith(s, r, j, func(next *attachedRecord) {
		if next.Termination == nil {
			next.Termination = &terminationProof{Protocol: jobrunner.Protocol, EffectID: r.Effect.ID, Spec: r.Effect.Spec, OwnerID: r.ExecID, Generation: r.Generation, ConfirmedAt: now}
		}
	})
}

// verifiedPreview renders a preview from the host-published artifact whose
// length and digest collectAttached checked. It never reopens a job-writable
// path after verification.
func (x *Attached) verifiedPreview(r attachedRecord, partial bool) (string, error) {
	root, err := os.OpenRoot(filepath.Join(x.cfg.State, "artifacts", r.Effect.ID))
	if err != nil {
		return "", err
	}
	defer root.Close()
	info, err := root.Stat("output")
	if err != nil {
		return "", err
	}
	if info.Size() == 0 {
		return "", nil
	}
	return jobrunner.PreviewEvidence(root, "output", x.cfg.OutputLimit, x.cfg.PreviewLimit, spoolOutput(r.Effect.ID), x.cfg.OutputLimit, partial)
}

func (x *Attached) collectAttached(r attachedRecord, j harness.Job, rec jobrunner.Record) (harness.Job, error) {
	spool, err := os.OpenRoot(filepath.Join(x.cfg.Attachment.Directory, "spool"))
	if err != nil {
		return j, errors.Join(errAttachedEvidence, err)
	}
	defer spool.Close()
	command, err := spool.OpenRoot(r.Effect.ID)
	if err != nil {
		return j, errors.Join(errAttachedEvidence, err)
	}
	defer command.Close()
	path := filepath.Join(x.cfg.State, "artifacts", r.Effect.ID, "output")
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return j, errors.Join(errAttachedDestination, err)
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return j, errors.Join(errAttachedDestination, err)
	}
	discard := func(cause error) (harness.Job, error) {
		_ = f.Close()
		_ = os.Remove(f.Name())
		if errors.Is(cause, jobrunner.ErrEvidenceDestination) {
			return j, errors.Join(errAttachedDestination, cause)
		}
		return j, errors.Join(errAttachedEvidence, cause)
	}
	if err = f.Chmod(0644); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return j, errors.Join(errAttachedDestination, err)
	}
	h := sha256.New()
	var n int64
	n, err = jobrunner.CopyEvidence(command, "output", x.cfg.OutputLimit, f, h)
	if err != nil {
		return discard(err)
	}
	if n != rec.Stored || hex.EncodeToString(h.Sum(nil)) != rec.Digest {
		return discard(errors.New("docker: command output differs from its completion record"))
	}
	j.Output = harness.ArtifactRef{Path: r.Effect.ID + "/output", Bytes: n, Available: true, Complete: !rec.Incomplete && rec.CaptureError == "" && rec.Bytes == rec.Stored}
	if err := install(f, path, false, nil); err != nil {
		return j, errors.Join(errAttachedDestination, err)
	}
	if err = syncDir(filepath.Dir(filepath.Dir(path))); err != nil {
		return j, errors.Join(errAttachedDestination, err)
	}
	switch {
	case rec.CaptureError != "":
		j.Output.Detail = rec.CaptureError
	case rec.Incomplete:
		j.Output.Detail = "foreground capture ended before inherited output pipes closed; only observed bytes are counted"
	case rec.Bytes > rec.Stored:
		j.Output.Detail = fmt.Sprintf("kept %d of %d observed bytes", rec.Stored, rec.Bytes)
	}
	if j.Status == harness.JobCancelled {
		j.Output.Complete = false
		j.Output.Detail = strings.TrimPrefix(j.Output.Detail+"; job was cancelled; output is partial", "; ")
	}
	return j, nil
}

func (x *Attached) finishAttached(s *environmentState, r *attachedRecord, j harness.Job) (harness.Job, error) {
	return x.finishAttachedWith(s, r, j, nil)
}

// finishAttachedWith saves a copy with its terminal receipt and only updates the
// caller's record and releases the slot after the authoritative write succeeds.
func (x *Attached) finishAttachedWith(s *environmentState, r *attachedRecord, j harness.Job, prepare func(*attachedRecord)) (harness.Job, error) {
	if err := j.Validate(r.Effect); err != nil {
		return harness.Job{}, err
	}
	next := *r
	if prepare != nil {
		prepare(&next)
	}
	next.Terminal = &j
	if err := x.saveJob(next); err != nil {
		return harness.Job{}, errors.Join(errTerminalWrite, err)
	}
	*r = next
	if err := x.releaseSlot(s, *r); err != nil {
		return harness.Job{}, errors.Join(errSlotRelease, err)
	}
	return j, nil
}

func (x *Attached) acknowledgeSample(s *environmentState, r attachedRecord, c preparedContainer) error {
	dir, request, err := x.loadRequest(r.Launch)
	if err != nil {
		return err
	}
	if request.Kind != "exec_start" || request.TargetID != r.ExecID || request.Generation != r.Generation {
		return errors.New("docker: positive exec sample has another launch binding")
	}
	if err = acknowledgeRequest(dir, request, 0, true); err != nil {
		return err
	}
	if err = x.pruneRequests(s); err != nil {
		return err
	}
	if s.Gate == "quarantined" && s.Pending == "" && len(s.Requests) == 0 {
		// Reuse follows exact acknowledgement of every dispatched request, never a
		// stopped sample standing in for an unacknowledged lifecycle operation.
		if c.State.Running {
			s.Gate = "open"
		} else {
			s.Gate = "stopped"
		}
	}
	return saveEnvironmentState(x.cfg.Attachment.Directory, *s)
}
