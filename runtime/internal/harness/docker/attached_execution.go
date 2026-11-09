package docker

import (
	"context"
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

var errAttachedEvidence = errors.New("docker: unsafe or unavailable command capture evidence")

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
	command := []string{a.Helper, "-directory", spoolRoot + "/" + r.Effect.ID, "-effect", r.Effect.ID, "-spec", r.Effect.Spec, "-generation", strconv.FormatUint(r.Generation, 10), "-workdir", x.cfg.Workdir, "-output-limit", strconv.FormatInt(x.cfg.OutputLimit, 10), "-preview-limit", strconv.Itoa(x.cfg.PreviewLimit)}
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
	c, err := x.backend.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if !c.State.Running || c.State.Paused || c.State.Restarting || c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
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
		err = x.backend.api(ctx, "exec-create", http.MethodPost, "/containers/"+a.ContainerID+"/exec", x.execRequest(*r), decodeResponse(&created))
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
	c, err = x.backend.inspectPrepared(ctx, a.ContainerID)
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
	response := 0
	started := x.backend.api(call, "start", http.MethodPost, "/exec/"+r.ExecID+"/start", struct{ Detach, Tty bool }{Detach: true}, func(res *http.Response) error { response = res.StatusCode; return nil })
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
	var m execMetadata
	err := x.backend.api(ctx, "exec-inspect", http.MethodGet, "/exec/"+r.ExecID+"/json", nil, decodeResponse(&m))
	if err != nil {
		return m, err
	}
	if m.ID != r.ExecID || m.ContainerID != x.cfg.Attachment.ContainerID {
		return m, errors.New("docker: exec metadata does not identify the bound command")
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
	if r.Terminal != nil {
		return *r.Terminal, x.releaseSlot(s, *r)
	}
	if r.ExecID == "" {
		j := harness.Job{EffectID: r.Effect.ID, Spec: r.Effect.Spec, Status: harness.JobAbsent}
		return j, nil
	}
	if r.DispatchedAt.IsZero() {
		j := x.baseJob(*r)
		j.Status = harness.JobPending
		return j, j.Validate(r.Effect)
	}
	if err := x.ensureLaunchReference(s, *r); err != nil {
		return harness.Job{}, err
	}
	c, err := x.backend.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil {
		return harness.Job{}, err
	}
	if c.State.StartedAt.Format(time.RFC3339Nano) != r.Epoch {
		return x.unknown(*r, "environment start epoch changed; stop or inspect the old effect without replay")
	}
	m, err := x.inspectExec(ctx, *r)
	if err != nil {
		if status(err) == http.StatusNotFound {
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
	return x.completed(ctx, s, r, m)
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
func (x *Attached) completed(ctx context.Context, s *environmentState, r *attachedRecord, m execMetadata) (harness.Job, error) {
	data, err := spoolEvidence(x.cfg.Attachment.Directory, r.Effect.ID, "completion.json", jobrunner.MetadataLimit)
	if err != nil {
		return x.unknown(*r, "command completion evidence is unavailable or unsafe: "+err.Error())
	}
	var rec jobrunner.Record
	if err = json.Unmarshal(data, &rec); err != nil {
		return x.unknown(*r, "invalid command completion evidence")
	}
	now := time.Now().UTC()
	if rec.Protocol != jobrunner.Protocol || rec.EffectID != r.Effect.ID || rec.Spec != r.Effect.Spec || rec.Generation != r.Generation || rec.ExitCode == nil || *rec.ExitCode != *m.ExitCode || rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt) || rec.StartedAt.Before(r.DispatchedAt.Add(-time.Second)) || rec.FinishedAt.After(now.Add(time.Second)) || rec.Stored < 0 || rec.Bytes < rec.Stored || rec.Stored > x.cfg.OutputLimit || len(rec.Preview) > x.cfg.PreviewLimit {
		return x.unknown(*r, "command completion contradicts bound process evidence or capture limits")
	}
	j := x.baseJob(*r)
	j.ExitCode = rec.ExitCode
	j.StartedAt, j.FinishedAt = rec.StartedAt, rec.FinishedAt
	j.Preview = rec.Preview
	j.Status = harness.JobSucceeded
	if *j.ExitCode != 0 {
		j.Status = harness.JobFailed
	}
	if r.Fenced {
		j.Status = harness.JobCancelled
	}
	j, err = x.collectAttached(*r, j, rec)
	if err != nil {
		if errors.Is(err, errAttachedEvidence) {
			return x.unknown(*r, err.Error())
		}
		return harness.Job{}, err
	}
	if x.cfg.Attachment.AdmissionEpoch != r.Epoch {
		j.Output.Detail = strings.TrimPrefix(j.Output.Detail+"; environment restarted since the stored attachment observation; inspect background services", "; ")
	}
	return x.finishAttached(s, r, j)
}
func (x *Attached) collectAttached(r attachedRecord, j harness.Job, rec jobrunner.Record) (harness.Job, error) {
	data, err := spoolEvidence(x.cfg.Attachment.Directory, r.Effect.ID, "output", x.cfg.OutputLimit)
	if err != nil {
		return j, errors.Join(errAttachedEvidence, err)
	}
	if int64(len(data)) != rec.Stored || harness.ConfigDigest(data) != rec.Digest {
		return j, errors.Join(errAttachedEvidence, errors.New("docker: command output differs from its completion record"))
	}
	path := filepath.Join(x.cfg.State, "artifacts", r.Effect.ID, "output")
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return j, err
	}
	if err = publish(path, data, false); err != nil {
		return j, err
	}
	if err = syncDir(filepath.Dir(filepath.Dir(path))); err != nil {
		return j, err
	}
	j.Output = harness.ArtifactRef{Path: r.Effect.ID + "/output", Bytes: int64(len(data)), Available: true, Complete: !rec.Incomplete && rec.CaptureError == "" && rec.Bytes == rec.Stored}
	switch {
	case rec.CaptureError != "":
		j.Output.Detail = rec.CaptureError
	case rec.Incomplete:
		j.Output.Detail = "foreground capture ended before inherited output pipes closed; only observed bytes are counted"
	case rec.Bytes > rec.Stored:
		j.Output.Detail = fmt.Sprintf("kept %d of %d observed bytes", rec.Stored, rec.Bytes)
	}
	return j, nil
}
func (x *Attached) finishAttached(s *environmentState, r *attachedRecord, j harness.Job) (harness.Job, error) {
	if err := j.Validate(r.Effect); err != nil {
		return harness.Job{}, err
	}
	r.Terminal = &j
	if err := x.saveJob(*r); err != nil {
		return harness.Job{}, err
	}
	if err := x.releaseSlot(s, *r); err != nil {
		return harness.Job{}, err
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
