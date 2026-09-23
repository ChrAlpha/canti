package harness

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/core"
)

const maxBackoff = 30 * time.Second

// LoopConfig wires a Loop to its environment. Zero values take the defaults
// noted on each field.
type LoopConfig struct {
	Environment    string           // Frame.Environment
	ArtifactDir    string           // Frame.ArtifactDir, as the agent sees it
	Workspace      string           // host workspace directory ReadNotes resolves in
	NotesFile      string           // notes file relative to Workspace, such as ".canti/notes.md"
	NotesPath      string           // the notes file as the agent sees it, such as "/workspace/.canti/notes.md"
	History        int              // recent steps per prompt; 8
	Limits         PromptLimits     // DefaultPromptLimits()
	Poll           time.Duration    // interval between inspections; 500ms
	Backoff        time.Duration    // wait after an incomplete response, doubling per consumed attempt up to 30s; 1s, none if negative
	InspectRetries int              // consecutive Inspect errors tolerated before OutcomeUnknown; 5
	Now            func() time.Time // clock of the wall-time and job budgets; time.Now
	Logger         *slog.Logger     // discards
}

// Loop drives the committed intents of runs in a Journal until they halt.
// It keeps no run state between calls: every Step reloads the latest
// snapshot and performs the one I/O its intent authorizes, so a fresh Loop
// on the same journal continues any run.
type Loop struct {
	journal  Journal
	executor Executor
	model    Model
	runner   Runner
	cfg      LoopConfig
}

// NewLoop validates cfg, applies its defaults, and builds a Loop that
// commits every transition through a Runner on j and x.
func NewLoop(j Journal, x Executor, m Model, cfg LoopConfig) (*Loop, error) {
	if j == nil || m == nil {
		return nil, errors.New("harness: loop journal and model are required")
	}
	for _, v := range []string{cfg.Environment, cfg.ArtifactDir, cfg.Workspace, cfg.NotesFile, cfg.NotesPath} {
		if strings.TrimSpace(v) == "" {
			return nil, errors.New("harness: loop environment, artifact directory, workspace, and notes paths are required")
		}
	}
	if cfg.History < 0 || cfg.Poll < 0 || cfg.InspectRetries < 0 {
		return nil, errors.New("harness: loop history, poll, and inspect retries must not be negative")
	}
	cfg.History = cmp.Or(cfg.History, 8)
	if cfg.Limits == (PromptLimits{}) {
		cfg.Limits = DefaultPromptLimits()
	}
	cfg.Poll = cmp.Or(cfg.Poll, 500*time.Millisecond)
	cfg.Backoff = cmp.Or(cfg.Backoff, time.Second)
	cfg.InspectRetries = cmp.Or(cfg.InspectRetries, 5)
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	r, err := NewRunner(logged{j, cfg.Logger}, x)
	if err != nil {
		return nil, err
	}
	return &Loop{journal: j, executor: x, model: m, runner: r, cfg: cfg}, nil
}

// Run steps the run until its intent is halt or a step fails.
func (l *Loop) Run(ctx context.Context, runID string) (Snapshot, error) {
	for {
		snap, err := l.Step(ctx, runID)
		if err != nil || snap.State.Intent() == core.IntentHalt {
			return snap, err
		}
	}
}

// Step performs the one intent of the run's latest snapshot and returns the
// snapshot it reached; a halted run is returned unchanged. Unless the run
// already records a stop reason, a spent wall-time, token, or output budget
// is committed as a stop instead. On an error the snapshot is the last one
// committed. Once ctx is done nothing more is committed, and a later Step
// resumes from the journal.
func (l *Loop) Step(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.load(ctx, runID)
	if err != nil {
		return snap, err
	}
	run, err := l.journal.Run(ctx, runID)
	if err != nil || snap.State.Intent() == core.IntentHalt {
		return snap, err
	}
	if snap.State.Reason == "" {
		reason, why, err := l.spent(ctx, run)
		if err != nil {
			return snap, err
		}
		if reason != "" {
			return l.runner.Commit(ctx, snap, core.StopRequested{Reason: reason}, Facts{Failure: why})
		}
	}
	switch snap.State.Intent() {
	case core.IntentPrepareRequest:
		return l.prepare(ctx, run, snap)
	case core.IntentSendRequest:
		return l.send(ctx, snap)
	case core.IntentDecode:
		return l.decode(ctx, snap)
	case core.IntentBind:
		return l.runner.Bind(ctx, snap)
	case core.IntentStart:
		return l.runner.Start(ctx, snap)
	case core.IntentInspect:
		return l.watch(ctx, run, snap)
	case core.IntentCancel:
		return l.cancel(ctx, run, snap)
	}
	return snap, nil
}

// Stop records a persistent stop for reason, unless the run is closed or
// already records a reason, and then runs it until it halts, so a possible
// effect is cancelled and its true result recorded. ctx should carry the
// cleanup deadline.
func (l *Loop) Stop(ctx context.Context, runID string, reason core.StopReason) (Snapshot, error) {
	snap, err := l.load(ctx, runID)
	if err != nil {
		return snap, err
	}
	if snap.State.Phase != core.PhaseReturned && snap.State.Reason == "" {
		if snap, err = l.runner.Commit(ctx, snap, core.StopRequested{Reason: reason}, Facts{}); err != nil {
			return snap, err
		}
	}
	return l.Run(ctx, runID)
}

// ResolveJob inspects the effect of a run in needs_resolution and records
// the job found for it with JobBound, so the run resumes behind the start
// boundary. It fails when no job is found.
func (l *Loop) ResolveJob(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.unresolved(ctx, runID)
	if err != nil {
		return snap, err
	}
	job, found, err := l.executor.Inspect(ctx, snap.State.EffectID)
	if err == nil && !found {
		err = fmt.Errorf("harness: no job owns effect %s", snap.State.EffectID)
	}
	if err != nil {
		return snap, err
	}
	return l.runner.Commit(ctx, snap, core.JobBound{EffectID: snap.State.EffectID, JobID: job.ID}, Facts{})
}

// Resolve records an explicit outcome for the effect of a run in
// needs_resolution. Resolving to core.ExecutionUnknown abandons the run.
func (l *Loop) Resolve(ctx context.Context, runID string, obs Observation) (Snapshot, error) {
	snap, err := l.unresolved(ctx, runID)
	if err != nil {
		return snap, err
	}
	return l.runner.Commit(ctx, snap, core.Resolved{EffectID: snap.State.EffectID, Execution: obs.Execution}, Facts{Observation: &obs})
}

func (l *Loop) load(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.journal.Latest(ctx, runID)
	if err == nil {
		err = snap.Validate()
	}
	return snap, err
}

func (l *Loop) unresolved(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.load(ctx, runID)
	if err == nil && snap.State.Phase != core.PhaseNeedsResolution {
		err = fmt.Errorf("harness: run %q in phase %q has no unknown outcome to resolve", runID, snap.State.Phase)
	}
	return snap, err
}

func (l *Loop) spent(ctx context.Context, run Run) (core.StopReason, string, error) {
	if why := l.late(run); why != "" {
		return core.StopTimedOut, why, nil
	}
	b := run.Budget
	if b.Tokens <= 0 && b.OutputBytes <= 0 {
		return "", "", nil
	}
	t, err := l.journal.Totals(ctx, run.ID)
	if err != nil {
		return "", "", err
	}
	switch tokens := t.Usage.InputTokens + t.Usage.OutputTokens; {
	case b.Tokens > 0 && tokens >= b.Tokens:
		return core.StopBudgetExhausted, fmt.Sprintf("%d tokens reached the %d-token budget", tokens, b.Tokens), nil
	case b.OutputBytes > 0 && t.OutputBytes >= b.OutputBytes:
		return core.StopBudgetExhausted, fmt.Sprintf("%d output bytes reached the %d-byte budget", t.OutputBytes, b.OutputBytes), nil
	}
	return "", "", nil
}

func (l *Loop) late(run Run) string {
	if w := run.Budget.WallTime; w > 0 {
		if d := l.cfg.Now().Sub(run.Created); d >= w {
			return fmt.Sprintf("run time %v reached the %v wall-time budget", d, w)
		}
	}
	return ""
}

func (l *Loop) prepare(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	s := snap.State
	recent, err := l.journal.Recent(ctx, s.RunID, l.cfg.History)
	if err != nil {
		return snap, err
	}
	f := Frame{
		Goal: run.Goal, Constraints: run.Constraints, Environment: l.cfg.Environment,
		Step: s.Step, MaxSteps: s.Limits.MaxSteps, Attempt: s.Attempt, MaxAttempts: s.Limits.MaxAttempts,
		Earlier: s.Step - 1 - len(recent), Steps: recent, ArtifactDir: l.cfg.ArtifactDir, Notes: Notes{Path: l.cfg.NotesPath},
	}
	if f.Notes.Text, f.Notes.Truncated, err = ReadNotes(l.cfg.Workspace, l.cfg.NotesFile, l.cfg.Limits.Notes); err != nil {
		l.cfg.Logger.WarnContext(ctx, "notes unreadable", "run_id", s.RunID, "seq", snap.Seq, "error", err)
		f.Notes.Text, f.Notes.Truncated = "notes unreadable: "+err.Error(), false
	}
	if s.RequestRev > 0 {
		resp, err := l.journal.Response(ctx, s.RunID, s.Step, s.Attempt-1)
		if err != nil {
			return snap, err
		}
		body := l.model.Decision(resp)
		if _, err := core.Decode(body); err != nil {
			f.Rejected = &Rejection{Error: err.Error(), Response: string(body)}
		}
	}
	p, err := f.Prompt(l.cfg.Limits)
	var req Request
	if err == nil {
		req, err = l.model.Freeze(p)
	}
	if err != nil {
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopUnrecoverable}, Facts{Failure: err.Error()})
	}
	return l.runner.Commit(ctx, snap, core.RequestFrozen{}, Facts{Request: &req})
}

func (l *Loop) send(ctx context.Context, snap Snapshot) (Snapshot, error) {
	s := snap.State
	req, err := l.journal.Request(ctx, s.RunID, s.Step, s.RequestRev)
	if err != nil {
		return snap, err
	}
	attempt := s.Attempt
	if s.Phase == core.PhaseRequestSent {
		l.cfg.Logger.WarnContext(ctx, "counting an interrupted attempt as lost", "run_id", s.RunID, "seq", snap.Seq, "step", s.Step, "attempt", attempt)
		attempt++
	}
	if snap, err = l.runner.Commit(ctx, snap, core.AttemptStarted{Step: s.Step, Attempt: attempt}, Facts{}); err != nil || snap.State.Phase != core.PhaseRequestSent {
		return snap, err
	}
	resp, err := l.model.Complete(ctx, req)
	var stop *StopError
	switch {
	case err == nil:
		return l.runner.Commit(ctx, snap, core.ResponseSaved{Step: s.Step, Attempt: attempt}, Facts{Response: &resp})
	case errors.As(err, &stop):
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: stop.Reason}, Facts{Failure: err.Error()})
	}
	facts := Facts{Failure: err.Error()}
	if len(resp.Body) > 0 {
		resp.Complete = false
		facts.Response = &resp
	}
	if snap, err = l.runner.Commit(ctx, snap, core.ResponseIncomplete{Step: s.Step, Attempt: attempt}, facts); err != nil || snap.State.Phase != core.PhaseRequestFrozen {
		return snap, err
	}
	d := l.backoff(snap.State.Attempt)
	l.cfg.Logger.InfoContext(ctx, "retrying an incomplete response", "run_id", s.RunID, "seq", snap.Seq, "attempt", snap.State.Attempt, "backoff", d)
	return snap, sleep(ctx, d)
}

func (l *Loop) backoff(attempt int) time.Duration {
	d := l.cfg.Backoff
	for i := 2; i < attempt && d > 0 && d < maxBackoff; i++ {
		d *= 2
	}
	return min(d, maxBackoff)
}

func (l *Loop) decode(ctx context.Context, snap Snapshot) (Snapshot, error) {
	s := snap.State
	resp, err := l.journal.Response(ctx, s.RunID, s.Step, s.Attempt)
	if err != nil {
		return snap, err
	}
	next, err := l.runner.Commit(ctx, snap, core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: l.model.Decision(resp)}, Facts{})
	if err == nil && next.State.Phase == core.PhaseIdle {
		l.cfg.Logger.WarnContext(ctx, "retrying an undecodable response", "run_id", s.RunID, "seq", next.Seq, "attempt", next.State.Attempt)
	}
	return next, err
}

// cancel fences an effect without a recorded job and records what the
// lookup finds; a recorded job is cancelled and watched to its result.
func (l *Loop) cancel(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	s := snap.State
	if err := l.executor.Cancel(ctx, s.EffectID); err != nil {
		return snap, err
	}
	if s.JobID != "" {
		return l.watch(ctx, run, snap)
	}
	job, found, err := l.executor.Inspect(ctx, s.EffectID)
	switch {
	case ctx.Err() != nil:
		return snap, ctx.Err()
	case errors.Is(err, ErrJobLost):
		return l.unknown(ctx, snap, err.Error())
	case err != nil:
		return snap, err
	case found:
		return l.runner.Commit(ctx, snap, core.JobBound{EffectID: s.EffectID, JobID: job.ID}, Facts{})
	}
	return l.observe(ctx, snap, Job{Output: ArtifactRef{Detail: "no job was bound to the effect"}}, core.ExecutionCancelled)
}

// watch polls the recorded job until it reports a result. While the run is
// not stopping it cancels a job past its timeout and stops a run past its
// wall time.
func (l *Loop) watch(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	s, limit := snap.State, run.Budget.JobTimeout
	stopping := s.Phase == core.PhaseCancelling
	timedOut, failures := false, 0
	for {
		job, found, err := l.executor.Inspect(ctx, s.EffectID)
		if ctx.Err() != nil {
			return snap, ctx.Err()
		}
		why := ""
		switch {
		case errors.Is(err, ErrJobLost):
			why = err.Error()
		case err != nil:
			if failures++; failures > l.cfg.InspectRetries {
				why = fmt.Sprintf("inspect failed %d consecutive times: %v", failures, err)
			} else {
				l.cfg.Logger.WarnContext(ctx, "retrying a failed inspection", "run_id", s.RunID, "seq", snap.Seq, "failures", failures, "error", err)
			}
		case !found:
			why = fmt.Sprintf("no job owns effect %s", s.EffectID)
		case job.ID != s.JobID:
			why = fmt.Sprintf("effect %s is owned by job %q, not the recorded job %q", s.EffectID, job.ID, s.JobID)
		case job.Status == JobSucceeded || job.Status == JobFailed:
			if job.ExitCode != nil {
				return l.observe(ctx, snap, job, core.ExecutionExited)
			}
			why = fmt.Sprintf("job %s %s without an exit code", job.ID, job.Status)
		case job.Status == JobCancelled:
			ran := limit > 0 && !job.StartedAt.IsZero() && !job.FinishedAt.IsZero() && job.FinishedAt.Sub(job.StartedAt) >= limit
			if !stopping && (timedOut || ran) {
				return l.observe(ctx, snap, job, core.ExecutionTimedOut)
			}
			return l.observe(ctx, snap, job, core.ExecutionCancelled)
		case job.Status != JobRunning:
			why = fmt.Sprintf("job %s reports %s", job.ID, job.Status)
		case !stopping:
			failures = 0
			if !timedOut && limit > 0 && !job.StartedAt.IsZero() && l.cfg.Now().Sub(job.StartedAt) >= limit {
				l.cfg.Logger.InfoContext(ctx, "cancelling a job past its timeout", "run_id", s.RunID, "seq", snap.Seq, "job_id", job.ID, "timeout", limit)
				if err := l.executor.Cancel(ctx, s.EffectID); err != nil {
					return snap, err
				}
				timedOut = true
			}
			if late := l.late(run); late != "" {
				return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: late})
			}
		default:
			failures = 0
		}
		if why != "" {
			return l.unknown(ctx, snap, why)
		}
		if err := sleep(ctx, l.cfg.Poll); err != nil {
			return snap, err
		}
	}
}

func (l *Loop) observe(ctx context.Context, snap Snapshot, job Job, execution core.ExecutionState) (Snapshot, error) {
	obs := Observation{Execution: execution, ExitCode: job.ExitCode, Preview: job.Preview, Output: job.Output}
	return l.runner.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: execution}, Facts{Observation: &obs})
}

func (l *Loop) unknown(ctx context.Context, snap Snapshot, why string) (Snapshot, error) {
	return l.runner.Commit(ctx, snap, core.OutcomeUnknown{EffectID: snap.State.EffectID}, Facts{Failure: why})
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// logged records every commit attempt of the runner.
type logged struct {
	Store
	log *slog.Logger
}

func (s logged) Commit(ctx context.Context, t Transition) error {
	err := s.Store.Commit(ctx, t)
	attrs := []any{"run_id", t.From.RunID, "seq", t.Seq, "phase", t.To.Phase, "event", t.Event.Kind()}
	switch {
	case err != nil:
		s.log.WarnContext(ctx, "commit failed", append(attrs, "error", err)...)
	case t.Facts.Failure != "":
		s.log.WarnContext(ctx, "committed", append(attrs, "failure", t.Facts.Failure)...)
	default:
		s.log.InfoContext(ctx, "committed", attrs...)
	}
	return err
}
