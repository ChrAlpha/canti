package harness

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

const maxBackoff = 30 * time.Second

const historyPage = 64

// LoopConfig wires a Loop to its environment. Zero values take the defaults
// noted on each field.
type LoopConfig struct {
	Environment    string           // Frame.Environment
	History        string           // host directory PublishHistory publishes the step records of every run under, which jobs see as archive.RunsRoot
	Limits         PromptLimits     // DefaultPromptLimits(); Total bounds one complete context page
	Poll           time.Duration    // interval between inspections; 500ms
	Backoff        time.Duration    // wait after an incomplete response, doubling per consumed attempt up to 30s; 1s, none if negative
	InspectRetries int              // consecutive Inspect errors tolerated before OutcomeUnknown; 5
	Now            func() time.Time // clock of the wall-time and job budgets; time.Now
	Logger         *slog.Logger     // discards
	Reader         *archive.Reader
	DecisionTool   bool // Frame.DecisionTool
	WindowSteps    int
	WindowBytes    int
	HandoverBytes  int
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
	if strings.TrimSpace(cfg.History) == "" {
		return nil, errPaths
	}
	cfg, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	r, err := NewRunner(logged{j, cfg.Logger}, x)
	if err != nil {
		return nil, err
	}
	return &Loop{journal: j, executor: x, model: m, runner: r, cfg: cfg}, nil
}

var errPaths = errors.New("harness: loop environment and history path are required")

// normalize validates cfg, apart from its history directory, and applies its
// defaults, for a loop and for an accounting of the context that it renders.
func normalize(cfg LoopConfig) (LoopConfig, error) {
	if strings.TrimSpace(cfg.Environment) == "" {
		return cfg, errPaths
	}
	if cfg.Poll < 0 || cfg.InspectRetries < 0 {
		return cfg, errors.New("harness: loop poll and inspect retries must not be negative")
	}
	if cfg.WindowSteps < 0 || cfg.WindowBytes < 0 || cfg.HandoverBytes < 0 {
		return cfg, errors.New("harness: context window limits must not be negative")
	}
	if cfg.HandoverBytes == 0 {
		cfg.HandoverBytes = 2048
	}
	if cfg.HandoverBytes > 8192 {
		return cfg, errors.New("harness: handover exceeds 8192 bytes")
	}
	if cfg.Limits == (PromptLimits{}) {
		cfg.Limits = DefaultPromptLimits()
	}
	if p := cfg.Limits; min(p.Goal, p.Constraints, p.Environment, p.Command, p.Preview, p.Rejected, p.Total, p.Purpose) <= 0 {
		return cfg, fmt.Errorf("harness: prompt limits %+v must be positive", p)
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
	if cfg.Reader != nil {
		r := *cfg.Reader
		r.Excluded = []string{RetrievePrefix, handoverName, DiscoveryMarker}
		cfg.Reader = &r
	}
	return cfg, nil
}

// Run steps the run until its intent is halt or a step fails. At a halt it
// publishes the run's step records, only logging a failure, which a later
// publication repairs.
func (l *Loop) Run(ctx context.Context, runID string) (Snapshot, error) {
	for {
		snap, err := l.Step(ctx, runID)
		if err != nil {
			return snap, err
		}
		if snap.State.Intent() == core.IntentHalt {
			if err := PublishHistory(ctx, l.journal, l.cfg.History, runID); err != nil {
				l.note(ctx, slog.LevelWarn, "step records not published", snap, "error", err)
			}
			return snap, nil
		}
	}
}

// Step performs the one intent of the run's latest snapshot and returns the
// snapshot it reached; a halted run is returned unchanged. Unless the run
// already records a stop reason or only has a saved response to decode, a
// spent wall-time, token, or output budget is committed as a stop instead. On an error the snapshot is the last one
// committed. Once ctx is done nothing more is committed, and a later Step
// resumes from the journal.
func (l *Loop) Step(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.load(ctx, runID)
	if err != nil || snap.State.Intent() == core.IntentHalt {
		return snap, err
	}
	run, err := l.journal.Run(ctx, runID)
	if err != nil {
		return snap, err
	}
	if snap.State.Reason == "" && snap.State.Intent() != core.IntentDecode {
		reason, why, err := l.spent(ctx, run)
		if err != nil {
			return snap, err
		}
		if reason != "" {
			return l.runner.Commit(ctx, snap, core.StopRequested{Reason: reason}, Facts{Failure: why})
		}
	}
	switch r, err := l.route(ctx, snap.State); {
	case err != nil:
		return snap, err
	case r == refused:
		return l.refuseWork(ctx, snap)
	case r == inProcess:
		if next, handled, err := l.builtin(ctx, snap); handled {
			return next, err
		}
	}
	switch snap.State.Intent() {
	case core.IntentPrepareRequest:
		return l.prepare(ctx, run, snap)
	case core.IntentSendRequest:
		return l.send(ctx, run, snap)
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

// ErrJobUnknown reports that the job of an unresolved effect inspects to an
// unknown outcome, which recording the job again would only reach again.
var ErrJobUnknown = errors.New("harness: the job reports an unknown outcome")

// ResolveJob inspects the effect of a run in needs_resolution and records
// the job found for it with JobBound, so the run resumes behind the start
// boundary. It fails when no job is found, and with ErrJobUnknown when the
// job reports an unknown outcome, which it takes for a stable report, as the
// docker executor's are, that only Resolve settles; neither commits anything.
func (l *Loop) ResolveJob(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.unresolved(ctx, runID)
	if err != nil {
		return snap, err
	}
	job, found, err := l.executor.Inspect(ctx, snap.State.EffectID)
	switch {
	case err != nil:
	case !found:
		err = fmt.Errorf("harness: no job owns effect %s", snap.State.EffectID)
	case job.Status == JobUnknown:
		detail := ""
		if job.Output.Detail != "" {
			detail = ": " + job.Output.Detail
		}
		err = fmt.Errorf("%w: job %s of effect %s%s", ErrJobUnknown, job.ID, snap.State.EffectID, detail)
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

// windowSteps reads the active page only, bounded by the persisted first step
// and the snapshot sequence. An older page never re-enters the prompt.
func (l *Loop) windowSteps(ctx context.Context, runID string, f Frame, snapshotSeq int64) ([]StepRecord, error) {
	var steps []StepRecord
	n := f.Step - f.Window.FirstStep
	for before := snapshotSeq + 1; len(steps) < n; {
		limit := min(historyPage, n-len(steps))
		page, err := l.journal.Recent(ctx, runID, before, limit)
		if err != nil {
			return nil, err
		}
		for i := len(page) - 1; i >= 0; i-- {
			record := page[i]
			if record.Step >= f.Window.FirstStep && record.Step < f.Step {
				steps = append(steps, record)
			}
		}
		if len(page) < limit || len(page) == 0 || page[0].Step <= f.Window.FirstStep {
			break
		}
		before = page[0].Seq
	}
	slices.Reverse(steps)
	return steps, nil
}

func (l *Loop) send(ctx context.Context, run Run, snap Snapshot) (Snapshot, error) {
	s := snap.State
	req, err := l.journal.Request(ctx, s.RunID, s.Step, s.RequestRev)
	if err != nil {
		return snap, err
	}
	attempt := s.Attempt
	if s.Phase == core.PhaseRequestSent {
		l.note(ctx, slog.LevelWarn, "counting an interrupted attempt as lost", snap, "attempt", attempt)
		attempt++
	}
	if snap, err = l.runner.Commit(ctx, snap, core.AttemptStarted{Step: s.Step, Attempt: attempt}, Facts{}); err != nil || snap.State.Phase != core.PhaseRequestSent {
		return snap, err
	}
	call, cancel := ctx, context.CancelFunc(func() {})
	if w := run.Budget.WallTime; w > 0 {
		call, cancel = context.WithTimeout(ctx, run.Created.Add(w).Sub(l.cfg.Now()))
	}
	defer cancel()
	resp, err := l.model.Complete(call, req)
	if err != nil && ctx.Err() == nil && call.Err() != nil {
		why := fmt.Sprintf("the %v wall-time budget ran out during the model request", run.Budget.WallTime)
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: why})
	}
	if err == nil && (!resp.Complete || len(resp.Body) == 0) {
		err = errors.New("harness: model reported success without a complete response")
	}
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
	l.note(ctx, slog.LevelInfo, "retrying an incomplete response", snap, "attempt", snap.State.Attempt, "backoff", d)
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
	next, err := l.runner.Commit(ctx, snap, core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: l.decision(resp)}, Facts{})
	if err == nil && next.State.Phase == core.PhaseIdle {
		l.note(ctx, slog.LevelWarn, "retrying an undecodable response", next, "attempt", next.State.Attempt)
	}
	return next, err
}

// decision is the model's decision text of resp, or its raw body when the
// model extracts none, so that an empty extraction is rejected by decoding.
func (l *Loop) decision(resp Response) []byte {
	if b := l.model.Decision(resp); len(b) > 0 {
		return b
	}
	return resp.Body
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
	w := watcher{Loop: l, run: run, snap: snap, stopping: snap.State.Phase == core.PhaseCancelling}
	for {
		job, found, err := l.executor.Inspect(ctx, snap.State.EffectID)
		if ctx.Err() != nil {
			return snap, ctx.Err()
		}
		if next, done, err := w.inspected(ctx, job, found, err); done {
			return next, err
		}
		if err := w.await(ctx, job, err == nil && found); err != nil {
			return snap, err
		}
	}
}

// maxWait bounds one wait, so a backend that never answers delays an
// observation by at most this long.
const maxWait = 30 * time.Second

// await waits for a running job to stop running, within the job's timeout
// and the run's wall time, when the executor can wait. After an inspection
// that failed, a failed wait, or a wait that returned early while the job
// kept running, it sleeps for the poll interval instead.
func (w *watcher) await(ctx context.Context, job Job, inspected bool) error {
	waiter, ok := w.executor.(Waiter)
	if !ok || !inspected || job.Status != JobRunning || w.early {
		w.early = false
		return sleep(ctx, w.cfg.Poll)
	}
	limit := maxWait
	if !w.stopping {
		if t := w.run.Budget.JobTimeout; t > 0 && !w.timedOut && !job.StartedAt.IsZero() {
			limit = min(limit, t-w.cfg.Now().Sub(job.StartedAt))
		}
		if t := w.run.Budget.WallTime; t > 0 {
			limit = min(limit, t-w.cfg.Now().Sub(w.run.Created))
		}
	}
	began := time.Now()
	wctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	err := waiter.Wait(wctx, w.snap.State.EffectID)
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case err != nil && wctx.Err() == nil:
		w.note(ctx, slog.LevelWarn, "waiting for the job failed", w.snap, "error", err)
		return sleep(ctx, w.cfg.Poll)
	}
	w.early = time.Since(began) < w.cfg.Poll
	return nil
}

// watcher follows the job of one awaited or cancelling effect across
// inspections: whether it was cancelled for its timeout and how many
// inspections failed in a row.
type watcher struct {
	*Loop
	run      Run
	snap     Snapshot
	stopping bool
	timedOut bool
	early    bool
	failures int
}

// inspected acts on one inspection of the job and reports whether the
// watch ends, with the snapshot it committed.
func (w *watcher) inspected(ctx context.Context, job Job, found bool, err error) (Snapshot, bool, error) {
	s := w.snap.State
	why := ""
	switch {
	case errors.Is(err, ErrJobLost):
		why = err.Error()
	case err != nil:
		if !w.stopping {
			if late := w.late(w.run); late != "" {
				next, err := w.runner.Commit(ctx, w.snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: late + "; " + err.Error()})
				return next, true, err
			}
		}
		if w.failures++; w.failures > w.cfg.InspectRetries {
			why = fmt.Sprintf("inspect failed %d consecutive times: %v", w.failures, err)
		} else {
			w.note(ctx, slog.LevelWarn, "retrying a failed inspection", w.snap, "failures", w.failures, "error", err)
		}
	case !found:
		why = fmt.Sprintf("no job owns effect %s", s.EffectID)
	case job.ID != s.JobID:
		why = fmt.Sprintf("effect %s is owned by job %q, not the recorded job %q", s.EffectID, job.ID, s.JobID)
	case job.Status == JobRunning:
		return w.running(ctx, job)
	default:
		return w.finished(ctx, job)
	}
	if why != "" {
		next, err := w.unknown(ctx, w.snap, why)
		return next, true, err
	}
	return w.snap, false, nil
}

// running enforces the job timeout and the wall time on a running job.
func (w *watcher) running(ctx context.Context, job Job) (Snapshot, bool, error) {
	w.failures = 0
	if w.stopping {
		return w.snap, false, nil
	}
	if limit := w.run.Budget.JobTimeout; !w.timedOut && limit > 0 && !job.StartedAt.IsZero() && w.cfg.Now().Sub(job.StartedAt) >= limit {
		w.note(ctx, slog.LevelInfo, "cancelling a job past its timeout", w.snap, "job_id", job.ID, "timeout", limit)
		if err := w.executor.Cancel(ctx, w.snap.State.EffectID); err != nil {
			return w.snap, true, err
		}
		w.timedOut = true
	}
	if late := w.late(w.run); late != "" {
		next, err := w.runner.Commit(ctx, w.snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: late})
		return next, true, err
	}
	return w.snap, false, nil
}

// finished observes a job that is no longer running, or records an
// unknown outcome for a status it cannot interpret.
func (w *watcher) finished(ctx context.Context, job Job) (Snapshot, bool, error) {
	var next Snapshot
	var err error
	switch job.Status {
	case JobSucceeded, JobFailed:
		if job.ExitCode == nil {
			next, err = w.unknown(ctx, w.snap, fmt.Sprintf("job %s %s without an exit code", job.ID, job.Status))
			return next, true, err
		}
		next, err = w.observe(ctx, w.snap, job, core.ExecutionExited)
	case JobCancelled:
		limit := w.run.Budget.JobTimeout
		ran := limit > 0 && !job.StartedAt.IsZero() && !job.FinishedAt.IsZero() && job.FinishedAt.Sub(job.StartedAt) >= limit
		execution := core.ExecutionCancelled
		if !w.stopping && (w.timedOut || ran) {
			execution = core.ExecutionTimedOut
		}
		next, err = w.observe(ctx, w.snap, job, execution)
	case JobFailedToStart:
		next, err = w.observe(ctx, w.snap, job, core.ExecutionFailedToStart)
	default:
		why := fmt.Sprintf("job %s reports %s", job.ID, job.Status)
		if job.Output.Detail != "" {
			why += ": " + job.Output.Detail
		}
		next, err = w.unknown(ctx, w.snap, why)
	}
	return next, true, err
}

func (l *Loop) observe(ctx context.Context, snap Snapshot, job Job, execution core.ExecutionState) (Snapshot, error) {
	obs := Observation{Execution: execution, ExitCode: job.ExitCode, Preview: job.Preview, Output: job.Output}
	return l.runner.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: execution}, Facts{Observation: &obs})
}

func (l *Loop) unknown(ctx context.Context, snap Snapshot, why string) (Snapshot, error) {
	return l.runner.Commit(ctx, snap, core.OutcomeUnknown{EffectID: snap.State.EffectID}, Facts{Failure: why})
}

func (l *Loop) note(ctx context.Context, level slog.Level, msg string, snap Snapshot, attrs ...any) {
	l.cfg.Logger.Log(ctx, level, msg, append([]any{"run_id", snap.State.RunID, "seq", snap.Seq, "phase", snap.State.Phase, "step", snap.State.Step}, attrs...)...)
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
		s.log.WarnContext(ctx, "commit failed", append(attrs, "failure", t.Facts.Failure, "error", err)...)
	case t.Facts.Failure != "":
		s.log.WarnContext(ctx, "committed", append(attrs, "failure", t.Facts.Failure)...)
	default:
		s.log.InfoContext(ctx, "committed", attrs...)
	}
	return err
}
