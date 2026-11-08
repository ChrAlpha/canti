package harness

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
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
	if j == nil || m == nil || x == nil {
		return nil, errors.New("harness: loop journal, executor and model are required")
	}
	if strings.TrimSpace(cfg.History) == "" {
		return nil, errPaths
	}
	cfg, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	r, err := NewRunner(logged{j, cfg.Logger})
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
		r.Excluded = []string{ActionNamespace, DiscoveryMarker}
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
	switch snap.State.Intent() {
	case core.IntentPrepareRequest:
		return l.prepare(ctx, run, snap)
	case core.IntentSendRequest:
		return l.send(ctx, run, snap)
	case core.IntentDecode:
		return l.decode(ctx, snap)
	case core.IntentEffect, core.IntentCancel:
		return l.effect(ctx, run, snap)
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

// ErrJobUnknown reports evidence which cannot authorize recovery.
var ErrJobUnknown = errors.New("harness: the effect has no known recoverable outcome")

// ResolveJob explicitly inspects unknown work. Active evidence recovers the
// same accepted effect; terminal evidence settles it with inspection provenance.
func (l *Loop) ResolveJob(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.unresolved(ctx, runID)
	if err != nil {
		return snap, err
	}
	job, err := l.inspect(ctx, snap)
	if err != nil {
		return snap, err
	}
	switch job.Status {
	case JobPending, JobRunning:
		return l.runner.Commit(ctx, snap, core.EffectRecovered{EffectID: snap.State.EffectID}, Facts{Receipt: &job})
	case JobAbsent, JobUnknown:
		return snap, fmt.Errorf("%w: %s: %s", ErrJobUnknown, job.Status, job.Output.Detail)
	}
	run, err := l.journal.Run(ctx, runID)
	if err != nil {
		return snap, err
	}
	execution := resultExecution(job, run.Budget.JobTimeout, snap.State.Reason != "")
	obs := observationOf(job, execution)
	return l.runner.Commit(ctx, snap, core.Resolved{EffectID: snap.State.EffectID, Execution: execution}, Facts{Observation: &obs, Receipt: &job, Resolution: &Resolution{Source: ResolutionInspection, OwnerID: job.ID}})
}

// Resolve records an explicit operator assertion, including physical owner
// provenance for exited/timed-out work. It performs no backend operation.
func (l *Loop) Resolve(ctx context.Context, runID string, obs Observation, ownerID string) (Snapshot, error) {
	if obs.Output != (ArtifactRef{}) && obs.Output != (ArtifactRef{Detail: "resolved by operator"}) {
		return Snapshot{}, errors.New("harness: operator assertion cannot fabricate retained output")
	}
	obs.Output = ArtifactRef{Detail: "resolved by operator"}

	snap, err := l.unresolved(ctx, runID)
	if err != nil {
		return snap, err
	}
	return l.runner.Commit(ctx, snap, core.Resolved{EffectID: snap.State.EffectID, Execution: obs.Execution}, Facts{Observation: &obs, Resolution: &Resolution{Source: ResolutionOperator, OwnerID: ownerID}})
}

// Inspect samples the current external effect without committing progress or
// starting work. Its snapshot and backend receipt are separate observations.
func (l *Loop) Inspect(ctx context.Context, runID string) (Snapshot, Job, error) {
	snap, err := l.load(ctx, runID)
	if err != nil {
		return snap, Job{}, err
	}
	job, err := l.inspect(ctx, snap)
	return snap, job, err
}

func (l *Loop) inspect(ctx context.Context, snap Snapshot) (Job, error) {
	a, err := ActionOf(ctx, l.journal, snap.State)
	if err != nil {
		return Job{}, err
	}
	if a.Local() {
		return Job{}, errors.New("harness: the accepted action is local")
	}
	run, err := l.journal.Run(ctx, snap.State.RunID)
	if err != nil {
		return Job{}, err
	}
	e, err := l.descriptor(run, snap.State, a)
	if err != nil {
		return Job{}, err
	}
	job, err := l.executor.Inspect(ctx, e)
	if err == nil {
		err = job.Validate(e)
	}
	return job, err
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
	tokens, err := CheckedAdd(t.Usage.InputTokens, t.Usage.OutputTokens)
	if err != nil {
		return "", "", err
	}
	switch {
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
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: why, Response: &resp})
	}
	if err == nil && (!resp.Complete || len(resp.Body) == 0) {
		err = errors.New("harness: model reported success without a complete response")
	}
	var limit *ContextLimitError
	var stop *StopError
	switch {
	case err == nil:
		return l.runner.Commit(ctx, snap, core.ResponseSaved{Step: s.Step, Attempt: attempt}, Facts{Response: &resp})
	case errors.As(err, &limit):
		return l.reject(ctx, snap, req, resp, limit)
	case errors.As(err, &stop):
		return l.runner.Commit(ctx, snap, core.StopRequested{Reason: stop.Reason}, Facts{Failure: err.Error(), Response: &resp})
	}
	resp.Complete = false
	facts := Facts{Failure: err.Error(), Response: &resp}
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

// decode commits the decision of the saved response and, when it accepts an
// exec decision, the action the harness reads it as under the request it
// answers.
func (l *Loop) decode(ctx context.Context, snap Snapshot) (Snapshot, error) {
	s := snap.State
	resp, err := l.journal.Response(ctx, s.RunID, s.Step, s.Attempt)
	if err != nil {
		return snap, err
	}
	event, facts := core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: l.decision(resp)}, Facts{}
	if accepted, err := s.Next(event); err == nil && accepted.Phase == core.PhaseEffectSaved {
		a := NormalizeAction(accepted.Decision.Payload, l.cfg.Reader != nil)
		facts.Action = &a
		if a.Recovery == RecoveryExternal {
			run, err := l.journal.Run(ctx, s.RunID)
			if err != nil {
				return snap, err
			}
			facts.Execution = &ExecutionIdentity{Spec: l.executor.Specification(accepted.Decision.Payload), ConfigDigest: ConfigDigest(run.Config)}
		}
	}
	next, err := l.runner.Commit(ctx, snap, event, facts)
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

// watch returns control to harness policy between bounded executor operations.
// No transient backend phase is recorded as Run progress.
func (l *Loop) watch(ctx context.Context, run Run, snap Snapshot, effect Effect) (Snapshot, error) {
	w := effectWatch{Loop: l, run: run, snap: snap, effect: effect, stopping: snap.State.Phase == core.PhaseCancelling}
	for {
		next, done, err := w.step(ctx)
		if done {
			return next, err
		}
	}
}

type effectWatch struct {
	*Loop
	run              Run
	snap             Snapshot
	effect           Effect
	stopping         bool
	failures         int
	timeoutRequested bool
	early            bool
}

func (w *effectWatch) step(ctx context.Context) (Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return w.snap, true, err
	}
	if !w.stopping {
		reason, why, err := w.spent(ctx, w.run)
		if err != nil {
			return w.snap, true, err
		}
		if reason != "" {
			next, err := w.runner.Commit(ctx, w.snap, core.StopRequested{Reason: reason}, Facts{Failure: why})
			return next, true, err
		}
	}
	job, expired, err := w.sample(ctx)
	if ctx.Err() != nil {
		return w.snap, true, ctx.Err()
	}
	if expired {
		next, err := w.runner.Commit(ctx, w.snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: "the wall-time budget ran out while driving the accepted effect"})
		return next, true, err
	}
	if err != nil {
		return w.failed(ctx, err)
	}
	if err := job.Validate(w.effect); err != nil {
		next, err := w.unknown(ctx, w.snap, err.Error(), nil)
		return next, true, err
	}
	w.failures = 0
	switch job.Status {
	case JobAbsent, JobUnknown:
		next, err := w.unknown(ctx, w.snap, fmt.Sprintf("effect %s reports %s: %s", w.effect.ID, job.Status, job.Output.Detail), &job)
		return next, true, err
	case JobPending, JobRunning:
		return w.pending(ctx, job)
	default:
		next, err := w.complete(ctx, w.run, w.snap, job)
		return next, true, err
	}
}

func (w *effectWatch) sample(ctx context.Context) (Job, bool, error) {
	if w.stopping {
		job, err := w.executor.Cancel(ctx, w.effect)
		return job, false, err
	}
	call, cancel := ctx, context.CancelFunc(func() {})
	budgeted := w.run.Budget.WallTime > 0
	if budgeted {
		call, cancel = context.WithTimeout(ctx, w.run.Created.Add(w.run.Budget.WallTime).Sub(w.cfg.Now()))
	}
	defer cancel()
	job, err := w.executor.Drive(call, w.effect)
	return job, budgeted && call.Err() != nil, err
}

func (w *effectWatch) failed(ctx context.Context, cause error) (Snapshot, bool, error) {
	if errors.Is(cause, ErrJobLost) {
		next, err := w.unknown(ctx, w.snap, cause.Error(), nil)
		return next, true, err
	}
	if !w.stopping {
		if why := w.late(w.run); why != "" {
			next, err := w.runner.Commit(ctx, w.snap, core.StopRequested{Reason: core.StopTimedOut}, Facts{Failure: why + "; " + cause.Error()})
			return next, true, err
		}
	}
	w.failures++
	if w.failures > w.cfg.InspectRetries {
		next, err := w.unknown(ctx, w.snap, cause.Error(), nil)
		return next, true, err
	}
	w.note(ctx, slog.LevelWarn, "retrying executor evidence", w.snap, "failures", w.failures, "error", cause)
	err := w.pause(ctx, Job{})
	return w.snap, err != nil, err
}

func (w *effectWatch) pending(ctx context.Context, job Job) (Snapshot, bool, error) {
	timeout := w.run.Budget.JobTimeout
	if !w.stopping && !w.timeoutRequested && job.Status == JobRunning && timeout > 0 && !job.budgetOrigin().IsZero() && w.cfg.Now().Sub(job.budgetOrigin()) >= timeout {
		var err error
		job, err = w.executor.Cancel(ctx, w.effect)
		if err != nil {
			return w.snap, true, err
		}
		if err := job.Validate(w.effect); err != nil {
			next, err := w.unknown(ctx, w.snap, err.Error(), nil)
			return next, true, err
		}
		w.timeoutRequested = true
		if job.Status != JobRunning && job.Status != JobPending {
			next, err := w.complete(ctx, w.run, w.snap, job)
			return next, true, err
		}
	}
	err := w.await(ctx, job)
	return w.snap, err != nil, err
}

const maxWait = 30 * time.Second

func (w *effectWatch) waitLimit(job Job, limit time.Duration) time.Duration {
	if w.stopping {
		return limit
	}
	if d := w.run.Budget.WallTime; d > 0 {
		limit = min(limit, d-w.cfg.Now().Sub(w.run.Created))
	}
	if d := w.run.Budget.JobTimeout; d > 0 && !job.Fenced && !job.budgetOrigin().IsZero() {
		limit = min(limit, d-w.cfg.Now().Sub(job.budgetOrigin()))
	}
	return limit
}

func (w *effectWatch) pause(ctx context.Context, job Job) error {
	return sleep(ctx, w.waitLimit(job, w.cfg.Poll))
}

func (w *effectWatch) await(ctx context.Context, job Job) error {
	waiter, ok := w.executor.(Waiter)
	if !ok || job.Status != JobRunning || w.early {
		w.early = false
		return w.pause(ctx, job)
	}
	began := time.Now()
	call, cancel := context.WithTimeout(ctx, w.waitLimit(job, maxWait))
	defer cancel()
	err := waiter.Wait(call, w.effect)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil && call.Err() == nil {
		return w.pause(ctx, job)
	}
	// Always sample after a wake-up. Only a still-active next sample sleeps
	// after an early wake, so a genuine exit is observed without poll latency.
	w.early = time.Since(began) < w.cfg.Poll
	return nil
}

func resultExecution(job Job, timeout time.Duration, stopping bool) core.ExecutionState {
	switch job.Status {
	case JobSucceeded, JobFailed:
		return core.ExecutionExited
	case JobFailedToStart:
		return core.ExecutionFailedToStart
	case JobCancelled:
		if !stopping && timeout > 0 && !job.budgetOrigin().IsZero() && !job.budgetEnd().IsZero() && job.budgetEnd().Sub(job.budgetOrigin()) >= timeout {
			return core.ExecutionTimedOut
		}
		return core.ExecutionCancelled
	}
	return core.ExecutionUnknown
}

func observationOf(job Job, execution core.ExecutionState) Observation {
	return Observation{Execution: execution, ExitCode: job.ExitCode, Preview: job.Preview, Output: job.Output}
}

func (l *Loop) complete(ctx context.Context, run Run, snap Snapshot, job Job) (Snapshot, error) {
	execution := resultExecution(job, run.Budget.JobTimeout, snap.State.Phase == core.PhaseCancelling)
	if execution == core.ExecutionUnknown {
		return l.unknown(ctx, snap, "executor reports unknown after cancellation", &job)
	}
	obs := observationOf(job, execution)
	return l.runner.Commit(ctx, snap, core.Observed{EffectID: snap.State.EffectID, Execution: execution}, Facts{Observation: &obs, Receipt: &job})
}

func (l *Loop) unknown(ctx context.Context, snap Snapshot, why string, receipt *Job) (Snapshot, error) {
	return l.runner.Commit(ctx, snap, core.OutcomeUnknown{EffectID: snap.State.EffectID}, Facts{Failure: why, Receipt: receipt})
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
