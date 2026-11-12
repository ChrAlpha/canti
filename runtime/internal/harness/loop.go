package harness

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

const maxBackoff = 30 * time.Second

const historyPage = 64

// LoopConfig wires a Loop to its environment. Zero values take the defaults
// noted on each field.
type LoopConfig struct {
	NativeArchive  bool             // archive paths are logical addresses, without shell mounts
	Environment    string           // Frame.Environment
	History        string           // host directory for published step records; archive.RunsRoot is their logical address
	Inputs         string           // host directory for published inputs; archive.InputsRoot is their logical address
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
	// cancels carries a principal's request to cancel the accepted effect to
	// the effect watch of the drive that owns it.
	cancels  chan effectCancel
	watching atomic.Bool
}

type effectCancel struct {
	done     chan error
	effectID string
}

// errCancelStale reports a cancellation whose inspected effect is no longer
// the one the watch owns. It is never applied to the current effect.
var errCancelStale = errors.New("harness: the effect to cancel is no longer current")

// NewLoop validates cfg, applies its defaults, and builds a Loop that
// commits every transition through a Runner on j and x.
func NewLoop(j Journal, x Executor, m Model, cfg LoopConfig) (*Loop, error) {
	reader := cfg.Reader
	if j == nil || m == nil || x == nil {
		return nil, errors.New("harness: loop journal, executor and model are required")
	}
	if strings.TrimSpace(cfg.History) == "" {
		return nil, errPaths
	}
	if strings.TrimSpace(cfg.Inputs) == "" {
		cfg.Inputs = filepath.Join(filepath.Dir(cfg.History), "inputs")
	}
	cfg, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	r, err := NewRunner(logged{j, cfg.Logger})
	if err != nil {
		return nil, err
	}
	l := &Loop{journal: j, executor: x, model: m, runner: r, cfg: cfg, cancels: make(chan effectCancel, 1)}
	if cfg.Reader != nil {
		cfg.Reader.RepairInput = l.repairInput
	}
	if reader != nil {
		reader.RepairInput = l.repairInput
	}
	return l, nil
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
	if err != nil {
		return snap, err
	}
	if snap.State.Phase != core.PhaseEffectSaved {
		// A cancellation request that reached the loop after its effect left
		// effect_saved cannot be served; answer it instead of holding its
		// caller's run lock.
		l.drainStepCancels(snap.State.EffectID)
	}
	if snap.State.Intent() == core.IntentHalt {
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

// RequestCancelEffect asks the effect watch that owns the accepted effect of
// runID to cancel it once and record its result, without stopping the run.
// The watch continues the run to its next step. It returns after that
// observation is committed, or with a bounded failure when the run left
// effect_saved without observing it; a caller waits under its own deadline.
func (l *Loop) RequestCancelEffect(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.load(ctx, runID)
	if err != nil {
		return snap, err
	}
	if snap.State.Phase != core.PhaseEffectSaved {
		return snap, fmt.Errorf("run %s in phase %q has no accepted effect to cancel", runID, snap.State.Phase)
	}
	req := effectCancel{done: make(chan error, 1), effectID: snap.State.EffectID}
	select {
	case l.cancels <- req:
	case <-ctx.Done():
		return snap, ctx.Err()
	}
	select {
	case err := <-req.done:
		next, loadErr := l.load(ctx, runID)
		if err != nil {
			return next, err
		}
		return next, loadErr
	case <-ctx.Done():
		return snap, ctx.Err()
	}
}

// CancelEffect cancels the accepted effect of runID through a loop that owns
// no drive, as a stop does without a model, and records its result so the
// run continues. It refuses when the run holds no accepted effect.
func (l *Loop) CancelEffect(ctx context.Context, runID string) (Snapshot, error) {
	snap, err := l.load(ctx, runID)
	if err != nil {
		return snap, err
	}
	if snap.State.Phase != core.PhaseEffectSaved {
		return snap, fmt.Errorf("run %s in phase %q has no accepted effect to cancel", runID, snap.State.Phase)
	}
	run, err := l.journal.Run(ctx, runID)
	if err != nil {
		return snap, err
	}
	effect, err := l.effectOf(ctx, snap)
	if err != nil {
		return snap, err
	}
	w := &effectWatch{Loop: l, run: run, snap: snap, effect: effect, userCancel: true, cancelIssued: true}
	next, done, err := w.cancelAndComplete(ctx)
	if done {
		return next, err
	}
	for {
		next, done, err = w.step(ctx)
		if done {
			return next, err
		}
	}
}

// effectOf reconstructs the accepted effect of snap, which must be in
// effect_saved.
func (l *Loop) effectOf(ctx context.Context, snap Snapshot) (Effect, error) {
	a, err := ActionOf(ctx, l.journal, snap.State)
	if err != nil {
		return Effect{}, err
	}
	if a.Local() {
		return Effect{}, errors.New("harness: the accepted action is local")
	}
	run, err := l.journal.Run(ctx, snap.State.RunID)
	if err != nil {
		return Effect{}, err
	}
	return l.descriptor(run, snap.State, a)
}

// cancelMatches is the one EffectID check for every path that receives a
// cancellation request. A request for another effect is answered stale and is
// never applied to the current one.
func cancelMatches(req effectCancel, effectID string) bool {
	if req.effectID != effectID {
		req.done <- errCancelStale
		return false
	}
	return true
}

// drainStepCancels answers requests that reached the loop after their effect
// left effect_saved, without applying them to the run's current effect.
func (l *Loop) drainStepCancels(effectID string) {
	for {
		select {
		case req := <-l.cancels:
			if cancelMatches(req, effectID) {
				replyCancel(req, false)
			}
		default:
			return
		}
	}
}

func replyCancel(req effectCancel, observed bool) {
	if observed {
		req.done <- nil
		return
	}
	req.done <- errCancelUnobserved
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

// bind commits the instructions admitted to the run that its state has not
// bound yet, so the next request or decision sees every instruction admitted
// before its commit. A steer admitted between the bind and the following
// commit is caught by the store's return guard or by decode's retry.
func (l *Loop) bind(ctx context.Context, snap Snapshot) (Snapshot, error) {
	steers, err := l.journal.Steers(ctx, snap.State.RunID)
	if err != nil {
		return snap, err
	}
	switch {
	case len(steers) < snap.State.Inputs:
		return snap, contextError{fmt.Errorf("harness: run %s holds %d steers but %d are bound", snap.State.RunID, len(steers), snap.State.Inputs)}
	case len(steers) == snap.State.Inputs:
		return snap, nil
	}
	return l.runner.Commit(ctx, snap, core.InputsBound{Count: len(steers)}, Facts{})
}

// decode commits the decision of the saved response and, when it accepts an
// exec decision, the action the harness reads it as under the request it
// answers. It binds instructions admitted since the request was frozen first;
// such a decision is superseded, and the step completes without an effect.
func (l *Loop) decode(ctx context.Context, snap Snapshot) (Snapshot, error) {
	s := snap.State
	resp, err := l.journal.Response(ctx, s.RunID, s.Step, s.Attempt)
	if err != nil {
		return snap, err
	}
	body := l.decision(resp)
	event := core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: body}
	for {
		if snap, err = l.bind(ctx, snap); err != nil {
			return snap, err
		}
		s = snap.State
		if s.Step != event.Step || s.Attempt != event.Attempt {
			return snap, contextError{fmt.Errorf("harness: response of step %d attempt %d no longer belongs to the run", event.Step, event.Attempt)}
		}
		facts := Facts{}
		decision, superseded := s.Superseded(body)
		if superseded {
			facts.Supersede = &Superseded{Instruction: s.Seen + 1, Kind: decision.Kind, Purpose: decision.Purpose}
		} else if accepted, err := s.Next(event); err == nil && accepted.Phase == core.PhaseEffectSaved {
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
		if errors.Is(err, ErrInputsPending) {
			continue
		}
		if err != nil {
			return next, err
		}
		switch {
		case facts.Supersede != nil:
			l.note(ctx, slog.LevelInfo, "superseded a decision with a later instruction", next, "instruction", facts.Supersede.Instruction, "decision", facts.Supersede.Kind)
		case next.State.Phase == core.PhaseIdle:
			l.note(ctx, slog.LevelWarn, "retrying an undecodable response", next, "attempt", next.State.Attempt)
		}
		return next, nil
	}
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
	defer func() {
		w.drainCancels(w.observed)
		w.replyCancels(w.cancelResult())
	}()
	for {
		next, done, err := w.step(ctx)
		if done {
			return next, err
		}
	}
}

// errCancelUnobserved is the bounded failure of a cancellation whose effect
// left effect_saved without a terminal observation.
var errCancelUnobserved = errors.New("harness: the run left effect_saved before the cancellation was observed")

type effectWatch struct {
	*Loop
	run              Run
	snap             Snapshot
	effect           Effect
	stopping         bool
	failures         int
	timeoutRequested bool
	early            bool
	userCancel       bool
	cancelIssued     bool
	observed         bool
	cancelRequests   []effectCancel
}

func (w *effectWatch) step(ctx context.Context) (Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return w.snap, true, err
	}
	if w.takeCancel() {
		w.userCancel = true
	}
	if w.userCancel && !w.cancelIssued {
		next, done, err := w.cancelAndComplete(ctx)
		if done {
			w.replyCancels(w.cancelResultFor(err))
			return next, true, err
		}
	}
	if !w.stopping && !w.userCancel {
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
		next, err := w.complete(ctx, job)
		return next, true, err
	}
}

func (w *effectWatch) sample(ctx context.Context) (Job, bool, error) {
	if w.stopping {
		job, err := w.executor.Cancel(ctx, w.effect)
		return job, false, err
	}
	call, cancel := ctx, context.CancelFunc(func() {})
	budgeted := w.run.Budget.WallTime > 0 && !w.userCancel
	if budgeted {
		call, cancel = context.WithTimeout(ctx, w.run.Created.Add(w.run.Budget.WallTime).Sub(w.cfg.Now()))
	}
	defer cancel()
	job, err := w.executor.Drive(call, w.effect)
	return job, budgeted && call.Err() != nil, err
}

// takeCancel keeps every pending principal cancellation for the watch to
// answer once its effect is observed, or to fail with the bounded result when
// the watch ends first.
func (w *effectWatch) takeCancel() bool {
	taken := false
	for {
		select {
		case req := <-w.cancels:
			if !cancelMatches(req, w.effect.ID) {
				continue
			}
			w.cancelRequests = append(w.cancelRequests, req)
			taken = true
		default:
			if w.observed && taken {
				w.replyCancels(nil)
			}
			return taken
		}
	}
}

// drainCancels answers requests received after the watch ended. A request
// for another effect is stale; a request for this effect is unobserved.
func (w *effectWatch) drainCancels(observed bool) {
	for {
		select {
		case req := <-w.cancels:
			if cancelMatches(req, w.effect.ID) {
				replyCancel(req, observed)
			}
		default:
			return
		}
	}
}

// replyCancels answers every cancellation this watch took, once.
func (w *effectWatch) replyCancels(err error) {
	for _, req := range w.cancelRequests {
		req.done <- err
	}
	w.cancelRequests = nil
}

// cancelResult is what a taken request learns when the watch ends: the
// committed observation, or the bounded failure.
func (w *effectWatch) cancelResult() error {
	if w.observed {
		return nil
	}
	return errCancelUnobserved
}

func (w *effectWatch) cancelResultFor(err error) error {
	if err != nil {
		return err
	}
	return w.cancelResult()
}

// cancelAndComplete calls Executor.Cancel once for the accepted effect and,
// when that is terminal, commits its observation. It reports whether the
// effect was completed.
func (w *effectWatch) cancelAndComplete(ctx context.Context) (Snapshot, bool, error) {
	w.cancelIssued, w.userCancel = true, true
	job, err := w.executor.Cancel(ctx, w.effect)
	if err != nil {
		return w.snap, true, err
	}
	if err := job.Validate(w.effect); err != nil {
		next, err := w.unknown(ctx, w.snap, err.Error(), &job)
		return next, true, err
	}
	switch job.Status {
	case JobSucceeded, JobFailed, JobFailedToStart, JobCancelled:
		next, err := w.complete(ctx, job)
		return next, true, err
	case JobPending, JobRunning:
		return w.snap, false, nil
	}
	next, err := w.unknown(ctx, w.snap, fmt.Sprintf("effect %s reports %s: %s", w.effect.ID, job.Status, job.Output.Detail), &job)
	return next, true, err
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
	if !w.stopping && !w.userCancel && !w.timeoutRequested && job.Status == JobRunning && timeout > 0 && !job.budgetOrigin().IsZero() && w.cfg.Now().Sub(job.budgetOrigin()) >= timeout {
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
			next, err := w.complete(ctx, job)
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
	done := make(chan error, 1)
	go func() { done <- waiter.Wait(call, w.effect) }()
	var err error
	for {
		select {
		case err = <-done:
		case req := <-w.cancels:
			if !cancelMatches(req, w.effect.ID) {
				continue
			}
			w.cancelRequests = append(w.cancelRequests, req)
			w.userCancel = true
			w.cancelIssued = false
			cancel()
			<-done
			return nil
		case <-ctx.Done():
			cancel()
			<-done
			return ctx.Err()
		}
		break
	}
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

func (w *effectWatch) complete(ctx context.Context, job Job) (Snapshot, error) {
	timeout := w.run.Budget.JobTimeout
	if w.userCancel {
		timeout = 0
	}
	execution := resultExecution(job, timeout, w.snap.State.Phase == core.PhaseCancelling)
	if execution == core.ExecutionUnknown {
		return w.unknown(ctx, w.snap, "executor reports unknown after cancellation", &job)
	}
	obs := observationOf(job, execution)
	next, err := w.runner.Commit(ctx, w.snap, core.Observed{EffectID: w.snap.State.EffectID, Execution: execution}, Facts{Observation: &obs, Receipt: &job})
	if err == nil {
		w.observed = true
	}
	return next, err
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
