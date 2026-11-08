package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

type storeFunc func(context.Context, harness.Transition) error

func (f storeFunc) Commit(ctx context.Context, t harness.Transition) error { return f(ctx, t) }

func newRunner(t testing.TB, store harness.Store) harness.Runner {
	t.Helper()
	runner, err := harness.NewRunner(store)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

// driver moves one run forward through harness.Runner and keeps every
// transition its store acknowledged.
type driver struct {
	t       testing.TB
	store   harness.Store
	runner  harness.Runner
	snap    harness.Snapshot
	commits []harness.Transition
}

func newDriver(t testing.TB, store harness.Store, snap harness.Snapshot) *driver {
	d := &driver{t: t, store: store, snap: snap}
	d.runner = newRunner(t, storeFunc(func(ctx context.Context, tr harness.Transition) error {
		if err := d.store.Commit(ctx, tr); err != nil {
			return err
		}
		d.commits = append(d.commits, tr)
		return nil
	}))
	return d
}

func (d *driver) try(event core.Event, facts harness.Facts) error {
	next, err := d.runner.Commit(context.Background(), d.snap, event, facts)
	if err == nil {
		d.snap = next
	}
	return err
}

func (d *driver) do(event core.Event, facts harness.Facts) {
	if _, ok := event.(core.Resolved); ok {
		facts.Resolution = &harness.Resolution{Source: harness.ResolutionOperator, OwnerID: "operator-owner"}
		if facts.Observation.Execution != core.ExecutionUnknown {
			facts.Observation.Output = harness.ArtifactRef{Detail: "resolved by operator"}
			if facts.Observation.Preview == "" {
				facts.Observation.Preview = "checked by operator"
			}
			if facts.Observation.Execution != core.ExecutionExited {
				facts.Observation.ExitCode = nil
			}
		}
		if facts.Observation.Execution == core.ExecutionUnknown {
			facts.Observation.ExitCode = nil
			facts.Observation.Output = harness.ArtifactRef{Detail: "resolved by operator"}
		}
	}
	d.t.Helper()
	if err := d.try(event, facts); err != nil {
		d.t.Fatalf("Commit(%T) at seq %d error = %v", event, d.snap.Seq+1, err)
	}
}

func (d *driver) freeze() {
	d.t.Helper()
	request := requestFor(d.snap.State)
	if j, ok := d.store.(interface {
		Context(context.Context, harness.Snapshot) (harness.ContextState, error)
	}); ok {
		current, err := j.Context(context.Background(), d.snap)
		if err != nil {
			d.t.Fatal(err)
		}
		request.ContextState = &current
	}
	d.do(core.RequestFrozen{}, harness.Facts{Request: request})
}

func (d *driver) send() {
	d.t.Helper()
	d.do(core.AttemptStarted{Step: d.snap.State.Step, Attempt: d.snap.State.Attempt}, harness.Facts{})
}

func (d *driver) save(body []byte, usage harness.Usage) {
	d.t.Helper()
	s := d.snap.State
	d.do(core.ResponseSaved{Step: s.Step, Attempt: s.Attempt}, harness.Facts{Response: responseFor(s, body, usage)})
}

func (d *driver) load(body []byte) {
	d.t.Helper()
	event := core.ResponseLoaded{Step: d.snap.State.Step, Attempt: d.snap.State.Attempt, Body: body}
	d.do(event, acceptance(d.snap.State, event))
}

// acceptance returns the facts of event, from s, which carry the action of
// an accepted exec decision under a request that requires no handover.
func acceptance(s core.State, event core.ResponseLoaded) harness.Facts {
	next, err := s.Next(event)
	if err != nil || next.Phase != core.PhaseEffectSaved {
		return harness.Facts{}
	}
	a := harness.NormalizeAction(next.Decision.Payload, true)
	f := harness.Facts{Action: &a}
	if a.Recovery == harness.RecoveryExternal {
		f.Execution = &harness.ExecutionIdentity{Spec: harness.ConfigDigest([]byte(next.Decision.Payload)), ConfigDigest: harness.ConfigDigest(testRun(next.RunID).Config)}
	}
	return f
}

func (d *driver) observe(execution core.ExecutionState, outputBytes int64) {
	d.t.Helper()
	obs := observation(execution, outputBytes)
	j := receipt(d.snap.State, *obs)
	d.do(core.Observed{EffectID: d.snap.State.EffectID, Execution: execution}, harness.Facts{Observation: obs, Receipt: &j})
}

// decide drives the current step from idle to a decoded decision.
func (d *driver) decide(body []byte, usage harness.Usage) {
	d.t.Helper()
	d.freeze()
	d.send()
	d.save(body, usage)
	d.load(body)
}

// step completes accepted external work and its atomic observation.
func (d *driver) step(command string, usage harness.Usage, outputBytes int64) {
	d.t.Helper()
	d.decide(execBody(command), usage)
	d.observe(core.ExecutionExited, outputBytes)
}

// perform completes the effect of the current step, a local action, with
// the outcome that its invalid arguments yield.
func (d *driver) perform() {
	d.t.Helper()
	a := harness.NormalizeAction(d.snap.State.Decision.Payload, true)
	if a.Invalid == "" {
		d.t.Fatalf("perform needs an invalid local action, not %+v", a)
	}
	d.do(core.Performed{EffectID: d.snap.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeFailed, Detail: a.Problem()}})
}

func execBody(command string) []byte {
	return fmt.Appendf(nil, `{"kind":"exec","payload":%q}`, command)
}

func requestFor(s core.State) *harness.Request {
	return &harness.Request{ContextState: &harness.ContextState{FirstStep: 1}, Body: fmt.Appendf(nil, "request %d.%d", s.Step, s.RequestRev+1), Model: "m", Protocol: "p", Adapter: "a"}
}

func responseFor(s core.State, body []byte, usage harness.Usage) *harness.Response {
	return &harness.Response{Body: body, Complete: true, Model: fmt.Sprintf("m-%d.%d", s.Step, s.Attempt), Usage: usage}
}

func observation(execution core.ExecutionState, outputBytes int64) *harness.Observation {
	code := 0
	return &harness.Observation{
		Execution: execution,
		ExitCode:  &code,
		Preview:   "ok",
		Output:    harness.ArtifactRef{Path: "out.log", Bytes: outputBytes, Complete: true, Available: true},
	}
}

// derive returns the transition Runner.Commit hands to its store for event,
// without storing it.
func derive(t testing.TB, snap harness.Snapshot, event core.Event, facts harness.Facts) harness.Transition {
	t.Helper()
	var got harness.Transition
	runner := newRunner(t, storeFunc(func(_ context.Context, tr harness.Transition) error {
		got = tr
		return nil
	}))
	if _, err := runner.Commit(context.Background(), snap, event, facts); err != nil {
		t.Fatalf("derive %T error = %v", event, err)
	}
	return got
}

func receipt(s core.State, obs harness.Observation) harness.Job {
	j := harness.Job{EffectID: s.EffectID, Spec: harness.ConfigDigest([]byte(s.Decision.Payload)), ID: "job-" + s.EffectID, StartedAt: time.Unix(1, 0).UTC(), FinishedAt: time.Unix(2, 0).UTC(), ExitCode: obs.ExitCode, Preview: obs.Preview, Output: obs.Output}
	switch obs.Execution {
	case core.ExecutionExited:
		j.Status = harness.JobSucceeded
		if obs.ExitCode != nil && *obs.ExitCode != 0 {
			j.Status = harness.JobFailed
		}
	case core.ExecutionCancelled, core.ExecutionTimedOut:
		j.Status, j.Fenced = harness.JobCancelled, true
	case core.ExecutionFailedToStart:
		j.Status = harness.JobFailedToStart
		j.StartedAt = time.Time{}
		j.ExitCode = nil
	case core.ExecutionUnknown:
		j.Status = harness.JobUnknown
	}
	return j
}
