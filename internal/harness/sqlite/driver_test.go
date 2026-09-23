package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

type executor struct{}

func (executor) Bind(_ context.Context, effect harness.Effect) (harness.Job, error) {
	return harness.Job{ID: "job-" + effect.ID, Status: harness.JobPending}, nil
}

func (executor) Start(_ context.Context, effectID string) (harness.Job, error) {
	return harness.Job{ID: "job-" + effectID, Status: harness.JobRunning}, nil
}

func (executor) Inspect(context.Context, string) (harness.Job, bool, error) {
	return harness.Job{}, false, nil
}

func (executor) Cancel(context.Context, string) error { return nil }

type storeFunc func(context.Context, harness.Transition) error

func (f storeFunc) Commit(ctx context.Context, t harness.Transition) error { return f(ctx, t) }

func newRunner(t testing.TB, store harness.Store) harness.Runner {
	t.Helper()
	runner, err := harness.NewRunner(store, executor{})
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
	d.t.Helper()
	if err := d.try(event, facts); err != nil {
		d.t.Fatalf("Commit(%T) at seq %d error = %v", event, d.snap.Seq+1, err)
	}
}

func (d *driver) call(op func(harness.Runner, context.Context, harness.Snapshot) (harness.Snapshot, error)) {
	d.t.Helper()
	next, err := op(d.runner, context.Background(), d.snap)
	if err != nil {
		d.t.Fatalf("runner call at seq %d error = %v", d.snap.Seq, err)
	}
	d.snap = next
}

func (d *driver) freeze() {
	d.t.Helper()
	d.do(core.RequestFrozen{}, harness.Facts{Request: requestFor(d.snap.State)})
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
	d.do(core.ResponseLoaded{Step: d.snap.State.Step, Attempt: d.snap.State.Attempt, Body: body}, harness.Facts{})
}

func (d *driver) observe(execution core.ExecutionState, outputBytes int64) {
	d.t.Helper()
	d.do(core.Observed{EffectID: d.snap.State.EffectID, Execution: execution}, harness.Facts{Observation: observation(execution, outputBytes)})
}

// decide drives the current step from idle to a decoded decision.
func (d *driver) decide(body []byte, usage harness.Usage) {
	d.t.Helper()
	d.freeze()
	d.send()
	d.save(body, usage)
	d.load(body)
}

// step completes one exec step through bind, start, and observation.
func (d *driver) step(command string, usage harness.Usage, outputBytes int64) {
	d.t.Helper()
	d.decide(execBody(command), usage)
	d.call(harness.Runner.Bind)
	d.call(harness.Runner.Start)
	d.observe(core.ExecutionExited, outputBytes)
}

func execBody(command string) []byte {
	return fmt.Appendf(nil, `{"kind":"exec","payload":%q}`, command)
}

func requestFor(s core.State) *harness.Request {
	return &harness.Request{Body: fmt.Appendf(nil, "request %d.%d", s.Step, s.RequestRev+1), Model: "m", Protocol: "p", Adapter: "a"}
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
