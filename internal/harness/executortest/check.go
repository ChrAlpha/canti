// Package executortest checks a harness.Executor against its start latch
// contract.
package executortest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// Fixture opens the executor under test and observes its executions. Open
// must return a new executor on every call that shares only clients and
// durable backend state, as a restarted or competing controller would see
// it; Check opens one for every operation and leaves releasing them to the
// fixture. Effect must return a fresh effect with a distinct command for each
// name, and Runs must report how many times the command of an effect has
// started running.
type Fixture struct {
	Open   func(context.Context) (harness.Executor, error)
	Effect func(name string) harness.Effect
	Runs   func(ctx context.Context, effectID string) (int, error)
}

// Check runs every contract case against f and returns the violations
// joined, or nil. After the cases it counts every run again, so an execution
// that lands before Check returns still fails its case. ctx bounds the whole
// check, including waits for started jobs to finish. Check cannot inject
// crashes, and it detects a start racing a cancel only when the two calls
// overlap.
func Check(ctx context.Context, f Fixture) error {
	c := checker{Fixture: f, x: fresh(f.Open)}
	type passed struct {
		name   string
		effect harness.Effect
		runs   int
	}
	var done []passed
	var errs []error
	for _, tc := range []struct {
		name  string
		check func(context.Context, harness.Effect) error
	}{
		{"bind", c.bind},
		{"changed-command", c.changedCommand},
		{"start-once", c.startOnce},
		{"unbound-start", c.unboundStart},
		{"fence-bound", c.fenceBound},
		{"fence-unbound", c.fenceUnbound},
		{"concurrent-start", c.concurrentStart},
		{"start-cancel", c.startCancel},
	} {
		e := f.Effect(tc.name)
		err := tc.check(ctx, e)
		if err == nil {
			var runs int
			if runs, err = f.Runs(ctx, e.ID); err == nil {
				done = append(done, passed{tc.name, e, runs})
			}
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("executortest: %s: %w", tc.name, err))
		}
	}
	for _, p := range done {
		if err := c.ran(ctx, p.effect, p.runs); err != nil {
			errs = append(errs, fmt.Errorf("executortest: %s: after every case: %w", p.name, err))
		}
	}
	return errors.Join(errs...)
}

type checker struct {
	Fixture
	x harness.Executor
}

func (c checker) bind(ctx context.Context, e harness.Effect) error {
	job, err := c.x.Bind(ctx, e)
	if err != nil {
		return err
	}
	if err := c.rebind(ctx, e, job.ID); err != nil {
		return err
	}
	return c.expect(ctx, e, c.inspect, harness.JobPending, 0)
}

func (c checker) changedCommand(ctx context.Context, e harness.Effect) error {
	if _, err := c.x.Bind(ctx, e); err != nil {
		return err
	}
	changed := harness.Effect{ID: e.ID, Command: c.Effect("changed-command-other").Command}
	if _, err := c.x.Bind(ctx, changed); err == nil {
		return errors.New("binding a changed command under the same effect succeeded")
	}
	return nil
}

func (c checker) startOnce(ctx context.Context, e harness.Effect) error {
	bound, err := c.x.Bind(ctx, e)
	if err != nil {
		return err
	}
	if err := c.start(ctx, e, bound.ID); err != nil {
		return err
	}
	settled, err := c.settle(ctx, e)
	if err != nil {
		return err
	}
	if err := c.rebind(ctx, e, bound.ID); err != nil {
		return err
	}
	if err := c.start(ctx, e, bound.ID); err != nil {
		return err
	}
	if err := c.x.Cancel(ctx, e.ID); err != nil {
		return err
	}
	return c.expect(ctx, e, c.inspect, settled.Status, 1)
}

func (c checker) unboundStart(ctx context.Context, e harness.Effect) error {
	if _, err := c.x.Start(ctx, e.ID); err == nil {
		return errors.New("starting an unbound effect succeeded")
	}
	return c.absent(ctx, e)
}

func (c checker) fenceBound(ctx context.Context, e harness.Effect) error {
	bound, err := c.x.Bind(ctx, e)
	if err != nil {
		return err
	}
	for range 2 {
		if err := c.x.Cancel(ctx, e.ID); err != nil {
			return err
		}
	}
	if err := c.rebind(ctx, e, bound.ID); err != nil {
		return err
	}
	if err := c.start(ctx, e, bound.ID); err != nil {
		return err
	}
	return c.expect(ctx, e, c.inspect, harness.JobCancelled, 0)
}

func (c checker) fenceUnbound(ctx context.Context, e harness.Effect) error {
	if err := c.x.Cancel(ctx, e.ID); err != nil {
		return err
	}
	if err := c.absent(ctx, e); err != nil {
		return err
	}
	bound, err := c.x.Bind(ctx, e)
	if err != nil {
		return err
	}
	if err := c.start(ctx, e, bound.ID); err != nil {
		return err
	}
	return c.expect(ctx, e, c.inspect, harness.JobCancelled, 0)
}

func (c checker) concurrentStart(ctx context.Context, e harness.Effect) error {
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() {
			bound, err := c.x.Bind(ctx, e)
			if err == nil {
				err = c.start(ctx, e, bound.ID)
			}
			errs[i] = err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if _, err := c.settle(ctx, e); err != nil {
		return err
	}
	return c.ran(ctx, e, 1)
}

func (c checker) startCancel(ctx context.Context, e harness.Effect) error {
	bound, err := c.x.Bind(ctx, e)
	if err != nil {
		return err
	}
	started := make(chan error, 1)
	go func() { started <- c.start(ctx, e, bound.ID) }()
	time.Sleep(5 * time.Millisecond)
	var cancelled harness.Job
	var runs int
	if err = c.x.Cancel(ctx, e.ID); err == nil {
		cancelled, err = c.settle(ctx, e)
	}
	if err == nil {
		runs, err = c.Runs(ctx, e.ID)
	}
	if err := errors.Join(err, <-started); err != nil {
		return err
	}
	return c.expect(ctx, e, c.inspect, cancelled.Status, runs)
}

func (c checker) rebind(ctx context.Context, e harness.Effect, jobID string) error {
	job, err := c.x.Bind(ctx, e)
	if err == nil && job.ID != jobID {
		err = fmt.Errorf("binding again returned job %q, want %q", job.ID, jobID)
	}
	return err
}

func (c checker) start(ctx context.Context, e harness.Effect, jobID string) error {
	job, err := c.x.Start(ctx, e.ID)
	if err == nil && job.ID != jobID {
		err = fmt.Errorf("start reported job %q, want bound job %q", job.ID, jobID)
	}
	return err
}

func (c checker) inspect(ctx context.Context, e harness.Effect) (harness.Job, error) {
	job, found, err := c.x.Inspect(ctx, e.ID)
	if err == nil && !found {
		err = errors.New("inspect found no job for a bound effect")
	}
	return job, err
}

func (c checker) settle(ctx context.Context, e harness.Effect) (harness.Job, error) {
	for {
		job, err := c.inspect(ctx, e)
		if err != nil || terminal(job.Status) {
			return job, err
		}
		select {
		case <-ctx.Done():
			return job, fmt.Errorf("job is still %q: %w", job.Status, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func terminal(status harness.JobStatus) bool {
	return status == harness.JobSucceeded || status == harness.JobFailed || status == harness.JobCancelled
}

func (c checker) expect(ctx context.Context, e harness.Effect, observe func(context.Context, harness.Effect) (harness.Job, error), status harness.JobStatus, runs int) error {
	job, err := observe(ctx, e)
	if err != nil {
		return err
	}
	if job.Status != status {
		return fmt.Errorf("job is %q, want %q", job.Status, status)
	}
	return c.ran(ctx, e, runs)
}

func (c checker) absent(ctx context.Context, e harness.Effect) error {
	_, found, err := c.x.Inspect(ctx, e.ID)
	if err == nil && found {
		err = errors.New("inspect found a job for an effect that was never bound")
	}
	return err
}

func (c checker) ran(ctx context.Context, e harness.Effect, want int) error {
	runs, err := c.Runs(ctx, e.ID)
	if err != nil {
		return err
	}
	if runs != want {
		return fmt.Errorf("command ran %d times, want %d", runs, want)
	}
	return nil
}

type fresh func(context.Context) (harness.Executor, error)

func (open fresh) Bind(ctx context.Context, effect harness.Effect) (harness.Job, error) {
	x, err := open(ctx)
	if err != nil {
		return harness.Job{}, err
	}
	return x.Bind(ctx, effect)
}

func (open fresh) Start(ctx context.Context, effectID string) (harness.Job, error) {
	x, err := open(ctx)
	if err != nil {
		return harness.Job{}, err
	}
	return x.Start(ctx, effectID)
}

func (open fresh) Inspect(ctx context.Context, effectID string) (harness.Job, bool, error) {
	x, err := open(ctx)
	if err != nil {
		return harness.Job{}, false, err
	}
	return x.Inspect(ctx, effectID)
}

func (open fresh) Cancel(ctx context.Context, effectID string) error {
	x, err := open(ctx)
	if err != nil {
		return err
	}
	return x.Cancel(ctx, effectID)
}
