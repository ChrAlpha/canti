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

// Fixture is the executor under test and a way to observe executions. Effect
// must return a fresh effect with a distinct command for each name, and Runs
// must report how many times the command of an effect has executed.
type Fixture struct {
	Executor harness.Executor
	Effect   func(name string) harness.Effect
	Runs     func(ctx context.Context, effectID string) (int, error)
}

// Check runs every contract case against f and returns the violations
// joined, or nil. ctx bounds the whole check, including waits for started
// jobs to finish.
func Check(ctx context.Context, f Fixture) error {
	c := checker{ctx: ctx, Fixture: f}
	var errs []error
	for _, tc := range []struct {
		name  string
		check func(harness.Effect) error
	}{
		{"bind", c.bind},
		{"changed-command", c.changedCommand},
		{"start-once", c.startOnce},
		{"unbound-start", c.unboundStart},
		{"fence-bound", c.fenceBound},
		{"fence-unbound", c.fenceUnbound},
		{"concurrent-start", c.concurrentStart},
	} {
		if err := tc.check(f.Effect(tc.name)); err != nil {
			errs = append(errs, fmt.Errorf("executortest: %s: %w", tc.name, err))
		}
	}
	return errors.Join(errs...)
}

type checker struct {
	ctx context.Context
	Fixture
}

func (c checker) bind(e harness.Effect) error {
	first, err := c.Executor.Bind(c.ctx, e)
	if err != nil {
		return err
	}
	again, err := c.Executor.Bind(c.ctx, e)
	if err != nil {
		return err
	}
	if first.ID == "" || again.ID != first.ID {
		return fmt.Errorf("binding twice returned jobs %q and %q", first.ID, again.ID)
	}
	return c.expect(e, c.inspect, harness.JobPending, 0)
}

func (c checker) changedCommand(e harness.Effect) error {
	if _, err := c.Executor.Bind(c.ctx, e); err != nil {
		return err
	}
	changed := harness.Effect{ID: e.ID, Command: c.Effect("changed-command-other").Command}
	if _, err := c.Executor.Bind(c.ctx, changed); err == nil {
		return errors.New("binding a changed command under the same effect succeeded")
	}
	return nil
}

func (c checker) startOnce(e harness.Effect) error {
	bound, err := c.Executor.Bind(c.ctx, e)
	if err != nil {
		return err
	}
	if err := c.start(e, bound.ID); err != nil {
		return err
	}
	finished, err := c.settle(e)
	if err != nil {
		return err
	}
	if err := c.start(e, bound.ID); err != nil {
		return err
	}
	if err := c.Executor.Cancel(c.ctx, e.ID); err != nil {
		return err
	}
	return c.expect(e, c.settle, finished.Status, 1)
}

func (c checker) unboundStart(e harness.Effect) error {
	if _, err := c.Executor.Start(c.ctx, e.ID); err == nil {
		return errors.New("starting an unbound effect succeeded")
	}
	return c.absent(e)
}

func (c checker) fenceBound(e harness.Effect) error {
	bound, err := c.Executor.Bind(c.ctx, e)
	if err != nil {
		return err
	}
	for range 2 {
		if err := c.Executor.Cancel(c.ctx, e.ID); err != nil {
			return err
		}
	}
	if err := c.start(e, bound.ID); err != nil {
		return err
	}
	return c.expect(e, c.inspect, harness.JobCancelled, 0)
}

func (c checker) fenceUnbound(e harness.Effect) error {
	if err := c.Executor.Cancel(c.ctx, e.ID); err != nil {
		return err
	}
	if err := c.absent(e); err != nil {
		return err
	}
	bound, err := c.Executor.Bind(c.ctx, e)
	if err != nil {
		return err
	}
	if err := c.start(e, bound.ID); err != nil {
		return err
	}
	return c.expect(e, c.inspect, harness.JobCancelled, 0)
}

func (c checker) concurrentStart(e harness.Effect) error {
	bound, err := c.Executor.Bind(c.ctx, e)
	if err != nil {
		return err
	}
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = c.start(e, bound.ID) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if _, err := c.settle(e); err != nil {
		return err
	}
	return c.ran(e, 1)
}

func (c checker) start(e harness.Effect, jobID string) error {
	job, err := c.Executor.Start(c.ctx, e.ID)
	if err == nil && job.ID != jobID {
		err = fmt.Errorf("start reported job %q, want bound job %q", job.ID, jobID)
	}
	return err
}

func (c checker) inspect(e harness.Effect) (harness.Job, error) {
	job, found, err := c.Executor.Inspect(c.ctx, e.ID)
	if err == nil && !found {
		err = errors.New("inspect found no job for a bound effect")
	}
	return job, err
}

func (c checker) settle(e harness.Effect) (harness.Job, error) {
	for {
		job, err := c.inspect(e)
		if err != nil || (job.Status != harness.JobPending && job.Status != harness.JobRunning) {
			return job, err
		}
		select {
		case <-c.ctx.Done():
			return job, c.ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c checker) expect(e harness.Effect, observe func(harness.Effect) (harness.Job, error), status harness.JobStatus, runs int) error {
	job, err := observe(e)
	if err != nil {
		return err
	}
	if job.Status != status {
		return fmt.Errorf("job is %q, want %q", job.Status, status)
	}
	return c.ran(e, runs)
}

func (c checker) absent(e harness.Effect) error {
	if _, found, err := c.Executor.Inspect(c.ctx, e.ID); err != nil || found {
		return fmt.Errorf("inspect = found %t, error %v; want no job", found, err)
	}
	return c.ran(e, 0)
}

func (c checker) ran(e harness.Effect, want int) error {
	runs, err := c.Runs(c.ctx, e.ID)
	if err != nil {
		return err
	}
	if runs != want {
		return fmt.Errorf("command ran %d times, want %d", runs, want)
	}
	return nil
}
