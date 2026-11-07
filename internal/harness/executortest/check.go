// Package executortest checks the resumable harness.Executor contract.
package executortest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// Fixture opens fresh controllers sharing only durable backend state. Effect
// returns a fresh accepted descriptor for each name; Runs counts actual command
// launches. The fixture releases resources after Check. Crash and publication
// fault injection remain concrete-adapter tests.
type Fixture struct {
	Open   func(context.Context) (harness.Executor, error)
	Effect func(string) harness.Effect
	Runs   func(context.Context, string) (int, error)
}

// Check exercises restart, identity, fencing, authoritative terminal evidence,
// cancellation and concurrency across the exact production seam. ctx bounds all
// operations. Physical effect counters detect a repeat even after a case ends.
func Check(ctx context.Context, f Fixture) error {
	c := checker{Fixture: f}
	var errs []error
	type result struct {
		effect harness.Effect
		runs   int
	}
	var done []result
	for _, tc := range []struct {
		name string
		run  func(context.Context, harness.Effect) error
	}{
		{"inspect-absent", c.absent}, {"changed-identity", c.changed},
		{"drive-once", c.once}, {"fence-unbound", c.fenced},
		{"cancelled-context", c.cancelledContext}, {"concurrent-drive", c.concurrent},
		{"drive-cancel", c.race},
	} {
		e := f.Effect(tc.name)
		err := tc.run(ctx, e)
		if err == nil {
			n, countErr := f.Runs(ctx, e.ID)
			err = countErr
			if err == nil {
				done = append(done, result{e, n})
			}
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("executortest: %s: %w", tc.name, err))
		}
	}
	for _, p := range done {
		if err := c.ran(ctx, p.effect, p.runs); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type checker struct{ Fixture }

func (c checker) raw(ctx context.Context, e harness.Effect, op string) (harness.Job, error) {
	x, err := c.Open(ctx)
	if err != nil {
		return harness.Job{}, err
	}
	var j harness.Job
	switch op {
	case "drive":
		j, err = x.Drive(ctx, e)
	case "inspect":
		j, err = x.Inspect(ctx, e)
	case "cancel":
		j, err = x.Cancel(ctx, e)
	}
	return j, err
}
func (c checker) call(ctx context.Context, e harness.Effect, op string) (harness.Job, error) {
	j, err := c.raw(ctx, e, op)
	if err == nil {
		err = j.Validate(e)
	}
	return j, err
}
func (c checker) ran(ctx context.Context, e harness.Effect, want int) error {
	n, err := c.Runs(ctx, e.ID)
	if err == nil && n != want {
		err = fmt.Errorf("effect %s ran %d times, want %d", e.ID, n, want)
	}
	return err
}
func (c checker) absent(ctx context.Context, e harness.Effect) error {
	for range 2 {
		j, err := c.call(ctx, e, "inspect")
		if err != nil {
			return err
		}
		if j.Status != harness.JobAbsent || j.ID != "" {
			return fmt.Errorf("unbound inspection = %+v", j)
		}
	}
	return c.ran(ctx, e, 0)
}
func (c checker) changed(ctx context.Context, e harness.Effect) error {
	if _, err := c.call(ctx, e, "drive"); err != nil {
		return err
	}
	final, err := c.settle(ctx, e)
	if err != nil {
		return err
	}
	before, err := c.Runs(ctx, e.ID)
	if err != nil {
		return err
	}
	for _, op := range []string{"drive", "inspect", "cancel"} {
		x, err := c.Open(ctx)
		if err != nil {
			return err
		}
		changed := e
		changed.Command += " changed"
		changed.Spec = x.Specification(changed.Command)
		if _, err = c.raw(ctx, changed, op); err == nil {
			return fmt.Errorf("%s accepted a changed command", op)
		}
		changed = e
		changed.Spec = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		if changed.Spec == e.Spec {
			changed.Spec = "0000000000000000000000000000000000000000000000000000000000000000"
		}
		if _, err = c.raw(ctx, changed, op); err == nil {
			return fmt.Errorf("%s accepted a foreign specification", op)
		}
	}
	if err = c.ran(ctx, e, before); err != nil {
		return err
	}
	return c.stable(ctx, e, final)
}
func terminal(s harness.JobStatus) bool {
	return s == harness.JobSucceeded || s == harness.JobFailed || s == harness.JobCancelled || s == harness.JobFailedToStart
}
func (c checker) settle(ctx context.Context, e harness.Effect) (harness.Job, error) {
	for {
		j, err := c.call(ctx, e, "inspect")
		if err != nil || terminal(j.Status) {
			return j, err
		}
		if j.Status == harness.JobAbsent || j.Status == harness.JobUnknown {
			return j, fmt.Errorf("effect did not settle: %s", j.Status)
		}
		select {
		case <-ctx.Done():
			return j, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (c checker) stable(ctx context.Context, e harness.Effect, want harness.Job) error {
	for _, op := range []string{"drive", "inspect", "cancel", "inspect", "drive"} {
		j, err := c.call(ctx, e, op)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(j, want) {
			return fmt.Errorf("%s changed retained terminal receipt: got %+v, want %+v", op, j, want)
		}
	}
	return nil
}
func (c checker) once(ctx context.Context, e harness.Effect) error {
	if _, err := c.call(ctx, e, "drive"); err != nil {
		return err
	}
	final, err := c.settle(ctx, e)
	if err != nil {
		return err
	}
	if err = c.stable(ctx, e, final); err != nil {
		return err
	}
	want := 1
	if final.Status == harness.JobFailedToStart {
		want = 0
	}
	return c.ran(ctx, e, want)
}
func (c checker) fenced(ctx context.Context, e harness.Effect) error {
	j, err := c.call(ctx, e, "cancel")
	if err != nil {
		return err
	}
	if j.Status != harness.JobCancelled || !j.Fenced || j.ID != "" {
		return fmt.Errorf("unbound fence = %+v", j)
	}
	if err = c.stable(ctx, e, j); err != nil {
		return err
	}
	return c.ran(ctx, e, 0)
}
func (c checker) cancelledContext(ctx context.Context, e harness.Effect) error {
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := c.call(cancelled, e, "drive"); err == nil {
		return errors.New("cancelled Drive succeeded")
	}
	return c.absent(ctx, e)
}
func (c checker) concurrent(ctx context.Context, e harness.Effect) error {
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() { _, errs[i] = c.call(ctx, e, "drive") })
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
func (c checker) race(ctx context.Context, e harness.Effect) error {
	driven := make(chan error, 1)
	go func() { _, err := c.call(ctx, e, "drive"); driven <- err }()
	_, cancelErr := c.call(ctx, e, "cancel")
	if err := errors.Join(cancelErr, <-driven); err != nil {
		return err
	}
	j, err := c.settle(ctx, e)
	if err != nil {
		return err
	}
	if err = c.stable(ctx, e, j); err != nil {
		return err
	}
	n, err := c.Runs(ctx, e.ID)
	if err == nil && (n < 0 || n > 1) {
		err = fmt.Errorf("racing effect ran %d times", n)
	}
	return err
}
