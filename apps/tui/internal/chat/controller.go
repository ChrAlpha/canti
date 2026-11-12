// Package chat owns the runtime process and one chat's submissions.
// Presenters receive protocol projections; rendering never drives lifecycle.
package chat

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

const (
	OK         = 0
	Error      = 1
	Usage      = 2
	Stopped    = 3
	Resolution = 4
	Active     = 5
	Locked     = 6
)

type Config struct {
	Workspace, State, RuntimePath, HostMode string
	Fresh                                   bool
	HostWait                                time.Duration
	Spec                                    hostrpc.Spec
	Constraints                             string
	ExplicitConstraints                     bool
	Binding                                 hostrpc.Binding
	Environment                             []string
	LogLevel                                slog.Level
}

// Snapshot names one immutable submission, including retries after a lost ACK.
type Snapshot struct{ InputID, Goal string }

func NewSnapshot(goal string) Snapshot { return Snapshot{"in_" + strings.ToLower(rand.Text()), goal} }

type Result struct {
	Run                                                     hostrpc.Run
	Err                                                     error
	Code                                                    int
	Rejected, Uncertain, NoInputFits, Detached, Interrupted bool
}

func result(r hostrpc.Run, err error) Result {
	out := Result{Run: r, Err: err, Code: Error}
	if err != nil {
		return out
	}
	switch r.Status {
	case hostrpc.StatusReturned:
		out.Code = OK
	case hostrpc.StatusStopped:
		out.Code = Stopped
	case hostrpc.StatusNeedsResolution:
		out.Code = Resolution
	default:
		out.Code = Active
		if r.DriveError != "" {
			out.Code = Error
			out.Err = errors.New(r.DriveError)
		}
	}
	return out
}

// Controller serializes turns. Runtime process fields are used only by Setup
// or its single active worker. UI drafts are never retained here.
type Controller struct {
	cfg           Config
	runtime       runtimeClient
	GoalLimit     int
	mu            sync.Mutex
	current       *Turn
	uncertain     *Snapshot
	uncertainStop bool
	fresh         bool
}

func New(cfg Config) *Controller {
	return &Controller{cfg: cfg, fresh: cfg.Fresh, runtime: runtimeClient{workspace: cfg.Workspace, state: cfg.State, runtimePath: cfg.RuntimePath, hostMode: cfg.HostMode, hostWait: cfg.HostWait, env: cfg.Environment, logLevel: cfg.LogLevel.String()}}
}
func (c *Controller) Owned() bool { return c.cfg.HostMode == Owned }
func (c *Controller) submission(s Snapshot, delivery string, fresh bool) hostrpc.Submission {
	return hostrpc.Submission{InputID: s.InputID, Goal: s.Goal, Constraints: c.cfg.Constraints,
		ExplicitConstraints: c.cfg.ExplicitConstraints, Delivery: delivery, Fresh: fresh, Spec: c.cfg.Spec}
}
func (c *Controller) Setup(ctx, shutdown context.Context) error {
	if err := c.runtime.checkHostMode(); err != nil {
		return hostrpc.SpecError{Err: err}
	}
	v, err := c.runtime.validate(ctx, c.submission(Snapshot{Goal: "chat"}, "", c.cfg.Fresh), c.cfg.Binding)
	if err != nil {
		return err
	}
	c.cfg.Spec = v.Spec
	c.cfg.State, c.runtime.state = v.State, v.State
	c.GoalLimit = v.GoalLimit
	_, err = c.runtime.connect(ctx)
	if c.Owned() {
		c.runtime.closeHost(shutdown)
	}
	if err == nil {
		err = ctx.Err()
	}
	return err
}
func (c *Controller) Check(goal string) error {
	switch {
	case strings.TrimSpace(goal) == "":
		return errors.New("input is required")
	case !utf8.ValidString(goal):
		return errors.New("input and -constraints must be valid UTF-8")
	case len(goal) > c.GoalLimit:
		return fmt.Errorf("input must be at most %d bytes; put a long specification in a workspace file and reference it from the goal", c.GoalLimit)
	}
	return nil
}

// Start admits no queue: the presenter must explicitly submit after completion.
func (c *Controller) Start(ctx context.Context, s Snapshot) (*Turn, error) {
	if err := c.Check(s.Goal); err != nil {
		return nil, err
	}
	if s.InputID == "" {
		return nil, errors.New("submission requires an InputID")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil {
		return nil, errors.New("a run is still active")
	}
	if c.uncertain != nil && *c.uncertain != s {
		return nil, errors.New("reconcile the pending InputID before submitting new input")
	}
	ctx, cancel := context.WithCancel(ctx)
	t := newTurn(s, c.Owned(), cancel)
	t.reconcile = c.uncertain != nil
	if c.uncertainStop {
		t.requestStop()
	}
	c.current = t
	go func() {
		out := c.run(ctx, t)
		if c.Owned() {
			c.runtime.closeHost(ctx)
		}
		if ctx.Err() != nil {
			out.Interrupted = true
			if c.Owned() {
				out.Code = Error
				if out.Err == nil {
					out.Err = fmt.Errorf("owned runtime cleanup interrupted: %w", ctx.Err())
				}
			}
		}
		cancel()
		c.mu.Lock()
		c.current = nil
		if out.Uncertain {
			saved := s
			c.uncertain = &saved
			c.uncertainStop = t.stopRequested()
		} else {
			c.uncertain = nil
			c.uncertainStop = false
			c.fresh = false
		}
		c.mu.Unlock()
		t.finish(out)
	}()
	return t, nil
}

// Follow attaches to a run an input already opened, when no follower owns
// the controller. It reports through the same progress and result path as a
// submitted run.
func (c *Controller) Follow(ctx context.Context, s Snapshot, ack hostrpc.Ack) (*Turn, error) {
	if ack.Run == nil {
		return nil, errors.New("the input opened no run to follow")
	}
	c.mu.Lock()
	if c.current != nil {
		c.mu.Unlock()
		return nil, errors.New("a run is still active")
	}
	ctx, cancel := context.WithCancel(ctx)
	t := newTurn(s, c.Owned(), cancel)
	t.accept(ack)
	c.current = t
	c.mu.Unlock()
	go func() {
		var out Result
		client, err := c.runtime.connect(ctx)
		if err != nil {
			out = Result{Err: err, Code: Error}
		} else {
			out = c.accepted(ctx, client, t, ack, false)
		}
		if c.Owned() {
			c.runtime.closeHost(ctx)
		}
		if ctx.Err() != nil {
			out.Interrupted = true
			if c.Owned() {
				out.Code = Error
				if out.Err == nil {
					out.Err = fmt.Errorf("owned runtime cleanup interrupted: %w", ctx.Err())
				}
			}
		}
		cancel()
		c.mu.Lock()
		c.current = nil
		c.mu.Unlock()
		t.finish(out)
	}()
	return t, nil
}

func (c *Controller) run(ctx context.Context, t *Turn) Result {
	c.mu.Lock()
	fresh := c.fresh
	c.mu.Unlock()
	sub := c.submission(t.Snapshot, "", fresh)
	var ack hostrpc.Ack
	var client *hostrpc.Client
	lost := t.reconcile
	err := c.runtime.retry(ctx, func(cl *hostrpc.Client) (err error) {
		client = cl
		ack, err = cl.Submit(ctx, sub, c.cfg.Binding, false)
		lost = lost || err != nil && !hostrpc.Unavailable(err) && !nonAdmission(err)
		if err != nil && (hostrpc.Lost(err) || hostrpc.Unavailable(err)) && c.cfg.LogLevel <= slog.LevelWarn {
			t.diagnostic(fmt.Sprintf("Host connection interrupted; reconciling input %s", t.Snapshot.InputID))
		}
		return err
	})
	if err != nil {
		out := Result{Err: err, Code: Error}
		out.Rejected = nonAdmission(err)
		out.Uncertain = !out.Rejected
		if out.Rejected && errors.As(err, new(hostrpc.SpecError)) {
			out.Code = Usage
			_, fitErr := c.runtime.validate(ctx, c.submission(Snapshot{Goal: "chat"}, "", c.cfg.Fresh), c.cfg.Binding)
			out.NoInputFits = errors.As(fitErr, new(hostrpc.SpecError))
		}
		return out
	}
	t.accept(ack)
	return c.accepted(ctx, client, t, ack, lost)
}

// A refused attempt proves nothing about an earlier ambiguous one. CodeFailed
// and malformed replies may follow durable admission and must keep the ID.
// nonAdmission reports an error the host produced after its replay lookup,
// which proves the submission was not admitted. Transport, closing and
// protocol errors never prove that.
func nonAdmission(err error) bool {
	return errors.As(err, new(hostrpc.SpecError)) || errors.Is(err, hostrpc.ErrConflict) || errors.Is(err, hostrpc.ErrHeld)
}
func (c *Controller) accepted(ctx context.Context, client *hostrpc.Client, t *Turn, ack hostrpc.Ack, reconcile bool) Result {
	if ack.Run == nil {
		return Result{Err: errors.New("the input opened no run; the agent is working, so steer it from an active chat"), Code: Error}
	}
	r := *ack.Run
	id := r.RunID
	cursor := ackCursor(ack)
	if t.takeStop() {
		return c.stop(ctx, client, t, id, cursor)
	}
	var err error
	if ack.Replayed && reconcile && r.Status == hostrpc.StatusActive && r.Driver != hostrpc.DriverDriving {
		_, err = client.Drive(ctx, id, c.cfg.Binding, false)
	}
	if t.takeStop() {
		return c.stop(ctx, client, t, id, cursor)
	}
	if err != nil && !errors.Is(err, hostrpc.ErrDriving) {
		return result(r, err)
	}
	return c.follow(ctx, client, t, id, cursor)
}

// ackCursor is where a chat follows the agent log: before the opening input
// of a new submission, so the input and every committed fact are shown, and
// at the run's position when an earlier invocation admitted it.
func ackCursor(ack hostrpc.Ack) string {
	if ack.Replayed && ack.Run != nil {
		return ack.Run.Cursor
	}
	if ack.Pos > 0 {
		return "pos:" + itoa(max(0, ack.Pos-1))
	}
	if ack.Run != nil {
		return ack.Run.Cursor
	}
	return ""
}

func itoa(n int64) string { return fmt.Sprint(n) }

// SubmitOutcome is the answer to a steer or follow-up submission. An
// acceptance is uncertain when an attempt may have reached the host but its
// answer did not come back; the snapshot's InputID must then be reconciled.
type SubmitOutcome struct {
	Ack       hostrpc.Ack
	Err       error
	Uncertain bool
	Rejected  bool
}

// Submit admits one steer or follow-up while a turn is active. It only
// acknowledges; the running turn's follower shows its progress.
func (c *Controller) Submit(ctx context.Context, s Snapshot, delivery string) SubmitOutcome {
	if err := c.Check(s.Goal); err != nil {
		return SubmitOutcome{Err: err, Rejected: true}
	}
	if s.InputID == "" {
		return SubmitOutcome{Err: errors.New("submission requires an InputID"), Rejected: true}
	}
	var ack hostrpc.Ack
	err := c.runtime.retry(ctx, func(cl *hostrpc.Client) (err error) {
		ack, err = cl.Submit(ctx, c.submission(s, delivery, false), c.cfg.Binding, false)
		return err
	})
	out := SubmitOutcome{Ack: ack, Err: err}
	out.Rejected = err != nil && nonAdmission(err)
	out.Uncertain = err != nil && !out.Rejected
	return out
}

// CancelEffect cancels the accepted effect of run id without stopping it.
func (c *Controller) CancelEffect(ctx context.Context, id string) (hostrpc.Run, error) {
	var run hostrpc.Run
	err := c.runtime.retry(ctx, func(cl *hostrpc.Client) (err error) {
		run, err = cl.CancelEffect(ctx, id)
		return err
	})
	return run, err
}

// Withdraw takes back every held input and returns them in position order.
func (c *Controller) Withdraw(ctx context.Context) ([]hostrpc.Input, error) {
	var in []hostrpc.Input
	err := c.runtime.retry(ctx, func(cl *hostrpc.Client) (err error) {
		in, err = cl.Withdraw(ctx, "")
		return err
	})
	return in, err
}

// rpcRun is the external seam for following and stopping the agent log.
type rpcRun interface {
	Progress(context.Context, string, int) (hostrpc.Page, error)
	Wait(context.Context, string) (hostrpc.Wait, error)
	View(context.Context, string) (hostrpc.Run, error)
	Stop(context.Context, string, time.Duration) (hostrpc.Run, error)
}

func (c *Controller) follow(ctx context.Context, client rpcRun, t *Turn, id, cursor string) Result {
	followCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan followed, 1)
	go func() { finished <- follow(followCtx, client, cursor, t.progress) }()
	select {
	case out := <-finished:
		if ctx.Err() != nil {
			return c.detached(id, ctx.Err())
		}
		select {
		case <-t.stop:
			return c.stop(ctx, client, t, id, out.Cursor)
		default:
			return out.Result
		}
	case <-t.stop:
		// Stop first interrupts the host driver. Join our follow producer before
		// issuing it, then await its own acknowledgement before final drain/View.
		cancel()
		out := <-finished
		return c.stop(ctx, client, t, id, out.Cursor)
	case <-ctx.Done():
		cancel()
		<-finished
		return c.detached(id, ctx.Err())
	}
}
func (c *Controller) stop(ctx context.Context, client rpcRun, t *Turn, id, cursor string) Result {
	if current := t.RunID(); current != "" {
		id = current
	}
	if _, err := client.Stop(ctx, id, stopCleanup); err != nil {
		if ctx.Err() != nil {
			return c.detached(id, ctx.Err())
		}
		return result(hostrpc.Run{RunID: id}, fmt.Errorf("stopping run %s: %w; finish with \"canti stop -run %[1]s\"", id, err))
	}
	return drain(ctx, client, cursor, t.progress)
}
func drain(ctx context.Context, src rpcRun, cursor string, emit func(hostrpc.Item)) Result {
	for {
		page, err := src.Progress(ctx, cursor, 64)
		if err != nil {
			return result(hostrpc.Run{}, err)
		}
		for _, item := range page.Items {
			emit(item)
		}
		cursor = page.Next
		if !page.More {
			r, err := src.View(ctx, "")
			return result(r, err)
		}
	}
}

func (c *Controller) detached(id string, err error) Result {
	if c.Owned() {
		return Result{Run: hostrpc.Run{RunID: id}, Err: fmt.Errorf("abandoned cleanup of run %s: %w; finish with \"canti stop -run %[1]s\"", id, err), Code: Error}
	}
	return Result{Run: hostrpc.Run{RunID: id}, Code: Active, Detached: true}
}

// follow is the only Progress/Wait producer. A final page is drained before
// View and completion, including commits that raced a terminal Wait response.
type followed struct {
	Result
	Cursor string
}

func follow(ctx context.Context, src rpcRun, cursor string, emit func(hostrpc.Item)) (out followed) {
	defer func() { out.Cursor = cursor }()
	final := false
	for {
		page, err := src.Progress(ctx, cursor, 64)
		if err != nil {
			return followed{Result: result(hostrpc.Run{}, fmt.Errorf("reading the agent log: %w; continue with \"canti resume\"", err))}
		}
		for _, item := range page.Items {
			emit(item)
		}
		cursor = page.Next
		if page.More {
			continue
		}
		// A final page is drained before the outcome, including rows that
		// raced a terminal Wait answer.
		if page.Quiescent || final {
			r, err := src.View(ctx, "")
			return followed{Result: result(r, err)}
		}
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		w, err := src.Wait(waitCtx, cursor)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			// A bounded wake-up keeps the chat responsive when the host owns
			// work that produces no further row.
			if r, viewErr := src.View(ctx, ""); viewErr == nil && (r.Status != hostrpc.StatusActive || r.Driver != hostrpc.DriverDriving) {
				return followed{Result: result(r, nil)}
			}
			continue
		}
		if err != nil {
			return followed{Result: result(hostrpc.Run{}, fmt.Errorf("the host went away while it drove the agent: %w; continue with \"canti resume -host auto\"", err))}
		}
		final = w.Driver != hostrpc.DriverDriving
		// Wait is a wake-up: the journal, read from the last row this follower
		// consumed, decides what follows, so rows committed while it waited
		// are not skipped.
	}
}
