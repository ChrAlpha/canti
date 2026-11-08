// Package session owns the runtime process and one chat's submissions.
// Presenters receive protocol projections; rendering never drives lifecycle.
package session

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
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
	Workspace, State, RuntimePath, HostMode, SessionID string
	HostWait                                           time.Duration
	Spec                                               hostrpc.Spec
	Constraints                                        string
	ExplicitConstraints                                bool
	Binding                                            hostrpc.Binding
	Environment                                        []string
	Logger                                             *slog.Logger
}

// Snapshot names one immutable submission, including retries after a lost ACK.
type Snapshot struct{ InputID, Goal string }

func NewSnapshot(goal string) Snapshot { return Snapshot{"in_" + strings.ToLower(rand.Text()), goal} }

type Result struct {
	Run                                        hostrpc.Run
	Err                                        error
	Code                                       int
	Rejected, Uncertain, NoInputFits, Detached bool
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
	cfg              Config
	runtime          runtimeClient
	GoalLimit        int
	constraintsLimit int
	fixed            string
	mu               sync.Mutex
	current          *Turn
}

func New(cfg Config) *Controller {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Controller{cfg: cfg, runtime: runtimeClient{workspace: cfg.Workspace, state: cfg.State, runtimePath: cfg.RuntimePath, hostMode: cfg.HostMode, hostWait: cfg.HostWait, env: cfg.Environment, log: cfg.Logger}}
}
func (c *Controller) ID() string  { return c.cfg.SessionID }
func (c *Controller) Owned() bool { return c.cfg.HostMode == Owned }
func (c *Controller) submission(s Snapshot) hostrpc.Submission {
	return hostrpc.Submission{SessionID: c.cfg.SessionID, InputID: s.InputID, Goal: s.Goal, Constraints: c.cfg.Constraints, ExplicitConstraints: c.cfg.ExplicitConstraints, Spec: c.cfg.Spec}
}
func (c *Controller) Setup(ctx context.Context) error {
	if err := c.runtime.checkHostMode(); err != nil {
		return hostrpc.SpecError{Err: err}
	}
	v, err := c.runtime.validate(ctx, c.submission(Snapshot{Goal: "chat"}), c.cfg.Binding)
	if err != nil {
		return err
	}
	c.cfg.Spec = v.Spec
	c.fixed = v.Constraints
	c.GoalLimit = v.GoalLimit
	c.constraintsLimit = v.ConstraintsLimit
	_, err = c.runtime.connect(ctx)
	if c.Owned() {
		c.runtime.closeHost()
	}
	return err
}
func (c *Controller) Check(goal string) error {
	switch {
	case strings.TrimSpace(goal) == "":
		return errors.New("input is required")
	case !utf8.ValidString(goal):
		return errors.New("input must be valid UTF-8")
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
	ctx, cancel := context.WithCancel(ctx)
	t := newTurn(s, c.Owned(), cancel)
	c.current = t
	go func() {
		out := c.run(ctx, t)
		if c.Owned() {
			if ctx.Err() != nil && c.runtime.child != nil {
				_ = c.runtime.child.Process.Kill()
			}
			c.runtime.closeHost()
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
	sub := c.submission(t.Snapshot)
	var ack hostrpc.Ack
	var client *hostrpc.Client
	lost := false
	err := c.runtime.retry(ctx, func(cl *hostrpc.Client) (err error) {
		client = cl
		ack, err = cl.Submit(ctx, sub, c.cfg.Binding, false)
		lost = lost || hostrpc.Lost(err)
		return err
	})
	if err != nil {
		out := Result{Err: err, Code: Error, Uncertain: lost}
		if errors.As(err, new(hostrpc.SpecError)) {
			out.Code = Usage
			out.Rejected = true
			out.Uncertain = false
			_, fitErr := c.runtime.validate(ctx, c.submission(Snapshot{Goal: "chat"}), c.cfg.Binding)
			out.NoInputFits = errors.As(fitErr, new(hostrpc.SpecError))
		}
		return out
	}
	t.accept(ack)
	if ack.Replayed && lost && ack.Run.Status == hostrpc.StatusActive && ack.Run.Driver != hostrpc.DriverDriving {
		if _, err = client.Drive(ctx, ack.Run.RunID, c.cfg.Binding, false); err != nil && !errors.Is(err, hostrpc.ErrDriving) {
			return result(ack.Run, err)
		}
	}
	// The same snapshot may be explicitly retried after an earlier ambiguous
	// result. Always project its complete progress; it never admits a second run.
	return c.follow(ctx, client, t, ack.Run.RunID)
}

// rpcRun is the external seam for following and stopping a runtime run.
type rpcRun interface {
	Progress(context.Context, string, string, int) (hostrpc.Page, error)
	Wait(context.Context, string, string) (hostrpc.Wait, error)
	View(context.Context, string) (hostrpc.Run, error)
	Stop(context.Context, string, time.Duration) (hostrpc.Run, error)
}

func (c *Controller) follow(ctx context.Context, client rpcRun, t *Turn, id string) Result {
	followCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan Result, 1)
	go func() { finished <- follow(followCtx, client, id, t.progress) }()
	var stopDone <-chan error
	for {
		select {
		case out := <-finished:
			if ctx.Err() != nil {
				out = c.detached(id, ctx.Err())
			}
			if stopDone != nil { // Join the stop operation; it has the same cancel context.
				cancel()
				err := <-stopDone
				if err != nil && !errors.Is(err, context.Canceled) && out.Err == nil {
					out.Err = err
					out.Code = Error
				}
			}
			return out
		case <-t.stop:
			if stopDone == nil {
				ch := make(chan error, 1)
				stopDone = ch
				go func() { _, err := client.Stop(followCtx, id, stopCleanup); ch <- err }()
			}
		case err := <-stopDone:
			stopDone = nil
			if err != nil {
				cancel()
				<-finished
				return result(hostrpc.Run{RunID: id}, fmt.Errorf("stopping run %s: %w; finish with \"canti stop -run %[1]s\"", id, err))
			}
		case <-ctx.Done():
			cancel()
			<-finished
			if stopDone != nil {
				<-stopDone
			}
			return c.detached(id, ctx.Err())
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
func follow(ctx context.Context, src rpcRun, id string, emit func(hostrpc.Item)) Result {
	cursor := ""
	final := false
	for {
		page, err := src.Progress(ctx, id, cursor, 64)
		if err != nil {
			return result(hostrpc.Run{RunID: id}, fmt.Errorf("reading progress of run %s: %w; continue with \"canti resume -run %[1]s\"", id, err))
		}
		for _, item := range page.Items {
			emit(item)
		}
		cursor = page.Next
		if page.More {
			continue
		}
		if final || page.Halted {
			r, err := src.View(ctx, id)
			return result(r, err)
		}
		w, err := src.Wait(ctx, id, cursor)
		if err != nil {
			return result(hostrpc.Run{RunID: id}, fmt.Errorf("the host went away while it drove run %s: %w; continue with \"canti resume -host auto -run %[1]s\"", id, err))
		}
		final = w.Halted || w.Driver != hostrpc.DriverDriving
	}
}
