package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/internal/host"
	"github.com/ChrAlpha/canti/internal/hostrpc"
)

// The ways a command reaches the host of its state directory: a host in its
// own process, which lives as long as the command, or the host process that
// serves the state directory, started on demand, which keeps driving a run
// after the command goes away.
const (
	hostEmbedded = "embedded"
	hostAuto     = "auto"
)

// autoIdle is how long a host that a client started serves without clients
// or drives before it exits.
const autoIdle = 10 * time.Minute

func hostFlags(fs *flag.FlagSet, a *app) {
	fs.StringVar(&a.hostMode, "host", hostAuto, "where the run is driven: auto, by the host process of the state directory, which this command starts when none serves it and which keeps driving the run when the command ends, or embedded, in this process, which ends the run's drive with the command")
	fs.DurationVar(&a.hostWait, "host-wait", 10*time.Second, "`time` to wait for a host process to start serving")
}

func (a *app) checkHostMode() error {
	if a.hostMode != hostEmbedded && a.hostMode != hostAuto {
		return fmt.Errorf("-host must be %s or %s", hostEmbedded, hostAuto)
	}
	if a.hostWait <= 0 {
		return errors.New("-host-wait must be positive")
	}
	return nil
}

func (a *app) remoteMode() bool { return a.hostMode == hostAuto }

// checkCredential checks, before anything is created or started, that a
// run of c can call its model: with this process's keys for its API, which
// newModel builds its adapter with, or, with -host auto, with a credential
// that the host already serving the state directory holds for the exact API
// and provider that url, the API's base URL variable, or its default name.
// It starts no host to find out.
func (a *app) checkCredential(ctx context.Context, c host.RunConfig, url string) error {
	if a.cred.For(c.API) == (host.Keys{}) && a.remoteMode() && a.hostHolds(ctx, host.Pair{API: c.API, Provider: cmp.Or(url, baseURL(c.API), host.DefaultProvider(c.API))}) {
		return host.CheckModel(c)
	}
	if a.credErr != nil {
		return a.credErr
	}
	_, err := newModel(c, url, a.cred)
	return err
}

// hostHolds reports whether a host serves the state directory and holds a
// credential bound to pair p, without starting one.
func (a *app) hostHolds(ctx context.Context, p host.Pair) bool {
	path, err := hostrpc.SocketPath(a.state)
	if err != nil {
		return false
	}
	h, err := (&hostrpc.Client{Path: path}).Hello(ctx)
	if err != nil {
		return false
	}
	for _, b := range h.Bindings {
		if b.API == p.API && b.Provider == p.Provider {
			return true
		}
	}
	return false
}

// leaving reports a Hello that a host leaving the state directory refused or
// left unanswered: it answered that it is shutting down, or it accepted the
// connection, as its listener queues one, and closed it unanswered as it
// stopped serving. Hello carries no state, so it is asked again, of the next
// host once this one has gone.
func leaving(err error) bool { return errors.Is(err, host.ErrClosing) || hostrpc.Lost(err) }

// maxStarts bounds the hosts one connect starts.
const maxStarts = 4

// connect returns a client of the host that serves the state directory,
// starting one when none answers and waiting until it serves. A host of
// another protocol version is refused, never replaced: it may be driving a
// run. Whoever holds the state directory's lock without serving, such as an
// embedded command, makes connect fail as locked once it gave up waiting.
func (a *app) connect(ctx context.Context) (*hostrpc.Client, error) {
	path, err := hostrpc.SocketPath(a.state)
	if err != nil {
		return nil, err
	}
	c := &hostrpc.Client{Path: path}
	_, err = c.Hello(ctx)
	if err == nil || !hostrpc.Unavailable(err) && !leaving(err) {
		return c, err
	}
	checkpoint(pointHostWaiting)
	starts := 0
	deadline := time.Now().Add(a.hostWait)
	for {
		_, err := c.Hello(ctx)
		switch {
		case err == nil || !hostrpc.Unavailable(err) && !leaving(err):
			return c, err
		case hostrpc.Unavailable(err) && starts < maxStarts && !locked(a.state):
			// The host that held the state directory, or the one this client
			// started, has exited without serving: one that was shutting down
			// as this client arrived, or one that lost the lock to it. Start
			// the next one.
			if err := a.startHost(); err != nil {
				return nil, err
			}
			starts++
		case time.Now().After(deadline):
			if locked(a.state) {
				return nil, fmt.Errorf("workspace %s is %w and no host serves it", a.workspace, host.ErrLocked)
			}
			return nil, fmt.Errorf("no host started serving workspace %s within %v; see %s", a.workspace, a.hostWait, filepath.Join(a.state, "host.log"))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// locked reports whether a process holds the lock of state.
func locked(state string) bool {
	f, err := os.Open(filepath.Join(state, "lock"))
	if err != nil {
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

// startHost starts a host process for the state directory in a session of
// its own, so that no signal of this terminal reaches it, logging to
// host.log in the state directory. Its environment holds no credential: the
// variables were removed at start, and each client gives the key of its run
// with each drive. Hosts that start together race for the lock, and only
// its holder serves.
func (a *app) startHost() error {
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	logPath := filepath.Join(a.state, "host.log")
	if info, err := os.Stat(logPath); err == nil && info.Size() > 4<<20 {
		os.Rename(logPath, logPath+".1")
	}
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(exe, "host", "-workspace", a.workspace, "-state", a.state, "-idle", autoIdle.String(), "-log-level", "warn")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); !isCredential(name) {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()
	return nil
}

func isCredential(name string) bool {
	for _, v := range credentialVariables {
		if name == v {
			return true
		}
	}
	return false
}

// newInputID names a submission that a client makes without -input, so that
// a connection that breaks before the answer can send it again.
func newInputID() string { return "in_" + strings.ToLower(rand.Text()) }

// remoteSubmit submits sub to the host, sending the same request again, on a
// host it starts if need be, while the outcome of an attempt is lost, and
// follows the run it names.
func (a *app) remoteSubmit(ctx context.Context, sub hostrpc.Submission, b host.Binding, once bool) (int, error) {
	if sub.InputID == "" {
		sub.InputID = newInputID()
		fmt.Fprintf(a.stderr, "canti: submitting input %s; if its outcome is lost, run again with -input %[1]s to find its run\n", sub.InputID)
	}
	wire := hostrpc.BindingOf(sub.Spec.API, b)
	var ack hostrpc.Ack
	var c *hostrpc.Client
	lost := false
	err := a.retry(ctx, func(client *hostrpc.Client) (err error) {
		c = client
		ack, err = c.Submit(ctx, sub, wire, once)
		lost = lost || hostrpc.Lost(err)
		return err
	})
	if err != nil {
		return submitCode(err), err
	}
	if ack.Replayed {
		a.noteDiffers(sub.InputID, ack.Run.RunID, ack.Differs)
	}
	after, err := followFrom(ack, lost)
	if err != nil {
		return exitError, err
	}
	switch {
	case ack.Replayed && lost:
		// This invocation's own submission was admitted before its answer
		// was lost. Drive the run, once, if it is open and no host drives
		// it, and show all of it from its start, also when it has halted
		// meanwhile, since this invocation has shown none of it.
		if ack.Run.Status == hostrpc.StatusActive && ack.Run.Driver != hostrpc.DriverDriving {
			if _, err := c.Drive(ctx, ack.Run.RunID, wire, once); err != nil && !errors.Is(err, host.ErrDriving) {
				return exitError, err
			}
		}
		return a.attach(ctx, after)
	case ack.Replayed && ack.Run.Driver == hostrpc.DriverDriving:
		return a.attach(ctx, after)
	case ack.Replayed:
		return a.remoteReport(ack.Run, nil)
	}
	return a.attach(ctx, after)
}

// followFrom is where a client that submitted and got ack follows the run
// from: the start of the run when this invocation's own submission admitted
// it, also one whose answer was lost and replayed, since this invocation has
// shown none of it; the run's state when the ack replays an input that an
// earlier invocation submitted.
func followFrom(ack hostrpc.Ack, lost bool) (host.Cursor, error) {
	if !ack.Replayed || lost {
		return host.Cursor{RunID: ack.Run.RunID}, nil
	}
	return hostrpc.ParseCursor(ack.Run.Cursor)
}

// attachRun attaches to run r from the cursor of its state.
func (a *app) attachRun(ctx context.Context, r hostrpc.Run) (int, error) {
	after, err := hostrpc.ParseCursor(r.Cursor)
	if err != nil {
		return exitError, err
	}
	return a.attach(ctx, after)
}

// retry calls op on a client of the host until it gets an answer: a call
// that reached no host, or whose connection broke before the answer, is made
// again, on a host started anew if the old one went away.
func (a *app) retry(ctx context.Context, op func(*hostrpc.Client) error) error {
	var err error
	for range 5 {
		var c *hostrpc.Client
		if c, err = a.connect(ctx); err != nil {
			return err
		}
		// A request that reached no host, whose answer was lost, or that a
		// host shutting down refused without acting on it, is sent again,
		// to a host started anew if the old one went away.
		if err = op(c); err == nil || !hostrpc.Lost(err) && !hostrpc.Unavailable(err) && !errors.Is(err, host.ErrClosing) {
			return err
		}
		a.log.Warn("the host connection broke; sending the request again", "error", err)
	}
	return err
}

// attach follows run id, which the host drives, from sequence after until it
// halts or the host stops driving it. The first signal asks the host to stop the run, the host
// bounding the cleanup; the second detaches, as does a.detach: the host
// keeps driving the run.
func (a *app) attach(ctx context.Context, after host.Cursor) (int, error) {
	id := after.RunID
	c, err := a.connect(ctx)
	if err != nil {
		return exitError, err
	}
	ctl := a.stopControl()
	stopCtx, requestStop := context.WithCancel(ctx)
	detachCtx, detach := context.WithCancel(ctx)
	ctl.cancels = []context.CancelFunc{requestStop, detach}
	start, handled := make(chan int, 1), make(chan struct{})
	go func() {
		defer close(handled)
		ctl.watch(detachCtx.Done(), start)
	}()
	defer func() {
		detach()
		<-handled
		a.record(ctl.sig)
		a.record(ctl.seen)
	}()
	start <- ctl.pending()
	if a.detach != nil {
		go func() {
			select {
			case <-a.detach:
				detach()
			case <-detachCtx.Done():
			}
		}()
	}
	f := &follower{src: c, next: after, w: a.steps, done: make(chan struct{})}
	f.start()
	halted, lost := make(chan struct{}), make(chan error, 1)
	go func() {
		defer close(halted)
		for cur := after; detachCtx.Err() == nil; {
			w, err := c.Wait(detachCtx, cur)
			if err != nil || w.Halted || !w.Driving {
				lost <- err
				return
			}
			cur = w.Cursor
		}
	}()
	var stopErr error
	select {
	case <-halted:
	case <-stopCtx.Done():
		if detachCtx.Err() == nil {
			_, stopErr = c.Stop(detachCtx, id, stopCleanup)
		}
	}
	f.finish()
	if detachCtx.Err() != nil {
		fmt.Fprintf(a.stderr, "canti: detached from run %s, which the host keeps driving; follow it with \"canti resume -host auto -run %[1]s\".\n", id)
		return exitActive, nil
	}
	if stopErr != nil {
		return exitError, fmt.Errorf("stopping run %s: %w; finish with \"canti stop -host auto -run %[1]s\"", id, stopErr)
	}
	var waitErr error
	select {
	case waitErr = <-lost:
	default:
	}
	v, err := c.View(ctx, id)
	if err != nil && waitErr != nil && detachCtx.Err() == nil {
		err = fmt.Errorf("the host went away while it drove run %s: %w; continue the run with \"canti resume -host auto -run %[1]s\"", id, waitErr)
	}
	if err != nil {
		return exitError, err
	}
	return a.remoteReport(v, nil)
}

// remoteReport reports run r as report reports a snapshot: a drive that
// ended with an error is that error.
func (a *app) remoteReport(r hostrpc.Run, err error) (int, error) {
	if err == nil && r.DriveError != "" && r.Status == hostrpc.StatusActive && r.Driver != hostrpc.DriverDriving {
		err = errors.New(r.DriveError)
	}
	snap, serr := r.State()
	if serr != nil {
		return exitError, errors.Join(err, serr)
	}
	if r.Status == hostrpc.StatusActive && r.Driver != hostrpc.DriverDriving && !r.Drivable && r.Why != "" {
		fmt.Fprintf(a.stderr, "canti: the host cannot continue run %s by itself: %s\n", r.RunID, r.Why)
	}
	return a.report(snap, err, exitActive, host.RunConfig{ContextVersion: r.ContextVersion})
}

// remoteRun is with for a host process: it selects the run and reports what
// op does with it. Unsupported context versions are refused before the operation.
func (a *app) remoteRun(ctx context.Context, op func(*hostrpc.Client, hostrpc.Run) (int, error)) (int, error) {
	// A command that acts on an existing run starts no host for a state
	// directory that holds no journal, which no host can have served.
	if _, err := os.Stat(filepath.Join(a.state, "canti.db")); errors.Is(err, os.ErrNotExist) {
		return exitError, fmt.Errorf("no runs in workspace %s", a.workspace)
	}
	c, err := a.connect(ctx)
	if err != nil {
		return exitError, err
	}
	r, err := c.View(ctx, a.runID)
	if err != nil {
		return exitError, err
	}
	if r.ContextVersion != host.ContextVersion {
		return exitError, fmt.Errorf("run %s %s", r.RunID, host.Unresumable(host.RunConfig{ContextVersion: r.ContextVersion}))
	}
	return op(c, r)
}

func cmpCode(code, active int) int {
	if code == exitActive {
		return active
	}
	return code
}

// remoteResume drives run r, or attaches to it when the host already drives
// it.
func (a *app) remoteResume(ctx context.Context, c *hostrpc.Client, r hostrpc.Run, url string, once bool) (int, error) {
	if r.Status != hostrpc.StatusActive {
		return a.remoteReport(r, nil)
	}
	if a.cred.For(r.API) == (host.Keys{}) && !r.Drivable && r.Driver != hostrpc.DriverDriving {
		return exitError, fmt.Errorf("%w; the host cannot drive run %s by itself: %s", cmp.Or(a.credErr, host.ErrNoKeys), r.RunID, r.Why)
	}
	_, err := c.Drive(ctx, r.RunID, hostrpc.BindingOf(r.API, a.binding(r.API, url)), once)
	if err != nil && !errors.Is(err, host.ErrDriving) {
		return exitError, err
	}
	return a.attachRun(ctx, r)
}
