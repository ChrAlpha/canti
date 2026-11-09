package host

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// ErrNotQuiescent reports a shutdown whose drives did not end before its
// deadline. The host keeps its lock: the caller must end the process, which
// releases the lock only when no goroutine of the host can act any more.
var ErrNotQuiescent = errors.New("host: drives did not end before the shutdown deadline")

// DriveOptions select how far a drive takes a run: to its halt, or one step.
type DriveOptions struct{ Once bool }

// driver is one drive of a run: the controller's cancel, and its result once
// done is closed.
type driver struct {
	cancel context.CancelFunc
	done   chan struct{}
	snap   harness.Snapshot
	err    error
}

// Drive is a drive in progress.
type Drive struct{ d *driver }

// Done is closed when the drive has ended.
func (d Drive) Done() <-chan struct{} { return d.d.done }

// Result is the snapshot the drive reached and its error, once Done is
// closed: a halt, one step with Once, or the error that ended it, such as
// the cancellation of a stop or a shutdown.
func (d Drive) Result() (harness.Snapshot, error) { <-d.d.done; return d.d.snap, d.d.err }

// Drive starts driving run id, of the host's context version, and returns
// at once. The model is called at the run's provider with the keys of b when
// b resolves to that exact provider, and otherwise with the host's own
// credential bound to the run's API and provider; the drive holds them only
// while it runs. A run the host already drives is refused, and so is one it
// could not drive under its rules.
func (h *Host) Drive(ctx context.Context, id string, b Binding, o DriveOptions) (Drive, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return Drive{}, err
	}
	defer leave()
	run, c, err := h.Find(ctx, id)
	if err != nil {
		return Drive{}, err
	}
	if c.ContextVersion != ContextVersion {
		return Drive{}, fmt.Errorf("run %s %s", id, Unresumable(c))
	}
	p := c.Provider
	if err := CheckProvider(p); err != nil {
		// Validate the exact stored endpoint before using a credential.
		return Drive{}, fmt.Errorf("run %s: %w", id, err)
	}
	k, err := h.keysFor(id, c, p, b)
	if err != nil {
		return Drive{}, err
	}
	m, err := NewModel(c, p, k)
	if err != nil {
		return Drive{}, err
	}
	l, err := h.loop(run, c, m)
	if err != nil {
		return Drive{}, err
	}
	unlock, err := h.lockRun(ctx, id)
	if err != nil {
		return Drive{}, err
	}
	defer unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case h.closing:
		return Drive{}, ErrClosing
	case h.drivers[id] != nil:
		return Drive{}, fmt.Errorf("run %s: %w", id, ErrDriving)
	}
	dctx, cancel := context.WithCancel(h.life)
	d := &driver{cancel: cancel, done: make(chan struct{})}
	h.drivers[id] = d
	go h.run(dctx, d, l, id, o)
	return Drive{d}, nil
}

func (h *Host) run(ctx context.Context, d *driver, l *harness.Loop, id string, o DriveOptions) {
	step := l.Run
	if o.Once {
		step = l.Step
	}
	d.snap, d.err = step(ctx, id)
	d.cancel()
	h.mu.Lock()
	delete(h.drivers, id)
	h.ended = ended{id, d.err}
	h.mu.Unlock()
	close(d.done)
	h.notify()
}

// Driving reports whether a drive or persistent stop owns work for run id.
func (h *Host) Driving(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.workingLocked(id)
}

// interrupt ends the drive of run id, if any, as a controller shutdown that
// commits nothing, and waits until it has ended or ctx expires. A timeout
// leaves the canceled controller accounted to the Host lifetime.
func (h *Host) interrupt(ctx context.Context, id string) error {
	h.mu.Lock()
	d := h.drivers[id]
	h.mu.Unlock()
	if d != nil {
		d.cancel()
		select {
		case <-d.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Stop ends the drive of run id, if any, then records a persistent stop
// with reason cancelled and carries it out within ctx, which bounds the
// cleanup: an effect that may exist is cancelled and its true result
// recorded. It calls no model and needs no credential. Stop is an operation
// of the host: it waits
// for the run's lock only until ctx is done or the host shuts down, and a
// shutdown ends its cleanup too, leaving the run stopped or cancelling, which
// a later stop or drive finishes.
func (h *Host) Stop(ctx context.Context, id string) (snap harness.Snapshot, err error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return harness.Snapshot{}, err
	}
	defer leave()
	run, c, err := h.Find(ctx, id)
	if err != nil {
		return harness.Snapshot{}, err
	}
	id = run.ID
	unlock, err := h.lockRun(ctx, id)
	if err != nil {
		return harness.Snapshot{}, err
	}
	defer unlock()
	finish := h.beginStop(id)
	defer func() { finish(err) }()
	if err := h.interrupt(ctx, id); err != nil {
		return harness.Snapshot{}, err
	}
	l, err := h.loop(run, c, NoModel)
	if err != nil {
		return harness.Snapshot{}, err
	}
	return l.Stop(ctx, id, core.StopCancelled)
}

// beginStop keeps the run's current lock holder observable while it hands off
// from an interrupted drive to durable stop/cleanup. The prior drive remains
// separately registered until it actually ends, including after a stop timeout.
func (h *Host) beginStop(id string) func(error) {
	h.mu.Lock()
	lock := h.runs[id]
	lock.stopping = true
	h.mu.Unlock()
	h.notify()
	return func(err error) {
		h.mu.Lock()
		h.ended = ended{id, err}
		lock.stopping = false
		h.mu.Unlock()
		h.notify()
	}
}

// workingLocked reads operational ownership under h.mu, not a journal phase.
func (h *Host) workingLocked(id string) bool {
	lock := h.runs[id]
	return h.drivers[id] != nil || lock != nil && lock.stopping
}

// Resolve records the confirmed outcome obs of the unknown effect of run id.
func (h *Host) Resolve(ctx context.Context, id string, obs harness.Observation, ownerID string) (harness.Snapshot, error) {
	return h.resolve(ctx, id, func(ctx context.Context, l *harness.Loop) (harness.Snapshot, error) {
		return l.Resolve(ctx, id, obs, ownerID)
	})
}

// ResolveJob records the job the executor finds for the unknown effect of
// run id.
func (h *Host) ResolveJob(ctx context.Context, id string) (harness.Snapshot, error) {
	return h.resolve(ctx, id, func(ctx context.Context, l *harness.Loop) (harness.Snapshot, error) { return l.ResolveJob(ctx, id) })
}

func (h *Host) resolve(ctx context.Context, id string, op func(context.Context, *harness.Loop) (harness.Snapshot, error)) (harness.Snapshot, error) {
	ctx, leave, err := h.Enter(ctx)
	if err != nil {
		return harness.Snapshot{}, err
	}
	defer leave()
	unlock, err := h.lockRun(ctx, id)
	if err != nil {
		return harness.Snapshot{}, err
	}
	defer unlock()
	if h.Driving(id) {
		return harness.Snapshot{}, fmt.Errorf("run %s: %w", id, ErrDriving)
	}
	run, c, err := h.Find(ctx, id)
	if err != nil {
		return harness.Snapshot{}, err
	}
	l, err := h.loop(run, c, NoModel)
	if err != nil {
		return harness.Snapshot{}, err
	}
	return op(ctx, l)
}

func (h *Host) isClosing() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closing
}

// Enter admits an operation into the host's lifetime unless the host is
// shutting down, and returns its context and the function that ends it.
// Shutdown awaits every operation it admitted before it releases the
// journal and the lock, so an operation cannot act once another process may
// own the state directory. The context derives from ctx and also ends when
// Shutdown begins. Submissions, drives being started, stops, and
// resolutions enter by themselves; a server enters for each request it
// handles.
func (h *Host) Enter(ctx context.Context) (context.Context, func(), error) {
	h.mu.Lock()
	if h.closing || h.released {
		h.mu.Unlock()
		return nil, nil, ErrClosing
	}
	h.ops.Add(1)
	h.active++
	h.mu.Unlock()
	octx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(h.life, cancel)
	var once sync.Once
	return octx, func() {
		once.Do(func() {
			stop()
			cancel()
			h.mu.Lock()
			h.active--
			h.mu.Unlock()
			h.ops.Done()
		})
	}, nil
}

// Shutdown shuts the host down. From the moment it is called, the host
// admits no operation; it then ends the context of every drive and every
// operation, which commits nothing more than the commit it may be in, and
// waits until all of them have ended or ctx is done. Shutdown itself commits
// nothing and cancels no job; a stop whose cleanup it ends leaves its run
// cancelling. Only once nothing of the host can act does it release the
// journal and the lock; otherwise it returns ErrNotQuiescent and keeps both,
// and the caller ends the process.
func (h *Host) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.closing = true
	drivers := make([]*driver, 0, len(h.drivers))
	for _, d := range h.drivers {
		drivers = append(drivers, d)
	}
	h.mu.Unlock()
	h.checkpoint(PointClosing)
	h.endLife()
	h.notify()
	quiet := make(chan struct{})
	go func() {
		for _, d := range drivers {
			<-d.done
		}
		h.ops.Wait()
		close(quiet)
	}()
	select {
	case <-quiet:
	case <-ctx.Done():
		return ErrNotQuiescent
	}
	return h.Close()
}

// runLock serializes starts, stops, resolutions and admission's publication
// of one run. Its slot is held by one operation at a time; n counts holders
// and waiters, so the host forgets it once none remains.
type runLock struct {
	slot     chan struct{}
	n        int
	stopping bool // holder is executing Stop; guarded by Host.mu
}

// lockRun locks run id until the returned function is called. It waits only
// until ctx is done, and once it holds the lock it refuses with ErrClosing a
// host that began to shut down while it waited, so that a queued operation
// does nothing after the shutdown began.
func (h *Host) lockRun(ctx context.Context, id string) (func(), error) {
	h.mu.Lock()
	l := h.runs[id]
	if l == nil {
		l = &runLock{slot: make(chan struct{}, 1)}
		h.runs[id] = l
	}
	l.n++
	h.mu.Unlock()
	forget := func() {
		h.mu.Lock()
		if l.n--; l.n == 0 {
			delete(h.runs, id)
		}
		h.mu.Unlock()
	}
	if err := h.acquire(ctx, l.slot); err != nil {
		forget()
		return nil, err
	}
	return func() {
		<-l.slot
		forget()
	}, nil
}

// acquire takes slot, a channel of capacity one, waiting until ctx is done,
// and refuses with ErrClosing once the host has begun to shut down.
func (h *Host) acquire(ctx context.Context, slot chan struct{}) error {
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		if h.isClosing() {
			return ErrClosing
		}
		return ctx.Err()
	}
	if h.isClosing() {
		<-slot
		return ErrClosing
	}
	return nil
}

// Interrupt ends the drive of run id, if any, as a shutdown of its
// controller: nothing is committed for it, no job is cancelled, and a later
// drive resumes the run from the journal.
func (h *Host) Interrupt(id string) { _ = h.interrupt(context.Background(), id) }

// ended is the run and error of the latest drive or stop that ended.
type ended struct {
	id  string
	err error
}

// DriveError returns the latest completed drive or stop error for run id when
// no operation owns its work. An in-flight stop supersedes its interrupted
// drive, then publishes its own actual result, including failure or cancellation.
func (h *Host) DriveError(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ended.id == id && !h.workingLocked(id) {
		return h.ended.err
	}
	return nil
}

// Busy reports whether the host drives a run or has an operation in flight
// or waiting.
func (h *Host) Busy() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.drivers) > 0 || h.active > 0 || len(h.runs) > 0
}
