package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// ErrLocked reports a state directory that another process owns.
var ErrLocked = errors.New("locked by another canti process")

// ErrClosing reports an operation that a host shutting down no longer admits.
var ErrClosing = errors.New("host: the host is shutting down")

// ErrDriving reports an operation on a run that the host is driving, which
// only a stop may interrupt.
var ErrDriving = errors.New("host: the run is being driven")

// ErrBusy reports a Close while operations of the host are in flight.
var ErrBusy = errors.New("host: operations are in flight; shut the host down instead")

// Options configure a Host. Workspace and State are resolved directories.
type Options struct {
	Workspace, State string
	Logger           *slog.Logger
	// WrapJournal wraps the journal every loop commits through; tests use it
	// to fail or kill the process around a commit.
	WrapJournal func(harness.Journal) harness.Journal
	// Checkpoint is called with a named point of the host's work; nil
	// ignores them. Tests use it to pause a process at one.
	Checkpoint func(point string)
	// Credentials are the host's own credentials, each bound to the exact
	// model API and provider of its pair: a drive or submission whose caller
	// gives no key for its run's pair uses the one bound to it. They live in
	// memory only. A foreground canti host binds the keys of its
	// environment; a host started for a client holds none.
	Credentials map[Pair]Keys
}

// The points a host reports to Options.Checkpoint.
const (
	PointAdmitting = "submit.admitting" // the checks of admission passed, before its transaction
	PointSubmitted = "submit.committed" // the admission transaction committed, before Submit returns
	PointWaiting   = "wait.registered"  // a Wait registered for changes, before it rereads the journal
	PointClosing   = "shutdown.linearized"
)

// Host is the runtime host of one state directory: it owns the state
// directory's lock and journal for its lifetime, admits runs serially,
// drives them, stops and resolves them, and projects their progress. It
// holds no run fact of its own: everything it reports is read from the
// journal, and what it keeps in memory, which runs it drives and with which
// binding, ends with each drive.
type Host struct {
	opts    Options
	store   *sqlite.Store
	journal harness.Journal
	lock    *os.File

	// life ends when Shutdown begins; every drive and operation context
	// derives from it.
	life    context.Context
	endLife context.CancelFunc
	// admission, a slot of capacity one, serializes the checks and the
	// commit of admission.
	admission chan struct{}

	mu       sync.Mutex
	changed  chan struct{} // closed and replaced whenever the host commits or a drive ends
	drivers  map[string]*driver
	runs     map[string]*runLock
	ended    ended
	ops      sync.WaitGroup // the operations Enter admitted and that have not ended
	active   int            // how many ops counts
	closing  bool
	released bool
}

// Open takes the lock of the state directory and opens its journal; the
// state directory must exist. Unless create is set, a missing journal is an
// error.
func Open(ctx context.Context, opts Options, create bool) (*Host, error) {
	path := filepath.Join(opts.State, "canti.db")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) && !create {
		return nil, fmt.Errorf("no runs in workspace %s", opts.Workspace)
	}
	f, err := os.OpenFile(filepath.Join(opts.State, "lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = fmt.Errorf("workspace %s is %w", opts.Workspace, ErrLocked)
		}
		return nil, err
	}
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		f.Close()
		return nil, err
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	life, endLife := context.WithCancel(context.Background())
	h := &Host{opts: opts, store: store, lock: f, life: life, endLife: endLife, admission: make(chan struct{}, 1),
		changed: make(chan struct{}), drivers: map[string]*driver{}, runs: map[string]*runLock{}}
	var j harness.Journal = store
	if opts.WrapJournal != nil {
		j = opts.WrapJournal(j)
	}
	h.journal = notifying{j, h}
	return h, nil
}

// Close releases the journal and the lock of a host that drives nothing and
// has no operation in flight, and refuses otherwise. It is idempotent;
// Shutdown ends the drives and operations first.
func (h *Host) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return nil
	}
	switch {
	case len(h.drivers) > 0:
		return ErrDriving
	case h.active > 0:
		return ErrBusy
	}
	h.released = true
	h.endLife()
	err := h.store.Close()
	return errors.Join(err, h.lock.Close())
}

func (h *Host) checkpoint(point string) {
	if h.opts.Checkpoint != nil {
		h.opts.Checkpoint(point)
	}
}

// notify wakes every Wait: closing the current channel reaches all of them
// at once, and each registers anew before it rereads the journal.
func (h *Host) notify() {
	h.mu.Lock()
	close(h.changed)
	h.changed = make(chan struct{})
	h.mu.Unlock()
}

// notifying wakes the waits of a host after each commit the store
// acknowledged.
type notifying struct {
	harness.Journal
	h *Host
}

func (n notifying) Commit(ctx context.Context, t harness.Transition) error {
	err := n.Journal.Commit(ctx, t)
	if err == nil {
		n.h.notify()
	}
	return err
}

// Find returns run id, or the latest run when id is empty, with its stored
// config, which must belong to the host's workspace.
func (h *Host) Find(ctx context.Context, id string) (harness.Run, RunConfig, error) {
	return Find(ctx, h.store, h.opts.Workspace, id)
}

// Find returns run id of j, or its latest run when id is empty, with the
// stored config, which must belong to workspace.
func Find(ctx context.Context, j interface {
	Run(context.Context, string) (harness.Run, error)
	LastRun(context.Context) (harness.Run, error)
}, workspace, id string) (harness.Run, RunConfig, error) {
	var run harness.Run
	var err error
	if id != "" {
		if run, err = j.Run(ctx, id); errors.Is(err, harness.ErrNotFound) {
			err = fmt.Errorf("run %q not found in workspace %s", id, workspace)
		}
	} else if run, err = j.LastRun(ctx); errors.Is(err, harness.ErrNotFound) {
		err = fmt.Errorf("no runs in workspace %s", workspace)
	}
	if err != nil {
		return run, RunConfig{}, err
	}
	c, err := DecodeConfig(run, workspace)
	return run, c, err
}

// loop reconstructs the exact stored configuration for every operation,
// including credential-free stopping, resolution and inspection.
func (h *Host) loop(run harness.Run, c RunConfig, m harness.Model) (*harness.Loop, error) {
	x, err := NewExecutor(c, h.opts.State)
	if err != nil {
		return nil, err
	}
	cfg := ContextConfig(c, run.Budget)
	cfg.History, cfg.Logger = HistoryRoot(h.opts.State), h.opts.Logger
	cfg.Reader.History, cfg.Reader.Artifacts = cfg.History, filepath.Join(h.opts.State, "artifacts")
	return harness.NewLoop(h.journal, x, m, cfg)
}

// InheritedConstraints returns the constraints a new input of session
// inherits: those of its latest run.
func (h *Host) InheritedConstraints(ctx context.Context, session string) (string, error) {
	prior, err := h.store.SessionRuns(ctx, session, 0, 1)
	if err != nil || len(prior) == 0 {
		return "", err
	}
	return prior[0].Constraints, nil
}

// Workspace is the workspace of the host's runs.
func (h *Host) Workspace() string { return h.opts.Workspace }
