package host

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

// CanonicalState resolves the effective directory used for admission, host
// identity and every state write, including when its leaf does not yet exist.
func CanonicalState(workspace, state string) (string, error) {
	return docker.ResolveState(workspace, state)
}

// Validate checks a prospective submission without creating state or durable
// facts. Submit remains the authoritative admission point under the host lock.
// A steer of a working run creates no run, so its prospective spec need not
// name the context window that every run creation requires.
func Validate(ctx context.Context, state string, sub Submission) error {
	requireWindow := true
	if sub.Delivery == harness.DeliverySteer {
		run, snap, err := readLatest(ctx, state)
		if err != nil {
			return err
		}
		requireWindow = run == nil || snap.State.Closed()
	}
	c, err := ResolveExecution(ctx, sub.Spec.Config, state)
	if err != nil {
		return SpecError{Err: err}
	}
	sub.Spec.Config = c
	if _, _, err := newRun(sub.Goal, sub.Constraints, sub.Spec, time.Now(), requireWindow); err != nil {
		return SpecError{Err: err}
	}
	return nil
}

// ReadInherited inspects existing state without acquiring a writer or creating
// a journal. A running host exposes the same information through
// InheritedConstraints.
func ReadInherited(ctx context.Context, state string) (string, error) {
	run, _, err := readLatest(ctx, state)
	if err != nil || run == nil {
		return "", err
	}
	return run.Constraints, nil
}

// readLatest reads the latest run of state and its state without creating a
// journal. It returns no run when the journal or a run is absent.
func readLatest(ctx context.Context, state string) (*harness.Run, harness.Snapshot, error) {
	path := filepath.Join(state, "canti.db")
	if fi, err := os.Stat(path); errors.Is(err, os.ErrNotExist) || err == nil && fi.Size() == 0 {
		return nil, harness.Snapshot{}, nil
	}
	store, err := sqlite.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, harness.Snapshot{}, err
	}
	defer store.Close()
	run, err := store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return nil, harness.Snapshot{}, nil
	}
	if err != nil {
		return nil, harness.Snapshot{}, err
	}
	snap, err := store.Latest(ctx, run.ID)
	if err != nil {
		return nil, harness.Snapshot{}, err
	}
	return &run, snap, nil
}

// ValidateLayout checks the filesystem layout before reading state.
func ValidateLayout(workspace, state string) error {
	return docker.CheckLayout(workspace, state)
}

// CheckLocation refuses a client's preflight for another host's directories.
func (h *Host) CheckLocation(workspace, state string) error {
	if workspace != h.opts.Workspace || state != h.opts.State {
		return SpecError{Err: errors.New("validation workspace and state must name this host's directories")}
	}
	return nil
}
