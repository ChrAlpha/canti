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
func Validate(ctx context.Context, state string, sub Submission) error {
	c, err := ResolveExecution(ctx, sub.Spec.Config, state)
	if err != nil {
		return SpecError{Err: err}
	}
	sub.Spec.Config = c
	if _, _, err := NewRun(sub.Goal, sub.Constraints, sub.Spec, time.Now()); err != nil {
		return SpecError{Err: err}
	}
	return nil
}

// ReadInherited inspects existing state without acquiring a writer or creating
// a journal. A running host exposes the same information through
// InheritedConstraints.
func ReadInherited(ctx context.Context, state string) (string, error) {
	path := filepath.Join(state, "canti.db")
	if fi, err := os.Stat(path); errors.Is(err, os.ErrNotExist) || err == nil && fi.Size() == 0 {
		return "", nil
	}
	store, err := sqlite.OpenReadOnly(ctx, path)
	if err != nil {
		return "", err
	}
	defer store.Close()
	last, err := store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return last.Constraints, nil
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
