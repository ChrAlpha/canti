package host

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

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
// a journal. A running host exposes the same information through Inherited.
func ReadInherited(ctx context.Context, state, session string) (string, error) {
	path := filepath.Join(state, "canti.db")
	if fi, err := os.Stat(path); errors.Is(err, os.ErrNotExist) || err == nil && fi.Size() == 0 {
		return "", nil
	}
	store, err := sqlite.OpenReadOnly(ctx, path)
	if err != nil {
		return "", err
	}
	defer store.Close()
	prior, err := store.SessionRuns(ctx, session, 0, 1)
	if err != nil || len(prior) == 0 {
		return "", err
	}
	return prior[0].Constraints, nil
}

// ValidateLayout checks identity and filesystem layout before reading state.
func ValidateLayout(workspace, state, session string) error {
	if session != "" && !sqlite.ValidKey(session) {
		return SpecError{Err: errors.New("-session must be 1 to 128 ASCII letters, digits, underscores, or hyphens")}
	}
	if err := docker.CheckLayout(workspace, state); err != nil {
		return err
	}
	return nil
}

// CheckLocation refuses a client's preflight for another host's directories.
func (h *Host) CheckLocation(workspace, state string) error {
	if workspace != h.opts.Workspace || state != h.opts.State {
		return SpecError{Err: errors.New("validation workspace and state must name this host's directories")}
	}
	return nil
}
