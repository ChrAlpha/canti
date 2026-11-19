package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	projectctx "github.com/ChrAlpha/canti/runtime/internal/project"
)

// CanonicalState resolves the effective directory used for admission, host
// identity and every state write, including when its leaf does not yet exist.
func CanonicalState(workspace, state string) (string, error) {
	return docker.ResolveState(workspace, state)
}

// Validate checks a prospective submission without creating state or durable
// facts and returns the specification a run would be created with, including
// the project snapshot a new run would bind, and whether the submission would
// open a run now. It follows the same state-dependent checks as Submit.
func Validate(ctx context.Context, state string, sub Submission) (Spec, bool, error) {
	if err := CheckContextVersion(sub.Spec.Config); err != nil {
		return sub.Spec, false, SpecError{Err: err}
	}
	store, latest, snap, last, err := readLatest(ctx, state)
	if err != nil {
		return sub.Spec, false, err
	}
	if store != nil {
		defer store.Close()
	}
	opening := latest == nil || snap.State.Closed()
	input := harness.InputSubmission{InputID: sub.InputID, Authority: harness.AuthorityPrincipal, Text: sub.Goal,
		Constraints: sub.Constraints, Delivery: sub.Delivery, Fresh: sub.Fresh}
	if err := sqlite.ValidateSubmission(input); err != nil {
		return sub.Spec, opening, SpecError{Err: err}
	}
	if !opening {
		switch sub.Delivery {
		case "":
			return sub.Spec, false, SpecError{Err: harness.ErrDeliveryRequired}
		case harness.DeliverySteer:
			if err := store.CheckSteer(ctx, latest.ID, sub.Goal); err != nil {
				return sub.Spec, false, SpecError{Err: err}
			}
			spec := sub.Spec
			spec.Project = projectctx.Snapshot{}
			return spec, false, nil
		case harness.DeliveryFollowUp:
			prepared, err := workingSubmission(sub)
			if err != nil {
				return sub.Spec, false, err
			}
			return preparedSpec(sub, prepared), false, nil
		}
	}
	if latest != nil {
		counts, err := store.InputCounts(ctx, latest.ID)
		if err != nil {
			return sub.Spec, opening, err
		}
		if counts.Held > 0 {
			return sub.Spec, opening, SpecError{Err: fmt.Errorf("%w; withdraw the held inputs first", harness.ErrHeld)}
		}
		if harness.Blocks(snap.State, last) {
			return sub.Spec, opening, fmt.Errorf("run %s is unfinished or has an unknown effect", latest.ID)
		}
	}
	spec, err := Prepare(ctx, state, sub.Spec)
	if err != nil {
		return spec, opening, err
	}
	p, err := newProvider(spec.Config.API, sub.Binding)
	if err != nil {
		return spec, opening, err
	}
	spec.Config.Provider = p
	if _, _, err := newRun(sub.Goal, sub.Constraints, spec, time.Now(), true); err != nil {
		return spec, opening, SpecError{Err: err}
	}
	return spec, opening, nil
}

// CheckContextVersion is the version gate shared by validation and submission.
func CheckContextVersion(c RunConfig) error {
	if c.ContextVersion != ContextVersion {
		return fmt.Errorf("the client creates runs of context version %d and this host renders %d", c.ContextVersion, ContextVersion)
	}
	return nil
}

func preparedSpec(sub Submission, prepared harness.InputSubmission) Spec {
	spec := sub.Spec
	var stored Spec
	if err := strictSpec(prepared.Spec, &stored); err == nil {
		spec = stored
	}
	return spec
}

// readLatest opens the journal read-only, if present, and reads its latest run.
func readLatest(ctx context.Context, state string) (*sqlite.Store, *harness.Run, harness.Snapshot, core.Event, error) {
	path := filepath.Join(state, "canti.db")
	if fi, err := os.Stat(path); errors.Is(err, os.ErrNotExist) || err == nil && fi.Size() == 0 {
		return nil, nil, harness.Snapshot{}, nil, nil
	}
	store, err := sqlite.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, nil, harness.Snapshot{}, nil, err
	}
	last, err := store.LastRun(ctx)
	if errors.Is(err, harness.ErrNotFound) {
		return store, nil, harness.Snapshot{}, nil, nil
	}
	if err != nil {
		store.Close()
		return nil, nil, harness.Snapshot{}, nil, err
	}
	snap, err := store.Latest(ctx, last.ID)
	if err != nil {
		store.Close()
		return nil, nil, harness.Snapshot{}, nil, err
	}
	event, err := store.LastEvent(ctx, last.ID)
	if err != nil {
		store.Close()
		return nil, nil, harness.Snapshot{}, nil, err
	}
	return store, &last, snap, event, nil
}

// ReadInherited inspects existing state without acquiring a writer or creating
// a journal. A running host exposes the same information through
// InheritedConstraints.
func ReadInherited(ctx context.Context, state string) (string, error) {
	store, run, _, _, err := readLatest(ctx, state)
	if store != nil {
		defer store.Close()
	}
	if err != nil || run == nil {
		return "", err
	}
	return run.Constraints, nil
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
