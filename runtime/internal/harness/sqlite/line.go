package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const (
	latestEventSQL = `SELECT kind, semantics, event FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`
	runOrdinalSQL  = `SELECT n FROM runs WHERE id = ?`
)

// Steers returns the principal steer rows of runID in admission order, each
// with the InputsBound transition that bound it when the run had bound it.
func (s *Store) Steers(ctx context.Context, runID string) ([]harness.Input, error) {
	rows, err := s.stmts[steersSQL].QueryContext(ctx, runID)
	if err != nil {
		return nil, err
	}
	var out []harness.Input
	for rows.Next() {
		in, err := scanInput(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, in)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	bounds, err := s.bounds(ctx, runID)
	if err != nil {
		return nil, err
	}
	for i := range out {
		for _, b := range bounds {
			if b.count >= i+1 {
				out[i].BoundSeq, out[i].BoundStep = b.seq, b.step
				break
			}
		}
	}
	return out, nil
}

type bound struct {
	seq   int64
	step  int
	count int
}

func (s *Store) bounds(ctx context.Context, runID string) ([]bound, error) {
	rows, err := s.stmts[boundsSQL].QueryContext(ctx, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []bound
	previous := 0
	for rows.Next() {
		var b bound
		if err := rows.Scan(&b.seq, &b.step, &b.count); err != nil {
			return nil, err
		}
		if b.seq < 1 || b.step < 1 || b.count <= previous {
			return nil, fmt.Errorf("%w: invalid instruction binding at sequence %d", ErrIncompatible, b.seq)
		}
		previous = b.count
		out = append(out, b)
	}
	return out, rows.Err()
}

// Observations returns the latest observation of each source and subject,
// most recently admitted first, at most harness.MaxObservationKeys.
func (s *Store) Observations(ctx context.Context) ([]harness.ObservationInput, error) {
	rows, err := s.stmts[observationsSQL].QueryContext(ctx, harness.MaxObservationKeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []harness.ObservationInput
	for rows.Next() {
		var o harness.ObservationInput
		if err := rows.Scan(&o.Source, &o.Subject, &o.Pos, &o.Text, &o.InputID); err != nil {
			return nil, err
		}
		if o.Pos < 1 {
			return nil, fmt.Errorf("%w: invalid observation at position %d", ErrIncompatible, o.Pos)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// PreviousRuns returns the closed runs of the run's line before it, most
// recent first, at most harness.MaxPreviousRuns, and the earlier runs the
// page leaves out.
func (s *Store) PreviousRuns(ctx context.Context, runID string) ([]harness.PreviousRun, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, previousCountSQL, runID).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, previousIDsSQL, runID, harness.MaxPreviousRuns)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	out := make([]harness.PreviousRun, 0, len(ids))
	for _, id := range ids {
		run, err := s.Run(ctx, id)
		if err != nil {
			return nil, 0, err
		}
		snap, err := s.latest(ctx, nil, id)
		if err != nil {
			return nil, 0, err
		}
		p := harness.PreviousRun{Run: run, State: snap.State, Closed: snap.State.Closed()}
		if !p.Closed {
			return nil, 0, fmt.Errorf("%w: previous run %q is %s", ErrIncompatible, id, snap.State.Phase)
		}
		out = append(out, p)
	}
	return out, max(0, total-len(out)), nil
}

// InputCounts reports the pending, queued, and held inputs.
func (s *Store) InputCounts(ctx context.Context, runID string) (harness.InputCounts, error) {
	return s.inputCounts(ctx, runID)
}

// LastEvent returns the latest committed event of runID, or nil before its
// first transition.
func (s *Store) LastEvent(ctx context.Context, runID string) (core.Event, error) {
	var kind string
	var semantics int
	var event string
	err := s.db.QueryRowContext(ctx, latestEventSQL, runID).Scan(&kind, &semantics, &event)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeEvent(kind, event, semantics)
}

// RunOrdinal is the serial position of runID among the runs of its state
// directory, from 1. An unknown run is harness.ErrNotFound.
func (s *Store) RunOrdinal(ctx context.Context, runID string) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, runOrdinalSQL, runID).Scan(&n); err != nil {
		return 0, missing(err, "run")
	}
	return n, nil
}
