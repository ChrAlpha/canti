package sqlite

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

var events = func() map[string]reflect.Type {
	m := make(map[string]reflect.Type)
	for _, e := range []core.Event{
		core.RequestFrozen{}, core.AttemptStarted{}, core.ResponseSaved{}, core.ResponseIncomplete{},
		core.ResponseLoaded{}, core.JobBound{}, core.JobStarted{}, core.Observed{},
		core.OutcomeUnknown{}, core.StopRequested{}, core.Resolved{},
	} {
		m[e.Kind()] = reflect.TypeOf(e)
	}
	return m
}()

// Run returns the stored run record.
func (s *Store) Run(ctx context.Context, runID string) (harness.Run, error) {
	return s.run(ctx, `SELECT run FROM runs WHERE id = ?`, runID)
}

// LastRun returns the run created most recently in this store.
func (s *Store) LastRun(ctx context.Context) (harness.Run, error) {
	return s.run(ctx, `SELECT run FROM runs ORDER BY n DESC LIMIT 1`)
}

func (s *Store) run(ctx context.Context, query string, args ...any) (harness.Run, error) {
	var run harness.Run
	var record []byte
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&record); err != nil {
		return run, missing(err, "run")
	}
	return run, strict(record, &run)
}

// Latest returns the last committed transition's sequence and state, or
// sequence zero with the initial state of a run without transitions.
func (s *Store) Latest(ctx context.Context, runID string) (harness.Snapshot, error) {
	return latest(ctx, s.db, runID)
}

const latestSQL = `SELECT seq, kind, semantics, to_state FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`

func latest(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, runID string) (harness.Snapshot, error) {
	var snap harness.Snapshot
	var kind string
	var semantics int
	var state []byte
	err := q.QueryRowContext(ctx, latestSQL, runID).Scan(&snap.Seq, &kind, &semantics, &state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		err = missing(q.QueryRowContext(ctx, `SELECT semantics, initial FROM runs WHERE id = ?`, runID).Scan(&semantics, &state), "run")
	case err == nil:
		err = known(kind)
	}
	if err == nil {
		err = current(semantics)
	}
	if err == nil {
		err = strict(state, &snap.State)
	}
	if err != nil {
		return harness.Snapshot{}, err
	}
	return snap, nil
}

// Totals returns the run-wide sums maintained by Commit.
func (s *Store) Totals(ctx context.Context, runID string) (harness.Totals, error) {
	var t harness.Totals
	err := s.db.QueryRowContext(ctx, `SELECT input_tokens, output_tokens, output_bytes, steps FROM runs WHERE id = ?`, runID).
		Scan(&t.Usage.InputTokens, &t.Usage.OutputTokens, &t.OutputBytes, &t.Steps)
	return t, missing(err, "run")
}

const transitionsSQL = `SELECT seq, event_id, kind, semantics, event, from_state, to_state, facts
	FROM transitions WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?`

// Transitions returns at most limit transitions of the run with a sequence
// after after, in commit order.
func (s *Store) Transitions(ctx context.Context, runID string, after int64, limit int) ([]harness.Transition, error) {
	if limit < 1 {
		return nil, s.exists(ctx, runID)
	}
	rows, err := s.db.QueryContext(ctx, transitionsSQL, runID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []harness.Transition
	for rows.Next() {
		var t harness.Transition
		var kind string
		var semantics int
		var event, from, to, facts []byte
		if err := rows.Scan(&t.Seq, &t.EventID, &kind, &semantics, &event, &from, &to, &facts); err != nil {
			return nil, err
		}
		if err := cmp.Or(known(kind), current(semantics)); err != nil {
			return nil, err
		}
		value := reflect.New(events[kind])
		if err := errors.Join(strict(event, value.Interface()), strict(from, &t.From), strict(to, &t.To), strict(facts, &t.Facts)); err != nil {
			return nil, err
		}
		t.Event = value.Elem().Interface().(core.Event)
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, s.exists(ctx, runID)
	}
	return out, nil
}

func (s *Store) exists(ctx context.Context, runID string) error {
	var one int
	return missing(s.db.QueryRowContext(ctx, `SELECT 1 FROM runs WHERE id = ?`, runID).Scan(&one), "run")
}

func known(kind string) error {
	if events[kind] == nil {
		return fmt.Errorf("%w: record kind %q", ErrIncompatible, kind)
	}
	return nil
}

func current(semantics int) error {
	if semantics != core.SemanticsVersion {
		return fmt.Errorf("%w: semantics version %d, want %d", ErrIncompatible, semantics, core.SemanticsVersion)
	}
	return nil
}

func missing(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite: %s: %w", what, harness.ErrNotFound)
	}
	return err
}

func strict(data []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(v)
	if err == nil {
		if _, trailing := decoder.Token(); trailing != io.EOF {
			err = errors.New("trailing data")
		}
	}
	if err != nil {
		return fmt.Errorf("%w: decode %T: %v", ErrIncompatible, v, err)
	}
	return nil
}
