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
	"slices"

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
	return s.run(s.stmts[runSQL].QueryRowContext(ctx, runID))
}

// LastRun returns the run created most recently in this store.
func (s *Store) LastRun(ctx context.Context) (harness.Run, error) {
	return s.run(s.stmts[lastRunSQL].QueryRowContext(ctx))
}

func (s *Store) run(row *sql.Row) (harness.Run, error) {
	var run harness.Run
	var record []byte
	if err := row.Scan(&record); err != nil {
		return run, missing(err, "run")
	}
	return run, strict(record, &run)
}

// Latest returns the last committed transition's sequence and state, or
// sequence zero with the initial state of a run without transitions.
func (s *Store) Latest(ctx context.Context, runID string) (harness.Snapshot, error) {
	return s.latest(ctx, nil, runID)
}

func (s *Store) latest(ctx context.Context, tx *sql.Tx, runID string) (harness.Snapshot, error) {
	var snap harness.Snapshot
	var kind string
	var semantics int
	var state []byte
	err := s.stmt(ctx, tx, latestSQL).QueryRowContext(ctx, runID).Scan(&snap.Seq, &kind, &semantics, &state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		err = missing(s.stmt(ctx, tx, initialSQL).QueryRowContext(ctx, runID).Scan(&semantics, &state), "run")
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
	err := s.stmts[totalsSQL].QueryRowContext(ctx, runID).Scan(&t.Usage.InputTokens, &t.Usage.OutputTokens, &t.OutputBytes, &t.Steps)
	return t, missing(err, "run")
}

// Transitions returns at most limit transitions of the run with a sequence
// after after, in commit order.
func (s *Store) Transitions(ctx context.Context, runID string, after int64, limit int) ([]harness.Transition, error) {
	var out []harness.Transition
	err := s.each(ctx, runID, limit, transitionsSQL, []any{runID, after, limit}, func(rows *sql.Rows) error {
		var t harness.Transition
		var kind string
		var semantics int
		var event, from, to, facts []byte
		if err := rows.Scan(&t.Seq, &t.EventID, &kind, &semantics, &event, &from, &to, &facts); err != nil {
			return err
		}
		if err := cmp.Or(known(kind), current(semantics)); err != nil {
			return err
		}
		value := reflect.New(events[kind])
		if err := cmp.Or(strict(event, value.Interface()), strict(from, &t.From), strict(to, &t.To), strict(facts, &t.Facts)); err != nil {
			return err
		}
		t.Event = value.Elem().Interface().(core.Event)
		out = append(out, t)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Request returns the request frozen at revision rev of step.
func (s *Store) Request(ctx context.Context, runID string, step, rev int) (harness.Request, error) {
	facts, err := s.facts(ctx, requestSQL, runID, step, rev)
	if err == nil && facts.Request == nil {
		err = fmt.Errorf("%w: frozen request without a request record", ErrIncompatible)
	}
	if err != nil {
		return harness.Request{}, err
	}
	return *facts.Request, nil
}

// Response returns the complete response saved for attempt of step.
func (s *Store) Response(ctx context.Context, runID string, step, attempt int) (harness.Response, error) {
	facts, err := s.facts(ctx, responseSQL, runID, step, attempt)
	if err == nil && facts.Response == nil {
		err = fmt.Errorf("%w: saved response without a response record", ErrIncompatible)
	}
	if err != nil {
		return harness.Response{}, err
	}
	return *facts.Response, nil
}

func (s *Store) facts(ctx context.Context, query, runID string, step, n int) (harness.Facts, error) {
	var facts harness.Facts
	var semantics int
	var data []byte
	err := missing(s.stmts[query].QueryRowContext(ctx, runID, step, n).Scan(&semantics, &data), "record")
	if err == nil {
		err = current(semantics)
	}
	if err == nil {
		err = strict(data, &facts)
	}
	return facts, err
}

// Recent returns at most limit of the latest observed or resolved steps,
// oldest first.
func (s *Store) Recent(ctx context.Context, runID string, limit int) ([]harness.StepRecord, error) {
	var out []harness.StepRecord
	err := s.each(ctx, runID, limit, recentSQL, []any{runID, limit}, func(rows *sql.Rows) error {
		var kind string
		var semantics int
		var from, data []byte
		var state core.State
		var facts harness.Facts
		if err := rows.Scan(&kind, &semantics, &from, &data); err != nil {
			return err
		}
		if err := cmp.Or(current(semantics), strict(from, &state), strict(data, &facts)); err != nil {
			return err
		}
		if facts.Observation == nil {
			return fmt.Errorf("%w: %s step without an observation", ErrIncompatible, kind)
		}
		out = append(out, harness.StepRecord{Step: state.Step, Command: state.Decision.Payload, Observation: *facts.Observation, Resolved: kind == "resolved"})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Reverse(out)
	return out, nil
}

// each calls fn for every row of a query bounded by limit and reports an
// unknown run when no row matched.
func (s *Store) each(ctx context.Context, runID string, limit int, query string, args []any, fn func(*sql.Rows) error) error {
	if limit > 0 {
		rows, err := s.stmts[query].QueryContext(ctx, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		found := false
		for rows.Next() {
			found = true
			if err := fn(rows); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil || found {
			return err
		}
	}
	var semantics int
	var state []byte
	return missing(s.stmts[initialSQL].QueryRowContext(ctx, runID).Scan(&semantics, &state), "run")
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
