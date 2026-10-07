package sqlite

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

var events = func() map[string]reflect.Type {
	m := make(map[string]reflect.Type)
	for _, e := range core.Events() {
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
	var data string
	if err := row.Scan(&data); err != nil {
		return run, missing(err, "run")
	}
	return run, strict(data, &run)
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
	var state string
	err := s.stmt(ctx, tx, latestSQL).QueryRowContext(ctx, runID).Scan(&snap.Seq, &kind, &semantics, &state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		err = missing(s.stmt(ctx, tx, initialSQL).QueryRowContext(ctx, runID).Scan(&semantics, &state), "run")
	case err == nil:
		err = known(kind)
	}
	if err == nil {
		err = decodeState(semantics, state, &snap.State)
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
		var r record
		var seq int64
		var eventID string
		if err := rows.Scan(&seq, &eventID, &r.kind, &r.semantics, &r.event, &r.from, &r.to, &r.facts); err != nil {
			return err
		}
		t, err := r.decode()
		if err != nil {
			return err
		}
		t.Seq, t.EventID = seq, eventID
		out = append(out, t)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r record) decode() (harness.Transition, error) {
	t, err := r.decodeEventStates()
	if err != nil {
		return t, err
	}
	if err := strict(r.facts, &t.Facts); err != nil {
		return t, err
	}
	return t, nil
}

func (r record) decodeEventStates() (harness.Transition, error) {
	var t harness.Transition
	event, err := decodeEvent(r.kind, r.event)
	if err := cmp.Or(err, decodeState(r.semantics, r.from, &t.From), decodeState(r.semantics, r.to, &t.To)); err != nil {
		return t, err
	}
	t.Event = event
	return t, nil
}

func decodeEvent(kind, data string) (core.Event, error) {
	if err := known(kind); err != nil {
		return nil, err
	}
	event := reflect.New(events[kind])
	if err := strict(data, event.Interface()); err != nil {
		return nil, err
	}
	return event.Elem().Interface().(core.Event), nil
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
	return decodeFacts(s.stmts[query].QueryRowContext(ctx, runID, step, n))
}

func decodeFacts(row *sql.Row) (harness.Facts, error) {
	var facts harness.Facts
	var semantics int
	var from, to, data string
	if err := row.Scan(&semantics, &from, &to, &data); err != nil {
		return facts, missing(err, "record")
	}
	var state core.State
	for _, data := range []string{from, to} {
		if err := decodeState(semantics, data, &state); err != nil {
			return facts, err
		}
	}
	return facts, strict(data, &facts)
}

// Recent returns at most limit of the latest observed or resolved steps
// committed below sequence before, or the latest ones when before is not
// positive, oldest first.
func (s *Store) Recent(ctx context.Context, runID string, before int64, limit int) ([]harness.StepRecord, error) {
	if before <= 0 {
		before = math.MaxInt64
	}
	var out []harness.StepRecord
	err := s.each(ctx, runID, limit, recentSQL, []any{runID, before, limit}, func(rows *sql.Rows) error {
		var seq int64
		var kind string
		var semantics int
		var from, data string
		var state core.State
		var facts harness.Facts
		if err := rows.Scan(&seq, &kind, &semantics, &from, &data); err != nil {
			return err
		}
		if err := cmp.Or(decodeState(semantics, from, &state), strict(data, &facts)); err != nil {
			return err
		}
		if facts.Observation == nil {
			return fmt.Errorf("%w: %s step without an observation", ErrIncompatible, kind)
		}
		out = append(out, harness.StepRecord{Seq: seq, Step: state.Step, Command: state.Decision.Payload, Purpose: state.Decision.Purpose,
			Observation: *facts.Observation, Resolved: kind == "resolved"})
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
	var state string
	if err := s.stmts[initialSQL].QueryRowContext(ctx, runID).Scan(&semantics, &state); err != nil {
		return missing(err, "run")
	}
	var initial core.State
	return decodeState(semantics, state, &initial)
}

func known(kind string) error {
	if events[kind] == nil {
		return fmt.Errorf("%w: record kind %q", ErrIncompatible, kind)
	}
	return nil
}

func decodeState(semantics int, data string, state *core.State) error {
	*state = core.State{}
	if err := core.KnownSemantics(semantics); err != nil {
		return fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	if err := strict(data, state); err != nil {
		return err
	}
	if err := core.Compatible(semantics, *state); err != nil {
		return fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	return nil
}

func missing(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite: %s: %w", what, harness.ErrNotFound)
	}
	return err
}

func strict(data string, v any) error {
	decoder := json.NewDecoder(strings.NewReader(data))
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
