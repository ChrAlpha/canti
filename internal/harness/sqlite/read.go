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
		err = known(kind, semantics)
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
		t, err := scanTransition(rows)
		if err == nil {
			out = append(out, t)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// scanTransition decodes a row of seq, event ID, kind, semantics, event,
// both states, and facts.
func scanTransition(rows *sql.Rows) (harness.Transition, error) {
	var r record
	var seq int64
	var eventID string
	if err := rows.Scan(&seq, &eventID, &r.kind, &r.semantics, &r.event, &r.from, &r.to, &r.facts); err != nil {
		return harness.Transition{}, err
	}
	t, err := r.decode()
	t.Seq, t.EventID = seq, eventID
	return t, err
}

func (r record) decode() (harness.Transition, error) {
	t, err := r.decodeEventStates()
	if err != nil {
		return t, err
	}
	t.Semantics = r.semantics
	if err := strict(r.facts, &t.Facts); err != nil {
		return t, err
	}
	return t, nil
}

func (r record) decodeEventStates() (harness.Transition, error) {
	var t harness.Transition
	event, err := decodeEvent(r.kind, r.event, r.semantics)
	if err := cmp.Or(err, decodeState(r.semantics, r.from, &t.From), decodeState(r.semantics, r.to, &t.To)); err != nil {
		return t, err
	}
	t.Event = event
	return t, nil
}

func decodeEvent(kind, data string, semantics int) (core.Event, error) {
	if err := known(kind, semantics); err != nil {
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
	return s.request(ctx, nil, runID, step, rev)
}

func (s *Store) request(ctx context.Context, tx *sql.Tx, runID string, step, rev int) (harness.Request, error) {
	facts, err := decodeFacts(s.stmt(ctx, tx, requestSQL).QueryRowContext(ctx, runID, step, rev))
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

// Accepted returns the acceptance of the exec decision of attempt of step:
// the transition right after its saved response, since only a decision or a
// stop follows one, found through the response index and the primary key.
func (s *Store) Accepted(ctx context.Context, runID string, step, attempt int) (harness.Accepted, error) {
	return s.accepted(ctx, nil, runID, step, attempt)
}

// accepted reads the kind, semantics, next phase, and action of the
// accepting row, without decoding its event or states, which repeat the
// decision, so that the check of every effect commit stays small.
func (s *Store) accepted(ctx context.Context, tx *sql.Tx, runID string, step, attempt int) (harness.Accepted, error) {
	var seq int64
	if err := s.stmt(ctx, tx, savedSQL).QueryRowContext(ctx, runID, step, attempt).Scan(&seq); err != nil {
		return harness.Accepted{}, missing(err, "acceptance")
	}
	var kind string
	var semantics int
	var phase, action sql.NullString
	if err := s.stmt(ctx, tx, acceptedSQL).QueryRowContext(ctx, runID, seq+1).Scan(&kind, &semantics, &phase, &action); err != nil {
		return harness.Accepted{}, missing(err, "acceptance")
	}
	if err := known(kind, semantics); err != nil {
		return harness.Accepted{}, err
	}
	if kind != "response_loaded" || phase.String != string(core.PhaseEffectSaved) {
		return harness.Accepted{}, fmt.Errorf("sqlite: acceptance: %w", harness.ErrNotFound)
	}
	var a *harness.Action
	if action.Valid {
		a = new(harness.Action)
		if err := strict(action.String, a); err != nil {
			return harness.Accepted{}, err
		}
	}
	accepted, err := harness.Acceptance(semantics, a)
	if err != nil {
		return harness.Accepted{}, fmt.Errorf("%w: step %d attempt %d of run %q: %v", ErrIncompatible, step, attempt, runID, err)
	}
	return accepted, nil
}

// window returns the window of the request frozen at revision rev of step,
// nil when it records none, without decoding the request body.
func (s *Store) window(ctx context.Context, tx *sql.Tx, runID string, step, rev int) (*harness.Window, error) {
	var semantics int
	var data sql.NullString
	if err := s.stmt(ctx, tx, windowSQL).QueryRowContext(ctx, runID, step, rev).Scan(&semantics, &data); err != nil {
		return nil, missing(err, "request")
	}
	if err := core.KnownSemantics(semantics); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	if !data.Valid {
		return nil, nil
	}
	var w harness.Window
	if err := strict(data.String, &w); err != nil {
		return nil, err
	}
	return &w, nil
}

// Recent returns at most limit of the latest observed, performed, or
// resolved steps committed below sequence before, or the latest ones when
// before is not positive, oldest first, in one statement that searches the
// partial index of each. A read-only store of a journal without
// performedIndex answers for a run whose latest transition precedes
// harness.ActionSemantics, which holds no performed step, and fails for any
// other once a new look finds the index still missing.
func (s *Store) Recent(ctx context.Context, runID string, before int64, limit int) ([]harness.StepRecord, error) {
	if before <= 0 {
		before = math.MaxInt64
	}
	query := stepsSQL
	if !s.performed.Load() {
		latest, err := s.latestSemantics(ctx, runID)
		if err != nil {
			return nil, err
		}
		if latest >= harness.ActionSemantics {
			if err := s.probePerformed(ctx); err != nil {
				return nil, err
			}
			if !s.performed.Load() {
				return nil, fmt.Errorf("%w: run %q has transitions of semantics version %d, and the journal lacks the index of its performed steps",
					ErrIncompatible, runID, latest)
			}
		} else {
			query = recentSQL
		}
	}
	var out []harness.StepRecord
	err := s.each(ctx, runID, limit, query, []any{runID, before, limit}, func(rows *sql.Rows) error {
		step, err := scanStep(rows)
		out = append(out, step)
		return err
	})
	if err != nil {
		return nil, err
	}
	slices.Reverse(out)
	return out, nil
}

// Completion returns the step that the transition at seq of the run
// completed: one read by primary key, which needs no index. It fails with
// harness.ErrNotFound when the run holds no transition at seq, and with both
// ErrIncompatible and harness.ErrIncompatible when the transition there
// completes no step or does not decode as a step its version records; a
// done context and a failed read are returned as they are.
func (s *Store) Completion(ctx context.Context, runID string, seq int64) (harness.StepRecord, error) {
	rows, err := s.stmts[completionSQL].QueryContext(ctx, runID, seq)
	if err != nil {
		return harness.StepRecord{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return harness.StepRecord{}, err
		}
		return harness.StepRecord{}, fmt.Errorf("sqlite: transition %d of run %q: %w", seq, runID, harness.ErrNotFound)
	}
	step, err := scanStep(rows)
	if err != nil && ctx.Err() == nil {
		return harness.StepRecord{}, fmt.Errorf("%w: %w", harness.ErrIncompatible, err)
	}
	if err != nil {
		return harness.StepRecord{}, err
	}
	return step, rows.Close()
}

// scanStep decodes a row of seq, kind, semantics, from state, and facts of a
// completed step and checks the shape harness.StepRecord.Validate requires
// of a record of its semantics version.
func scanStep(rows *sql.Rows) (harness.StepRecord, error) {
	var seq int64
	var kind string
	var semantics int
	var from, data string
	var state core.State
	var facts harness.Facts
	if err := rows.Scan(&seq, &kind, &semantics, &from, &data); err != nil {
		return harness.StepRecord{}, err
	}
	if err := cmp.Or(known(kind, semantics), decodeState(semantics, from, &state), strict(data, &facts)); err != nil {
		return harness.StepRecord{}, err
	}
	if kind != "observed" && kind != "performed" && kind != "resolved" {
		return harness.StepRecord{}, fmt.Errorf("%w: a %s transition at seq %d completes no step", ErrIncompatible, kind, seq)
	}
	r := harness.StepRecord{Seq: seq, Step: state.Step, Command: state.Decision.Payload, Purpose: state.Decision.Purpose,
		Outcome: facts.Outcome, Window: facts.Window, Resolved: kind == "resolved"}
	if facts.Observation != nil {
		r.Observation = *facts.Observation
	}
	if err := r.Validate(semantics, kind == "performed"); err != nil {
		return r, fmt.Errorf("%w: %s step %d of seq %d: %v", ErrIncompatible, kind, r.Step, seq, err)
	}
	return r, nil
}

// latestSemantics returns the semantics version of the latest transition of
// the run, or of its initial state.
func (s *Store) latestSemantics(ctx context.Context, runID string) (int, error) {
	var seq int64
	var kind, state string
	var semantics int
	err := s.stmts[latestSQL].QueryRowContext(ctx, runID).Scan(&seq, &kind, &semantics, &state)
	if errors.Is(err, sql.ErrNoRows) {
		err = missing(s.stmts[initialSQL].QueryRowContext(ctx, runID).Scan(&semantics, &state), "run")
	}
	return semantics, err
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

// known reports whether this package reads a record of kind committed
// under semantics.
func known(kind string, semantics int) error {
	if events[kind] == nil {
		return fmt.Errorf("%w: record kind %q", ErrIncompatible, kind)
	}
	if err := core.CompatibleEvent(semantics, reflect.Zero(events[kind]).Interface().(core.Event)); err != nil {
		return fmt.Errorf("%w: %v", ErrIncompatible, err)
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
