package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

type ContextPage struct {
	AsOfSeq    int64
	Run        harness.Run
	Request    harness.Transition
	Previous   *harness.Transition
	Next       *harness.Transition
	Events     []harness.Transition
	More       bool
	EventBytes int64
}

const ContextEventPageBytes = 16 << 20

const contextColumns = `seq, event_id, kind, semantics, event, from_state, to_state, facts`

const contextFrozenSQL = `SELECT ` + contextColumns + `, step, rev
	FROM transitions INDEXED BY transitions_request WHERE run_id = ? AND kind = 'request_frozen'`

// ContextRequest reads only the selected frozen request in one snapshot.
// Zero step or revision selects the latest; a revision needs a step.
func (s *Store) ContextRequest(ctx context.Context, runID string, step, revision int) (harness.Transition, error) {
	if step < 0 || revision < 0 || step == 0 && revision != 0 {
		return harness.Transition{}, errors.New("sqlite: invalid context selection")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return harness.Transition{}, err
	}
	defer tx.Rollback()
	request, err := selectContextRequest(ctx, tx, runID, step, revision)
	if err != nil {
		return harness.Transition{}, err
	}
	if err := tx.Commit(); err != nil {
		return harness.Transition{}, err
	}
	return *request, nil
}

// ContextRecords reads one snapshot. Zero step or revision selects the latest;
// a revision needs a step. Events follow the selected request and precede the
// next frozen revision, with sequences above after. Limit must be 1 through 100.
func (s *Store) ContextRecords(ctx context.Context, runID string, step, revision int, after int64, limit int) (ContextPage, error) {
	if step < 0 || revision < 0 || after < 0 || limit < 1 || limit > 100 || step == 0 && revision != 0 {
		return ContextPage{}, errors.New("sqlite: invalid context selection or page")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ContextPage{}, err
	}
	defer tx.Rollback()
	page, err := readContextPage(ctx, tx, runID, step, revision, after, limit)
	if err != nil {
		return ContextPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return ContextPage{}, err
	}
	return page, nil
}

func readContextPage(ctx context.Context, tx *sql.Tx, runID string, step, revision int, after int64, limit int) (ContextPage, error) {
	var page ContextPage
	var semantics int
	var data, initial string
	err := tx.QueryRowContext(ctx, `SELECT semantics, run, initial,
		COALESCE((SELECT max(seq) FROM transitions WHERE run_id = runs.id), 0)
		FROM runs WHERE id = ?`, runID).Scan(&semantics, &data, &initial, &page.AsOfSeq)
	if err != nil {
		return page, missing(err, "run")
	}
	var state core.State
	if err := decodeState(semantics, initial, &state); err != nil {
		return page, err
	}
	if err := strict(data, &page.Run); err != nil {
		return page, err
	}
	if page.Run.ID != runID {
		return page, fmt.Errorf("%w: run identity differs from its key", ErrIncompatible)
	}
	request, err := selectContextRequest(ctx, tx, runID, step, revision)
	if err != nil {
		return page, err
	}
	page.Request = *request
	state = request.To
	args := []any{runID, state.Step, state.RequestRev}
	page.Previous, err = contextFrozen(ctx, tx, runID,
		" AND (step, rev) < (?, ?) ORDER BY step DESC, rev DESC LIMIT 1", args)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return page, err
	}
	page.Next, err = contextFrozen(ctx, tx, runID,
		" AND (step, rev) > (?, ?) ORDER BY step, rev LIMIT 1", args)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return page, err
	}
	if request.Seq > page.AsOfSeq ||
		page.Previous != nil && page.Previous.Seq >= request.Seq ||
		page.Next != nil && (page.Next.Seq <= request.Seq || page.Next.Seq > page.AsOfSeq) {
		return page, fmt.Errorf("%w: frozen request sequence order", ErrIncompatible)
	}
	before := page.AsOfSeq
	if page.Next != nil {
		before = page.Next.Seq - 1
	}
	end, size, more, err := contextEventPage(ctx, tx, runID, max(after, request.Seq), before, limit)
	if err != nil {
		return page, err
	}
	page.EventBytes, page.More = size, more
	page.Events, err = contextEvents(ctx, tx, runID, max(after, request.Seq), end, limit)
	return page, err
}

func contextEventPage(ctx context.Context, tx *sql.Tx, runID string, after, before int64, limit int) (int64, int64, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT seq,
		octet_length(event) + octet_length(from_state) + octet_length(to_state) + octet_length(facts)
		FROM transitions WHERE run_id = ? AND seq > ? AND seq <= ? ORDER BY seq LIMIT ?`, runID, after, before, limit+1)
	if err != nil {
		return 0, 0, false, err
	}
	defer rows.Close()
	end, size, count := after, int64(0), 0
	for rows.Next() {
		var seq, n int64
		if err := rows.Scan(&seq, &n); err != nil {
			return 0, 0, false, err
		}
		if count == limit || count > 0 && size+n > ContextEventPageBytes {
			return end, size, true, nil
		}
		// A single oversized event advances intact; later rows use the next page.
		end, size, count = seq, size+n, count+1
	}
	return end, size, false, rows.Err()
}

func selectContextRequest(ctx context.Context, tx *sql.Tx, runID string, step, revision int) (*harness.Transition, error) {
	selection, args := " ORDER BY step DESC, rev DESC LIMIT 1", []any{runID}
	if step > 0 {
		selection, args = " AND step = ? ORDER BY rev DESC LIMIT 1", append(args, step)
		if revision > 0 {
			selection, args = " AND step = ? AND rev = ?", append(args, revision)
		}
	}
	request, err := contextFrozen(ctx, tx, runID, selection, args)
	return request, missing(err, "frozen request")
}

func contextFrozen(ctx context.Context, tx *sql.Tx, runID, selection string, args []any) (*harness.Transition, error) {
	var r record
	var seq int64
	var eventID string
	var step, revision int
	err := tx.QueryRowContext(ctx, contextFrozenSQL+selection, args...).Scan(
		&seq, &eventID, &r.kind, &r.semantics, &r.event, &r.from, &r.to, &r.facts, &step, &revision)
	if err != nil {
		return nil, err
	}
	t, err := r.decode()
	if err != nil {
		return nil, err
	}
	_, frozen := t.Event.(core.RequestFrozen)
	if !frozen || seq <= 0 || step < 1 || revision < 1 || t.Facts.Request == nil ||
		t.From.RunID != runID || t.To.RunID != runID || t.To.Step != step || t.To.RequestRev != revision {
		return nil, fmt.Errorf("%w: frozen request identity or facts at seq %d", ErrIncompatible, seq)
	}
	t.Seq, t.EventID = seq, eventID
	if err := harness.ValidateTransition(t); err != nil {
		return nil, fmt.Errorf("%w: invalid frozen context: %v", ErrIncompatible, err)
	}
	return &t, nil
}

func contextEvents(ctx context.Context, tx *sql.Tx, runID string, after, before int64, limit int) ([]harness.Transition, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+contextColumns+`
		FROM transitions WHERE run_id = ? AND seq > ? AND seq <= ? ORDER BY seq LIMIT ?`, runID, after, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]harness.Transition, 0, limit)
	for rows.Next() {
		t, err := scanTransition(rows)
		if err != nil {
			return nil, err
		}
		if t.From.RunID != runID || t.To.RunID != runID {
			return nil, fmt.Errorf("%w: context event run identity at seq %d", ErrIncompatible, t.Seq)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
