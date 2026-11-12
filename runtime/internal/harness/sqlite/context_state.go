package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const anchorKindSQL = `SELECT kind FROM transitions WHERE run_id=? AND seq=?`

const (
	// contextSeqSQL reads the sequence of the context head of a run, zero
	// before its first.
	contextSeqSQL = `SELECT context_seq FROM runs WHERE id = ?`
	// contextHeadSQL reads the transition at the context head of a run with
	// the facts that establish it.
	contextHeadSQL = `SELECT kind,semantics,from_state,to_state,event,json_extract(facts,'$.ContextState'),
        json_extract(facts,'$.Outcome'),json_extract(facts,'$.Rejection'),
        COALESCE(json_type(facts,'$.Response')='object' AND length(json_extract(facts,'$.Failure'))>0,0) FROM transitions WHERE run_id=? AND seq=?`
	// transitionBySeqSQL reads one transition with its event identity and
	// facts, for handover and rejection provenance.
	transitionBySeqSQL = `SELECT kind,semantics,event,from_state,to_state,facts FROM transitions WHERE run_id=? AND seq=?`
	// lineContainsSQL reports whether origin is in runID's permitted line.
	lineContainsSQL = `WITH RECURSIVE line(n, id, fresh) AS (
		SELECT n, id, fresh FROM runs WHERE id = ?1
		UNION ALL
		SELECT r.n, r.id, r.fresh FROM runs r JOIN line l ON r.n = l.n - 1 WHERE l.fresh = 0)
		SELECT count(*) FROM line WHERE id = ?2`
)

// lineRunsSQL walks the run's line: itself, then its predecessors while the
// newer run did not start fresh. The run that started fresh is included and
// ends the walk. It reads runs only: unrelated runs and their transitions are
// never in the line.
const lineRunsSQL = `WITH RECURSIVE line(n, id, fresh) AS (
	SELECT n, id, fresh FROM runs WHERE id = ?1
	UNION ALL
	SELECT r.n, r.id, r.fresh FROM runs r JOIN line l ON r.n = l.n - 1 WHERE l.fresh = 0)
	SELECT id FROM line WHERE id <> ?1 ORDER BY n DESC`

// latestHandoverSQL reads the newest successful handover of one run by its
// own performed index; the initial head calls it once per line run.
const latestHandoverSQL = `SELECT seq, json_extract(from_state,'$.Step') FROM transitions
	WHERE run_id = ? AND kind = 'performed'
	  AND (CASE WHEN json_valid(facts) THEN json_type(facts,'$.Outcome.Handover') END) = 'object'
	ORDER BY seq DESC LIMIT 1`

// Context refuses to combine an old run position with a newer context head.
func (s *Store) Context(ctx context.Context, expected harness.Snapshot) (harness.ContextState, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return harness.ContextState{}, err
	}
	defer tx.Rollback()
	result, err := s.contextState(ctx, tx, expected)
	if err != nil {
		return harness.ContextState{}, err
	}
	if err := tx.Commit(); err != nil {
		return harness.ContextState{}, err
	}
	return result, nil
}

func (s *Store) contextState(ctx context.Context, tx *sql.Tx, expected harness.Snapshot) (harness.ContextState, error) {
	latest, err := s.latest(ctx, tx, expected.State.RunID)
	if err != nil {
		return harness.ContextState{}, err
	}
	if latest != expected {
		return harness.ContextState{}, harness.ErrStaleSnapshot
	}
	return s.contextHead(ctx, tx, expected)
}

// contextHead is contextState for expected, which tx has already read as
// the latest snapshot of its run. Zero context_seq materializes the effective
// initial head, including a handover inherited from the immutable line.
func (s *Store) contextHead(ctx context.Context, tx *sql.Tx, expected harness.Snapshot) (harness.ContextState, error) {
	var pointer int64
	if err := s.stmt(ctx, tx, contextSeqSQL).QueryRowContext(ctx, expected.State.RunID).Scan(&pointer); err != nil {
		return harness.ContextState{}, err
	}
	if pointer == 0 {
		return s.initialHead(ctx, tx, expected)
	}
	if pointer < 0 || pointer > expected.Seq {
		return harness.ContextState{}, fmt.Errorf("%w: invalid context head %d", ErrIncompatible, pointer)
	}
	return s.readContextState(ctx, tx, expected, pointer)
}

// initialHead derives the effective initial head of a run: the newest
// successful handover of the closest previous run of its line, unless the run
// started fresh.
func (s *Store) initialHead(ctx context.Context, tx *sql.Tx, expected harness.Snapshot) (harness.ContextState, error) {
	rows, err := tx.QueryContext(ctx, lineRunsSQL, expected.State.RunID)
	if err != nil {
		return harness.ContextState{}, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return harness.ContextState{}, err
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return harness.ContextState{}, err
	}
	for _, id := range ids {
		var ref harness.HandoverRef
		var step sql.NullInt64
		err := s.stmt(ctx, tx, latestHandoverSQL).QueryRowContext(ctx, id).Scan(&ref.Seq, &step)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return harness.ContextState{}, err
		}
		ref.RunID, ref.Step = id, int(step.Int64)
		state := harness.ContextState{FirstStep: 1, Handover: &ref}
		if err := state.Validate(expected.State.Step, 0); err != nil {
			return harness.ContextState{}, fmt.Errorf("%w: %v", ErrIncompatible, err)
		}
		if err := s.checkHandoverRef(ctx, tx, expected.State.RunID, &ref, 0); err != nil {
			return harness.ContextState{}, err
		}
		return state, nil
	}
	return harness.InitialContext(), nil
}

func (s *Store) readContextState(ctx context.Context, tx *sql.Tx, expected harness.Snapshot, pointer int64) (harness.ContextState, error) {
	var kind, data, from, to, event string
	var outcome, rejection sql.NullString
	var semantics int
	var hasRejectionResponse bool
	err := s.stmt(ctx, tx, contextHeadSQL).QueryRowContext(ctx, expected.State.RunID, pointer).Scan(&kind, &semantics, &from, &to, &event, &data, &outcome, &rejection, &hasRejectionResponse)
	if err != nil {
		return harness.ContextState{}, fmt.Errorf("%w: context head: %v", ErrIncompatible, err)
	}
	var state harness.ContextState
	var before, after core.State
	if err := decodeState(semantics, from, &before); err != nil {
		return state, err
	}
	if err := decodeState(semantics, to, &after); err != nil {
		return state, err
	}
	if err := strict(data, &state); err != nil {
		return state, err
	}
	e, err := decodeEvent(kind, event, semantics)
	if err != nil {
		return state, err
	}
	next, err := before.Next(e)
	if err != nil || next != after || before.RunID != expected.State.RunID || after.RunID != expected.State.RunID {
		return state, fmt.Errorf("%w: invalid context transition", ErrIncompatible)
	}
	facts := harness.Facts{ContextState: &state}
	switch kind {
	case "request_rejected":
		if !rejection.Valid || !hasRejectionResponse {
			return state, fmt.Errorf("%w: missing context rejection", ErrIncompatible)
		}
		facts.Rejection = new(harness.ContextRejection)
		if err := strict(rejection.String, facts.Rejection); err != nil {
			return state, err
		}
		if err := facts.Rejection.Previous.Validate(before.Step, pointer-1); err != nil {
			return state, fmt.Errorf("%w: invalid previous context", ErrIncompatible)
		}
		if err := harness.CheckContextMutation(harness.Transition{Seq: pointer, From: before, To: after, Event: e, Facts: facts}, facts.Rejection.Previous); err != nil {
			return state, fmt.Errorf("%w: %v", ErrIncompatible, err)
		}
		if err := s.checkRejectionCapture(ctx, tx, before, facts.Rejection); err != nil {
			return state, err
		}
	case "performed":
		if !outcome.Valid {
			return state, fmt.Errorf("%w: handover head has no outcome", ErrIncompatible)
		}
		facts.Outcome = new(harness.Outcome)
		if err := strict(outcome.String, facts.Outcome); err != nil {
			return state, err
		}
		if err := s.checkHandoverHead(ctx, tx, pointer, before, after, state, facts.Outcome); err != nil {
			return state, err
		}
	default:
		return state, fmt.Errorf("%w: invalid context provenance", ErrIncompatible)
	}
	if err := state.Validate(expected.State.Step, pointer); err != nil {
		return state, fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	if err := s.checkHandoverRef(ctx, tx, expected.State.RunID, state.Handover, pointer); err != nil {
		return state, err
	}
	if err := s.checkHeadRejection(ctx, tx, expected.State.RunID, pointer, state.Rejection); err != nil {
		return state, err
	}
	return state, nil
}

// checkHandoverHead proves that a materialized performed head is a
// successful accepted handover, with the prescribed head and the outcome text
// equal to its committed payload.
func (s *Store) checkHandoverHead(ctx context.Context, tx *sql.Tx, pointer int64, before, after core.State, state harness.ContextState, outcome *harness.Outcome) error {
	accepted, err := s.accepted(ctx, tx, before.RunID, before.Step, before.Attempt)
	if err != nil {
		return err
	}
	if accepted.Action.Name != harness.ActionHandover || accepted.Action.Recovery != harness.RecoveryLocal {
		return fmt.Errorf("%w: context head is not a handover completion", ErrIncompatible)
	}
	if err := accepted.Action.Matches(before.Decision.Payload); err != nil {
		return fmt.Errorf("%w: invalid handover acceptance", ErrIncompatible)
	}
	if outcome.Status != harness.OutcomeDone || outcome.Handover == nil {
		return fmt.Errorf("%w: handover head has no handover outcome", ErrIncompatible)
	}
	if err := outcome.Validate(); err != nil {
		return fmt.Errorf("%w: invalid handover outcome: %v", ErrIncompatible, err)
	}
	text, err := harness.HandoverText(before.Decision.Payload)
	if err != nil || text != outcome.Handover.Text {
		return fmt.Errorf("%w: handover outcome differs from its payload", ErrIncompatible)
	}
	want := harness.ContextState{
		AnchorSeq: pointer, FirstStep: after.Step,
		Handover: &harness.HandoverRef{RunID: before.RunID, Seq: pointer, Step: before.Step},
	}
	if state.AnchorSeq != want.AnchorSeq || state.FirstStep != want.FirstStep || state.Minimal || !state.Handover.Equal(want.Handover) {
		return fmt.Errorf("%w: handover head differs from its committed position", ErrIncompatible)
	}
	return nil
}

// checkRejectionCapture proves that a rejection record describes the frozen
// request it rejected: the head, the resident steps, the repair and every
// optional section.
func (s *Store) checkRejectionCapture(ctx context.Context, tx *sql.Tx, before core.State, rejection *harness.ContextRejection) error {
	var semantics int
	var from, to, factsData string
	_ = to
	err := s.stmt(ctx, tx, requestSQL).QueryRowContext(ctx, before.RunID, before.Step, before.RequestRev).Scan(&semantics, &from, &to, &factsData)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: no frozen request for the rejected revision", ErrIncompatible)
	}
	if err != nil {
		return err
	}
	var frozen core.State
	if err := decodeState(semantics, from, &frozen); err != nil {
		return err
	}
	var facts harness.Facts
	if err := strict(factsData, &facts); err != nil {
		return err
	}
	request := facts.Request
	if request == nil || request.ContextState == nil || request.Context == nil || !request.ContextState.Equal(rejection.Previous) {
		return fmt.Errorf("%w: rejected request context differs from the recorded previous head", ErrIncompatible)
	}
	capture := request.Context
	optional := capture.Bytes.Handover > 0 || capture.Bytes.Index > 0 || capture.Bytes.Previous > 0 || capture.Bytes.Observations > 0
	if rejection.EpochSteps != len(capture.Bytes.Steps) || rejection.Repair != (capture.Preparation == "protocol_repair") ||
		rejection.OptionalContent != optional || rejection.Minimal != capture.Anchor.Minimal {
		return fmt.Errorf("%w: rejection shape differs from the frozen request capture", ErrIncompatible)
	}
	return nil
}

// checkHandoverRef proves that ref names a successful handover completion of
// this run or of a run in its permitted line. A reference to the transition
// being committed (seq == ref.Seq in the same run) is validated by shape only;
// every other reference must be a complete, self-consistent transition of its
// run and carry the provenance of a successful handover completion: its
// accepted action, outcome, and prescribed head.
func (s *Store) checkHandoverRef(ctx context.Context, tx *sql.Tx, runID string, ref *harness.HandoverRef, headSeq int64) error {
	if ref == nil {
		return nil
	}
	if ref.RunID == runID && headSeq > 0 && ref.Seq == headSeq {
		if ref.Step < 1 {
			return fmt.Errorf("%w: invalid handover step", ErrIncompatible)
		}
		return nil
	}
	if ref.RunID != runID {
		var n int
		if err := tx.QueryRowContext(ctx, lineContainsSQL, runID, ref.RunID).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: handover origin run is outside the line", ErrIncompatible)
		}
	}
	var kind string
	if err := tx.QueryRowContext(ctx, anchorKindSQL, ref.RunID, ref.Seq).Scan(&kind); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: no transition carries the handover", ErrIncompatible)
		}
		return err
	}
	if kind != "performed" {
		return fmt.Errorf("%w: handover reference is not a performed completion", ErrIncompatible)
	}
	before, after, facts, err := s.readTransition(ctx, tx, ref.RunID, ref.Seq)
	if err != nil {
		return err
	}
	if before.Step != ref.Step || facts.ContextState == nil || facts.Outcome == nil {
		return fmt.Errorf("%w: handover reference does not name its completion", ErrIncompatible)
	}
	if err := s.checkHandoverHead(ctx, tx, ref.Seq, before, after, *facts.ContextState, facts.Outcome); err != nil {
		return err
	}
	if ref.RunID == runID && ref.Seq > headSeq {
		return fmt.Errorf("%w: handover reference follows its head", ErrIncompatible)
	}
	return nil
}

// readTransition reads one transition of runID and validates it exactly as a
// context head read does: the decoded event applies to its from_state, the
// next state is the recorded to_state, and both states belong to runID.
func (s *Store) readTransition(ctx context.Context, tx *sql.Tx, runID string, seq int64) (core.State, core.State, harness.Facts, error) {
	var kind string
	var semantics int
	var event, from, to, factsData string
	if err := tx.QueryRowContext(ctx, transitionBySeqSQL, runID, seq).Scan(&kind, &semantics, &event, &from, &to, &factsData); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.State{}, core.State{}, harness.Facts{}, fmt.Errorf("%w: no transition %d of run %s", ErrIncompatible, seq, runID)
		}
		return core.State{}, core.State{}, harness.Facts{}, err
	}
	var before, after core.State
	if err := decodeState(semantics, from, &before); err != nil {
		return core.State{}, core.State{}, harness.Facts{}, err
	}
	if err := decodeState(semantics, to, &after); err != nil {
		return core.State{}, core.State{}, harness.Facts{}, err
	}
	e, err := decodeEvent(kind, event, semantics)
	if err != nil {
		return core.State{}, core.State{}, harness.Facts{}, fmt.Errorf("%w: transition %d of run %s: %v", ErrIncompatible, seq, runID, err)
	}
	next, err := before.Next(e)
	if err != nil || next != after || before.RunID != runID || after.RunID != runID {
		return core.State{}, core.State{}, harness.Facts{}, fmt.Errorf("%w: invalid transition %d of run %s", ErrIncompatible, seq, runID)
	}
	var facts harness.Facts
	if err := strict(factsData, &facts); err != nil {
		return core.State{}, core.State{}, harness.Facts{}, err
	}
	return before, after, facts, nil
}

// checkHeadRejection proves that ref is exactly the rejection that opened the
// current head lineage of runID, and that its fields match the source rejection
// record. A rejection that opens no head never moves the pointer, so a terminal
// or attempt-exhausted rejection after the head leaves this check valid. It
// derives the expected value from the run, never from ref itself.
func (s *Store) checkHeadRejection(ctx context.Context, tx *sql.Tx, runID string, headSeq int64, ref *harness.RejectionRef) error {
	expected, err := s.headRejection(ctx, tx, runID, headSeq)
	if err != nil {
		return err
	}
	switch {
	case expected == nil && ref != nil:
		return fmt.Errorf("%w: head carries a rejection the run does not hold", ErrIncompatible)
	case expected != nil && ref == nil:
		return fmt.Errorf("%w: head dropped the run's rejection", ErrIncompatible)
	case expected != nil && *ref != *expected:
		return fmt.Errorf("%w: head rejection differs from the rejection that opened the head", ErrIncompatible)
	}
	return nil
}

// headRejection returns the rejection that opened runID's current head
// lineage from the run's rejection pointer and that transition's record.
func (s *Store) headRejection(ctx context.Context, tx *sql.Tx, runID string, headSeq int64) (*harness.RejectionRef, error) {
	var seq int64
	if err := s.stmt(ctx, tx, rejectionSeqSQL).QueryRowContext(ctx, runID).Scan(&seq); err != nil {
		return nil, err
	}
	if seq == 0 {
		return nil, nil
	}
	if seq > headSeq {
		return nil, fmt.Errorf("%w: rejection follows the head", ErrIncompatible)
	}
	var kind string
	if err := tx.QueryRowContext(ctx, anchorKindSQL, runID, seq).Scan(&kind); err != nil {
		return nil, fmt.Errorf("%w: rejection record: %v", ErrIncompatible, err)
	}
	if kind != "request_rejected" {
		return nil, fmt.Errorf("%w: rejection pointer is not a rejection", ErrIncompatible)
	}
	before, _, facts, err := s.readTransition(ctx, tx, runID, seq)
	if err != nil {
		return nil, err
	}
	if before.Step < 1 || facts.Rejection == nil || !facts.Rejection.Signal.Confirmed() {
		return nil, fmt.Errorf("%w: rejection pointer does not name its rejection", ErrIncompatible)
	}
	return &harness.RejectionRef{Seq: seq, Step: before.Step, MaxAcceptedInput: facts.Rejection.MaxAcceptedInput, MaxAcceptedReported: facts.Rejection.MaxAcceptedReported}, nil
}

// checkContext checks t against the context head of its run inside the write
// transaction tx that commits it, which has read t.From at t.Seq-1 as the
// latest snapshot of the run.
func (s *Store) checkContext(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	current, err := s.contextHead(ctx, tx, harness.Snapshot{Seq: t.Seq - 1, State: t.From})
	if err != nil {
		return err
	}
	if err := harness.CheckContextMutation(t, current); err != nil {
		return err
	}
	if rejection := t.Facts.Rejection; rejection != nil {
		if err := s.checkRejectionCapture(ctx, tx, t.From, rejection); err != nil {
			return err
		}
	}
	next := t.Facts.ContextState
	if next == nil {
		return nil
	}
	if next.Handover != nil && next.Handover.RunID == t.From.RunID && next.Handover.Seq == t.Seq {
		if next.Handover.Step != t.From.Step {
			return fmt.Errorf("%w: handover head names another step", harness.ErrFacts)
		}
	} else if err := s.checkHandoverRef(ctx, tx, t.From.RunID, next.Handover, t.Seq); err != nil {
		return err
	}
	return nil
}
