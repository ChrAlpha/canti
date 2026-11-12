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
	// facts, for rejection provenance.
	transitionBySeqSQL = `SELECT kind,semantics,event,from_state,to_state,facts FROM transitions WHERE run_id=? AND seq=?`
)

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
// the latest snapshot of its run. Zero context_seq materializes the head
// every run starts from.
func (s *Store) contextHead(ctx context.Context, tx *sql.Tx, expected harness.Snapshot) (harness.ContextState, error) {
	var pointer int64
	if err := s.stmt(ctx, tx, contextSeqSQL).QueryRowContext(ctx, expected.State.RunID).Scan(&pointer); err != nil {
		return harness.ContextState{}, err
	}
	if pointer == 0 {
		return harness.InitialContext(), nil
	}
	if pointer < 0 || pointer > expected.Seq {
		return harness.ContextState{}, fmt.Errorf("%w: invalid context head %d", ErrIncompatible, pointer)
	}
	return s.readContextState(ctx, tx, expected, pointer)
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
	if err := s.checkHeadRejection(ctx, tx, expected.State.RunID, pointer, state.Rejection); err != nil {
		return state, err
	}
	return state, nil
}

// checkHandoverHead proves that a materialized performed head is a successful
// accepted handover with valid arguments, the closed done outcome, and the
// prescribed head.
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
	note, err := harness.HandoverText(before.Decision.Payload)
	if err != nil || harness.ValidateHandoverNote(note) != nil {
		return fmt.Errorf("%w: invalid handover arguments", ErrIncompatible)
	}
	if outcome == nil || *outcome != (harness.Outcome{Status: harness.OutcomeDone}) {
		return fmt.Errorf("%w: handover outcome differs from its payload", ErrIncompatible)
	}
	want := harness.ContextState{AnchorSeq: pointer, FirstStep: after.Step}
	if state.AnchorSeq != want.AnchorSeq || state.FirstStep != want.FirstStep || state.Minimal {
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
	optional := capture.Bytes.Index > 0 || capture.Bytes.Previous > 0 || capture.Bytes.Observations > 0
	if rejection.EpochSteps != len(capture.Bytes.Steps) || rejection.Repair != (capture.Preparation == "protocol_repair") ||
		rejection.OptionalContent != optional || rejection.Minimal != capture.Anchor.Minimal {
		return fmt.Errorf("%w: rejection shape differs from the frozen request capture", ErrIncompatible)
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
	return nil
}
