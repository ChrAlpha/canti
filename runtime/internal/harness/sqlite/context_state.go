package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
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
	var pointer int64
	if err := tx.QueryRowContext(ctx, `SELECT context_seq FROM runs WHERE id=?`, expected.State.RunID).Scan(&pointer); err != nil {
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
	err := tx.QueryRowContext(ctx, `SELECT kind,semantics,from_state,to_state,event,json_extract(facts,'$.ContextState'),
        json_extract(facts,'$.Outcome'),json_extract(facts,'$.Rejection'),
        COALESCE(json_type(facts,'$.Response')='object' AND length(json_extract(facts,'$.Failure'))>0,0) FROM transitions WHERE run_id=? AND seq=?`, expected.State.RunID, pointer).Scan(&kind, &semantics, &from, &to, &event, &data, &outcome, &rejection, &hasRejectionResponse)
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
	case "performed":
		if err := s.checkFocusHead(ctx, tx, expected, pointer, before, state, outcome); err != nil {
			return state, err
		}
	default:
		return state, fmt.Errorf("%w: invalid context provenance", ErrIncompatible)
	}
	if err := state.Validate(expected.State.Step, pointer); err != nil {
		return state, fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	return state, nil
}

func (s *Store) checkFocusHead(ctx context.Context, tx *sql.Tx, expected harness.Snapshot, pointer int64, before core.State, state harness.ContextState, outcome sql.NullString) error {
	accepted, err := s.accepted(ctx, tx, before.RunID, before.Step, before.Attempt)
	if err != nil {
		return err
	}
	if accepted.Action.Name != harness.ActionFocus || accepted.Action.Recovery != harness.RecoveryLocal || !outcome.Valid {
		return fmt.Errorf("%w: context head is not a focus completion", ErrIncompatible)
	}
	focusOutcome := new(harness.Outcome)
	if err := strict(outcome.String, focusOutcome); err != nil {
		return err
	}
	if err := focusOutcome.Validate(); err != nil || focusOutcome.Focus == nil {
		return fmt.Errorf("%w: invalid focus head", ErrIncompatible)
	}
	if err := accepted.Action.Matches(before.Decision.Payload); err != nil {
		return fmt.Errorf("%w: invalid focus acceptance", ErrIncompatible)
	}
	if state.AnchorSeq > 0 {
		if state.AnchorSeq >= pointer {
			return fmt.Errorf("%w: focus anchor is not prior", ErrIncompatible)
		}
		var anchorKind string
		if err := tx.QueryRowContext(ctx, `SELECT kind FROM transitions WHERE run_id=? AND seq=?`, expected.State.RunID, state.AnchorSeq).Scan(&anchorKind); err != nil || anchorKind != "request_rejected" {
			return fmt.Errorf("%w: focus anchor is not a rejection", ErrIncompatible)
		}
		anchor, err := s.readContextState(ctx, tx, expected, state.AnchorSeq)
		if err != nil || anchor.AnchorSeq != state.AnchorSeq || anchor.FirstStep != state.FirstStep || len(anchor.Focus) != 0 {
			return fmt.Errorf("%w: focus names no rejection anchor", ErrIncompatible)
		}
	}
	if !slices.Equal(state.Focus, focusOutcome.Focus.Sources) {
		return fmt.Errorf("%w: focus head differs from outcome", ErrIncompatible)
	}
	return nil
}

func (s *Store) checkContext(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	current, err := s.contextState(ctx, tx, harness.Snapshot{Seq: t.Seq - 1, State: t.From})
	if err != nil {
		return err
	}
	if err := harness.CheckContextMutation(t, current); err != nil {
		return err
	}
	if t.Facts.ContextState != nil {
		for _, ref := range t.Facts.ContextState.Focus {
			record, err := s.completion(ctx, tx, t.From.RunID, ref.SourceSeq)
			if err != nil {
				return err
			}
			if err := harness.ValidateEvidence(record, ref); err != nil {
				return fmt.Errorf("%w: %v", harness.ErrFacts, err)
			}
		}
	}
	return nil
}
