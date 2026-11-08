package sqlite

import (
	"context"
	"database/sql"
	"fmt"

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
	return readContextState(ctx, tx, expected, pointer)
}

func readContextState(ctx context.Context, tx *sql.Tx, expected harness.Snapshot, pointer int64) (harness.ContextState, error) {
	var kind, data, from, to string
	var semantics int
	err := tx.QueryRowContext(ctx, `SELECT kind,semantics,from_state,to_state,json_extract(facts,'$.ContextState') FROM transitions WHERE run_id=? AND seq=?`, expected.State.RunID, pointer).Scan(&kind, &semantics, &from, &to, &data)
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
	valid := kind == "request_rejected" && after.Phase == core.PhaseIdle && state.AnchorSeq == pointer && state.FirstStep == before.Step && len(state.Focus) == 0 || kind == "performed"
	if !valid || before.RunID != expected.State.RunID || after.RunID != expected.State.RunID {
		return state, fmt.Errorf("%w: invalid context provenance", ErrIncompatible)
	}
	if err := state.Validate(expected.State.Step, pointer); err != nil {
		return state, fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	return state, nil
}

func (s *Store) checkContext(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	// Requests without context are accepted only while adding the contract;
	// the reader-driven loop installs it on every request at the policy switch.
	if t.Facts.ContextState == nil && t.Facts.Rejection == nil && (t.Facts.Request == nil || t.Facts.Request.ContextState == nil) {
		return nil
	}
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
