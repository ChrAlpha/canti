package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const admissionBlockSQL = `SELECT r.id FROM runs r
LEFT JOIN transitions t ON t.run_id = r.id AND t.seq = (SELECT max(seq) FROM transitions WHERE run_id = r.id)
WHERE COALESCE(json_extract(t.to_state, '$.Phase'), json_extract(r.initial, '$.Phase')) NOT IN ('returned', 'stopped')
OR (json_extract(t.to_state, '$.Phase') = 'stopped'
	AND json_extract(t.to_state, '$.EffectID') != ''
	AND t.kind = 'resolved'
	AND json_extract(t.event, '$.Execution') = 'unknown') LIMIT 1`

// ValidKey reports whether id can name a session or an input: 1 to 128
// ASCII letters, digits, underscores, or hyphens.
func ValidKey(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range []byte(id) {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// CheckAdmission refuses a new run while any workspace run is unfinished or
// has a stopped effect whose execution outcome remains unknown.
func (s *Store) CheckAdmission(ctx context.Context) error {
	return s.checkAdmission(ctx, nil)
}

func (s *Store) checkAdmission(ctx context.Context, tx *sql.Tx) error {
	var blocked string
	var err error
	if tx == nil {
		err = s.db.QueryRowContext(ctx, admissionBlockSQL).Scan(&blocked)
	} else {
		err = tx.QueryRowContext(ctx, admissionBlockSQL).Scan(&blocked)
	}
	if err == nil {
		return fmt.Errorf("sqlite: run %q is unfinished or has an unknown effect", blocked)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// Submit creates one durable session input and its run in a single write transaction.
// Reusing an input ID with the same goal and constraints returns the original run;
// generated run IDs, creation times, budgets, and host config are ignored on retry.
func (s *Store) Submit(ctx context.Context, run harness.Run, initial core.State) (harness.Run, error) {
	if !ValidKey(run.SessionID) || !ValidKey(run.InputID) || run.SessionSeq != 0 {
		return harness.Run{}, errors.New("sqlite: invalid session or input identity")
	}
	if _, _, err := encodeRun(run, initial); err != nil {
		return harness.Run{}, err
	}
	var result harness.Run
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var data string
		err := tx.QueryRowContext(ctx, `SELECT r.run FROM session_runs sr JOIN runs r ON r.id = sr.run_id WHERE sr.session_id = ? AND sr.input_id = ?`, run.SessionID, run.InputID).Scan(&data)
		if err == nil {
			if err := strict(data, &result); err != nil {
				return err
			}
			if result.Goal != run.Goal || result.Constraints != run.Constraints {
				return fmt.Errorf("sqlite: input %q conflicts with its stored goal or constraints", run.InputID)
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := s.checkAdmission(ctx, tx); err != nil {
			return err
		}
		var seq int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(max(seq), 0) + 1 FROM session_runs WHERE session_id = ?`, run.SessionID).Scan(&seq); err != nil {
			return err
		}
		run.SessionSeq = seq
		data, state, err := encodeRun(run, initial)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insertRunSQL, run.ID, core.SemanticsVersion, data, state); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO session_runs (session_id, seq, input_id, run_id) VALUES (?, ?, ?, ?)`, run.SessionID, seq, run.InputID, run.ID); err != nil {
			return err
		}
		result = run
		result.Created = run.Created.UTC()
		return nil
	})
	if err != nil {
		return harness.Run{}, err
	}
	return result, nil
}

// SessionInput returns the run first admitted for this input identity.
func (s *Store) SessionInput(ctx context.Context, sessionID, inputID string) (harness.Run, error) {
	if !ValidKey(sessionID) || !ValidKey(inputID) {
		return harness.Run{}, errors.New("sqlite: invalid session or input identity")
	}
	return s.run(s.db.QueryRowContext(ctx, `SELECT r.run FROM session_runs sr JOIN runs r ON r.id = sr.run_id WHERE sr.session_id = ? AND sr.input_id = ?`, sessionID, inputID))
}

// SessionRuns returns newest-first runs, excluding before when positive.
func (s *Store) SessionRuns(ctx context.Context, sessionID string, before int64, limit int) ([]harness.Run, error) {
	if !ValidKey(sessionID) || before < 0 || limit <= 0 {
		return nil, errors.New("sqlite: invalid session page")
	}
	if limit > 32 {
		limit = 32
	}
	if before == 0 {
		before = int64(^uint64(0) >> 1)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.run FROM session_runs sr JOIN runs r ON r.id = sr.run_id WHERE sr.session_id = ? AND sr.seq < ? ORDER BY sr.seq DESC LIMIT ?`, sessionID, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := make([]harness.Run, 0, limit)
	for rows.Next() {
		var data string
		var run harness.Run
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		if err := strict(data, &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}
