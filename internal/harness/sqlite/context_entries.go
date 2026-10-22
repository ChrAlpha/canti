package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func appendContextEntry(ctx context.Context, tx *sql.Tx, owner string, e harness.ContextEntry) error {
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(max(n),0)+1 FROM context_entries WHERE owner=?", owner).Scan(&e.Index); err != nil {
		return err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO context_entries(owner,n,run_id,source_seq,data) VALUES(?,?,?,?,?)", owner, e.Index, e.RunID, e.SourceSeq, string(data))
	return err
}

func appendContextInput(ctx context.Context, tx *sql.Tx, r harness.Run) error {
	return appendContextEntry(ctx, tx, r.ContextOwner(), harness.ContextEntry{SessionSeq: r.SessionSeq, RunID: r.ID, Kind: "input", Goal: r.Goal, Constraints: r.Constraints})
}

func (s *Store) appendContextTransition(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	e := harness.ContextEntry{RunID: t.From.RunID, SourceSeq: t.Seq}
	switch t.Event.(type) {
	case core.Observed, core.Resolved:
		if t.Facts.Observation == nil {
			return errors.New("sqlite: completed step has no observation")
		}
		if t.Facts.Observation.Execution == core.ExecutionUnknown {
			return nil
		}
		e.Kind = "step"
		e.Step = &harness.StepRecord{Seq: t.Seq, Step: t.From.Step, Command: t.From.Decision.Payload, Purpose: t.From.Decision.Purpose, Observation: *t.Facts.Observation, Resolved: t.Event.Kind() == "resolved"}
	}
	if t.To.Closed() && !t.From.Closed() {
		e.Phase, e.Reason = t.To.Phase, t.To.Reason
		if e.Kind == "" {
			e.Kind = "result"
		}
		if t.To.Phase == core.PhaseReturned {
			e.Result = t.To.Decision.Payload
		}
	}
	if e.Kind == "" {
		return nil
	}
	run, err := s.run(tx.QueryRowContext(ctx, runSQL, t.From.RunID))
	if err != nil {
		return err
	}
	e.SessionSeq = run.SessionSeq
	return appendContextEntry(ctx, tx, run.ContextOwner(), e)
}

// ContextEntries pages forward, bounded by both rows and persisted bytes.
// It never skips an oversized unit or silently truncates coverage.
func (s *Store) ContextEntries(ctx context.Context, owner string, after int64, limit, maxBytes int) ([]harness.ContextEntry, bool, error) {
	if owner == "" || after < 0 || limit < 1 || limit > 256 || maxBytes < 1 || maxBytes > 32<<20 {
		return nil, false, errors.New("sqlite: invalid context source page")
	}
	rows, err := s.db.QueryContext(ctx, "SELECT n,octet_length(data),CASE WHEN octet_length(data)<=? THEN data ELSE '' END FROM context_entries WHERE owner=? AND n>? ORDER BY n LIMIT ?", maxBytes, owner, after, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var entries []harness.ContextEntry
	total := 0
	for rows.Next() {
		var index int64
		var size int
		var data string
		if err := rows.Scan(&index, &size, &data); err != nil {
			return nil, false, err
		}
		if len(entries) == limit || total+size > maxBytes {
			if len(entries) == 0 {
				return nil, false, fmt.Errorf("sqlite: context source %d exceeds %d-byte read limit", index, maxBytes)
			}
			return entries, true, nil
		}
		var e harness.ContextEntry
		if err := strict(data, &e); err != nil {
			return nil, false, err
		}
		if err := e.Validate(); err != nil {
			return nil, false, fmt.Errorf("%w: %v", ErrIncompatible, err)
		}
		if e.Index != index || index != after+int64(len(entries))+1 {
			return nil, false, fmt.Errorf("%w: context source coverage gap", ErrIncompatible)
		}
		entries = append(entries, e)
		total += size
	}
	return entries, false, rows.Err()
}

func verifyCompactionSources(ctx context.Context, tx *sql.Tx, p harness.CompactPlan) error {
	var size int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(sum(octet_length(data)),0) FROM context_entries WHERE owner=? AND n>? AND n<=?", p.Owner, p.After, p.Through).Scan(&size); err != nil {
		return err
	}
	if size > 32<<20 {
		return errors.New("sqlite: compaction sources exceed the read bound")
	}
	rows, err := tx.QueryContext(ctx, "SELECT data FROM context_entries WHERE owner=? AND n>? AND n<=? ORDER BY n", p.Owner, p.After, p.Through)
	if err != nil {
		return err
	}
	defer rows.Close()
	var entries []harness.ContextEntry
	for rows.Next() {
		var data string
		var e harness.ContextEntry
		if err := rows.Scan(&data); err != nil {
			return err
		}
		if err := strict(data, &e); err != nil {
			return err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if harness.ContextSourceHash(entries) != p.SourceHash {
		return errors.New("sqlite: compaction source hash differs from its committed coverage")
	}
	return nil
}
