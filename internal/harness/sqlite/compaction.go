package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

const compactSchema = `
CREATE TABLE context_entries (
 owner TEXT NOT NULL, n INTEGER NOT NULL, run_id TEXT NOT NULL REFERENCES runs(id),
 source_seq INTEGER NOT NULL, data TEXT NOT NULL,
 PRIMARY KEY(owner,n), UNIQUE(run_id,source_seq)
);
CREATE TABLE context_heads (owner TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE compactions (
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), run_seq INTEGER NOT NULL,
 data TEXT NOT NULL, parent_id TEXT NOT NULL, UNIQUE(run_id,run_seq,parent_id)
);
`

var _ harness.CompactionJournal = (*Store)(nil)

func (s *Store) ActiveCheckpoint(ctx context.Context, owner string) (harness.Checkpoint, error) {
	return readCheckpoint(ctx, s.db, owner)
}

type contextQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readCheckpoint(ctx context.Context, q contextQuerier, owner string) (harness.Checkpoint, error) {
	var c harness.Checkpoint
	var data string
	err := q.QueryRowContext(ctx, "SELECT data FROM context_heads WHERE owner = ?", owner).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return c, nil
	}
	if err == nil {
		err = strict(data, &c)
	}
	if err == nil && (c.Owner != owner || c.ID == "" || c.Through < 1 || harness.ValidateSummary(c.Summary, 64<<10) != nil) {
		err = fmt.Errorf("%w: invalid checkpoint", ErrIncompatible)
	}
	return c, err
}

func (s *Store) PendingCompaction(ctx context.Context, runID string, runSeq int64) (harness.Compaction, error) {
	var c harness.Compaction
	var data string
	err := s.db.QueryRowContext(ctx, "SELECT data FROM compactions WHERE run_id = ? AND run_seq = ? ORDER BY rowid DESC LIMIT 1", runID, runSeq).Scan(&data)
	if err == nil {
		err = decodeCompaction(data, &c)
	}
	return c, missing(err, "compaction")
}

func readCompaction(ctx context.Context, tx *sql.Tx, id string) (harness.Compaction, error) {
	var c harness.Compaction
	var data string
	err := tx.QueryRowContext(ctx, "SELECT data FROM compactions WHERE id = ?", id).Scan(&data)
	if err == nil {
		err = decodeCompaction(data, &c)
	}
	return c, missing(err, "compaction")
}

func writeCompaction(ctx context.Context, tx *sql.Tx, c harness.Compaction) error {
	if err := c.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE compactions SET data = ? WHERE id = ?", string(data), c.Plan.ID)
	return err
}

func (s *Store) compactPosition(ctx context.Context, tx *sql.Tx, p harness.CompactPlan) error {
	snap, err := s.latest(ctx, tx, p.RunID)
	if err != nil {
		return err
	}
	if snap.Seq != p.RunSeq || snap.State.Intent() != core.IntentPrepareRequest || snap.State.RequestRev != 0 {
		return fmt.Errorf("%w: compaction requires its original request preparation position", harness.ErrStaleSnapshot)
	}
	run, err := s.run(tx.QueryRowContext(ctx, runSQL, p.RunID))
	if err != nil {
		return err
	}
	head, err := readCheckpoint(ctx, tx, p.Owner)
	if err != nil {
		return err
	}
	if run.ContextOwner() != p.Owner || head.ID != p.Base {
		return fmt.Errorf("%w: compaction source owner or parent changed", harness.ErrStaleSnapshot)
	}
	if p.Parent == head.ID {
		if p.After != head.Through {
			return harness.ErrStaleSnapshot
		}
		return nil
	}
	parent, err := readCompaction(ctx, tx, p.Parent)
	if err != nil {
		return err
	}
	if parent.Plan.RunID != p.RunID || parent.Plan.RunSeq != p.RunSeq || parent.Plan.Base != p.Base || parent.Plan.Through != p.After || parent.Summary == "" {
		return harness.ErrStaleSnapshot
	}
	return nil
}

func (s *Store) PlanCompaction(ctx context.Context, p harness.CompactPlan) error {
	if err := p.Validate(); err != nil {
		return err
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		old, err := readCompaction(ctx, tx, p.ID)
		if err == nil && reflect.DeepEqual(old.Plan, p) {
			return nil
		}
		if !errors.Is(err, harness.ErrNotFound) {
			if err == nil {
				return harness.ErrStaleSnapshot
			}
			return err
		}
		if err := s.compactPosition(ctx, tx, p); err != nil {
			return err
		}
		var count int64
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM context_entries WHERE owner = ? AND n > ? AND n <= ?", p.Owner, p.After, p.Through).Scan(&count); err != nil {
			return err
		}
		if count != p.Through-p.After {
			return errors.New("sqlite: compaction coverage is not contiguous")
		}
		if err := verifyCompactionSources(ctx, tx, p); err != nil {
			return err
		}
		data, err := json.Marshal(harness.Compaction{Plan: p})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO compactions(id,run_id,run_seq,data,parent_id) VALUES(?,?,?,?,?)", p.ID, p.RunID, p.RunSeq, string(data), p.Parent)
		return err
	})
}

func (s *Store) StartCompactionAttempt(ctx context.Context, id string, number int) error {
	return s.changeCompaction(ctx, id, func(ctx context.Context, tx *sql.Tx, c *harness.Compaction) error {
		if number < 1 || number > c.Plan.MaxAttempts || c.Summary != "" {
			return errors.New("sqlite: invalid compaction attempt")
		}
		if number <= len(c.Attempts) {
			return nil
		}
		if number != len(c.Attempts)+1 {
			return errors.New("sqlite: compaction attempt skipped")
		}
		if n := len(c.Attempts); n > 0 && !c.Attempts[n-1].Done {
			c.Attempts[n-1].Done = true
			c.Attempts[n-1].Failure = "interrupted attempt; response and usage unknown"
			c.Attempts[n-1].UnknownUsage = true
		}
		c.Attempts = append(c.Attempts, harness.CompactAttempt{Number: number, UnknownUsage: true})
		_, err := tx.ExecContext(ctx, "UPDATE runs SET compact_reserved = compact_reserved + ? WHERE id = ?", c.Plan.TokenReserve, c.Plan.RunID)
		return err
	})
}

func (s *Store) SaveCompactionAttempt(ctx context.Context, id string, result harness.CompactAttempt) error {
	return s.changeCompaction(ctx, id, func(ctx context.Context, tx *sql.Tx, c *harness.Compaction) error {
		if err := result.Validate(); err != nil {
			return err
		}
		n := result.Number
		if n < 1 || n != len(c.Attempts) || !result.Done {
			return errors.New("sqlite: invalid compaction response position")
		}
		if c.Attempts[n-1].Done {
			if reflect.DeepEqual(c.Attempts[n-1], result) {
				return nil
			}
			return harness.ErrStaleSnapshot
		}
		if !result.UnknownUsage && (result.Response == nil || !result.Response.UsageKnown || result.Response.Usage.InputTokens < 0 || result.Response.Usage.OutputTokens < 0) {
			return errors.New("sqlite: missing compaction usage must remain unknown")
		}
		c.Attempts[n-1] = result
		if result.UnknownUsage {
			return nil
		}
		u := result.Response.Usage
		_, err := tx.ExecContext(ctx, `UPDATE runs SET input_tokens=input_tokens+?, output_tokens=output_tokens+?, compact_reserved=compact_reserved-?, compact_input=compact_input+?, compact_output=compact_output+? WHERE id=?`, u.InputTokens, u.OutputTokens, c.Plan.TokenReserve, u.InputTokens, u.OutputTokens, c.Plan.RunID)
		return err
	})
}

func (s *Store) FinishCompaction(ctx context.Context, id, summary string) error {
	return s.changeCompaction(ctx, id, func(_ context.Context, _ *sql.Tx, c *harness.Compaction) error {
		if err := c.Plan.ValidateResult(summary); err != nil {
			return err
		}
		n := len(c.Attempts)
		if n == 0 || !c.Attempts[n-1].Done || c.Attempts[n-1].Failure != "" || c.Attempts[n-1].Response == nil || !c.Attempts[n-1].Response.Complete {
			return errors.New("sqlite: summary has no complete successful response")
		}
		if c.Summary != "" && c.Summary != summary {
			return harness.ErrStaleSnapshot
		}
		c.Summary = summary
		return nil
	})
}

func (s *Store) changeCompaction(ctx context.Context, id string, change func(context.Context, *sql.Tx, *harness.Compaction) error) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		c, err := readCompaction(ctx, tx, id)
		if err != nil {
			return err
		}
		if c.Accepted {
			return errors.New("sqlite: compaction already adopted")
		}
		if err := s.compactPosition(ctx, tx, c.Plan); err != nil {
			return err
		}
		if err := change(ctx, tx, &c); err != nil {
			return err
		}
		return writeCompaction(ctx, tx, c)
	})
}

// adoptCompaction is part of the same transaction as the business request.
func (s *Store) adoptCompaction(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	if t.Facts.Request == nil {
		return nil
	}
	if _, ok := t.Event.(core.RequestFrozen); !ok {
		return errors.New("sqlite: checkpoint adoption requires RequestFrozen")
	}
	id := t.Facts.Request.CheckpointID
	run, err := s.run(tx.QueryRowContext(ctx, runSQL, t.From.RunID))
	if err != nil {
		return err
	}
	head, err := readCheckpoint(ctx, tx, run.ContextOwner())
	if err != nil {
		return err
	}
	if id == "" {
		if head.ID != "" {
			return errors.New("sqlite: working request omitted the active checkpoint")
		}
		return nil
	}
	if head.ID == id {
		return matchesCheckpoint(t.Facts.Request, head)
	}
	c, err := readCompaction(ctx, tx, id)
	if err != nil {
		return err
	}
	if c.Plan.RunID != run.ID || c.Plan.RunSeq != t.Seq-1 || c.Plan.Base != head.ID || c.Plan.Owner != run.ContextOwner() || c.Accepted || c.Summary == "" {
		return fmt.Errorf("%w: checkpoint cannot be adopted by this request", harness.ErrStaleSnapshot)
	}
	if t.From.Intent() != core.IntentPrepareRequest || t.From.RequestRev != 0 {
		return harness.ErrStaleSnapshot
	}
	if err := matchesCheckpoint(t.Facts.Request, c.Checkpoint()); err != nil {
		return err
	}
	data, err := json.Marshal(c.Checkpoint())
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO context_heads(owner,data) VALUES(?,?) ON CONFLICT(owner) DO UPDATE SET data=excluded.data", run.ContextOwner(), string(data)); err != nil {
		return err
	}
	c.Accepted = true
	return writeCompaction(ctx, tx, c)
}

func matchesCheckpoint(r *harness.Request, c harness.Checkpoint) error {
	if r.Window == nil || r.Window.Through != c.Through || r.Window.Summary != c.Summary {
		return errors.New("sqlite: request context differs from its checkpoint")
	}
	return nil
}

func decodeCompaction(data string, c *harness.Compaction) error {
	if err := strict(data, c); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	return nil
}
