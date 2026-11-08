package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// Select identities through the completion indexes before extracting bounded
// fields. No command, retrieval body, or full facts object crosses the SQL boundary.
const receiptSQL = `WITH selected AS (
    SELECT seq FROM (SELECT seq FROM transitions WHERE run_id=?1 AND kind IN ('observed','resolved') AND seq<?2 ORDER BY seq DESC LIMIT ?3)
    UNION ALL SELECT seq FROM (SELECT seq FROM transitions WHERE run_id=?1 AND kind='performed' AND seq<?2 ORDER BY seq DESC LIMIT ?3)
    ORDER BY seq DESC LIMIT ?3
) SELECT t.seq,t.semantics,json_extract(t.from_state,'$.Step'),
    substr(json_extract(t.from_state,'$.Decision.Purpose'),1,4096),
    COALESCE(json_extract(t.facts,'$.Outcome.Status'),json_extract(t.facts,'$.Observation.Execution')),
    json_extract(t.facts,'$.Observation.ExitCode'),
    COALESCE(json_extract(t.facts,'$.Resolution.Source')='operator',0)
    FROM selected JOIN transitions t ON t.run_id=?1 AND t.seq=selected.seq ORDER BY t.seq DESC`

func (s *Store) Receipts(ctx context.Context, runID string, before int64, limit int) ([]harness.StepReceipt, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("sqlite: invalid receipt page limit")
	}
	if before <= 0 {
		before = math.MaxInt64
	}
	rows, err := s.db.QueryContext(ctx, receiptSQL, runID, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []harness.StepReceipt
	for rows.Next() {
		var r harness.StepReceipt
		var semantics int
		var purpose sql.NullString
		var code sql.NullInt64
		if err := rows.Scan(&r.Seq, &semantics, &r.Step, &purpose, &r.Status, &code, &r.Resolved); err != nil {
			return nil, err
		}
		if err := core.KnownSemantics(semantics); err != nil {
			return nil, err
		}
		if r.Seq < 1 || r.Step < 1 {
			return nil, fmt.Errorf("%w: invalid receipt identity", ErrIncompatible)
		}
		r.Purpose = purpose.String
		if code.Valid {
			n := int(code.Int64)
			r.ExitCode = &n
		}
		out = append(out, r)
	}
	slices.Reverse(out)
	return out, rows.Err()
}
