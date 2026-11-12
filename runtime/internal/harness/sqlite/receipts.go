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
    UNION ALL SELECT seq FROM (SELECT seq FROM transitions WHERE run_id=?1 AND kind='response_loaded'
        AND (CASE WHEN json_valid(facts) THEN json_type(facts,'$.Supersede') END)='object' AND seq<?2 ORDER BY seq DESC LIMIT ?3)
    ORDER BY seq DESC LIMIT ?3
) SELECT t.seq,t.semantics,json_extract(t.from_state,'$.Step'),
    CASE WHEN (CASE WHEN json_valid(t.facts) THEN json_type(t.facts,'$.Supersede') END)='object'
        THEN substr(json_extract(t.facts,'$.Supersede.Purpose'),1,4096)
        ELSE substr(json_extract(t.from_state,'$.Decision.purpose'),1,4096) END,
    CASE WHEN (CASE WHEN json_valid(t.facts) THEN json_type(t.facts,'$.Supersede') END)='object' THEN 'superseded'
        ELSE COALESCE(json_extract(t.facts,'$.Outcome.Status'),json_extract(t.facts,'$.Observation.Execution')) END,
    json_extract(t.facts,'$.Observation.ExitCode'),
    COALESCE(json_extract(t.facts,'$.Resolution.Source')='operator',0),
    COALESCE(json_extract(t.facts,'$.Supersede.Instruction'),0),
    COALESCE(json_extract(t.facts,'$.Supersede.Kind'),''),
    CASE WHEN (CASE WHEN json_valid(t.facts) THEN json_type(t.facts,'$.Outcome.Retrieval') END)='object'
        THEN json_extract(t.facts,'$.Outcome.Retrieval.query.operation') END,
    CASE WHEN (CASE WHEN json_valid(t.facts) THEN json_type(t.facts,'$.Outcome.Retrieval') END)='object'
        THEN json_extract(t.facts,'$.Outcome.Retrieval.query.path') END,
    CASE WHEN (CASE WHEN json_valid(t.facts) THEN json_type(t.facts,'$.Outcome.Retrieval') END)='object'
        THEN json_extract(t.facts,'$.Outcome.Retrieval.query.offset') END,
    CASE WHEN (CASE WHEN json_valid(t.facts) THEN json_type(t.facts,'$.Outcome.Retrieval') END)='object'
        THEN json_extract(t.facts,'$.Outcome.Retrieval.query.limit') END
    FROM selected CROSS JOIN transitions t INDEXED BY sqlite_autoindex_transitions_1
    WHERE t.run_id=?1 AND t.seq=selected.seq ORDER BY t.seq DESC`

func (s *Store) Receipts(ctx context.Context, runID string, before int64, limit int) ([]harness.StepReceipt, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("sqlite: invalid receipt page limit")
	}
	if before <= 0 {
		before = math.MaxInt64
	}
	rows, err := s.stmts[receiptSQL].QueryContext(ctx, runID, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []harness.StepReceipt
	for rows.Next() {
		var r harness.StepReceipt
		var semantics int
		var purpose, kind, operation, retrievalPath sql.NullString
		var code, offset, limit sql.NullInt64
		if err := rows.Scan(&r.Seq, &semantics, &r.Step, &purpose, &r.Status, &code, &r.Resolved, &r.SupersededBy, &kind,
			&operation, &retrievalPath, &offset, &limit); err != nil {
			return nil, err
		}
		if err := core.KnownSemantics(semantics); err != nil {
			return nil, err
		}
		if r.Seq < 1 || r.Step < 1 {
			return nil, fmt.Errorf("%w: invalid receipt identity", ErrIncompatible)
		}
		r.Purpose = purpose.String
		r.DecisionKind = core.Kind(kind.String)
		if code.Valid {
			n := int(code.Int64)
			r.ExitCode = &n
		}
		if r.SupersededBy > 0 && (r.DecisionKind != core.Exec && r.DecisionKind != core.Return || r.Status != "superseded") {
			return nil, fmt.Errorf("%w: invalid superseded receipt at sequence %d", ErrIncompatible, r.Seq)
		}
		if operation.Valid || retrievalPath.Valid {
			r.Retrieval = &harness.ReceiptRetrieval{Operation: operation.String, Path: retrievalPath.String, Offset: offset.Int64, Limit: limit.Int64}
		}
		out = append(out, r)
	}
	slices.Reverse(out)
	return out, rows.Err()
}
