package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// ProgressRow is what a progress projection reads of one transition: its
// position, kind, and resulting phase, and the few fields of its event,
// states, and facts that describe a step to a reader. It never holds request
// or response bodies or retrieved evidence, which SQLite reads past instead
// of returning, and every text is clipped in SQL to its cap, then cut at a
// rune boundary, with Truncated set when any text was cut, so a row is
// bounded by its caps whatever the journal holds.
type ProgressRow struct {
	Pos              int64
	RunID            string
	Seq              int64
	Kind             string
	Semantics        int
	Phase            core.Phase
	Step             int // of the state the transition leaves
	Reason           core.StopReason
	EffectID         string
	External         bool // completion carries an executor receipt
	ResolutionSource string
	Failure          string

	// The decision of a response_loaded row.
	Decision core.Decision
	// The recorded action of an accepting response_loaded row.
	Action string
	// The instruction that superseded a response_loaded row, and its decision
	// kind.
	SupersedeBy   int
	SupersedeKind core.Kind
	// The bound instruction count after an inputs_bound row.
	Inputs int

	// The observation of an observed or resolved row.
	Execution core.ExecutionState
	ExitCode  *int
	Preview   string
	Detail    string

	// The outcome of a performed row.
	Outcome        string
	OutcomeDetail  string
	RetrievalError string
	Sources        int
	Entries        int
	Focus          bool
	FocusSources   int
	Retrieval      bool
	AnchorSeq      int64

	Cut ProgressFields // the texts that were longer than their caps and are cut
}

// ProgressFields names texts of a ProgressRow.
type ProgressFields uint16

// The texts of a ProgressRow that its caps can cut. A name or an identifier
// of a closed set that is cut is ProgressName, whichever it is.
const (
	ProgressPayload ProgressFields = 1 << iota // Decision.Payload
	ProgressPurpose                            // Decision.Purpose
	ProgressFailure
	ProgressPreview
	ProgressDetail
	ProgressOutcomeDetail
	ProgressRetrievalError
	ProgressName
	ProgressInputText
	ProgressInputKey
)

// The caps, in bytes, of the texts of a ProgressRow: a command or a purpose,
// the payload of a return, a preview, any other text (a failure or a
// detail), and a name from a closed set or an identifier.
const (
	ProgressCommandBytes = 2 << 10
	ProgressResultBytes  = 8 << 10
	ProgressPreviewBytes = 4 << 10
	ProgressTextBytes    = 2 << 10
	ProgressNameBytes    = 256
)

// ProgressRowBytes bounds the texts of one ProgressRow.
const ProgressRowBytes = ProgressResultBytes + ProgressCommandBytes + ProgressPreviewBytes + 5*ProgressTextBytes + 9*ProgressNameBytes + harness.InputKeyBytes

// clipped selects expr, a text, as at most limit+1 of its bytes, so that the
// reader can tell a cut one.
func clipped(expr string, limit int) string {
	return fmt.Sprintf("substr(CAST(%s AS BLOB), 1, %d)", expr, limit+1)
}

var progressColumns = `pos, run_id, seq, kind, semantics, ` + strings.Join([]string{
	clipped(`json_extract(to_state, '$.Phase')`, ProgressNameBytes),
	`json_extract(from_state, '$.Step')`,
	clipped(`json_extract(to_state, '$.Reason')`, ProgressNameBytes),
	clipped(`json_extract(from_state, '$.EffectID')`, ProgressNameBytes),
	`json_type(facts, '$.Receipt')`,
	clipped(`json_extract(facts, '$.Resolution.Source')`, ProgressNameBytes),
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(to_state, '$.Decision.kind')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(to_state, '$.Decision.payload')`, ProgressResultBytes) + ` END`,
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(to_state, '$.Decision.purpose')`, ProgressCommandBytes) + ` END`,
	`CASE WHEN kind IN ('observed', 'resolved', 'performed') THEN ` + clipped(`json_extract(from_state, '$.Decision.payload')`, ProgressCommandBytes) + ` END`,
	`CASE WHEN kind IN ('response_incomplete', 'request_rejected', 'outcome_unknown', 'stop_requested') THEN ` + clipped(`json_extract(facts, '$.Failure')`, ProgressTextBytes) + ` END`,
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(facts, '$.Action.Name')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(facts, '$.Supersede.Instruction')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(facts, '$.Supersede.Kind')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind = 'inputs_bound' THEN ` + clipped(`json_extract(to_state, '$.Inputs')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind IN ('observed', 'resolved') THEN ` + clipped(`json_extract(facts, '$.Observation.Execution')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind IN ('observed', 'resolved') THEN json_extract(facts, '$.Observation.ExitCode') END`,
	`CASE WHEN kind IN ('observed', 'resolved') THEN ` + clipped(`json_extract(facts, '$.Observation.Preview')`, ProgressPreviewBytes) + ` END`,
	`CASE WHEN kind IN ('observed', 'resolved') THEN ` + clipped(`json_extract(facts, '$.Observation.Output.Detail')`, ProgressTextBytes) + ` END`,
	`CASE WHEN kind = 'performed' THEN ` + clipped(`json_extract(facts, '$.Outcome.Status')`, ProgressNameBytes) + ` END`,
	`CASE WHEN kind = 'performed' THEN ` + clipped(`json_extract(facts, '$.Outcome.Detail')`, ProgressTextBytes) + ` END`,
	`CASE WHEN kind = 'performed' THEN ` + clipped(`json_extract(facts, '$.Outcome.Retrieval.error')`, ProgressTextBytes) + ` END`,
	`CASE WHEN kind = 'performed' THEN json_array_length(facts, '$.Outcome.Retrieval.sources') END`,
	`CASE WHEN kind = 'performed' THEN json_array_length(facts, '$.Outcome.Retrieval.entries') END`,
	`CASE WHEN kind = 'performed' THEN json_type(facts, '$.Outcome.Retrieval') END`,
	`CASE WHEN kind = 'performed' THEN json_type(facts, '$.Outcome.Focus') END`,
	`CASE WHEN kind = 'performed' THEN json_array_length(facts, '$.Outcome.Focus.Sources') END`,
	`json_extract(facts, '$.ContextState.AnchorSeq')`,
}, ",\n\t")

var progressSQL = `SELECT ` + progressColumns + `
	FROM transitions WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?`

// logTransitionsSQL pages the agent log across runs by position.
var logTransitionsSQL = `SELECT ` + progressColumns + `
	FROM transitions WHERE pos > ? ORDER BY pos LIMIT ?`

// logWithdrawalsSQL pages one withdrawal fact per transaction, naming at
// most withdrawalNames of the inputs it took back. A group is one log row;
// names are ordered by the withdrawn input's own position.
var logWithdrawalsSQL = `WITH page AS (
	SELECT withdrawn_pos FROM inputs WHERE withdrawn_pos > ? GROUP BY withdrawn_pos ORDER BY withdrawn_pos LIMIT ?
), rows AS (
	SELECT i.withdrawn_pos,
		COALESCE(i.input_id, 'pos:' || i.pos) AS name,
		row_number() OVER (PARTITION BY i.withdrawn_pos ORDER BY i.pos) AS n,
		count(*) OVER (PARTITION BY i.withdrawn_pos) AS total
	FROM inputs i JOIN page p ON p.withdrawn_pos = i.withdrawn_pos
)
SELECT withdrawn_pos, name, n, total FROM rows WHERE n <= ? ORDER BY withdrawn_pos, n`

// withdrawalNames bounds the inputs one withdrawn item names.
const withdrawalNames = 64

// logInputsSQL pages admitted inputs by position, with their bounded text.
const runPosSQL = `SELECT COALESCE(max(pos), 0) FROM transitions WHERE run_id = ?`

var logInputsSQL = `SELECT pos, COALESCE(run_id, ''), authority, delivery, disposition, COALESCE(input_id, ''),
	` + clipped(`text`, ProgressCommandBytes) + `, ` + clipped(`source`, harness.InputKeyBytes) + `, ` + clipped(`subject`, harness.InputKeyBytes) + `
	FROM inputs WHERE pos > ? ORDER BY pos LIMIT ?`

// Progress returns at most limit transitions of the run after sequence
// after, in commit order, as ProgressRows: one search of the primary key that
// extracts and clips fields instead of decoding records. Each row's kind and
// semantics version are checked as every read checks them; the fields are
// only shown, so they are not decoded strictly. An unknown run is
// ErrNotFound.
func (s *Store) Progress(ctx context.Context, runID string, after int64, limit int) ([]ProgressRow, error) {
	rows, err := s.stmts[progressSQL].QueryContext(ctx, runID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProgressRow
	found := false
	for rows.Next() {
		found = true
		r, err := scanProgress(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !found {
		if _, err := s.Head(ctx, runID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// LogEntry is one agent-log row: a committed transition or an admitted input.
type LogEntry struct {
	Pos          int64
	RunID        string
	Transition   *ProgressRow
	Input        *harness.Input
	InputCut     bool
	Withdrawn    []string
	WithdrawnCut bool
}

// Log returns at most limit agent-log rows after position after, in position
// order, merging transitions and inputs under one read transaction. Every
// text is bounded; no request, response, or evidence body crosses the SQL
// boundary.
func (s *Store) Log(ctx context.Context, after int64, limit int) ([]LogEntry, error) {
	if after < 0 || limit <= 0 || limit > 256 {
		return nil, fmt.Errorf("sqlite: invalid log page")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	transitions, err := s.logTransitions(ctx, tx, after, limit)
	if err != nil {
		return nil, err
	}
	inputs, err := s.logInputs(ctx, tx, after, limit)
	if err != nil {
		return nil, err
	}
	withdrawals, err := s.logWithdrawals(ctx, tx, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]LogEntry, 0, len(transitions)+len(inputs)+len(withdrawals))
	out = append(out, transitions...)
	out = append(out, inputs...)
	out = append(out, withdrawals...)
	sort.Slice(out, func(i, j int) bool { return out[i].Pos < out[j].Pos })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, tx.Commit()
}

func (s *Store) logTransitions(ctx context.Context, tx *sql.Tx, after int64, limit int) ([]LogEntry, error) {
	rows, err := tx.QueryContext(ctx, logTransitionsSQL, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogEntry
	for rows.Next() {
		r, err := scanProgress(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, LogEntry{Pos: r.Pos, RunID: r.RunID, Transition: &r})
	}
	return out, rows.Err()
}

func (s *Store) logWithdrawals(ctx context.Context, tx *sql.Tx, after int64, limit int) ([]LogEntry, error) {
	rows, err := tx.QueryContext(ctx, logWithdrawalsSQL, after, limit, withdrawalNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogEntry
	index := map[int64]int{}
	for rows.Next() {
		var pos, n, total int64
		var name string
		if err := rows.Scan(&pos, &name, &n, &total); err != nil {
			return nil, err
		}
		i, ok := index[pos]
		if !ok {
			i = len(out)
			index[pos] = i
			out = append(out, LogEntry{Pos: pos, WithdrawnCut: total > withdrawalNames})
		}
		out[i].Withdrawn = append(out[i].Withdrawn, name)
	}
	return out, rows.Err()
}

func (s *Store) logInputs(ctx context.Context, tx *sql.Tx, after int64, limit int) ([]LogEntry, error) {
	rows, err := tx.QueryContext(ctx, logInputsSQL, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogEntry
	for rows.Next() {
		var e LogEntry
		var in harness.Input
		var authority, delivery, disposition string
		var text, source, subject []byte
		if err := rows.Scan(&in.Pos, &in.RunID, &authority, &delivery, &disposition, &in.InputID, &text, &source, &subject); err != nil {
			return nil, err
		}
		cut := ProgressFields(0)
		in.Authority, in.Delivery, in.Disposition = harness.InputAuthority(authority), harness.InputDelivery(delivery), harness.InputDisposition(disposition)
		in.Text = textOf(text, ProgressCommandBytes, &cut, ProgressInputText)
		in.Source = textOf(source, harness.InputKeyBytes, &cut, ProgressInputKey)
		in.Subject = textOf(subject, harness.InputKeyBytes, &cut, ProgressInputKey)
		e.Pos, e.RunID, e.Input, e.InputCut = in.Pos, in.RunID, &in, cut != 0
		out = append(out, e)
	}
	return out, rows.Err()
}

// text returns b, at most limit+1 bytes of a clipped text, cut to at most
// limit bytes at a rune boundary, and adds field to *cut when it was longer.
func text(b []byte, limit int, cut *ProgressFields, field ProgressFields) string {
	return textOf(b, limit, cut, field)
}

func textOf(b []byte, limit int, cut *ProgressFields, field ProgressFields) string {
	if len(b) <= limit {
		return string(b)
	}
	*cut |= field
	i := limit
	for i > 0 && !utf8.RuneStart(b[i]) {
		i--
	}
	return string(b[:i])
}

// number reads a clipped integer expression.
func number(b []byte) int {
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0
	}
	return n
}

func scanProgress(rows *sql.Rows) (ProgressRow, error) {
	var r ProgressRow
	var phase, reason, effect, resolution, decision, payload, purpose, command, failure, action, supersede, supersedeBy, inputs, execution, preview, detail []byte
	var outcome, outcomeDetail, retrievalError []byte
	var retrieval, focus, receipt sql.NullString
	var step, exitCode, sources, entries, focusSources, anchor sql.NullInt64
	err := rows.Scan(&r.Pos, &r.RunID, &r.Seq, &r.Kind, &r.Semantics, &phase, &step, &reason, &effect, &receipt, &resolution, &decision, &payload, &purpose, &command,
		&failure, &action, &supersedeBy, &supersede, &inputs, &execution, &exitCode, &preview, &detail, &outcome, &outcomeDetail, &retrievalError, &sources, &entries,
		&retrieval, &focus, &focusSources, &anchor)
	if err != nil {
		return r, err
	}
	if err := known(r.Kind, r.Semantics); err != nil {
		return r, err
	}
	if r.Pos < 1 {
		return r, fmt.Errorf("%w: transition without a log position", ErrIncompatible)
	}
	cut := &r.Cut
	name := func(b []byte) string { return text(b, ProgressNameBytes, cut, ProgressName) }
	r.Phase, r.Step, r.Reason, r.EffectID = core.Phase(name(phase)), int(step.Int64), core.StopReason(name(reason)), name(effect)
	r.Failure = text(failure, ProgressTextBytes, cut, ProgressFailure)
	switch {
	case r.Kind == "response_loaded" && r.Phase == core.PhaseReturned:
		r.Decision = core.Decision{Kind: core.Kind(name(decision)), Payload: text(payload, ProgressResultBytes, cut, ProgressPayload),
			Purpose: text(purpose, ProgressCommandBytes, cut, ProgressPurpose)}
	case r.Kind == "response_loaded":
		r.Decision = core.Decision{Kind: core.Kind(name(decision)), Payload: text(payload, ProgressCommandBytes, cut, ProgressPayload),
			Purpose: text(purpose, ProgressCommandBytes, cut, ProgressPurpose)}
	default:
		r.Decision = core.Decision{Payload: text(command, ProgressCommandBytes, cut, ProgressPayload)}
	}
	r.Action = name(action)
	r.SupersedeBy, r.SupersedeKind = number(supersedeBy), core.Kind(name(supersede))
	r.Inputs = number(inputs)
	r.Execution = core.ExecutionState(name(execution))
	r.Preview, r.Detail = text(preview, ProgressPreviewBytes, cut, ProgressPreview), text(detail, ProgressTextBytes, cut, ProgressDetail)
	if exitCode.Valid {
		code := int(exitCode.Int64)
		r.ExitCode = &code
	}
	r.Outcome = name(outcome)
	r.OutcomeDetail = text(outcomeDetail, ProgressTextBytes, cut, ProgressOutcomeDetail)
	r.RetrievalError = text(retrievalError, ProgressTextBytes, cut, ProgressRetrievalError)
	r.Sources, r.Entries, r.FocusSources = int(sources.Int64), int(entries.Int64), int(focusSources.Int64)
	r.AnchorSeq = anchor.Int64
	r.Retrieval, r.Focus = retrieval.String == "object", focus.String == "object"
	r.External, r.ResolutionSource = receipt.String == "object", name(resolution)
	return r, nil
}

// LastPos is the latest position of the agent log, zero when it is empty.
func (s *Store) LastPos(ctx context.Context) (int64, error) {
	var pos int64
	if err := s.stmts[lastPosSQL].QueryRowContext(ctx).Scan(&pos); err != nil {
		return 0, err
	}
	return pos, nil
}

// RunPos is the agent-log position of the latest transition of runID, zero
// before its first transition. An unknown run is harness.ErrNotFound.
func (s *Store) RunPos(ctx context.Context, runID string) (int64, error) {
	var pos int64
	err := s.db.QueryRowContext(ctx, runPosSQL, runID).Scan(&pos)
	if err != nil {
		return 0, err
	}
	var semantics int
	var state string
	if err := s.stmts[initialSQL].QueryRowContext(ctx, runID).Scan(&semantics, &state); err != nil {
		return 0, missing(err, "run")
	}
	return pos, nil
}
