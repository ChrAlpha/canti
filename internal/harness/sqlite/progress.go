package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
)

// ProgressRow is what a progress projection reads of one transition: its
// position, kind, and resulting phase, and the few fields of its event,
// states, and facts that describe a step to a reader. It never holds request
// or response bodies or retrieved evidence, which SQLite reads past instead
// of returning, and every text is clipped in SQL to its cap, then cut at a
// rune boundary, with Truncated set when any text was cut, so a row is
// bounded by its caps whatever the journal holds.
type ProgressRow struct {
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

	// The observation of an observed or resolved row.
	Execution core.ExecutionState
	ExitCode  *int
	Preview   string
	Detail    string

	// The outcome of a performed row.
	Outcome         string
	OutcomeDetail   string
	RetrievalError  string
	Sources         int
	Entries         int
	Handover        bool
	Retrieval       bool
	WindowFirstStep int // of the window a performed row commits, 0 for none

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
const ProgressRowBytes = ProgressResultBytes + ProgressCommandBytes + ProgressPreviewBytes + 5*ProgressTextBytes + 9*ProgressNameBytes

// clipped selects expr, a text, as at most limit+1 of its bytes, so that the
// reader can tell a cut one.
func clipped(expr string, limit int) string {
	return fmt.Sprintf("substr(CAST(%s AS BLOB), 1, %d)", expr, limit+1)
}

var progressSQL = `SELECT seq, kind, semantics, ` + strings.Join([]string{
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
	`CASE WHEN kind IN ('response_incomplete', 'outcome_unknown', 'stop_requested') THEN ` + clipped(`json_extract(facts, '$.Failure')`, ProgressTextBytes) + ` END`,
	`CASE WHEN kind = 'response_loaded' THEN ` + clipped(`json_extract(facts, '$.Action.Name')`, ProgressNameBytes) + ` END`,
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
	`CASE WHEN kind = 'performed' THEN json_type(facts, '$.Outcome.Handover') END`,
	`CASE WHEN kind = 'performed' THEN json_extract(facts, '$.Window.FirstStep') END`,
}, ",\n\t") + `
	FROM transitions WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?`

// Progress returns at most limit transitions of the run after sequence
// after, in commit order, as ProgressRows: one search of the primary key that
// extracts and clips fields instead of decoding records. Each row's kind and
// semantics version are checked as every read checks them; the fields are
// only shown, so they are not decoded strictly. An unknown run is
// ErrNotFound.
func (s *Store) Progress(ctx context.Context, runID string, after int64, limit int) ([]ProgressRow, error) {
	var out []ProgressRow
	err := s.each(ctx, runID, limit, progressSQL, []any{runID, after, limit}, func(rows *sql.Rows) error {
		r, err := scanProgress(rows)
		out = append(out, r)
		return err
	})
	return out, err
}

// text returns b, at most limit+1 bytes of a clipped text, cut to at most
// limit bytes at a rune boundary, and adds field to *cut when it was longer.
func text(b []byte, limit int, cut *ProgressFields, field ProgressFields) string {
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

func scanProgress(rows *sql.Rows) (ProgressRow, error) {
	var r ProgressRow
	var phase, reason, effect, resolution, decision, payload, purpose, command, failure, action, execution, preview, detail []byte
	var outcome, outcomeDetail, retrievalError []byte
	var retrieval, handover, receipt sql.NullString
	var step, exitCode, sources, entries, window sql.NullInt64
	err := rows.Scan(&r.Seq, &r.Kind, &r.Semantics, &phase, &step, &reason, &effect, &receipt, &resolution, &decision, &payload, &purpose, &command,
		&failure, &action, &execution, &exitCode, &preview, &detail, &outcome, &outcomeDetail, &retrievalError, &sources, &entries,
		&retrieval, &handover, &window)
	if err != nil {
		return r, err
	}
	if err := known(r.Kind, r.Semantics); err != nil {
		return r, err
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
	r.Execution = core.ExecutionState(name(execution))
	r.Preview, r.Detail = text(preview, ProgressPreviewBytes, cut, ProgressPreview), text(detail, ProgressTextBytes, cut, ProgressDetail)
	if exitCode.Valid {
		code := int(exitCode.Int64)
		r.ExitCode = &code
	}
	r.Outcome = name(outcome)
	r.OutcomeDetail = text(outcomeDetail, ProgressTextBytes, cut, ProgressOutcomeDetail)
	r.RetrievalError = text(retrievalError, ProgressTextBytes, cut, ProgressRetrievalError)
	r.Sources, r.Entries, r.WindowFirstStep = int(sources.Int64), int(entries.Int64), int(window.Int64)
	r.Retrieval, r.Handover = retrieval.String == "object", handover.String == "object"
	r.External, r.ResolutionSource = receipt.String == "object", name(resolution)
	return r, nil
}
