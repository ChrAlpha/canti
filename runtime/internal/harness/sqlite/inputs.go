package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const latestRunsSQL = `SELECT r.id, r.semantics, r.initial, t.kind, t.semantics, t.event, t.to_state FROM runs r
LEFT JOIN transitions t ON t.run_id = r.id AND t.seq = (SELECT max(seq) FROM transitions WHERE run_id = r.id)
ORDER BY r.n`

const (
	inputColumns = `pos, COALESCE(input_id, ''), authority, delivery, disposition, COALESCE(run_id, ''), text,
		constraints, explicit_constraints, fresh, spec, source, subject, admitted_at, COALESCE(admitted_run_id, ''), COALESCE(delivered_pos, 0)`
	steersSQL = `SELECT ` + inputColumns + ` FROM inputs
		WHERE run_id = ? AND authority = 'principal' AND disposition = 'steer' ORDER BY pos`
	boundsSQL = `SELECT seq, json_extract(from_state, '$.Step'), json_extract(event, '$.Count')
		FROM transitions WHERE run_id = ? AND kind = 'inputs_bound' ORDER BY seq`
	observationsSQL = `SELECT k.source, k.subject, k.latest_pos, i.text, COALESCE(i.input_id, '')
		FROM observation_keys k JOIN inputs i ON i.pos = k.latest_pos
		ORDER BY k.latest_pos DESC LIMIT ?`
	observationKeySQL = `INSERT INTO observation_keys (source, subject, latest_pos) VALUES (?, ?, ?)
		ON CONFLICT (source, subject) DO UPDATE SET latest_pos = excluded.latest_pos`
	previousIDsSQL = `WITH RECURSIVE line(n, id, fresh) AS (
		SELECT n, id, fresh FROM runs WHERE id = ?1
		UNION ALL
		SELECT r.n, r.id, r.fresh FROM runs r JOIN line l ON r.n = l.n - 1 WHERE l.fresh = 0)
		SELECT id FROM line WHERE id <> ?1 ORDER BY n DESC LIMIT ?2`
	previousCountSQL = `WITH RECURSIVE line(n, id, fresh) AS (
		SELECT n, id, fresh FROM runs WHERE id = ?1
		UNION ALL
		SELECT r.n, r.id, r.fresh FROM runs r JOIN line l ON r.n = l.n - 1 WHERE l.fresh = 0)
		SELECT count(*) FROM line WHERE id <> ?1`
	queuedSQL             = `SELECT ` + inputColumns + ` FROM inputs WHERE disposition = 'queued' ORDER BY pos`
	queuedCountSQL        = `SELECT count(*) FROM inputs WHERE disposition = 'queued'`
	inputByClientSQL      = `SELECT ` + inputColumns + ` FROM inputs WHERE input_id = ?`
	withdrawnPositionSQL  = `SELECT COALESCE(withdrawn_pos, 0) FROM inputs WHERE pos = ?`
	withdrawableSteersSQL = `SELECT ` + inputColumns + ` FROM inputs
		WHERE run_id = ? AND authority = 'principal' AND disposition = 'steer' ORDER BY pos`
	markWithdrawnSQL   = `UPDATE inputs SET disposition = 'withdrawn', withdrawn_pos = ? WHERE pos = ?`
	steerOrdinalSQL    = `SELECT count(*) FROM inputs WHERE run_id = ? AND authority = 'principal' AND disposition = 'steer' AND pos <= ?`
	openFollowUpSQL    = `UPDATE inputs SET disposition = 'opened', run_id = ? WHERE pos = ? AND disposition = 'queued'`
	inputByPositionSQL = `SELECT ` + inputColumns + ` FROM inputs WHERE pos = ?`
	inputAtSQL         = `SELECT ` + inputColumns + ` FROM inputs WHERE pos = ?`
	inputsAtSQL        = `SELECT ` + inputColumns + ` FROM inputs WHERE pos IN (SELECT value FROM json_each(?)) ORDER BY pos`
	deliveredSQL       = `UPDATE inputs SET delivered_pos = ? WHERE pos = ? AND delivered_pos IS NULL`
	steerBytesSQL      = `SELECT COALESCE(sum(length(CAST(text AS BLOB))), 0) FROM inputs
		WHERE run_id = ? AND authority = 'principal' AND disposition = 'steer'`
	withdrawQueuedSQL = `UPDATE inputs SET disposition = 'withdrawn' WHERE disposition = 'queued'`
	insertInputSQL    = `INSERT INTO inputs (pos, input_id, authority, delivery, disposition, run_id, text, constraints,
		explicit_constraints, fresh, spec, source, subject, admitted_at, admitted_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// ValidKey reports whether id can name an input: 1 to 128 ASCII letters,
// digits, underscores, or hyphens.
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
	query := s.db.QueryContext
	if tx != nil {
		query = tx.QueryContext
	}
	rows, err := query(ctx, latestRunsSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, initial string
		var runSemantics int
		var kind, event, to sql.NullString
		var semantics sql.NullInt64
		if err := rows.Scan(&id, &runSemantics, &initial, &kind, &semantics, &event, &to); err != nil {
			return err
		}
		var state core.State
		var last core.Event
		if kind.Valid {
			last, err = decodeEvent(kind.String, event.String, int(semantics.Int64))
			if err == nil {
				err = decodeState(int(semantics.Int64), to.String, &state)
			}
		} else {
			err = decodeState(runSemantics, initial, &state)
		}
		if err != nil {
			return fmt.Errorf("sqlite: run %q: %w", id, err)
		}
		if harness.Blocks(state, last) {
			return fmt.Errorf("sqlite: run %q is unfinished or has an unknown effect", id)
		}
	}
	return rows.Err()
}

// Admit implements the admission rules in one write transaction: a replayed
// client input, an appended observation, a steer or queued follow-up for the
// open run, the refusal of a held input, and the opening of a new run.
func (s *Store) Admit(ctx context.Context, sub harness.InputSubmission) (harness.Admission, error) {
	if err := validateSubmission(sub); err != nil {
		return harness.Admission{}, err
	}
	var out harness.Admission
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = s.admit(ctx, tx, sub)
		return err
	})
	if err != nil {
		return harness.Admission{}, err
	}
	return out, nil
}

func (s *Store) admit(ctx context.Context, tx *sql.Tx, sub harness.InputSubmission) (harness.Admission, error) {
	if sub.InputID != "" {
		previous, err := s.inputByClient(ctx, tx, sub.InputID)
		if err == nil {
			return s.replayInput(ctx, tx, sub, previous)
		}
		if !errors.Is(err, harness.ErrNotFound) {
			return harness.Admission{}, err
		}
	}
	latest, snap, last, err := s.latestRun(ctx, tx)
	if err != nil {
		return harness.Admission{}, err
	}
	if sub.Authority == harness.AuthorityObservation {
		admission, err := s.appendInput(ctx, tx, sub, "", "")
		if err != nil {
			return harness.Admission{}, err
		}
		if _, err := s.stmt(ctx, tx, observationKeySQL).ExecContext(ctx, sub.Source, sub.Subject, admission.Input.Pos); err != nil {
			return harness.Admission{}, err
		}
		return admission, nil
	}
	if latest != nil && !snap.State.Closed() {
		switch sub.Delivery {
		case "":
			return harness.Admission{}, fmt.Errorf("%w: run %s is %s; name steer or follow_up", harness.ErrDeliveryRequired, latest.ID, snap.State.Phase)
		case harness.DeliverySteer:
			if err := s.checkSteerBudget(ctx, tx, latest.ID, sub.Text); err != nil {
				return harness.Admission{}, err
			}
			return s.appendInput(ctx, tx, sub, latest.ID, harness.DispositionSteer)
		case harness.DeliveryFollowUp:
			return s.appendInput(ctx, tx, sub, "", harness.DispositionQueued)
		default:
			return harness.Admission{}, fmt.Errorf("%w: unknown delivery %q", harness.ErrInputSpec, sub.Delivery)
		}
	}
	held, err := s.heldCount(ctx, tx, latest, snap)
	if err != nil {
		return harness.Admission{}, err
	}
	if held > 0 {
		return harness.Admission{}, fmt.Errorf("%w: %d held input(s)", harness.ErrHeld, held)
	}
	if latest != nil && harness.Blocks(snap.State, last) {
		return harness.Admission{}, fmt.Errorf("sqlite: run %q is unfinished or has an unknown effect", latest.ID)
	}
	pos, err := nextPos(ctx, tx)
	if err != nil {
		return harness.Admission{}, err
	}
	run := sub.Run
	if run.ID == "" {
		return harness.Admission{}, fmt.Errorf("%w: the latest run is closed but the input carries no run", harness.ErrAdmissionRace)
	}
	run.InputID, run.InputPos, run.Fresh = sub.InputID, pos, sub.Fresh
	run.Goal = sub.Text
	run.Created = sub.Now.UTC()
	data, state, err := encodeRun(run, sub.Initial)
	if err != nil {
		return harness.Admission{}, err
	}
	if run.InputPos > 0 {
		if _, err := tx.ExecContext(ctx, insertRunSQL, run.ID, core.SemanticsVersion, data, state, run.InputPos, run.Fresh); err != nil {
			return harness.Admission{}, err
		}
	}
	in, err := insertInput(ctx, tx, pos, sub, run.ID, harness.DispositionOpened)
	if err != nil {
		return harness.Admission{}, err
	}
	return harness.Admission{Disposition: harness.DispositionOpened, Input: in, Run: &run}, nil
}

func (s *Store) appendInput(ctx context.Context, tx *sql.Tx, sub harness.InputSubmission, runID string, disposition harness.InputDisposition) (harness.Admission, error) {
	pos, err := nextPos(ctx, tx)
	if err != nil {
		return harness.Admission{}, err
	}
	in, err := insertInput(ctx, tx, pos, sub, runID, disposition)
	if err != nil {
		return harness.Admission{}, err
	}
	return harness.Admission{Disposition: disposition, Input: in}, nil
}

func insertInput(ctx context.Context, tx *sql.Tx, pos int64, sub harness.InputSubmission, runID string, disposition harness.InputDisposition) (harness.Input, error) {
	in := harness.Input{Pos: pos, InputID: sub.InputID, Authority: sub.Authority, Delivery: sub.Delivery,
		Disposition: disposition, RunID: runID, AdmittedRunID: runID, Text: sub.Text, Constraints: sub.Constraints,
		ExplicitConstraints: sub.ExplicitConstraints, Fresh: sub.Fresh, Spec: sub.Spec,
		Source: sub.Source, Subject: sub.Subject, Admitted: sub.Now.UTC()}
	var client any
	if sub.InputID != "" {
		client = sub.InputID
	}
	var target any
	if runID != "" {
		target = runID
	}
	_, err := tx.ExecContext(ctx, insertInputSQL, pos, client, string(sub.Authority), string(sub.Delivery), string(disposition),
		target, sub.Text, sub.Constraints, sub.ExplicitConstraints, sub.Fresh, string(sub.Spec), sub.Source, sub.Subject,
		sub.Now.UTC().Format(time.RFC3339Nano), target)
	return in, err
}

func (s *Store) replayInput(ctx context.Context, tx *sql.Tx, sub harness.InputSubmission, previous harness.Input) (harness.Admission, error) {
	constraints := (sub.ExplicitConstraints || sub.Fresh) && previous.Constraints != sub.Constraints
	if previous.Authority != sub.Authority || previous.Text != sub.Text || constraints ||
		previous.Delivery != sub.Delivery || previous.Fresh != sub.Fresh || previous.Source != sub.Source || previous.Subject != sub.Subject {
		return harness.Admission{}, fmt.Errorf("%w: input %q", harness.ErrInputConflict, sub.InputID)
	}
	out := harness.Admission{Disposition: previous.Disposition, Input: previous, Replayed: true}
	if previous.RunID != "" && previous.Disposition == harness.DispositionOpened {
		run, err := s.runAt(ctx, tx, previous.RunID)
		if err != nil {
			return harness.Admission{}, err
		}
		out.Run = &run
	}
	return out, nil
}

func (s *Store) inputByClient(ctx context.Context, tx *sql.Tx, inputID string) (harness.Input, error) {
	return scanInput(s.stmt(ctx, tx, inputByClientSQL).QueryRowContext(ctx, inputID))
}

// Input returns the input admitted for client ID.
func (s *Store) Input(ctx context.Context, inputID string) (harness.Input, error) {
	if !ValidKey(inputID) {
		return harness.Input{}, fmt.Errorf("%w: invalid input ID", harness.ErrInputSpec)
	}
	return scanInput(s.stmts[inputByClientSQL].QueryRowContext(ctx, inputID))
}

// InputAt returns the admitted input at position pos.
func (s *Store) InputAt(ctx context.Context, pos int64) (harness.Input, error) {
	return scanInput(s.stmts[inputAtSQL].QueryRowContext(ctx, pos))
}

// InputsAt returns the admitted inputs at the given positions, in position
// order. The list is bounded by the caller's request.
func (s *Store) InputsAt(ctx context.Context, positions []int64) ([]harness.Input, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(positions)
	if err != nil {
		return nil, err
	}
	rows, err := s.stmts[inputsAtSQL].QueryContext(ctx, string(data))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []harness.Input
	for rows.Next() {
		in, err := scanInput(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// Withdraw takes back the named queued follow-up or unseen steer of the
// stopped latest run, or with an empty input ID every such input, in one
// transaction, and returns the inputs it withdrew. A transaction that changes
// any disposition takes one agent-log position and records it on every row it
// withdraws; already withdrawn inputs and an empty withdrawal commit no new
// position.
func (s *Store) Withdraw(ctx context.Context, inputID string) ([]harness.Input, error) {
	if inputID != "" && !ValidKey(inputID) {
		return nil, fmt.Errorf("%w: invalid input ID", harness.ErrInputSpec)
	}
	var out []harness.Input
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		latest, snap, _, err := s.latestRun(ctx, tx)
		if err != nil {
			return err
		}
		if inputID != "" {
			out, err = s.withdrawNamed(ctx, tx, inputID, latest, snap)
			return err
		}
		out, err = s.withdrawAll(ctx, tx, latest, snap)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) withdrawNamed(ctx context.Context, tx *sql.Tx, inputID string, latest *harness.Run, snap harness.Snapshot) ([]harness.Input, error) {
	in, err := s.inputByClient(ctx, tx, inputID)
	if err != nil {
		return nil, err
	}
	if in.Disposition == harness.DispositionWithdrawn {
		if err := tx.QueryRowContext(ctx, withdrawnPositionSQL, in.Pos).Scan(&in.WithdrawnPos); err != nil {
			return nil, err
		}
		return []harness.Input{in}, nil
	}
	eligible, err := s.withdrawable(ctx, tx, in, latest, snap)
	if err != nil {
		return nil, err
	}
	if !eligible {
		return nil, harness.ErrNotWithdrawable
	}
	pos, err := nextPos(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := markWithdrawn(ctx, tx, in.Pos, pos); err != nil {
		return nil, err
	}
	in.Disposition, in.WithdrawnPos = harness.DispositionWithdrawn, pos
	return []harness.Input{in}, nil
}

func (s *Store) withdrawAll(ctx context.Context, tx *sql.Tx, latest *harness.Run, snap harness.Snapshot) ([]harness.Input, error) {
	candidates, err := s.queryInputs(ctx, tx, queuedSQL)
	if err != nil {
		return nil, err
	}
	steers, err := s.withdrawableSteers(ctx, tx, latest, snap)
	if err != nil {
		return nil, err
	}
	candidates = append(candidates, steers...)
	if len(candidates) == 0 {
		return nil, nil
	}
	pos, err := nextPos(ctx, tx)
	if err != nil {
		return nil, err
	}
	var out []harness.Input
	for _, in := range candidates {
		if err := markWithdrawn(ctx, tx, in.Pos, pos); err != nil {
			return nil, err
		}
		in.Disposition, in.WithdrawnPos = harness.DispositionWithdrawn, pos
		out = append(out, in)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pos < out[j].Pos })
	return out, nil
}

func (s *Store) withdrawable(ctx context.Context, tx *sql.Tx, in harness.Input, latest *harness.Run, snap harness.Snapshot) (bool, error) {
	switch in.Disposition {
	case harness.DispositionQueued:
		return true, nil
	case harness.DispositionSteer:
		if latest == nil || latest.ID != in.RunID || snap.State.Phase != core.PhaseStopped {
			return false, nil
		}
		ordinal, err := s.steerOrdinal(ctx, tx, in)
		return ordinal > snap.State.Seen, err
	}
	return false, nil
}

func (s *Store) withdrawableSteers(ctx context.Context, tx *sql.Tx, latest *harness.Run, snap harness.Snapshot) ([]harness.Input, error) {
	if latest == nil || snap.State.Phase != core.PhaseStopped {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, withdrawableSteersSQL, latest.ID)
	if err != nil {
		return nil, err
	}
	var out []harness.Input
	for rows.Next() {
		in, err := scanInput(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		ordinal, err := s.steerOrdinal(ctx, tx, in)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if ordinal > snap.State.Seen {
			out = append(out, in)
		}
	}
	return out, errors.Join(rows.Err(), rows.Close())
}

func markWithdrawn(ctx context.Context, tx *sql.Tx, pos, withdrawnPos int64) error {
	_, err := tx.ExecContext(ctx, markWithdrawnSQL, withdrawnPos, pos)
	return err
}

// steerOrdinal is the 1-based position of a steer among the non-withdrawn
// steers of its run, which is the count its binding events use.
func (s *Store) steerOrdinal(ctx context.Context, tx *sql.Tx, in harness.Input) (int, error) {
	var n int
	err := tx.QueryRowContext(ctx, steerOrdinalSQL, in.RunID, in.Pos).Scan(&n)
	return n, err
}

// Queued returns the oldest queued follow-up.
func (s *Store) Queued(ctx context.Context) (harness.Input, error) {
	rows, err := s.stmts[queuedSQL].QueryContext(ctx)
	if err != nil {
		return harness.Input{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return harness.Input{}, errors.Join(rows.Err(), fmt.Errorf("sqlite: queued input: %w", harness.ErrNotFound))
	}
	return scanInput(rows)
}

// OpenFollowUp turns the queued input at in.Pos into the opened input of run
// in one transaction, after the latest run returned.
func (s *Store) OpenFollowUp(ctx context.Context, in harness.Input, run harness.Run, initial core.State) (harness.Admission, error) {
	if in.Disposition != harness.DispositionQueued {
		return harness.Admission{}, fmt.Errorf("%w: input %d is %s", harness.ErrNotWithdrawable, in.Pos, in.Disposition)
	}
	run.InputID, run.InputPos, run.Fresh = in.InputID, in.Pos, in.Fresh
	// The host resolved any inherited constraints when it built the run; the
	// queued row's text remains the run's goal.
	run.Goal = in.Text
	data, state, err := encodeRun(run, initial)
	if err != nil {
		return harness.Admission{}, err
	}
	var out harness.Admission
	err = s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		latest, snap, _, err := s.latestRun(ctx, tx)
		if err != nil {
			return err
		}
		if latest == nil || snap.State.Phase != core.PhaseReturned {
			return fmt.Errorf("%w: the latest run has not returned", harness.ErrInputSpec)
		}
		oldest, err := s.queuedInput(ctx, tx)
		if err != nil {
			return err
		}
		if oldest.Pos != in.Pos {
			return fmt.Errorf("%w: queued input %d is no longer the oldest", harness.ErrInputSpec, in.Pos)
		}
		if _, err := tx.ExecContext(ctx, insertRunSQL, run.ID, core.SemanticsVersion, data, state, run.InputPos, run.Fresh); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, openFollowUpSQL, run.ID, in.Pos); err != nil {
			return err
		}
		opened := in
		opened.Disposition, opened.RunID = harness.DispositionOpened, run.ID
		out = harness.Admission{Disposition: harness.DispositionOpened, Input: opened, Run: &run}
		return nil
	})
	return out, err
}

func (s *Store) queuedInput(ctx context.Context, tx *sql.Tx) (harness.Input, error) {
	return scanInput(s.stmt(ctx, tx, queuedSQL).QueryRowContext(ctx))
}

func (s *Store) latestRun(ctx context.Context, tx *sql.Tx) (*harness.Run, harness.Snapshot, core.Event, error) {
	run, err := s.run(s.stmt(ctx, tx, lastRunSQL).QueryRowContext(ctx))
	if errors.Is(err, harness.ErrNotFound) {
		return nil, harness.Snapshot{}, nil, nil
	}
	if err != nil {
		return nil, harness.Snapshot{}, nil, err
	}
	snap, err := s.latest(ctx, tx, run.ID)
	if err != nil {
		return nil, harness.Snapshot{}, nil, err
	}
	var kind string
	var semantics int
	var event string
	err = tx.QueryRowContext(ctx, latestEventSQL, run.ID).Scan(&kind, &semantics, &event)
	if errors.Is(err, sql.ErrNoRows) {
		return &run, snap, nil, nil
	}
	if err != nil {
		return nil, harness.Snapshot{}, nil, err
	}
	last, err := decodeEvent(kind, event, semantics)
	return &run, snap, last, err
}

func (s *Store) heldCount(ctx context.Context, tx *sql.Tx, latest *harness.Run, snap harness.Snapshot) (int, error) {
	if latest == nil {
		return 0, nil
	}
	var queued int
	if err := tx.QueryRowContext(ctx, queuedCountSQL).Scan(&queued); err != nil {
		return 0, err
	}
	held := 0
	if snap.State.Closed() {
		held += queued
	}
	if snap.State.Phase == core.PhaseStopped {
		var steers int
		if err := s.stmt(ctx, tx, steerCountSQL).QueryRowContext(ctx, latest.ID).Scan(&steers); err != nil {
			return 0, err
		}
		held += max(0, steers-snap.State.Seen)
	}
	return held, nil
}

func (s *Store) checkSteerBudget(ctx context.Context, tx *sql.Tx, runID, text string) error {
	var used int64
	if err := tx.QueryRowContext(ctx, steerBytesSQL, runID).Scan(&used); err != nil {
		return err
	}
	if limit := int64(harness.DefaultPromptLimits().Instructions); used+int64(len(text)) > limit {
		return fmt.Errorf("%w: the steers of run %s would exceed %d bytes", harness.ErrInputSpec, runID, limit)
	}
	return nil
}

func validateSubmission(sub harness.InputSubmission) error {
	switch sub.Authority {
	case harness.AuthorityPrincipal:
		if sub.Text == "" || !utf8.ValidString(sub.Text) || !utf8.ValidString(sub.Constraints) {
			return fmt.Errorf("%w: a principal input needs valid UTF-8 text", harness.ErrInputSpec)
		}
		if len(sub.Text) > harness.DefaultPromptLimits().Goal {
			return fmt.Errorf("%w: the input exceeds %d bytes", harness.ErrInputSpec, harness.DefaultPromptLimits().Goal)
		}
		switch sub.Delivery {
		case "", harness.DeliverySteer, harness.DeliveryFollowUp:
		default:
			return fmt.Errorf("%w: unknown delivery %q", harness.ErrInputSpec, sub.Delivery)
		}
	case harness.AuthorityObservation:
		if sub.Delivery != "" || sub.Fresh || len(sub.Constraints) > 0 || sub.ExplicitConstraints || len(sub.Spec) > 0 {
			return fmt.Errorf("%w: an observation carries no delivery, run spec, or freshness", harness.ErrInputSpec)
		}
		if sub.Text == "" || !utf8.ValidString(sub.Text) || len(sub.Text) > harness.ObservationTextBytes {
			return fmt.Errorf("%w: an observation needs valid UTF-8 text of at most %d bytes", harness.ErrInputSpec, harness.ObservationTextBytes)
		}
		if sub.Source == "" || sub.Subject == "" || !utf8.ValidString(sub.Source) || !utf8.ValidString(sub.Subject) ||
			len(sub.Source) > harness.InputKeyBytes || len(sub.Subject) > harness.InputKeyBytes {
			return fmt.Errorf("%w: an observation needs a source and subject of at most %d bytes", harness.ErrInputSpec, harness.InputKeyBytes)
		}
	default:
		return fmt.Errorf("%w: unknown authority %q", harness.ErrInputSpec, sub.Authority)
	}
	if sub.InputID != "" && !ValidKey(sub.InputID) {
		return fmt.Errorf("%w: input ID must be 1 to 128 ASCII letters, digits, underscores, or hyphens", harness.ErrInputSpec)
	}
	if sub.Authority == harness.AuthorityPrincipal && sub.Fresh && sub.Delivery == harness.DeliverySteer {
		return fmt.Errorf("%w: a steer cannot start a fresh line", harness.ErrInputSpec)
	}
	return nil
}

func scanInput(row interface{ Scan(...any) error }) (harness.Input, error) {
	var in harness.Input
	var authority, delivery, disposition, admitted string
	var spec sql.NullString
	err := row.Scan(&in.Pos, &in.InputID, &authority, &delivery, &disposition, &in.RunID, &in.Text,
		&in.Constraints, &in.ExplicitConstraints, &in.Fresh, &spec, &in.Source, &in.Subject, &admitted, &in.AdmittedRunID, &in.DeliveredPos)
	if err != nil {
		return in, missing(err, "input")
	}
	in.Authority, in.Delivery, in.Disposition = harness.InputAuthority(authority), harness.InputDelivery(delivery), harness.InputDisposition(disposition)
	in.Spec = []byte(spec.String)
	in.Admitted, err = time.Parse(time.RFC3339Nano, admitted)
	if err != nil || in.Pos < 1 {
		return in, fmt.Errorf("%w: input %d", ErrIncompatible, in.Pos)
	}
	return in, nil
}

func (s *Store) queryInputs(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]harness.Input, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []harness.Input
	for rows.Next() {
		in, err := scanInput(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// InputCounts reports the pending steers of the latest run or of runID, the
// queued follow-ups, and the held inputs, all derived from the log.
func (s *Store) inputCounts(ctx context.Context, runID string) (harness.InputCounts, error) {
	var counts harness.InputCounts
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return counts, err
	}
	defer tx.Rollback()
	latest, latestSnap, _, err := s.latestRun(ctx, tx)
	if err != nil {
		return counts, err
	}
	if err := tx.QueryRowContext(ctx, queuedCountSQL).Scan(&counts.Queued); err != nil {
		return counts, err
	}
	if latest == nil {
		return counts, nil
	}
	target, targetSnap := latest.ID, latestSnap
	if runID != "" && runID != latest.ID {
		target = runID
		if targetSnap, err = s.latest(ctx, tx, runID); err != nil {
			return counts, err
		}
	}
	var steers int
	if err := tx.QueryRowContext(ctx, steerCountSQL, target).Scan(&steers); err != nil {
		return counts, err
	}
	counts.Pending = max(0, steers-targetSnap.State.Seen)
	if latestSnap.State.Closed() {
		counts.Held += counts.Queued
	}
	if latestSnap.State.Phase == core.PhaseStopped {
		var unseen int
		if err := tx.QueryRowContext(ctx, steerCountSQL, latest.ID).Scan(&unseen); err != nil {
			return counts, err
		}
		counts.Held += max(0, unseen-latestSnap.State.Seen)
	}
	return counts, nil
}
