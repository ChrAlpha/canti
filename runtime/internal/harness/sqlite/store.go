// Package sqlite is the durable harness.Journal on SQLite. Every commit is one
// immediate write transaction on a WAL database with synchronous=FULL, so an
// acknowledged transition survives a process crash and, subject to the
// filesystem honouring fsync, a power loss. The store does not lock the
// workspace: rejecting a stale commit does not stop a stale harness from
// acting on its executor. Records are JSON, so invalid UTF-8 in fact text
// reads back as U+FFFD.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	modernc "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrIncompatible reports a database or stored record this package cannot
// read exactly: another schema or semantics version, an unknown record kind,
// or content the strict decoder rejects. Such records are refused, never
// reinterpreted.
var ErrIncompatible = errors.New("sqlite: incompatible record")

const schemaVersion = 6

const (
	journalModeSQL        = `PRAGMA journal_mode`
	userVersionCheckSQL   = `PRAGMA user_version`
	schemaObjectsSQL      = `SELECT count(*) FROM sqlite_schema`
	contextSeqUpdateSQL   = `UPDATE runs SET context_seq=? WHERE id=?`
	rejectionSeqUpdateSQL = `UPDATE runs SET rejection_seq=? WHERE id=?`
)

var userVersionSQL = fmt.Sprintf("PRAGMA user_version = %d;", schemaVersion)

const options = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=fullfsync(1)&_txlock=immediate"

const schema = `
CREATE TABLE agent_log (
	n INTEGER PRIMARY KEY CHECK (n = 1),
	pos INTEGER NOT NULL
);
INSERT INTO agent_log (n, pos) VALUES (1, 0);
CREATE TABLE runs (
	n INTEGER PRIMARY KEY,
	id TEXT NOT NULL UNIQUE,
	semantics INTEGER NOT NULL,
	run TEXT NOT NULL,
	initial TEXT NOT NULL,
	input_pos INTEGER,
	fresh INTEGER NOT NULL DEFAULT 0,
	input_tokens INTEGER NOT NULL DEFAULT 0,
	output_tokens INTEGER NOT NULL DEFAULT 0,
	output_bytes INTEGER NOT NULL DEFAULT 0,
	steps INTEGER NOT NULL DEFAULT 0,
	context_seq INTEGER NOT NULL DEFAULT 0,
	rejection_seq INTEGER NOT NULL DEFAULT 0,
	usage_attempts INTEGER NOT NULL DEFAULT 0,
	input_reports INTEGER NOT NULL DEFAULT 0,
	output_reports INTEGER NOT NULL DEFAULT 0,
	lost_attempts INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE inputs (
	pos INTEGER PRIMARY KEY,
	input_id TEXT,
	authority TEXT NOT NULL,
	delivery TEXT NOT NULL DEFAULT '',
	disposition TEXT NOT NULL DEFAULT '',
	withdrawn_pos INTEGER,
	delivered_pos INTEGER,
	run_id TEXT REFERENCES runs (id),
	admitted_run_id TEXT,
	text TEXT NOT NULL,
	constraints TEXT NOT NULL DEFAULT '',
	explicit_constraints INTEGER NOT NULL DEFAULT 0,
	fresh INTEGER NOT NULL DEFAULT 0,
	spec TEXT NOT NULL DEFAULT '',
	source TEXT NOT NULL DEFAULT '',
	subject TEXT NOT NULL DEFAULT '',
	admitted_at TEXT NOT NULL
);
CREATE UNIQUE INDEX inputs_client ON inputs (input_id);
CREATE INDEX inputs_run ON inputs (run_id, pos);
CREATE INDEX inputs_disposition ON inputs (disposition, pos);
CREATE INDEX inputs_observation ON inputs (source, subject, pos);
CREATE INDEX inputs_withdrawn ON inputs (withdrawn_pos, pos) WHERE withdrawn_pos IS NOT NULL;
CREATE INDEX inputs_delivered ON inputs (delivered_pos, pos) WHERE delivered_pos IS NOT NULL;
CREATE TABLE observation_keys (
	source TEXT NOT NULL,
	subject TEXT NOT NULL,
	latest_pos INTEGER NOT NULL,
	PRIMARY KEY (source, subject)
);
CREATE INDEX observation_keys_latest ON observation_keys (latest_pos DESC);
CREATE TABLE transitions (
	run_id TEXT NOT NULL REFERENCES runs (id),
	seq INTEGER NOT NULL CHECK (seq > 0),
	pos INTEGER NOT NULL,
	event_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	semantics INTEGER NOT NULL,
	event TEXT NOT NULL,
	from_state TEXT NOT NULL,
	to_state TEXT NOT NULL,
	facts TEXT NOT NULL,
	step INTEGER NOT NULL,
	attempt INTEGER NOT NULL,
	rev INTEGER NOT NULL,
	PRIMARY KEY (run_id, seq)
);
CREATE UNIQUE INDEX transitions_pos ON transitions (pos);
CREATE UNIQUE INDEX transitions_request ON transitions (run_id, step, rev) WHERE kind = 'request_frozen';
CREATE UNIQUE INDEX transitions_response ON transitions (run_id, step, attempt) WHERE kind = 'response_saved';
CREATE UNIQUE INDEX transitions_completion_step ON transitions (run_id, json_extract(from_state, '$.Step'))
	WHERE kind IN ('observed','resolved','performed') OR (kind = 'response_loaded' AND (CASE WHEN json_valid(facts) THEN json_type(facts, '$.Supersede') END) = 'object');
CREATE INDEX transitions_steps ON transitions (run_id, seq) WHERE kind IN ('observed', 'resolved');
CREATE INDEX transitions_superseded ON transitions (run_id, seq)
	WHERE kind = 'response_loaded' AND (CASE WHEN json_valid(facts) THEN json_type(facts, '$.Supersede') END) = 'object';
` + performedIndex

// performedIndex serves bounded reads of local completions.
const performedIndex = `CREATE INDEX transitions_performed ON transitions (run_id, seq) WHERE kind = 'performed';`

const (
	sameRunSQL   = `SELECT semantics, initial, run = ? AND initial = ? AND input_pos IS ? AND fresh = ? FROM runs WHERE id = ?`
	insertRunSQL = `INSERT INTO runs (id, semantics, run, initial, input_pos, fresh) VALUES (?, ?, ?, ?, ?, ?)`
	runSQL       = `SELECT semantics, run, input_pos, fresh FROM runs WHERE id = ?`
	lastRunSQL   = `SELECT semantics, run, input_pos, fresh FROM runs WHERE n = (SELECT max(n) FROM runs)`
	initialSQL   = `SELECT semantics, initial FROM runs WHERE id = ?`
	totalsSQL    = `SELECT input_tokens, output_tokens, output_bytes, steps, usage_attempts, input_reports, output_reports, lost_attempts FROM runs WHERE id = ?`
	addSQL       = `UPDATE runs SET input_tokens = ?, output_tokens = ?, output_bytes = ?, steps = ?,
        usage_attempts = ?, input_reports = ?, output_reports = ?, lost_attempts = ? WHERE id = ?`
	latestSQL = `SELECT seq, kind, semantics, to_state FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`
	sameSQL   = `SELECT semantics, from_state, to_state, event_id = ? AND kind = ? AND semantics = ? AND event = ? AND from_state = ? AND to_state = ? AND facts = ?
		FROM transitions WHERE run_id = ? AND seq = ?`
	insertSQL = `INSERT INTO transitions
		(run_id, seq, pos, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	agentPosSQL    = `SELECT pos FROM agent_log WHERE n = 1`
	bumpPosSQL     = `UPDATE agent_log SET pos = ? WHERE n = 1`
	lastPosSQL     = `SELECT pos FROM agent_log WHERE n = 1`
	steerCountSQL  = `SELECT count(*) FROM inputs WHERE run_id = ? AND authority = 'principal' AND disposition = 'steer'`
	transitionsSQL = `SELECT seq, event_id, kind, semantics, event, from_state, to_state, facts
		FROM transitions WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?`
	requestSQL  = `SELECT semantics, from_state, to_state, facts FROM transitions WHERE run_id = ? AND kind = 'request_frozen' AND step = ? AND rev = ?`
	responseSQL = `SELECT semantics, from_state, to_state, facts FROM transitions WHERE run_id = ? AND kind = 'response_saved' AND step = ? AND attempt = ?`
	recentSQL   = `SELECT seq, kind, semantics, from_state, facts FROM transitions
		WHERE run_id = ? AND kind IN ('observed', 'resolved') AND seq < ? ORDER BY seq DESC LIMIT ?`
	// stepsSQL is recentSQL and its performed counterpart in one statement,
	// so one snapshot answers both, each searching its partial index.
	stepsSQL = `SELECT * FROM (SELECT seq, kind, semantics, from_state, facts FROM transitions
		WHERE run_id = ?1 AND kind IN ('observed', 'resolved') AND seq < ?2 ORDER BY seq DESC LIMIT ?3)
		UNION ALL SELECT * FROM (SELECT seq, kind, semantics, from_state, facts FROM transitions
		WHERE run_id = ?1 AND kind = 'performed' AND seq < ?2 ORDER BY seq DESC LIMIT ?3)
		UNION ALL SELECT * FROM (SELECT seq, kind, semantics, from_state, facts FROM transitions
		WHERE run_id = ?1 AND kind = 'response_loaded' AND (CASE WHEN json_valid(facts) THEN json_type(facts, '$.Supersede') END) = 'object' AND seq < ?2 ORDER BY seq DESC LIMIT ?3)
		ORDER BY seq DESC LIMIT ?3`
	// completionSQL reads one transition by primary key for Completion.
	completionSQL = `SELECT seq, kind, semantics, from_state, facts FROM transitions WHERE run_id = ? AND seq = ?`
	savedSQL      = `SELECT seq FROM transitions WHERE run_id = ? AND kind = 'response_saved' AND step = ? AND attempt = ?`
	acceptedSQL   = `SELECT kind, semantics, json_extract(to_state, '$.Phase'), json_extract(facts, '$.Action'), json_extract(facts, '$.Execution')
		FROM transitions WHERE run_id = ? AND seq = ?`
	// rejectionSeqSQL reads the sequence of the confirmed provider rejection
	// that opened the run's current head lineage, zero before its first. A
	// terminal or attempt-exhausted rejection opens no head and does not move
	// it.
	rejectionSeqSQL = `SELECT rejection_seq FROM runs WHERE id = ?`
	// headSQL is latestSQL without the decision payload, which SQLite drops
	// before the state crosses the SQL boundary.
	headSQL = `SELECT seq, kind, semantics, json_remove(to_state, '$.Decision.payload') FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`
	// priorRequestSQL reads the sequence of the previous frozen request of a
	// run, zero before its first. The scan is bounded by the transitions of
	// the current and previous step.
	priorRequestSQL = `SELECT COALESCE(max(seq), 0) FROM transitions WHERE run_id = ?1 AND kind = 'request_frozen' AND seq < ?2`
	// priorUsageSQL reads the last settled attempt of that request, whose
	// usage the execution position reports; it never reaches past it.
	priorUsageSQL = `SELECT json_extract(facts,'$.Response.Usage.InputTokens'), json_extract(facts,'$.Response.Usage.InputReported')
		FROM transitions WHERE run_id = ?1 AND seq > ?2 AND seq < ?3
		AND kind IN ('response_saved','response_incomplete','request_rejected')
		ORDER BY seq DESC LIMIT 1`
	// acceptedInputSQL aggregates the largest reported input usage of the
	// run's complete, accepted responses before a rejection.
	acceptedInputSQL = `SELECT COALESCE(max(CASE WHEN json_extract(facts,'$.Response.Usage.InputReported') THEN CAST(json_extract(facts,'$.Response.Usage.InputTokens') AS INTEGER) END), 0),
		COALESCE(max(CASE WHEN json_extract(facts,'$.Response.Usage.InputReported') THEN 1 ELSE 0 END), 0)
		FROM transitions WHERE run_id = ?1 AND kind = 'response_saved' AND seq < ?2`
)

var queries = []string{
	sameRunSQL, insertRunSQL, runSQL, lastRunSQL, initialSQL, totalsSQL, addSQL,
	latestSQL, headSQL, sameSQL, insertSQL, transitionsSQL, requestSQL, responseSQL, recentSQL,
	stepsSQL, completionSQL, savedSQL, acceptedSQL, progressSQL, logWithdrawalsSQL,
	contextSeqSQL, contextHeadSQL, rejectionSeqSQL, receiptSQL, agentPosSQL, bumpPosSQL,
	lastPosSQL, steerCountSQL, steersSQL, boundsSQL, observationsSQL,
	previousIDsSQL, previousCountSQL, queuedSQL, queuedCountSQL,
	priorRequestSQL, priorUsageSQL, acceptedInputSQL, lineRunsSQL, latestHandoverSQL, lineContainsSQL, transitionBySeqSQL,
	inputByClientSQL, inputByPositionSQL, inputAtSQL, inputsAtSQL, deliveredSQL, observationKeySQL, steerBytesSQL, insertInputSQL, latestRunsSQL,
}

// Store is a harness.Journal backed by one SQLite database file. It is safe
// for concurrent use, including by several Stores on the same file.
type Store struct {
	db       *sql.DB
	stmts    map[string]*sql.Stmt
	readOnly bool
	onCommit atomic.Pointer[func()]
}

var _ harness.Journal = (*Store)(nil)

// Open opens the database at path, creating the file and schema if missing.
// A database with another schema version is refused with ErrIncompatible.
func Open(ctx context.Context, path string) (*Store, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", OmitHost: true, Path: path, RawQuery: options}).String())
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, stmts: make(map[string]*sql.Stmt, len(queries))}
	err = s.write(ctx, s.initialize)
	for deadline := time.Now().Add(time.Second); busy(err) && time.Now().Before(deadline) && ctx.Err() == nil; {
		time.Sleep(10 * time.Millisecond)
		err = s.write(ctx, s.initialize)
	}
	for _, query := range queries {
		if err == nil {
			s.stmts[query], err = db.PrepareContext(ctx, query)
		}
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	return s, nil
}

// busy reports SQLITE_BUSY, which converting a new file to WAL returns at
// once, without the busy timeout, when another first Open races it.
func busy(err error) bool {
	var e *modernc.Error
	return errors.As(err, &e) && e.Code()&0xff == sqlite3.SQLITE_BUSY
}

func (s *Store) initialize(ctx context.Context, tx *sql.Tx) error {
	var mode string
	if err := tx.QueryRowContext(ctx, journalModeSQL).Scan(&mode); err != nil {
		return err
	}
	if mode != "wal" {
		return fmt.Errorf("journal mode is %q, want wal", mode)
	}
	var version int
	if err := tx.QueryRowContext(ctx, userVersionCheckSQL).Scan(&version); err != nil {
		return err
	}
	var objects int
	if err := tx.QueryRowContext(ctx, schemaObjectsSQL).Scan(&objects); err != nil {
		return err
	}
	switch {
	case version == schemaVersion:
		return nil
	case version == 0 && objects == 0:
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, userVersionSQL)
		return err

	}
	return fmt.Errorf("%w: schema version %d with %d objects, want %d; use fresh state", ErrIncompatible, version, objects, schemaVersion)
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// SetOnCommit installs the function every successful write calls after its
// transaction commits. It is the single notification point for readers of the
// agent log: a write that advances a position or changes an input fact has
// committed by the time the hook runs, so a woken reader cannot observe a
// stale journal. A failed or rolled-back write calls nothing.
func (s *Store) SetOnCommit(fn func()) {
	if fn == nil {
		s.onCommit.Store(nil)
		return
	}
	s.onCommit.Store(&fn)
}

func (s *Store) committed() {
	if fn := s.onCommit.Load(); fn != nil {
		(*fn)()
	}
}

func (s *Store) write(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.readOnly {
		return ErrReadOnly
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.committed()
	return nil
}

func (s *Store) stmt(ctx context.Context, tx *sql.Tx, query string) *sql.Stmt {
	if tx == nil {
		return s.stmts[query]
	}
	return tx.StmtContext(ctx, s.stmts[query])
}

// CreateRun records run with its initial state at sequence zero. The state
// must be the fresh state of the same run. Repeating an identical create
// succeeds without change; different content under an existing ID fails. A
// run created by an admitted input, which the host alone does, is created
// with its input in the same transaction instead.
func (s *Store) CreateRun(ctx context.Context, run harness.Run, initial core.State) error {
	if run.InputPos != 0 || run.InputID != "" {
		return errors.New("sqlite: a run with an input is created by admission")
	}
	data, state, err := encodeRun(run, initial)
	if err != nil {
		return err
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var same bool
		var semantics int
		var stored string
		err := s.stmt(ctx, tx, sameRunSQL).QueryRowContext(ctx, data, state, nil, run.Fresh, run.ID).Scan(&semantics, &stored, &same)
		if err == nil {
			var initial core.State
			if err := decodeState(semantics, stored, &initial); err != nil {
				return err
			}
		}
		switch {
		case err == nil && same:
			return nil
		case err == nil:
			return fmt.Errorf("sqlite: run %q exists with different content", run.ID)
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if err := s.checkAdmission(ctx, tx); err != nil {
			return err
		}
		_, err = s.stmt(ctx, tx, insertRunSQL).ExecContext(ctx, run.ID, core.SemanticsVersion, data, state, nil, run.Fresh)
		return err
	})
}

func encodeRun(run harness.Run, initial core.State) (string, string, error) {
	if err := initial.Validate(); err != nil {
		return "", "", err
	}
	if want, _ := core.NewState(run.ID, initial.Limits); initial != want {
		return "", "", fmt.Errorf("sqlite: run %q must start from its fresh state", run.ID)
	}
	if run.InputPos < 0 {
		return "", "", fmt.Errorf("sqlite: run %q has a negative input position", run.ID)
	}
	if len(run.ID) > harness.MaxRunIDBytes {
		return "", "", fmt.Errorf("sqlite: run ID exceeds %d bytes", harness.MaxRunIDBytes)
	}
	for _, text := range []string{run.ID, run.Goal, run.Constraints, run.InputID} {
		if !utf8.ValidString(text) {
			return "", "", fmt.Errorf("sqlite: run %q holds invalid UTF-8", run.ID)
		}
	}
	run.Created = run.Created.UTC()
	data, err := json.Marshal(run)
	if err != nil {
		return "", "", err
	}
	state, err := json.Marshal(initial)
	if err != nil {
		return "", "", err
	}
	return string(data), string(state), nil
}

// Commit records t, its facts, and the run totals in one transaction, or
// nothing. The run is t.From.RunID. A transition identical to the one already
// recorded at t.Seq succeeds without recording again; any other transition at
// a used or non-consecutive sequence fails with harness.ErrStaleSnapshot, and
// one whose From is not the state committed at t.Seq-1 fails with
// harness.ErrBrokenChain.
func (s *Store) Commit(ctx context.Context, t harness.Transition) error {
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx) error { return s.commit(ctx, tx, t) })
}

func (s *Store) commit(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	r, err := encode(t)
	if err != nil {
		return err
	}
	last, err := s.latestAs(ctx, tx, r.run, r.from, &t.From)
	if err != nil {
		return err
	}
	if t.Seq > 0 && t.Seq <= last.Seq {
		var same bool
		var semantics int
		var from, to string
		if err := s.stmt(ctx, tx, sameSQL).QueryRowContext(ctx, t.EventID, r.kind, r.semantics,
			r.event, r.from, r.to, r.facts, r.run, t.Seq).Scan(&semantics, &from, &to, &same); err != nil {
			return err
		}
		var state core.State
		for _, data := range []string{from, to} {
			if err := decodeState(semantics, data, &state); err != nil {
				return err
			}
		}
		if same {
			return nil
		}
		return fmt.Errorf("%w: seq %d of run %q holds another transition", harness.ErrStaleSnapshot, t.Seq, r.run)
	}
	if t.Seq != last.Seq+1 {
		return fmt.Errorf("%w: seq %d does not follow seq %d of run %q", harness.ErrStaleSnapshot, t.Seq, last.Seq, r.run)
	}
	if t.From != last.State {
		return fmt.Errorf("%w: seq %d of run %q", harness.ErrBrokenChain, t.Seq, r.run)
	}
	if r.semantics != core.SemanticsVersion {
		return fmt.Errorf("sqlite: a new transition of semantics version %d, want %d", r.semantics, core.SemanticsVersion)
	}
	if err := harness.ValidateTransition(t); err != nil {
		return err
	}
	if err := s.checkContext(ctx, tx, t); err != nil {
		return err
	}
	if err := s.checkEffect(ctx, tx, t); err != nil {
		return err
	}
	if err := s.checkInputs(ctx, tx, t); err != nil {
		return err
	}
	var pos int64
	if pos, err = nextPos(ctx, tx); err != nil {
		return err
	}
	if _, err := s.stmt(ctx, tx, insertSQL).ExecContext(ctx, r.run, t.Seq, pos, t.EventID, r.kind, r.semantics,
		r.event, r.from, r.to, r.facts, t.To.Step, t.To.Attempt, t.To.RequestRev); err != nil {
		return err
	}
	if err := s.markDelivered(ctx, tx, pos, t.Facts.Delivered); err != nil {
		return err
	}
	if t.Facts.ContextState != nil {
		if _, err := tx.ExecContext(ctx, contextSeqUpdateSQL, t.Seq, r.run); err != nil {
			return err
		}
	}
	// Only a rejection that opens a context head moves the run's rejection
	// pointer: a terminal or attempt-exhausted rejection opens no head, so the
	// head it leaves behind still carries the older rejection and its usage.
	if rejection := t.Facts.Rejection; rejection != nil && rejection.Signal.Confirmed() && t.Facts.ContextState != nil {
		if _, err := tx.ExecContext(ctx, rejectionSeqUpdateSQL, t.Seq, r.run); err != nil {
			return err
		}
	}
	return s.addTotals(ctx, tx, r.run, delta(t))
}

type record struct {
	run, kind              string
	semantics              int
	event, from, to, facts string
}

// encode fails unless the event and states read back exactly, because the
// chain check and replay compare them; JSON replaces invalid UTF-8, and reads
// return value events.
func encode(t harness.Transition) (record, error) {
	if t.Event == nil {
		return record{}, errors.New("sqlite: transition has no event")
	}
	r := record{run: t.From.RunID, kind: t.Event.Kind(), semantics: t.Semantics}
	if t.To.RunID != r.run {
		return r, fmt.Errorf("sqlite: transition moves run %q to run %q", r.run, t.To.RunID)
	}
	for dst, v := range map[*string]any{&r.event: t.Event, &r.from: t.From, &r.to: t.To, &r.facts: t.Facts} {
		data, err := json.Marshal(v)
		if err != nil {
			return r, err
		}
		*dst = string(data)
	}
	back, err := r.decodeEventStates()
	if err != nil {
		return r, err
	}
	if back.From != t.From || back.To != t.To || !reflect.DeepEqual(back.Event, t.Event) {
		return r, fmt.Errorf("sqlite: %T transition does not read back as committed", t.Event)
	}
	return r, nil
}

func delta(t harness.Transition) harness.Totals {
	var d harness.Totals
	settled := false
	if _, ok := t.Event.(core.ResponseLoaded); ok && t.Facts.Supersede != nil {
		d.Steps = 1
	}
	switch t.Event.(type) {
	case core.ResponseSaved, core.ResponseIncomplete, core.RequestRejected:
		settled = true
	case core.StopRequested:
		settled = t.From.Phase == core.PhaseRequestSent
	case core.AttemptStarted:
		if t.From.Phase == core.PhaseRequestSent {
			settled = true
			d.Coverage.Lost = 1
		}
	case core.Performed:
		d.Steps = 1
	case core.Observed, core.Resolved:
		d.Steps = 1
		if t.Facts.Observation != nil {
			d.OutputBytes = t.Facts.Observation.Output.Bytes
		}
	}
	if settled {
		d.Coverage.Attempts = 1
		if r := t.Facts.Response; r != nil {
			d.Usage.InputTokens, d.Usage.OutputTokens = r.Usage.InputTokens, r.Usage.OutputTokens
			if r.Usage.InputReported {
				d.Coverage.InputReports = 1
			}
			if r.Usage.OutputReported {
				d.Coverage.OutputReports = 1
			}
		} else {
			d.Coverage.Lost = 1
		}
	}
	return d
}

// Read and add in the write transaction rather than relying on SQLite's
// arithmetic, which promotes an overflowing INTEGER to an imprecise REAL.
func (s *Store) addTotals(ctx context.Context, tx *sql.Tx, runID string, d harness.Totals) error {
	if d == (harness.Totals{}) {
		return nil
	}
	total, err := s.totals(ctx, tx, runID)
	if err != nil {
		return err
	}
	pairs := [][2]*int64{
		{&total.Usage.InputTokens, &d.Usage.InputTokens}, {&total.Usage.OutputTokens, &d.Usage.OutputTokens},
		{&total.OutputBytes, &d.OutputBytes}, {&total.Coverage.Attempts, &d.Coverage.Attempts},
		{&total.Coverage.InputReports, &d.Coverage.InputReports}, {&total.Coverage.OutputReports, &d.Coverage.OutputReports},
		{&total.Coverage.Lost, &d.Coverage.Lost},
	}
	for _, pair := range pairs {
		*pair[0], err = harness.CheckedAdd(*pair[0], *pair[1])
		if err != nil {
			return err
		}
	}
	if err = total.Usage.Validate(); err != nil {
		return err
	}
	steps, err := harness.CheckedAdd(int64(total.Steps), int64(d.Steps))
	if err != nil || int64(int(steps)) != steps {
		return errors.New("sqlite: step count overflow")
	}
	_, err = s.stmt(ctx, tx, addSQL).ExecContext(ctx, total.Usage.InputTokens, total.Usage.OutputTokens, total.OutputBytes, steps,
		total.Coverage.Attempts, total.Coverage.InputReports, total.Coverage.OutputReports, total.Coverage.Lost, runID)
	return err
}

// checkInputs enforces the input side of a commit inside the transaction
// that writes it: the principal steers a ResponseLoaded answers must all be
// bound, and an InputsBound must not claim more steers than its run admitted.
// A bind may commit the prefix it read before a concurrent steer arrived; the
// remaining steers bind at the next binding point.
func (s *Store) checkInputs(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	switch e := t.Event.(type) {
	case core.InputsBound:
		var count int
		if err := s.stmt(ctx, tx, steerCountSQL).QueryRowContext(ctx, t.From.RunID).Scan(&count); err != nil {
			return err
		}
		if e.Count > count {
			return fmt.Errorf("%w: %d bound instructions exceed the %d steers of run %q", harness.ErrFacts, e.Count, count, t.From.RunID)
		}
	case core.ResponseLoaded:
		var count int
		if err := s.stmt(ctx, tx, steerCountSQL).QueryRowContext(ctx, t.From.RunID).Scan(&count); err != nil {
			return err
		}
		if count > t.From.Inputs {
			return fmt.Errorf("%w: run %q holds %d steers and %d are bound", harness.ErrInputsPending, t.From.RunID, count, t.From.Inputs)
		}
	}
	return nil
}

// markDelivered records the first request position that showed each input.
func (s *Store) markDelivered(ctx context.Context, tx *sql.Tx, pos int64, d *harness.Delivered) error {
	if d == nil {
		return nil
	}
	for _, inputPos := range d.Positions {
		if _, err := s.stmt(ctx, tx, deliveredSQL).ExecContext(ctx, pos, inputPos); err != nil {
			return err
		}
	}
	return nil
}

// nextPos takes the next agent-log position in tx.
func nextPos(ctx context.Context, tx *sql.Tx) (int64, error) {
	var pos int64
	if err := tx.QueryRowContext(ctx, agentPosSQL).Scan(&pos); err != nil {
		return 0, err
	}
	pos++
	_, err := tx.ExecContext(ctx, bumpPosSQL, pos)
	return pos, err
}

// checkEffect applies harness.CheckEffect to t inside the transaction tx
// that commits it: with the acceptance of its effect, read by the response
// index and the primary key, and with immutable run configuration.
func (s *Store) checkEffect(ctx context.Context, tx *sql.Tx, t harness.Transition) error {
	if !harness.EffectTransition(t) {
		return nil
	}
	var accepted harness.Accepted
	var err error
	_, accepting := t.Event.(core.ResponseLoaded)
	if !accepting {
		accepted, err = s.accepted(ctx, tx, t.From.RunID, t.From.Step, t.From.Attempt)
	}
	if err == nil {
		var run harness.Run
		run, err = s.run(s.stmt(ctx, tx, runSQL).QueryRowContext(ctx, t.From.RunID))
		if err == nil {
			err = harness.CheckEffect(t, accepted, run.Config)
		}
		if err != nil {
			err = fmt.Errorf("%w: %v", harness.ErrFacts, err)
		}
	}
	return err
}
