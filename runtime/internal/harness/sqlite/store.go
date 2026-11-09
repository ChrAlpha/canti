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

const schemaVersion = 4

const options = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=fullfsync(1)&_txlock=immediate"

const schema = `
CREATE TABLE runs (
	n INTEGER PRIMARY KEY,
	id TEXT NOT NULL UNIQUE,
	semantics INTEGER NOT NULL,
	run TEXT NOT NULL,
	initial TEXT NOT NULL,
	input_tokens INTEGER NOT NULL DEFAULT 0,
	output_tokens INTEGER NOT NULL DEFAULT 0,
	output_bytes INTEGER NOT NULL DEFAULT 0,
	steps INTEGER NOT NULL DEFAULT 0,
	context_seq INTEGER NOT NULL DEFAULT 0,
	usage_attempts INTEGER NOT NULL DEFAULT 0,
	input_reports INTEGER NOT NULL DEFAULT 0,
	output_reports INTEGER NOT NULL DEFAULT 0,
	lost_attempts INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE transitions (
	run_id TEXT NOT NULL REFERENCES runs (id),
	seq INTEGER NOT NULL CHECK (seq > 0),
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
CREATE UNIQUE INDEX transitions_request ON transitions (run_id, step, rev) WHERE kind = 'request_frozen';
CREATE UNIQUE INDEX transitions_response ON transitions (run_id, step, attempt) WHERE kind = 'response_saved';
CREATE UNIQUE INDEX transitions_completion_step ON transitions (run_id, json_extract(from_state, '$.Step')) WHERE kind IN ('observed','resolved','performed');
CREATE INDEX transitions_steps ON transitions (run_id, seq) WHERE kind IN ('observed', 'resolved');
` + performedIndex + `
CREATE TABLE session_runs (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL CHECK (seq > 0),
	input_id TEXT NOT NULL,
	run_id TEXT NOT NULL UNIQUE REFERENCES runs (id),
	PRIMARY KEY (session_id, seq),
	UNIQUE (session_id, input_id)
);
`

// performedIndex serves bounded reads of local completions.
const performedIndex = `CREATE INDEX transitions_performed ON transitions (run_id, seq) WHERE kind = 'performed';`

const (
	sameRunSQL   = `SELECT semantics, initial, run = ? AND initial = ? FROM runs WHERE id = ?`
	insertRunSQL = `INSERT INTO runs (id, semantics, run, initial) VALUES (?, ?, ?, ?)`
	runSQL       = `SELECT semantics, run FROM runs WHERE id = ?`
	lastRunSQL   = `SELECT semantics, run FROM runs ORDER BY n DESC LIMIT 1`
	initialSQL   = `SELECT semantics, initial FROM runs WHERE id = ?`
	totalsSQL    = `SELECT input_tokens, output_tokens, output_bytes, steps, usage_attempts, input_reports, output_reports, lost_attempts FROM runs WHERE id = ?`
	addSQL       = `UPDATE runs SET input_tokens = ?, output_tokens = ?, output_bytes = ?, steps = ?,
        usage_attempts = ?, input_reports = ?, output_reports = ?, lost_attempts = ? WHERE id = ?`
	latestSQL = `SELECT seq, kind, semantics, to_state FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`
	sameSQL   = `SELECT semantics, from_state, to_state, event_id = ? AND kind = ? AND semantics = ? AND event = ? AND from_state = ? AND to_state = ? AND facts = ?
		FROM transitions WHERE run_id = ? AND seq = ?`
	insertSQL = `INSERT INTO transitions
		(run_id, seq, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
		ORDER BY seq DESC LIMIT ?3`
	// completionSQL reads one transition by primary key for Completion.
	completionSQL = `SELECT seq, kind, semantics, from_state, facts FROM transitions WHERE run_id = ? AND seq = ?`
	savedSQL      = `SELECT seq FROM transitions WHERE run_id = ? AND kind = 'response_saved' AND step = ? AND attempt = ?`
	acceptedSQL   = `SELECT kind, semantics, json_extract(to_state, '$.Phase'), json_extract(facts, '$.Action'), json_extract(facts, '$.Execution')
		FROM transitions WHERE run_id = ? AND seq = ?`
	// headSQL is latestSQL without the decision payload, which SQLite drops
	// before the state crosses the SQL boundary.
	headSQL = `SELECT seq, kind, semantics, json_remove(to_state, '$.Decision.payload') FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`
)

var queries = []string{
	sameRunSQL, insertRunSQL, runSQL, lastRunSQL, initialSQL, totalsSQL, addSQL,
	latestSQL, headSQL, sameSQL, insertSQL, transitionsSQL, requestSQL, responseSQL, recentSQL,
	stepsSQL, completionSQL, savedSQL, acceptedSQL, progressSQL,
	contextSeqSQL, contextHeadSQL, receiptSQL, sessionRunsSQL,
}

// Store is a harness.Journal backed by one SQLite database file. It is safe
// for concurrent use, including by several Stores on the same file.
type Store struct {
	db       *sql.DB
	stmts    map[string]*sql.Stmt
	readOnly bool
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
	if err := tx.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return err
	}
	if mode != "wal" {
		return fmt.Errorf("journal mode is %q, want wal", mode)
	}
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	var objects int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&objects); err != nil {
		return err
	}
	switch {
	case version == schemaVersion:
		return nil
	case version == 0 && objects == 0:
		_, err := tx.ExecContext(ctx, schema+fmt.Sprintf("PRAGMA user_version = %d;", schemaVersion))
		return err

	}
	return fmt.Errorf("%w: schema version %d with %d objects, want %d; use fresh state", ErrIncompatible, version, objects, schemaVersion)
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

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
	return tx.Commit()
}

func (s *Store) stmt(ctx context.Context, tx *sql.Tx, query string) *sql.Stmt {
	if tx == nil {
		return s.stmts[query]
	}
	return tx.StmtContext(ctx, s.stmts[query])
}

// CreateRun records run with its initial state at sequence zero. The state
// must be the fresh state of the same run. Repeating an identical create
// succeeds without change; different content under an existing ID fails.
func (s *Store) CreateRun(ctx context.Context, run harness.Run, initial core.State) error {
	if run.SessionID != "" || run.InputID != "" || run.SessionSeq != 0 {
		return errors.New("sqlite: session runs require Submit")
	}
	data, state, err := encodeRun(run, initial)
	if err != nil {
		return err
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var same bool
		var semantics int
		var stored string
		err := s.stmt(ctx, tx, sameRunSQL).QueryRowContext(ctx, data, state, run.ID).Scan(&semantics, &stored, &same)
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
		_, err = s.stmt(ctx, tx, insertRunSQL).ExecContext(ctx, run.ID, core.SemanticsVersion, data, state)
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
	for _, text := range []string{run.ID, run.Goal, run.Constraints} {
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
	if _, err := s.stmt(ctx, tx, insertSQL).ExecContext(ctx, r.run, t.Seq, t.EventID, r.kind, r.semantics,
		r.event, r.from, r.to, r.facts, t.To.Step, t.To.Attempt, t.To.RequestRev); err != nil {
		return err
	}
	if t.Facts.ContextState != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET context_seq=? WHERE id=?`, t.Seq, r.run); err != nil {
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
