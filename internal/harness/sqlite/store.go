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

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	modernc "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrIncompatible reports a database or stored record this package cannot
// read exactly: another schema or semantics version, an unknown record kind,
// or content the strict decoder rejects. Such records are refused, never
// reinterpreted.
var ErrIncompatible = errors.New("sqlite: incompatible record")

const schemaVersion = 2

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
	steps INTEGER NOT NULL DEFAULT 0
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
CREATE INDEX transitions_steps ON transitions (run_id, seq) WHERE kind IN ('observed', 'resolved');
CREATE TABLE session_runs (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL CHECK (seq > 0),
	input_id TEXT NOT NULL,
	run_id TEXT NOT NULL UNIQUE REFERENCES runs (id),
	PRIMARY KEY (session_id, seq),
	UNIQUE (session_id, input_id)
);
`

const sessionSchema = `CREATE TABLE session_runs (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL CHECK (seq > 0),
	input_id TEXT NOT NULL,
	run_id TEXT NOT NULL UNIQUE REFERENCES runs (id),
	PRIMARY KEY (session_id, seq),
	UNIQUE (session_id, input_id)
);`

const (
	sameRunSQL   = `SELECT semantics, initial, run = ? AND initial = ? FROM runs WHERE id = ?`
	insertRunSQL = `INSERT INTO runs (id, semantics, run, initial) VALUES (?, ?, ?, ?)`
	runSQL       = `SELECT run FROM runs WHERE id = ?`
	lastRunSQL   = `SELECT run FROM runs ORDER BY n DESC LIMIT 1`
	initialSQL   = `SELECT semantics, initial FROM runs WHERE id = ?`
	totalsSQL    = `SELECT input_tokens, output_tokens, output_bytes, steps FROM runs WHERE id = ?`
	addSQL       = `UPDATE runs SET input_tokens = input_tokens + ?, output_tokens = output_tokens + ?,
		output_bytes = output_bytes + ?, steps = steps + ? WHERE id = ?`
	latestSQL = `SELECT seq, kind, semantics, to_state FROM transitions WHERE run_id = ? ORDER BY seq DESC LIMIT 1`
	sameSQL   = `SELECT semantics, from_state, to_state, event_id = ? AND kind = ? AND event = ? AND from_state = ? AND to_state = ? AND facts = ?
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
)

var queries = []string{
	sameRunSQL, insertRunSQL, runSQL, lastRunSQL, initialSQL, totalsSQL, addSQL,
	latestSQL, sameSQL, insertSQL, transitionsSQL, requestSQL, responseSQL, recentSQL,
}

// Store is a harness.Journal backed by one SQLite database file. It is safe
// for concurrent use, including by several Stores on the same file.
type Store struct {
	db       *sql.DB
	stmts    map[string]*sql.Stmt
	readOnly bool
	sessions bool
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
	s := &Store{db: db, stmts: make(map[string]*sql.Stmt, len(queries)), sessions: true}
	err = s.write(ctx, s.migrate)
	for deadline := time.Now().Add(time.Second); busy(err) && time.Now().Before(deadline) && ctx.Err() == nil; {
		time.Sleep(10 * time.Millisecond)
		err = s.write(ctx, s.migrate)
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

func (s *Store) migrate(ctx context.Context, tx *sql.Tx) error {
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
	case version == 1:
		_, err := tx.ExecContext(ctx, sessionSchema+"PRAGMA user_version = 2;")
		return err
	}
	return fmt.Errorf("%w: schema version %d with %d objects, want %d", ErrIncompatible, version, objects, schemaVersion)
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
	last, err := s.latest(ctx, tx, r.run)
	if err != nil {
		return err
	}
	if t.Seq > 0 && t.Seq <= last.Seq {
		var same bool
		var semantics int
		var from, to string
		if err := s.stmt(ctx, tx, sameSQL).QueryRowContext(ctx, t.EventID, r.kind,
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
	if _, err := s.stmt(ctx, tx, insertSQL).ExecContext(ctx, r.run, t.Seq, t.EventID, r.kind, r.semantics,
		r.event, r.from, r.to, r.facts, t.To.Step, t.To.Attempt, t.To.RequestRev); err != nil {
		return err
	}
	d := delta(t)
	if d == (harness.Totals{}) {
		return nil
	}
	_, err = s.stmt(ctx, tx, addSQL).ExecContext(ctx, d.Usage.InputTokens, d.Usage.OutputTokens, d.OutputBytes, d.Steps, r.run)
	return err
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
	r := record{run: t.From.RunID, kind: t.Event.Kind(), semantics: core.SemanticsVersion}
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
	switch t.Event.(type) {
	case core.ResponseSaved, core.ResponseIncomplete:
		if t.Facts.Response != nil {
			d.Usage = t.Facts.Response.Usage
		}
	case core.Observed, core.Resolved:
		d.Steps = 1
		if t.Facts.Observation != nil {
			d.OutputBytes = t.Facts.Observation.Output.Bytes
		}
	}
	return d
}
