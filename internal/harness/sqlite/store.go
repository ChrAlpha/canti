// Package sqlite is the durable harness.Journal on SQLite. Every commit is one
// immediate write transaction on a WAL database with synchronous=FULL, so an
// acknowledged transition survives a process crash and, subject to the
// filesystem honouring fsync, a power loss.
package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	_ "modernc.org/sqlite"
)

// ErrIncompatible reports a record kind, semantics version, or schema version
// this package does not support. Such records are refused, never
// reinterpreted.
var ErrIncompatible = errors.New("sqlite: incompatible record")

const schemaVersion = 1

const options = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_txlock=immediate"

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
PRAGMA user_version = 1;
`

// Store is a harness.Journal backed by one SQLite database file. It is safe
// for concurrent use, including by several Stores on the same file.
type Store struct {
	db *sql.DB
}

// Open opens the database at path, creating the file and schema if missing.
// A database with another schema version is refused with ErrIncompatible.
func Open(ctx context.Context, path string) (*Store, error) {
	dsn := (&url.URL{Scheme: "file", OmitHost: true, Path: path, RawQuery: options}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.write(ctx, s.migrate); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	return s, nil
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
	switch version {
	case schemaVersion:
		return nil
	case 0:
		_, err := tx.ExecContext(ctx, schema)
		return err
	}
	return fmt.Errorf("%w: schema version %d, want %d", ErrIncompatible, version, schemaVersion)
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) write(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
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

// CreateRun records run with its initial state at sequence zero. The state
// must be the fresh state of the same run. Repeating an identical create
// succeeds without change; different content under an existing ID fails.
func (s *Store) CreateRun(ctx context.Context, run harness.Run, initial core.State) error {
	if err := initial.Validate(); err != nil {
		return err
	}
	if want, _ := core.NewState(run.ID, initial.Limits); initial != want {
		return fmt.Errorf("sqlite: run %q must start from its fresh state", run.ID)
	}
	run.Created = run.Created.UTC()
	record, err := json.Marshal(run)
	if err != nil {
		return err
	}
	state, err := json.Marshal(initial)
	if err != nil {
		return err
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var same bool
		err := tx.QueryRowContext(ctx, `SELECT semantics = ? AND run = ? AND initial = ? FROM runs WHERE id = ?`,
			core.SemanticsVersion, string(record), string(state), run.ID).Scan(&same)
		switch {
		case err == nil && same:
			return nil
		case err == nil:
			return fmt.Errorf("sqlite: run %q exists with different content", run.ID)
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO runs (id, semantics, run, initial) VALUES (?, ?, ?, ?)`,
			run.ID, core.SemanticsVersion, string(record), string(state))
		return err
	})
}

// Run returns the stored run record.
func (s *Store) Run(ctx context.Context, runID string) (harness.Run, error) {
	return s.run(ctx, `SELECT run FROM runs WHERE id = ?`, runID)
}

// LastRun returns the run created most recently in this store.
func (s *Store) LastRun(ctx context.Context) (harness.Run, error) {
	return s.run(ctx, `SELECT run FROM runs ORDER BY n DESC LIMIT 1`)
}

func (s *Store) run(ctx context.Context, query string, args ...any) (harness.Run, error) {
	var run harness.Run
	var record []byte
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&record); err != nil {
		return run, missing(err, "run")
	}
	return run, strict(record, &run)
}

func missing(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlite: %s: %w", what, harness.ErrNotFound)
	}
	return err
}

func strict(data []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(v)
	if err == nil {
		if _, trailing := decoder.Token(); trailing != io.EOF {
			err = errors.New("trailing data")
		}
	}
	if err != nil {
		return fmt.Errorf("%w: decode %T: %v", ErrIncompatible, v, err)
	}
	return nil
}
