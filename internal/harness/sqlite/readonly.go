package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
)

var ErrReadOnly = errors.New("sqlite: read-only store")

func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dsn := &url.URL{Scheme: "file", OmitHost: true, Path: path,
		RawQuery: "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, stmts: make(map[string]*sql.Stmt, len(queries)), readOnly: true}
	var version int
	err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version)
	if err == nil && version != 1 && version != schemaVersion {
		err = fmt.Errorf("%w: schema version %d, want 1 or %d", ErrIncompatible, version, schemaVersion)
	}
	s.sessions = version == schemaVersion
	if err == nil {
		err = s.probePerformed(ctx)
	}
	for _, query := range queries {
		if err == nil {
			s.stmts[query], err = db.PrepareContext(ctx, query)
		}
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: open read-only %s: %w", path, err)
	}
	return s, nil
}

// probePerformed records whether the journal holds performedIndex, which a
// writable open of another store may add after this one opened.
func (s *Store) probePerformed(ctx context.Context) error {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND name = 'transitions_performed'`).Scan(&n)
	if err == nil {
		s.performed.Store(n == 1)
	}
	return err
}
