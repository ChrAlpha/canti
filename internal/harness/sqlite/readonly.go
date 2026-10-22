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
	if err == nil && version != schemaVersion {
		err = fmt.Errorf("%w: schema version %d, want %d", ErrIncompatible, version, schemaVersion)
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
