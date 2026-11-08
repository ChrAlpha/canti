package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// acceptanceWriteLock is a test-executable fault surface, never a CLI or RPC
// operation. Holding a real SQLite writer transaction reproduces blocked stop
// commits while the independent TUI is exercised by repository acceptance.
func acceptanceWriteLock(args []string) int {
	if len(args) != 2 {
		return 2
	}
	db, err := sql.Open("sqlite", filepath.Join(args[0], "canti.db"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("BEGIN IMMEDIATE"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer db.Exec("ROLLBACK")
	if err = os.WriteFile(args[1], nil, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	_, err = io.Copy(io.Discard, os.Stdin)
	if err != nil {
		return 1
	}
	return 0
}
