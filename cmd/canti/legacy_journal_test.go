package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnsupportedJournalSchemasAreRejectedWithoutChangingTheirFacts(t *testing.T) {
	for _, version := range []int{1, 2, 999} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			f := newFixture(t, nil)
			if err := os.MkdirAll(f.state, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.state, "canti.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version=%d; CREATE TABLE retained_fact(value TEXT); INSERT INTO retained_fact VALUES ('preserve');", version)); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for index, command := range [][]string{{"status"}, {"history"}, {"context"}, {"context", "-raw"}, {"inspect"}, {"resume"}, {"stop"}, {"resolve", "-job"}, {"run", "-goal", "fresh"}} {
				r := f.cli(nil, command[0], command[1:]...)
				if r.code != exitError || !strings.Contains(r.stderr, "schema version") || !strings.Contains(r.stderr, "use fresh state") || r.stdout != "" {
					t.Errorf("%q: %+v", command, r)
				}
				// Journal-only commands must leave even the stored file bytes intact.
				// Writer opens may select WAL mode before rejecting the schema.
				if index < 4 {
					after, err := os.ReadFile(path)
					if err != nil || sha256.Sum256(after) != sha256.Sum256(before) {
						t.Fatalf("read-only rejection changed journal: %v", err)
					}
				}
			}
			db, err = sql.Open("sqlite", "file:"+path+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var storedVersion int
			if err := db.QueryRow("PRAGMA user_version").Scan(&storedVersion); err != nil || storedVersion != version {
				t.Fatalf("schema changed to %d: %v", storedVersion, err)
			}
			var fact string
			if err := db.QueryRowContext(context.Background(), "SELECT value FROM retained_fact").Scan(&fact); err != nil || fact != "preserve" || len(f.model.requests()) != 0 {
				t.Fatalf("facts=%q err=%v requests=%d", fact, err, len(f.model.requests()))
			}
		})
	}
}
