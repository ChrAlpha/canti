package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// TestLegacyRunsKeepTheirConfigAndEnvironment requires that the CLI still
// decodes the stored config of every run of the legacy journal corpus and
// renders the environment its producer rendered into the first request,
// which the harness tests of the corpus copy.
func TestLegacyRunsKeepTheirConfigAndEnvironment(t *testing.T) {
	cases, err := filepath.Glob(filepath.Join("..", "..", "internal", "harness", "testdata", "legacy-journals", "*", "*"))
	if err != nil || len(cases) == 0 {
		t.Fatalf("no legacy journal cases: %v", err)
	}
	ctx := context.Background()
	for _, dir := range cases {
		t.Run(filepath.Base(filepath.Dir(dir))[:7]+"/"+filepath.Base(dir), func(t *testing.T) {
			var m struct {
				Paths struct{ Workspace string } `json:"paths"`
			}
			data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			if err == nil {
				err = json.Unmarshal(data, &m)
			}
			var db []byte
			if err == nil {
				data, err = os.ReadFile(filepath.Join(dir, "journal.db.gz"))
			}
			if err == nil {
				var r *gzip.Reader
				if r, err = gzip.NewReader(bytes.NewReader(data)); err == nil {
					db, err = io.ReadAll(r)
				}
			}
			file := filepath.Join(t.TempDir(), "canti.db")
			if err == nil {
				err = os.WriteFile(file, db, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := sql.Open("sqlite", "file:"+file+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			rows, err := raw.QueryContext(ctx, "SELECT id FROM runs ORDER BY n")
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil || len(ids) == 0 {
				t.Fatalf("runs %v: %v", ids, err)
			}
			store, err := sqlite.OpenReadOnly(ctx, file)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for _, id := range ids {
				run, err := store.Run(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				c, err := decodeConfig(run, m.Paths.Workspace)
				if err != nil {
					t.Fatalf("the CLI refuses legacy run %s: %v", id, err)
				}
				first, err := store.ContextRequest(ctx, id, 1, 1)
				if err != nil {
					t.Fatal(err)
				}
				var body struct {
					Messages []struct {
						Content []struct{ Text string }
					}
				}
				if err := json.Unmarshal(first.Facts.Request.Body, &body); err != nil || len(body.Messages) != 1 || len(body.Messages[0].Content) == 0 {
					t.Fatalf("the first request of %s is not one Messages request: %v", id, err)
				}
				section := "## Environment\n" + environment(c, run.Budget) + "\n"
				if !strings.Contains(body.Messages[0].Content[0].Text, section) {
					t.Errorf("the CLI renders the environment of %s as %q, which its producer did not", id, section)
				}
			}
		})
	}
}
