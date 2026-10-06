package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// TestLegacyRunsKeepTheirConfigAndEnvironment requires that the CLI still
// decodes the stored config of every run of the legacy journal corpus and
// renders the environment its producer rendered into the first request,
// which the harness tests of the corpus copy.
func TestLegacyRunsKeepTheirConfigAndEnvironment(t *testing.T) {
	ctx := context.Background()
	for _, legacy := range legacyCases(t) {
		t.Run(legacy.name, func(t *testing.T) {
			m := legacy.manifest
			file := filepath.Join(t.TempDir(), "canti.db")
			legacy.expand(t, file)
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

// TestInspectionLeavesLegacyJournalsUntouched requires that status, history,
// and the constraints a chat session inherits read every journal of the
// legacy corpus without changing it: a schema-1 journal keeps its schema,
// every journal keeps its bytes, and no step record is published.
func TestInspectionLeavesLegacyJournalsUntouched(t *testing.T) {
	cases := legacyCases(t)
	if !slices.ContainsFunc(cases, func(c legacyCase) bool { return c.manifest.Journal.UserVersion == 1 }) {
		t.Fatal("the legacy corpus holds no schema-1 journal")
	}
	ctx := context.Background()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			m, state := c.manifest, t.TempDir()
			file := filepath.Join(state, "canti.db")
			c.expand(t, file)
			inspect := func(command func(*flag.FlagSet, *app) action) string {
				t.Helper()
				var out strings.Builder
				a := &app{stdout: &out, stderr: io.Discard, workspace: m.Paths.Workspace, state: state}
				if code, err := command(flag.NewFlagSet("canti", flag.ContinueOnError), a)(ctx); code != exitOK || err != nil {
					t.Fatalf("= %d, %v", code, err)
				}
				return out.String()
			}
			last := m.Journal.Runs[len(m.Journal.Runs)-1]
			var s status
			if err := strict(inspect(statusCommand), &s); err != nil || s.Run.ID != last.ID || s.Seq != last.Seq || s.Phase != last.Phase {
				t.Errorf("status = %+v, %v; the producer paused run %s at %d in %s", s, err, last.ID, last.Seq, last.Phase)
			}
			var seq int64
			for line := range strings.Lines(inspect(historyCommand)) {
				var e entry
				if err := strict(line, &e); err != nil || e.Seq != seq+1 {
					t.Fatalf("history line %q after %d: %v", line, seq, err)
				}
				seq = e.Seq
			}
			if seq != last.Seq {
				t.Errorf("history ends at %d, the producer at %d", seq, last.Seq)
			}
			if constraints, err := inherited(ctx, &app{workspace: m.Paths.Workspace, state: state}, "legacy"); constraints != "" || err != nil {
				t.Errorf("inherited = %q, %v", constraints, err)
			}
			if data, err := os.ReadFile(file); err != nil || sha256.Sum256(data) != c.sum {
				t.Errorf("inspection changed the journal: %v", err)
			}
			raw, err := sql.Open("sqlite", "file:"+file+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			var version int
			if err := raw.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != m.Journal.UserVersion {
				t.Errorf("schema version %d after inspection, the producer's %d: %v", version, m.Journal.UserVersion, err)
			}
			if _, err := os.Stat(filepath.Join(state, "history")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("inspection published step records: %v", err)
			}
		})
	}
}

// legacyCase is one case of the legacy journal corpus of the harness tests.
type legacyCase struct {
	name, dir string
	sum       [sha256.Size]byte
	manifest  struct {
		Paths   struct{ Workspace string } `json:"paths"`
		Journal struct {
			DB struct {
				SHA256 string `json:"sha256"`
			} `json:"db"`
			UserVersion int `json:"user_version"`
			Runs        []struct {
				ID    string     `json:"id"`
				Seq   int64      `json:"seq"`
				Phase core.Phase `json:"phase"`
			} `json:"runs"`
		} `json:"journal"`
	}
}

// legacyCases returns every case of the corpus with its manifest.
func legacyCases(t *testing.T) []legacyCase {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join("..", "..", "internal", "harness", "testdata", "legacy-journals", "*", "*"))
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no legacy journal cases: %v", err)
	}
	var cases []legacyCase
	for _, dir := range dirs {
		c := legacyCase{name: filepath.Base(filepath.Dir(dir))[:7] + "/" + filepath.Base(dir), dir: dir}
		data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err == nil {
			err = json.Unmarshal(data, &c.manifest)
		}
		if err == nil {
			_, err = hex.Decode(c.sum[:], []byte(c.manifest.Journal.DB.SHA256))
		}
		if err != nil || len(c.manifest.Journal.Runs) == 0 {
			t.Fatalf("%s: %v", c.name, err)
		}
		cases = append(cases, c)
	}
	return cases
}

// expand writes the journal of the case, as its manifest records it, to
// file.
func (c legacyCase) expand(t *testing.T, file string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(c.dir, "journal.db.gz"))
	var db []byte
	if err == nil {
		var r *gzip.Reader
		if r, err = gzip.NewReader(bytes.NewReader(data)); err == nil {
			db, err = io.ReadAll(r)
		}
	}
	if err == nil && sha256.Sum256(db) != c.sum {
		err = errors.New("the journal differs from its manifest")
	}
	if err == nil {
		err = os.WriteFile(file, db, 0o600)
	}
	if err != nil {
		t.Fatalf("%s: %v", c.name, err)
	}
}
