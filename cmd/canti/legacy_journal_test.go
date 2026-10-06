package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// TestLegacyRunsKeepTheirConfig requires that the CLI still decodes the
// stored config of every run of the legacy journal corpus, which stop and
// inspection read.
func TestLegacyRunsKeepTheirConfig(t *testing.T) {
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
				if c, err := decodeConfig(run, m.Paths.Workspace); err != nil || c.ContextVersion != 1 {
					t.Errorf("legacy run %s decodes to context version %d: %v", id, c.ContextVersion, err)
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
		Paths   struct{ Root, Workspace, State, Imports string } `json:"paths"`
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

// TestLegacyRunsAreClosedAndFollowedByNewRuns requires of every journal of
// the legacy corpus, recreated at the paths its producer recorded, that
// resume refuses its open run of another context version without a
// transition, a request, or a step record; that status, history, and
// context read it; that stop closes it; and that a new run in the same
// state directory, alone and in the producer's session, then returns.
func TestLegacyRunsAreClosedAndFollowedByNewRuns(t *testing.T) {
	for _, c := range legacyCases(t) {
		t.Run(c.name, func(t *testing.T) {
			m := c.manifest
			last := m.Journal.Runs[len(m.Journal.Runs)-1]
			db, history := c.recreate(t)
			f := &fixture{t: t, workspace: m.Paths.Workspace, state: m.Paths.State, key: "sk-test-legacy",
				model: newScriptedModel(t, "sk-test-legacy", map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})}
			if r := f.cli(nil, "resume"); r.code != exitError || !refusesResume(r.stderr, last.ID, "1") {
				t.Fatalf("resume = %d\nstderr: %s", r.code, r.stderr)
			}
			if _, seq, rows := legacyRows(t, db, last.ID); seq != last.Seq || rows != int(last.Seq) {
				t.Errorf("the refused resume left %d transitions of run %s, the last at %d; the producer left %d", rows, last.ID, seq, last.Seq)
			}
			if got := legacyTree(t, filepath.Join(m.Paths.State, "history")); !maps.Equal(got, history) || len(f.model.requests()) != 0 {
				t.Errorf("the refused resume published %v and sent %d requests", slices.Sorted(maps.Keys(got)), len(f.model.requests()))
			}
			if _, err := os.Stat(filepath.Join(m.Paths.State, "jobs")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("the refused resume built the executor: %v", err)
			}
			if s := f.status(); s.Run.ID != last.ID || s.Seq != last.Seq || s.Phase != last.Phase {
				t.Errorf("status = %+v; the producer paused run %s at %d in %s", s, last.ID, last.Seq, last.Phase)
			}
			if h := f.history(); len(h) != int(last.Seq) {
				t.Errorf("history has %d transitions, the producer %d", len(h), last.Seq)
			}
			if r := f.cli(nil, "context"); r.code != exitOK || !strings.Contains(r.stdout, last.ID) {
				t.Errorf("context = %d\nstderr: %s", r.code, r.stderr)
			}
			if r := f.cli(nil, "run", "-goal", "next"); r.code != exitError || !strings.Contains(r.stderr, fmt.Sprintf("is %s: stop it first", last.Phase)) {
				t.Errorf("run beside the open run = %d\nstderr: %s", r.code, r.stderr)
			}
			if r := f.cli(nil, "stop"); r.code != exitStopped {
				t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
			}
			if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled || len(f.model.requests()) != 0 {
				t.Errorf("status after stop = %+v, after %d requests", s, len(f.model.requests()))
			}
			if r := f.cli(nil, "run", "-goal", "Report done."); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("run after stop = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			if r := f.cli(nil, "run", "-session", "legacy-w0a", "-input", "after-upgrade", "-goal", "Report done."); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("session input after stop = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
		})
	}
}

// legacyReserved holds the workspace and state directory of every producer
// of the legacy corpus.
const legacyReserved = "/tmp/canti-legacy-w0a"

// reserveLegacyRoot claims root, a new directory under legacyReserved, while
// holding the lock of legacyReserved, which must be private to the user,
// and removes root and releases the lock when the test ends.
func reserveLegacyRoot(t *testing.T, root string) {
	t.Helper()
	if filepath.Dir(root) != legacyReserved {
		t.Fatalf("the recorded root %s is not under %s", root, legacyReserved)
	}
	if err := os.Mkdir(legacyReserved, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		t.Fatal(err)
	}
	dir, err := os.Open(legacyReserved)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dir.Close() })
	info, err := dir.Stat()
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
		t.Fatalf("%s is not a directory that only this user can write: %v", legacyReserved, err)
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("claiming the recorded root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
}

// recreate writes the journal and the roots of the case at the paths its
// producer recorded, which it reserves until the test ends, and returns the
// path of the journal and the files of the history root by path below it.
func (c legacyCase) recreate(t *testing.T) (db string, history map[string]string) {
	t.Helper()
	m := c.manifest
	reserveLegacyRoot(t, m.Paths.Root)
	db = filepath.Join(m.Paths.State, "canti.db")
	for _, dir := range []string{m.Paths.Workspace, m.Paths.State} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	c.expand(t, db)
	return db, c.expandRoots(t, map[string]string{"history": filepath.Join(m.Paths.State, "history"),
		"artifacts": filepath.Join(m.Paths.State, "artifacts"), "imports": m.Paths.Imports})
}

// expandRoots writes the files of the case's roots under the directories
// that dirs names for each root and returns those of the history root, by
// path below it.
func (c legacyCase) expandRoots(t *testing.T, dirs map[string]string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(c.dir, "roots.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	history := map[string]string{}
	for r := tar.NewReader(z); ; {
		h, err := r.Next()
		if err == io.EOF {
			return history
		}
		if err != nil {
			t.Fatal(err)
		}
		root, rest, _ := strings.Cut(strings.TrimSuffix(h.Name, "/"), "/")
		dir, ok := dirs[root]
		if !ok || rest != "" && (path.Clean(rest) != rest || strings.HasPrefix(rest, "..")) {
			t.Fatalf("roots entry %q is not a clean path under a recorded root", h.Name)
		}
		target := filepath.Join(dir, filepath.FromSlash(rest))
		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			var b []byte
			if b, err = io.ReadAll(r); err == nil {
				err = os.MkdirAll(filepath.Dir(target), 0o755)
			}
			if err == nil {
				err = os.WriteFile(target, b, 0o644)
			}
			if root == "history" {
				history[rest] = string(b)
			}
		default:
			err = fmt.Errorf("roots entry %q has type %q", h.Name, h.Typeflag)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// legacyTree returns the regular files under dir by slash-separated path.
func legacyTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		rel, _ := filepath.Rel(dir, p)
		files[filepath.ToSlash(rel)] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// legacyRows reads the schema version of the journal in file, and the last
// sequence and the number of transitions of run id, without changing it.
func legacyRows(t *testing.T, file, id string) (version int, seq int64, rows int) {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+file+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	ctx := context.Background()
	err = raw.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version)
	if err == nil {
		err = raw.QueryRowContext(ctx, "SELECT coalesce(max(seq), 0), count(*) FROM transitions WHERE run_id = ?", id).Scan(&seq, &rows)
	}
	if err != nil {
		t.Fatal(err)
	}
	return version, seq, rows
}
