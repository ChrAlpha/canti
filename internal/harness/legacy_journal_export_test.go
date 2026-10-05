//go:build legacygen

package harness_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func legacyProduce(t *testing.T, name string, spec legacySpec, built legacyBuilt) {
	out := filepath.Join(*legacyOut, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatalf("refusing to write %s: %v; give a new fixture a new case name", out, err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			os.RemoveAll(out)
		}
	})
	if err := os.Mkdir(legacyReserved, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		t.Fatal(err)
	}
	info, err := os.Lstat(legacyReserved)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
		t.Fatalf("%s is not a directory that only this user can write: %v", legacyReserved, err)
	}
	t.Cleanup(func() { os.Remove(legacyReserved) })
	root := filepath.Join(legacyReserved, path.Dir(name)[:7]+"-"+path.Base(name))
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("claiming the producer root: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	g := &legacyGen{t: t, spec: spec, bin: built.bin, root: root, workspace: filepath.Join(root, "workspace"),
		state: filepath.Join(root, "state"), home: filepath.Join(root, "home"), tm: filepath.Join(root, "tmp"),
		shim: filepath.Join(root, "bin"), held: make(chan legacyKey, 1), release: make(chan struct{})}
	if len(spec.imports) > 0 {
		g.imports = filepath.Join(root, "imports")
	}
	for _, dir := range []string{g.workspace, g.home, g.tm, g.shim, g.imports} {
		if dir == "" {
			continue
		}
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for file, text := range spec.imports {
		if err := os.WriteFile(filepath.Join(g.imports, file), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	shim := "#!/bin/sh\nprintf 'docker %s\\n' \"$*\" >>'" + filepath.Join(root, "docker-calls") + "'\nexit 97\n"
	if err := os.WriteFile(filepath.Join(g.shim, "docker"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	g.server = httptest.NewServer(g)
	t.Cleanup(func() { g.unhold(); g.server.Close() })
	defer func() { t.Logf("producer log:\n%s", g.text()) }()
	spec.drive(g)
	g.unhold()
	g.server.Close()
	if _, err := os.Stat(filepath.Join(root, "docker-calls")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a producer invoked docker: %v", err)
	}
	if t.Failed() {
		return
	}
	file := filepath.Join(g.state, "canti.db")
	checkpoint := legacyCheckpointDB(t, file)
	legacyCheckState(t, g.state)
	if k := g.killed; k != nil {
		v, kind, seq := g.latest(g.state, k.run)
		if checkpoint.Log == 0 || v != k.view || kind != k.kind || seq != k.seq {
			t.Fatalf("the checkpoint after SIGKILL moved %d frames and left %s at seq %d into %+v, want %s at seq %d",
				checkpoint.Log, kind, seq, v, k.kind, k.seq)
		}
	}
	db, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(t.TempDir(), "canti.db")
	if err := os.WriteFile(scratch, db, 0o600); err != nil {
		t.Fatal(err)
	}
	raw := snapshotLegacy(t, scratch)
	journal := legacySummary(t, raw)
	journal.DB = legacyFileOf("journal.db", db)
	var capacity *legacyCapacity
	if spec.probe {
		capacity = g.probe(raw, journal)
	}
	tarData, roots := legacyTarRoots(t, [][2]string{{"artifacts", filepath.Join(g.state, "artifacts")},
		{"history", filepath.Join(g.state, "history")}, {"imports", g.imports}})
	files := map[string][]byte{legacyDBName: legacyGzip(t, db), legacyRootsName: legacyGzip(t, tarData)}
	m := legacyManifest{Format: 1, Case: name, Producer: built.producer,
		Generator:   legacyGenerator{GoVersion: runtime.Version(), SQLite: legacySQLiteVersion(t, scratch)},
		Paths:       legacyPaths{Root: root, Workspace: g.workspace, State: g.state, Imports: g.imports},
		Environment: g.env(true), Invocations: g.invocations, Replies: g.replies, Checkpoint: checkpoint,
		Journal: journal, Capacity: capacity, Coverage: spec.coverage, Roots: roots}
	for _, source := range []string{"legacy_journal_gen_test.go", "legacy_journal_export_test.go", "legacy_journal_fixture_test.go"} {
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		m.Generator.Files = append(m.Generator.Files, legacyFileOf(source, data))
	}
	for _, f := range []string{legacyDBName, legacyRootsName} {
		m.Files = append(m.Files, legacyFileOf(f, files[f]))
	}
	manifest, err := json.MarshalIndent(m, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	files[legacyManifestName] = append(manifest, '\n')
	g.scan(map[string][]byte{"journal.db": db, "roots.tar": tarData, legacyDBName: files[legacyDBName],
		legacyRootsName: files[legacyRootsName], legacyManifestName: files[legacyManifestName], "producer log": []byte(g.text())})
	for f, data := range files {
		if err := os.WriteFile(filepath.Join(out, f), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s: journal %d bytes (%d compressed), roots %d bytes (%d compressed), %d transitions",
		name, len(db), len(files[legacyDBName]), len(tarData), len(files[legacyRootsName]), journal.Rows["transitions"])
}

// legacyCheckpointDB moves every committed WAL frame into the database file
// after all producers exited, through SQLite alone, and verifies the file. A
// FULL checkpoint counts and moves the frames, because a successful TRUNCATE
// reports zero for both; the TRUNCATE then empties the log.
func legacyCheckpointDB(t *testing.T, file string) legacyCheckpoint {
	t.Helper()
	db := openLegacySQL(t, file, "_pragma=busy_timeout(5000)")
	var c legacyCheckpoint
	var truncated [3]int
	err := db.QueryRow("PRAGMA wal_checkpoint(FULL)").Scan(&c.Busy, &c.Log, &c.Checkpointed)
	if err == nil {
		err = db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&truncated[0], &truncated[1], &truncated[2])
	}
	if err == nil {
		err = db.QueryRow("PRAGMA integrity_check").Scan(&c.Integrity)
	}
	if err == nil {
		err = db.QueryRow("PRAGMA journal_mode").Scan(&c.JournalMode)
	}
	var violations bool
	if err == nil {
		var rows *sql.Rows
		if rows, err = db.Query("PRAGMA foreign_key_check"); err == nil {
			violations = rows.Next()
			err = errors.Join(rows.Err(), rows.Close())
		}
	}
	if err = errors.Join(err, db.Close()); err != nil {
		t.Fatal(err)
	}
	if c.Busy != 0 || c.Log != c.Checkpointed || truncated != [3]int{} || c.Integrity != "ok" || c.JournalMode != "wal" || violations {
		t.Fatalf("checkpoint of %s: %+v, truncation %v, foreign key violations %v", file, c, truncated, violations)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(file + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s%s remains after the checkpoint: %v", file, suffix, err)
		}
	}
	return c
}

// legacyCheckState fails when the producer's state directory holds more than
// the journal, the lock, the archived roots, and an empty job directory.
func legacyCheckState(t *testing.T, state string) {
	t.Helper()
	entries, err := os.ReadDir(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch e.Name() {
		case "canti.db", "lock", "artifacts", "history":
		case "jobs":
			if jobs, err := os.ReadDir(filepath.Join(state, "jobs")); err != nil || len(jobs) > 0 {
				t.Fatalf("the producer left job records, which this generator does not archive: %v", err)
			}
		default:
			t.Fatalf("the producer left %s in its state directory", e.Name())
		}
	}
}

func legacySQLiteVersion(t *testing.T, file string) string {
	t.Helper()
	db := openLegacySQL(t, file, "_pragma=query_only(1)")
	defer db.Close()
	var v string
	if err := db.QueryRow("SELECT sqlite_version()").Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// probe lets the producer prepare the next request of the paused run on a
// copy of the finished state directory and reports its handover check,
// next to the one the journal's last request recorded.
func (g *legacyGen) probe(raw legacyDB, j legacyJournal) *legacyCapacity {
	t := g.t
	run := j.Runs[len(j.Runs)-1]
	dir := filepath.Join(g.root, "probe")
	legacyCopyTree(t, g.state, dir)
	inv := g.invoke(dir, "resume", "-once")
	if inv.Exit != "5" {
		t.Fatalf("the probe exited %s", inv.Exit)
	}
	probed := snapshotLegacy(t, filepath.Join(dir, "canti.db"))
	c := &legacyCapacity{Last: legacyLastCheck(t, raw, run.ID), Probe: legacyLastCheck(t, probed, run.ID), ProbeArgs: inv.Args}
	if c.Probe.Step != run.Step || c.Last.Step >= run.Step {
		t.Fatalf("the probe froze step %d after step %d, want step %d", c.Probe.Step, c.Last.Step, run.Step)
	}
	var record struct{ Config []byte }
	var cfg struct {
		ContextBytes int `json:"context_bytes"`
	}
	for _, row := range raw.runs(t) {
		if row.text(t, "id") == run.ID {
			if err := errors.Join(json.Unmarshal([]byte(row.text(t, "run")), &record), json.Unmarshal(record.Config, &cfg)); err != nil {
				t.Fatal(err)
			}
		}
	}
	c.ContextBytes = cfg.ContextBytes
	g.logf("probe: last request %+v, next request %+v\n", c.Last, c.Probe)
	return c
}

func legacyCopyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		switch {
		case d.IsDir():
			return os.MkdirAll(target, 0o755)
		case d.Type().IsRegular():
			data, err := os.ReadFile(p)
			if err == nil {
				err = os.WriteFile(target, data, 0o644)
			}
			return err
		}
		return fmt.Errorf("%s is neither a file nor a directory", p)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// legacyTarRoots archives the directories and regular files of each root,
// named by its label, with fixed metadata.
func legacyTarRoots(t *testing.T, roots [][2]string) ([]byte, []legacyFile) {
	t.Helper()
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	var files []legacyFile
	for _, root := range roots {
		label, host := root[0], root[1]
		if host == "" {
			continue
		}
		if _, err := os.Stat(host); errors.Is(err, os.ErrNotExist) {
			continue
		}
		err := filepath.WalkDir(host, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(host, p)
			if err != nil {
				return err
			}
			name := path.Join(label, filepath.ToSlash(rel))
			h := &tar.Header{Name: name, ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}
			var data []byte
			switch {
			case d.IsDir():
				h.Typeflag, h.Name, h.Mode = tar.TypeDir, name+"/", 0o755
			case d.Type().IsRegular():
				if data, err = os.ReadFile(p); err != nil {
					return err
				}
				h.Typeflag, h.Mode, h.Size = tar.TypeReg, 0o644, int64(len(data))
				files = append(files, legacyFileOf(name, data))
			default:
				return fmt.Errorf("%s is neither a file nor a directory", p)
			}
			if err := w.WriteHeader(h); err != nil {
				return err
			}
			_, err = w.Write(data)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(files, func(a, b legacyFile) int { return strings.Compare(a.Path, b.Path) })
	return buf.Bytes(), files
}

func legacyGzip(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err == nil {
		_, err = w.Write(data)
	}
	if err = errors.Join(err, w.Close()); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// scan fails when an output, or any file the producers left, holds the fake
// credential or a credential of the generator's own environment.
func (g *legacyGen) scan(outputs map[string][]byte) {
	t := g.t
	secrets := []string{legacyCredential}
	for _, name := range []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"} {
		if v := os.Getenv(name); len(v) >= 8 {
			secrets = append(secrets, v)
		}
	}
	err := filepath.WalkDir(g.root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			outputs[p], err = os.ReadFile(p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range outputs {
		for _, secret := range secrets {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("%s holds a credential", name)
			}
		}
	}
	t.Logf("scanned %d outputs for %d credentials", len(outputs), len(secrets))
}
