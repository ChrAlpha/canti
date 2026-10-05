package harness_test

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
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// A legacy journal case is a directory testdata/legacy-journals/<producer
// SHA>/<case> holding a journal written by an unmodified historical canti
// binary, the history, artifact, and import roots it left beside it, and the
// manifest that describes how it was produced. Cases are immutable: a new
// fixture gets a new case name.
var legacyDir = filepath.Join("testdata", "legacy-journals")

// The producers of the corpus: the last schema-1 revision, and a semantics-2
// revision whose production code equals the current one at the time of
// generation.
const (
	legacySchema1    = "b9446723f59eb87923d81912b4a7a78fce8dbc68"
	legacySemantics2 = "7624f85e8d0c2ad759c1b18c80a2facc57c9b41a"
)

const (
	legacyManifestName = "manifest.json"
	legacyDBName       = "journal.db.gz"
	legacyRootsName    = "roots.tar.gz"
	legacyMaxDB        = 16 << 20
	legacyMaxCorpus    = 4 << 20
)

type legacyFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type legacyManifest struct {
	Format      int                `json:"format"`
	Case        string             `json:"case"`
	Producer    legacyProducer     `json:"producer"`
	Generator   legacyGenerator    `json:"generator"`
	Paths       legacyPaths        `json:"paths"`
	Environment []string           `json:"environment"`
	Invocations []legacyInvocation `json:"invocations"`
	Replies     []legacyReply      `json:"replies"`
	Checkpoint  legacyCheckpoint   `json:"checkpoint"`
	Journal     legacyJournal      `json:"journal"`
	Capacity    *legacyCapacity    `json:"capacity,omitempty"`
	Coverage    []string           `json:"coverage"`
	Files       []legacyFile       `json:"files"`
	Roots       []legacyFile       `json:"roots"`
}

type legacyProducer struct {
	SHA       string     `json:"sha"`
	Tree      string     `json:"tree"`
	Binary    legacyFile `json:"binary"`
	GoVersion string     `json:"go_version"`
	Build     []string   `json:"build"`
}

type legacyGenerator struct {
	Files     []legacyFile `json:"files"`
	GoVersion string       `json:"go_version"`
	SQLite    string       `json:"sqlite"`
}

type legacyPaths struct {
	Root      string `json:"root"`
	Workspace string `json:"workspace"`
	State     string `json:"state"`
	Imports   string `json:"imports,omitempty"`
}

type legacyInvocation struct {
	Args  []string `json:"args"`
	Exit  string   `json:"exit"`
	Phase string   `json:"phase"`
}

type legacyReply struct {
	Run      int    `json:"run"`
	Step     int    `json:"step"`
	Attempt  int    `json:"attempt"`
	Request  string `json:"request_sha256"`
	Decision string `json:"decision,omitempty"`
	Hold     bool   `json:"hold,omitempty"`
}

type legacyCheckpoint struct {
	Busy         int    `json:"busy"`
	Log          int    `json:"log"`
	Checkpointed int    `json:"checkpointed"`
	Integrity    string `json:"integrity"`
	JournalMode  string `json:"journal_mode"`
}

// legacyJournal summarizes the raw database; the consumer recomputes it from
// its own snapshot before the current store opens the file.
type legacyJournal struct {
	DB          legacyFile       `json:"db"`
	UserVersion int              `json:"user_version"`
	Rows        map[string]int   `json:"rows"`
	Kinds       []string         `json:"kinds"`
	Runs        []legacyRunState `json:"runs"`
}

type legacyRunState struct {
	N          int64  `json:"n"`
	ID         string `json:"id"`
	Semantics  int64  `json:"semantics"`
	SessionID  string `json:"session_id,omitempty"`
	SessionSeq int64  `json:"session_seq,omitempty"`
	InputID    string `json:"input_id,omitempty"`
	Seq        int64  `json:"seq"`
	Phase      string `json:"phase"`
	Step       int    `json:"step"`
	Attempt    int    `json:"attempt"`
	RequestRev int    `json:"request_rev"`
	EffectID   string `json:"effect_id,omitempty"`
	JobID      string `json:"job_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// legacyCapacity is the producer's own evaluation of the handover trigger:
// at the last request the journal holds, and at the next preparation, which
// the producer ran on a discarded copy of the finished state directory.
type legacyCapacity struct {
	ContextBytes int                 `json:"context_bytes"`
	Last         legacyHandoverCheck `json:"last_request"`
	Probe        legacyHandoverCheck `json:"next_request_probe"`
	ProbeArgs    []string            `json:"probe_args"`
}

type legacyHandoverCheck struct {
	Step      int      `json:"step"`
	Status    string   `json:"status"`
	Used      int      `json:"used"`
	Capacity  int      `json:"capacity"`
	Growth    int      `json:"growth"`
	Headroom  int      `json:"headroom"`
	Requested bool     `json:"requested"`
	Reasons   []string `json:"reasons,omitempty"`
	Body      string   `json:"body_sha256"`
}

func legacyHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func legacyFileOf(name string, data []byte) legacyFile {
	return legacyFile{Path: name, Bytes: int64(len(data)), SHA256: legacyHash(data)}
}

func legacyStrict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// legacyCaseNames lists the <producer SHA>/<case> directories of the corpus.
func legacyCaseNames(t *testing.T) []string {
	t.Helper()
	producers, err := os.ReadDir(legacyDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range producers {
		cases, err := os.ReadDir(filepath.Join(legacyDir, p.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			if !c.IsDir() {
				t.Errorf("%s/%s is not a case directory", p.Name(), c.Name())
				continue
			}
			names = append(names, p.Name()+"/"+c.Name())
		}
	}
	slices.Sort(names)
	return names
}

// legacyCopy is a verified case expanded into a fresh directory: the journal
// at db and the producer's roots under roots.
type legacyCopy struct {
	manifest legacyManifest
	db       string
	roots    string
	files    map[string][]byte
	stored   int64
}

// openLegacyCase verifies every file of a case against its manifest and
// expands the journal and roots into a new temporary directory.
func openLegacyCase(t *testing.T, name string) legacyCopy {
	t.Helper()
	dir := filepath.Join(legacyDir, filepath.FromSlash(name))
	data, err := os.ReadFile(filepath.Join(dir, legacyManifestName))
	if err != nil {
		t.Fatal(err)
	}
	c := legacyCopy{files: map[string][]byte{}}
	if err := legacyStrict(data, &c.manifest); err != nil {
		t.Fatalf("%s: manifest: %v", name, err)
	}
	m := c.manifest
	if m.Format != 1 || m.Case != name || path.Dir(name) != m.Producer.SHA || len(m.Producer.SHA) != 40 {
		t.Fatalf("%s: manifest names format %d, case %q, producer %q", name, m.Format, m.Case, m.Producer.SHA)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, want := []string{}, []string{legacyManifestName}
	for _, e := range entries {
		got = append(got, e.Name())
	}
	stored := map[string][]byte{}
	for _, f := range m.Files {
		want = append(want, f.Path)
		b, err := os.ReadFile(filepath.Join(dir, f.Path))
		if err != nil {
			t.Fatal(err)
		}
		if legacyFileOf(f.Path, b) != f {
			t.Fatalf("%s: %s does not match its manifest entry %+v", name, f.Path, f)
		}
		stored[f.Path] = b
		c.stored += int64(len(b))
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("%s holds %v, its manifest lists %v", name, got, want)
	}
	db := legacyGunzip(t, stored[legacyDBName])
	if legacyFileOf("journal.db", db) != m.Journal.DB {
		t.Fatalf("%s: expanded journal does not match %+v", name, m.Journal.DB)
	}
	tmp := t.TempDir()
	c.db, c.roots = filepath.Join(tmp, "canti.db"), filepath.Join(tmp, "roots")
	if err := os.WriteFile(c.db, db, 0o600); err != nil {
		t.Fatal(err)
	}
	var members []legacyFile
	for name, b := range legacyUntar(t, legacyGunzip(t, stored[legacyRootsName]), c.roots) {
		members = append(members, legacyFileOf(name, b))
		c.files[name] = b
	}
	byPath := func(a, b legacyFile) int { return strings.Compare(a.Path, b.Path) }
	slices.SortFunc(members, byPath)
	if !slices.Equal(members, m.Roots) {
		t.Fatalf("%s: roots hold %+v, the manifest lists %+v", name, members, m.Roots)
	}
	return c
}

func legacyGunzip(t *testing.T, data []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(io.LimitReader(r, legacyMaxDB+1))
	if err == nil && len(out) > legacyMaxDB {
		err = fmt.Errorf("expands beyond %d bytes", legacyMaxDB)
	}
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// legacyUntar writes the directories and regular files of a tar archive
// under dir and returns the files by slash-separated name.
func legacyUntar(t *testing.T, data []byte, dir string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := r.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "" || path.Clean(name) != name || path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			t.Fatalf("roots entry %q is not a clean relative path", h.Name)
		}
		target := filepath.Join(dir, filepath.FromSlash(name))
		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			var b []byte
			if b, err = io.ReadAll(r); err == nil {
				files[name] = b
				err = os.MkdirAll(filepath.Dir(target), 0o755)
			}
			if err == nil {
				err = os.WriteFile(target, b, 0o644)
			}
		default:
			err = fmt.Errorf("roots entry %q has type %q", h.Name, h.Typeflag)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// legacyDB is a raw snapshot of every table, keyed by primary key, with the
// column values exactly as SQLite returns them.
type legacyDB struct {
	version int
	tables  map[string]legacyTable
}

type legacyTable struct {
	key  []string
	rows map[string]legacyRow
}

type legacyRow map[string]any

func openLegacySQL(t *testing.T, file, query string) *sql.DB {
	t.Helper()
	abs, err := filepath.Abs(file)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", OmitHost: true, Path: abs, RawQuery: query}).String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

// snapshotLegacy reads every row of the database at file without writing.
func snapshotLegacy(t *testing.T, file string) legacyDB {
	t.Helper()
	db := openLegacySQL(t, file, "_pragma=query_only(1)&_pragma=busy_timeout(5000)")
	defer db.Close()
	ctx := context.Background()
	out := legacyDB{tables: map[string]legacyTable{}}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&out.version); err != nil {
		t.Fatal(err)
	}
	out.tables["sqlite_schema"] = legacyReadTable(t, db, "sqlite_schema", []string{"name"})
	for _, row := range out.tables["sqlite_schema"].rows {
		name := row.text(t, "name")
		if row.text(t, "type") != "table" || strings.HasPrefix(name, "sqlite_") {
			continue
		}
		rows, err := db.QueryContext(ctx, `SELECT name, pk FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk`, name)
		if err != nil {
			t.Fatal(err)
		}
		var key []string
		for rows.Next() {
			var column string
			var pk int
			if err := rows.Scan(&column, &pk); err != nil {
				t.Fatal(err)
			}
			key = append(key, column)
		}
		if err := rows.Close(); err != nil || len(key) == 0 {
			t.Fatalf("table %s has no primary key: %v", name, err)
		}
		out.tables[name] = legacyReadTable(t, db, name, key)
	}
	return out
}

func legacyReadTable(t *testing.T, db *sql.DB, name string, key []string) legacyTable {
	t.Helper()
	rows, err := db.Query(`SELECT * FROM "` + name + `"`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	table := legacyTable{key: key, rows: map[string]legacyRow{}}
	for rows.Next() {
		values, targets := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		row := legacyRow{}
		for i, c := range columns {
			row[c] = values[i]
		}
		k := row.key(key)
		if _, dup := table.rows[k]; dup {
			t.Fatalf("table %s repeats key %s", name, k)
		}
		table.rows[k] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return table
}

func (r legacyRow) key(columns []string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = fmt.Sprint(r[c])
	}
	return strings.Join(parts, "\x00")
}

func (r legacyRow) text(t *testing.T, column string) string {
	t.Helper()
	s, ok := r[column].(string)
	if !ok {
		t.Fatalf("column %s holds %T, not TEXT", column, r[column])
	}
	return s
}

func (r legacyRow) num(t *testing.T, column string) int64 {
	t.Helper()
	n, ok := r[column].(int64)
	if !ok {
		t.Fatalf("column %s holds %T, not INTEGER", column, r[column])
	}
	return n
}

// sorted returns the rows of table ordered by the integer columns given.
func (db legacyDB) sorted(t *testing.T, table string, keep func(legacyRow) bool, by ...string) []legacyRow {
	t.Helper()
	var out []legacyRow
	for _, row := range db.tables[table].rows {
		if keep == nil || keep(row) {
			out = append(out, row)
		}
	}
	slices.SortFunc(out, func(a, b legacyRow) int {
		for _, c := range by {
			if x, y := a.num(t, c), b.num(t, c); x != y {
				return int(min(max(x-y, -1), 1))
			}
		}
		return 0
	})
	return out
}

func (db legacyDB) runs(t *testing.T) []legacyRow { return db.sorted(t, "runs", nil, "n") }

func (db legacyDB) transitions(t *testing.T, runID string) []legacyRow {
	return db.sorted(t, "transitions", func(r legacyRow) bool { return r.text(t, "run_id") == runID }, "seq")
}

// legacyView is the part of a stored state the corpus describes. It decodes
// the raw JSON without the current store.
type legacyView struct {
	Step, Attempt, RequestRev int
	Phase, EffectID, JobID    string
	Reason                    string
}

func legacyViewOf(t *testing.T, state string) legacyView {
	t.Helper()
	var v legacyView
	if err := json.Unmarshal([]byte(state), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// legacyFacts decodes the raw facts JSON of a transition for the fields an
// oracle needs, independently of the current store.
type legacyFacts struct {
	Request *struct {
		Body    []byte
		Window  *struct{ Requested bool }
		Context *struct {
			Handover struct {
				Status                 string
				Used, Capacity, Growth int
				Reasons                []string
			}
		}
	}
	Response *struct{ Body []byte }
}

func legacyFactsOf(t *testing.T, row legacyRow) legacyFacts {
	t.Helper()
	var f legacyFacts
	if err := json.Unmarshal([]byte(row.text(t, "facts")), &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// legacySummary derives the manifest's journal summary from a raw snapshot.
func legacySummary(t *testing.T, db legacyDB) legacyJournal {
	t.Helper()
	j := legacyJournal{UserVersion: db.version, Rows: map[string]int{}}
	for name, table := range db.tables {
		if name != "sqlite_schema" {
			j.Rows[name] = len(table.rows)
		}
	}
	for _, row := range db.tables["transitions"].rows {
		if kind := row.text(t, "kind"); !slices.Contains(j.Kinds, kind) {
			j.Kinds = append(j.Kinds, kind)
		}
	}
	slices.Sort(j.Kinds)
	sessions := map[string]legacyRow{}
	for _, row := range db.tables["session_runs"].rows {
		sessions[row.text(t, "run_id")] = row
	}
	for _, run := range db.runs(t) {
		id := run.text(t, "id")
		s := legacyRunState{N: run.num(t, "n"), ID: id, Semantics: run.num(t, "semantics")}
		if row, ok := sessions[id]; ok {
			s.SessionID, s.SessionSeq, s.InputID = row.text(t, "session_id"), row.num(t, "seq"), row.text(t, "input_id")
		}
		state := run.text(t, "initial")
		if rows := db.transitions(t, id); len(rows) > 0 {
			last := rows[len(rows)-1]
			s.Seq, state = last.num(t, "seq"), last.text(t, "to_state")
		}
		v := legacyViewOf(t, state)
		s.Phase, s.Step, s.Attempt, s.RequestRev = v.Phase, v.Step, v.Attempt, v.RequestRev
		s.EffectID, s.JobID, s.Reason = v.EffectID, v.JobID, v.Reason
		j.Runs = append(j.Runs, s)
	}
	return j
}

// legacyLastCheck returns the handover check the last frozen request of a
// run recorded.
func legacyLastCheck(t *testing.T, db legacyDB, runID string) legacyHandoverCheck {
	t.Helper()
	rows := db.transitions(t, runID)
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].text(t, "kind") != "request_frozen" {
			continue
		}
		f := legacyFactsOf(t, rows[i])
		if f.Request == nil || f.Request.Window == nil || f.Request.Context == nil {
			t.Fatalf("request at seq %d records no window or context", rows[i].num(t, "seq"))
		}
		h := f.Request.Context.Handover
		return legacyHandoverCheck{Step: int(rows[i].num(t, "step")), Status: h.Status, Used: h.Used, Capacity: h.Capacity,
			Growth: h.Growth, Headroom: h.Capacity - h.Used - h.Growth, Requested: f.Request.Window.Requested,
			Reasons: h.Reasons, Body: legacyHash(f.Request.Body)}
	}
	t.Fatalf("run %s froze no request", runID)
	return legacyHandoverCheck{}
}
