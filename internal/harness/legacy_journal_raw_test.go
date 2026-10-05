package harness_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

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
