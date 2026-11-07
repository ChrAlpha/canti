package harness_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func legacyState(t *testing.T, data string) core.State {
	t.Helper()
	var s core.State
	if err := json.Unmarshal([]byte(data), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// legacySame fails unless v, as read through the current store, encodes to
// exactly the JSON the producer stored, so no read lost or invented a field.
func legacySame(t *testing.T, what string, v any, raw string) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != raw {
		at := 0
		for at < min(len(got), len(raw)) && got[at] == raw[at] {
			at++
		}
		from := max(0, at-80)
		t.Fatalf("%s reads back differently at byte %d:\nread   …%s…\nstored …%s…", what, at, got[from:min(len(got), at+80)], raw[from:min(len(raw), at+80)])
	}
}

// legacyField returns the raw JSON of one fact of a stored transition.
func legacyField(t *testing.T, row legacyRow, name string) string {
	t.Helper()
	var facts map[string]json.RawMessage
	if err := json.Unmarshal([]byte(row.text(t, "facts")), &facts); err != nil {
		t.Fatal(err)
	}
	return string(facts[name])
}

func legacySameTransition(t *testing.T, tr harness.Transition, row legacyRow) {
	t.Helper()
	what := row.text(t, "run_id") + " seq " + strconv.FormatInt(row.num(t, "seq"), 10)
	if tr.Seq != row.num(t, "seq") || tr.EventID != row.text(t, "event_id") || tr.Event.Kind() != row.text(t, "kind") {
		t.Fatalf("%s reads as seq %d %s %q", what, tr.Seq, tr.Event.Kind(), tr.EventID)
	}
	legacySame(t, what+" event", tr.Event, row.text(t, "event"))
	legacySame(t, what+" from", tr.From, row.text(t, "from_state"))
	legacySame(t, what+" to", tr.To, row.text(t, "to_state"))
	legacySame(t, what+" facts", tr.Facts, row.text(t, "facts"))
}

// exerciseLegacyReads reads every run through every read of the current
// store, paging with limits 1 and 2 until exhausted, and requires each
// result to re-encode to the raw JSON of db, a snapshot of the same file.
// open names the run that must block admission, or is empty when admission
// must succeed; sessions is false for a store that has no session table.
func exerciseLegacyReads(t *testing.T, store *sqlite.Store, db legacyDB, open string, sessions bool) {
	t.Helper()
	ctx := context.Background()
	runs := db.runs(t)
	raw := map[string]string{}
	for _, row := range runs {
		raw[row.text(t, "id")] = row.text(t, "run")
	}
	for _, row := range runs {
		id := row.text(t, "id")
		rows := db.transitions(t, id)
		bySeq := map[int64]legacyRow{}
		for _, r := range rows {
			bySeq[r.num(t, "seq")] = r
		}
		initial := legacyState(t, row.text(t, "initial"))
		run, err := store.Run(ctx, id)
		if err != nil {
			t.Fatalf("Run(%s): %v", id, err)
		}
		legacySame(t, "run "+id, run, raw[id])
		last, final := int64(0), initial
		if len(rows) > 0 {
			last, final = rows[len(rows)-1].num(t, "seq"), legacyState(t, rows[len(rows)-1].text(t, "to_state"))
		}
		if snap, err := store.Latest(ctx, id); err != nil || snap.Seq != last || snap.State != final {
			t.Fatalf("Latest(%s) = %d %+v, %v; the raw journal ends at %d %+v", id, snap.Seq, snap.State, err, last, final)
		}
		for _, limit := range []int{1, 2} {
			var got []harness.Transition
			for after := int64(0); ; {
				page, err := store.Transitions(ctx, id, after, limit)
				if err != nil {
					t.Fatalf("Transitions(%s, %d, %d): %v", id, after, limit, err)
				}
				got = append(got, page...)
				if len(page) < limit {
					break
				}
				after = page[len(page)-1].Seq
			}
			if len(got) != len(rows) {
				t.Fatalf("Transitions of %s by %d returned %d of %d records", id, limit, len(got), len(rows))
			}
			from := initial
			for i, tr := range got {
				legacySameTransition(t, tr, rows[i])
				if tr.From != from {
					t.Fatalf("seq %d of %s does not extend the recorded chain", tr.Seq, id)
				}
				from = tr.To
			}
		}
		var frozen, steps []legacyRow
		for _, r := range rows {
			step := int(r.num(t, "step"))
			switch r.text(t, "kind") {
			case "request_frozen":
				frozen = append(frozen, r)
				req, err := store.Request(ctx, id, step, int(r.num(t, "rev")))
				if err != nil {
					t.Fatalf("Request(%s, %d, %d): %v", id, step, r.num(t, "rev"), err)
				}
				legacySame(t, "request of "+id, req, legacyField(t, r, "Request"))
			case "response_saved":
				resp, err := store.Response(ctx, id, step, int(r.num(t, "attempt")))
				if err != nil {
					t.Fatalf("Response(%s, %d, %d): %v", id, step, r.num(t, "attempt"), err)
				}
				legacySame(t, "response of "+id, resp, legacyField(t, r, "Response"))
			case "observed", "resolved":
				steps = append(steps, r)
			}
		}
		req, err := store.LatestRequest(ctx, id)
		switch {
		case len(frozen) == 0 && !errors.Is(err, harness.ErrNotFound):
			t.Fatalf("LatestRequest(%s) of a run without requests: %v", id, err)
		case len(frozen) > 0 && err != nil:
			t.Fatalf("LatestRequest(%s): %v", id, err)
		case len(frozen) > 0:
			legacySame(t, "latest request of "+id, req, legacyField(t, frozen[len(frozen)-1], "Request"))
		}
		_, errRequest := store.Request(ctx, id, 1<<20, 1)
		_, errResponse := store.Response(ctx, id, 1<<20, 1)
		if !errors.Is(errRequest, harness.ErrNotFound) || !errors.Is(errResponse, harness.ErrNotFound) {
			t.Fatalf("missing request or response of %s: %v, %v", id, errRequest, errResponse)
		}
		for _, limit := range []int{1, 2} {
			var got []harness.StepRecord
			for before := int64(0); ; {
				page, err := store.Recent(ctx, id, before, limit)
				if err != nil {
					t.Fatalf("Recent(%s, %d, %d): %v", id, before, limit, err)
				}
				got = append(slices.Clone(page), got...)
				if len(page) < limit {
					break
				}
				before = page[0].Seq
			}
			if len(got) != len(steps) {
				t.Fatalf("Recent of %s by %d returned %d of %d steps", id, limit, len(got), len(steps))
			}
			for i, s := range got {
				from := legacyState(t, steps[i].text(t, "from_state"))
				if s.Seq != steps[i].num(t, "seq") || s.Step != from.Step || s.Command != from.Decision.Payload || s.Resolved != (steps[i].text(t, "kind") == "resolved") {
					t.Fatalf("step record %d of %s is %d step %d, want seq %d step %d", i+1, id, s.Seq, s.Step, steps[i].num(t, "seq"), from.Step)
				}
				legacySame(t, "step record of "+id, s.Observation, legacyField(t, steps[i], "Observation"))
			}
		}
		totals, err := store.Totals(ctx, id)
		want := harness.Totals{Usage: harness.Usage{InputTokens: row.num(t, "input_tokens"), OutputTokens: row.num(t, "output_tokens")},
			OutputBytes: row.num(t, "output_bytes"), Steps: int(row.num(t, "steps"))}
		if err != nil || totals != want {
			t.Fatalf("Totals(%s) = %+v, %v; the run row holds %+v", id, totals, err, want)
		}
		exerciseLegacyContext(t, store, id, raw[id], rows, bySeq, frozen, last)
	}
	latest, err := store.LastRun(ctx)
	if err != nil {
		t.Fatalf("LastRun: %v", err)
	}
	legacySame(t, "last run", latest, raw[runs[len(runs)-1].text(t, "id")])
	if err := store.CheckAdmission(ctx); open == "" && err != nil || open != "" && (err == nil || !strings.Contains(err.Error(), open)) {
		t.Fatalf("CheckAdmission = %v, want a refusal naming %q", err, open)
	}
	if sessions {
		exerciseLegacySessions(t, store, db, raw)
	} else {
		_, errRuns := store.SessionRuns(ctx, "legacy-missing", 0, 1)
		_, errInput := store.SessionInput(ctx, "legacy-missing", "input-1")
		if errRuns == nil || errInput == nil {
			t.Fatalf("session reads of a journal without a session table = %v, %v", errRuns, errInput)
		}
	}
	const missing = "run_legacy_missing"
	_, errRun := store.Run(ctx, missing)
	_, errLatest := store.Latest(ctx, missing)
	_, errTotals := store.Totals(ctx, missing)
	_, errTransitions := store.Transitions(ctx, missing, 0, 1)
	_, errRecent := store.Recent(ctx, missing, 0, 1)
	_, errContext := store.ContextRequest(ctx, missing, 0, 0)
	_, errRecords := store.ContextRecords(ctx, missing, 0, 0, 0, 1)
	for i, err := range []error{errRun, errLatest, errTotals, errTransitions, errRecent, errContext, errRecords} {
		if !errors.Is(err, harness.ErrNotFound) {
			t.Fatalf("read %d of a missing run: %v", i+1, err)
		}
	}
}

// exerciseLegacyContext reads every frozen request of a run through both
// context reads and pages each request's events with limits 1 and 2.
func exerciseLegacyContext(t *testing.T, store *sqlite.Store, id, run string, rows []legacyRow, bySeq map[int64]legacyRow, frozen []legacyRow, last int64) {
	t.Helper()
	ctx := context.Background()
	if len(frozen) > 0 {
		latest, err := store.ContextRequest(ctx, id, 0, 0)
		if err != nil {
			t.Fatalf("latest ContextRequest(%s): %v", id, err)
		}
		legacySameTransition(t, latest, frozen[len(frozen)-1])
	}
	if _, err := store.ContextRequest(ctx, id, 1<<20, 0); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("ContextRequest of a missing step of %s: %v", id, err)
	}
	for i, r := range frozen {
		step, rev, seq := int(r.num(t, "step")), int(r.num(t, "rev")), r.num(t, "seq")
		req, err := store.ContextRequest(ctx, id, step, rev)
		if err != nil {
			t.Fatalf("ContextRequest(%s, %d, %d): %v", id, step, rev, err)
		}
		legacySameTransition(t, req, r)
		end := last
		if i+1 < len(frozen) {
			end = frozen[i+1].num(t, "seq") - 1
		}
		var want []int64
		for _, e := range rows {
			if s := e.num(t, "seq"); s > seq && s <= end {
				want = append(want, s)
			}
		}
		for _, limit := range []int{1, 2} {
			var got []int64
			for after, n := int64(0), 0; ; n++ {
				page, err := store.ContextRecords(ctx, id, step, rev, after, limit)
				if err != nil || page.Request.Seq != seq || page.AsOfSeq != last || len(page.Events) > limit {
					t.Fatalf("ContextRecords(%s, %d, %d, %d, %d) = request %d as of %d with %d events, %v",
						id, step, rev, after, limit, page.Request.Seq, page.AsOfSeq, len(page.Events), err)
				}
				legacySame(t, "context run of "+id, page.Run, run)
				legacySameTransition(t, page.Request, r)
				for j, near := range []*harness.Transition{page.Previous, page.Next} {
					k := i - 1 + 2*j
					if (near != nil) != (k >= 0 && k < len(frozen)) {
						t.Fatalf("ContextRecords of %s step %d revision %d has the wrong neighbouring requests", id, step, rev)
					}
					if near != nil {
						legacySameTransition(t, *near, frozen[k])
					}
				}
				for _, e := range page.Events {
					legacySameTransition(t, e, bySeq[e.Seq])
					got, after = append(got, e.Seq), e.Seq
				}
				if !page.More {
					break
				}
				if len(page.Events) == 0 || n > len(rows) {
					t.Fatalf("ContextRecords of %s step %d revision %d made no progress", id, step, rev)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("ContextRecords of %s step %d revision %d by %d returned %v, want %v", id, step, rev, limit, got, want)
			}
		}
	}
}

func exerciseLegacySessions(t *testing.T, store *sqlite.Store, db legacyDB, raw map[string]string) {
	t.Helper()
	ctx := context.Background()
	sessions := map[string][]legacyRow{}
	for _, r := range db.sorted(t, "session_runs", nil, "seq") {
		sessions[r.text(t, "session_id")] = append(sessions[r.text(t, "session_id")], r)
	}
	for session, rows := range sessions {
		var want []string
		for i := len(rows) - 1; i >= 0; i-- {
			want = append(want, rows[i].text(t, "run_id"))
		}
		for _, limit := range []int{1, 2} {
			var got []string
			for before := int64(0); ; {
				page, err := store.SessionRuns(ctx, session, before, limit)
				if err != nil {
					t.Fatalf("SessionRuns(%s, %d, %d): %v", session, before, limit, err)
				}
				for _, r := range page {
					legacySame(t, "session run "+r.ID, r, raw[r.ID])
					got = append(got, r.ID)
				}
				if len(page) < limit {
					break
				}
				before = page[len(page)-1].SessionSeq
			}
			if !slices.Equal(got, want) {
				t.Fatalf("SessionRuns of %s by %d returned %v, want %v", session, limit, got, want)
			}
		}
		for _, r := range rows {
			run, err := store.SessionInput(ctx, session, r.text(t, "input_id"))
			if err != nil || run.ID != r.text(t, "run_id") || run.SessionSeq != r.num(t, "seq") {
				t.Fatalf("SessionInput(%s, %s) = %q at %d, %v", session, r.text(t, "input_id"), run.ID, run.SessionSeq, err)
			}
			legacySame(t, "session input "+run.ID, run, raw[run.ID])
		}
	}
	if page, err := store.SessionRuns(ctx, "legacy-missing", 0, 1); err != nil || len(page) != 0 {
		t.Fatalf("SessionRuns of a missing session = %d runs, %v", len(page), err)
	}
	if _, err := store.SessionInput(ctx, "legacy-missing", "input-1"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("SessionInput of a missing session: %v", err)
	}
}

// compareLegacy fails unless after, at schema version, keeps every row of
// before byte for byte and adds nothing but, to a schema-1 journal it
// migrated, exactly the session table a new journal has and, after a
// writable open, the index of performed steps a new journal has.
func compareLegacy(t *testing.T, before, after legacyDB, version int, writable bool) {
	t.Helper()
	if after.version != version {
		t.Errorf("schema version %d, want %d after version %d", after.version, version, before.version)
	}
	for name, table := range before.tables {
		now, ok := after.tables[name]
		if !ok {
			t.Errorf("table %s is gone", name)
			continue
		}
		for key, row := range table.rows {
			got, ok := now.rows[key]
			if !ok {
				t.Errorf("%s row %q is gone", name, key)
				continue
			}
			if !reflect.DeepEqual(got, row) {
				t.Errorf("%s row %q = %#v, want %#v", name, key, got, row)
			}
		}
	}
	session := map[string]legacyRow{}
	if before.version == 1 && version == 2 {
		session = legacySessionSchema(t)
	}
	if _, ok := before.tables["sqlite_schema"].rows[performedIndex]; !ok && writable {
		session[performedIndex] = freshSchemaObject(t, performedIndex)
	}
	for name, table := range after.tables {
		for key, row := range table.rows {
			if _, ok := before.tables[name].rows[key]; ok {
				continue
			}
			fresh, ok := session[key]
			if name != "sqlite_schema" || !ok {
				t.Errorf("%s gained row %q", name, key)
				continue
			}
			for _, column := range []string{"type", "name", "tbl_name", "sql"} {
				if !reflect.DeepEqual(row[column], fresh[column]) {
					t.Errorf("migrated schema object %s has %s %#v, a new journal %#v", key, column, row[column], fresh[column])
				}
			}
			delete(session, key)
		}
	}
	for name := range session {
		t.Errorf("the migration did not create %s", name)
	}
}

// performedIndex is the schema object that a writable open adds to a
// journal of an earlier producer without changing its schema version.
const performedIndex = "transitions_performed"

// freshSchemaObject returns the schema object name of a new journal.
func freshSchemaObject(t *testing.T, name string) legacyRow {
	t.Helper()
	file := filepath.Join(t.TempDir(), "canti.db")
	s, err := sqlite.Open(context.Background(), file)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	row, ok := snapshotLegacy(t, file).tables["sqlite_schema"].rows[name]
	if !ok {
		t.Fatalf("a new journal has no %s", name)
	}
	delete(row, "rootpage")
	return row
}

// legacySessionSchema returns the schema objects of the session table that
// the semantics-2 producer created in every journal of the corpus it wrote.
func legacySessionSchema(t *testing.T) map[string]legacyRow {
	t.Helper()
	var objects map[string]legacyRow
	for _, name := range legacyCaseNames(t) {
		if !strings.HasPrefix(name, legacySemantics2+"/") {
			continue
		}
		found := map[string]legacyRow{}
		for key, row := range snapshotLegacy(t, openLegacyCase(t, name).db).tables["sqlite_schema"].rows {
			if row.text(t, "tbl_name") == "session_runs" {
				delete(row, "rootpage")
				found[key] = row
			}
		}
		if objects != nil && !reflect.DeepEqual(found, objects) {
			t.Fatalf("%s holds another session table than the other journals of its producer", name)
		}
		objects = found
	}
	if len(objects) == 0 {
		t.Fatalf("no journal of %s holds a session table", legacySemantics2)
	}
	return objects
}
