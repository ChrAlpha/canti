package harness_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
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

// exerciseLegacyReads reads every run through every read of the current
// store, paging with limits 1 and 2 until exhausted, and checks each result
// against db, a raw snapshot of the same file. open names the run that must
// block admission, or is empty when admission must succeed.
func exerciseLegacyReads(t *testing.T, store *sqlite.Store, db legacyDB, open string) {
	t.Helper()
	ctx := context.Background()
	runs := db.runs(t)
	for _, row := range runs {
		id := row.text(t, "id")
		rows := db.transitions(t, id)
		initial := legacyState(t, row.text(t, "initial"))
		if run, err := store.Run(ctx, id); err != nil || run.ID != id {
			t.Fatalf("Run(%s) = %q, %v", id, run.ID, err)
		}
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
				r := rows[i]
				if tr.Seq != r.num(t, "seq") || tr.EventID != r.text(t, "event_id") || tr.Event.Kind() != r.text(t, "kind") ||
					tr.From != from || tr.To != legacyState(t, r.text(t, "to_state")) {
					t.Fatalf("transition %d of %s reads as seq %d %s %q, breaking the recorded chain", i+1, id, tr.Seq, tr.Event.Kind(), tr.EventID)
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
				if err != nil || !bytes.Equal(req.Body, legacyFactsOf(t, r).Request.Body) {
					t.Fatalf("Request(%s, %d, %d) does not return the frozen body: %v", id, step, r.num(t, "rev"), err)
				}
			case "response_saved":
				resp, err := store.Response(ctx, id, step, int(r.num(t, "attempt")))
				if err != nil || !bytes.Equal(resp.Body, legacyFactsOf(t, r).Response.Body) {
					t.Fatalf("Response(%s, %d, %d) does not return the saved body: %v", id, step, r.num(t, "attempt"), err)
				}
			case "observed", "resolved":
				steps = append(steps, r)
			}
		}
		req, err := store.LatestRequest(ctx, id)
		switch {
		case len(frozen) == 0 && !errors.Is(err, harness.ErrNotFound):
			t.Fatalf("LatestRequest(%s) of a run without requests: %v", id, err)
		case len(frozen) > 0 && (err != nil || !bytes.Equal(req.Body, legacyFactsOf(t, frozen[len(frozen)-1]).Request.Body)):
			t.Fatalf("LatestRequest(%s) is not the last frozen request: %v", id, err)
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
			}
		}
		totals, err := store.Totals(ctx, id)
		want := harness.Totals{Usage: harness.Usage{InputTokens: row.num(t, "input_tokens"), OutputTokens: row.num(t, "output_tokens")},
			OutputBytes: row.num(t, "output_bytes"), Steps: int(row.num(t, "steps"))}
		if err != nil || totals != want {
			t.Fatalf("Totals(%s) = %+v, %v; the run row holds %+v", id, totals, err, want)
		}
		exerciseLegacyContext(t, store, id, rows, frozen, last)
	}
	if latest, err := store.LastRun(ctx); err != nil || latest.ID != runs[len(runs)-1].text(t, "id") {
		t.Fatalf("LastRun = %q, %v", latest.ID, err)
	}
	if err := store.CheckAdmission(ctx); open == "" && err != nil || open != "" && (err == nil || !strings.Contains(err.Error(), open)) {
		t.Fatalf("CheckAdmission = %v, want a refusal naming %q", err, open)
	}
	exerciseLegacySessions(t, store, db)
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
func exerciseLegacyContext(t *testing.T, store *sqlite.Store, id string, rows, frozen []legacyRow, last int64) {
	t.Helper()
	ctx := context.Background()
	if len(frozen) > 0 {
		latest, err := store.ContextRequest(ctx, id, 0, 0)
		if err != nil || latest.Seq != frozen[len(frozen)-1].num(t, "seq") {
			t.Fatalf("latest ContextRequest(%s) = seq %d, %v", id, latest.Seq, err)
		}
	}
	if _, err := store.ContextRequest(ctx, id, 1<<20, 0); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("ContextRequest of a missing step of %s: %v", id, err)
	}
	for i, r := range frozen {
		step, rev, seq := int(r.num(t, "step")), int(r.num(t, "rev")), r.num(t, "seq")
		if req, err := store.ContextRequest(ctx, id, step, rev); err != nil || req.Seq != seq || req.EventID != r.text(t, "event_id") {
			t.Fatalf("ContextRequest(%s, %d, %d) = seq %d, %v", id, step, rev, req.Seq, err)
		}
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
				if err != nil || page.Run.ID != id || page.Request.Seq != seq || page.AsOfSeq != last || len(page.Events) > limit {
					t.Fatalf("ContextRecords(%s, %d, %d, %d, %d) = request %d as of %d with %d events, %v",
						id, step, rev, after, limit, page.Request.Seq, page.AsOfSeq, len(page.Events), err)
				}
				for _, e := range page.Events {
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

func exerciseLegacySessions(t *testing.T, store *sqlite.Store, db legacyDB) {
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
		}
	}
	if page, err := store.SessionRuns(ctx, "legacy-missing", 0, 1); err != nil || len(page) != 0 {
		t.Fatalf("SessionRuns of a missing session = %d runs, %v", len(page), err)
	}
	if _, err := store.SessionInput(ctx, "legacy-missing", "input-1"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("SessionInput of a missing session: %v", err)
	}
}

type legacyCounters struct{ input, output, bytes, steps int64 }

// compareLegacy fails unless after keeps every row of before byte for byte,
// except that the counters of the runs in grown rose by exactly those
// amounts, and adds nothing but the appended transitions of those runs and,
// to a schema-1 journal, the objects of its new session table.
func compareLegacy(t *testing.T, before, after legacyDB, grown map[string]legacyCounters, appended map[string]int) {
	t.Helper()
	if after.version != 2 {
		t.Errorf("schema version %d after the current store opened version %d", after.version, before.version)
	}
	counters := []string{"input_tokens", "output_tokens", "output_bytes", "steps"}
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
			want := row
			if name == "runs" {
				want = maps.Clone(row)
				d := grown[row.text(t, "id")]
				for i, add := range []int64{d.input, d.output, d.bytes, d.steps} {
					want[counters[i]] = row.num(t, counters[i]) + add
				}
			}
			for column := range maps.Keys(got) {
				if _, ok := want[column]; !ok {
					t.Errorf("%s row %q gained column %s", name, key, column)
				}
			}
			for column, value := range want {
				if !reflect.DeepEqual(got[column], value) {
					t.Errorf("%s row %q column %s = %#v, want %#v", name, key, column, got[column], value)
				}
			}
		}
	}
	added := map[string]int{}
	for name, table := range after.tables {
		for key, row := range table.rows {
			if _, ok := before.tables[name].rows[key]; ok {
				continue
			}
			switch {
			case name == "sqlite_schema" && before.version == 1 && row.text(t, "tbl_name") == "session_runs":
			case name == "transitions" && appended[row.text(t, "run_id")] > 0:
				added[row.text(t, "run_id")]++
			default:
				t.Errorf("%s gained row %q", name, key)
			}
		}
	}
	for id, n := range appended {
		if added[id] != n {
			t.Errorf("run %s gained %d transitions, want %d", id, added[id], n)
		}
	}
}
