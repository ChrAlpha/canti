package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestContextRawReadsOnlySelectedFrozenRequest(t *testing.T) {
	for _, tc := range []struct {
		name, update string
		revision     int
		refused      string
	}{
		{"previous request", `UPDATE transitions SET facts = '{' WHERE kind = 'request_frozen' AND rev = 1`, 2, ""},
		{"next request", `UPDATE transitions SET facts = '{' WHERE kind = 'request_frozen' AND rev = 2`, 1, ""},
		{"following response", `UPDATE transitions SET facts = '{' WHERE kind = 'response_saved'`, 1, ""},
		{"following event", `UPDATE transitions SET event = '{' WHERE kind = 'attempt_started'`, 2, ""},
		{"run metadata", `UPDATE runs SET run = '{'`, 2, "incompatible record"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
			f.model.answer(1, 1, reply{text: "invalid decision"})
			if r := f.cli(nil, "run", "-goal", "inspect raw isolation"); r.code != exitOK {
				t.Fatalf("run: %+v", r)
			}
			runID := f.status().Run.ID
			requests := f.model.requests()
			db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(tc.update); err != nil {
				t.Fatal(err)
			}
			before := rawJournalSnapshot(t, db)
			r := f.cli(nil, "context", "-raw", "-run", runID, "-step", "1", "-revision", fmt.Sprint(tc.revision))
			if tc.refused != "" {
				if r.code != exitError || r.stdout != "" || !strings.Contains(r.stderr, tc.refused) {
					t.Fatalf("export without the run's workspace: code %d, stderr %s, bytes %d; want a refusal", r.code, r.stderr, len(r.stdout))
				}
			} else if r.code != exitOK || r.stdout != string(requests[tc.revision-1]) {
				t.Fatalf("selected request export: code %d, stderr %s, bytes %d; want %d", r.code, r.stderr, len(r.stdout), len(requests[tc.revision-1]))
			}
			if after := rawJournalSnapshot(t, db); !reflect.DeepEqual(before, after) || len(f.model.requests()) != len(requests) {
				t.Fatal("raw export changed durable records or called provider")
			}
		})
	}
}

func TestContextRawSelectsLatestRunAndRevision(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	f.model.answer(1, 1, reply{text: "invalid decision"})
	if r := f.cli(nil, "run", "-goal", "first run"); r.code != exitOK {
		t.Fatalf("first run: %+v", r)
	}
	firstID := f.status().Run.ID
	if r := f.cli(nil, "run", "-goal", "second run"); r.code != exitOK {
		t.Fatalf("second run: %+v", r)
	}
	requests := f.model.requests()
	for _, tc := range []struct {
		args  []string
		index int
	}{
		{nil, 2},
		{[]string{"-run", firstID}, 1},
		{[]string{"-run", firstID, "-step", "1"}, 1},
		{[]string{"-run", firstID, "-step", "1", "-revision", "1"}, 0},
	} {
		r := f.cli(nil, "context", append([]string{"-raw"}, tc.args...)...)
		if r.code != exitOK || r.stdout != string(requests[tc.index]) {
			t.Fatalf("selection %v: code %d, stderr %s, bytes %d", tc.args, r.code, r.stderr, len(r.stdout))
		}
	}
}

func TestContextRawRejectsMalformedSelectedRequest(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "selected record must remain strict"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE transitions SET facts = json_insert(facts, '$.Unknown', 1) WHERE kind = 'request_frozen'`); err != nil {
		t.Fatal(err)
	}
	before := rawJournalSnapshot(t, db)
	if r := f.cli(nil, "context", "-raw"); r.code != exitError || r.stdout != "" {
		t.Fatalf("malformed selected request: %+v", r)
	}
	if after := rawJournalSnapshot(t, db); !reflect.DeepEqual(before, after) || len(f.model.requests()) != 1 {
		t.Fatal("failed export changed durable records or called provider")
	}
}

func rawJournalSnapshot(t *testing.T, db *sql.DB) [3]string {
	t.Helper()
	var snapshot [3]string
	for i, query := range []string{
		`SELECT json_group_array(json_array(n, id, semantics, run, initial, input_tokens, output_tokens, output_bytes, steps)) FROM (SELECT * FROM runs ORDER BY n)`,
		`SELECT json_group_array(json_array(run_id, seq, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)) FROM (SELECT * FROM transitions ORDER BY run_id, seq)`,
		`SELECT json_group_array(json_array(session_id, seq, input_id, run_id)) FROM (SELECT * FROM session_runs ORDER BY session_id, seq)`,
	} {
		if err := db.QueryRow(query).Scan(&snapshot[i]); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}
