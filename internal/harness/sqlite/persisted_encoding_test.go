package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

const (
	persistedRun    = "run_0123456789abcdef0123456789abcdef"
	persistedEffect = "effect_150f1d740dec1a6883c2261d90b0981f51817ab5dfbc37fa76e7a0dd77aeca1a"
	persistedJob    = "9d4c1f6e2b7a8053c1e4f7a2b9d0c3e6f8a1b4d7c0e3f6a9b2c5d8e1f4a7b0c3"

	runRecord = `{"ID":"run_0123456789abcdef0123456789abcdef","SessionID":"session-1","SessionSeq":1,"InputID":"input-1",` +
		`"Goal":"Fix the failing test in ./parser.","Constraints":"Do not touch vendor/.",` +
		`"Budget":{"WallTime":1800000000000,"Tokens":1000000,"OutputBytes":268435456,"JobTimeout":600000000000},` +
		`"Created":"2026-10-05T09:30:15.123456789Z","Config":"eyJ3b3Jrc3BhY2UiOiIvc3J2L3Byb2plY3QiLCJjb250ZXh0X3ZlcnNpb24iOjF9"}`
	initialState = `{"RunID":"run_0123456789abcdef0123456789abcdef","Step":1,"Attempt":1,"RequestRev":0,"Phase":"idle",` +
		`"Limits":{"MaxSteps":20,"MaxAttempts":3},"Decision":{"kind":"","payload":""},"EffectID":"","JobID":"","Started":false,"Reason":""}`
	fullState = `{"RunID":"run_0123456789abcdef0123456789abcdef","Step":9,"Attempt":2,"RequestRev":3,"Phase":"cancelling",` +
		`"Limits":{"MaxSteps":20,"MaxAttempts":3},"Decision":{"kind":"exec","payload":"go test ./..."},` +
		`"EffectID":"effect_150f1d740dec1a6883c2261d90b0981f51817ab5dfbc37fa76e7a0dd77aeca1a",` +
		`"JobID":"9d4c1f6e2b7a8053c1e4f7a2b9d0c3e6f8a1b4d7c0e3f6a9b2c5d8e1f4a7b0c3","Started":true,"Reason":"cancelled"}`
	fullFacts = `{"Request":{"Body":"eyJtb2RlbCI6ImNsYXVkZS1vcHVzLTUiLCJtYXhfdG9rZW5zIjoxNjAwMH0=","Model":"claude-opus-5",` +
		`"Protocol":"anthropic-messages/2023-06-01+server-side-fallback-2026-07-01","Adapter":"canti-anthropic/2",` +
		`"Window":{"FirstStep":7,"Handover":"Rerun the parser tests.","SourceStep":6,"Requested":true,"Failures":1},` +
		`"Context":{"Bytes":{"Total":21000,"Limit":262144,"System":4100,"Goal":40,"Constraints":30,"Environment":600,` +
		`"Session":300,"Window":90,"Handover":120,"StepHeader":25,"EmptySteps":7,"Progress":60,"HandoverRequired":520,"Repair":210,` +
		`"Steps":[{"Step":7,"Bytes":700},{"Step":8,"Bytes":14000}]},` +
		`"Session":[{"RunID":"run_prev","SessionSeq":1,"Status":"returned","IncludedGoal":true}],"SessionNavigation":"run_prev",` +
		`"Retrievals":[{"Step":8,"Operation":"search","Query":{"operation":"search",` +
		`"path":"/canti/runs/run_0123456789abcdef0123456789abcdef/steps","reason":"find the failing assertion",` +
		`"offset":128,"limit":4096,"query":"FAIL","cursor":"eyJiaW5kaW5nIjoiYSJ9","ignore_case":true},` +
		`"More":true,"Incomplete":true,"Error":"source changed"}],` +
		`"Sources":[{"Step":8,"Source":{"path":"/canti/artifacts/effect_150f1d740dec1a6883c2261d90b0981f51817ab5dfbc37fa76e7a0dd77aeca1a/output",` +
		`"offset":512,"length":64,"sha256":"5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"},"Available":true,"Error":"sha256 mismatch"}],` +
		`"Handover":{"Status":"evaluated","Used":14700,"Capacity":150000,"Growth":9000,"Steps":2,"WindowSteps":2,"WindowBytes":65536,` +
		`"Reasons":["window_steps","capacity"]}}},` +
		`"Response":{"Body":"eyJzdG9wX3JlYXNvbiI6ImVuZF90dXJuIn0=","Complete":true,"Model":"claude-opus-5-live",` +
		`"Protocol":"anthropic-messages/2023-06-01","Adapter":"canti-anthropic/3-tool","Usage":{"InputTokens":1200,"OutputTokens":34}},` +
		`"Observation":{"Execution":"exited","ExitCode":2,"Preview":"--- FAIL: TestParse\n",` +
		`"Output":{"Path":"effect_150f1d740dec1a6883c2261d90b0981f51817ab5dfbc37fa76e7a0dd77aeca1a/output","Bytes":9000,"Complete":true,` +
		`"Available":true,"Detail":"kept 9000 of 9100 bytes"},` +
		`"Retrieval":{"query":{"operation":"search","path":"/canti/runs/run_0123456789abcdef0123456789abcdef/steps",` +
		`"reason":"find the failing assertion","offset":128,"limit":4096,"query":"FAIL","cursor":"eyJiaW5kaW5nIjoiYSJ9","ignore_case":true},` +
		`"sources":[{"path":"/canti/artifacts/effect_150f1d740dec1a6883c2261d90b0981f51817ab5dfbc37fa76e7a0dd77aeca1a/output",` +
		`"offset":512,"length":64,"sha256":"5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"}],` +
		`"entries":["/canti/runs/run_prev/steps/00000001"],"next_offset":8192,"next_cursor":"eyJzdGVwIjoyfQ","more":true,` +
		`"incomplete":true,"skipped_count":3,"skipped":[{"step":1,"field":"artifact","reason":"utf8"}],` +
		`"error":"directory contains a non-UTF-8 path"},` +
		`"Handover":{"run_id":"run_0123456789abcdef0123456789abcdef","first_step":7,"text":"Rerun the parser tests."}},` +
		`"Failure":"anthropic: status 529 overloaded_error"}`
	bareFacts = `{"Request":{"Body":"e30=","Model":"claude-opus-5","Protocol":"anthropic-messages/2023-06-01","Adapter":"canti-anthropic/2"},` +
		`"Response":null,"Observation":{"Execution":"cancelled","ExitCode":null,"Preview":"",` +
		`"Output":{"Path":"","Bytes":0,"Complete":false,"Available":false,"Detail":"no job was bound to the effect"}},"Failure":""}`
	sparseFacts = `{"Request":{"Body":"e30=","Model":"claude-opus-5","Protocol":"anthropic-messages/2023-06-01","Adapter":"canti-anthropic/2",` +
		`"Window":{"FirstStep":1,"Handover":"","SourceStep":0,"Requested":false,"Failures":0},` +
		`"Context":{"Bytes":{"Total":4500,"Limit":262144,"System":0,"Goal":0,"Constraints":0,"Environment":0,"Session":0,"Window":0,` +
		`"Handover":0,"StepHeader":0,"EmptySteps":0,"Progress":0,"HandoverRequired":0,"Repair":0},` +
		`"Retrievals":[{"Step":1,"More":false,"Incomplete":false}],` +
		`"Sources":[{"Step":1,"Source":{"path":"","offset":0,"length":0,"sha256":""},"Available":false}],` +
		`"Handover":{"Status":"request_revision","Used":0,"Capacity":0,"Growth":0,"Steps":0,"WindowSteps":0,"WindowBytes":0}}},` +
		`"Response":null,"Observation":{"Execution":"exited","ExitCode":0,"Preview":"",` +
		`"Output":{"Path":"","Bytes":0,"Complete":false,"Available":false,"Detail":""},` +
		`"Retrieval":{"query":{"operation":"read","path":"/canti/runs","reason":"check"},"next_offset":0,"more":false}},"Failure":""}`
	emptyFacts = `{"Request":null,"Response":null,"Observation":null,"Failure":""}`
)

func persistedRecords() (harness.Run, core.State, []harness.Facts) {
	run := harness.Run{
		ID: persistedRun, SessionID: "session-1", SessionSeq: 1, InputID: "input-1",
		Goal: "Fix the failing test in ./parser.", Constraints: "Do not touch vendor/.",
		Budget:  harness.Budget{WallTime: 30 * time.Minute, Tokens: 1_000_000, OutputBytes: 256 << 20, JobTimeout: 10 * time.Minute},
		Created: time.Date(2026, 10, 5, 17, 30, 15, 123456789, time.FixedZone("UTC+8", 8*3600)),
		Config:  []byte(`{"workspace":"/srv/project","context_version":1}`),
	}
	state := core.State{
		RunID: persistedRun, Step: 9, Attempt: 2, RequestRev: 3, Phase: core.PhaseCancelling,
		Limits:   core.Limits{MaxSteps: 20, MaxAttempts: 3},
		Decision: core.Decision{Kind: core.Exec, Payload: "go test ./..."},
		EffectID: persistedEffect, JobID: persistedJob, Started: true, Reason: core.StopCancelled,
	}
	exit := 2
	query := harness.RetrievalQuery{Operation: "search", Path: "/canti/runs/" + persistedRun + "/steps", Reason: "find the failing assertion",
		Offset: 128, Limit: 4096, Query: "FAIL", Cursor: "eyJiaW5kaW5nIjoiYSJ9", IgnoreCase: true}
	source := harness.SourceRange{Path: "/canti/artifacts/" + persistedEffect + "/output", Offset: 512, Length: 64,
		SHA256: "5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"}
	full := harness.Facts{
		Request: &harness.Request{
			Body: []byte(`{"model":"claude-opus-5","max_tokens":16000}`), Model: "claude-opus-5",
			Protocol: "anthropic-messages/2023-06-01+server-side-fallback-2026-07-01", Adapter: "canti-anthropic/2",
			Window: &harness.Window{FirstStep: 7, Handover: "Rerun the parser tests.", SourceStep: 6, Requested: true, Failures: 1},
			Context: &harness.ContextCapture{
				Bytes: harness.ContextBytes{Total: 21000, Limit: 262144, System: 4100, Goal: 40, Constraints: 30, Environment: 600,
					Session: 300, Window: 90, Handover: 120, StepHeader: 25, EmptySteps: 7, Progress: 60, HandoverRequired: 520, Repair: 210,
					Steps: []harness.ContextStepBytes{{Step: 7, Bytes: 700}, {Step: 8, Bytes: 14000}}},
				Session:           []harness.ContextSessionSource{{RunID: "run_prev", SessionSeq: 1, Status: "returned", IncludedGoal: true}},
				SessionNavigation: "run_prev",
				Retrievals:        []harness.ContextRetrieval{{Step: 8, Operation: "search", Query: &query, More: true, Incomplete: true, Error: "source changed"}},
				Sources:           []harness.ContextSource{{Step: 8, Source: source, Available: true, Error: "sha256 mismatch"}},
				Handover: harness.HandoverCheck{Status: "evaluated", Used: 14700, Capacity: 150000, Growth: 9000,
					Steps: 2, WindowSteps: 2, WindowBytes: 65536, Reasons: []string{"window_steps", "capacity"}},
			},
		},
		Response: &harness.Response{Body: []byte(`{"stop_reason":"end_turn"}`), Complete: true, Model: "claude-opus-5-live",
			Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/3-tool", Usage: harness.Usage{InputTokens: 1200, OutputTokens: 34}},
		Observation: &harness.Observation{
			Execution: core.ExecutionExited, ExitCode: &exit, Preview: "--- FAIL: TestParse\n",
			Output: harness.ArtifactRef{Path: persistedEffect + "/output", Bytes: 9000, Complete: true, Available: true, Detail: "kept 9000 of 9100 bytes"},
			Retrieval: &harness.Retrieval{Query: query, Sources: []harness.SourceRange{source}, Entries: []string{"/canti/runs/run_prev/steps/00000001"},
				NextOffset: 8192, NextCursor: "eyJzdGVwIjoyfQ", More: true, Incomplete: true, SkippedCount: 3,
				Skipped: []harness.DiscoverySkip{{Step: 1, Field: "artifact", Reason: "utf8"}}, Error: "directory contains a non-UTF-8 path"},
			Handover: &harness.Handover{RunID: persistedRun, FirstStep: 7, Text: "Rerun the parser tests."},
		},
		Failure: "anthropic: status 529 overloaded_error",
	}
	bare := harness.Facts{
		Request:     &harness.Request{Body: []byte(`{}`), Model: "claude-opus-5", Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/2"},
		Observation: &harness.Observation{Execution: core.ExecutionCancelled, Output: harness.ArtifactRef{Detail: "no job was bound to the effect"}},
	}
	sparse := harness.Facts{
		Request: &harness.Request{Body: []byte(`{}`), Model: "claude-opus-5", Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/2",
			Window: &harness.Window{FirstStep: 1},
			Context: &harness.ContextCapture{Bytes: harness.ContextBytes{Total: 4500, Limit: 262144},
				Retrievals: []harness.ContextRetrieval{{Step: 1}}, Sources: []harness.ContextSource{{Step: 1}},
				Handover: harness.HandoverCheck{Status: "request_revision"}}},
		Observation: &harness.Observation{Execution: core.ExecutionExited, ExitCode: new(int),
			Retrieval: &harness.Retrieval{Query: harness.RetrievalQuery{Operation: "read", Path: "/canti/runs", Reason: "check"}}},
	}
	return run, state, []harness.Facts{full, bare, sparse}
}

func TestPersistedRecordsKeepTheirEncoding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "canti.db")
	s, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	run, state, facts := persistedRecords()
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	initial, err := core.NewState(run.ID, state.Limits)
	if err != nil {
		t.Fatal(err)
	}
	submitted := run
	submitted.SessionSeq = 0
	if _, err := s.Submit(ctx, submitted, initial); err != nil {
		t.Fatal(err)
	}
	events := []core.Event{
		core.RequestFrozen{}, core.AttemptStarted{Step: 9, Attempt: 2}, core.ResponseSaved{Step: 9, Attempt: 2},
		core.ResponseIncomplete{Step: 9, Attempt: 2},
		core.ResponseLoaded{Step: 9, Attempt: 2, Body: []byte(`{"kind":"exec","payload":"go test ./..."}`)},
		core.JobBound{EffectID: persistedEffect, JobID: persistedJob}, core.JobStarted{EffectID: persistedEffect, JobID: persistedJob},
		core.Observed{EffectID: persistedEffect, Execution: core.ExecutionExited}, core.OutcomeUnknown{EffectID: persistedEffect},
		core.StopRequested{Reason: core.StopBudgetExhausted}, core.Resolved{EffectID: persistedEffect, Execution: core.ExecutionFailedToStart},
	}
	goldens := []string{fullFacts, bareFacts, sparseFacts}
	var committed []harness.Transition
	from := initial
	for i, event := range events {
		tr := harness.Transition{Seq: int64(i + 1), EventID: "event_" + event.Kind(), From: from, Event: event, To: state}
		if i < len(facts) {
			tr.Facts = facts[i]
		}
		if err := s.Commit(ctx, tr); err != nil {
			t.Fatalf("commit %s: %v", event.Kind(), err)
		}
		committed = append(committed, tr)
		from = state
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var runColumn, initialColumn string
	if err := db.QueryRowContext(ctx, `SELECT run, initial FROM runs WHERE id = ?`, run.ID).Scan(&runColumn, &initialColumn); err != nil {
		t.Fatal(err)
	}
	same(t, "runs.run", runColumn, runRecord)
	same(t, "runs.initial", initialColumn, initialState)
	rows, err := db.QueryContext(ctx, `SELECT seq, kind, event, from_state, to_state, facts FROM transitions WHERE run_id = ? ORDER BY seq`, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for ; rows.Next(); n++ {
		var seq int64
		var kind, event, from, to, factsColumn string
		if err := rows.Scan(&seq, &kind, &event, &from, &to, &factsColumn); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(events[n])
		if err != nil {
			t.Fatal(err)
		}
		if seq != int64(n+1) || kind != events[n].Kind() || event != string(encoded) {
			t.Errorf("transition %d = seq %d kind %s event %s; want %s %s", n+1, seq, kind, event, events[n].Kind(), encoded)
		}
		wantFrom, wantFacts := fullState, emptyFacts
		if n == 0 {
			wantFrom = initialState
		}
		if n < len(goldens) {
			wantFacts = goldens[n]
		}
		same(t, kind+" from_state", from, wantFrom)
		same(t, kind+" to_state", to, fullState)
		same(t, kind+" facts", factsColumn, wantFacts)
	}
	if err := rows.Err(); err != nil || n != len(events) {
		t.Fatalf("read %d transitions, %v; want %d", n, err, len(events))
	}

	got, err := s.Run(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Created.Equal(run.Created) || got.Created.Location() != time.UTC {
		t.Errorf("created = %v, want %v in UTC", got.Created, run.Created)
	}
	got.Created = run.Created
	if !reflect.DeepEqual(got, run) {
		t.Errorf("run decodes as %+v, want %+v", got, run)
	}
	back, err := s.Transitions(ctx, run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, committed) {
		t.Errorf("transitions decode as\n%+v\nwant\n%+v", back, committed)
	}
}

func same(t *testing.T, column, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s encodes as\n%s\nwant\n%s", column, got, want)
	}
}
