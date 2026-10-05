package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
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
	persistedLater  = "effect_3429cd52138cd2cdeafbbac1c72a000e75b62aa8b1a03ff61472b9869a8e7a46"
	persistedJob    = "9d4c1f6e2b7a8053c1e4f7a2b9d0c3e6f8a1b4d7c0e3f6a9b2c5d8e1f4a7b0c3"
	plainRun        = "run_fedcba9876543210fedcba9876543210"
	plainEffect     = "effect_77d03f6fc12313fbd499e665dd6bb0204147df9de19b77d3c09cb6e343d89a3a"
	plainJob        = "read_" + plainEffect
)

// persistedRuns returns a session run, which Submit records, and a run
// outside any session, which CreateRun records as canti run does.
func persistedRuns() (session, plain harness.Run) {
	session = harness.Run{
		ID: persistedRun, SessionID: "session-1", InputID: "input-1",
		Goal: "Fix the failing test in ./parser.", Constraints: "Do not touch vendor/.",
		Budget:  harness.Budget{WallTime: 30 * time.Minute, Tokens: 1_000_000, OutputBytes: 256 << 20, JobTimeout: 10 * time.Minute},
		Created: time.Date(2026, 10, 5, 17, 30, 15, 123456789, time.FixedZone("UTC+8", 8*3600)),
		Config:  []byte(`{"workspace":"/srv/project","context_version":1}`),
	}
	plain = harness.Run{
		ID: plainRun, Goal: "List the workspace.",
		Budget:  harness.Budget{WallTime: 30 * time.Minute, OutputBytes: 256 << 20, JobTimeout: 10 * time.Minute},
		Created: time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC),
		Config: []byte(`{"workspace":"/srv/project","image":"debian:bookworm-slim","network":"none","memory":"2g","cpus":"","pids":512,` +
			`"output_limit":8388608,"model":"claude-opus-5","max_tokens":16000,"structured":true,"fallbacks":true,` +
			`"context_bytes":262144,"context_version":1,"handover_bytes":2048}`),
	}
	return session, plain
}

type commit struct {
	event core.Event
	facts harness.Facts
}

// persistedChains returns legal chains of the two runs that commit every
// event kind with the facts it permits: the session run retries a lost
// response, observes step 1, and resolves an unknown step 2 as stopped; the
// plain run observes a retrieval and exhausts its single step.
func persistedChains() (session, plain []commit) {
	exit, zero := 2, 0
	query := harness.RetrievalQuery{Operation: "search", Path: "/canti/runs/" + persistedRun + "/steps", Reason: "find the failing assertion",
		Offset: 128, Limit: 4096, Query: "FAIL", Cursor: "eyJiaW5kaW5nIjoiYSJ9", IgnoreCase: true}
	source := harness.SourceRange{Path: "/canti/artifacts/" + persistedEffect + "/output", Offset: 512, Length: 64,
		SHA256: "5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"}
	full := &harness.Request{
		Body: []byte(`{"model":"claude-opus-5","max_tokens":16000}`), Model: "claude-opus-5",
		Protocol: "anthropic-messages/2023-06-01+server-side-fallback-2026-07-01", Adapter: "canti-anthropic/2",
		Window: &harness.Window{FirstStep: 2, Handover: "Rerun the parser tests.", SourceStep: 1, Requested: true, Failures: 1},
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
	}
	observed := &harness.Observation{
		Execution: core.ExecutionExited, ExitCode: &exit, Preview: "--- FAIL: TestParse\n",
		Output: harness.ArtifactRef{Path: persistedEffect + "/output", Bytes: 9000, Complete: true, Available: true, Detail: "kept 9000 of 9100 bytes"},
		Retrieval: &harness.Retrieval{Query: query, Sources: []harness.SourceRange{source}, Entries: []string{"/canti/runs/run_prev/steps/00000001"},
			NextOffset: 8192, NextCursor: "eyJzdGVwIjoyfQ", More: true, Incomplete: true, SkippedCount: 3,
			Skipped: []harness.DiscoverySkip{{Step: 1, Field: "artifact", Reason: "utf8"}}, Error: "directory contains a non-UTF-8 path"},
		Handover: &harness.Handover{RunID: persistedRun, FirstStep: 7, Text: "Rerun the parser tests."},
	}
	session = []commit{
		{core.RequestFrozen{}, harness.Facts{Request: &harness.Request{Body: []byte(`{}`), Model: "claude-opus-5",
			Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/2"}}},
		{core.AttemptStarted{Step: 1, Attempt: 1}, harness.Facts{}},
		{core.ResponseIncomplete{Step: 1, Attempt: 1}, harness.Facts{Failure: "anthropic: status 529 overloaded_error",
			Response: &harness.Response{Body: []byte(`{"type":"message"`), Model: "claude-opus-5",
				Protocol: "anthropic-messages/2023-06-01+server-side-fallback-2026-07-01", Adapter: "canti-anthropic/2", Usage: harness.Usage{InputTokens: 1200}}}},
		{core.AttemptStarted{Step: 1, Attempt: 2}, harness.Facts{}},
		{core.ResponseSaved{Step: 1, Attempt: 2}, harness.Facts{Response: &harness.Response{Body: []byte(`{"stop_reason":"end_turn"}`), Complete: true,
			Model: "claude-opus-5-live", Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/3-tool",
			Usage: harness.Usage{InputTokens: 1200, OutputTokens: 34}}}},
		{core.ResponseLoaded{Step: 1, Attempt: 2, Body: []byte(`{"kind":"exec","payload":"go test ./..."}`)}, harness.Facts{}},
		{core.JobBound{EffectID: persistedEffect, JobID: persistedJob}, harness.Facts{}},
		{core.JobStarted{EffectID: persistedEffect, JobID: persistedJob}, harness.Facts{}},
		{core.Observed{EffectID: persistedEffect, Execution: core.ExecutionExited}, harness.Facts{Observation: observed}},
		{core.RequestFrozen{}, harness.Facts{Request: full}},
		{core.AttemptStarted{Step: 2, Attempt: 1}, harness.Facts{}},
		{core.ResponseSaved{Step: 2, Attempt: 1}, harness.Facts{Response: &harness.Response{Body: []byte(`{"stop_reason":"end_turn"}`), Complete: true,
			Model: "claude-opus-5", Protocol: "anthropic-messages/2023-06-01+server-side-fallback-2026-07-01", Adapter: "canti-anthropic/2",
			Usage: harness.Usage{InputTokens: 2100, OutputTokens: 20}}}},
		{core.ResponseLoaded{Step: 2, Attempt: 1, Body: []byte(`{"kind":"exec","payload":"go vet ./..."}`)}, harness.Facts{}},
		{core.OutcomeUnknown{EffectID: persistedLater}, harness.Facts{Failure: `executor returned no job ID with status "unknown"`}},
		{core.StopRequested{Reason: core.StopBudgetExhausted}, harness.Facts{Failure: "1000000 tokens reached the 1000000-token budget"}},
		{core.Resolved{EffectID: persistedLater, Execution: core.ExecutionFailedToStart},
			harness.Facts{Observation: &harness.Observation{Execution: core.ExecutionFailedToStart, Output: harness.ArtifactRef{Detail: "resolved by operator"}}}},
	}
	plain = []commit{
		{core.RequestFrozen{}, harness.Facts{Request: &harness.Request{Body: []byte(`{}`), Model: "claude-opus-5",
			Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/2", Window: &harness.Window{FirstStep: 1},
			Context: &harness.ContextCapture{Bytes: harness.ContextBytes{Total: 4500, Limit: 262144},
				Retrievals: []harness.ContextRetrieval{{Step: 1}}, Sources: []harness.ContextSource{{Step: 1}},
				Handover: harness.HandoverCheck{Status: "request_revision"}}}}},
		{core.AttemptStarted{Step: 1, Attempt: 1}, harness.Facts{}},
		{core.ResponseSaved{Step: 1, Attempt: 1}, harness.Facts{Response: &harness.Response{Body: []byte(`{}`), Complete: true,
			Model: "claude-opus-5", Protocol: "anthropic-messages/2023-06-01", Adapter: "canti-anthropic/2"}}},
		{core.ResponseLoaded{Step: 1, Attempt: 1, Body: []byte(`{"kind":"exec","payload":"canti:retrieve {\"operation\":\"read\",\"path\":\"/canti/runs\",\"reason\":\"check\"}"}`)},
			harness.Facts{}},
		{core.JobBound{EffectID: plainEffect, JobID: plainJob}, harness.Facts{}},
		{core.JobStarted{EffectID: plainEffect, JobID: plainJob}, harness.Facts{}},
		{core.Observed{EffectID: plainEffect, Execution: core.ExecutionExited}, harness.Facts{Observation: &harness.Observation{
			Execution: core.ExecutionExited, ExitCode: &zero,
			Retrieval: &harness.Retrieval{Query: harness.RetrievalQuery{Operation: "read", Path: "/canti/runs", Reason: "check"}}}}},
	}
	return session, plain
}

// recorder keeps every transition the store acknowledged.
type recorder struct {
	*sqlite.Store
	commits []harness.Transition
}

func (r *recorder) Commit(ctx context.Context, t harness.Transition) error {
	err := r.Store.Commit(ctx, t)
	if err == nil {
		r.commits = append(r.commits, t)
	}
	return err
}

type noExecutor struct{}

var errNoExecutor = errors.New("this test commits events without an executor")

func (noExecutor) Bind(context.Context, harness.Effect) (harness.Job, error) {
	return harness.Job{}, errNoExecutor
}
func (noExecutor) Start(context.Context, string) (harness.Job, error) {
	return harness.Job{}, errNoExecutor
}
func (noExecutor) Inspect(context.Context, string) (harness.Job, bool, error) {
	return harness.Job{}, false, errNoExecutor
}
func (noExecutor) Cancel(context.Context, string) error { return errNoExecutor }

func TestPersistedRecordsKeepTheirEncoding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "canti.db")
	s, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	session, plain := persistedRuns()
	sessionInitial, err := core.NewState(session.ID, core.Limits{MaxSteps: 20, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	plainInitial, err := core.NewState(plain.ID, core.Limits{MaxSteps: 1, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if session, err = s.Submit(ctx, session, sessionInitial); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(ctx, plain, plainInitial); err != nil {
		t.Fatal(err)
	}
	r := &recorder{Store: s}
	runner, err := harness.NewRunner(r, noExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	sessionChain, plainChain := persistedChains()
	for _, run := range []struct {
		initial core.State
		chain   []commit
	}{{sessionInitial, sessionChain}, {plainInitial, plainChain}} {
		snap := harness.Snapshot{State: run.initial}
		for _, c := range run.chain {
			if snap, err = runner.Commit(ctx, snap, c.event, c.facts); err != nil {
				t.Fatalf("commit %s at seq %d of %s: %v", c.event.Kind(), snap.Seq+1, run.initial.RunID, err)
			}
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkRows(t, db, "runs", `SELECT n, id, semantics, run, initial, input_tokens, output_tokens, output_bytes, steps,
		typeof(n)||' '||typeof(id)||' '||typeof(semantics)||' '||typeof(run)||' '||typeof(initial)||' '||typeof(input_tokens)||' '||
		typeof(output_tokens)||' '||typeof(output_bytes)||' '||typeof(steps) FROM runs ORDER BY n`, persistedRunRows())
	checkRows(t, db, "session_runs", `SELECT session_id, seq, input_id, run_id,
		typeof(session_id)||' '||typeof(seq)||' '||typeof(input_id)||' '||typeof(run_id) FROM session_runs ORDER BY session_id, seq`, persistedSessionRows())
	checkRows(t, db, "transitions", `SELECT t.run_id, t.seq, t.event_id, t.kind, t.semantics, t.event, t.from_state, t.to_state, t.facts,
		t.step, t.attempt, t.rev, typeof(t.run_id)||' '||typeof(t.seq)||' '||typeof(t.event_id)||' '||typeof(t.kind)||' '||
		typeof(t.semantics)||' '||typeof(t.event)||' '||typeof(t.from_state)||' '||typeof(t.to_state)||' '||typeof(t.facts)||' '||
		typeof(t.step)||' '||typeof(t.attempt)||' '||typeof(t.rev) FROM transitions t JOIN runs r ON r.id = t.run_id ORDER BY r.n, t.seq`,
		persistedTransitionRows())

	committed := map[string][]harness.Transition{}
	for _, tr := range r.commits {
		committed[tr.From.RunID] = append(committed[tr.From.RunID], tr)
	}
	for _, want := range []harness.Run{session, plain} {
		got, err := s.Run(ctx, want.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Created.Equal(want.Created) || got.Created.Location() != time.UTC {
			t.Errorf("run %s created = %v, want %v in UTC", want.ID, got.Created, want.Created)
		}
		got.Created = want.Created
		if !reflect.DeepEqual(got, want) {
			t.Errorf("run decodes as %+v, want %+v", got, want)
		}
		back, err := s.Transitions(ctx, want.ID, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(back, committed[want.ID]) {
			t.Errorf("transitions of %s decode as\n%+v\nwant\n%+v", want.ID, back, committed[want.ID])
		}
	}
}

// checkRows compares every row the query selects, each column scanned as
// text or an integer and the last one listing their storage classes, to want.
func checkRows(t *testing.T, db *sql.DB, table, query string, want [][]any) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var got [][]any
	for rows.Next() {
		row := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Errorf("%s holds %d rows, want %d", table, len(got), len(want))
	}
	for i := range min(len(got), len(want)) {
		for j := range columns {
			if j >= len(want[i]) || !reflect.DeepEqual(got[i][j], want[i][j]) {
				t.Errorf("%s row %d column %s = %#v\nwant %#v", table, i+1, columns[j], got[i][j], index(want[i], j))
			}
		}
	}
}

func index(row []any, j int) any {
	if j < len(row) {
		return row[j]
	}
	return nil
}

// row lists the columns of an expected row, with integers as SQLite reads
// them back.
func row(columns ...any) []any {
	for i, c := range columns {
		if n, ok := c.(int); ok {
			columns[i] = int64(n)
		}
	}
	return columns
}
