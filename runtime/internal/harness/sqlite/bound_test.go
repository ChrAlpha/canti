package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// shortcut returns the next event and facts on the shortest path through an
// exec step, an unknown harness action that the harness performs as a
// failure: five transitions per completed step.
func shortcut(s core.State) (core.Event, harness.Facts) {
	body := execBody(fmt.Sprintf("canti:step %d", s.Step))
	switch s.Phase {
	case core.PhaseIdle:
		return core.RequestFrozen{}, harness.Facts{Request: requestFor(s)}
	case core.PhaseRequestFrozen:
		return core.AttemptStarted{Step: s.Step, Attempt: s.Attempt}, harness.Facts{}
	case core.PhaseRequestSent:
		return core.ResponseSaved{Step: s.Step, Attempt: s.Attempt}, harness.Facts{Response: responseFor(s, body, harness.Usage{InputTokens: 2, OutputTokens: 1})}
	case core.PhaseResponseSaved:
		event := core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: body}
		return event, acceptance(s, event)
	case core.PhaseEffectSaved:
		a := harness.NormalizeAction(s.Decision.Payload, true)
		return core.Performed{EffectID: s.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeFailed, Detail: a.Problem()}}
	}
	return nil, harness.Facts{}
}

// sums recomputes the totals of transitions independently of the store.
func sums(transitions []harness.Transition) harness.Totals {
	var t harness.Totals
	for _, tr := range transitions {
		switch tr.Event.(type) {
		case core.ResponseSaved, core.ResponseIncomplete, core.RequestRejected:
			t.Coverage.Attempts++
			if tr.Facts.Response == nil {
				t.Coverage.Lost++
				continue
			}
			if tr.Facts.Response.Usage.InputReported {
				t.Coverage.InputReports++
			}
			if tr.Facts.Response.Usage.OutputReported {
				t.Coverage.OutputReports++
			}
			t.Usage.InputTokens += tr.Facts.Response.Usage.InputTokens
			t.Usage.OutputTokens += tr.Facts.Response.Usage.OutputTokens
		case core.Performed:
			t.Steps++
		case core.Observed, core.Resolved:
			t.Steps++
			t.OutputBytes += tr.Facts.Observation.Output.Bytes
		}
	}
	return t
}

// seed commits a run of steps observed steps through Runner.Commit and the
// store's insert path inside one transaction.
func seed(t testing.TB, s *Store, runID string, steps int) {
	t.Helper()
	snap := createRun(t, s, runID, core.Limits{MaxSteps: steps, MaxAttempts: 1})
	err := s.write(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		d := newDriver(t, storeFunc(func(ctx context.Context, tr harness.Transition) error { return s.commit(ctx, tx, tr) }), snap)
		for d.snap.State.Intent() != core.IntentHalt {
			d.do(shortcut(d.snap.State))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed %s error = %v", runID, err)
	}
}

func plan(t testing.TB, s *Store, query string, args ...any) string {
	t.Helper()
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, "; ")
}

func TestReadsStayBoundedByTheirLimit(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	seed(t, s, "short", 200)
	seed(t, s, "long", 2000)

	all, err := s.Transitions(ctx, "long", 0, 20000)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := totalsOf(t, s, "long"), sums(all); got != want || got.Steps != 2000 {
		t.Fatalf("Totals() = %#v, want %#v with 2000 steps", got, want)
	}
	next, pages := 2000, 0
	for before := int64(0); next > 0; pages++ {
		page, err := s.Recent(ctx, "long", before, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != min(64, next) {
			t.Fatalf("Recent(%d, 64) returned %d records, want %d", before, len(page), min(64, next))
		}
		for i, record := range page {
			if step := next - len(page) + 1 + i; record.Step != step || record.Seq != 5*int64(step) || record.Command != fmt.Sprintf("canti:step %d", step) || record.Outcome == nil {
				t.Fatalf("Recent(%d, 64)[%d] = %#v, want step %d at seq %d", before, i, record, step, 5*step)
			}
		}
		next, before = next-len(page), page[0].Seq
	}
	if pages != 32 {
		t.Fatalf("paged 2000 steps in %d pages of 64, want 32", pages)
	}

	queries := map[string]struct {
		query, index string
		args         []any
	}{
		"Latest":                {latestSQL, "sqlite_autoindex_transitions_1", []any{"long"}},
		"Transitions":           {transitionsSQL, "sqlite_autoindex_transitions_1", []any{"long", 100, 10}},
		"Progress":              {progressSQL, "sqlite_autoindex_transitions_1", []any{"long", 100, 10}},
		"Completion":            {completionSQL, "sqlite_autoindex_transitions_1", []any{"long", 100}},
		"Request":               {requestSQL, "transitions_request", []any{"long", 7, 1}},
		"Response":              {responseSQL, "transitions_response", []any{"long", 7, 1}},
		"Recent before actions": {recentSQL, "transitions_steps (run_id=? AND seq<?)", []any{"long", int64(math.MaxInt64), 5}},
		"Recent page":           {recentSQL, "transitions_steps (run_id=? AND seq<?)", []any{"long", 5000, 5}},
	}
	// stepsSQL searches each partial index for at most limit rows and merges
	// those, so its sort is bounded by twice the limit, not by the history.
	for _, before := range []int64{math.MaxInt64, 5000} {
		detail := plan(t, s, stepsSQL, "long", before, 5)
		for _, index := range []string{"transitions_steps (run_id=? AND seq<?)", "transitions_performed (run_id=? AND seq<?)"} {
			if !strings.Contains(detail, "SEARCH transitions USING INDEX "+index) {
				t.Errorf("Recent plan %q does not search %s", detail, index)
			}
		}
		if strings.Contains(detail, "SCAN transitions") {
			t.Errorf("Recent plan %q scans the transitions", detail)
		}
	}
	for name, q := range queries {
		detail := plan(t, s, q.query, q.args...)
		if !strings.HasPrefix(detail, "SEARCH transitions USING ") || !strings.Contains(detail, q.index) || strings.Contains(detail, "TEMP B-TREE") {
			t.Errorf("%s plan = %q, want an index search on %s without a temporary sort", name, detail, q.index)
		}
	}

	allocs := func(runID string, before int64) float64 {
		return testing.AllocsPerRun(50, func() {
			if _, err := s.Recent(ctx, runID, before, 5); err != nil {
				t.Fatal(err)
			}
		})
	}
	short, long, deep := allocs("short", 0), allocs("long", 0), allocs("long", 5000)
	if math.Abs(long-short) > 2 || math.Abs(deep-long) > 2 {
		t.Fatalf("Recent allocations depend on history: %.0f at 200 steps, %.0f at 2000, %.0f 1000 steps back", short, long, deep)
	}
	t.Logf("Recent(5) allocations: %.0f at 200 steps, %.0f at 2000 steps, %.0f 1000 steps back", short, long, deep)
}

func benchmarkStore(b *testing.B) *Store {
	b.Helper()
	s := open(b, tempPath(b))
	seed(b, s, "long", 2000)
	b.ReportAllocs()
	return s
}

func BenchmarkLatest(b *testing.B) {
	s := benchmarkStore(b)
	for b.Loop() {
		if _, err := s.Latest(context.Background(), "long"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecent(b *testing.B) {
	s := benchmarkStore(b)
	for b.Loop() {
		if _, err := s.Recent(context.Background(), "long", 0, 5); err != nil {
			b.Fatal(err)
		}
	}
}

// TestProgressSelectsNoBodyAndClipsEveryText requires that the progress read
// selects every text it returns through its cap and never names a request,
// a response, or retrieved evidence, so that what it returns is bounded
// whatever a row holds.
func TestProgressSelectsNoBodyAndClipsEveryText(t *testing.T) {
	rest := regexp.MustCompile(`substr\(CAST\(json_extract\((from_state|to_state|facts), '[^']*'\) AS BLOB\), 1, \d+\)`).ReplaceAllString(progressSQL, "")
	for _, m := range regexp.MustCompile(`json_extract\([^)]*\)`).FindAllString(rest, -1) {
		switch m {
		case `json_extract(from_state, '$.Step')`, `json_extract(facts, '$.Observation.ExitCode')`, `json_extract(facts, '$.ContextState.AnchorSeq')`:
		default:
			t.Errorf("progress selects %s without a cap", m)
		}
	}
	for _, body := range []string{"Request", "Response", "vidence", "Body"} {
		if strings.Contains(progressSQL, body) {
			t.Errorf("progress names %s", body)
		}
	}
	if ProgressRowBytes > 32<<10 {
		t.Errorf("a progress row may take %d bytes", ProgressRowBytes)
	}
}

func TestReceiptLookupUsesBoundedCompletionIndexes(t *testing.T) {
	s := open(t, tempPath(t))
	detail := plan(t, s, receiptSQL, "run", int64(math.MaxInt64), 5)
	t.Logf("receipt plan: %s", detail)
	for _, index := range []string{"transitions_steps (run_id=? AND seq<?)", "transitions_performed (run_id=? AND seq<?)"} {
		if !strings.Contains(detail, "INDEX "+index) {
			t.Errorf("receipt plan does not bound selected identities through %s: %s", index, detail)
		}
	}
	if !strings.Contains(detail, "SEARCH t USING INDEX sqlite_autoindex_transitions_1 (run_id=? AND seq=?)") {
		t.Errorf("receipt hydration does not use one primary-key read per selected identity: %s", detail)
	}
}
