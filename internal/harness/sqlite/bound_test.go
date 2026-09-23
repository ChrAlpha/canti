package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// shortcut returns the next event and facts on the shortest path through an
// exec step whose job fails to start: five transitions per observed step.
func shortcut(s core.State) (core.Event, harness.Facts) {
	body := execBody(fmt.Sprintf("step %d", s.Step))
	switch s.Phase {
	case core.PhaseIdle:
		return core.RequestFrozen{}, harness.Facts{Request: requestFor(s)}
	case core.PhaseRequestFrozen:
		return core.AttemptStarted{Step: s.Step, Attempt: s.Attempt}, harness.Facts{}
	case core.PhaseRequestSent:
		return core.ResponseSaved{Step: s.Step, Attempt: s.Attempt}, harness.Facts{Response: responseFor(s, body, harness.Usage{InputTokens: 2, OutputTokens: 1})}
	case core.PhaseResponseSaved:
		return core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: body}, harness.Facts{}
	case core.PhaseEffectSaved:
		return core.Observed{EffectID: s.EffectID, Execution: core.ExecutionFailedToStart}, harness.Facts{Observation: observation(core.ExecutionFailedToStart, 3)}
	}
	return nil, harness.Facts{}
}

// sums recomputes the totals of transitions independently of the store.
func sums(transitions []harness.Transition) harness.Totals {
	var t harness.Totals
	for _, tr := range transitions {
		switch tr.Event.(type) {
		case core.ResponseSaved, core.ResponseIncomplete:
			t.Usage.InputTokens += tr.Facts.Response.Usage.InputTokens
			t.Usage.OutputTokens += tr.Facts.Response.Usage.OutputTokens
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
			if step := next - len(page) + 1 + i; record.Step != step || record.Seq != 5*int64(step) || record.Command != fmt.Sprintf("step %d", step) {
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
		"Latest":      {latestSQL, "sqlite_autoindex_transitions_1", []any{"long"}},
		"Transitions": {transitionsSQL, "sqlite_autoindex_transitions_1", []any{"long", 100, 10}},
		"Request":     {requestSQL, "transitions_request", []any{"long", 7, 1}},
		"Response":    {responseSQL, "transitions_response", []any{"long", 7, 1}},
		"Recent":      {recentSQL, "transitions_steps (run_id=? AND seq<?)", []any{"long", int64(math.MaxInt64), 5}},
		"Recent page": {recentSQL, "transitions_steps (run_id=? AND seq<?)", []any{"long", 5000, 5}},
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
