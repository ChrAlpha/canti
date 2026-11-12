package sqlite

import (
	"context"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func lastPosPlan(t *testing.T, s *Store) string {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+lastPosSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		b.WriteString(detail)
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func storedCounter(t *testing.T, s *Store) int64 {
	t.Helper()
	var pos int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT pos FROM agent_log WHERE n = 1`).Scan(&pos); err != nil {
		t.Fatal(err)
	}
	return pos
}

func TestLastPosReadsTheAgentLogCounter(t *testing.T) {
	for _, history := range []int{64, 4096} {
		t.Run("history-"+itoa(history), func(t *testing.T) {
			path := tempPath(t)
			s := open(t, path)
			run := admitOpen(t, s, "run-1", "Do the task.", true)
			seedObservations(t, s, history, false)
			ctx := context.Background()
			got, err := s.LastPos(ctx)
			if err != nil || got != storedCounter(t, s) {
				t.Fatalf("LastPos = %d, %v; counter = %d", got, err, storedCounter(t, s))
			}
			if got != int64(history+1) {
				t.Fatalf("LastPos after %d observations = %d", history, got)
			}
			plan := lastPosPlan(t, s)
			if strings.Contains(plan, "SCAN") || !strings.Contains(plan, "agent_log") {
				t.Fatalf("LastPos plan is not a bounded counter read:\n%s", plan)
			}
			// A transition and a withdrawal both advance the counter.
			d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
			d.do(core.RequestFrozen{}, harness.Facts{Request: requestFor(d.snap.State)})
			if got, err := s.LastPos(ctx); err != nil || got != storedCounter(t, s) {
				t.Fatalf("LastPos after transition = %d, %v; counter = %d", got, err, storedCounter(t, s))
			}
			queued := admitKind(t, s, harness.DeliveryFollowUp, "later")
			if _, err := s.Withdraw(ctx, ""); err != nil {
				t.Fatal(err)
			}
			afterWithdraw, err := s.LastPos(ctx)
			if err != nil || afterWithdraw != storedCounter(t, s) || afterWithdraw <= int64(history+2) {
				t.Fatalf("LastPos after withdrawal = %d, %v; counter = %d", afterWithdraw, err, storedCounter(t, s))
			}
			_ = queued
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = open(t, path)
			defer s.Close()
			if got, err := s.LastPos(ctx); err != nil || got != afterWithdraw {
				t.Fatalf("LastPos after reopen = %d, %v; want %d", got, err, afterWithdraw)
			}
		})
	}
}
