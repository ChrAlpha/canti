package sqlite

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestHistoryTransitionsPlanUsesIndexOrderAtTwoSizes(t *testing.T) {
	for _, size := range []int{100, 10000} {
		s := transitionPlanStore(t, size)
		plan := planOf(t, s, historyTransitionsRunSQL, "run-1", int64(size+1), 64)
		if strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, "sqlite_autoindex_transitions_1") {
			t.Fatalf("%d transitions: history plan is not index-ordered:\n%s", size, plan)
		}
	}
}

func transitionPlanStore(t *testing.T, transitions int) *Store {
	t.Helper()
	s := open(t, tempPath(t))
	if _, err := s.db.Exec(`INSERT INTO runs (n, id, semantics, run, initial, project, project_meta, input_pos, fresh, line) VALUES (1, 'run-1', ?, '{}', '{}', '{}', '{}', 1, 0, 1)`, core.SemanticsVersion); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO transitions (run_id, seq, pos, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)
		VALUES ('run-1', ?, ?, ?, 'attempt_started', ?, '{}', '{}', '{}', '{}', 1, 1, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 1; i <= transitions; i++ {
		if _, err := stmt.Exec(i, i, fmt.Sprintf("event-%d", i), core.SemanticsVersion); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestHistoryLineQueriesUseIndexSeeks(t *testing.T) {
	s := historyPlanStore(t, 100)
	for _, tc := range []struct {
		name, query string
		args        []any
		want        string
	}{
		{"latest line", latestLineSQL, nil, "runs"},
		{"line runs", historyRunIDsSQL, []any{1, 64}, "runs_line"},
		{"line inputs", historyInputsLineSQL, []any{1, int64(100), 64}, "inputs_line"},
		{"queued inputs", historyQueuedLineSQL, []any{int64(100), 64}, "inputs_queued_line"},
		{"withdrawals", historyWithdrawalsSQL, []any{1, int64(100), 64, 64}, "inputs_line_withdrawn"},
		{"state at end", historyStateSQL, []any{"run-1"}, "sqlite_autoindex_transitions_1"},
	} {
		plan := planOf(t, s, tc.query, tc.args...)
		if strings.Contains(plan, "SCAN inputs") || strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, tc.want) {
			t.Fatalf("%s plan is not an index seek:\n%s", tc.name, plan)
		}
	}
}
