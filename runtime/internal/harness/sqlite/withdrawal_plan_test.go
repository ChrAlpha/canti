package sqlite

import (
	"fmt"
	"strings"
	"testing"
)

func TestWithdrawalQueryPlanUsesIndexOrderAtTwoSizes(t *testing.T) {
	for _, size := range []int{100, 10000} {
		s := withdrawalPlanStore(t, size)
		plan := planOf(t, s, logWithdrawalsSQL, int64(0), 10, withdrawalNames+1)
		if strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, "inputs_withdrawn") {
			t.Fatalf("%d inputs: withdrawal plan groups or sorts before the cap:\n%s", size, plan)
		}
	}
}

func withdrawalPlanStore(t *testing.T, inputs int) *Store {
	t.Helper()
	s := open(t, tempPath(t))
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO runs (n, id, semantics, run, initial, project, project_meta, input_pos, fresh, line) VALUES (1, 'run-1', 8, '{}', '{}', '{}', '{}', 1, 0, 1)`); err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO inputs (pos, authority, disposition, withdrawn_pos, text, admitted_at)
		VALUES (?, 'principal', 'withdrawn', ?, ?, '2026-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 1; i <= inputs; i++ {
		if _, err := stmt.Exec(i, (i+1)/2*2, fmt.Sprintf("input-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s
}
