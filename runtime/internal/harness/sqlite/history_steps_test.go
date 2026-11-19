package sqlite

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// TestHistoryQueriesExamineConstantRows measures the SQLite VM steps of the
// history queries at 100 and 10,000 excluded predecessor rows. Each query
// must stop at its LIMIT through an index, not walk the sparse range.
func TestHistoryQueriesExamineConstantRows(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("python3 is required for SQLite step measurement: %v", err)
	}
	for _, tc := range []struct {
		name  string
		query string
		args  []any
	}{
		{"transitions", historyTransitionsRunSQL, []any{"run-2", int64(0), 64}},
		{"old before transitions", historyTransitionsSeqSQL, []any{"run-1", int64(-1), 64}},
		{"inputs", historyInputsLineSQL, []any{1, int64(0), 64}},
		{"queued inputs", historyQueuedLineSQL, []any{int64(0), 64}},
		{"withdrawals", historyWithdrawalsSQL, []any{1, int64(0), 65, 65}},
		{"live withdrawals", logWithdrawalsSQL, []any{int64(0), 65, 65}},
		{"latest line", latestLineSQL, nil},
		{"run line", runLineSQL, []any{"run-1"}},
		{"line runs", historyRunIDsSQL, []any{1, 64}},
		{"state at end", historyStateSQL, []any{"run-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			small := measureHistorySteps(t, python, 100, tc.query, tc.args)
			large := measureHistorySteps(t, python, 10000, tc.query, tc.args)
			if large > small+1000 {
				t.Fatalf("%d rows took %d steps and 10,000 took %d; the query walks the excluded range", 100, small, large)
			}
		})
	}
}

func measureHistorySteps(t *testing.T, python string, size int, query string, template []any) int {
	t.Helper()
	path, end := historyStepsStore(t, size)
	args := make([]any, len(template))
	copy(args, template)
	for i, arg := range args {
		switch v := arg.(type) {
		case int64:
			switch v {
			case 0:
				args[i] = end
			case -1:
				args[i] = int64(size)
			}
		case string:
			if v == "run-2" {
				args[i] = "run-2"
			}
		}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	script := `import json, sqlite3, sys
con = sqlite3.connect(sys.argv[1])
query, args = sys.argv[2], json.loads(sys.argv[3])
steps = 0
def progress():
    global steps
    steps += 1
    return 0
con.set_progress_handler(progress, 1)
con.execute(query, args).fetchall()
print(steps)
`
	out, err := exec.Command(python, "-c", script, path, query, string(encoded)).Output()
	if err != nil {
		t.Fatalf("measure %q: %v", query, err)
	}
	steps, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("steps %q: %v", out, err)
	}
	return steps
}

// historyStepsStore builds a line with run-1 holding size predecessor rows
// and run-2 as the current line. The history queries target run-2, so every
// run-1 row is excluded.
func historyStepsStore(t *testing.T, size int) (string, int64) {
	t.Helper()
	path := tempPath(t)
	s := open(t, path)
	for i := 1; i <= 2; i++ {
		if _, err := s.db.Exec(`INSERT INTO runs (n, id, semantics, run, initial, project, project_meta, input_pos, fresh, line) VALUES (?, ?, 8, '{}', '{}', '{}', '{}', ?, ?, ?)`,
			i, fmt.Sprintf("run-%d", i), i, i == 2, i); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	transition, err := tx.Prepare(`INSERT INTO transitions (run_id, seq, pos, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)
		VALUES (?, ?, ?, ?, 'attempt_started', 8, '{}', '{}', '{}', '{}', 1, 1, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	defer transition.Close()
	for i := 1; i <= size; i++ {
		if _, err := transition.Exec("run-1", i, i, fmt.Sprintf("e1-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := transition.Exec("run-2", 1, size+1, "e2-1"); err != nil {
		t.Fatal(err)
	}
	input, err := tx.Prepare(`INSERT INTO inputs (pos, authority, disposition, withdrawn_pos, run_id, text, admitted_at)
		VALUES (?, 'principal', ?, ?, ?, ?, '2026-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	for i := 1; i <= size; i++ {
		if _, err := input.Exec(i, "steer", i+100000, "run-1", fmt.Sprintf("input-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := input.Exec(size+1, "opened", nil, "run-2", "line input"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= size; i++ {
		if _, err := input.Exec(size+1+i, "queued", nil, nil, fmt.Sprintf("queued-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	end := int64(size + 100002)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return path, end
}
