package sqlite

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

var recordHotPlans = flag.Bool("record-hot-plans", false, "print the complete plans of the hot-path queries")

type hotQuery struct {
	name string
	sql  string
	args []any
}

// hotFixture builds a representative history: several runs in one line,
// hundreds of inputs of every authority and disposition including
// withdrawals, observation keys, and many transitions.
func hotFixture(t *testing.T) *Store {
	t.Helper()
	s := open(t, tempPath(t))
	ctx := context.Background()
	now := admittedAt.UTC().Format(time.RFC3339Nano)
	for i := 1; i <= 4; i++ {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO runs (n, id, semantics, run, initial, fresh) VALUES (?, ?, 8, '{}', '{}', ?)`,
			i, fmt.Sprintf("run-%d", i), i == 1); err != nil {
			t.Fatal(err)
		}
	}
	principalOrdinal := 0
	for i := 0; i < 400; i++ {
		pos := int64(i + 1)
		authority, disposition, runID, admittedRun := "principal", "opened", any("run-1"), any("run-1")
		source, subject := "", ""
		if i%2 == 1 {
			authority, disposition, runID, admittedRun = "observation", "observation", nil, nil
			source, subject = "source-"+itoa(i%7), "subject-"+itoa(i%5)
		} else {
			switch principalOrdinal % 4 {
			case 0:
				disposition = "opened"
			case 1:
				disposition = "steer"
			case 2:
				disposition = "queued"
			default:
				disposition = "withdrawn"
			}
			principalOrdinal++
		}
		var inputID any
		if i%3 == 0 {
			inputID = "in-" + itoa(i)
		}
		var withdrawn, delivered any
		if disposition == "withdrawn" {
			withdrawn = int64(9000 + i)
		}
		if i%5 == 0 {
			delivered = int64(8000 + i)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO inputs
			(pos, input_id, authority, delivery, disposition, run_id, text, constraints, explicit_constraints, fresh, spec,
			 source, subject, admitted_at, admitted_run_id, withdrawn_pos, delivered_pos)
			VALUES (?, ?, ?, '', ?, ?, ?, '', 0, 0, '', ?, ?, ?, ?, ?, ?)`,
			pos, inputID, authority, disposition, runID, "input "+itoa(i), source, subject, now, admittedRun, withdrawn, delivered); err != nil {
			t.Fatal(err)
		}
		if authority == "observation" {
			if _, err := s.db.ExecContext(ctx, `INSERT INTO observation_keys (source, subject, latest_pos) VALUES (?, ?, ?)
				ON CONFLICT (source, subject) DO UPDATE SET latest_pos = excluded.latest_pos`, source, subject, pos); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 1; i <= 300; i++ {
		kind := "observed"
		if i%3 == 0 {
			kind = "inputs_bound"
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO transitions
			(run_id, seq, pos, event_id, kind, semantics, event, from_state, to_state, facts, step, attempt, rev)
			VALUES ('run-1', ?, ?, ?, ?, 8, '{}', '{}', '{}', '{}', 1, 1, 1)`,
			i, 1000+i, fmt.Sprintf("event-%d", i), kind); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE agent_log SET pos = ?`, 5000); err != nil {
		t.Fatal(err)
	}
	return s
}

func assertHotFixture(t *testing.T, s *Store) {
	t.Helper()
	var runs, observations, opened, steer, queued, withdrawn, keys, transitions, bindings, positions int
	checks := []struct {
		query string
		value *int
	}{
		{`SELECT count(*) FROM runs`, &runs},
		{`SELECT count(*) FROM inputs WHERE authority = 'observation'`, &observations},
		{`SELECT count(*) FROM inputs WHERE authority = 'principal' AND disposition = 'opened'`, &opened},
		{`SELECT count(*) FROM inputs WHERE authority = 'principal' AND disposition = 'steer'`, &steer},
		{`SELECT count(*) FROM inputs WHERE authority = 'principal' AND disposition = 'queued'`, &queued},
		{`SELECT count(*) FROM inputs WHERE authority = 'principal' AND disposition = 'withdrawn'`, &withdrawn},
		{`SELECT count(*) FROM observation_keys`, &keys},
		{`SELECT count(*) FROM transitions`, &transitions},
		{`SELECT count(*) FROM transitions WHERE kind = 'inputs_bound'`, &bindings},
		{`SELECT count(*) FROM inputs WHERE withdrawn_pos IS NOT NULL`, &positions},
	}
	for _, check := range checks {
		if err := s.db.QueryRowContext(context.Background(), check.query).Scan(check.value); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 4 || observations != 200 || opened != 50 || steer != 50 || queued != 50 || withdrawn != 50 ||
		keys != 35 || transitions != 300 || bindings != 100 || positions != 50 {
		t.Fatalf("hot fixture distribution = runs %d observations %d opened %d steer %d queued %d withdrawn %d keys %d transitions %d bindings %d withdrawal positions %d",
			runs, observations, opened, steer, queued, withdrawn, keys, transitions, bindings, positions)
	}
}

func planOf(t *testing.T, s *Store, query string, args ...any) string {
	t.Helper()
	rows, err := s.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
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
		fmt.Fprintf(&b, "%d|%d|%d|%s\n", id, parent, notused, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

var hotPlans = map[string]string{
	"boundsSQL":             "4|0|61|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=?)\n",
	"steerCountSQL":         "4|0|61|SEARCH inputs USING INDEX inputs_disposition (disposition=?)\n",
	"steersSQL":             "4|0|61|SEARCH inputs USING INDEX inputs_disposition (disposition=?)\n",
	"deliveredSQL":          "4|0|33|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n",
	"openFollowUpSQL":       "4|0|33|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n",
	"headSQL":               "5|0|61|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=?)\n",
	"inputAtSQL":            "2|0|33|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n",
	"inputByPositionSQL":    "2|0|33|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n",
	"inputsAtSQL":           "3|0|91|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n7|0|0|LIST SUBQUERY 1\n9|7|0|SCAN json_each VIRTUAL TABLE INDEX 1:\n",
	"lastRunSQL":            "2|0|33|SEARCH runs USING INTEGER PRIMARY KEY (rowid=?)\n5|0|0|SCALAR SUBQUERY 1\n10|5|216|SEARCH runs\n",
	"latestEventSQL":        "5|0|61|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=?)\n",
	"latestSQL":             "5|0|61|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=?)\n",
	"lastPosSQL":            "2|0|33|SEARCH agent_log USING INTEGER PRIMARY KEY (rowid=?)\n",
	"logInputsSQL":          "4|0|196|SEARCH inputs USING INTEGER PRIMARY KEY (rowid>?)\n",
	"logTransitionsSQL":     "5|0|202|SEARCH transitions USING INDEX transitions_pos (pos>?)\n",
	"logWithdrawalsSQL":     "2|0|0|CO-ROUTINE rows\n5|2|0|CO-ROUTINE (subquery-4)\n8|5|0|CO-ROUTINE (subquery-5)\n10|8|0|CO-ROUTINE page\n18|10|177|SEARCH inputs USING COVERING INDEX inputs_withdrawn (withdrawn_pos>?)\n51|8|49|SCAN p\n54|8|60|SEARCH i USING INDEX inputs_withdrawn (withdrawn_pos=?)\n69|8|0|USE TEMP B-TREE FOR ORDER BY\n87|5|82|SCAN (subquery-5)\n143|5|0|USE TEMP B-TREE FOR ORDER BY\n162|2|82|SCAN (subquery-4)\n214|0|82|SCAN rows\n226|0|0|USE TEMP B-TREE FOR ORDER BY\n",
	"observationKeySQL":     "",
	"observationsSQL":       "6|0|210|SCAN k USING INDEX observation_keys_latest\n9|0|45|SEARCH i USING INTEGER PRIMARY KEY (rowid=?)\n",
	"previousCountSQL":      "2|0|0|CO-ROUTINE line\n5|2|0|SETUP\n8|5|39|SEARCH runs USING INDEX sqlite_autoindex_runs_1 (id=?)\n29|2|0|RECURSIVE STEP\n31|29|216|SCAN l\n34|29|45|SEARCH r USING INTEGER PRIMARY KEY (rowid=?)\n47|0|206|SCAN line\n",
	"previousIDsSQL":        "2|0|0|CO-ROUTINE line\n5|2|0|SETUP\n8|5|39|SEARCH runs USING INDEX sqlite_autoindex_runs_1 (id=?)\n29|2|0|RECURSIVE STEP\n31|29|216|SCAN l\n34|29|45|SEARCH r USING INTEGER PRIMARY KEY (rowid=?)\n48|0|206|SCAN line\n63|0|0|USE TEMP B-TREE FOR ORDER BY\n",
	"queuedSQL":             "4|0|61|SEARCH inputs USING INDEX inputs_disposition (disposition=?)\n",
	"queuedCountSQL":        "3|0|53|SEARCH inputs USING COVERING INDEX inputs_disposition (disposition=?)\n",
	"inputByClientSQL":      "3|0|39|SEARCH inputs USING INDEX inputs_client (input_id=?)\n",
	"runSQL":                "3|0|39|SEARCH runs USING INDEX sqlite_autoindex_runs_1 (id=?)\n",
	"runOrdinalSQL":         "2|0|39|SEARCH runs USING COVERING INDEX sqlite_autoindex_runs_1 (id=?)\n",
	"runPosSQL":             "4|0|61|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=?)\n",
	"progressSQL":           "5|0|50|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=? AND seq>?)\n",
	"steerBytesSQL":         "4|0|61|SEARCH inputs USING INDEX inputs_disposition (disposition=?)\n",
	"withdrawQueuedSQL":     "5|0|53|SEARCH inputs USING INDEX inputs_disposition (disposition=?)\n",
	"withdrawableSteersSQL": "4|0|61|SEARCH inputs USING INDEX inputs_disposition (disposition=?)\n",
	"steerOrdinalSQL":       "4|0|50|SEARCH inputs USING INDEX inputs_disposition (disposition=? AND pos<?)\n",
	"markWithdrawnSQL":      "4|0|33|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n",
	"withdrawnPositionSQL":  "2|0|33|SEARCH inputs USING INTEGER PRIMARY KEY (rowid=?)\n",
	"priorRequestSQL":       "4|0|50|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=? AND seq<?)\n",
	"priorUsageSQL":         "5|0|49|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=? AND seq>? AND seq<?)\n",
	"acceptedInputSQL":      "4|0|50|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=? AND seq<?)\n",
	"rejectionSeqSQL":       "3|0|39|SEARCH runs USING INDEX sqlite_autoindex_runs_1 (id=?)\n",
	"transitionBySeqSQL":    "3|0|39|SEARCH transitions USING INDEX sqlite_autoindex_transitions_1 (run_id=? AND seq=?)\n",
}

func hotQueries() []hotQuery {
	return []hotQuery{
		{name: "inputByClientSQL", sql: inputByClientSQL, args: []any{"in-0"}},
		{name: "inputByPositionSQL", sql: inputByPositionSQL, args: []any{int64(1)}},
		{name: "inputAtSQL", sql: inputAtSQL, args: []any{int64(1)}},
		{name: "inputsAtSQL", sql: inputsAtSQL, args: []any{"[1,2,3]"}},
		{name: "steersSQL", sql: steersSQL, args: []any{"run-1"}},
		{name: "steerCountSQL", sql: steerCountSQL, args: []any{"run-1"}},
		{name: "boundsSQL", sql: boundsSQL, args: []any{"run-1"}},
		{name: "steerBytesSQL", sql: steerBytesSQL, args: []any{"run-1"}},
		{name: "deliveredSQL", sql: deliveredSQL, args: []any{int64(2), int64(1)}},
		{name: "observationsSQL", sql: observationsSQL, args: []any{harness.MaxObservationKeys}},
		{name: "observationKeySQL", sql: observationKeySQL, args: []any{"source-0", "subject-0", int64(1)}},
		{name: "lastPosSQL", sql: lastPosSQL},
		{name: "logTransitionsSQL", sql: logTransitionsSQL, args: []any{int64(0), 10}},
		{name: "logInputsSQL", sql: logInputsSQL, args: []any{int64(0), 10}},
		{name: "logWithdrawalsSQL", sql: logWithdrawalsSQL, args: []any{int64(0), 10, 64}},
		{name: "queuedSQL", sql: queuedSQL},
		{name: "queuedCountSQL", sql: queuedCountSQL},
		{name: "withdrawQueuedSQL", sql: withdrawQueuedSQL},
		{name: "progressSQL", sql: progressSQL, args: []any{"run-1", int64(0), 10}},
		{name: "lastRunSQL", sql: lastRunSQL},
		{name: "runSQL", sql: runSQL, args: []any{"run-1"}},
		{name: "latestSQL", sql: latestSQL, args: []any{"run-1"}},
		{name: "headSQL", sql: headSQL, args: []any{"run-1"}},
		{name: "previousIDsSQL", sql: previousIDsSQL, args: []any{"run-4", 16}},
		{name: "previousCountSQL", sql: previousCountSQL, args: []any{"run-4"}},
		{name: "steerOrdinalSQL", sql: steerOrdinalSQL, args: []any{"run-1", int64(1)}},
		{name: "markWithdrawnSQL", sql: markWithdrawnSQL, args: []any{int64(2), int64(1)}},
		{name: "withdrawableSteersSQL", sql: withdrawableSteersSQL, args: []any{"run-1"}},
		{name: "openFollowUpSQL", sql: openFollowUpSQL, args: []any{"run-2", int64(1)}},
		{name: "latestEventSQL", sql: latestEventSQL, args: []any{"run-1"}},
		{name: "runOrdinalSQL", sql: runOrdinalSQL, args: []any{"run-1"}},
		{name: "runPosSQL", sql: runPosSQL, args: []any{"run-1"}},
		{name: "withdrawnPositionSQL", sql: withdrawnPositionSQL, args: []any{int64(1)}},
		{name: "priorRequestSQL", sql: priorRequestSQL, args: []any{"run-1", int64(300)}},
		{name: "priorUsageSQL", sql: priorUsageSQL, args: []any{"run-1", int64(0), int64(300)}},
		{name: "acceptedInputSQL", sql: acceptedInputSQL, args: []any{"run-1", int64(300)}},
		{name: "rejectionSeqSQL", sql: rejectionSeqSQL, args: []any{"run-1"}},
		{name: "transitionBySeqSQL", sql: transitionBySeqSQL, args: []any{"run-1", int64(1)}},
	}
}

// hotPathExemptions are statements in the prepared-statement registry that
// deliberately stay outside the golden plan table, with the reason each is not
// a hot-path plan. The SQL is carried here so the registry check compares the
// exact prepared text rather than a name.
type hotPathExemption struct {
	sql    string
	reason string
}

var hotPathExemptions = map[string]hotPathExemption{
	"sameRunSQL":     {sameRunSQL, "CreateRun replay equality check, not an admission or prepare path"},
	"insertRunSQL":   {insertRunSQL, "write, not a read plan"},
	"initialSQL":     {initialSQL, "run initial-state recovery read"},
	"totalsSQL":      {totalsSQL, "run usage projection read"},
	"addSQL":         {addSQL, "write, not a read plan"},
	"sameSQL":        {sameSQL, "commit replay equality check, not an admission lookup"},
	"insertSQL":      {insertSQL, "write, not a read plan"},
	"transitionsSQL": {transitionsSQL, "recovery/history read, not the hot admission path"},
	"requestSQL":     {requestSQL, "frozen-request recovery read"},
	"responseSQL":    {responseSQL, "response recovery read"},
	"recentSQL":      {recentSQL, "step history read"},
	"stepsSQL":       {stepsSQL, "step projection read"},
	"savedSQL":       {savedSQL, "response recovery lookup"},
	"acceptedSQL":    {acceptedSQL, "acceptance recovery read"},
	"contextSeqSQL":  {contextSeqSQL, "context recovery read"},
	"contextHeadSQL": {contextHeadSQL, "context recovery read"},
	"receiptSQL":     {receiptSQL, "receipt projection read"},
	"agentPosSQL":    {agentPosSQL, "writer counter read inside nextPos; log end is covered by lastPosSQL"},
	"bumpPosSQL":     {bumpPosSQL, "write, not a read plan"},
	"insertInputSQL": {insertInputSQL, "write, not a read plan"},
	"latestRunsSQL":  {latestRunsSQL, "pre-existing full-history admission-validation scan on main, outside this delta"},
}

// missingPreparedStatements returns registry statements that are neither in
// the golden plan table nor explicitly exempt. The comparison is by prepared
// SQL text, so a name cannot grant coverage to a different statement.
func missingPreparedStatements(s *Store, plans map[string]string, exempt map[string]hotPathExemption) []string {
	classified := make(map[string]bool, len(plans)+len(exempt))
	for _, q := range hotQueries() {
		if _, ok := plans[q.name]; ok {
			classified[q.sql] = true
		}
	}
	for _, e := range exempt {
		classified[e.sql] = true
	}
	var missing []string
	for query := range s.stmts {
		if !classified[query] {
			missing = append(missing, query)
		}
	}
	sort.Strings(missing)
	return missing
}

// TestPreparedStatementRegistryIsComplete requires every statement in a
// Store's prepared-statement registry to be a golden plan or an explicit
// exemption. Its scope is only the registry: it does not inspect SQL call
// sites, and statements executed inside transactions are covered by their
// golden table entries as before.
func TestPreparedStatementRegistryIsComplete(t *testing.T) {
	s := hotFixture(t)
	assertHotFixture(t, s)
	if missing := missingPreparedStatements(s, hotPlans, hotPathExemptions); len(missing) > 0 {
		t.Fatalf("prepared-statement registry has %d unclassified statement(s):\n%s", len(missing), strings.Join(missing, "\n"))
	}

	t.Run("removed golden entry", func(t *testing.T) {
		plans := make(map[string]string, len(hotPlans))
		for name, plan := range hotPlans {
			plans[name] = plan
		}
		delete(plans, "observationsSQL")
		missing := missingPreparedStatements(s, plans, hotPathExemptions)
		if !slices.Contains(missing, observationsSQL) {
			t.Fatalf("removing observationsSQL from the golden table was not detected: %v", missing)
		}
	})
}

// TestHotPathGuardRejectsPlanMutations proves the exact-plan guard catches
// an aliased persistent scan and an aggregate that SQLite labels SEARCH but
// still iterates the whole index.
func TestHotPathGuardRejectsPlanMutations(t *testing.T) {
	s := hotFixture(t)
	alias := planOf(t, s, `SELECT `+inputColumns+` FROM inputs AS historical WHERE +input_id = ?`, "in-0")
	if err := planGuard(alias, hotPlans["inputByClientSQL"]); err == nil {
		t.Fatal("aliased scan passed the exact-plan guard")
	}
	aggregate := planOf(t, s, `SELECT semantics, run, input_pos, fresh FROM runs WHERE n = (SELECT max(+n) FROM runs)`)
	if err := planGuard(aggregate, hotPlans["lastRunSQL"]); err == nil {
		t.Fatal("max(+n) aggregate passed the exact-plan guard")
	}
}

func planGuard(got, want string) error {
	if got == want {
		return nil
	}
	return fmt.Errorf("query plan changed: got %q, want %q", got, want)
}

func TestHotPathQueryPlans(t *testing.T) {
	s := hotFixture(t)
	assertHotFixture(t, s)
	for _, q := range hotQueries() {
		t.Run(q.name, func(t *testing.T) {
			got := planOf(t, s, q.sql, q.args...)
			if *recordHotPlans {
				t.Logf("PLAN %s\n%s", q.name, got)
				return
			}
			want, ok := hotPlans[q.name]
			if !ok {
				t.Fatalf("hot query %s has no golden plan", q.name)
			}
			if got != want {
				t.Fatalf("plan changed for %s; got:\n%s\nwant:\n%s", q.name, got, want)
			}
		})
	}
}
