package sqlite

import (
	"context"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestAdmissionPlanDoesNotSearchEarlierTransitions(t *testing.T) {
	s := open(t, tempPath(t))
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN ` + latestRunsSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var correlated int
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "CORRELATED SCALAR SUBQUERY") {
			correlated++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if correlated > 1 {
		t.Fatalf("admission plan has %d correlated history lookups; want only the latest transition lookup", correlated)
	}
}

func TestAdmissionStoppedResolutionWithCancellation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		execution core.ExecutionState
		blocked   bool
	}{
		{"unknown", core.ExecutionUnknown, true},
		{"known", core.ExecutionExited, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t, tempPath(t))
			r := sessionRun("run-1", "input-1")
			if _, err := s.Submit(ctx, r, fresh(t, r.ID, limits)); err != nil {
				t.Fatal(err)
			}
			d := newDriver(t, s, harness.Snapshot{State: fresh(t, r.ID, limits)})
			d.decide(execBody("printf ok"), harness.Usage{})
			effectID := d.snap.State.EffectID
			d.do(core.OutcomeUnknown{EffectID: effectID}, harness.Facts{Failure: "lost job"})
			d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
			d.do(core.Resolved{EffectID: effectID, Execution: tc.execution}, harness.Facts{Observation: observation(tc.execution, 0)})
			if d.snap.State.Phase != core.PhaseStopped || d.snap.State.Reason != core.StopCancelled {
				t.Fatalf("terminal state = %#v", d.snap.State)
			}
			err := s.CheckAdmission(ctx)
			if (err != nil) != tc.blocked {
				t.Fatalf("CheckAdmission = %v, blocked want %t", err, tc.blocked)
			}
		})
	}
}
