package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestWithdrawAdvancesTheAgentLogAndSurvivesReopen(t *testing.T) {
	path := tempPath(t)
	s := open(t, path)
	admitOpen(t, s, "run-1", "Do the task.", true)
	first := admitKind(t, s, harness.DeliveryFollowUp, "first held")
	second := admitKind(t, s, harness.DeliveryFollowUp, "second held")
	before, err := s.LastPos(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	withdrawn, err := s.Withdraw(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(withdrawn) != 2 || withdrawn[0].Pos != first.Input.Pos || withdrawn[1].Pos != second.Input.Pos {
		t.Fatalf("withdrawn = %+v", withdrawn)
	}
	after, err := s.LastPos(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after <= before {
		t.Fatalf("withdraw did not take an agent-log position: %d -> %d", before, after)
	}
	for _, in := range withdrawn {
		if in.WithdrawnPos != after {
			t.Fatalf("input %d records withdrawal position %d, want %d", in.Pos, in.WithdrawnPos, after)
		}
	}
	assertWithdrawalLog(t, s, before, after)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	again, err := s.LastPos(context.Background())
	if err != nil || again != after {
		t.Fatalf("position after reopen = %d, %v; want %d", again, err, after)
	}
	assertWithdrawalLog(t, s, before, after)
}

func assertWithdrawalLog(t *testing.T, s *Store, before, after int64) {
	t.Helper()
	rows, err := s.Log(context.Background(), before, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Pos != after {
		t.Fatalf("log after %d = %+v, want one withdrawal at %d", before, rows, after)
	}
	if rows[0].Withdrawn == nil || len(rows[0].Withdrawn) != 2 {
		t.Fatalf("withdrawal names = %v", rows[0].Withdrawn)
	}
	if rows[0].Withdrawn[0] != fmt.Sprintf("pos:%d", before-1) || rows[0].Withdrawn[1] != fmt.Sprintf("pos:%d", before) {
		t.Fatalf("withdrawal names = %v, want the two queued positions", rows[0].Withdrawn)
	}
}
