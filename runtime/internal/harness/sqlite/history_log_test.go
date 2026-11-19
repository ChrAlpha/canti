package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func TestHistoryPagesTheNewestRowsWithinTheLine(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "Do the task.", true)
	admitKind(t, s, harness.DeliverySteer, "Try the tests first.")
	d := newDriver(t, s, latestOf(t, s, run.Run.ID))
	d.do(core.InputsBound{Count: 1}, harness.Facts{})
	d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{})

	end, err := s.LastPos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.History(ctx, HistoryCursor{}, 2, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Runs) != 1 || first.Runs[0].Run.ID != run.Run.ID || first.Runs[0].Ordinal != 1 || first.Runs[0].State.Phase != core.PhaseReturned {
		t.Fatalf("history runs = %+v, want the returned first run", first.Runs)
	}
	if len(first.Rows) != 2 || first.Older.Pos == 0 || first.Cursor != end || first.Rows[1].Pos != end || first.Rows[0].Pos >= first.Rows[1].Pos {
		t.Fatalf("newest page = %+v, older %d, cursor %d; want two ascending rows ending at %d", first.Rows, first.Older.Pos, first.Cursor, end)
	}
	second, err := s.History(ctx, first.Older, 2, 64, 8, first.Line)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Runs) != len(first.Runs) {
		t.Fatalf("older page summaries = %+v, want the snapshot summaries", second.Runs)
	}
	if len(second.Rows) == 0 || second.Rows[len(second.Rows)-1].Pos >= first.Rows[0].Pos {
		t.Fatalf("older page = %+v, want rows before %d", second.Rows, first.Rows[0].Pos)
	}
	oldest := second.Rows[0].Pos
	for cursor := second.Older; cursor.Pos != 0; {
		page, err := s.History(ctx, cursor, 2, 64, 8, first.Line)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Rows) == 0 {
			break
		}
		if page.Rows[0].Pos >= oldest {
			t.Fatalf("history page repeated position %d at or after %d", page.Rows[0].Pos, oldest)
		}
		oldest = page.Rows[0].Pos
		cursor = page.Older
	}
	if oldest != int64(run.Input.Pos) {
		t.Fatalf("history ended at %d, want the line's input at %d", oldest, run.Input.Pos)
	}
	if _, err := s.History(ctx, HistoryCursor{}, 0, 64, 8, HistoryLine{}); err == nil {
		t.Fatal("a history page without a bound was accepted")
	}
	if _, err := s.History(ctx, HistoryCursor{Pos: end + 2}, 2, 64, 8, HistoryLine{}); err == nil {
		t.Fatal("a history cursor past the end was accepted")
	}
}

func TestHistoryLineStopsAtTheLatestFreshRun(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first", true)
	driveReturn(t, s, first.Run.ID)
	second := admitOpen(t, s, "run-2", "second", false)
	driveReturn(t, s, second.Run.ID)
	third := admitOpen(t, s, "run-3", "third", true)

	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Runs) != 1 || data.Runs[0].Run.ID != third.Run.ID || data.Runs[0].Ordinal != 3 {
		t.Fatalf("history runs = %+v, want only the fresh run", data.Runs)
	}
	for _, row := range data.Rows {
		if row.RunID != third.Run.ID {
			t.Fatalf("history crossed the fresh boundary into %s: %+v", row.RunID, row)
		}
	}
	driveReturn(t, s, third.Run.ID)
	fourth := admitOpen(t, s, "run-4", "fourth", false)
	data, err = s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Runs) != 2 || data.Runs[0].Run.ID != fourth.Run.ID || data.Runs[1].Run.ID != third.Run.ID {
		t.Fatalf("history runs after the non-fresh run = %+v", data.Runs)
	}
	if _, err := s.History(ctx, HistoryCursor{}, 64, 0, 8, HistoryLine{}); err == nil {
		t.Fatal("a line walk without a run bound was accepted")
	}
}

func TestHistoryKeepsAFreshFollowUpInItsNewLine(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	follow, err := s.Admit(ctx, harness.InputSubmission{InputID: "follow-input", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "new line", Fresh: true, Now: admittedAt})
	if err != nil {
		t.Fatal(err)
	}
	driveReturn(t, s, first.Run.ID)
	firstEnd, err := s.RunPos(ctx, first.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := submission(t, "run-2", "new line", true)
	next.Run.Fresh = true
	opened, err := s.OpenFollowUp(ctx, follow.Input, next.Run, next.Initial, project.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if data.Cursor != firstEnd {
		t.Fatalf("follow cursor = %d, want the examined log end %d", data.Cursor, firstEnd)
	}
	for _, row := range data.Rows {
		if row.Pos == firstEnd {
			t.Fatalf("follow page replays the predecessor's final row at %d: %+v", firstEnd, row)
		}
	}
	driveReturn(t, s, opened.Run.ID)
	data, err = s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Runs) != 1 || data.Runs[0].Run.ID != opened.Run.ID {
		t.Fatalf("history runs = %+v, want only the fresh follow-up run", data.Runs)
	}
	if len(data.Rows) == 0 || data.Rows[0].Pos != follow.Input.Pos || data.Rows[0].RunID != opened.Run.ID {
		t.Fatalf("fresh follow-up history starts with %+v, want its queued input at %d", data.Rows, follow.Input.Pos)
	}
	for _, row := range data.Rows {
		if row.RunID != opened.Run.ID {
			t.Fatalf("history includes the predecessor row %+v", row)
		}
	}
	if follow.Input.Pos >= firstEnd {
		t.Fatalf("the follow-up input at %d does not precede the predecessor's final row %d", follow.Input.Pos, firstEnd)
	}
	if data.Rows[0].Pos >= data.Rows[len(data.Rows)-1].Pos && len(data.Rows) > 1 {
		t.Fatalf("history rows are not in position order: %+v", data.Rows)
	}
}

func TestHistoryIncludesAQueuedFollowUpBeforeItOpens(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	admitOpen(t, s, "run-1", "working", true)
	queued := admitKind(t, s, harness.DeliveryFollowUp, "queued next")
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	var found *LogEntry
	for i := range data.Rows {
		row := &data.Rows[i]
		if row.Input != nil && row.Input.Delivery == harness.DeliveryFollowUp && row.Input.Text == "queued next" {
			found = row
		}
	}
	if found == nil || found.RunID != "" || found.Pos != queued.Input.Pos {
		t.Fatalf("queued follow-up row = %+v, want position %d without a run", found, queued.Input.Pos)
	}
}

func TestHistoryBoundsWithdrawalNamesBeforeWindowing(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	admitOpen(t, s, "run-1", "hold them", true)
	var lastPos int64
	for range 70 {
		lastPos = admitKind(t, s, harness.DeliveryFollowUp, "held").Input.Pos
	}
	if _, err := s.Withdraw(ctx, ""); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Log(ctx, lastPos, 1)
	if err != nil || len(rows) != 1 || len(rows[0].Withdrawn) != withdrawalNames || !rows[0].WithdrawnCut {
		t.Fatalf("withdrawal log = %+v, %v; want %d names and a cut", rows, err, withdrawalNames)
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	var withdrawal *LogEntry
	for i := range data.Rows {
		if data.Rows[i].Withdrawn != nil {
			withdrawal = &data.Rows[i]
		}
	}
	if withdrawal == nil || len(withdrawal.Withdrawn) != withdrawalNames || !withdrawal.WithdrawnCut {
		t.Fatalf("history withdrawal = %+v, want bounded names", withdrawal)
	}
	joined := strings.Join(rows[0].Withdrawn, " ")
	if !strings.Contains(joined, "pos:2 ") || strings.Contains(joined, "pos:70") {
		t.Fatalf("withdrawal names = %q, want the first %d in position order", rows[0].Withdrawn, withdrawalNames)
	}
}

func TestHistoryAnEmptyStateDirectoryHasNoLatestRun(t *testing.T) {
	s := open(t, tempPath(t))
	data, err := s.History(context.Background(), HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil || data.Runs != nil || data.Rows != nil || data.Cursor != 0 || data.Older.Pos != 0 {
		t.Fatalf("empty history = %+v, %v", data, err)
	}
	if _, err := s.History(context.Background(), HistoryCursor{}, 0, 64, 8, HistoryLine{}); err == nil {
		t.Fatal("a history page without a bound was accepted")
	}
	if _, err := s.History(context.Background(), HistoryCursor{Pos: -1}, 64, 64, 8, HistoryLine{}); err == nil {
		t.Fatal("a negative history cursor was accepted")
	}
	if _, err := s.History(context.Background(), HistoryCursor{}, 64, 64, 0, HistoryLine{}); err == nil {
		t.Fatal("a history read without a summary bound was accepted")
	}
}

func driveReturn(t *testing.T, s *Store, runID string) {
	t.Helper()
	d := newDriver(t, s, latestOf(t, s, runID))
	d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{})
}

func TestHistoryKeepsAnInputThatOpensANewRunBetweenPages(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	queued := admitKind(t, s, harness.DeliveryFollowUp, "late input")
	d := newDriver(t, s, latestOf(t, s, first.Run.ID))
	d.freeze()
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Cursor != 3 || page.Older.Pos != 3 || len(page.Rows) != 1 || page.Rows[0].Transition == nil || page.Rows[0].Transition.Kind != "request_frozen" {
		t.Fatalf("first page = %+v, want the transition at 3 and an older page", page)
	}
	// The queued input opens a new run after the first page; its admission
	// position 2 was inside the snapshot, so the old line token still shows it.
	next := submission(t, "run-2", "late input", false)
	next.Run.InputPos, next.Run.Goal, next.Run.Fresh = queued.Input.Pos, "late input", false
	data, state, err := encodeRun(next.Run, next.Initial)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO runs (n, id, semantics, run, initial, project, project_meta, input_pos, fresh, line) VALUES (2, 'run-2', ?, ?, ?, '{}', '{}', ?, 0, 1)`,
		core.SemanticsVersion, data, state, queued.Input.Pos); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE inputs SET run_id = 'run-2', disposition = 'opened', line = 1 WHERE pos = ?`, queued.Input.Pos); err != nil {
		t.Fatal(err)
	}
	older, err := s.History(ctx, page.Older, 64, 64, 8, page.Line)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range older.Rows {
		if item.Pos == queued.Input.Pos {
			if found || item.RunID != "" || item.Input == nil || item.Input.Disposition != harness.DispositionQueued {
				t.Fatalf("snapshot input = %+v, want the queued goal", item)
			}
			found = true
		}
	}
	if !found || page.Line.Start != 1 || page.Line.EndPos != 4 {
		t.Fatalf("older page = %+v, line = %+v; want the late input once", older, page.Line)
	}
	_ = first
}

func TestHistoryExcludesAPredecessorsWithdrawal(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	driveReturn(t, s, first.Run.ID)
	second := admitOpen(t, s, "run-2", "fresh line", true)
	if _, err := s.db.Exec(`INSERT INTO inputs (pos, authority, disposition, withdrawn_pos, run_id, text, admitted_at)
		VALUES (50, 'principal', 'withdrawn', 60, 'run-1', 'predecessor', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE agent_log SET pos = 60`); err != nil {
		t.Fatal(err)
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range data.Rows {
		if row.Withdrawn != nil {
			t.Fatalf("a predecessor withdrawal appeared in the fresh line: %+v", row)
		}
		if row.RunID != "" && row.RunID != second.Run.ID {
			t.Fatalf("the fresh line included predecessor row %+v", row)
		}
	}
}

func TestHistoryExcludesAnOrphanWithdrawalFromThePredecessorLine(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	queuedFresh, err := s.Admit(ctx, harness.InputSubmission{InputID: "follow-input", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "fresh line", Fresh: true, Now: admittedAt})
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := s.Admit(ctx, harness.InputSubmission{InputID: "orphan-input", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "orphan", Now: admittedAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Withdraw(ctx, orphan.Input.InputID); err != nil {
		t.Fatal(err)
	}
	driveReturn(t, s, first.Run.ID)
	next := submission(t, "run-2", "fresh line", true)
	next.Run.Fresh = true
	if _, err := s.OpenFollowUp(ctx, queuedFresh.Input, next.Run, next.Initial, project.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range data.Rows {
		if row.Pos == orphan.Input.Pos || (row.Input != nil && row.Input.Text == "orphan") {
			t.Fatalf("the fresh line carried the predecessor's orphan withdrawal: %+v", row)
		}
	}
}

func TestHistoryKeepsAWithdrawalOnTheIdleFreshLine(t *testing.T) {
	for _, count := range []int{1, 9} {
		t.Run(fmt.Sprintf("%d", count), func(t *testing.T) {
			ctx := context.Background()
			s := open(t, tempPath(t))
			run := admitOpen(t, s, "run-1", "fresh", true)
			var queued []harness.Admission
			for i := 0; i < count; i++ {
				q := admitKind(t, s, harness.DeliveryFollowUp, fmt.Sprintf("orphan-%d", i))
				queued = append(queued, q)
			}
			for _, q := range queued {
				if _, err := s.Withdraw(ctx, q.Input.InputID); err != nil {
					t.Fatal(err)
				}
			}
			driveReturn(t, s, run.Run.ID)
			data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
			if err != nil {
				t.Fatal(err)
			}
			seen := map[int64]bool{}
			for _, row := range data.Rows {
				if row.Input != nil && row.Input.Text == fmt.Sprintf("orphan-%d", count-1) {
					seen[row.Pos] = true
				}
			}
			if len(seen) != 1 {
				t.Fatalf("%d withdrawn inputs on the idle fresh line were not retained: %+v", count, data.Rows)
			}
		})
	}
}

func TestHistorySnapshotKeepsAQueuedInputQueued(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first", true)
	queued, err := s.Admit(ctx, harness.InputSubmission{InputID: "fresh-queued", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Fresh: true, Text: "fresh goal", Now: admittedAt})
	if err != nil {
		t.Fatal(err)
	}
	driveReturn(t, s, first.Run.ID)
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	next := submission(t, "run-2", "fresh goal", true)
	next.Run.Fresh = true
	if _, err := s.OpenFollowUp(ctx, queued.Input, next.Run, next.Initial, project.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	older, err := s.History(ctx, page.Older, 64, 64, 8, page.Line)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range older.Rows {
		if row.Pos == queued.Input.Pos {
			found = true
			if row.Input == nil || row.Input.Disposition != harness.DispositionQueued || row.Input.Text != "fresh goal" {
				t.Fatalf("snapshot input = %+v, want the queued snapshot state", row.Input)
			}
		}
	}
	if !found {
		t.Fatalf("fresh queued goal missing from the pinned page: %+v", older.Rows)
	}
}

func TestHistorySnapshotKeepsAWithdrawnQueuedInput(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first", true)
	queued := admitKind(t, s, harness.DeliveryFollowUp, "queued goal")
	driveReturn(t, s, first.Run.ID)
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Withdraw(ctx, queued.Input.InputID); err != nil {
		t.Fatal(err)
	}
	older, err := s.History(ctx, page.Older, 64, 64, 8, page.Line)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range older.Rows {
		if row.Pos == queued.Input.Pos {
			if row.Input == nil || row.Input.Disposition != harness.DispositionQueued {
				t.Fatalf("snapshot disposition = %+v, want queued", row.Input)
			}
			return
		}
	}
	t.Fatalf("queued input missing from the snapshot page: %+v", older.Rows)
}

func TestHistorySessionsAreIndependentAndRelease(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "line", true)
	driveReturn(t, s, first.Run.ID)
	baseline := s.db.Stats().InUse
	var pages []HistoryData
	for i := 0; i < 12; i++ {
		page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, page)
	}
	if len(s.histories.entries) > historySessionMax {
		t.Fatalf("open sessions = %d, want <= %d", len(s.histories.entries), historySessionMax)
	}
	seen := map[string]bool{}
	for _, page := range pages {
		if page.Older.Session == "" || seen[page.Older.Session] {
			t.Fatalf("session id not unique: %q", page.Older.Session)
		}
		seen[page.Older.Session] = true
	}
	if _, err := s.History(ctx, pages[0].Older, 1, 64, 8, pages[0].Line); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("evicted cursor error = %v", err)
	}
	if _, err := s.History(ctx, pages[11].Older, 1, 64, 8, pages[11].Line); err != nil {
		t.Fatalf("live session cursor = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.db.Stats().InUse; got != baseline {
		t.Fatalf("connections in use after close = %d, baseline %d", got, baseline)
	}
	if len(s.histories.entries) != 0 {
		t.Fatalf("sessions after close = %d", len(s.histories.entries))
	}
}

func TestHistorySessionCapEvicts(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	for i := 0; i < historySessionMax+1; i++ {
		run := admitOpen(t, s, fmt.Sprintf("run-%d", i), "goal", true)
		driveReturn(t, s, run.Run.ID)
		if _, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{}); err != nil {
			t.Fatal(err)
		}
	}
	s.histories.mu.Lock()
	n := len(s.histories.entries)
	s.histories.mu.Unlock()
	if n > historySessionMax {
		t.Fatalf("open history snapshots = %d, want at most %d", n, historySessionMax)
	}
}

func TestHistorySnapshotKeepsSeenState(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "goal", true)
	steer := admitKind(t, s, harness.DeliverySteer, "steer")
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, s, latestOf(t, s, run.Run.ID))
	d.do(core.RequestFrozen{}, harness.Facts{Request: requestFor(d.snap.State), Delivered: &harness.Delivered{Positions: []int64{steer.Input.Pos}}})
	older, err := s.History(ctx, page.Older, 64, 64, 8, page.Line)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range older.Rows {
		if row.Input != nil && row.Input.Pos == steer.Input.Pos && row.Input.DeliveredPos != 0 {
			t.Fatalf("pinned Seen changed after delivery: %+v", row.Input)
		}
	}
}

func TestHistoryRejectsUnissuedCursors(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "goal", true)
	driveReturn(t, s, run.Run.ID)
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	for _, forged := range []HistoryCursor{
		{Pos: page.Older.Pos, Seq: map[string]int64{}},
		{Pos: page.Older.Pos, Seq: map[string]int64{"forged": 1000000}},
		{Pos: 2, Seq: map[string]int64{}},
	} {
		if _, err := s.History(ctx, forged, 64, 64, 8, page.Line); !errors.Is(err, ErrHistoryCursor) {
			t.Fatalf("forged cursor %+v error = %v", forged, err)
		}
	}
}

func TestHistorySnapshotExpires(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	s.SetHistoryLifetime(20 * time.Millisecond)
	first := admitOpen(t, s, "run-1", "first", true)
	driveReturn(t, s, first.Run.ID)
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, err := s.History(ctx, page.Older, 64, 64, 8, page.Line); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("expired session error = %v", err)
	}
	s.histories.mu.Lock()
	open := len(s.histories.entries)
	s.histories.mu.Unlock()
	if open != 0 {
		t.Fatalf("expired session still tracked: %d", open)
	}
}

func TestHistoryPinnedPaginationDoesNotCrossANewFreshLine(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	a := admitOpen(t, s, "run-a", "A goal", true)
	driveReturn(t, s, a.Run.ID)
	first, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Older.Pos == 0 {
		t.Fatal("the pinned page has no continuation")
	}
	b := admitOpen(t, s, "run-b", "B goal", true)
	driveReturn(t, s, b.Run.ID)
	successors := map[string]bool{}
	for i := 0; i < 9; i++ {
		c := admitOpen(t, s, fmt.Sprintf("run-c-%d", i), fmt.Sprintf("C goal %d", i), false)
		driveReturn(t, s, c.Run.ID)
		successors[c.Run.ID] = true
	}
	for cursor := first.Older; cursor.Pos != 0; {
		page, err := s.History(ctx, cursor, 64, 64, 8, first.Line)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Rows {
			if successors[row.RunID] || (row.Input != nil && strings.HasPrefix(row.Input.Text, "C goal")) {
				t.Fatalf("pinned A pagination crossed into a successor: %+v", row)
			}
		}
		if page.Older.Pos == 0 {
			break
		}
		cursor = page.Older
	}
}

func TestHistoryKeepsLateOpeningsAtAndBelowTheBound(t *testing.T) {
	for _, count := range []int{1, 8} {
		t.Run(fmt.Sprintf("%d", count), func(t *testing.T) {
			ctx := context.Background()
			s := open(t, tempPath(t))
			first := admitOpen(t, s, "run-1", "first line", true)
			var queued []harness.Admission
			for i := 1; i <= count; i++ {
				next, err := s.Admit(ctx, harness.InputSubmission{InputID: fmt.Sprintf("queued-%d", i), Authority: harness.AuthorityPrincipal,
					Delivery: harness.DeliveryFollowUp, Text: fmt.Sprintf("queued-%d", i), Now: admittedAt})
				if err != nil {
					t.Fatal(err)
				}
				queued = append(queued, next)
			}
			page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
			if err != nil {
				t.Fatal(err)
			}
			driveReturn(t, s, first.Run.ID)
			for i, q := range queued {
				next := submission(t, fmt.Sprintf("late-%d", i+1), q.Input.Text, false)
				opened, err := s.OpenFollowUp(ctx, q.Input, next.Run, next.Initial, project.Snapshot{})
				if err != nil {
					t.Fatal(err)
				}
				driveReturn(t, s, opened.Run.ID)
			}
			seen := map[int64]bool{}
			for _, row := range page.Rows {
				seen[row.Pos] = true
			}
			omitted := false
			for cursor := page.Older; ; {
				next, err := s.History(ctx, cursor, 1, 64, 8, page.Line)
				if err != nil {
					t.Fatal(err)
				}
				omitted = omitted || next.Omitted
				for _, row := range next.Rows {
					seen[row.Pos] = true
				}
				if next.Older.Pos == 0 {
					break
				}
				cursor = next.Older
			}
			if !seen[queued[0].Input.Pos] {
				t.Fatalf("%d late runs dropped the oldest opening at %d; seen %v", count, queued[0].Input.Pos, seen)
			}
			if omitted {
				t.Fatalf("%d late openings should stay outside the snapshot, omitted=%v", count, omitted)
			}
		})
	}
}

func TestHistoryKeepsAllLateOpeningsAndReportsTheBound(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	var queued []harness.Admission
	for i := 1; i <= 9; i++ {
		next, err := s.Admit(ctx, harness.InputSubmission{InputID: fmt.Sprintf("queued-%d", i), Authority: harness.AuthorityPrincipal,
			Delivery: harness.DeliveryFollowUp, Text: fmt.Sprintf("queued-%d", i), Now: admittedAt})
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, next)
	}
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Older.Pos == 0 {
		t.Fatal("the snapshot did not leave an older page")
	}
	driveReturn(t, s, first.Run.ID)
	for i, q := range queued {
		next := submission(t, fmt.Sprintf("late-%d", i+1), q.Input.Text, false)
		opened, err := s.OpenFollowUp(ctx, q.Input, next.Run, next.Initial, project.Snapshot{})
		if err != nil {
			t.Fatal(err)
		}
		driveReturn(t, s, opened.Run.ID)
	}
	seen := map[int64]bool{}
	omitted := false
	for cursor := page.Older; ; {
		next, err := s.History(ctx, cursor, 1, 64, 8, page.Line)
		if err != nil {
			t.Fatal(err)
		}
		omitted = omitted || next.Omitted
		for _, row := range next.Rows {
			seen[row.Pos] = true
		}
		if next.Older.Pos == 0 {
			break
		}
		cursor = next.Older
	}
	if !seen[queued[0].Input.Pos] {
		t.Fatalf("the oldest late opening at %d was silently dropped: seen %v", queued[0].Input.Pos, seen)
	}
	if omitted {
		t.Fatal("late openings must not enter the snapshot summaries")
	}
}

func TestHistoryLaterPageCarriesTheRunThatOpenedAfterTheSnapshot(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	follow := admitKind(t, s, harness.DeliveryFollowUp, "late goal")
	driveReturn(t, s, first.Run.ID)
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Older.Pos == 0 {
		t.Fatal("the snapshot left no older page")
	}
	next := submission(t, "run-2", "late goal", false)
	opened, err := s.OpenFollowUp(ctx, follow.Input, next.Run, next.Initial, project.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	driveReturn(t, s, opened.Run.ID)
	older, err := s.History(ctx, page.Older, 64, 64, 8, page.Line)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range older.Runs {
		if run.Run.ID == opened.Run.ID {
			t.Fatalf("the snapshot included a run opened after it: %+v", run)
		}
	}
}

func TestHistoryKeepsTenLateOpeningsAndReportsTheBound(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	first := admitOpen(t, s, "run-1", "first line", true)
	var queued []harness.Admission
	for i := 1; i <= 10; i++ {
		next, err := s.Admit(ctx, harness.InputSubmission{InputID: fmt.Sprintf("queued-%d", i), Authority: harness.AuthorityPrincipal,
			Delivery: harness.DeliveryFollowUp, Text: fmt.Sprintf("queued-%d", i), Now: admittedAt})
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, next)
	}
	page, err := s.History(ctx, HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	driveReturn(t, s, first.Run.ID)
	for i, q := range queued {
		next := submission(t, fmt.Sprintf("late-%d", i+1), q.Input.Text, false)
		opened, err := s.OpenFollowUp(ctx, q.Input, next.Run, next.Initial, project.Snapshot{})
		if err != nil {
			t.Fatal(err)
		}
		driveReturn(t, s, opened.Run.ID)
	}
	seen := map[int64]bool{}
	omitted := false
	for cursor := page.Older; ; {
		next, err := s.History(ctx, cursor, 1, 64, 8, page.Line)
		if err != nil {
			t.Fatal(err)
		}
		omitted = omitted || next.Omitted
		for _, row := range next.Rows {
			seen[row.Pos] = true
		}
		if next.Older.Pos == 0 {
			break
		}
		cursor = next.Older
	}
	if !seen[queued[0].Input.Pos] || omitted {
		t.Fatalf("ten late openings: seen oldest %v, omitted %v", seen[queued[0].Input.Pos], omitted)
	}
}

func TestHistoryPaginationIsPageSizeIndependent(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	for i := 1; i <= 9; i++ {
		run := admitOpen(t, s, fmt.Sprintf("run-%d", i), fmt.Sprintf("goal-%d", i), i == 1)
		driveReturn(t, s, run.Run.ID)
	}
	admitOpen(t, s, "run-10", "goal-10", false)
	queued := admitKind(t, s, harness.DeliveryFollowUp, "queued text")
	if _, err := s.Withdraw(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Admit(ctx, harness.InputSubmission{InputID: "still-queued", Authority: harness.AuthorityPrincipal,
		Delivery: harness.DeliveryFollowUp, Text: "still queued", Now: admittedAt}); err != nil {
		t.Fatal(err)
	}
	reference, err := s.History(ctx, HistoryCursor{}, 256, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]bool{}
	for _, row := range reference.Rows {
		want[row.Pos] = true
	}
	for _, limit := range []int{1, 2, 3, 64} {
		got := map[int64]int{}
		line := HistoryLine{}
		for cursor := (HistoryCursor{}); ; {
			page, err := s.History(ctx, cursor, limit, 64, 8, line)
			line = page.Line
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range page.Rows {
				got[row.Pos]++
				if row.Pos == queued.Input.Pos {
					if row.Input == nil || row.Input.Text != "queued text" || row.Input.Disposition != harness.DispositionWithdrawn {
						t.Fatalf("withdrawn queued input lost its text: %+v", row)
					}
				}
			}
			if page.Older.Pos == 0 {
				break
			}
			cursor = page.Older
		}
		if len(got) != len(want) {
			t.Fatalf("limit %d returned %d positions, reference %d; reference %v, got %v", limit, len(got), len(want), want, got)
		}
		for pos := range want {
			if got[pos] != 1 {
				t.Fatalf("limit %d position %d occurred %d times; want once (queued %d)", limit, pos, got[pos], queued.Input.Pos)
			}
		}
	}
}

func TestHistoryPaginationWithExcludedWithdrawals(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	next := submission(t, "run-2", "line goal", true)
	next.Run.InputPos, next.Run.Fresh = 10, true
	data, state, err := encodeRun(next.Run, next.Initial)
	if err != nil {
		t.Fatal(err)
	}
	h, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Rollback()
	if _, err := h.Exec(`INSERT INTO runs (n, id, semantics, run, initial, project, project_meta, input_pos, fresh, line) VALUES (1, 'run-1', ?, '{}', '{}', '{}', '{}', 1, 1, 1)`, core.SemanticsVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Exec(`INSERT INTO runs (n, id, semantics, run, initial, project, project_meta, input_pos, fresh, line) VALUES (2, 'run-2', ?, ?, ?, '{}', '{}', 10, 1, 2)`, core.SemanticsVersion, data, state); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Exec(`INSERT INTO inputs (pos, authority, disposition, withdrawn_pos, run_id, line, text, admitted_at) VALUES
		(10, 'principal', 'opened', 20, 'run-2', 2, 'line goal', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	for _, pos := range []int64{11, 12, 13, 14, 15, 21, 22, 23, 24, 25} {
		if _, err := h.Exec(`INSERT INTO inputs (pos, authority, disposition, withdrawn_pos, run_id, line, text, admitted_at)
			VALUES (?, 'principal', 'withdrawn', ?, 'run-1', 1, 'old', '2026-01-01T00:00:00Z')`, pos, pos); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.Exec(`UPDATE agent_log SET pos = 25`); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(); err != nil {
		t.Fatal(err)
	}
	reference, err := s.History(ctx, HistoryCursor{}, 256, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]bool{}
	for _, row := range reference.Rows {
		want[row.Pos] = true
	}
	if !want[20] {
		t.Fatalf("reference lacks the eligible withdrawal: %+v", reference.Rows)
	}
	for _, limit := range []int{1, 2, 3, 64} {
		got := map[int64]int{}
		var lineForPages HistoryLine
		for cursor := (HistoryCursor{}); ; {
			page, err := s.History(ctx, cursor, limit, 64, 8, lineForPages)
			lineForPages = page.Line
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range page.Rows {
				got[row.Pos]++
			}
			if page.Older.Pos == 0 {
				break
			}
			cursor = page.Older
		}
		if len(got) != len(want) {
			t.Fatalf("limit %d returned %d positions, reference %d; want %v, got %v", limit, len(got), len(want), want, got)
		}
		for pos := range want {
			if got[pos] != 1 {
				t.Fatalf("limit %d position %d occurred %d times; want once", limit, pos, got[pos])
			}
		}
	}
}

func TestHistoryMarksOnlyTheSteerDeliveredToTheFrozenRequest(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "goal", true)
	seen := admitKind(t, s, harness.DeliverySteer, "delivered steer")
	d := newDriver(t, s, harness.Snapshot{State: latestOf(t, s, run.Run.ID).State})
	d.do(core.RequestFrozen{}, harness.Facts{Request: requestFor(d.snap.State), Delivered: &harness.Delivered{Positions: []int64{seen.Input.Pos}}})
	held := admitKind(t, s, harness.DeliverySteer, "held steer")
	d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	found := map[int64]bool{}
	for _, row := range data.Rows {
		if row.Input == nil {
			continue
		}
		switch row.Input.Pos {
		case seen.Input.Pos:
			found[seen.Input.Pos] = row.Input.DeliveredPos != 0
		case held.Input.Pos:
			found[held.Input.Pos] = row.Input.DeliveredPos != 0
		}
	}
	if !found[seen.Input.Pos] || found[held.Input.Pos] {
		t.Fatalf("seen states = %v; want %d seen and %d held", found, seen.Input.Pos, held.Input.Pos)
	}
}

func TestHistoryAndLogSelectDeliveredPos(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	run := admitOpen(t, s, "run-1", "goal", true)
	steer := admitKind(t, s, harness.DeliverySteer, "delivered steer")
	if _, err := s.db.Exec(`UPDATE inputs SET delivered_pos = 99 WHERE pos = ?`, steer.Input.Pos); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Log(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var logSeen bool
	for _, row := range rows {
		if row.Input != nil && row.Input.Pos == steer.Input.Pos {
			logSeen = row.Input.DeliveredPos == 99
		}
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	var historySeen bool
	for _, row := range data.Rows {
		if row.Input != nil && row.Input.Pos == steer.Input.Pos {
			historySeen = row.Input.DeliveredPos == 99
		}
	}
	if !logSeen || !historySeen {
		t.Fatalf("delivered_pos was not selected: log %v, history %v (%s)", logSeen, historySeen, run.Run.ID)
	}
}

func TestHistoryReportsRunsBeyondTheSummaryBound(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	for i := 1; i <= 9; i++ {
		run := admitOpen(t, s, fmt.Sprintf("run-%d", i), fmt.Sprintf("goal-%d", i), i == 1)
		driveReturn(t, s, run.Run.ID)
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if !data.Omitted || len(data.Runs) != 8 {
		t.Fatalf("history runs = %d, omitted %v; want eight summaries and an omission", len(data.Runs), data.Omitted)
	}
}

func TestHistoryWithOnlyObservationsHasTheExaminedEnd(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	if _, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation, Source: "ci", Subject: "build", Text: "green", Now: admittedAt}); err != nil {
		t.Fatal(err)
	}
	end, err := s.LastPos(ctx)
	if err != nil || end == 0 {
		t.Fatalf("observation position = %d, %v", end, err)
	}
	data, err := s.History(ctx, HistoryCursor{}, 64, 64, 8, HistoryLine{})
	if err != nil || data.Cursor != end || data.Runs != nil || len(data.Rows) != 0 {
		t.Fatalf("history without a run = %+v, %v; want cursor %d", data, err, end)
	}
}

func TestHistoryQueryPlansUseIndexOrderAtTwoSizes(t *testing.T) {
	for _, size := range []int{100, 10000} {
		s := historyPlanStore(t, size)
		plan := planOf(t, s, historyTransitionsRunSQL, "run-1", int64(size+1), int64(1), 64)
		if strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, "sqlite_autoindex_transitions_1") {
			t.Fatalf("%d transitions: history transition plan is not index-ordered:\n%s", size, plan)
		}
		plan = planOf(t, s, historyWithdrawalsSQL, 1, int64(size+1), 65, 65)
		if strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, "inputs_line_withdrawn") {
			t.Fatalf("%d transitions: history withdrawal plan is not index-bounded:\n%s", size, plan)
		}
	}
}

func historyPlanStore(t *testing.T, transitions int) *Store {
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
