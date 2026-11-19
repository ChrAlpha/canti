package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A terminal page marks a snapshot draining. Every nonterminal page that
// decides afterwards sees ErrHistoryExpired even while other operations are
// still pending, because admission refuses a draining session and its Older
// could not be followed.
func TestHistoryDrainingSessionReturnsExpiredToEveryNonterminalPage(t *testing.T) {
	s := open(t, tempPath(t))
	a := admitOpen(t, s, "a", "goal", true)
	driveReturn(t, s, a.Run.ID)
	first, err := s.History(context.Background(), HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Older.Pos == 0 {
		t.Fatal("fixture has no continuation")
	}
	s.histories.mu.Lock()
	session := s.histories.entries[first.Line.Session]
	session.pending = 3
	s.histories.mu.Unlock()

	closeNow, status := s.finishHistoryOp(session, true)
	if closeNow || status != historyOpOK || !session.draining {
		t.Fatalf("terminal page: close=%v status=%d draining=%v", closeNow, status, session.draining)
	}
	for i := 0; i < 2; i++ {
		closeNow, status = s.finishHistoryOp(session, false)
		if status != historyOpExpired {
			t.Fatalf("nonterminal %d: close=%v status=%d", i, closeNow, status)
		}
		if i == 0 && closeNow {
			t.Fatal("closed while another operation was pending")
		}
		if i == 1 && !closeNow {
			t.Fatal("did not close when the last operation returned")
		}
	}
	s.histories.mu.Lock()
	_, stillOpen := s.histories.entries[first.Line.Session]
	s.histories.mu.Unlock()
	if stillOpen {
		t.Fatal("draining session remained open")
	}
	s.closeSession(session)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryAdmissionRefusesADrainingSessionWithoutRegistering(t *testing.T) {
	s := open(t, tempPath(t))
	a := admitOpen(t, s, "a", "goal", true)
	driveReturn(t, s, a.Run.ID)
	first, err := s.History(context.Background(), HistoryCursor{}, 1, 64, 8, HistoryLine{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Older.Pos == 0 {
		t.Fatal("fixture has no continuation")
	}
	s.histories.mu.Lock()
	session := s.histories.entries[first.Line.Session]
	session.pending = 1
	session.draining = true
	s.histories.mu.Unlock()

	if _, err := s.admitContinuation(first.Line.Session); !errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("admission returned %v", err)
	}
	s.histories.mu.Lock()
	pending := session.pending
	s.histories.mu.Unlock()
	if pending != 1 {
		t.Fatalf("rejected admission changed pending to %d", pending)
	}
	s.histories.mu.Lock()
	session.expires = time.Now().Add(time.Minute)
	s.histories.mu.Unlock()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
