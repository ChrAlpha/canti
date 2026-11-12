package sqlite

import (
	"context"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// TestEveryCommittedWriteNotifies pins the one notification point readers of
// the agent log rely on: a write that inserts an input, changes a disposition
// without advancing a position, and a later failing write.
func TestEveryCommittedWriteNotifies(t *testing.T) {
	s := open(t, tempPath(t))
	ctx := context.Background()
	var calls int
	s.SetOnCommit(func() { calls++ })
	admitOpen(t, s, "run-1", "Do the task.", true)
	if calls != 1 {
		t.Fatalf("opening admission notifications = %d, want 1", calls)
	}
	admitKind(t, s, harness.DeliveryFollowUp, "later")
	if calls != 2 {
		t.Fatalf("queued admission notifications = %d, want 2", calls)
	}
	if _, err := s.Withdraw(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("withdraw notification = %d, want 3 even though no position advanced", calls)
	}
	if _, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal}); err == nil {
		t.Fatal("invalid admission succeeded")
	}
	if calls != 3 {
		t.Fatalf("a failed write notified %d times, want 3", calls)
	}
}
