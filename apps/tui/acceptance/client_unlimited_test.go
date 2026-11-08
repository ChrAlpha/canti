//go:build acceptance

package acceptance

import (
	"strings"
	"testing"
)

func TestChatUnlimitedSteps(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "done"}})
	_, wait := f.startWith(strings.NewReader("first\nsecond\n"), nil, tuiCommand, "-max-steps", "0")
	if r := wait(); r.code != exitOK || r.stdout != "done\ndone\n" {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 2 || s.Phase != "returned" {
		t.Fatalf("session did not continue: %+v", s)
	}
	for _, e := range f.history() {
		if e.State.Limits.MaxSteps != 0 {
			t.Fatalf("lost unlimited budget: %+v", e.State)
		}
	}
}
