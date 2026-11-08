//go:build acceptance && linux

package acceptance

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestChatPTYThirtyTwoReturnedRoundsKeepProgressAndDescriptorsBounded(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: strings.Repeat("x", 4096) + "\nanswer-01"}})
	for i := 2; i <= 32; i++ {
		decision, err := json.Marshal(decision{Kind: "return", Payload: strings.Repeat("x", 4096) + fmt.Sprintf("\nanswer-%02d", i)})
		if err != nil {
			t.Fatal(err)
		}
		f.model.answer(1, i, reply{text: string(decision)})
	}
	p := startPTYChatWithDeadline(t, f, nil, nil, true, 120*time.Second)
	pos := p.await(t, "ready", 0)
	baseline := ptyFDCount(t, p.cmd.Process.Pid)
	for i := 1; i <= 32; i++ {
		p.send(t, fmt.Sprintf("round %02d\r", i))
		pos = p.await(t, fmt.Sprintf("answer-%02d", i), pos)
		if s := f.status(); s.Phase != "returned" || s.Run.SessionSeq != int64(i) || s.Run.Goal != fmt.Sprintf("round %02d", i) {
			t.Fatalf("round %d did not return: %+v", i, s)
		}
	}
	got := ptyFDCount(t, p.cmd.Process.Pid)
	t.Logf("process descriptors after 32 rounds: %d -> %d", baseline, got)
	if got > baseline+4 {
		t.Fatalf("process descriptors grew from %d to %d after 32 rounds", baseline, got)
	}
	if requests := f.model.prompts(1); len(requests) != 32 || !strings.Contains(requests[31], "round 32") {
		t.Fatalf("model requests after 32 rounds: %d", len(requests))
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func ptyFDCount(t *testing.T, pid int) int {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
