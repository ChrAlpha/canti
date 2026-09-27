package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
)

func TestChatContinuesOneSessionAcrossInputs(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second answer"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"third answer"}`})
	_, wait := f.startWith(strings.NewReader("first question\n\n  \nsecond question\n"), nil, "chat", "-constraints", "Be brief")
	r := wait()
	if r.code != exitOK || r.stdout != "first answer\nsecond answer\n" {
		t.Fatalf("chat: %+v", r)
	}
	s := f.status()
	if s.Run.SessionSeq != 2 || s.Run.Goal != "second question" || s.Run.Constraints != "Be brief" || !strings.Contains(r.stderr, s.Run.SessionID) {
		t.Fatalf("session: %+v\nstderr: %s", s.Run, r.stderr)
	}
	if strings.Contains(r.stderr, "level=INFO") {
		t.Fatalf("info logs in chat: %s", r.stderr)
	}
	if p := f.model.prompts(1); len(p) != 2 || !strings.Contains(p[1], "first question") || !strings.Contains(p[1], "first answer") {
		t.Fatalf("second prompt lacks the dialogue: %q", p)
	}
	_, wait = f.startWith(strings.NewReader("third question"), nil, "chat", "-session", s.Run.SessionID)
	if r := wait(); r.code != exitOK || r.stdout != "third answer\n" {
		t.Fatalf("continued chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 3 || s.Run.Constraints != "Be brief" || !strings.Contains(f.model.prompts(1)[2], "second question") {
		t.Fatalf("continued session: %+v", s.Run)
	}
}

func TestChatRefusedInputKeepsPrompting(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "busy", "-once"); r.code != exitActive {
		t.Fatalf("busy: %+v", r)
	}
	busy := f.status().Run.ID
	_, wait := f.startWith(strings.NewReader("next\nagain\n"), nil, "chat")
	r := wait()
	if r.code != exitOK || r.stdout != "" || strings.Count(r.stderr, "resume or stop it first") != 2 {
		t.Fatalf("chat: %+v", r)
	}
	if f.status().Run.ID != busy || len(f.model.requests()) != 0 {
		t.Fatal("refused input created a run or called the model")
	}
	_, wait = f.startWith(strings.NewReader("never read\n"), nil, "chat", "-max-steps", "0")
	if r := wait(); r.code != exitUsage || strings.Contains(r.stderr, "> ") {
		t.Fatalf("invalid flags: %+v", r)
	}
}

func TestChatInterruptStopsTurnAndKeepsPrompting(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
	held := f.model.holdRequests()
	in, w := io.Pipe()
	cmd, wait := f.startWith(in, nil, "chat")
	io.WriteString(w, "slow\n")
	select {
	case <-held:
	case <-time.After(time.Minute):
		t.Fatal("no model request")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.status().Phase == core.PhaseStopped })
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	io.WriteString(w, "again\n")
	w.Close()
	r := wait()
	if r.code != exitOK || r.stdout != "answer\n" || !strings.Contains(r.stderr, "stopped: cancelled") {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "again" || s.Phase != core.PhaseReturned {
		t.Fatalf("after interrupt: %+v", s)
	}
}

func TestExcerpt(t *testing.T) {
	long := strings.Repeat("x", 300)
	for _, tc := range []struct {
		in   string
		tail bool
		want []string
	}{
		{"a\nb\n", false, []string{"a", "b"}},
		{"a\x1b[2Jb\r\tc", false, []string{"a[2Jb\tc"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", false, []string{"1", "2", "3", "4", "5", "6", "7", "8", "… 2 more lines"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", true, []string{"… 2 more lines", "3", "4", "5", "6", "7", "8", "9", "10"}},
		{long, false, []string{long[:200] + "…"}},
		{"é" + long, false, []string{"é" + long[:198] + "…"}},
		{"", false, nil},
	} {
		if got := excerpt(tc.in, tc.tail); strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("excerpt(%q, %v) = %q, want %q", tc.in, tc.tail, got, tc.want)
		}
	}
}
