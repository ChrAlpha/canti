package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func TestChatContinuesOneSessionAcrossInputs(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second\u001b]0;t\u0007 ‮answer\r\n"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"third answer"}`})
	_, wait := f.startWith(strings.NewReader("first question\n\n  \n\"\"\"\nsecond\n  question\n\"\"\"\n"), nil, "chat", "-constraints", "Be brief")
	r := wait()
	if r.code != exitOK || r.stdout != "first answer\nsecond]0;t answer\n\n" {
		t.Fatalf("chat: %+v", r)
	}
	s := f.status()
	if s.Run.SessionSeq != 2 || s.Run.Goal != "second\n  question" || s.Run.Constraints != "Be brief" || !strings.Contains(r.stderr, s.Run.SessionID) {
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
	if s := f.status(); s.Run.SessionSeq != 3 || s.Run.Constraints != "Be brief" || !strings.Contains(f.model.prompts(1)[2], "second\n  question") {
		t.Fatalf("continued session: %+v", s.Run)
	}
}

func TestChatShowsBuiltinSteps(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"look"}`},
		2: {Kind: core.Return, Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("look around\n"), []string{"PATH=" + t.TempDir()}, "chat")
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "$ canti:retrieve {\"operation\":\"list\",\"path\":\"/canti/runs\",\"reason\":\"look\"}\n") || !strings.Contains(r.stderr, "\n└ exit 0\n") {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatReportsBadInputAndExitsWhenBlocked(t *testing.T) {
	l := harness.DefaultPromptLimits()
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	_, wait := f.startWith(strings.NewReader("\xff\n"+strings.Repeat("g", l.Goal+1)+"\nok\n"), nil, "chat")
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "input and -constraints must be valid UTF-8") || !strings.Contains(r.stderr, "input must be at most") {
		t.Fatalf("chat: %+v", r)
	}
	if r := f.cli(nil, "run", "-goal", "busy", "-once"); r.code != exitActive {
		t.Fatalf("busy: %+v", r)
	}
	busy, calls := f.status().Run.ID, len(f.model.requests())
	_, wait = f.startWith(strings.NewReader("next\nnever\n"), nil, "chat")
	if r := wait(); r.code != exitError || r.stdout != "" || strings.Count(r.stderr, "resume or stop it first") != 1 {
		t.Fatalf("blocked chat: %+v", r)
	}
	if f.status().Run.ID != busy || len(f.model.requests()) != calls {
		t.Fatal("refused input created a run or called the model")
	}
	for _, args := range [][]string{{"-max-steps", "0"}, {"-session", "my session"}, {"-model-url", "http://example.com"}, {"-constraints", "\xff"}} {
		_, wait = f.startWith(strings.NewReader("never\n"), nil, "chat", args...)
		if r := wait(); r.code != exitUsage && r.code != exitError || strings.Contains(r.stderr, "> ") {
			t.Fatalf("chat %q: %+v", args, r)
		}
	}
}

func TestChatExitsWhenATurnNeedsResolution(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
	_, wait := f.startWith(strings.NewReader("act\nnever\n"), []string{"PATH=" + t.TempDir()}, "chat")
	if r := wait(); r.code != exitResolution || !strings.Contains(r.stderr, "needs resolution") {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 1 || s.Phase != core.PhaseNeedsResolution {
		t.Fatalf("after resolution exit: %+v", s)
	}
}

func TestChatSignals(t *testing.T) {
	for _, tc := range []struct {
		name     string
		terminal bool
		signal   os.Signal
		code     int
	}{
		{"interrupt from a pipe", false, os.Interrupt, exitStopped},
		{"terminate on a terminal", true, syscall.SIGTERM, exitStopped},
		{"interrupt on a terminal", true, os.Interrupt, exitOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
			held := f.model.holdRequests()
			var env []string
			if tc.terminal {
				env = []string{"CANTI_TEST_TERMINAL=1"}
			}
			in, w := io.Pipe()
			cmd, wait := f.startWith(in, env, "chat")
			io.WriteString(w, "slow\n")
			waitFor(t, func() bool { return len(held) > 0 })
			if err := cmd.Process.Signal(tc.signal); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return f.status().Phase == core.PhaseStopped })
			f.model.mu.Lock()
			f.model.hold = nil
			f.model.mu.Unlock()
			go func() { io.WriteString(w, "again\n"); w.Close() }()
			r := wait()
			in.Close()
			if r.code != tc.code || !strings.Contains(r.stderr, "stopped: cancelled") {
				t.Fatalf("chat: %+v", r)
			}
			s := f.status()
			if tc.code == exitOK {
				if r.stdout != "answer\n" || s.Run.SessionSeq != 2 || s.Run.Goal != "again" || s.Phase != core.PhaseReturned {
					t.Fatalf("after interrupt: %+v, %+v", r, s)
				}
			} else if r.stdout != "" || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("chat continued after the signal: %+v, %+v", r, s)
			}
		})
	}
	t.Run("interrupt at the prompt", func(t *testing.T) {
		f := newFixture(t, nil)
		in, w := io.Pipe()
		cmd, wait := f.startWith(in, []string{"CANTI_TEST_TERMINAL=1"}, "chat")
		waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.state, "history")); return err == nil })
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		w.Close()
		if r := wait(); r.code != -1 {
			t.Fatalf("chat: %+v", r)
		}
		if r := f.cli(nil, "status"); r.code != exitError || !strings.Contains(r.stderr, "no runs") {
			t.Fatalf("status: %+v", r)
		}
	})
}

func TestExcerpt(t *testing.T) {
	long := strings.Repeat("x", 300)
	for _, tc := range []struct {
		in   string
		tail bool
		want []string
	}{
		{"a\nb\n", false, []string{"a", "b"}},
		{"a\x1b[2Jb\u0085‮\tc", false, []string{"a[2Jb\tc"}},
		{"10%\r20%\rdone\r\nnext\r\n", false, []string{"done", "next"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", false, []string{"1", "2", "3", "4", "5", "6", "7", "8", "…"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", true, []string{"…", "3", "4", "5", "6", "7", "8", "9", "10"}},
		{long, false, []string{long[:200] + "…"}},
		{"é" + long, false, []string{"é" + long[:198] + "…"}},
		{"\xff", false, []string{"�"}},
		{"", false, nil},
	} {
		if got := excerpt(tc.in, tc.tail); strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("excerpt(%q, %v) = %q, want %q", tc.in, tc.tail, got, tc.want)
		}
	}
}
