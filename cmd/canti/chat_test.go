package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func TestChatContinuesOneSessionAcrossInputs(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second\u001b]0;t\u0007 ‮answer\r\n"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"third answer"}`})
	block := "second\ndef f():\n    \"\"\"doc\"\"\"\n\"\"\"\nEN\n END\nquestion"
	block += "\n\tEND"
	_, wait := f.startWith(strings.NewReader("first question\n\n  \n<< 'END'\n"+block+"\nEND\n"), nil, "chat", "-constraints", "Be brief")
	r := wait()
	if r.code != exitOK || r.stdout != "first answer\nsecond]0;t answer\n\n" {
		t.Fatalf("chat: %+v", r)
	}
	if strings.Count(r.stderr, "… ") != 9 {
		t.Fatalf("block prompts: %s", r.stderr)
	}
	if fi, err := os.Stat(f.state); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("state directory: %v, %v", fi.Mode(), err)
	}
	s := f.status()
	if s.Run.SessionSeq != 2 || s.Run.Goal != block || s.Run.Constraints != "Be brief" || !strings.Contains(r.stderr, s.Run.SessionID) {
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
	if s := f.status(); s.Run.SessionSeq != 3 || s.Run.Constraints != "Be brief" || !strings.Contains(f.model.prompts(1)[2], "EN\n END") {
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
	_, wait := f.startWith(strings.NewReader("\xff\n"+strings.Repeat("g", l.Goal+1)+"\n"+strings.Repeat("g", 20000)+"\n<<EOF and more\nok\n"), nil, "chat", "-context-bytes", "81600")
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "input and -constraints must be valid UTF-8") || !strings.Contains(r.stderr, "input must be at most") ||
		!strings.Contains(r.stderr, "canti chat: the input does not fit -context-bytes (harness: ") || !strings.Contains(r.stderr, "start a block with <<WORD") || !strings.Contains(r.stderr, "put an input that starts with << in a block") {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 1 {
		t.Fatalf("refused inputs were submitted: %+v", s.Run)
	}
	_, wait = f.startWith(strings.NewReader("<<END\nhalf of it\nEN"), nil, "chat")
	if r := wait(); r.code != exitError || !strings.Contains(r.stderr, "input ended inside <<END; it was not submitted") {
		t.Fatalf("unterminated: %+v", r)
	}
	_, wait = f.startWith(strings.NewReader("<<what now"), nil, "chat")
	if r := wait(); r.code != exitOK || !strings.Contains(r.stderr, "start a block with <<WORD") {
		t.Fatalf("opener at end of input: %+v", r)
	}
	if s := f.status(); s.Run.Goal != "ok" {
		t.Fatalf("unterminated input was submitted: %+v", s.Run)
	}
	_, wait = f.startWith(strings.NewReader("<<-EOF\n\tindented\n\tEOF\n"), nil, "chat")
	if r := wait(); r.code != exitOK || f.status().Run.Goal != "indented" {
		t.Fatalf("tab-stripped block: %+v, %q", r, f.status().Run.Goal)
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
	if r := f.cli(nil, "resume"); r.code != exitOK {
		t.Fatalf("resume: %+v", r)
	}
	_, wait = f.startWith(strings.NewReader("with long rules\n"), nil, "chat", "-session", "long", "-constraints", strings.Repeat("c", 16000))
	if r := wait(); r.code != exitOK {
		t.Fatalf("long constraints: %+v", r)
	}
	if err := os.Chmod(filepath.Join(f.state, "canti.db"), 0); err != nil {
		t.Fatal(err)
	}
	_, wait = f.startWith(strings.NewReader("never\n"), nil, "chat", "-session", "long")
	r = wait()
	os.Chmod(filepath.Join(f.state, "canti.db"), 0o600)
	if r.code != exitError || strings.Contains(r.stderr, "> ") {
		t.Fatalf("unreadable journal: %+v", r)
	}
	for _, args := range [][]string{{"-max-steps", "-1"}, {"-session", "my session"}, {"-model-url", "http://example.com"}, {"-constraints", "\xff"}, {"-session", "long", "-context-bytes", "71600"}} {
		_, wait = f.startWith(strings.NewReader("never\n"), nil, "chat", args...)
		if r := wait(); r.code != exitUsage && r.code != exitError || strings.Contains(r.stderr, "> ") {
			t.Fatalf("chat %q: %+v", args, r)
		}
	}
}

func TestChatExitsWhenItsFixedContextStopsFitting(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	imports := t.TempDir()
	in, w := io.Pipe()
	_, wait := f.startWith(in, nil, "chat", "-history-imports", imports)
	io.WriteString(w, "one\n")
	waitFor(t, func() bool { r := f.cli(nil, "status"); return strings.Contains(r.stdout, `"phase":"returned"`) })
	if err := os.Remove(imports); err != nil {
		t.Fatal(err)
	}
	go func() { io.WriteString(w, "two\nthree\n"); w.Close() }()
	r := wait()
	in.Close()
	if r.code != exitUsage || strings.Count(r.stderr, "history imports") != 1 || f.status().Run.SessionSeq != 1 {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatEndsWhenAnotherProcessChangesItsConstraints(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	in, w := io.Pipe()
	_, wait := f.startWith(in, nil, "chat", "-session", "shared", "-context-bytes", "71600")
	io.WriteString(w, "one\n")
	waitFor(t, func() bool { r := f.cli(nil, "status"); return strings.Contains(r.stdout, `"phase":"returned"`) })
	if r := f.cli(nil, "run", "-session", "shared", "-input", "other", "-goal", "g", "-constraints", strings.Repeat("c", 16000)); r.code != exitOK {
		t.Fatalf("other process: %+v", r)
	}
	go func() { io.WriteString(w, "two\nthree\n"); w.Close() }()
	r := wait()
	in.Close()
	if r.code != exitUsage || strings.Contains(r.stderr, "does not fit") || f.status().Run.SessionSeq != 2 {
		t.Fatalf("chat: %+v", r)
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
	for name, signals := range map[string][]os.Signal{
		"terminate while the answer is written":               {syscall.SIGTERM},
		"interrupt and terminate while the answer is written": {os.Interrupt, syscall.SIGTERM},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
			if err := os.MkdirAll(f.state, 0o700); err != nil {
				t.Fatal(err)
			}
			store, err := sqlite.Open(t.Context(), filepath.Join(f.state, "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			fillPipe(t, w)
			cmd, wait := f.startIO(strings.NewReader("one\ntwo\n"), w, []string{"CANTI_TEST_TERMINAL=1"}, "chat")
			w.Close()
			deadline := time.Now().Add(5 * time.Second)
			for {
				run, err := store.LastRun(t.Context())
				if err != nil && !errors.Is(err, harness.ErrNotFound) {
					t.Fatal(err)
				}
				if err == nil {
					snapshot, err := store.Latest(t.Context(), run.ID)
					if err != nil {
						t.Fatal(err)
					}
					if snapshot.State.Phase == core.PhaseReturned {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("first run did not return while stdout was blocked")
				}
				time.Sleep(5 * time.Millisecond)
			}
			for _, s := range signals {
				if err := cmd.Process.Signal(s); err != nil {
					t.Fatal(err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if res := wait(); res.code != exitError || !strings.Contains(res.stderr, "chat display: i/o timeout") || f.status().Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("chat continued after the signal: %+v", res)
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

func TestChatKeepsTheDialogueAcrossInterrupts(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "last answer"}})
	f.model.answer(1, 1, reply{text: `{"kind":"return","payload":"first answer"}`})
	terminal := []string{"CANTI_TEST_TERMINAL=1"}
	in, w := io.Pipe()
	cmd, wait := f.startWith(in, terminal, "chat", "-session", "s")
	io.WriteString(w, "first request\n")
	waitFor(t, func() bool { return strings.Contains(f.cli(nil, "status").stdout, `"phase":"returned"`) })
	held := f.model.holdRequests()
	for i, input := range []string{"interrupted request\n", "interrupted again\n"} {
		io.WriteString(w, input)
		waitFor(t, func() bool { return len(held) > 0 })
		<-held
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { s := f.status(); return s.Run.SessionSeq == int64(i+2) && s.Phase == core.PhaseStopped })
	}
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	w.Close()
	if r := wait(); r.code != exitOK || strings.Count(r.stderr, "stopped: cancelled") != 2 {
		t.Fatalf("interrupted chat: %+v", r)
	}
	_, wait = f.startWith(strings.NewReader("last request\n"), terminal, "chat", "-session", "s")
	if r := wait(); r.code != exitOK || r.stdout != "last answer\n" {
		t.Fatalf("continued chat: %+v", r)
	}
	p := f.model.prompts(1)
	if len(p) != 4 {
		t.Fatalf("%d step 1 requests", len(p))
	}
	c := p[3]
	first, answer, interrupted, again := strings.Index(c, "first request"), strings.Index(c, "first answer"), strings.Index(c, "interrupted request"), strings.Index(c, "interrupted again")
	if !strings.Contains(c, "## Goal\nlast request\n") || first < 0 || !(first < answer && answer < interrupted && interrupted < again) ||
		strings.Count(c, "stopped, reason cancelled, recorded steps 0; no assistant return.") != 2 {
		t.Fatalf("the input after interrupts lost the dialogue:\n%s", c)
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
		{"a\x1b[2Jb\u0085‮\tc", true, []string{"a[2Jb\tc"}},
		{"a\x1b[2Jb\u0085‮\tc\b", false, []string{`(quoted) "a\x1b[2Jb\u0085\u202e\tc\b"`}},
		{`printf 'a\x1bb'`, false, []string{`printf 'a\x1bb'`}},
		{`"printf 'a\x1bb'"`, false, []string{`"printf 'a\x1bb'"`}},
		{"printf 'a\x1bb'", false, []string{`(quoted) "printf 'a\x1bb'"`}},
		{"\b" + strings.Repeat("x", 199) + "TAIL", false, []string{`(quoted) "\b` + strings.Repeat("x", 199) + `"…`}},
		{"\b" + long, false, []string{`(quoted) "\b` + long[:199] + `"…`}},
		{"\b" + long[:199] + "TAIL", false, []string{`(quoted) "\b` + long[:199] + `"…`}},
		{"\b" + long[:198], false, []string{`(quoted) "\b` + long[:198] + `"`}},
		{"\b" + long[:199], false, []string{`(quoted) "\b` + long[:199] + `"`}},
		{"\b" + long[:198] + "é", false, []string{`(quoted) "\b` + long[:198] + `"…`}},
		{"10%\r20%\rdone\r\nnext\r\n", true, []string{"done", "next"}},
		{"rm -rf x;\recho hi\r\n", false, []string{`(quoted) "rm -rf x;\recho hi"`}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n", false, []string{"1", "2", "3", "4", "5", "6", "7", "8", "… 1 more line"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", false, []string{"1", "2", "3", "4", "5", "6", "7", "8", "… 2 more lines"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", true, []string{"…", "3", "4", "5", "6", "7", "8", "9", "10"}},
		{long, false, []string{long[:200] + "…"}},
		{"é" + long, false, []string{"é" + long[:198] + "…"}},
		{"\xff", true, []string{"�"}},
		{"\xff", false, []string{`(quoted) "\xff"`}},
		{"", false, nil},
	} {
		if got := excerpt(tc.in, tc.tail); strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("excerpt(%q, %v) = %q, want %q", tc.in, tc.tail, got, tc.want)
		}
	}
}
