//go:build acceptance

package acceptance

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestChatContinuesOneSessionAcrossInputs(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second\u001b]0;t\u0007 ‮answer\r\n"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"third answer"}`})
	block := "second\ndef f():\n    \"\"\"doc\"\"\"\n\"\"\"\nEN\n END\nquestion"
	block += "\n\tEND"
	_, wait := f.startWith(strings.NewReader("first question\n\n  \n<< 'END'\n"+block+"\nEND\n"), nil, tuiCommand, "-constraints", "Be brief")
	r := wait()
	if r.code != exitOK || r.stdout != "first answer\nsecond answer\n\n" {
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
	if p := f.model.prompts(1); len(p) != 2 || !strings.Contains(p[1], "Previous run: /canti/runs/") || !strings.Contains(p[1], block) {
		t.Fatalf("second prompt lacks current task or prior navigation: %q", p)
	}
	_, wait = f.startWith(strings.NewReader("third question"), nil, tuiCommand, "-session", s.Run.SessionID)
	if r := wait(); r.code != exitOK || r.stdout != "third answer\n" {
		t.Fatalf("continued chat: %+v", r)
	}
	if current := f.status(); current.Run.SessionSeq != 3 || current.Run.Constraints != "Be brief" || !strings.Contains(f.model.prompts(1)[2], "Previous run: /canti/runs/"+s.Run.ID+"/") {
		t.Fatalf("continued session: %+v", s.Run)
	}
}

func TestChatShowsBuiltinSteps(t *testing.T) {
	f := newFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"look"}`},
		2: {Kind: "return", Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("look around\n"), []string{"PATH=" + t.TempDir()}, tuiCommand)
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "$ canti:retrieve {\"operation\":\"list\",\"path\":\"/canti/runs\",\"reason\":\"look\"}\n") || !strings.Contains(r.stderr, "\n│ retrieved 0 sources and 1 directory entries\n└ done\n") || strings.Contains(r.stderr, "exit 0") {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatReportsBadInputAndExitsWhenBlocked(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "done"}})
	_, wait := f.startWith(strings.NewReader("\xff\n"+strings.Repeat("g", 32768+1)+"\n"+strings.Repeat("g", 20000)+"\n<<EOF and more\nok\n"), nil, tuiCommand, "-request-bytes", "20000")
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" || !strings.Contains(r.stderr, "input and -constraints must be valid UTF-8") || !strings.Contains(r.stderr, "input must be at most") ||
		!strings.Contains(r.stderr, "canti-tui: the input does not fit -request-bytes (") || !strings.Contains(r.stderr, "start a block with <<WORD") || !strings.Contains(r.stderr, "put an input that starts with << in a block") {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 1 {
		t.Fatalf("refused inputs were submitted: %+v", s.Run)
	}
	_, wait = f.startWith(strings.NewReader("<<END\nhalf of it\nEN"), nil, tuiCommand)
	if r := wait(); r.code != exitError || !strings.Contains(r.stderr, "input ended inside <<END; it was not submitted") {
		t.Fatalf("unterminated: %+v", r)
	}
	_, wait = f.startWith(strings.NewReader("<<what now"), nil, tuiCommand)
	if r := wait(); r.code != exitOK || !strings.Contains(r.stderr, "start a block with <<WORD") {
		t.Fatalf("opener at end of input: %+v", r)
	}
	if s := f.status(); s.Run.Goal != "ok" {
		t.Fatalf("unterminated input was submitted: %+v", s.Run)
	}
	_, wait = f.startWith(strings.NewReader("<<-EOF\n\tindented\n\tEOF\n"), nil, tuiCommand)
	if r := wait(); r.code != exitOK || f.status().Run.Goal != "indented" {
		t.Fatalf("tab-stripped block: %+v, %q", r, f.status().Run.Goal)
	}
	if r := f.cli(nil, "run", "-goal", "busy", "-once"); r.code != exitActive {
		t.Fatalf("busy: %+v", r)
	}
	busy, calls := f.status().Run.ID, len(f.model.requests())
	_, wait = f.startWith(strings.NewReader("next\nnever\n"), nil, tuiCommand)
	if r := wait(); r.code != exitError || r.stdout != "" || strings.Count(r.stderr, "resume or stop it first") != 1 {
		t.Fatalf("blocked chat: %+v", r)
	}
	if f.status().Run.ID != busy || len(f.model.requests()) != calls {
		t.Fatal("refused input created a run or called the model")
	}
	if r := f.cli(nil, "resume"); r.code != exitOK {
		t.Fatalf("resume: %+v", r)
	}
	_, wait = f.startWith(strings.NewReader("with long rules\n"), nil, tuiCommand, "-session", "long", "-constraints", strings.Repeat("c", 16000))
	if r := wait(); r.code != exitOK {
		t.Fatalf("long constraints: %+v", r)
	}
	if err := os.Chmod(filepath.Join(f.state, "canti.db"), 0); err != nil {
		t.Fatal(err)
	}
	_, wait = f.startWith(strings.NewReader("never\n"), nil, tuiCommand, "-session", "long")
	r = wait()
	os.Chmod(filepath.Join(f.state, "canti.db"), 0o600)
	if r.code != exitError || strings.Contains(r.stderr, "> ") {
		t.Fatalf("unreadable journal: %+v", r)
	}
	for _, args := range [][]string{{"-max-steps", "-1"}, {"-session", "my session"}, {"-model-url", "http://example.com"}, {"-constraints", "\xff"}, {"-session", "long", "-request-bytes", "12000"}} {
		_, wait = f.startWith(strings.NewReader("never\n"), nil, tuiCommand, args...)
		if r := wait(); r.code != exitUsage && r.code != exitError || strings.Contains(r.stderr, "> ") {
			t.Fatalf("chat %q: %+v", args, r)
		}
	}
}

func TestChatExitsWhenItsFixedContextStopsFitting(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "done"}})
	imports := t.TempDir()
	in, w := io.Pipe()
	_, wait := f.startWith(in, nil, tuiCommand, "-history-imports", imports)
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
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "done"}})
	in, w := io.Pipe()
	_, wait := f.startWith(in, nil, tuiCommand, "-session", "shared", "-request-bytes", "12000")
	io.WriteString(w, "one\n")
	waitFor(t, func() bool { r := f.cli(nil, "status"); return strings.Contains(r.stdout, `"phase":"returned"`) })
	// The terminal transition precedes closing the journal and releasing the
	// workspace lock. Retry only a busy admission, with the same input identity.
	waitFor(t, func() bool {
		r := f.cli(nil, "run", "-session", "shared", "-input", "other", "-goal", "g", "-constraints", strings.Repeat("c", 16000))
		if r.code == exitLocked {
			return false
		}
		if r.code != exitOK {
			t.Fatalf("other process: %+v", r)
		}
		return true
	})
	go func() { io.WriteString(w, "two\nthree\n"); w.Close() }()
	r := wait()
	in.Close()
	if r.code != exitUsage || strings.Contains(r.stderr, "does not fit") || f.status().Run.SessionSeq != 2 {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatExitsWhenATurnNeedsResolution(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "exec", Payload: "true"}})
	_, wait := f.startWith(strings.NewReader("act\nnever\n"), []string{"PATH=" + t.TempDir()}, tuiCommand)
	if r := wait(); r.code != exitResolution || !strings.Contains(r.stderr, "needs resolution") {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 1 || s.Phase != "needs_resolution" {
		t.Fatalf("after resolution exit: %+v", s)
	}
}

func TestChatSignals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		signal os.Signal
	}{{"interrupt from a pipe", os.Interrupt}, {"terminate from a pipe", syscall.SIGTERM}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "answer"}})
			held := f.model.holdRequests()
			in, w := io.Pipe()
			cmd, wait := f.startWith(in, nil, tuiCommand)
			io.WriteString(w, "slow\n")
			waitFor(t, func() bool { return len(held) > 0 })
			if err := cmd.Process.Signal(tc.signal); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return f.status().Phase == "stopped" })
			f.model.mu.Lock()
			f.model.hold = nil
			f.model.mu.Unlock()
			go func() { io.WriteString(w, "again\n"); w.Close() }()
			r := wait()
			in.Close()
			if r.code != exitStopped || !strings.Contains(r.stderr, "stopped: cancelled") || r.stdout != "" || f.status().Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("chat continued after the signal: %+v, %+v", r, f.status())
			}
		})
	}
	for name, signals := range map[string][]os.Signal{
		"terminate while the answer is written":               {syscall.SIGTERM},
		"interrupt and terminate while the answer is written": {os.Interrupt, syscall.SIGTERM},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "answer"}})
			if err := os.MkdirAll(f.state, 0o700); err != nil {
				t.Fatal(err)
			}
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			fillPipe(t, w)
			cmd, wait := f.startIO(strings.NewReader("one\ntwo\n"), w, nil, tuiCommand)
			w.Close()
			f.awaitReturned()
			for _, s := range signals {
				if err := cmd.Process.Signal(s); err != nil {
					t.Fatal(err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if res := wait(); res.code != exitError || !strings.Contains(res.stderr, "i/o timeout") || f.status().Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("chat continued after the signal: %+v", res)
			}
		})
	}
	t.Run("interrupt at the prompt", func(t *testing.T) {
		f := newFixture(t, nil)
		in, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer in.Close()
		defer w.Close()
		cmd, wait := f.startWith(in, nil, tuiCommand)
		waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.state, "history")); return err == nil })
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		if r := wait(); r.code != exitStopped {
			t.Fatalf("chat: %+v", r)
		}
		if r := f.cli(nil, "status"); r.code != exitError || !strings.Contains(r.stderr, "no runs") {
			t.Fatalf("status: %+v", r)
		}
	})
}

func TestChatKeepsTheDialogueAcrossInterrupts(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "last answer"}})
	f.model.answer(1, 1, reply{text: `{"kind":"return","payload":"first answer"}`})
	_, wait := f.startWith(strings.NewReader("first request\n"), nil, tuiCommand, "-session", "s")
	if r := wait(); r.code != exitOK || r.stdout != "first answer\n" {
		t.Fatalf("first chat: %+v", r)
	}
	held := f.model.holdRequests()
	for i, input := range []string{"interrupted request\n", "interrupted again\n"} {
		in, w := io.Pipe()
		cmd, wait := f.startWith(in, nil, tuiCommand, "-session", "s")
		io.WriteString(w, input)
		waitFor(t, func() bool { return len(held) > 0 })
		<-held
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { s := f.status(); return s.Run.SessionSeq == int64(i+2) && s.Phase == "stopped" })
		w.Close()
		if r := wait(); r.code != exitStopped || !strings.Contains(r.stderr, "stopped: cancelled") {
			t.Fatalf("interrupted chat %d: %+v", i+1, r)
		}
		in.Close()
	}
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	_, wait = f.startWith(strings.NewReader("last request\n"), nil, tuiCommand, "-session", "s")
	if r := wait(); r.code != exitOK || r.stdout != "last answer\n" {
		t.Fatalf("continued chat: %+v", r)
	}
	p := f.model.prompts(1)
	if len(p) != 4 {
		t.Fatalf("%d step 1 requests", len(p))
	}
	c := p[3]
	if !strings.Contains(c, "## Goal\nlast request\n") || !strings.Contains(c, "Previous run: /canti/runs/") {
		t.Fatalf("last input lost task/navigation: %s", c)
	}
	// Resolve the session chain through published records after interrupted runs.
	id := f.status().Run.ID
	for i, want := range []string{"last request", "interrupted again", "interrupted request", "first request"} {
		root := filepath.Join(f.state, "history", id)
		checkFile(t, filepath.Join(root, "goal"), want)
		body, err := os.ReadFile(filepath.Join(root, "session"))
		if err != nil {
			t.Fatal(err)
		}
		var meta struct {
			Previous string `json:"previous_run_id"`
		}
		if err := json.Unmarshal(body, &meta); err != nil {
			t.Fatal(err)
		}
		if i == 1 || i == 2 {
			result, err := os.ReadFile(filepath.Join(root, "result"))
			if err != nil || !strings.Contains(string(result), "reason: cancelled") {
				t.Fatalf("lost cancellation evidence: %s, %v", result, err)
			}
		}
		id = meta.Previous
	}
	if id != "" {
		t.Fatal("session chain did not end at first run")
	}

}
