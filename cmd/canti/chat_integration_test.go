//go:build integration

package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestChatShowsCommandsAndTheirOutput(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "printf 'hello\\033[2J\\nworld\\n'\nseq 20\n:\n:\n:\n:\n:\n:\nexit 3"},
		2: {Kind: core.Return, Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, "chat", "-image", image)
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("chat: %+v", r)
	}
	want := "$ printf 'hello\\033[2J\\nworld\\n'\n  seq 20\n  :\n  :\n  :\n  :\n  :\n  :\n  … 1 more line\n│ …\n│ 13\n│ 14\n│ 15\n│ 16\n│ 17\n│ 18\n│ 19\n│ 20\n└ exit 3\n"
	if !strings.Contains(r.stderr, want) || strings.Contains(r.stderr, "\x1b") {
		t.Fatalf("stderr:\n%s", r.stderr)
	}
}

func TestChatShowsATimedOutJob(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "sleep 60"},
		2: {Kind: core.Return, Payload: "gave up"},
	})
	_, wait := f.startWith(strings.NewReader("wait\n"), nil, "chat", "-image", image, "-job-timeout", "2s")
	r := wait()
	if r.code != exitOK || r.stdout != "gave up\n" || !strings.Contains(r.stderr, "$ sleep 60\n└ timed_out") || strings.Contains(r.stderr, "exit 1") {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatShowsAllOfACommandWithCarriageReturns(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "touch hidden # \r echo shown"},
		2: {Kind: core.Return, Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, "chat", "-image", image)
	r := wait()
	if r.code != exitOK || !strings.Contains(r.stderr, "$ (quoted) \"touch hidden # \\r echo shown\"\n└ exit 0\n") {
		t.Fatalf("chat: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.workspace, "hidden")); err != nil {
		t.Fatal(err)
	}
}

func TestChatKeepsTheDialogueAfterInterruptingACommand(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "third answer"}})
	f.model.answer(1, 1, reply{text: `{"kind":"return","payload":"first answer"}`})
	f.model.answer(1, 2, reply{text: `{"kind":"exec","payload":"echo started > started; sleep 60"}`})
	in, w := io.Pipe()
	cmd, wait := f.startWith(in, []string{"CANTI_TEST_TERMINAL=1"}, "chat", "-session", "s", "-image", image)
	io.WriteString(w, "first request\n")
	waitFor(t, func() bool { return strings.Contains(f.cli(nil, "status").stdout, `"phase":"returned"`) })
	io.WriteString(w, "interrupted request\n")
	waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.workspace, "started")); return err == nil })
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.status().Phase == core.PhaseStopped })
	s := f.status()
	io.WriteString(w, "third request\n")
	w.Close()
	r := wait()
	if r.code != exitOK || r.stdout != "first answer\nthird answer\n" || !strings.Contains(r.stderr, "$ echo started > started; sleep 60\n") || !strings.Contains(r.stderr, "\n└ cancelled (") ||
		!strings.Contains(r.stderr, "stopped: cancelled") {
		t.Fatalf("chat: %+v", r)
	}
	if s.Reason != core.StopCancelled || s.Totals.Steps != 1 || f.status().Run.SessionSeq != 3 {
		t.Fatalf("interrupted run: %+v", s)
	}
	p := f.model.prompts(1)
	if len(p) != 3 {
		t.Fatalf("%d step 1 requests", len(p))
	}
	c := p[2]
	first, answer, interrupted := strings.Index(c, "first request"), strings.Index(c, "first answer"), strings.Index(c, "interrupted request")
	if first < 0 || !(first < answer && answer < interrupted) ||
		!strings.Contains(c, s.Run.ID+"/goal, steps, and result): stopped, reason cancelled, completed steps 1; no assistant return.") {
		t.Fatalf("the input after an interrupted command lost the dialogue:\n%s", c)
	}
	checkReleased(t, f, f.history("-run", s.Run.ID))
}
