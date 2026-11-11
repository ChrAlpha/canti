//go:build acceptance && integration

package acceptance

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatShowsCommandsAndTheirOutput(t *testing.T) {
	f, image := dockerFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: "printf 'hello\\033[2J\\nworld\\n'\nseq 20\n:\n:\n:\n:\n:\n:\nexit 3"},
		2: {Kind: "return", Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, tuiCommand, "-image", image)
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
	f, image := dockerFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: "sleep 60"},
		2: {Kind: "return", Payload: "gave up"},
	})
	_, wait := f.startWith(strings.NewReader("wait\n"), nil, tuiCommand, "-image", image, "-job-timeout", "2s")
	r := wait()
	if r.code != exitOK || r.stdout != "gave up\n" || !strings.Contains(r.stderr, "$ sleep 60\n└ timed_out") || strings.Contains(r.stderr, "exit 1") {
		t.Fatalf("chat: %+v", r)
	}
}

func TestChatShowsAllOfACommandWithCarriageReturns(t *testing.T) {
	f, image := dockerFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: "touch hidden # \r echo shown"},
		2: {Kind: "return", Payload: "done"},
	})
	_, wait := f.startWith(strings.NewReader("go\n"), nil, tuiCommand, "-image", image)
	r := wait()
	if r.code != exitOK || !strings.Contains(r.stderr, "$ (quoted) \"touch hidden # \\r echo shown\"\n└ exit 0\n") {
		t.Fatalf("chat: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.workspace, "hidden")); err != nil {
		t.Fatal(err)
	}
}

func TestChatKeepsTheDialogueAfterInterruptingACommand(t *testing.T) {
	f, image := recoveryFixture(t, map[int]decision{1: {Kind: "return", Payload: "third answer"}})
	f.model.answer(1, 1, reply{text: `{"kind":"return","payload":"first answer"}`})
	f.model.answer(1, 2, reply{text: `{"kind":"exec","payload":"echo started > started; sleep 60"}`})
	in, w := io.Pipe()
	cmd, wait := f.startWith(in, nil, tuiCommand, "-image", image)
	io.WriteString(w, "first request\n")
	waitFor(t, func() bool { return strings.Contains(f.cli(nil, "status").stdout, `"phase":"returned"`) })
	io.WriteString(w, "interrupted request\n")
	waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.workspace, "started")); return err == nil })
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.status().Phase == "stopped" })
	s := f.status()
	w.Close()
	r := wait()
	in.Close()
	if r.code != exitStopped || r.stdout != "first answer\n" || !strings.Contains(r.stderr, "$ echo started > started; sleep 60\n") || !strings.Contains(r.stderr, "\n└ cancelled (") ||
		!strings.Contains(r.stderr, "stopped: cancelled") {
		t.Fatalf("chat: %+v", r)
	}
	_, wait = f.startWith(strings.NewReader("third request\n"), nil, tuiCommand, "-image", image)
	if r := wait(); r.code != exitOK || r.stdout != "third answer\n" {
		t.Fatalf("continued chat: %+v", r)
	}
	if s.Reason != "cancelled" || s.Totals.Steps != 1 || f.status().Run.Ordinal != 3 {
		t.Fatalf("interrupted run: %+v", s)
	}
	p := f.model.prompts(1)
	if len(p) != 3 {
		t.Fatalf("%d step 1 requests", len(p))
	}
	c := p[2]
	current := f.status().Run
	checkLineLink(t, f, c, current.ID, s.Run.ID, "third request")
	if strings.Contains(c, "sleep 60") {
		t.Fatal("prior command replayed inline")
	}
	first := archivePrevious(t, f, s.Run.ID)
	if first == "" || archivePrevious(t, f, first) != "" {
		t.Fatalf("previous run chain: %q", first)
	}
	for path, want := range map[string]string{
		first + "/goal": "first request", first + "/result": "phase: returned\n\nfirst answer",
		s.Run.ID + "/goal": "interrupted request", s.Run.ID + "/result": "phase: stopped\nreason: cancelled\n",
	} {
		if got := archiveText(t, f, "/canti/runs/"+path); got != want {
			t.Fatalf("archive %s=%q, want %q", path, got, want)
		}
	}

	checkReleased(t, f, f.history("-run", s.Run.ID))
}
