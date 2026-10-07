package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

func TestSessionDialogueAndInputRetry(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "Option one: async. Option two: sync."}})
	args := []string{"-session", "design", "-input", "first", "-goal", "Explain both options", "-constraints", "No new infrastructure"}
	if r := f.cli(nil, "run", args...); r.code != exitOK {
		t.Fatalf("first: %+v", r)
	}
	first := f.status().Run.ID
	if r := f.cli(nil, "run", args...); r.code != exitOK || len(f.model.requests()) != 1 {
		t.Fatalf("retry: %+v, calls=%d", r, len(f.model.requests()))
	}
	if r := f.cli(nil, "run", "-session", "design", "-input", "first", "-goal", "different"); r.code != exitError {
		t.Fatalf("conflict: %+v", r)
	}
	if r := f.cli(nil, "run", "-session", "design", "-input", "second", "-goal", "Use the second option"); r.code != exitOK {
		t.Fatalf("second: %+v", r)
	}
	s := f.status()
	if s.Run.ID == first || s.Step != 1 || s.Totals.Steps != 0 || s.Run.Constraints != "No new infrastructure" {
		t.Fatalf("second run: %+v", s)
	}
	p := f.model.prompts(1)[1]
	for _, want := range []string{"Explain both options", "Option two: sync.", "Use the second option", first} {
		if !strings.Contains(p, want) {
			t.Errorf("missing %q in %s", want, p)
		}
	}
	if r := f.cli(nil, "run", args...); r.code != exitOK || len(f.model.requests()) != 2 {
		t.Fatalf("old retry after newer run: %+v", r)
	}
}

func TestSessionCrashReplayAndFrozenRetry(t *testing.T) {
	t.Run("before admission", func(t *testing.T) {
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
		marker := filepath.Join(t.TempDir(), "before-submit")
		args := []string{"-session", "s", "-input", "one", "-goal", "question"}
		cmd, wait := f.start([]string{"CANTI_TEST_SETUP=signal", "CANTI_TEST_BLOCKED=" + marker}, "run", args...)
		waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if r := wait(); r.code != -1 {
			t.Fatalf("crash: %+v", r)
		}
		if r := f.cli(nil, "status"); r.code != exitError || !strings.Contains(r.stderr, "no runs") {
			t.Fatalf("orphan before admission: %+v", r)
		}
		if r := f.cli(nil, "run", args...); r.code != exitOK {
			t.Fatalf("retry: %+v", r)
		}
		if f.status().Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
			t.Fatal("pre-admission crash persisted input or duplicated execution")
		}
	})
	for _, boundary := range []string{"before:request_frozen", "after:request_frozen", "after:response_loaded"} {
		t.Run(boundary, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
			args := []string{"-session", "s", "-input", "one", "-goal", "question"}
			if r := f.cli([]string{"CANTI_TEST_CRASH=" + boundary}, "run", args...); r.code != -1 {
				t.Fatalf("crash: %+v", r)
			}
			s := f.status()
			calls := len(f.model.requests())
			r := f.cli(nil, "run", args...)
			if len(f.model.requests()) != calls || f.status().Seq != s.Seq {
				t.Fatalf("submission replay executed: %+v", r)
			}
			if boundary == "after:response_loaded" {
				if r.code != exitOK || r.stdout != "answer\n" {
					t.Fatalf("lost output: %+v", r)
				}
			} else if r.code != exitActive {
				t.Fatalf("pending replay: %+v", r)
			}
			if r := f.cli(nil, "resume"); r.code != exitOK {
				t.Fatalf("resume: %+v", r)
			}
			if f.status().Run.ID != s.Run.ID || len(f.model.requests()) != 1 {
				t.Fatal("crash duplicated run or execution")
			}
		})
	}
	t.Run("transport", func(t *testing.T) {
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "previous answer"}})
		if r := f.cli(nil, "run", "-session", "s", "-input", "one", "-goal", "previous question"); r.code != exitOK {
			t.Fatal(r)
		}
		f.model.answer(1, 2, reply{status: 503, body: "temporarily unavailable"})
		if r := f.cli(nil, "run", "-session", "s", "-input", "two", "-goal", "continue"); r.code != exitOK {
			t.Fatal(r)
		}
		reqs := f.model.requests()
		if len(reqs) != 3 || !bytes.Equal(reqs[1], reqs[2]) {
			t.Fatal("transport retry changed frozen dialogue request")
		}
	})
}

func TestSessionGrowthRemainsBoundedAndIsolated(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: strings.Repeat("answer ", 300)}})
	for i := 0; i < 40; i++ {
		if r := f.cli(nil, "run", "-session", "growing", "-input", fmt.Sprint(i), "-goal", fmt.Sprintf("question-%02d %s", i, strings.Repeat("x", 1000)), "-context-bytes", "98304"); r.code != exitOK {
			t.Fatalf("round %d: %+v", i, r)
		}
		s := f.status()
		if s.Run.SessionSeq != int64(i+1) || s.Run.SessionID != "growing" {
			t.Fatalf("round %d: %+v", i, s.Run)
		}
	}
	ps := f.model.prompts(1)
	if len(ps) != 40 || !strings.Contains(ps[39], "question-38") || strings.Contains(ps[39], "question-00") {
		t.Fatal("history projection did not remain bounded")
	}
	for i, p := range ps {
		if len(p) > 98304 {
			t.Fatalf("round %d prompt %d bytes", i, len(p))
		}
	}
	if r := f.cli(nil, "run", "-session", "separate", "-input", "one", "-goal", "unrelated"); r.code != exitOK {
		t.Fatal(r)
	}
	if strings.Contains(f.model.prompts(1)[40], "question-") {
		t.Fatal("another session's dialogue leaked")
	}
}

func TestSessionActiveRejectsNewInputWithoutQueue(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	args := []string{"-session", "s", "-input", "one", "-goal", "first", "-once"}
	if r := f.cli(nil, "run", args...); r.code != exitActive {
		t.Fatalf("first: %+v", r)
	}
	first := f.status().Run.ID
	if r := f.cli(nil, "run", args...); r.code != exitActive || f.status().Seq != 1 {
		t.Fatalf("retry advanced execution: %+v", r)
	}
	for _, session := range []string{"s", "other"} {
		if r := f.cli(nil, "run", "-session", session, "-input", "two", "-goal", "next"); r.code != exitError {
			t.Fatalf("accepted busy: %+v", r)
		}
	}
	if f.status().Run.ID != first {
		t.Fatal("rejected input created a run")
	}
	if r := f.cli(nil, "resume"); r.code != exitOK {
		t.Fatalf("resume: %+v", r)
	}
	if r := f.cli(nil, "run", "-session", "s", "-input", "two", "-goal", "next", "-constraints", ""); r.code != exitOK {
		t.Fatalf("next: %+v", r)
	}
}

func TestSessionUnknownEffectCannotBeBypassed(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}, 2: {Kind: core.Return, Payload: "recovered"}})
	if r := f.cli([]string{"PATH=" + t.TempDir()}, "run", "-session", "s", "-input", "one", "-goal", "act"); r.code != exitResolution {
		t.Fatalf("unknown: %+v", r)
	}
	first := f.status()
	history := f.history()
	for _, phase := range []string{"needs_resolution", "unknown_rejected"} {
		if r := f.cli(nil, "run", "-session", "other", "-input", "two", "-goal", "next"); r.code != exitError {
			t.Fatalf("%s admitted: %+v", phase, r)
		}
		if r := f.cli(nil, "run", "-goal", "standalone next", "-once"); r.code != exitError {
			t.Fatalf("%s bypassed through standalone run: %+v", phase, r)
		}
		if r := f.cli(nil, "resolve", "-execution", "unknown", "-note", "not yet known"); r.code != exitUsage || !strings.Contains(r.stderr, "needs_resolution") {
			t.Fatalf("unknown resolution: %+v", r)
		}
		if !reflect.DeepEqual(f.status(), first) || !reflect.DeepEqual(f.history(), history) || len(f.model.requests()) != 1 {
			t.Fatal("refused inputs or unknown resolution changed committed state or executed")
		}
	}
	if r := f.cli(nil, "resolve", "-execution", "failed_to_start", "-note", "Docker was unavailable; verified no job started"); r.code != exitOK {
		t.Fatalf("known resolution: %+v", r)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "recovered\n" {
		t.Fatalf("recovery: %+v", r)
	}
	if len(f.model.prompts(1)) != 1 || len(f.model.prompts(2)) != 1 {
		t.Fatal("old effect decision was replayed")
	}
	for i, args := range [][]string{
		{"-session", "s", "-input", "two", "-goal", "next"},
		{"-goal", "standalone next"},
	} {
		f.model.answer(1, i+2, reply{text: `{"kind":"return","payload":"next accepted"}`})
		if r := f.cli(nil, "run", args...); r.code != exitOK || r.stdout != "next accepted\n" {
			t.Fatalf("next input: %+v", r)
		}
		if f.status().Run.ID == first.Run.ID {
			t.Fatal("new input revived original run")
		}
	}
}

func TestSessionRetryReportsTheAcceptedInput(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	first := []string{"-session", "s", "-input", "first", "-goal", strings.Repeat("g", 8000), "-context-bytes", "80000"}
	second := []string{"-session", "s", "-input", "second", "-goal", "g", "-constraints", strings.Repeat("c", 16000), "-context-bytes", "120000"}
	for _, args := range [][]string{first, second} {
		if r := f.cli(nil, "run", args...); r.code != exitOK {
			t.Fatalf("%q: %+v", args[3], r)
		}
	}
	for _, retry := range []struct {
		name string
		env  []string
		args []string
	}{
		{"beside grown constraints", nil, first},
		{"without a credential", []string{"ANTHROPIC_API_KEY="}, second},
		{"with an invalid model URL", nil, append(slices.Clone(second), "-model-url", "ftp://model")},
	} {
		if r := f.cli(retry.env, "run", retry.args...); r.code != exitOK || r.stdout != "done\n" || len(f.model.requests()) != 2 {
			t.Errorf("retry %s: %+v, calls=%d", retry.name, r, len(f.model.requests()))
		}
	}
}

func TestSessionRetryReportsAnInputAcceptedDuringItsChecks(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	args := []string{"-session", "s", "-input", "i", "-goal", "g"}
	checked := filepath.Join(t.TempDir(), "checked")
	cmd, wait := f.start([]string{"ANTHROPIC_API_KEY=", "CANTI_TEST_SETUP=checked", "CANTI_TEST_BLOCKED=" + checked}, "run", args...)
	waitFor(t, func() bool { _, err := os.Stat(checked); return err == nil })
	if r := f.cli(nil, "run", args...); r.code != exitOK {
		t.Fatalf("accepted input: %+v", r)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if r := wait(); r.code != exitOK || r.stdout != "done\n" || len(f.model.requests()) != 1 {
		t.Fatalf("retry checked before the input was accepted: %+v, calls=%d", r, len(f.model.requests()))
	}
}

func TestSessionInputInitializesAnEmptyJournal(t *testing.T) {
	for _, chat := range []bool{false, true} {
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
		if err := os.Mkdir(f.state, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(f.state, "canti.db"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		var r result
		if chat {
			_, wait := f.startWith(strings.NewReader("g\n"), nil, "chat")
			r = wait()
		} else {
			r = f.cli(nil, "run", "-session", "s", "-input", "i", "-goal", "g")
		}
		if r.code != exitOK || r.stdout != "done\n" {
			t.Errorf("chat=%v over an empty journal: %+v", chat, r)
		}
	}
}
