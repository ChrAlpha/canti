package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
	if r := f.cli([]string{"PATH=" + t.TempDir()}, "run", "-session", "s", "-input", "one", "-goal", "act"); r.code != exitResolution {
		t.Fatalf("unknown: %+v", r)
	}
	first := f.status().Run.ID
	for _, phase := range []string{"needs_resolution", "stopped_unknown"} {
		if r := f.cli(nil, "run", "-session", "other", "-input", "two", "-goal", "next"); r.code != exitError {
			t.Fatalf("%s admitted: %+v", phase, r)
		}
		if f.status().Run.ID != first {
			t.Fatal("rejected input persisted")
		}
		if r := f.cli(nil, "run", "-goal", "standalone next", "-once"); r.code != exitError || f.status().Run.ID != first {
			t.Fatalf("%s bypassed through standalone run: %+v", phase, r)
		}
		if phase == "needs_resolution" {
			if r := f.cli(nil, "resolve", "-execution", "unknown", "-note", "abandoned unknown"); r.code != exitStopped {
				t.Fatalf("resolve: %+v", r)
			}
		}
	}
}
