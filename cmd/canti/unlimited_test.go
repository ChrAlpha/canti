package main

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
)

func TestRunUnlimitedSteps(t *testing.T) {
	s := runSpec(t, "-max-steps", "0")
	run, initial, err := newRun("goal", "", s, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if initial.Limits.MaxSteps != 0 || run.Budget.WallTime != 30*time.Minute || initial.Limits.MaxAttempts != 3 {
		t.Fatalf("step limit changed other budgets: %+v %+v", initial, run.Budget)
	}
	if err := initial.Validate(); err != nil {
		t.Fatal(err)
	}
	if s := runSpec(t); s.limits.MaxSteps != 20 {
		t.Fatalf("default steps = %d, want 20", s.limits.MaxSteps)
	}
}

func TestUnlimitedStepsFlagHelp(t *testing.T) {
	var s spec
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	s.register(fs)
	if !strings.Contains(fs.Lookup("max-steps").Usage, "0 is unlimited") {
		t.Fatal("missing unlimited steps help")
	}
	s = runSpec(t)
	s.limits.MaxSteps = -1
	_, _, err := newRun("goal", "", s, time.Now())
	if err == nil || !strings.Contains(err.Error(), "-max-steps must be nonnegative") {
		t.Fatalf("invalid steps: %v", err)
	}
}

func TestChatUnlimitedSteps(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	_, wait := f.startWith(strings.NewReader("first\nsecond\n"), nil, "chat", "-max-steps", "0")
	if r := wait(); r.code != exitOK || r.stdout != "done\ndone\n" {
		t.Fatalf("chat: %+v", r)
	}
	if s := f.status(); s.Run.SessionSeq != 2 || s.Phase != core.PhaseReturned {
		t.Fatalf("session did not continue: %+v", s)
	}
	for _, e := range f.history() {
		if e.State.Limits.MaxSteps != 0 {
			t.Fatalf("lost unlimited budget: %+v", e.State)
		}
	}
}
