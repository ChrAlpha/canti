package main

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/host"
)

func TestRunUnlimitedSteps(t *testing.T) {
	s := runSpec(t, "-max-steps", "0")
	run, initial, err := host.NewRun("goal", "", s, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if initial.Limits.MaxSteps != 0 || run.Budget.WallTime != 30*time.Minute || initial.Limits.MaxAttempts != 3 {
		t.Fatalf("step limit changed other budgets: %+v %+v", initial, run.Budget)
	}
	if err := initial.Validate(); err != nil {
		t.Fatal(err)
	}
	if s := runSpec(t); s.Limits.MaxSteps != 20 {
		t.Fatalf("default steps = %d, want 20", s.Limits.MaxSteps)
	}
}

func TestUnlimitedStepsFlagHelp(t *testing.T) {
	var s host.Spec
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	registerSpec(fs, &s)
	if !strings.Contains(fs.Lookup("max-steps").Usage, "0 is unlimited") {
		t.Fatal("missing unlimited steps help")
	}
	s = runSpec(t)
	s.Limits.MaxSteps = -1
	_, _, err := host.NewRun("goal", "", s, time.Now())
	if err == nil || !strings.Contains(err.Error(), "-max-steps must be nonnegative") {
		t.Fatalf("invalid steps: %v", err)
	}
}
