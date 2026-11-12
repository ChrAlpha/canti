//go:build integration

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestSteerDuringAJobIsSeenAtTheNextDecision(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo started > started; sleep 1"},
		2: {Kind: core.Return, Payload: "after steer"},
	})
	f.autoHost()
	if r := f.cli(nil, "run", "-host", "auto", "-input", "one", "-goal", "first", "-once", "-image", image); r.code != exitActive {
		t.Fatalf("freeze: %+v", r)
	}
	done := make(chan result, 1)
	go func() { done <- f.cli(nil, "resume", "-host", "auto") }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case r := <-done:
			t.Fatalf("the effect step ended before the job started: %+v status=%+v", r, f.status())
		default:
		}
		if _, err := os.Stat(filepath.Join(f.workspace, "started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job never started: status=%+v", f.status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-input", "steer", "-goal", "check the job", "-delivery", "steer"); r.code != exitOK {
		t.Fatalf("steer while the job runs: %+v", r)
	}
	if r := <-done; r.code != exitOK || r.stdout != "after steer\n" {
		t.Fatalf("the steered decision: %+v", r)
	}
	prompts := f.model.prompts(2)
	if len(prompts) != 1 || !containsAll(prompts[0], "Instruction #1 from the principal", "check the job") {
		t.Fatalf("the next request did not render the bound instruction: %q", prompts)
	}
	if s := f.status(); s.Inputs < 1 || s.Seen != s.Inputs {
		t.Fatalf("the steer was not seen: %+v", s)
	}
}

func TestCancelEffectContinuesAnEphemeralRun(t *testing.T) {
	f, image := dockerFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo started > started; sleep 60"},
		2: {Kind: core.Return, Payload: "after cancel"},
	})
	f.autoHost()
	done := make(chan result, 1)
	go func() { done <- f.cli(nil, "run", "-host", "auto", "-input", "one", "-goal", "job", "-image", image) }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(f.workspace, "started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the job never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r := f.cli(nil, "cancel", "-host", "auto"); r.code != exitOK {
		t.Fatalf("cancel: %+v", r)
	}
	if r := <-done; r.code != exitOK || r.stdout != "after cancel\n" {
		t.Fatalf("the run did not continue: %+v", r)
	}
	s := f.status()
	if s.Phase != core.PhaseReturned || s.Reason != "" {
		t.Fatalf("cancel stopped the run: %+v", s)
	}
	cancelled := false
	for _, e := range f.history("-run", s.Run.ID) {
		if e.Kind == "observed" && e.Facts.Observation != nil && e.Facts.Observation.Execution == core.ExecutionCancelled {
			cancelled = true
		}
	}
	if !cancelled {
		t.Fatal("no cancelled observation")
	}
}

func containsAll(s string, wants ...string) bool {
	for _, want := range wants {
		if !strings.Contains(s, want) {
			return false
		}
	}
	return true
}
