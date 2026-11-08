//go:build integration

package main

import (
	"cmp"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

// dockerFixture is a fixture whose runs use the image CANTI_TEST_IMAGE
// (default alpine:3.23); the containers of its state directory are removed
// when the test ends.
func dockerFixture(t *testing.T, script map[int]core.Decision) (*fixture, string) {
	t.Helper()
	f := newFixture(t, script)
	t.Cleanup(func() { removeContainers(t, f.state) })
	return f, cmp.Or(os.Getenv("CANTI_TEST_IMAGE"), "alpine:3.23")
}

func removeContainers(t *testing.T, state string) {
	state, err := filepath.EvalSymlinks(state)
	if os.IsNotExist(err) {
		return
	}
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=canti.state="+state).Output()
	if err != nil {
		t.Errorf("list containers: %v", err)
		return
	}
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		if out, err := exec.Command("docker", append([]string{"rm", "-f"}, ids...)...).CombinedOutput(); err != nil {
			t.Errorf("remove containers: %v: %s", err, out)
		}
	}
}

var greeting = map[int]core.Decision{
	1: {Kind: core.Exec, Payload: "echo hello > greeting.txt; cat greeting.txt"},
	2: {Kind: core.Exec, Payload: "cat greeting.txt"},
	3: {Kind: core.Return, Payload: "done"},
}

func checkGreeting(t *testing.T, f *fixture, r result) {
	t.Helper()
	if r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("exit %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	each := []string{"request_frozen", "attempt_started", "response_saved", "response_loaded", "observed"}
	want := slices.Concat(each, each, each[:4])
	h := f.history()
	if !slices.Equal(kinds(h), want) || h[len(h)-1].To != core.PhaseReturned || h[len(h)-1].State.Decision.Payload != "done" {
		t.Errorf("history = %q", kinds(h))
	}
	for step := 1; step <= 3; step++ {
		if n := len(f.model.prompts(step)); n != 1 {
			t.Errorf("step %d was requested %d times", step, n)
		}
	}
	for step, output := range map[int]string{2: "hello", 3: "hello"} {
		if p := f.model.prompts(step); len(p) == 0 || !strings.Contains(p[0], "Preview:\n```\n"+output+"\n```") {
			t.Errorf("step %d prompt lacks the output %q: %q", step, output, p)
		}
	}
	if s := f.status(); s.Phase != core.PhaseReturned || s.Step != 3 || s.Totals.Steps != 2 || s.Totals.OutputBytes != 12 {
		t.Errorf("status = %+v", s)
	}
	if data, err := os.ReadFile(filepath.Join(f.workspace, "greeting.txt")); err != nil || string(data) != "hello\n" {
		t.Errorf("greeting.txt = %q, %v", data, err)
	}
	checkReleased(t, f, h)
}

func TestRunDrivesExecAndReturn(t *testing.T) {
	f, image := dockerFixture(t, greeting)
	checkGreeting(t, f, f.cli(nil, "run", "-goal", "greet", "-image", image))
}

func TestRunOnceStepsAcrossProcesses(t *testing.T) {
	f, image := dockerFixture(t, greeting)
	r := f.cli(nil, "run", "-once", "-goal", "greet", "-image", image)
	for n := 1; r.code == exitActive; n++ {
		if n > 30 {
			t.Fatalf("still active after %d processes", n)
		}
		r = f.cli(nil, "resume", "-once")
	}
	checkGreeting(t, f, r)
}
