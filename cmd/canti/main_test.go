package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/docker"
)

// TestMain runs the binary as the CLI when CANTI_TEST_MAIN=1. There
// CANTI_TEST_CRASH=before:KIND or after:KIND kills the process with SIGKILL
// right before, or right after, it commits the first transition of KIND;
// block:KIND holds that commit until its context is done, after creating the
// file CANTI_TEST_BLOCKED names. CANTI_TEST_SETUP=signal holds the setup of
// the loop, after creating that file, until the process receives SIGINT or
// SIGTERM.
func TestMain(m *testing.M) {
	if os.Getenv("CANTI_TEST_MAIN") == "1" {
		if spec := os.Getenv("CANTI_TEST_CRASH"); spec != "" {
			when, kind, _ := strings.Cut(spec, ":")
			if when != "before" && when != "after" && when != "block" {
				panic("CANTI_TEST_CRASH must be before:KIND, after:KIND, or block:KIND")
			}
			wrapJournal = func(j harness.Journal) harness.Journal {
				return crashing{j, when, kind, os.Getenv("CANTI_TEST_BLOCKED")}
			}
		}
		if os.Getenv("CANTI_TEST_SETUP") == "signal" {
			wrap := wrapJournal
			wrapJournal = func(j harness.Journal) harness.Journal {
				awaitSignal(os.Getenv("CANTI_TEST_BLOCKED"))
				return wrap(j)
			}
		}
		os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type crashing struct {
	harness.Journal
	when, kind, marker string
}

func (c crashing) Commit(ctx context.Context, t harness.Transition) error {
	switch {
	case t.Event.Kind() != c.kind:
		return c.Journal.Commit(ctx, t)
	case c.when == "block":
		os.WriteFile(c.marker, nil, 0o600)
		<-ctx.Done()
		return ctx.Err()
	case c.when == "before":
		kill()
	}
	err := c.Journal.Commit(ctx, t)
	if err == nil {
		kill()
	}
	return err
}

// awaitSignal creates marker and returns once SIGINT or SIGTERM has been
// delivered to every channel notified of it.
func awaitSignal(marker string) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	os.WriteFile(marker, nil, 0o600)
	<-sig
	signal.Stop(sig)
}

func kill() {
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func TestUsageAndVersion(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "")
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	for _, tc := range []struct {
		args           []string
		code           int
		stdout, stderr string
	}{
		{nil, exitUsage, "", "Usage:"},
		{[]string{"version"}, exitOK, "dev\n", ""},
		{[]string{"-version"}, exitOK, "dev\n", ""},
		{[]string{"-h"}, exitOK, "Usage:", ""},
		{[]string{"help"}, exitOK, "Usage:", ""},
		{[]string{"launch"}, exitUsage, "", `unknown command "launch"`},
		{[]string{"run", "-h"}, exitOK, "-goal", ""},
		{[]string{"status", "-bogus"}, exitUsage, "", "flag provided but not defined"},
		{[]string{"status", "extra"}, exitUsage, "", `unexpected argument "extra"`},
		{[]string{"status", "-log-level", "loud"}, exitUsage, "", "loud"},
		{[]string{"run"}, exitUsage, "", "-goal is required"},
		{[]string{"run", "-goal", "g", "-max-steps", "0"}, exitUsage, "", "-max-steps"},
		{[]string{"run", "-goal", "g", "-history", "0"}, exitUsage, "", "-history"},
		{[]string{"run", "-goal", "g", "-max-tokens", "0"}, exitUsage, "", "-max-tokens"},
		{[]string{"run", "-goal", "g", "-network", ""}, exitUsage, "", "-network"},
		{[]string{"stop", "-cleanup", "0s"}, exitUsage, "", "-cleanup"},
		{[]string{"resolve"}, exitUsage, "", "exactly one of -job and -execution"},
		{[]string{"resolve", "-job", "-execution", "cancelled"}, exitUsage, "", "exactly one of -job and -execution"},
		{[]string{"resolve", "-job", "-note", "n"}, exitUsage, "", "-job"},
		{[]string{"resolve", "-execution", "lost"}, exitUsage, "", "-execution must be one of"},
		{[]string{"resolve", "-execution", "exited"}, exitUsage, "", "-exit-code"},
		{[]string{"resolve", "-execution", "cancelled", "-exit-code", "0"}, exitUsage, "", "-exit-code"},
	} {
		var stdout, stderr bytes.Buffer
		code := cli(t.Context(), tc.args, &stdout, &stderr)
		out, errOut := stdout.String(), stderr.String()
		if code != tc.code || !strings.Contains(out, tc.stdout) || !strings.Contains(errOut, tc.stderr) ||
			(tc.stdout == "" && out != "") || (tc.code != exitUsage && errOut != "") {
			t.Errorf("canti %q = %d\nstdout: %s\nstderr: %s", tc.args, code, out, errOut)
		}
	}
}

func TestCommandsWithoutACredentialTouchNothing(t *testing.T) {
	f := newFixture(t, nil)
	for _, args := range [][]string{{"run", "-goal", "g"}, {"resume"}} {
		r := runCLI(t, nil, append(args, "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL)...)
		if r.code != exitError || !strings.Contains(r.stderr, "ANTHROPIC_API_KEY") {
			t.Errorf("canti %q = %d\nstderr: %s", args, r.code, r.stderr)
		}
	}
	if _, err := os.Stat(f.state); !os.IsNotExist(err) {
		t.Errorf("state directory after refusals: %v", err)
	}
	if len(f.model.requests()) != 0 {
		t.Error("the model was called")
	}
}

func TestReadersReportMissingRuns(t *testing.T) {
	f := newFixture(t, nil)
	for _, c := range []string{"status", "history", "resume", "stop"} {
		if r := f.cli(nil, c); r.code != exitError || !strings.Contains(r.stderr, "no runs in workspace") {
			t.Errorf("%s = %d\nstderr: %s", c, r.code, r.stderr)
		}
	}
	if _, err := os.Stat(f.state); !os.IsNotExist(err) {
		t.Errorf("state directory after reads: %v", err)
	}
	if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
		t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
	}
	for _, c := range []string{"status", "history", "resume", "stop", "resolve"} {
		args := []string{"-run", "run_missing"}
		if c == "resolve" {
			args = append(args, "-job")
		}
		if r := f.cli(nil, c, args...); r.code != exitError || !strings.Contains(r.stderr, `run "run_missing" not found`) {
			t.Errorf("%s -run run_missing = %d\nstderr: %s", c, r.code, r.stderr)
		}
	}
}

func TestStateInsideTheWorkspaceIsRefused(t *testing.T) {
	f := newFixture(t, nil)
	state := filepath.Join(f.workspace, "state")
	r := f.cli(nil, "run", "-goal", "g", "-state", state)
	if r.code != exitError || !strings.Contains(r.stderr, "overlap") {
		t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
	}
	if _, err := os.Stat(filepath.Join(state, "canti.db")); !os.IsNotExist(err) {
		t.Errorf("journal inside the workspace: %v", err)
	}
}

func TestWritersRefuseALockedWorkspace(t *testing.T) {
	f := newFixture(t, nil)
	if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
		t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
	}
	lock, err := os.OpenFile(filepath.Join(f.state, "lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"run", "-goal", "g"}, {"resume"}, {"stop"}, {"resolve", "-job"}} {
		if r := f.cli(nil, args[0], args[1:]...); r.code != exitLocked || !strings.Contains(r.stderr, f.workspace) {
			t.Errorf("%q = %d\nstderr: %s", args, r.code, r.stderr)
		}
	}
	if s := f.status(); s.Phase != core.PhaseRequestFrozen || len(f.history()) != 1 {
		t.Errorf("status while locked: %+v", s)
	}
}

func TestRunReturnsWithoutExecuting(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "nothing to do"}})
	r := f.cli([]string{"ANTHROPIC_BASE_URL=" + f.model.URL}, "run", "-goal", "say hi", "-constraints", "be brief", "-model-url", "")
	if r.code != exitOK || r.stdout != "nothing to do\n" {
		t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	h := f.history()
	want := []string{"request_frozen", "attempt_started", "response_saved", "response_loaded"}
	if !slices.Equal(kinds(h), want) || h[3].To != core.PhaseReturned || h[0].From != core.PhaseIdle || h[0].Facts.Request == nil {
		t.Fatalf("history = %q\n%+v", kinds(h), h)
	}
	s := f.status()
	if s.Run.Goal != "say hi" || s.Run.Constraints != "be brief" || s.Phase != core.PhaseReturned || s.Intent != core.IntentHalt ||
		s.Seq != 4 || s.Decision == nil || s.Decision.Payload != "nothing to do" || s.Totals.InputTokens != 100 ||
		s.Run.Budget.WallTime != "30m0s" || s.Run.Budget.OutputBytes != 256<<20 || !strings.HasPrefix(s.Run.ID, "run_") {
		t.Errorf("status = %+v", s)
	}
	if !strings.Contains(r.stderr, s.Run.ID) {
		t.Errorf("stderr does not name the run: %s", r.stderr)
	}
	r = f.cli(nil, "resume")
	if r.code != exitOK || r.stdout != "nothing to do\n" || len(f.model.requests()) != 1 {
		t.Errorf("resume of a returned run = %d, %d requests\nstdout: %s", r.code, len(f.model.requests()), r.stdout)
	}
}

func TestRunRefusesWhileTheLatestRunIsOpen(t *testing.T) {
	f := newFixture(t, nil)
	if r := f.cli(nil, "run", "-once", "-goal", "first"); r.code != exitActive || !strings.Contains(r.stderr, "resume") {
		t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
	}
	first := f.status().Run.ID
	if r := f.cli(nil, "run", "-goal", "second"); r.code != exitError || !strings.Contains(r.stderr, "resume or stop") {
		t.Fatalf("run over an open run = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resolve", "-job"); r.code != exitError || !strings.Contains(r.stderr, "no unknown outcome") {
		t.Errorf("resolve = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "stop"); r.code != exitStopped || !strings.Contains(r.stderr, "cancelled") {
		t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "run", "-once", "-goal", "second"); r.code != exitActive {
		t.Fatalf("run after stop = %d\nstderr: %s", r.code, r.stderr)
	}
	if s := f.status(); s.Run.Goal != "second" || s.Run.ID == first {
		t.Errorf("latest run = %+v", s.Run)
	}
	if s := f.status("-run", first); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
		t.Errorf("first run = %+v", s)
	}
	if h := f.history("-run", first); !slices.Equal(kinds(h), []string{"request_frozen", "stop_requested"}) {
		t.Errorf("first run history = %q", kinds(h))
	}
	if len(f.model.requests()) != 0 {
		t.Error("the model was called")
	}
}

func TestACrashAtACommitResumesFromTheJournal(t *testing.T) {
	for _, tc := range []struct {
		crash         string
		before, after []string
		requests      int
	}{
		{"after:response_saved", []string{"request_frozen", "attempt_started", "response_saved"},
			[]string{"request_frozen", "attempt_started", "response_saved", "response_loaded"}, 1},
		{"before:response_saved", []string{"request_frozen", "attempt_started"},
			[]string{"request_frozen", "attempt_started", "attempt_started", "response_saved", "response_loaded"}, 2},
	} {
		t.Run(tc.crash, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
			if r := f.cli([]string{"CANTI_TEST_CRASH=" + tc.crash}, "run", "-goal", "g"); r.code != -1 {
				t.Fatalf("crashing run = %d\nstderr: %s", r.code, r.stderr)
			}
			if h := f.history(); !slices.Equal(kinds(h), tc.before) {
				t.Fatalf("history after the crash = %q", kinds(h))
			}
			if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			if h := f.history(); !slices.Equal(kinds(h), tc.after) || len(f.model.requests()) != tc.requests {
				t.Errorf("history = %q after %d requests", kinds(h), len(f.model.requests()))
			}
		})
	}
}

func TestASignalStopsTheRunPersistently(t *testing.T) {
	f := newFixture(t, nil)
	held := f.model.holdRequests()
	cmd, wait := f.start(nil, "run", "-goal", "g")
	select {
	case <-held:
	case <-time.After(time.Minute):
		t.Fatal("the model was never called")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if r := wait(); r.code != exitStopped || !strings.Contains(r.stderr, "stopped: cancelled") {
		t.Fatalf("interrupted run = %d\nstderr: %s", r.code, r.stderr)
	}
	if h := f.history(); !slices.Equal(kinds(h), []string{"request_frozen", "attempt_started", "stop_requested"}) || h[2].To != core.PhaseStopped {
		t.Errorf("history = %q", kinds(h))
	}
}

func TestASignalDuringSetupStopsTheRunPersistently(t *testing.T) {
	for _, tc := range []struct {
		command string
		args    []string
		signal  os.Signal
		kinds   []string
	}{
		{"run", []string{"-goal", "g"}, os.Interrupt, []string{"stop_requested"}},
		{"resume", nil, syscall.SIGTERM, []string{"request_frozen", "stop_requested"}},
	} {
		t.Run(tc.command, func(t *testing.T) {
			f := newFixture(t, nil)
			if tc.command == "resume" {
				if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
					t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
				}
			}
			blocked := filepath.Join(t.TempDir(), "blocked")
			cmd, wait := f.start([]string{"CANTI_TEST_SETUP=signal", "CANTI_TEST_BLOCKED=" + blocked}, tc.command, tc.args...)
			waitFor(t, func() bool { _, err := os.Stat(blocked); return err == nil })
			if err := cmd.Process.Signal(tc.signal); err != nil {
				t.Fatal(err)
			}
			if r := wait(); r.code != exitStopped || !strings.Contains(r.stderr, "stopped: cancelled") {
				t.Fatalf("%s interrupted during setup = %d\nstderr: %s", tc.command, r.code, r.stderr)
			}
			if h := f.history(); !slices.Equal(kinds(h), tc.kinds) || h[len(h)-1].To != core.PhaseStopped {
				t.Errorf("history = %q", kinds(h))
			}
			if len(f.model.requests()) != 0 {
				t.Error("the model was called")
			}
		})
	}
}

func TestASecondSignalAbandonsTheCleanup(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		t.Run(map[bool]string{false: "while unwinding", true: "while stopping"}[stopping], func(t *testing.T) {
			f := newFixture(t, nil)
			held, blocked := f.model.holdRequests(), filepath.Join(t.TempDir(), "blocked")
			cmd, wait := f.start([]string{"CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED=" + blocked}, "run", "-goal", "g")
			waitFor(t, func() bool { return len(held) > 0 })
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if stopping {
				waitFor(t, func() bool { _, err := os.Stat(blocked); return err == nil })
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			r := wait()
			if r.code != exitError || !strings.Contains(r.stderr, `; finish with "canti stop -run`) || strings.Contains(r.stderr, "deadline exceeded") {
				t.Fatalf("doubly interrupted run = %d\nstderr: %s", r.code, r.stderr)
			}
			if h := f.history(); !slices.Equal(kinds(h), []string{"request_frozen", "attempt_started"}) {
				t.Errorf("history = %q", kinds(h))
			}
			if r := f.cli(nil, "stop"); r.code != exitStopped {
				t.Errorf("stop = %d\nstderr: %s", r.code, r.stderr)
			}
		})
	}
}

func waitFor(t *testing.T, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Minute); !done(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
	}
}

func TestAnUnknownOutcomeWaitsForAnExplicitResolution(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}, 2: {Kind: core.Return, Payload: "done"}})
	noDocker := []string{"PATH=" + t.TempDir()}
	r := f.cli(noDocker, "run", "-goal", "g")
	if r.code != exitResolution || !strings.Contains(r.stderr, "canti resolve -run") {
		t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
	}
	if s := f.status(); s.Phase != core.PhaseNeedsResolution || s.EffectID == "" || s.JobID != "" {
		t.Fatalf("status = %+v", s)
	}
	if r := f.cli(noDocker, "resume"); r.code != exitResolution {
		t.Errorf("resume = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(noDocker, "run", "-goal", "again"); r.code != exitError || !strings.Contains(r.stderr, "resolve it first") {
		t.Errorf("run over an unresolved run = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(noDocker, "resolve", "-job"); r.code != exitError {
		t.Errorf("resolve -job without docker = %d\nstderr: %s", r.code, r.stderr)
	}
	r = runCLI(t, noDocker, "resolve", "-workspace", f.workspace, "-state", f.state, "-execution", "failed_to_start", "-note", "checked by hand")
	if r.code != exitOK || !strings.Contains(r.stderr, "canti resume -run") {
		t.Fatalf("resolve = %d\nstderr: %s", r.code, r.stderr)
	}
	h := f.history()
	last := h[len(h)-1]
	want := harness.Observation{Execution: core.ExecutionFailedToStart, Preview: "checked by hand", Output: harness.ArtifactRef{Detail: "resolved by operator"}}
	if last.Kind != "resolved" || last.To != core.PhaseIdle || last.Facts.Observation == nil || *last.Facts.Observation != want {
		t.Errorf("resolution = %+v", last)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Errorf("resume after the resolution = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	other := t.TempDir()
	for _, c := range []string{"status", "history", "stop"} {
		if r := runCLI(t, nil, c, "-workspace", other, "-state", f.state); r.code != exitError || !strings.Contains(r.stderr, "belongs to workspace") {
			t.Errorf("%s from another workspace = %d\nstderr: %s", c, r.code, r.stderr)
		}
	}
	if r := f.cli(nil, "run", "-goal", "g", "-workspace", other); r.code != exitError || !strings.Contains(r.stderr, "belongs to workspace") {
		t.Errorf("run from another workspace = %d\nstderr: %s", r.code, r.stderr)
	}
}

func TestAStopWaitsForTheResolutionOfAnUnknownOutcome(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
	noDocker := []string{"PATH=" + t.TempDir()}
	if r := f.cli(noDocker, "run", "-goal", "g"); r.code != exitResolution {
		t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
	}
	r := f.cli(noDocker, "stop")
	if r.code != exitResolution || !strings.Contains(r.stderr, "the stop is recorded") || !strings.Contains(r.stderr, "may still run") {
		t.Fatalf("stop of an unresolved run = %d\nstderr: %s", r.code, r.stderr)
	}
	if s := f.status(); s.Phase != core.PhaseNeedsResolution || s.Reason != core.StopCancelled {
		t.Fatalf("status = %+v", s)
	}
	f.cli(noDocker, "resolve", "-execution", "failed_to_start")
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
		t.Fatalf("status after the resolution = %+v", s)
	}
}

func TestHistoryPrintsEveryPageInOrder(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}, 2: {Kind: core.Return, Payload: "done"}})
	noDocker := []string{"PATH=" + t.TempDir()}
	if r := f.cli(noDocker, "run", "-goal", "g"); r.code != exitResolution {
		t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(noDocker, "resolve", "-execution", "failed_to_start"); r.code != exitOK {
		t.Fatalf("resolve = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK {
		t.Fatalf("resume = %d\nstderr: %s", r.code, r.stderr)
	}
	step := []string{"request_frozen", "attempt_started", "response_saved", "response_loaded"}
	h := f.history()
	if want := slices.Concat(step, []string{"outcome_unknown", "resolved"}, step); len(h) <= historyPage || !slices.Equal(kinds(h), want) {
		t.Fatalf("history = %q\nwant %q", kinds(h), want)
	}
	for i, e := range h {
		if e.Seq != int64(i+1) {
			t.Errorf("line %d has seq %d", i, e.Seq)
		}
	}
}

// racing is a journal where a commit lands right before each of the next
// races reads of the totals.
type racing struct {
	harness.Journal
	seq   int64
	races int
}

func (r *racing) Latest(context.Context, string) (harness.Snapshot, error) {
	return harness.Snapshot{Seq: r.seq}, nil
}

func (r *racing) Totals(context.Context, string) (harness.Totals, error) {
	if r.races > 0 {
		r.races--
		r.seq++
	}
	return harness.Totals{Steps: int(r.seq)}, nil
}

func TestStatusReadsTheTotalsOfItsSnapshot(t *testing.T) {
	for races, ok := range map[int]bool{0: true, 4: true, 5: false} {
		snap, totals, err := consistent(t.Context(), &racing{seq: 1, races: races}, "run_x")
		if ok && (err != nil || snap.Seq != int64(totals.Steps)) || !ok && (err == nil || !strings.Contains(err.Error(), "run_x")) {
			t.Errorf("%d races: seq %d with the totals of seq %d, %v", races, snap.Seq, totals.Steps, err)
		}
	}
}

func TestDefaultStateDirectory(t *testing.T) {
	ws := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(ws, link); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(ws))
	hash := hex.EncodeToString(sum[:])[:16]
	xdg, home := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	for _, tc := range []struct{ xdg, want string }{
		{xdg, filepath.Join(xdg, "canti", hash)},
		{"", filepath.Join(home, ".local", "state", "canti", hash)},
		{"relative", filepath.Join(home, ".local", "state", "canti", hash)},
	} {
		t.Setenv("XDG_STATE_HOME", tc.xdg)
		for _, dir := range []string{ws, link} {
			a := &app{}
			if err := a.locate(dir, ""); err != nil || a.workspace != ws || a.state != tc.want {
				t.Errorf("locate(%s) with XDG_STATE_HOME=%q = %s, %s, %v; want %s", dir, tc.xdg, a.workspace, a.state, err, tc.want)
			}
		}
	}
}

func parseSpec(t *testing.T, args ...string) spec {
	t.Helper()
	var s spec
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	s.register(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRunFlagsDefaults(t *testing.T) {
	s := parseSpec(t)
	want := spec{
		config: config{Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512, OutputLimit: 8 << 20,
			Model: "claude-opus-5", MaxTokens: 16000, Structured: true, Fallbacks: true, History: 8},
		budget: harness.Budget{WallTime: 30 * time.Minute, OutputBytes: 256 << 20, JobTimeout: 10 * time.Minute},
		limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
	}
	if s != want {
		t.Errorf("defaults = %+v\nwant %+v", s, want)
	}
}

func TestRunFlagsPersistInTheRun(t *testing.T) {
	s := parseSpec(t, "-image", "alpine:3.23", "-network", "bridge", "-memory", "512m", "-cpus", "1.5", "-pids", "64",
		"-output-limit", "4096", "-model", "claude-test", "-max-tokens", "900", "-structured=false", "-fallbacks=false",
		"-history", "3", "-max-steps", "7", "-max-attempts", "2", "-wall-time", "1h", "-token-budget", "5000",
		"-output-budget", "1000000", "-job-timeout", "90s")
	s.config.Workspace = "/work"
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	run, initial, err := newRun("goal", "constraints", s, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.ID) != 36 || !strings.HasPrefix(run.ID, "run_") || strings.Trim(run.ID[4:], "0123456789abcdef") != "" {
		t.Errorf("run ID %q", run.ID)
	}
	if want, _ := core.NewState(run.ID, core.Limits{MaxSteps: 7, MaxAttempts: 2}); initial != want {
		t.Errorf("initial state = %+v", initial)
	}
	budget := harness.Budget{WallTime: time.Hour, Tokens: 5000, OutputBytes: 1000000, JobTimeout: 90 * time.Second}
	if run.Goal != "goal" || run.Constraints != "constraints" || run.Budget != budget || !run.Created.Equal(now) {
		t.Errorf("run = %+v", run)
	}
	c, err := decodeConfig(run, "/work")
	if err != nil || c != s.config {
		t.Fatalf("stored config = %+v, %v\nwant %+v", c, err, s.config)
	}
	x := executorConfig(c, "/state")
	if x != (docker.Config{Image: "alpine:3.23", Workspace: "/work", State: "/state", Network: "bridge", Memory: "512m",
		CPUs: "1.5", PidsLimit: 64, OutputLimit: 4096}) {
		t.Errorf("executor config = %+v", x)
	}
	m := modelConfig(c, "http://model.test", credential{authToken: "token"})
	if m != (anthropic.Config{BaseURL: "http://model.test", AuthToken: "token", Model: "claude-test", MaxTokens: 900}) {
		t.Errorf("model config = %+v", m)
	}
	env := environment(c, run.Budget)
	for _, want := range []string{"alpine:3.23", docker.WorkDir, docker.ArtifactDir, "bridge", "512m", "1.5", "64", "1m30s"} {
		if !strings.Contains(env, want) {
			t.Errorf("environment %q lacks %q", env, want)
		}
	}
	if _, _, err := newRun("goal", "", parseSpec(t, "-job-timeout", "-1s"), now); err == nil {
		t.Error("a negative budget was accepted")
	}
	if _, err := decodeConfig(run, "/elsewhere"); err == nil {
		t.Error("a config of another workspace was accepted")
	}
	run.Config = []byte(`{"workspace":"/work","unknown":1}`)
	if _, err := decodeConfig(run, "/work"); err == nil {
		t.Error("an unknown config field was accepted")
	}
}

func TestCredentialsPreferTheAPIKeyAndLeaveTheEnvironment(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "key")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "token")
	if c, err := credentials(); err != nil || c != (credential{apiKey: "key"}) {
		t.Errorf("credentials = %+v, %v", c, err)
	}
	if _, err := credentials(); err == nil {
		t.Error("the credential stayed in the environment")
	}
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "token")
	if c, err := credentials(); err != nil || c != (credential{authToken: "token"}) {
		t.Errorf("credentials = %+v, %v", c, err)
	}
}
