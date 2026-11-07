package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// TestMain runs the binary as the CLI when CANTI_TEST_MAIN=1. There
// CANTI_TEST_CRASH=before:KIND or after:KIND kills the process with SIGKILL
// right before, or right after, it commits the first transition of KIND;
// block:KIND holds that commit until its context is done, after creating the
// file CANTI_TEST_BLOCKED names. CANTI_TEST_SETUP=signal holds the setup of
// the loop, and CANTI_TEST_SETUP=checked a session input after its checks,
// after creating that file, until the process receives SIGINT or SIGTERM.
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
		if os.Getenv("CANTI_TEST_SETUP") == "checked" {
			inputChecked = func() { awaitSignal(os.Getenv("CANTI_TEST_BLOCKED")) }
		}
		os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type crashing struct {
	harness.Journal
	when, kind, marker string
}

func (c crashing) SessionRuns(ctx context.Context, sessionID string, before int64, limit int) ([]harness.Run, error) {
	return c.Journal.(interface {
		SessionRuns(context.Context, string, int64, int) ([]harness.Run, error)
	}).SessionRuns(ctx, sessionID, before, limit)
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
		{[]string{"run", "-goal", "g", "-max-steps", "-1"}, exitUsage, "", "-max-steps"},
		{[]string{"run", "-goal", "g", "-history", "8"}, exitUsage, "", "flag provided but not defined: -history"},
		{[]string{"run", "-goal", "g"}, exitUsage, "", "-api must be anthropic-messages or openai-responses"},
		{[]string{"run", "-goal", "g", "-api", "openai-completions", "-model", "m"}, exitUsage, "", "-api openai-completions is not supported"},
		{[]string{"run", "-goal", "g", "-api", "anthropic-messages"}, exitUsage, "", "-model is required"},
		{[]string{"run", "-goal", "g", "-api", "anthropic-messages", "-model", "m", "-context-bytes", "65535"}, exitUsage, "", "canti run: -context-bytes: harness: fixed context leaves no room for work and handover reserve: it needs a total of at least "},
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
	for _, args := range [][]string{append([]string{"run", "-goal", "g"}, fixtureModel...), append([]string{"run", "-session", "s", "-input", "i", "-goal", "g"}, fixtureModel...), {"resume"}} {
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

func TestRunRefusesAGoalOrConstraintsOverTheirLimit(t *testing.T) {
	l := harness.DefaultPromptLimits()
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-goal", strings.Repeat("g", l.Goal+1)}, fmt.Sprintf("-goal must be at most %d bytes", l.Goal)},
		{[]string{"-goal", "g", "-constraints", strings.Repeat("c", l.Constraints-2) + "日"}, fmt.Sprintf("-constraints must be at most %d bytes", l.Constraints)},
	} {
		r := f.cli(nil, "run", tc.args...)
		if r.code != exitUsage || !strings.Contains(r.stderr, tc.want) || !strings.Contains(r.stderr, "put a long specification in a workspace file and reference it from the goal") {
			t.Errorf("run = %d\nstderr: %s", r.code, r.stderr)
		}
	}
	if _, err := os.Stat(f.state); !os.IsNotExist(err) {
		t.Errorf("state directory after refusals: %v", err)
	}
	if len(f.model.requests()) != 0 {
		t.Error("the model was called")
	}
	goal, constraints := strings.Repeat("g", l.Goal), strings.Repeat("c", l.Constraints)
	if r := f.cli(nil, "run", "-goal", "\n"+goal+" ", "-constraints", constraints+"\t"); r.code != exitOK {
		t.Fatalf("run at the limits = %d\nstderr: %s", r.code, r.stderr)
	}
	if p := f.model.prompts(1); len(p) != 1 || !strings.Contains(p[0], "## Goal\n"+goal+"\n\n## Constraints\n"+constraints+"\n\n") {
		t.Error("the prompt does not hold the whole goal and constraints")
	}
}

func TestReadersReportMissingRuns(t *testing.T) {
	f := newFixture(t, nil)
	for _, c := range []string{"status", "history", "context", "resume", "stop"} {
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
	for _, c := range []string{"status", "history", "context", "resume", "stop", "resolve"} {
		args := []string{"-run", "run_missing"}
		if c == "resolve" {
			args = append(args, "-job")
		}
		if r := f.cli(nil, c, args...); r.code != exitError || !strings.Contains(r.stderr, `run "run_missing" not found`) {
			t.Errorf("%s -run run_missing = %d\nstderr: %s", c, r.code, r.stderr)
		}
	}
}

func TestRunsOfAnEarlierRevisionAreRefused(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if err := os.MkdirAll(f.state, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, path := t.Context(), filepath.Join(f.state, "canti.db")
	store, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := json.Marshal(f.workspace)
	legacy := harness.Run{ID: "run_legacy", Goal: "g", Created: time.Now(), Config: []byte(`{"workspace":` + string(ws) + `,"image":"debian:bookworm-slim",` +
		`"network":"none","memory":"2g","cpus":"","pids":512,"output_limit":8388608,"model":"claude-opus-5","max_tokens":16000,` +
		`"structured":true,"fallbacks":true,"history":8}`)}
	initial, err := core.NewState(legacy.ID, core.Limits{MaxSteps: 20, MaxAttempts: 3})
	if err == nil {
		err = store.CreateRun(ctx, legacy, initial)
	}
	store.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"status"}, {"history"}, {"context"}, {"context", "-raw"}, {"resume"}, {"stop"}, {"resolve", "-job"}, {"run", "-goal", "g"}} {
		if r := f.cli(nil, args[0], args[1:]...); r.code != exitError || !strings.Contains(r.stderr, "run run_legacy has an unreadable config") || !strings.Contains(r.stderr, "use a fresh -state") {
			t.Errorf("%q = %d\nstderr: %s", args, r.code, r.stderr)
		}
	}
	if store, err = sqlite.Open(ctx, path); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if last, err := store.LastRun(ctx); err != nil || last.ID != legacy.ID || len(f.model.requests()) != 0 {
		t.Errorf("latest run %s, %v after %d requests; want %s and none", last.ID, err, len(f.model.requests()), legacy.ID)
	}
	if snap, err := store.Latest(ctx, legacy.ID); err != nil || snap.Seq != 0 {
		t.Errorf("the refused run has %d transitions, %v", snap.Seq, err)
	}
}

// TestRunsOfAnotherContextVersionStayReadableAndClosable requires that a run
// whose stored config records another context version, or none, can be
// inspected and stopped, and that once it is closed the next run, alone or
// in a session, is admitted after it.
func TestRunsOfAnotherContextVersionStayReadableAndClosable(t *testing.T) {
	for _, tc := range []struct{ name, version string }{{"missing", ""}, {"zero", "0"},
		{"earlier", strconv.Itoa(contextVersion - 1)}, {"later", strconv.Itoa(contextVersion + 1)}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
			if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
				t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
			}
			frozen, steps := f.status(), f.history()
			setContextVersion(t, f.state, frozen.Run.ID, tc.version)
			if s := f.status(); s.Run.ID != frozen.Run.ID || s.Seq != frozen.Seq || s.Phase != core.PhaseRequestFrozen {
				t.Errorf("status = %+v\nwant %+v", s, frozen)
			}
			if h := f.history(); !reflect.DeepEqual(h, steps) {
				t.Errorf("history = %v, want %v", kinds(h), kinds(steps))
			}
			var report struct {
				RunID string `json:"run_id"`
			}
			if r := f.cli(nil, "context"); r.code != exitOK || json.Unmarshal([]byte(r.stdout), &report) != nil || report.RunID != frozen.Run.ID {
				t.Errorf("context = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			if r := f.cli(nil, "context", "-raw"); r.code != exitOK || !json.Valid([]byte(r.stdout)) {
				t.Errorf("context -raw = %d\nstderr: %s", r.code, r.stderr)
			}
			sent := len(f.model.requests())
			for _, args := range [][]string{{"resume"}, {"resume", "-once"}} {
				if r := f.cli(nil, args[0], args[1:]...); r.code != exitError || !refusesResume(r.stderr, frozen.Run.ID, tc.version) {
					t.Errorf("%q = %d\nstderr: %s", args, r.code, r.stderr)
				}
			}
			if h := f.history(); !reflect.DeepEqual(h, steps) || len(f.model.requests()) != sent {
				t.Errorf("refused resumes appended %v and sent %d requests", kinds(h[len(steps):]), len(f.model.requests())-sent)
			}
			if r := f.cli(nil, "run", "-goal", "next"); r.code != exitError || !strings.Contains(r.stderr, "is request_frozen: stop it first") {
				t.Errorf("run beside the open run = %d\nstderr: %s", r.code, r.stderr)
			}
			if r := f.cli(nil, "stop"); r.code != exitStopped {
				t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
			}
			if s := f.status(); s.Run.ID != frozen.Run.ID || s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
				t.Errorf("status after stop = %+v", s)
			}
			if r := f.cli(nil, "run", "-goal", "next"); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("run after stop = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			input := []string{"run", "-session", "s", "-input", "first", "-goal", "first"}
			if r := f.cli(nil, input[0], input[1:]...); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("first session input = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			first := f.status()
			setContextVersion(t, f.state, first.Run.ID, tc.version)
			if r := f.cli(nil, input[0], input[1:]...); r.code != exitOK || r.stdout != "done\n" {
				t.Errorf("retried session input = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			if r := f.cli(nil, "run", "-session", "s", "-input", "second", "-goal", "second"); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("next session input = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			if s := f.status(); s.Run.ID == first.Run.ID || s.Run.SessionID != "s" || s.Run.SessionSeq != 2 || s.Phase != core.PhaseReturned {
				t.Errorf("next session run = %+v", s)
			}
		})
	}
}

// TestHintsForAnOpenRunOfAnotherContextVersionNameStop requires that resume
// also refuses a run of another context version that saved a response or
// whose unknown outcome was resolved, and that every hint for such a run,
// after a resolution or a retried session input, names stop, which closes
// it.
func TestHintsForAnOpenRunOfAnotherContextVersionNameStop(t *testing.T) {
	t.Run("response saved", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
		if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
			t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
		}
		if r := f.cli(nil, "resume", "-once"); r.code != exitActive {
			t.Fatalf("resume -once = %d\nstderr: %s", r.code, r.stderr)
		}
		s := f.status()
		if s.Phase != core.PhaseResponseSaved {
			t.Fatalf("status = %+v", s)
		}
		setContextVersion(t, f.state, s.Run.ID, "1")
		steps, sent := f.history(), len(f.model.requests())
		if r := f.cli(nil, "resume"); r.code != exitError || !refusesResume(r.stderr, s.Run.ID, "1") {
			t.Errorf("resume = %d\nstderr: %s", r.code, r.stderr)
		}
		if h := f.history(); !reflect.DeepEqual(h, steps) || len(f.model.requests()) != sent {
			t.Errorf("the refused resume appended %v and sent %d requests", kinds(h[len(steps):]), len(f.model.requests())-sent)
		}
		if r := f.cli(nil, "stop"); r.code != exitStopped {
			t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
		}
		if r := f.cli(nil, "run", "-goal", "next"); r.code != exitOK || r.stdout != "done\n" {
			t.Errorf("run after stop = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
	})
	t.Run("resolved", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
		noDocker := []string{"PATH=" + t.TempDir()}
		input := []string{"-session", "s", "-input", "i", "-goal", "g"}
		if r := f.cli(noDocker, "run", input...); r.code != exitResolution {
			t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
		}
		id := f.status().Run.ID
		setContextVersion(t, f.state, id, "1")
		steps := f.history()
		if r := f.cli(noDocker, "resume"); r.code != exitResolution || !strings.Contains(r.stderr, "needs resolution") {
			t.Errorf("resume of the unresolved run = %d\nstderr: %s", r.code, r.stderr)
		}
		if h := f.history(); !reflect.DeepEqual(h, steps) {
			t.Errorf("resume of the unresolved run appended %v", kinds(h[len(steps):]))
		}
		stop := `stop it with "canti stop -run ` + id + `"`
		r := runCLI(t, noDocker, "resolve", "-workspace", f.workspace, "-state", f.state, "-execution", "failed_to_start")
		if r.code != exitOK || !strings.Contains(r.stderr, stop) || strings.Contains(r.stderr, "canti resume") {
			t.Fatalf("resolve = %d\nstderr: %s", r.code, r.stderr)
		}
		if r := f.cli(noDocker, "run", input...); r.code != exitActive || !strings.Contains(r.stderr, stop) || strings.Contains(r.stderr, "canti resume") {
			t.Errorf("retried session input = %d\nstderr: %s", r.code, r.stderr)
		}
		sent := len(f.model.requests())
		if r := f.cli(nil, "resume"); r.code != exitError || !refusesResume(r.stderr, id, "1") {
			t.Errorf("resume of the resolved run = %d\nstderr: %s", r.code, r.stderr)
		}
		if r := f.cli(nil, "run", "-goal", "next"); r.code != exitError || !strings.Contains(r.stderr, "is idle: stop it first") {
			t.Errorf("run beside the resolved run = %d\nstderr: %s", r.code, r.stderr)
		}
		if len(f.model.requests()) != sent {
			t.Error("a refused command called the model")
		}
		if r := f.cli(nil, "stop"); r.code != exitStopped {
			t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
		}
		if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
			t.Errorf("status after stop = %+v", s)
		}
	})
}

// TestResumeOnlyReportsAHaltedRunOfAnotherContextVersion requires that
// resume reports a run of another context version that has halted without
// building the model or the executor or publishing its step records.
func TestResumeOnlyReportsAHaltedRunOfAnotherContextVersion(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "g"); r.code != exitOK {
		t.Fatalf("run = %d\nstderr: %s", r.code, r.stderr)
	}
	id, steps, sent := f.status().Run.ID, f.history(), len(f.model.requests())
	setContextVersion(t, f.state, id, "1")
	for _, dir := range []string{"history", "jobs"} {
		if err := os.RemoveAll(filepath.Join(f.state, dir)); err != nil {
			t.Fatal(err)
		}
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Errorf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	for _, dir := range []string{"history", "jobs"} {
		if _, err := os.Stat(filepath.Join(f.state, dir)); !os.IsNotExist(err) {
			t.Errorf("resume created %s: %v", dir, err)
		}
	}
	if h := f.history(); !reflect.DeepEqual(h, steps) || len(f.model.requests()) != sent {
		t.Errorf("resume appended %v and sent %d requests", kinds(h[len(steps):]), len(f.model.requests())-sent)
	}
}

// TestRunsOfAnotherContextVersionCloseWhateverTheirContextSettings requires
// that stop and resolve, which render no request, close a run of another
// context version whose context settings this canti does not accept.
func TestRunsOfAnotherContextVersionCloseWhateverTheirContextSettings(t *testing.T) {
	foreign := func(f *fixture, id string) {
		setContextVersion(t, f.state, id, "3")
		setConfig(t, f.state, id, "handover_bytes", "16384")
		setConfig(t, f.state, id, "context_bytes", "0")
	}
	t.Run("stop", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
		if r := f.cli(nil, "run", "-once", "-goal", "g"); r.code != exitActive {
			t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
		}
		id := f.status().Run.ID
		foreign(f, id)
		if r := f.cli(nil, "stop"); r.code != exitStopped {
			t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
		}
		if s := f.status(); s.Run.ID != id || s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
			t.Errorf("status after stop = %+v", s)
		}
	})
	t.Run("resolve", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
		noDocker := []string{"PATH=" + t.TempDir()}
		if r := f.cli(noDocker, "run", "-goal", "g"); r.code != exitResolution {
			t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
		}
		id := f.status().Run.ID
		foreign(f, id)
		if r := runCLI(t, noDocker, "resolve", "-workspace", f.workspace, "-state", f.state, "-execution", "failed_to_start"); r.code != exitOK {
			t.Fatalf("resolve = %d\nstderr: %s", r.code, r.stderr)
		}
		if r := f.cli(noDocker, "stop"); r.code != exitStopped {
			t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
		}
		if s := f.status(); s.Run.ID != id || s.Phase != core.PhaseStopped {
			t.Errorf("status after stop = %+v", s)
		}
	})
}

// refusesResume reports whether stderr refuses to resume run id of the
// stored context version, as setContextVersion records it, and names stop.
func refusesResume(stderr, id, version string) bool {
	if version == "" || version == "0" {
		version = "none"
	}
	return strings.Contains(stderr, fmt.Sprintf("run %s is ", id)) &&
		strings.Contains(stderr, fmt.Sprintf(" and uses context version %s, which this canti, of context version %d, does not resume; stop it with \"canti stop -run %s\"", version, contextVersion, id))
}

// setContextVersion rewrites the stored config of run id to record context
// version, or no context version when it is empty, as a run of another
// context revision would.
func setContextVersion(t *testing.T, state, id, version string) {
	t.Helper()
	setConfig(t, state, id, "context_version", version)
}

// setConfig rewrites field of the stored config of run id to value, or
// removes it when value is empty.
func setConfig(t *testing.T, state, id, field, value string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(state, "canti.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var data []byte
	var run harness.Run
	var c map[string]json.RawMessage
	err = db.QueryRow(`SELECT run FROM runs WHERE id = ?`, id).Scan(&data)
	if err == nil {
		err = json.Unmarshal(data, &run)
	}
	if err == nil {
		err = json.Unmarshal(run.Config, &c)
	}
	if err == nil {
		delete(c, field)
		if value != "" {
			c[field] = json.RawMessage(value)
		}
		run.Config, err = json.Marshal(c)
	}
	if err == nil {
		data, err = json.Marshal(run)
	}
	if err == nil {
		_, err = db.Exec(`UPDATE runs SET run = ? WHERE id = ?`, string(data), id)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestRefusedSessionInputsTouchNothing(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		code   int
		stderr string
	}{
		{[]string{"-session", "s s", "-input", "i"}, exitUsage, "-session and -input must be"},
		{[]string{"-session", "s", "-input", "i/i"}, exitUsage, "-session and -input must be"},
		{[]string{"-session", "s", "-input", "i", "-context-bytes", "65535"}, exitUsage, "-context-bytes: "},
		{[]string{"-session", "s", "-input", "i", "-model-url", "ftp://model"}, exitError, "BaseURL"},
	} {
		f := newFixture(t, nil)
		r := f.cli(nil, "run", append([]string{"-goal", "g"}, tc.args...)...)
		if r.code != tc.code || !strings.Contains(r.stderr, tc.stderr) {
			t.Errorf("canti run %q = %d\nstderr: %s", tc.args, r.code, r.stderr)
		}
		if _, err := os.Stat(f.state); !os.IsNotExist(err) {
			t.Errorf("state directory after refusing %q: %v", tc.args, err)
		}
	}
}

func TestAStateDockerCannotMountIsRefusedBeforeItExists(t *testing.T) {
	for _, args := range [][]string{{"run", "-goal", "g"}, {"run", "-session", "s", "-input", "i", "-goal", "g"}, {"chat"}} {
		f := newFixture(t, nil)
		state := filepath.Join(t.TempDir(), "a:b", "state")
		r := f.cli(nil, args[0], slices.Concat(args[1:], []string{"-state", state})...)
		if r.code != exitError || !strings.Contains(r.stderr, "can be mounted") {
			t.Errorf("canti %q = %d\nstderr: %s", args, r.code, r.stderr)
		}
		if _, err := os.Stat(filepath.Dir(state)); !os.IsNotExist(err) {
			t.Errorf("canti %q created %s: %v", args, filepath.Dir(state), err)
		}
	}
}

func TestStateInsideTheWorkspaceIsRefused(t *testing.T) {
	for _, layout := range []struct {
		name     string
		paths    func(workspace, link string) (string, string)
		absent   bool
		existing bool
	}{
		{"inside", func(ws, _ string) (string, string) { return ws, filepath.Join(ws, "state") }, true, false},
		{"containing", func(ws, _ string) (string, string) { return ws, filepath.Dir(ws) }, false, false},
		{"inside a linked workspace", func(_, link string) (string, string) { return link, filepath.Join(link, "state") }, true, false},
		{"inside, holding a journal", func(ws, _ string) (string, string) { return ws, filepath.Join(ws, "state") }, false, true},
	} {
		for _, command := range []struct {
			name string
			args []string
		}{
			{"run", []string{"run", "-goal", "g"}},
			{"run -session", []string{"run", "-session", "s", "-input", "i", "-goal", "g"}},
			{"chat", []string{"chat"}},
		} {
			t.Run(layout.name+"/"+command.name, func(t *testing.T) {
				f := newFixture(t, nil)
				link := filepath.Join(t.TempDir(), "workspace")
				if err := os.Symlink(f.workspace, link); err != nil {
					t.Fatal(err)
				}
				workspace, state := layout.paths(f.workspace, link)
				if layout.existing {
					if err := os.Mkdir(state, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(state, "canti.db"), []byte("not a journal\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				tree := func() (files map[string]string) {
					files = map[string]string{}
					for _, dir := range []string{filepath.Dir(f.workspace), filepath.Dir(link)} {
						if err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							files[p] = info.ModTime().String()
							if info.Mode().IsRegular() {
								data, err := os.ReadFile(p)
								files[p] += string(data)
								return err
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					}
					return files
				}
				before := tree()
				r := f.cli(nil, command.args[0], slices.Concat(command.args[1:], []string{"-workspace", workspace, "-state", state})...)
				if r.code != exitError || !strings.Contains(r.stderr, "overlap") {
					t.Fatalf("%s = %d\nstderr: %s", command.name, r.code, r.stderr)
				}
				if _, err := os.Stat(state); layout.absent && !os.IsNotExist(err) {
					t.Errorf("state directory inside the workspace: %v", err)
				}
				if _, err := os.Stat(filepath.Join(state, "history")); !os.IsNotExist(err) {
					t.Errorf("history directory of the refused state: %v", err)
				}
				after := tree()
				for _, p := range slices.Sorted(maps.Keys(after)) {
					if b, ok := before[p]; !ok || b != after[p] {
						t.Errorf("the refusal created or changed %s", p)
					}
				}
				for p := range before {
					if _, ok := after[p]; !ok {
						t.Errorf("the refusal removed %s", p)
					}
				}
				if len(f.model.requests()) != 0 {
					t.Error("the model was called")
				}
			})
		}
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

func TestRunPublishesThePreviousRunFirst(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "echo one"}, 2: {Kind: core.Return, Payload: "done"}})
	noDocker := []string{"PATH=" + t.TempDir()}
	if r := f.cli(noDocker, "run", "-goal", "first"); r.code != exitResolution {
		t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(noDocker, "resolve", "-execution", "failed_to_start", "-note", "checked"); r.code != exitOK {
		t.Fatalf("resolve = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	history, first := filepath.Join(f.state, "history"), f.status().Run.ID
	want := map[string]string{
		"goal":                   "first",
		"steps/00000001/meta":    "step: 1\nexecution: failed_to_start\nresolved: true\noutput: unavailable\noutput_bytes: 0\noutput_complete: false\noutput_detail: resolved by operator\n",
		"steps/00000001/command": "echo one",
		"steps/00000001/preview": "checked",
		"result":                 "phase: returned\n\ndone",
	}
	check := func(when string) {
		t.Helper()
		for name, data := range want {
			if got, err := os.ReadFile(filepath.Join(history, first, name)); err != nil || string(got) != data {
				t.Errorf("%s: %s = %q, %v; want %q", when, name, got, err, data)
			}
		}
	}
	check("after the first run")
	for _, name := range []string{"result", "steps/00000001"} {
		if err := os.RemoveAll(filepath.Join(history, first, name)); err != nil {
			t.Fatal(err)
		}
	}
	if r := f.cli(nil, "run", "-once", "-goal", "second"); r.code != exitActive {
		t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
	}
	check("after the next run started")
	second := f.status().Run.ID
	if r := f.cli(nil, "stop"); r.code != exitStopped {
		t.Fatalf("stop = %d\nstderr: %s", r.code, r.stderr)
	}
	if data, err := os.ReadFile(filepath.Join(history, second, "result")); err != nil || string(data) != "phase: stopped\nreason: cancelled\n" {
		t.Fatalf("result of the stopped run = %q, %v", data, err)
	}
	if err := os.RemoveAll(filepath.Join(history, second)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(history, second), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	requests := len(f.model.requests())
	if r := f.cli(nil, "run", "-goal", "third"); r.code != exitError || !strings.Contains(r.stderr, "step records of run "+second) {
		t.Fatalf("run over unpublishable records = %d\nstderr: %s", r.code, r.stderr)
	}
	if s := f.status(); s.Run.ID != second || len(f.model.requests()) != requests {
		t.Errorf("latest run %s after the refusal, %d requests; want %s and %d", s.Run.ID, len(f.model.requests()), second, requests)
	}
	entries, err := os.ReadDir(history)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if err != nil || !slices.Equal(names, slices.Sorted(slices.Values([]string{first, second}))) {
		t.Errorf("history holds %q, %v; want only the two runs", names, err)
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
			cmd, wait := f.start([]string{"GOMAXPROCS=1", "CANTI_TEST_SETUP=signal", "CANTI_TEST_BLOCKED=" + blocked}, tc.command, tc.args...)
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

func TestDriveReleaseWithoutOperationKeepsQueuedSignal(t *testing.T) {
	signals := make(chan os.Signal, 2)
	a := &app{stopSignals: signals}
	_, release := a.drive(context.Background(), false)
	signals <- os.Interrupt
	signals <- syscall.SIGTERM
	done := make(chan struct{})
	go func() { release(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("release without op blocked")
	}
	if a.signal != syscall.SIGTERM {
		t.Fatalf("released signal = %v, want SIGTERM", a.signal)
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
			if r.code != exitError || !strings.Contains(r.stderr, `; finish with "canti stop -run`) || !strings.Contains(r.stderr, "with a longer -cleanup") || strings.Contains(r.stderr, "deadline exceeded") {
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

func TestAThirdSignalEndsTheProcess(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fillPipe(t, w)
	cmd, wait := f.startIO(nil, w, nil, "run", "-goal", "g")
	w.Close()
	waitFor(t, func() bool { r := f.cli(nil, "status"); return strings.Contains(r.stdout, `"phase":"returned"`) })
	for range 3 {
		time.Sleep(200 * time.Millisecond)
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	go io.Copy(io.Discard, r)
	if res := wait(); res.code != -1 {
		t.Fatalf("run after three signals: %+v", res)
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
	for _, args := range [][]string{{"status"}, {"history"}, {"context"}, {"context", "-raw"}, {"stop"}} {
		if r := runCLI(t, nil, append(args, "-workspace", other, "-state", f.state)...); r.code != exitError || !strings.Contains(r.stderr, "belongs to workspace") || r.stdout != "" {
			t.Errorf("%q from another workspace = %d\nstdout: %s\nstderr: %s", args, r.code, r.stdout, r.stderr)
		}
	}
	if r := f.cli(nil, "run", "-goal", "g", "-workspace", other); r.code != exitError || !strings.Contains(r.stderr, "belongs to workspace") {
		t.Errorf("run from another workspace = %d\nstderr: %s", r.code, r.stderr)
	}
}

func TestAStopWaitsForTheResolutionOfAnUnknownOutcome(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
	noDocker := []string{"PATH=" + t.TempDir()}
	if r := f.cli(noDocker, "run", "-goal", "g"); r.code != exitResolution || !strings.Contains(r.stderr, "leaves a job that may still run untouched") {
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

func TestTheContextBytesOfTheRunBoundEveryPrompt(t *testing.T) {
	const steps = 6
	for _, tc := range []struct {
		args  []string
		shown bool
	}{{[]string{"-context-bytes", "65536"}, false}, {nil, true}} {
		t.Run(fmt.Sprint(tc.args), func(t *testing.T) {
			script := map[int]core.Decision{steps + 1: {Kind: core.Return, Payload: "done"}}
			for i := 1; i <= steps; i++ {
				script[i] = core.Decision{Kind: core.Exec, Payload: "true"}
			}
			f := newFixture(t, script)
			noDocker := []string{"PATH=" + t.TempDir()}
			args := append([]string{"-goal", strings.Repeat("g", 32<<10), "-constraints", strings.Repeat("c", 16<<10)}, tc.args...)
			r := f.cli(noDocker, "run", args...)
			if !tc.shown {
				if r.code == exitOK || len(f.model.requests()) != 0 || !strings.Contains(r.stderr, "handover") {
					t.Fatalf("oversized fixed context should fail before calling model: %d, %s", r.code, r.stderr)
				}
				return
			}
			if r.code != exitResolution {
				t.Fatalf("run without docker = %d\nstderr: %s", r.code, r.stderr)
			}
			for i := 1; i <= steps; i++ {
				if r := f.cli(noDocker, "resolve", "-execution", "failed_to_start", "-note", strings.Repeat("n", 4000)); r.code != exitOK {
					t.Fatalf("resolve %d = %d\nstderr: %s", i, r.code, r.stderr)
				}
				if i < steps {
					if r := f.cli(noDocker, "resume"); r.code != exitResolution {
						t.Fatalf("resume %d without docker = %d\nstderr: %s", i, r.code, r.stderr)
					}
				}
			}
			if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
			}
			p := f.model.prompts(steps + 1)
			if len(p) != 1 || strings.Contains(p[0], "\n### Step 1\n") != tc.shown {
				t.Fatalf("last prompt shows step 1 %v, want %v:\n%s", strings.Contains(p[0], "\n### Step 1\n"), tc.shown, p)
			}
		})
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

// runSpec is parseSpec with the model API and model of a fixture, which
// every run names.
func runSpec(t *testing.T, args ...string) spec {
	t.Helper()
	return parseSpec(t, append(slices.Clone(fixtureModel), args...)...)
}

func TestRunFlagsDefaults(t *testing.T) {
	s := parseSpec(t)
	want := spec{
		config: config{Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512, OutputLimit: 8 << 20,
			MaxTokens: 16000, Structured: true, ContextBytes: 256 << 10, ContextVersion: contextVersion, HandoverBytes: 2048},
		budget: harness.Budget{WallTime: 30 * time.Minute, OutputBytes: 256 << 20, JobTimeout: 10 * time.Minute},
		limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
	}
	if s != want {
		t.Errorf("defaults = %+v\nwant %+v", s, want)
	}
	l := harness.DefaultPromptLimits()
	l.Total = 256 << 10
	if got := promptLimits(s.config); got != l {
		t.Errorf("prompt limits = %+v\nwant %+v", got, l)
	}
}

func TestRunFlagsPersistInTheRun(t *testing.T) {
	s := parseSpec(t, "-image", "alpine:3.23", "-network", "bridge", "-memory", "512m", "-cpus", "1.5", "-pids", "64",
		"-output-limit", "4096", "-api", "anthropic-messages", "-model", "claude-test", "-max-tokens", "900", "-effort", "medium", "-structured=false", "-fallbacks=false",
		"-context-bytes", "98304", "-max-steps", "7", "-max-attempts", "2", "-wall-time", "1h", "-token-budget", "5000",
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
	if !reflect.DeepEqual(x, docker.Config{Image: "alpine:3.23", Workspace: "/work", State: "/state", Network: "bridge", Memory: "512m",
		CPUs: "1.5", PidsLimit: 64, OutputLimit: 4096, PreviewLimit: harness.DefaultPromptLimits().Preview, Mounts: []docker.Mount{{Source: "/state/history", Target: "/canti/runs"}}}) {
		t.Errorf("executor config = %+v", x)
	}
	if l := promptLimits(c); l.Total != 98304 || l.Goal != harness.DefaultPromptLimits().Goal {
		t.Errorf("prompt limits = %+v", l)
	}
	m := anthropicConfig(c, "http://model.test", credential{anthropicToken: "token"})
	if m != (anthropic.Config{BaseURL: "http://model.test", AuthToken: "token", Model: "claude-test", MaxTokens: 900, Effort: "medium"}) {
		t.Errorf("model config = %+v", m)
	}
	env := environment(c, run.Budget)
	for _, want := range []string{"alpine:3.23", docker.WorkDir, "bridge", "512m", "1.5", "64", "1m30s"} {
		if !strings.Contains(env, want) {
			t.Errorf("environment %q lacks %q", env, want)
		}
	}
	if strings.Contains(env, archive.ArtifactsRoot) {
		t.Errorf("environment %q names the artifact directory, which the prompt states", env)
	}
	if _, _, err := newRun("goal", "", runSpec(t, "-job-timeout", "-1s"), now); err == nil {
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
