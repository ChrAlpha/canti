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

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

// TestMain runs the binary as the CLI when CANTI_TEST_MAIN=1. There
// CANTI_TEST_CRASH=before:KIND or after:KIND kills the process with SIGKILL
// right before, or right after, it commits the first transition of KIND;
// block:KIND holds that commit until its context is done, after creating the
// file CANTI_TEST_BLOCKED names, and commits nothing; lose:KIND commits it,
// creates that file, and then holds until the context is done and returns
// its error, as a commit whose acknowledgement was lost. KIND@STEP narrows
// any of them to a transition of KIND that leaves step STEP. CANTI_TEST_SETUP=signal holds the setup of
// the loop, and CANTI_TEST_SETUP=checked an input after its checks,
// after creating that file, until the process receives SIGINT or SIGTERM.
func TestMain(m *testing.M) {
	if filepath.Base(os.Args[0]) == "docker" {
		os.Exit(preflightDockerProxy())
	}
	if os.Getenv("CANTI_TEST_MAIN") != "1" && len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		// A process started as a canti command without CANTI_TEST_MAIN,
		// as a host started from inside a test process would be, would run
		// the tests again; refuse instead.
		fmt.Fprintf(os.Stderr, "canti.test: started as canti %s without CANTI_TEST_MAIN=1\n", os.Args[1])
		os.Exit(2)
	}
	if os.Getenv("CANTI_TEST_MAIN") == "1" {
		if len(os.Args) > 1 && os.Args[1] == "fixture-write-lock" {
			os.Exit(acceptanceWriteLock(os.Args[2:]))
		}

		if spec := os.Getenv("CANTI_TEST_CRASH"); spec != "" {
			when, kind, _ := strings.Cut(spec, ":")
			if when != "before" && when != "after" && when != "block" && when != "lose" {
				panic("CANTI_TEST_CRASH must be before:KIND, after:KIND, block:KIND, or lose:KIND")
			}
			step := 0
			if k, s, ok := strings.Cut(kind, "@"); ok {
				kind, step = k, atoi(s)
			}
			wrapJournal = func(j harness.Journal) harness.Journal {
				return crashing{j, when, kind, step, os.Getenv("CANTI_TEST_BLOCKED")}
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
		if point, ok := strings.CutPrefix(os.Getenv("CANTI_TEST_SETUP"), "point:"); ok {
			checkpoint = func(p string) {
				if p == point {
					awaitRemoval(os.Getenv("CANTI_TEST_BLOCKED"))
				}
			}
		}
		os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type crashing struct {
	harness.Journal
	when, kind string
	step       int // when positive, only a transition leaving this step
	marker     string
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return n
}

func (c crashing) PreviousRuns(ctx context.Context, runID string) ([]harness.PreviousRun, int, error) {
	return c.Journal.PreviousRuns(ctx, runID)
}

func (c crashing) Commit(ctx context.Context, t harness.Transition) error {
	switch {
	case t.Event.Kind() != c.kind || c.step > 0 && t.From.Step != c.step:
		return c.Journal.Commit(ctx, t)
	case c.when == "lose":
		if err := c.Journal.Commit(ctx, t); err != nil {
			return err
		}
		os.WriteFile(c.marker, nil, 0o600)
		<-ctx.Done()
		return ctx.Err()
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

// awaitRemoval records this process's PID in marker and waits for removal.
// Unlike awaitSignal, it leaves the process's own signal handling alone.
func awaitRemoval(marker string) {
	os.WriteFile(marker, []byte(strconv.Itoa(os.Getpid())), 0o600)
	for {
		if _, err := os.Stat(marker); os.IsNotExist(err) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
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
		{[]string{"run", "-ephemeral", "-h"}, exitOK, "-goal", ""},
		{[]string{"status", "-bogus"}, exitUsage, "", "flag provided but not defined"},
		{[]string{"status", "extra"}, exitUsage, "", `unexpected argument "extra"`},
		{[]string{"status", "-log-level", "loud"}, exitUsage, "", "loud"},
		{[]string{"run"}, exitUsage, "", "-goal is required"},
		{[]string{"run", "-ephemeral", "-goal", "g", "-max-steps", "-1"}, exitUsage, "", "-max-steps"},
		{[]string{"run", "-ephemeral", "-goal", "g", "-history", "8"}, exitUsage, "", "flag provided but not defined: -history"},
		{[]string{"run", "-ephemeral", "-goal", "g"}, exitUsage, "", "-api must be anthropic-messages or openai-responses"},
		{[]string{"run", "-ephemeral", "-goal", "g", "-api", "openai-completions", "-model", "m"}, exitUsage, "", "-api openai-completions is not supported"},
		{[]string{"run", "-ephemeral", "-goal", "g", "-api", "anthropic-messages"}, exitUsage, "", "-model is required"},
		{[]string{"run", "-ephemeral", "-goal", "g", "-api", "anthropic-messages", "-model", "m", "-request-bytes", "1"}, exitUsage, "", "canti run: -request-bytes: authoritative orientation needs at least "},
		{[]string{"run", "-ephemeral", "-goal", "g", "-max-tokens", "0"}, exitUsage, "", "-max-tokens"},
		{[]string{"run", "-ephemeral", "-goal", "g", "-network", ""}, exitUsage, "", "-network"},
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
	for _, args := range [][]string{append([]string{"run", "-goal", "g"}, fixtureModel...), append([]string{"run", "-input", "i", "-goal", "g"}, fixtureModel...), {"resume"}} {
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
	goal, constraints := "\n"+strings.Repeat("g", l.Goal-2)+" ", strings.Repeat("c", l.Constraints-1)+"\t"
	if r := f.cli(nil, "run", "-goal", goal, "-constraints", constraints); r.code != exitOK {
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

// Unsupported versions fail before model, execution, or journal progress,
// including commands that only read or settle an already-returned run.
func TestUnsupportedContextVersionsRejectEveryCommandWithoutProgress(t *testing.T) {
	for _, phase := range []string{"request_frozen", "returned"} {
		for _, version := range []string{"", "0", strconv.Itoa(host.ContextVersion - 1), strconv.Itoa(host.ContextVersion + 1)} {
			t.Run(phase+"/"+version, func(t *testing.T) {
				f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
				args := []string{"-input", "one", "-goal", "g"}
				if phase == "request_frozen" {
					args = append(args, "-once")
				}
				r := f.cli(nil, "run", args...)
				if r.code != exitOK && r.code != exitActive {
					t.Fatalf("initial run: %+v", r)
				}
				id := f.status().Run.ID
				before, sent := f.history(), len(f.model.requests())
				setContextVersion(t, f.state, id, version)
				for _, command := range [][]string{{"status"}, {"history"}, {"context"}, {"context", "-raw"}, {"inspect"}, {"resume"}, {"resume", "-once"}, {"stop"}, {"resolve", "-job"}, {"resolve", "-execution", "failed_to_start", "-note", "checked"}, {"run", "-goal", "new"}, {"run", "-input", "one", "-goal", "g"}} {
					r := f.cli(nil, command[0], command[1:]...)
					if r.code != exitError || !strings.Contains(r.stderr, "unsupported context version") || !strings.Contains(r.stderr, "use fresh state") || r.stdout != "" {
						t.Errorf("%q: %+v", command, r)
					}
				}
				store, err := sqlite.OpenReadOnly(context.Background(), filepath.Join(f.state, "canti.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				snap, err := store.Latest(context.Background(), id)
				if err != nil || snap.Seq != int64(len(before)) || snap.State != before[len(before)-1].State || len(f.model.requests()) != sent {
					t.Fatalf("unsupported commands made progress: %+v, %v requests=%d", snap, err, len(f.model.requests()))
				}
			})
		}
	}
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

func TestRefusedInputsTouchNothing(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		code   int
		stderr string
	}{
		{[]string{"-input", "i/i"}, exitUsage, "-input must be"},
		{[]string{"-input", "i", "-request-bytes", "1"}, exitUsage, "-request-bytes: "},
		{[]string{"-input", "i", "-model-url", "ftp://model"}, exitError, "BaseURL"},
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
	for _, args := range [][]string{{"run", "-goal", "g"}, {"run", "-input", "i", "-goal", "g"}} {
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
			{"run -input", []string{"run", "-input", "i", "-goal", "g"}},
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
	if r := f.cli(nil, "run", "-goal", "second"); r.code != exitUsage || !strings.Contains(r.stderr, "-delivery") {
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
	if s := f.status(); s.Phase != core.PhaseNeedsResolution || s.EffectID == "" {
		t.Fatalf("status = %+v", s)
	}
	if r := f.cli(noDocker, "resume"); r.code != exitResolution {
		t.Errorf("resume = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(noDocker, "run", "-goal", "again"); r.code != exitUsage || !strings.Contains(r.stderr, "-delivery") {
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
	f.cli(noDocker, "resolve", "-execution", "failed_to_start", "-note", "checked by operator")
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
		t.Fatalf("status after the resolution = %+v", s)
	}
}

func TestTheRequestBytesOfTheRunBoundEveryPrompt(t *testing.T) {
	const steps = 6
	for _, tc := range []struct {
		args  []string
		shown bool
	}{{[]string{"-request-bytes", "50000"}, false}, {nil, true}} {
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
				if r.code == exitOK || len(f.model.requests()) != 0 || !strings.Contains(r.stderr, "resource cap") {
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
			if len(p) != 1 || strings.Contains(p[0], "### Step 1\n") != tc.shown {
				t.Fatalf("last prompt shows step 1 %v, want %v:\n%s", strings.Contains(p[0], "### Step 1\n"), tc.shown, p)
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
	if r := f.cli(noDocker, "resolve", "-execution", "failed_to_start", "-note", "checked by operator"); r.code != exitOK {
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

func parseSpec(t *testing.T, args ...string) host.Spec {
	t.Helper()
	var s host.Spec
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	registerSpec(fs, &s)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return s
}

// runSpec is parseSpec with the model API and model of a fixture, which
// every run names.
func runSpec(t *testing.T, args ...string) host.Spec {
	t.Helper()
	s := parseSpec(t, append(slices.Clone(fixtureModel), args...)...)
	s.Config.Provider = "http://127.0.0.1:1"
	return s
}

func TestRunFlagsDefaults(t *testing.T) {
	s := parseSpec(t)
	want := host.Spec{
		Config: host.RunConfig{Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512, OutputLimit: 8 << 20,
			MaxTokens: 16000, Structured: true, RequestBytes: 8 << 20, ContextVersion: host.ContextVersion},
		Budget: harness.Budget{WallTime: 30 * time.Minute, OutputBytes: 256 << 20, JobTimeout: 10 * time.Minute},
		Limits: core.Limits{MaxSteps: 20, MaxAttempts: 3},
	}
	if s != want {
		t.Errorf("defaults = %+v\nwant %+v", s, want)
	}
	l := harness.DefaultPromptLimits()
	l.Total = 8 << 20
	if got := host.PromptLimits(s.Config); got != l {
		t.Errorf("prompt limits = %+v\nwant %+v", got, l)
	}
}

func TestRunFlagsPersistInTheRun(t *testing.T) {
	s := parseSpec(t, "-ephemeral", "-image", "alpine:3.23", "-network", "bridge", "-memory", "512m", "-cpus", "1.5", "-pids", "64",
		"-output-limit", "4096", "-api", "anthropic-messages", "-model", "claude-test", "-max-tokens", "900", "-effort", "medium", "-structured=false", "-fallbacks=false",
		"-request-bytes", "98304", "-max-steps", "7", "-max-attempts", "2", "-wall-time", "1h", "-token-budget", "5000",
		"-output-budget", "1000000", "-job-timeout", "90s")
	s.Config.Workspace = "/work"
	s.Config.Provider = "http://127.0.0.1:1"
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	run, initial, err := host.NewRun("goal", "constraints", s, now)
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
	c, err := host.DecodeConfig(run, "/work")
	if err != nil || c != s.Config {
		t.Fatalf("stored config = %+v, %v\nwant %+v", c, err, s.Config)
	}
	x := host.ExecutorConfig(c, "/state")
	if !reflect.DeepEqual(x, docker.Config{Image: "alpine:3.23", Workspace: "/work", State: "/state", Network: "bridge", Memory: "512m",
		CPUs: "1.5", PidsLimit: 64, OutputLimit: 4096, PreviewLimit: harness.DefaultPromptLimits().Preview,
		Mounts: []docker.Mount{{Source: "/state/history", Target: "/canti/runs"}, {Source: "/state/inputs", Target: "/canti/inputs"}}}) {
		t.Errorf("executor config = %+v", x)
	}
	if l := host.PromptLimits(c); l.Total != 98304 || l.Goal != harness.DefaultPromptLimits().Goal {
		t.Errorf("prompt limits = %+v", l)
	}
	m := host.AnthropicConfig(c, "http://model.test", host.Keys{AnthropicToken: "token"})
	if m != (anthropic.Config{BaseURL: "http://model.test", AuthToken: "token", Model: "claude-test", MaxTokens: 900, Effort: "medium"}) {
		t.Errorf("model config = %+v", m)
	}
	env := host.Environment(c, run.Budget)
	for _, want := range []string{"alpine:3.23", docker.WorkDir, "bridge", "512m", "1.5", "64", "1m30s"} {
		if !strings.Contains(env, want) {
			t.Errorf("environment %q lacks %q", env, want)
		}
	}
	if strings.Contains(env, archive.ArtifactsRoot) {
		t.Errorf("environment %q names the artifact directory, which the prompt states", env)
	}
	if _, _, err := host.NewRun("goal", "", runSpec(t, "-job-timeout", "-1s"), now); err == nil {
		t.Error("a negative budget was accepted")
	}
	if _, err := host.DecodeConfig(run, "/elsewhere"); err == nil {
		t.Error("a config of another workspace was accepted")
	}
	run.Config = []byte(`{"workspace":"/work","unknown":1}`)
	if _, err := host.DecodeConfig(run, "/work"); err == nil {
		t.Error("an unknown config field was accepted")
	}
}

func TestAResumedRunCallsTheProviderItStores(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "g", "-once"); r.code != exitActive {
		t.Fatalf("run: %+v", r)
	}
	other := newScriptedModel(t, f.key, map[int]core.Decision{1: {Kind: core.Return, Payload: "elsewhere"}})
	if r := f.cli(nil, "resume", "-model-url", other.URL); r.code != exitError || !strings.Contains(r.stderr, "calls the model at "+f.model.URL) {
		t.Fatalf("resume against another provider: %+v", r)
	}
	for _, env := range [][]string{{"ANTHROPIC_BASE_URL=" + other.URL}, nil} {
		r := runCLI(t, append([]string{"ANTHROPIC_API_KEY=" + f.key}, env...), "resume", "-workspace", f.workspace, "-state", f.state)
		if r.code != exitError || !strings.Contains(r.stderr, "calls the model at "+f.model.URL+", but the credential given is for anthropic-messages at ") ||
			strings.Contains(r.stderr, f.key) {
			t.Fatalf("resume from a terminal whose key is bound to another provider (%q): %+v", env, r)
		}
	}
	if len(other.requests()) != 0 || len(f.model.requests()) != 0 {
		t.Fatalf("a refused resume sent %d and %d requests", len(f.model.requests()), len(other.requests()))
	}
	r := runCLI(t, []string{"ANTHROPIC_API_KEY=" + f.key, "ANTHROPIC_BASE_URL=" + f.model.URL}, "resume", "-workspace", f.workspace, "-state", f.state)
	if r.code != exitOK || r.stdout != "done\n" || len(other.requests()) != 0 || len(f.model.requests()) != 1 {
		t.Fatalf("resume from a terminal bound to the run's provider: %+v, calls %d and %d", r, len(f.model.requests()), len(other.requests()))
	}
}

func TestRunExecutionModeAndProvisioningFlagsFailBeforeStateCreation(t *testing.T) {
	for _, mode := range [][]string{nil, {"-ephemeral", "-environment", "/missing"}, {"-environment", "/missing", "-image", "alpine"}, {"-environment", "/missing"}, {"-ephemeral", "-environment-workdir", "/workspace"}} {
		root := t.TempDir()
		workspace := filepath.Join(root, "workspace")
		if err := os.Mkdir(workspace, 0700); err != nil {
			t.Fatal(err)
		}
		state := filepath.Join(root, "state")
		args := append([]string{"run", "-workspace", workspace, "-state", state, "-goal", "goal", "-api", "openai-responses", "-model", "fixture"}, mode...)
		r := runCLI(t, []string{"OPENAI_API_KEY=fixture"}, args...)
		if r.code != exitUsage {
			t.Fatalf("mode %v = %d: %s", mode, r.code, r.stderr)
		}
		if _, err := os.Stat(state); !os.IsNotExist(err) {
			t.Fatalf("mode %v created state: %v", mode, err)
		}
	}
}

func TestCLIUsesCanonicalProspectiveState(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	parent := t.TempDir()
	alias := filepath.Join(t.TempDir(), "safe-state-alias")
	if err := os.Symlink(parent, alias); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(alias, "missing", "state")
	got := f.cli(nil, "run", "-state", state, "-goal", "safe outside State")
	if got.code != exitOK {
		t.Fatalf("safe State alias: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(parent, "missing", "state", "canti.db")); err != nil {
		t.Fatal(err)
	}
	if got := f.status("-state", state); got.Run.ID == "" {
		t.Fatal("alias did not reach the canonical journal")
	}
	for _, kind := range []string{"dangling", "loop"} {
		dir := t.TempDir()
		link := filepath.Join(dir, "link")
		target := link
		if kind == "dangling" {
			target = filepath.Join(dir, "absent")
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		got := f.cli(nil, "run", "-state", filepath.Join(link, "missing", "state"), "-goal", "reject unresolved ancestor")
		if got.code != exitError {
			t.Fatalf("unresolved State ancestor accepted: %+v", got)
		}
		if _, err := os.Lstat(filepath.Join(dir, "absent")); !os.IsNotExist(err) {
			t.Fatalf("unresolved ancestor was recreated: %v", err)
		}
	}
}
