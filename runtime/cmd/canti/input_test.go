package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

func TestAnInputRetryAndLineNavigation(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{
		1: {Kind: core.Return, Payload: "Option one: async. Option two: sync."},
		2: {Kind: core.Return, Payload: "Use the second option."},
	})
	args := []string{"-input", "first", "-goal", "Explain both options", "-constraints", "No new infrastructure"}
	if r := f.cli(nil, "run", args...); r.code != exitOK {
		t.Fatalf("first: %+v", r)
	}
	first := f.status().Run.ID
	if r := f.cli(nil, "run", args...); r.code != exitOK || len(f.model.requests()) != 1 {
		t.Fatalf("retry: %+v, calls=%d", r, len(f.model.requests()))
	}
	if r := f.cli(nil, "run", "-input", "first", "-goal", "different"); r.code != exitError || !strings.Contains(r.stderr, "input ID already exists") {
		t.Fatalf("conflict: %+v", r)
	}
	if r := f.cli(nil, "run", "-input", "second", "-goal", "Use the second option"); r.code != exitOK {
		t.Fatalf("second: %+v", r)
	}
	s := f.status()
	if s.Run.ID == first || s.Step != 1 || s.Totals.Steps != 0 || s.Run.Constraints != "No new infrastructure" {
		t.Fatalf("second run: %+v", s)
	}
	p := f.model.prompts(1)[1]
	for _, want := range []string{"Use the second option", "No new infrastructure", "## Previous runs", "- " + first + ":", "/canti/runs/" + first + "/"} {
		if !strings.Contains(p, want) {
			t.Errorf("missing %q in %s", want, p)
		}
	}
	checkFile(t, filepath.Join(f.state, "history", first, "goal"), "Explain both options")
	checkFile(t, filepath.Join(f.state, "history", first, "result"), "phase: returned\n\nOption one: async. Option two: sync.")
	if r := f.cli(nil, "run", args...); r.code != exitOK || len(f.model.requests()) != 2 {
		t.Fatalf("old retry after newer run: %+v", r)
	}
}

func TestAnInputCrashReplayAndFrozenRetry(t *testing.T) {
	t.Run("before admission", func(t *testing.T) {
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
		marker := filepath.Join(t.TempDir(), "before-submit")
		args := []string{"-input", "one", "-goal", "question"}
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
		if f.status().Run.InputPos != 1 || len(f.model.requests()) != 1 {
			t.Fatal("pre-admission crash persisted input or duplicated execution")
		}
	})
	for _, boundary := range []string{"before:request_frozen", "after:request_frozen", "after:response_loaded"} {
		t.Run(boundary, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
			args := []string{"-input", "one", "-goal", "question"}
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
		f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "previous answer"}, 2: {Kind: core.Return, Payload: "continue"}})
		if r := f.cli(nil, "run", "-input", "one", "-goal", "previous question"); r.code != exitOK {
			t.Fatal(r)
		}
		f.model.answer(1, 2, reply{status: 503, body: "temporarily unavailable"})
		if r := f.cli(nil, "run", "-input", "two", "-goal", "continue"); r.code != exitOK {
			t.Fatal(r)
		}
		reqs := f.model.requests()
		if len(reqs) != 3 || !bytes.Equal(reqs[1], reqs[2]) {
			t.Fatal("transport retry changed frozen dialogue request")
		}
	})
}

func TestLineGrowthRemainsBounded(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: strings.Repeat("answer ", 300)}})
	var previous string
	for i := 0; i < 40; i++ {
		if r := f.cli(nil, "run", "-input", fmt.Sprint(i), "-goal", fmt.Sprintf("question-%02d %s", i, strings.Repeat("x", 1000)), "-request-bytes", "98304"); r.code != exitOK {
			t.Fatalf("round %d: %+v", i, r)
		}
		s := f.status()
		p := f.model.prompts(1)[i]
		if previous != "" && !strings.Contains(p, "## Previous runs") {
			t.Fatalf("round %d lost navigation", i)
		}
		previous = s.Run.ID
		if s.Run.InputPos < 1 {
			t.Fatalf("round %d input position = %d", i, s.Run.InputPos)
		}
	}
	ps := f.model.prompts(1)
	if len(ps) != 40 || !strings.Contains(ps[39], "question-39") || strings.Contains(ps[39], "question-00") {
		t.Fatal("history projection did not remain bounded")
	}
	for i, p := range ps {
		if len(p) > 98304 {
			t.Fatalf("round %d prompt %d bytes", i, len(p))
		}
	}
	if r := f.cli(nil, "run", "-fresh", "-input", "separate", "-goal", "unrelated", "-request-bytes", "98304"); r.code != exitOK {
		t.Fatal(r)
	}
	if strings.Contains(f.model.prompts(1)[40], "question-") {
		t.Fatal("another line's runs leaked")
	}
	s := f.status()
	if !s.Run.Fresh || s.Run.InputPos <= 40 {
		t.Fatalf("fresh run = %+v", s.Run)
	}
}

func TestAWorkingAgentNeedsADeliveryAndAcknowledgesWithoutDriving(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "sleep 1"},
		2: {Kind: core.Return, Payload: "first done"},
		3: {Kind: core.Return, Payload: "follow-up done"},
	})
	f.autoHost()
	args := []string{"-host", "auto", "-input", "one", "-goal", "first", "-once"}
	if r := f.cli(nil, "run", args...); r.code != exitActive {
		t.Fatalf("first: %+v", r)
	}
	first := f.status().Run.ID
	if r := f.cli(nil, "run", args...); r.code != exitActive || f.status().Seq != 1 {
		t.Fatalf("retry advanced execution: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-input", "two", "-goal", "next"); r.code != exitUsage || !strings.Contains(r.stderr, "-delivery") {
		t.Fatalf("accepted an input without delivery: %+v", r)
	}
	if f.status().Run.ID != first {
		t.Fatal("rejected input created a run")
	}
	r := f.cli(nil, "run", "-host", "auto", "-input", "steer-1", "-goal", "try the tests", "-delivery", "steer")
	if r.code != exitOK {
		t.Fatalf("steer: %+v", r)
	}
	var ack struct {
		InputID     string `json:"input_id"`
		Disposition string `json:"disposition"`
		Pos         int64  `json:"pos"`
		RunID       string `json:"run_id"`
		Replayed    bool   `json:"replayed"`
	}
	if err := strict(r.stdout, &ack); err != nil || ack.Disposition != "steer" || ack.Pos == 0 || ack.RunID != "" || ack.InputID != "steer-1" || len(f.model.requests()) != 0 {
		t.Fatalf("steer acknowledgment = %+v, %v; calls=%d", ack, err, len(f.model.requests()))
	}
	r = f.cli(nil, "run", "-host", "auto", "-input", "follow-1", "-goal", "then report", "-delivery", "follow_up")
	if r.code != exitOK {
		t.Fatalf("follow-up: %+v", r)
	}
	ack = struct {
		InputID     string `json:"input_id"`
		Disposition string `json:"disposition"`
		Pos         int64  `json:"pos"`
		RunID       string `json:"run_id"`
		Replayed    bool   `json:"replayed"`
	}{}
	if err := strict(r.stdout, &ack); err != nil || ack.Disposition != "queued" || ack.Pos == 0 || ack.RunID != "" || ack.InputID != "follow-1" {
		t.Fatalf("follow-up acknowledgment = %+v, %v", ack, err)
	}
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "first done\n" {
		t.Fatalf("resume: %+v", r)
	}
	waitFor(t, func() bool {
		s := f.status()
		return s.Run.InputID == "follow-1" && s.Run.InputPos > 0
	})
	waitFor(t, func() bool {
		s := f.status()
		return s.Phase == core.PhaseReturned
	})
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.InputID != "follow-1" {
		t.Fatalf("follow-up run = %+v", s)
	}
}

func TestAnUnknownEffectCannotBeBypassed(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}, 2: {Kind: core.Return, Payload: "recovered"}, 3: {Kind: core.Return, Payload: "next accepted"}})
	if r := f.cli([]string{"PATH=" + t.TempDir()}, "run", "-input", "one", "-goal", "act"); r.code != exitResolution {
		t.Fatalf("unknown: %+v", r)
	}
	first := f.status()
	history := f.history()
	// The run is open, so a new principal input needs a delivery; a follow-up
	// is admitted but waits, and the run does not advance.
	if r := f.cli(nil, "run", "-host", "auto", "-input", "two", "-goal", "next"); r.code != exitUsage || !strings.Contains(r.stderr, "-delivery") {
		t.Fatalf("a no-delivery input while the effect is unknown: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-input", "two", "-goal", "next", "-delivery", "follow_up"); r.code != exitOK {
		t.Fatalf("a queued follow-up while the effect is unknown: %+v", r)
	}
	if r := f.cli(nil, "resolve", "-host", "auto", "-execution", "unknown", "-note", "not yet known"); r.code != exitUsage || !strings.Contains(r.stderr, "needs_resolution") {
		t.Fatalf("unknown resolution: %+v", r)
	}
	if !reflect.DeepEqual(f.status(), first) || !reflect.DeepEqual(f.history(), history) || len(f.model.requests()) != 1 {
		t.Fatal("a queued input or unknown resolution changed committed state or executed")
	}
	if r := f.cli(nil, "withdraw", "-host", "auto", "-input", "two"); r.code != exitOK || !strings.Contains(r.stdout, "next") {
		t.Fatalf("withdraw the queued follow-up: %+v", r)
	}
	if r := f.cli(nil, "resolve", "-host", "auto", "-execution", "failed_to_start", "-note", "Docker was unavailable; verified no job started"); r.code != exitOK {
		t.Fatalf("known resolution: %+v", r)
	}
	if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "recovered\n" {
		t.Fatalf("recovery: %+v", r)
	}
	if len(f.model.prompts(1)) != 1 || len(f.model.prompts(2)) != 1 {
		t.Fatal("old effect decision was replayed")
	}
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"next accepted"}`})
	if r := f.cli(nil, "run", "-host", "auto", "-input", "three", "-goal", "next"); r.code != exitOK || r.stdout != "next accepted\n" {
		t.Fatalf("next input: %+v", r)
	}
	if f.status().Run.ID == first.Run.ID {
		t.Fatal("new input revived the original run")
	}
}

func TestAnInputRetryReportsTheAcceptedInput(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}, 2: {Kind: core.Return, Payload: "done"}})
	first := []string{"-input", "first", "-goal", strings.Repeat("g", 8000), "-request-bytes", "80000"}
	second := []string{"-input", "second", "-goal", "g", "-constraints", strings.Repeat("c", 16000), "-request-bytes", "120000"}
	for _, args := range [][]string{first, second} {
		if r := f.cli(nil, "run", args...); r.code != exitOK {
			t.Fatalf("%q: %+v", args[1], r)
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

func TestAnInputRetryReportsAnInputAcceptedDuringItsChecks(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	args := []string{"-input", "i", "-goal", "g"}
	checked := filepath.Join(t.TempDir(), "checked")
	cmd, wait := f.start([]string{"CANTI_TEST_SETUP=checked", "CANTI_TEST_BLOCKED=" + checked}, "run", args...)
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

func TestAnInputInitializesAnEmptyJournal(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if err := os.Mkdir(f.state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.state, "canti.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r := f.cli(nil, "run", "-input", "i", "-goal", "g")
	if r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("run over an empty journal: %+v", r)
	}
}

func TestObserveAndWithdrawCommands(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "observe", "-source", "ci", "-subject", "build", "-text", "green"); r.code != exitOK {
		t.Fatalf("observe: %+v", r)
	}
	var ack struct {
		Pos      int64 `json:"pos"`
		Replayed bool  `json:"replayed"`
	}
	if err := strict(f.cli(nil, "observe", "-source", "ci", "-subject", "build", "-text", "red").stdout, &ack); err != nil || ack.Pos != 2 {
		t.Fatalf("second observation = %+v, %v", ack, err)
	}
	if r := f.cli(nil, "run", "-input", "one", "-goal", "first", "-once"); r.code != exitActive {
		t.Fatalf("first: %+v", r)
	}
	if s := f.status(); s.Run.Ordinal != 1 {
		t.Fatalf("an observation opened a run: %+v", s.Run)
	}
	runID := f.status().Run.ID
	var frozen []byte
	for _, e := range f.history("-run", runID) {
		if e.Facts.Request != nil {
			frozen = e.Facts.Request.Body
		}
	}
	if _, prompt, err := promptOf(frozen); err != nil || !strings.Contains(prompt, "## Observations") ||
		!strings.Contains(prompt, "ci / build") || !strings.Contains(prompt, "red") || strings.Contains(prompt, "green") {
		t.Fatalf("observations not coalesced into the request: %v\n%s", err, prompt)
	}
	run := f.status().Run
	textFile := filepath.Join(f.state, "inputs", fmt.Sprintf("%012d", run.InputPos), "text")
	if body, err := os.ReadFile(textFile); err != nil || string(body) != "first" {
		t.Fatalf("opening input publication = %q, %v", body, err)
	}
	if body, err := os.ReadFile(filepath.Join(f.state, "inputs", fmt.Sprintf("%012d", run.InputPos), "meta")); err != nil ||
		!strings.Contains(string(body), "authority: principal") || strings.Contains(string(body), "disposition") ||
		!strings.Contains(string(body), "run: "+run.ID) || !strings.Contains(string(body), "admitted:") {
		t.Fatalf("opening input meta = %q, %v", body, err)
	}
	if r := f.cli(nil, "run", "-input", "steer", "-goal", "look", "-delivery", "steer"); r.code != exitOK {
		t.Fatalf("steer: %+v", r)
	}
	// A stopped run holds the unbound steer; withdraw returns its text.
	if r := f.cli(nil, "run", "-input", "follow", "-goal", "later", "-delivery", "follow_up"); r.code != exitOK {
		t.Fatalf("follow-up: %+v", r)
	}
	if r := f.cli(nil, "stop", "-cleanup", "5s"); r.code != exitStopped {
		t.Fatalf("stop: %+v", r)
	}
	r := f.cli(nil, "withdraw")
	if r.code != exitOK || !strings.Contains(r.stdout, "\"text\":\"look\"") || !strings.Contains(r.stdout, "\"text\":\"later\"") {
		t.Fatalf("withdraw = %+v", r)
	}
	if r := f.cli(nil, "withdraw"); r.code != exitOK || r.stdout != "" {
		t.Fatalf("empty withdraw = %+v", r)
	}
	if r := f.cli(nil, "run", "-input", "next", "-goal", "next"); r.code != exitOK {
		t.Fatalf("after withdrawal: %+v", r)
	}
}

func TestPersistentInputRetryDoesNotResolveTheEnvironmentAgain(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{})
	spec := runSpec(t)
	c := &spec.Config
	c.Ephemeral, c.Environment, c.EnvironmentWorkdir = false, filepath.Join(t.TempDir(), "missing-environment"), "/workspace"
	c.Workspace, c.Provider = f.workspace, f.model.URL
	c.Image, c.Network, c.Memory, c.CPUs, c.Pids = "", "", "", "", 0
	digest := strings.Repeat("a", 64)
	c.Attachment = &docker.Attachment{Format: 1, Directory: c.Environment, Endpoint: "unix:///unavailable.sock", DaemonID: "daemon", ContainerID: digest, AuthorityID: digest, Incarnation: 1,
		Helper: "/canti/bin/" + digest + "/canti-job", HelperDigest: digest, User: "1000", Home: "/home/developer", Path: "/usr/bin:/bin",
		Mounts: []docker.EnvironmentMount{{Type: "bind", Source: c.Workspace, Target: "/workspace", Writable: true}, {Type: "bind", Source: filepath.Join(c.Environment, "public"), Target: "/canti"}, {Type: "bind", Source: filepath.Join(c.Environment, "spool"), Target: "/var/lib/canti/jobs", Writable: true}}}
	run, initial, err := host.NewRun("retry exact goal", "", spec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	run.InputID = "accepted-input"
	storedSpec, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(f.state, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open(t.Context(), filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Admit(t.Context(), harness.InputSubmission{InputID: run.InputID, Authority: harness.AuthorityPrincipal,
		Text: run.Goal, Constraints: run.Constraints, ExplicitConstraints: true, Spec: storedSpec, Run: run, Initial: initial, Now: time.Now()})
	store.Close()
	if err != nil {
		t.Fatal(err)
	}
	r := f.cli(nil, "run", "-input", run.InputID, "-goal", run.Goal, "-ephemeral=false", "-environment", c.Environment)
	if r.code != exitActive || len(f.model.requests()) != 0 {
		t.Fatalf("offline input replay = %+v; calls=%d", r, len(f.model.requests()))
	}
	status := f.status()
	if status.Run.ID != run.ID || status.Seq != 0 || status.Execution.Environment == nil || status.Execution.Environment.ContainerID != digest {
		t.Fatalf("replay changed stored facts: %+v", status)
	}
	if _, err := os.Stat(c.Environment); !os.IsNotExist(err) {
		t.Fatalf("retry created environment: %v", err)
	}
}
