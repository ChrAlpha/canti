package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

var recordContext = flag.Bool("record-context", false, "record the context rules of a context version that has none")

// TestContextVersionPinsItsRules requires that the rules a stored context
// version stands for are those recorded for contextVersion: the rendering of
// every system prompt variant in every request transport and of every block
// kind, the prompt limits, the defaults the loop applies, every term of the
// context accounting at the default flags and at their extremes, and the
// action prefixes. Changing any of them requires a new context version,
// whose rules -record-context records once; it never rewrites a version.
func TestContextVersionPinsItsRules(t *testing.T) {
	file := filepath.Join("testdata", fmt.Sprintf("context-v%d.txt", contextVersion))
	got := contextRules(t)
	want, err := os.ReadFile(file)
	switch {
	case *recordContext && errors.Is(err, fs.ErrNotExist):
		if err := os.WriteFile(file, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	case err != nil:
		t.Fatalf("context version %d has no recorded rules: %v", contextVersion, err)
	}
	if lines := strings.Split(string(want), "\n"); got != string(want) {
		var changed []string
		for _, line := range strings.Split(got, "\n") {
			if !slices.Contains(lines, line) {
				changed = append(changed, line)
			}
		}
		t.Fatalf("the context rules are not those of context version %d, which runs of it store; bump contextVersion and record the rules of the new version with -record-context. Changed:\n%s",
			contextVersion, strings.Join(changed, "\n"))
	}
}

func contextRules(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	var b strings.Builder
	fmt.Fprintf(&b, "context version %d\nactions %q %q\nprompt limits %+v\n", contextVersion, harness.RetrievePrefix, harness.HandoverPrefix, harness.DefaultPromptLimits())
	run := harness.Run{ID: "run_" + strings.Repeat("0", 32), Goal: "Complete the task.", Constraints: "Preserve the evidence."}
	limits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	loop := harness.LoopConfig{Environment: "Linux", ArtifactDir: docker.ArtifactDir, RunsPath: runsDir, Reader: &harness.Reader{}}
	fit, err := harness.ContextFit(ctx, nil, loop, run, limits)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&b, "loop defaults: handover %d, total %d\n", fit.Handover, fit.Limit)
	transports := []anthropic.Config{{DecisionFormat: "text", StructuredOutput: true, Fallbacks: true}, {DecisionFormat: "text"}, {DecisionFormat: "tool", Fallbacks: true}}
	for _, flags := range [][2]bool{{false, false}, {true, false}, {true, true}} {
		f := harness.Frame{Goal: run.Goal, Constraints: run.Constraints, Environment: "Linux", Step: 1, MaxSteps: 20, Attempt: 1, MaxAttempts: 3,
			ArtifactDir: docker.ArtifactDir, RunsDir: runsDir, RunID: run.ID, Window: &harness.Window{FirstStep: 1},
			RetrievalEnabled: flags[0], ImportsEnabled: flags[1], HandoverBytes: 2048}
		p, err := f.Prompt(harness.DefaultPromptLimits())
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "system, retrieval %v, imports %v: %s\n", flags[0], flags[1], digest(p.System))
		for _, c := range transports {
			c.APIKey, c.Model, c.MaxTokens = "key", "claude-opus-5", 16000
			m, err := anthropic.New(c)
			if err != nil {
				t.Fatal(err)
			}
			req, err := m.Freeze(p)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "  request, %s, structured %v, fallbacks %v: %s %s %s\n", c.DecisionFormat, c.StructuredOutput, c.Fallbacks, req.Protocol, req.Adapter, digest(string(req.Body)))
		}
	}
	page := everyBlock(run)
	p, err := page.Prompt(harness.DefaultPromptLimits())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&b, "page of every block kind: %s\n", digest(p.System+"\x00"+strings.Join(p.Blocks, "\x00")))
	imports, previous := t.TempDir(), ""
	for _, flags := range [][]string{nil, {"-handover-bytes", "1"}, {"-handover-bytes", "8192"}, {"-history-imports", imports},
		{"-max-steps", "0", "-max-attempts", "1"}, {"-pids", "0", "-job-timeout", "0", "-memory", "", "-cpus", "4", "-network", "bridge"}} {
		s := parseSpec(t, flags...)
		fit, err := harness.ContextFit(ctx, nil, contextConfig(s.config, s.budget), run, s.limits)
		if err != nil {
			t.Fatal(err)
		}
		if len(flags) > 0 && flags[0] == "-history-imports" {
			flags = []string{"-history-imports", "DIR"}
		}
		fmt.Fprintf(&b, "run %q: %+v, required %d\n", flags, fit, fit.Required())
		if env := environment(s.config, s.budget); env != previous {
			fmt.Fprintf(&b, "  environment %q\n", env)
			previous = env
		}
	}
	b.WriteString(sessionRules(t))
	return b.String()
}

// sessionRules records the first request that a run of a session freezes
// after nine earlier runs, the fifth stopped, through the CLI: the earlier
// dialogue the run selects, how it renders, and the sources it records, with
// run IDs named by their session sequence.
func sessionRules(t *testing.T) string {
	t.Helper()
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "Earlier answer."}})
	f.model.answer(1, 5, reply{text: "not a decision"})
	for i := 1; i <= 9; i++ {
		args, want := []string{"-session", "s", "-input", fmt.Sprintf("in%d", i), "-goal", fmt.Sprintf("Earlier goal %d.", i)}, exitOK
		if i == 5 {
			args, want = append(args, "-max-attempts", "1"), exitStopped
		}
		if r := f.cli(nil, "run", args...); r.code != want {
			t.Fatalf("earlier session run %d: %+v", i, r)
		}
	}
	if r := f.cli(nil, "run", "-session", "s", "-input", "in10", "-goal", "Current goal.", "-once"); r.code != exitActive {
		t.Fatalf("session run 10: %+v", r)
	}
	store, err := sqlite.OpenReadOnly(context.Background(), filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var named []string
	for seq := int64(1); seq <= 10; seq++ {
		runs, err := store.SessionRuns(context.Background(), "s", seq+1, 1)
		if err != nil || len(runs) != 1 || runs[0].SessionSeq != seq {
			t.Fatalf("session run %d: %+v, %v", seq, runs, err)
		}
		named = append(named, runs[0].ID, fmt.Sprintf("RUN%d", seq))
	}
	name := strings.NewReplacer(named...).Replace
	last, err := store.LastRun(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	req, err := store.Request(context.Background(), last.ID, 1, 1)
	if err != nil || req.Context == nil {
		t.Fatalf("frozen request of session run 10: %+v, %v", req, err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "session run 10 of 10: request %s, navigation %s\n", digest(name(string(req.Body))), name(req.Context.SessionNavigation))
	for _, src := range req.Context.Session {
		fmt.Fprintf(&b, "  earlier %s, sequence %d, %s, goal %v\n", name(src.RunID), src.SessionSeq, src.Status, src.IncludedGoal)
	}
	return b.String()
}

// everyBlock is a page of a requested handover window after a handover,
// with a step of every kind and a rejected response.
func everyBlock(run harness.Run) harness.Frame {
	exit, failed := 0, 1
	read := harness.Retrieval{Query: harness.RetrievalQuery{Operation: "read", Path: "/canti/runs/r/steps/00000002/preview", Reason: "check", Limit: 64},
		Sources: []harness.SourceRange{{Path: "/canti/runs/r/steps/00000002/preview", Length: 5, SHA256: strings.Repeat("a", 64)}}, NextOffset: 5}
	list := harness.Retrieval{Query: harness.RetrievalQuery{Operation: "list", Path: "/canti/runs"}, Entries: []string{"r/"}, NextOffset: 1}
	discover := harness.Retrieval{Query: harness.RetrievalQuery{Operation: "discover", Path: "/canti/runs/r/steps", Reason: "find", Query: "FAIL"},
		Sources: []harness.SourceRange{{Path: "/canti/runs/r/steps/00000003/preview", Length: 4, SHA256: strings.Repeat("b", 64)}}, More: true, NextCursor: "c"}
	steps := []harness.StepRecord{
		{Step: 3, Command: "go test ./...", Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &failed, Preview: "FAIL ```",
			Output: harness.ArtifactRef{Path: "effect_x/output", Bytes: 8, Complete: true, Available: true}}},
		{Step: 4, Command: harness.RetrievePrefix + `{"operation":"read"}`, Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &exit, Preview: "{}", Retrieval: &read}},
		{Step: 5, Command: harness.RetrievePrefix + `{"operation":"list"}`, Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &exit, Retrieval: &list}},
		{Step: 6, Command: "sleep 1", Resolved: true, Observation: harness.Observation{Execution: core.ExecutionTimedOut, Output: harness.ArtifactRef{Detail: "resolved by operator"}}},
		{Step: 7, Command: harness.HandoverPrefix + `{}`, Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &failed, Preview: "Context action failed: x"}},
		{Step: 8, Command: harness.RetrievePrefix + `{"operation":"discover"}`, Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &exit, Retrieval: &discover}},
		{Step: 9, Command: harness.HandoverPrefix + `{"run_id":"r","first_step":3,"text":"Go on."}`, Observation: harness.Observation{Execution: core.ExecutionExited, ExitCode: &exit,
			Preview: "Handover recorded; the next request starts a new context window.", Handover: &harness.Handover{RunID: "r", FirstStep: 3, Text: "Go on."}}},
	}
	return harness.Frame{Goal: run.Goal, Constraints: run.Constraints, Environment: "Linux", Step: 10, MaxSteps: 0, Attempt: 2, MaxAttempts: 3,
		Earlier: 2, Steps: steps, ArtifactDir: docker.ArtifactDir, RunsDir: runsDir, RunID: run.ID,
		Rejected:         &harness.Rejection{Error: "core: decision has no kind", Response: "{}"},
		Window:           &harness.Window{FirstStep: 3, SourceStep: 2, Handover: "Continue the tests.", Requested: true, Failures: 1},
		RetrievalEnabled: true, ImportsEnabled: true, HandoverBytes: 4096, Retrieved: map[int]string{4: "Source x:\nBytes [0,5), sha256 a:\nhello\n"},
		SessionContext: "Run run_0 (session sequence 1; source /canti/runs/run_0/goal and result; returned):\n"}
}

func digest(s string) string {
	return fmt.Sprintf("%d bytes, sha256 %x", len(s), sha256.Sum256([]byte(s)))
}
