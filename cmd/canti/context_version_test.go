package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
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
// context accounting at the default flags and at their extremes, what the
// CLI maps stored flags onto, the action prefixes, and the requests of every
// scenario of the frozen-request corpus. Changing any of them, or adding a
// scenario, which may observe a changed rule, requires a new context version,
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
	if got == string(want) {
		return
	}
	lines, gotLines := strings.Split(string(want), "\n"), strings.Split(got, "\n")
	var changed []string
	for _, line := range lines {
		if !slices.Contains(gotLines, line) {
			changed = append(changed, "- "+line)
		}
	}
	for _, line := range gotLines {
		if !slices.Contains(lines, line) {
			changed = append(changed, "+ "+line)
		}
	}
	t.Fatalf("the context rules are not those of context version %d, which runs of it store; bump contextVersion and record the rules of the new version with -record-context. Changed:\n%s",
		contextVersion, strings.Join(changed, "\n"))
}

func contextRules(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "context version %d\nactions %q %q\nprompt limits %+v\n", contextVersion, harness.RetrievePrefix, harness.HandoverPrefix, harness.DefaultPromptLimits())
	run := harness.Run{ID: "run_" + strings.Repeat("0", 32), Goal: "Complete the task.", Constraints: "Preserve the evidence."}
	limits := core.Limits{MaxSteps: 20, MaxAttempts: 3}
	loop := harness.LoopConfig{Environment: "Linux", ArtifactDir: docker.ArtifactDir, RunsPath: runsDir, Reader: &archive.Reader{}}
	fit, err := harness.ContextFit(loop, run, limits)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&b, "loop defaults: handover %d, total %d\n", fit.Handover, fit.Limit)
	transports := []anthropic.Config{{DecisionFormat: "text", StructuredOutput: true, Fallbacks: true}, {DecisionFormat: "text"}, {DecisionFormat: "tool", Fallbacks: true}}
	for _, flags := range [][2]bool{{false, false}, {true, false}, {true, true}} {
		for _, c := range transports {
			f := harness.Frame{Goal: run.Goal, Constraints: run.Constraints, Environment: "Linux", Step: 1, MaxSteps: 20, Attempt: 1, MaxAttempts: 3,
				ArtifactDir: docker.ArtifactDir, RunsDir: runsDir, RunID: run.ID, Window: &harness.Window{FirstStep: 1},
				DecisionTool: c.DecisionFormat == "tool", RetrievalEnabled: flags[0], ImportsEnabled: flags[1], HandoverBytes: 2048}
			p, err := f.Prompt(harness.DefaultPromptLimits())
			if err != nil {
				t.Fatal(err)
			}
			c.APIKey, c.Model, c.MaxTokens = "key", "claude-opus-5", 16000
			m, err := anthropic.New(c)
			if err != nil {
				t.Fatal(err)
			}
			req, err := m.Freeze(p)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "request, %s, structured %v, fallbacks %v, retrieval %v, imports %v: system %s, %s %s %s\n", c.DecisionFormat, c.StructuredOutput, c.Fallbacks,
				flags[0], flags[1], digest(p.System), req.Protocol, req.Adapter, digest(string(req.Body)))
		}
	}
	page := everyBlock(run)
	p, err := page.Prompt(harness.DefaultPromptLimits())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&b, "page of every block kind: %s\n", digest(p.System+"\x00"+strings.Join(p.Blocks, "\x00")))
	imports, previous, mapped := t.TempDir(), "", ""
	for _, flags := range [][]string{nil, {"-handover-bytes", "1"}, {"-handover-bytes", "8192"}, {"-history-imports", imports},
		{"-max-steps", "0", "-max-attempts", "1"}, {"-pids", "0", "-job-timeout", "0", "-memory", "", "-cpus", "4", "-network", "bridge"},
		{"-image", "\xff" + strings.Repeat("x", 2047)}, {"-image", "debian\xff"},
		{"-model", "claude-sonnet-5", "-max-tokens", "4096", "-structured=false", "-decision-format", "tool", "-fallbacks=false", "-context-bytes", "131072", "-window-steps", "6", "-window-bytes", "9000"}} {
		s := parseSpec(t, flags...)
		loop := contextConfig(s.config, s.budget)
		fit, err := harness.ContextFit(loop, run, s.limits)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case len(flags) > 0 && flags[0] == "-history-imports":
			flags = []string{"-history-imports", "DIR"}
		case len(flags) > 0 && flags[0] == "-image":
			flags = []string{"-image", fmt.Sprintf("%d bytes, %q first", len(flags[1]), flags[1][:min(len(flags[1]), 7)])}
		}
		fmt.Fprintf(&b, "run %q: %+v, required %d\n", flags, fit, fit.Required())
		if env := environment(s.config, s.budget); env != previous {
			fmt.Fprintf(&b, "  environment %s\n", digest(env))
			previous = env
		}
		mc := modelConfig(s.config, "", credential{})
		if m := fmt.Sprintf("model %s, max tokens %d, structured %v, decision format %q, fallbacks %v, limits %+v, window steps %d, window bytes %d, imports %v",
			mc.Model, mc.MaxTokens, mc.StructuredOutput, mc.DecisionFormat, mc.Fallbacks, loop.Limits, loop.WindowSteps, loop.WindowBytes, loop.Reader.Imports != ""); m != mapped {
			fmt.Fprintf(&b, "  %s\n", m)
			mapped = m
		}
	}
	b.WriteString(sessionRules(t))
	b.WriteString(storedRules(t))
	b.WriteString(corpusRules(t))
	return b.String()
}

// storedRules records every request of a run whose model and context flags
// all differ from their defaults but its decision format, the first frozen
// by run -once and the next by a resume process that rebuilds the run from
// its stored config, after the run read an imported file and its window
// asked for a handover; the repairs of a run of the tool decision format,
// which a resume process freezes when text answers it; a run of the text
// decision format named explicitly, which a resume process returns; and a
// run whose window outgrows a page of the journal, after 65 reads.
func storedRules(t *testing.T) string {
	t.Helper()
	read := core.Decision{Kind: core.Exec, Payload: harness.RetrievePrefix + `{"operation":"read","path":"/canti/imports/notes.txt","reason":"Read the notes."}`}
	imports := t.TempDir()
	if err := os.WriteFile(filepath.Join(imports, "notes.txt"), []byte("The parser was rewritten in v2.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, map[int]core.Decision{1: read, 2: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-once", "-goal", "Report what the notes say.", "-model", "claude-sonnet-5", "-max-tokens", "4096", "-structured=false",
		"-fallbacks=false", "-context-bytes", "131072", "-window-steps", "1", "-window-bytes", "9000", "-handover-bytes", "1000",
		"-history-imports", imports, "-memory", "1g"); r.code != exitActive {
		t.Fatalf("run -once = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	b := frozenRequests(t, f, "stored run")
	f = newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-once", "-goal", "Report done.", "-decision-format", "tool"); r.code != exitActive {
		t.Fatalf("run -once -decision-format tool = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resume"); r.code != exitStopped {
		t.Fatalf("resume of the tool run = %d\nstderr: %s", r.code, r.stderr)
	}
	b += frozenRequests(t, f, "stored tool run")
	purposed := read
	purposed.Purpose = "Read the notes, then report them."
	f = newFixture(t, map[int]core.Decision{1: purposed, 2: {Kind: core.Return, Payload: "done"}})
	f.model.answer(1, 2, reply{status: http.StatusOK, body: toolReply(purposed)})
	f.model.answer(2, 1, reply{status: http.StatusOK, body: toolReply(core.Decision{Kind: core.Return, Purpose: "The notes are read.", Payload: "done"})})
	if r := f.cli(nil, "run", "-once", "-goal", "Report what the notes say.", "-decision-format", "tool", "-history-imports", imports); r.code != exitActive {
		t.Fatalf("run -once of the stepping tool run = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume of the stepping tool run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	b += frozenRequests(t, f, "stored stepping tool run")
	f = newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-once", "-goal", "Report done.", "-decision-format", "text"); r.code != exitActive {
		t.Fatalf("run -once -decision-format text = %d\nstderr: %s", r.code, r.stderr)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume of the text run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	b += frozenRequests(t, f, "stored text run")
	long := map[int]core.Decision{66: {Kind: core.Return, Payload: "done"}}
	for step := 1; step <= 65; step++ {
		long[step] = read
	}
	f = newFixture(t, long)
	if r := f.cli(nil, "run", "-goal", "Read the notes 65 times.", "-max-steps", "0", "-history-imports", imports); r.code != exitOK {
		t.Fatalf("run of 66 steps = %d\nstderr: %s", r.code, r.stderr)
	}
	h := sha256.New()
	io.WriteString(h, frozenRequests(t, f, "step"))
	return b + fmt.Sprintf("long window: %d requests, sha256 %x\n", len(f.model.requests()), h.Sum(nil))
}

// frozenRequests digests, one line labelled label per request, every request
// the latest run of f froze, with its run ID named RUN.
func frozenRequests(t *testing.T, f *fixture, label string) string {
	t.Helper()
	store, err := sqlite.OpenReadOnly(context.Background(), filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	last, err := store.LastRun(context.Background())
	var all []harness.Transition
	if err == nil {
		all, err = store.Transitions(context.Background(), last.ID, 0, 1<<20)
	}
	if err != nil {
		t.Fatal(err)
	}
	name := strings.NewReplacer(last.ID, "RUN").Replace
	var b strings.Builder
	for _, tr := range all {
		if req := tr.Facts.Request; req != nil {
			req.Body = []byte(name(string(req.Body)))
			facts, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "%s step %d revision %d: %s\n", label, tr.To.Step, tr.To.RequestRev, digest(name(string(facts))))
		}
	}
	return b.String()
}

// corpusRules digests, for every scenario of the frozen-request corpus that
// TestLoopScenariosKeepTheirCommittedRecords keeps equal to what the loop
// commits, the body of every request it freezes and its journal: the event
// ID and facts of every transition, which record the window and context
// accounting of each request and the outcome of each step and stop.
func corpusRules(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..", "..", "internal", "harness", "testdata", "frozen-requests")
	scenarios, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, sc := range scenarios {
		files, err := os.ReadDir(filepath.Join(root, sc.Name()))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		for _, f := range files {
			data, err := os.ReadFile(filepath.Join(root, sc.Name(), f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(h, "%s %d\n%s", f.Name(), len(data), data)
		}
		fmt.Fprintf(&b, "corpus %s: sha256 %x\n", sc.Name(), h.Sum(nil))
	}
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
	read := archive.Retrieval{Query: archive.RetrievalQuery{Operation: "read", Path: "/canti/runs/r/steps/00000002/preview", Reason: "check", Limit: 64},
		Sources: []archive.SourceRange{{Path: "/canti/runs/r/steps/00000002/preview", Length: 5, SHA256: strings.Repeat("a", 64)}}, NextOffset: 5}
	list := archive.Retrieval{Query: archive.RetrievalQuery{Operation: "list", Path: "/canti/runs"}, Entries: []string{"r/"}, NextOffset: 1}
	discover := archive.Retrieval{Query: archive.RetrievalQuery{Operation: "discover", Path: "/canti/runs/r/steps", Reason: "find", Query: "FAIL"},
		Sources: []archive.SourceRange{{Path: "/canti/runs/r/steps/00000003/preview", Length: 4, SHA256: strings.Repeat("b", 64)}}, More: true, NextCursor: "c"}
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
