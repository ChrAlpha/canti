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

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

var recordContext = flag.Bool("record-context", false, "record the context rules of a context version that has none")

// TestContextVersionPinsItsRules requires that the rules a stored context
// version stands for are those recorded for host.ContextVersion: the rendering of
// every system prompt variant in every request transport and of every block
// kind, the prompt limits, the defaults the loop applies, every term of the
// context accounting at the default flags and at their extremes, what the
// CLI maps stored flags onto, the action prefixes, and the requests of every
// scenario of the frozen-request corpus. Changing any of them, or adding a
// scenario, which may observe a changed rule, requires a new context version,
// whose rules -record-context records once; it never rewrites a version.
func TestContextVersionPinsItsRules(t *testing.T) {
	file := filepath.Join("testdata", fmt.Sprintf("context-v%d.txt", host.ContextVersion))
	got := contextRules(t)
	want, err := os.ReadFile(file)
	switch {
	case *recordContext && errors.Is(err, fs.ErrNotExist):
		if err := os.WriteFile(file, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	case err != nil:
		t.Fatalf("context version %d has no recorded rules: %v", host.ContextVersion, err)
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
	t.Fatalf("the context rules are not those of context version %d, which runs of it store; bump host.ContextVersion and record the rules of the new version with -record-context. Changed:\n%s",
		host.ContextVersion, strings.Join(changed, "\n"))
}

func contextRules(t *testing.T) string {
	t.Helper()
	spec := runSpec(t)
	run := harness.Run{ID: "run_fixed", Goal: "Exact task\n", Constraints: "Exact restriction\n"}
	cfg := host.ContextConfig(spec.Config, spec.Budget)
	var b strings.Builder
	defaults, _ := json.Marshal(struct {
		Config         host.RunConfig
		Limits         harness.PromptLimits
		MaximumRequest int
		HandoverBytes  int
		IndexSteps     int
		IndexBytes     int
		IndexPurpose   int
		ReadBytes      int
		MaxRunID       int
	}{spec.Config, cfg.Limits, harness.MaxRequestBytes, harness.HandoverBytes, harness.IndexSteps, harness.IndexBytes, harness.IndexPurposeBytes, archive.MaxBytes, harness.MaxRunIDBytes})
	fmt.Fprintf(&b, "context version %d\nsemantics %d\ndefaults %s\nactions %q %q\n", host.ContextVersion, core.SemanticsVersion, defaults, harness.RetrievePrefix, harness.HandoverPrefix)
	text := "frozen original evidence"
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(text)))
	retrieval := archive.Retrieval{Query: archive.RetrievalQuery{Operation: "read", Path: archive.ImportsRoot + "/notes", Reason: "recover"}, Sources: []archive.SourceRange{{Path: archive.ImportsRoot + "/notes", Length: len(text), SHA256: sum}}, Evidence: []string{text}, More: true, NextOffset: int64(len(text))}
	latest := harness.StepRecord{Seq: 5, Step: 1, Command: harness.RetrievePrefix + `{"operation":"read"}`, Outcome: &harness.Outcome{Status: harness.OutcomeDone, Retrieval: &retrieval}}
	tokens := int64(1234)
	zero := 0
	for _, tool := range []bool{false, true} {
		initial := harness.Frame{Goal: run.Goal, Constraints: run.Constraints, Environment: host.Environment(spec.Config, spec.Budget), RunID: run.ID, Step: 1, MaxSteps: 20, Attempt: 1, MaxAttempts: 4, DecisionTool: tool, RetrievalEnabled: true, ImportsEnabled: true, ContextState: &harness.ContextState{FirstStep: 1}}
		frames := []harness.Frame{initial}
		persistent := initial
		persistent.NativeArchive = true
		persistent.Environment = host.Environment(host.RunConfig{Environment: "/environment", EnvironmentWorkdir: "/workspace"}, spec.Budget)
		frames = append(frames, persistent)
		recent := initial
		recent.Step = 2
		recent.Steps = []harness.StepRecord{latest}
		recent.PreviousUsage = &tokens
		frames = append(frames, recent)
		instructed := recent
		instructed.Instructions = []harness.Instruction{{Index: 1, Text: "Apply the latest instruction.", Pos: 10, Seq: 6, Step: 1}}
		frames = append(frames, instructed)
		anchored := initial
		anchored.Step = 3
		anchored.ContextState = &harness.ContextState{AnchorSeq: 8, FirstStep: 3}
		frames = append(frames, anchored)
		rejected := harness.ContextState{AnchorSeq: 9, FirstStep: 2,
			Rejection: &harness.RejectionRef{Seq: 9, Step: 1, MaxAcceptedInput: 4096, MaxAcceptedReported: true}}
		reset := initial
		reset.Step = 2
		reset.Attempt = 2
		reset.ContextState = &rejected
		reset.Index = []harness.StepReceipt{{Seq: 5, Step: 1, Purpose: "Inspect original", Status: "done", ExitCode: &zero, Retrieval: &harness.ReceiptRetrieval{Operation: "read", Path: archive.ImportsRoot + "/notes", Offset: 0, Limit: 4096}}}
		reset.IndexTotal = 1
		reset.Latest = &latest
		frames = append(frames, reset)
		repair := reset
		repair.Attempt = 3
		repair.Rejected = &harness.Rejection{Error: "invalid decision", Response: `{"wrong":true}`}
		frames = append(frames, repair)
		minimal := reset
		minimal.ContextState = &harness.ContextState{AnchorSeq: 9, FirstStep: 2, Minimal: true, Rejection: rejected.Rejection}
		minimal.Attempt = 3
		minimal.Index, minimal.IndexTotal, minimal.Previous = nil, 0, nil
		frames = append(frames, minimal)
		for i, frame := range frames {
			prompt, err := frame.Prompt(cfg.Limits)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "transport tool=%t frame=%d stable=%d system=%s\n", tool, i, prompt.Stable, digest(prompt.System))
			for j, block := range prompt.Blocks {
				fmt.Fprintf(&b, " block %d %s\n", j, digest(block))
			}
		}
	}
	b.WriteString(runtimeMappingRules(t))
	b.WriteString(storedRules(t))
	b.WriteString(corpusRules(t))
	return b.String()
}

func corpusRules(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..", "..", "internal", "harness", "testdata", "frozen-requests-v20")
	scenarios, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, sc := range scenarios {
		fmt.Fprintf(&b, "corpus %s: sha256 %s\n", sc.Name(), corpusDigest(t, filepath.Join(root, sc.Name())))
	}
	return b.String()
}

// corpusDigest digests the files of the scenario directory dir as the
// recorded context rules do.
func corpusDigest(t *testing.T, dir string) string {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "%s %d\n%s", f.Name(), len(data), data)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// TestEarlierVersionsKeepTheirCorpus requires that every scenario a later
// context version renders anew keeps, under frozen-requests-v<version>, the
// requests and journal that its earlier version froze: the files whose
// digest that version's recorded rules hold, never rewritten.
func TestEarlierVersionsKeepTheirCorpus(t *testing.T) {
	root := filepath.Join("..", "..", "internal", "harness", "testdata")
	dirs, err := filepath.Glob(filepath.Join(root, "frozen-requests-v*"))
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no retained corpus of an earlier version: %v", err)
	}
	dirs = append(dirs, filepath.Join(root, "frozen-requests"))
	for _, dir := range dirs {
		version := strings.TrimPrefix(filepath.Base(dir), "frozen-requests-v")
		if filepath.Base(dir) == "frozen-requests" {
			version = "16"
		}
		rules, err := os.ReadFile(filepath.Join("testdata", "context-v"+version+".txt"))
		if err != nil {
			t.Fatalf("retained corpus %s has no recorded context version: %v", dir, err)
		}
		scenarios, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, sc := range scenarios {
			retained := corpusDigest(t, filepath.Join(dir, sc.Name()))
			if !strings.Contains(string(rules), fmt.Sprintf("corpus %s: sha256 %s\n", sc.Name(), retained)) {
				t.Errorf("version %s retains %s as sha256 %s, which its recorded rules do not identify", version, sc.Name(), retained)
			}
		}
	}
}

func digest(s string) string {
	return fmt.Sprintf("%d bytes, sha256 %x", len(s), sha256.Sum256([]byte(s)))
}

// runtimeMappingRules pins actual runtime environment and model construction,
// including both APIs, transports and non-default options. It records behavior,
// independently of the handcrafted frames that isolate block composition.
func runtimeMappingRules(t *testing.T) string {
	t.Helper()
	run := harness.Run{ID: "run_fixed", Goal: "Complete the task.\n", Constraints: "Preserve evidence.\n"}
	var b strings.Builder
	transports := []host.RunConfig{
		{API: host.APIMessages, DecisionFormat: "text", Effort: "high", Structured: true, Fallbacks: true},
		{API: host.APIMessages, DecisionFormat: "text"},
		{API: host.APIMessages, DecisionFormat: "tool", Effort: "high", Fallbacks: true},
		{API: host.APIResponses, DecisionFormat: "text", Effort: "high", Structured: true},
		{API: host.APIResponses, DecisionFormat: "text"},
		{API: host.APIResponses, DecisionFormat: "tool", Effort: "high"},
	}
	base := runSpec(t)
	for _, enabled := range [][2]bool{{false, false}, {true, false}, {true, true}} {
		for _, transport := range transports {
			c := base.Config
			c.API, c.DecisionFormat, c.Effort, c.Structured, c.Fallbacks = transport.API, transport.DecisionFormat, transport.Effort, transport.Structured, transport.Fallbacks
			c.Model, c.MaxTokens = "test-model", 4096
			loop := host.ContextConfig(c, base.Budget)
			frame := harness.Frame{Goal: run.Goal, Constraints: run.Constraints, RunID: run.ID, Environment: loop.Environment, Step: 1, MaxSteps: base.Limits.MaxSteps, Attempt: 1, MaxAttempts: base.Limits.MaxAttempts, ContextState: &harness.ContextState{FirstStep: 1}, DecisionTool: loop.DecisionTool, NativeArchive: loop.NativeArchive, RetrievalEnabled: enabled[0], ImportsEnabled: enabled[1]}
			prompt, err := frame.Prompt(loop.Limits)
			if err != nil {
				t.Fatal(err)
			}
			model, err := newModel(c, "https://model.test", host.Keys{AnthropicKey: "test-key", OpenAIKey: "test-key"})
			if err != nil {
				t.Fatal(err)
			}
			request, err := model.Freeze(prompt)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "runtime transport %s %s effort=%q structured=%t fallbacks=%t retrieval=%t imports=%t: system %s; %s %s %s\n", c.API, c.DecisionFormat, c.Effort, c.Structured, c.Fallbacks, enabled[0], enabled[1], digest(prompt.System), request.Protocol, request.Adapter, digest(string(request.Body)))
		}
	}
	imports := t.TempDir()
	variants := [][]string{nil, {"-history-imports", imports}, {"-max-steps", "0", "-max-attempts", "1"}, {"-pids", "0", "-job-timeout", "0", "-memory", "", "-cpus", "4", "-network", "bridge"}, {"-image", "custom-image:tag", "-output-limit", "4096", "-job-timeout", "1s"}, {"-image", strings.Repeat("x", 2047)}, {"-image", "debian\xff"}, {"-api", "anthropic-messages", "-model", "test-model", "-effort", "max", "-fallbacks"}, {"-api", "openai-responses", "-model", "test-model", "-max-tokens", "4096", "-effort", "none", "-structured=false", "-decision-format", "tool", "-request-bytes", "131072"}}
	type configCase struct {
		label string
		spec  host.Spec
	}
	var configs []configCase
	for _, flags := range variants {
		label := strings.ReplaceAll(strings.Join(flags, " "), imports, "DIR")
		configs = append(configs, configCase{label, parseSpec(t, flags...)})
	}
	// Stored synthetic wiring exercises persistent rendering without admission
	// or Docker. Its fixed identity keeps the request bytes reproducible.
	attached := base
	attached.Config.Ephemeral = false
	attached.Config.Environment, attached.Config.EnvironmentWorkdir = "/fixed/environment", "/workspace/project"
	attached.Config.Image, attached.Config.Network, attached.Config.Memory, attached.Config.CPUs, attached.Config.Pids = "", "", "", "", 0
	attached.Config.Attachment = &docker.Attachment{ContainerID: strings.Repeat("c", 64), Incarnation: 7}
	configs = append(configs, configCase{"attached environment", attached})
	for _, config := range configs {
		spec := config.spec
		loop := host.ContextConfig(spec.Config, spec.Budget)
		frame := harness.Frame{Goal: run.Goal, Constraints: run.Constraints, RunID: run.ID, Environment: loop.Environment, Step: 1, MaxSteps: spec.Limits.MaxSteps, Attempt: 1, MaxAttempts: spec.Limits.MaxAttempts, ContextState: &harness.ContextState{FirstStep: 1}, DecisionTool: loop.DecisionTool, NativeArchive: loop.NativeArchive, RetrievalEnabled: loop.Reader != nil, ImportsEnabled: loop.Reader != nil && loop.Reader.Imports != ""}
		prompt, err := frame.Prompt(loop.Limits)
		encoded, _ := json.Marshal(struct {
			Limits                harness.PromptLimits
			Core                  core.Limits
			Tool, Reader, Imports bool
			NativeArchive         bool `json:",omitempty"`
		}{loop.Limits, spec.Limits, loop.DecisionTool, loop.Reader != nil, loop.Reader != nil && loop.Reader.Imports != "", loop.NativeArchive})
		fmt.Fprintf(&b, "runtime config %q: %s; environment %s; prompt %s; error %v\n", config.label, encoded, digest(host.Environment(spec.Config, spec.Budget)), digest(prompt.System+prompt.User()), err)
	}
	return b.String()
}

// storedRules pins actual CLI run/resume requests built from saved options,
// including native evidence, provider reset, protocol repair and >64 receipts.
func storedRules(t *testing.T) string {
	t.Helper()
	read := core.Decision{Kind: core.Exec, Payload: harness.RetrievePrefix + `{"operation":"read","path":"/canti/imports/notes.txt","reason":"Read the notes."}`}
	imports := t.TempDir()
	if err := os.WriteFile(filepath.Join(imports, "notes.txt"), []byte("The parser was rewritten in v2.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, map[int]core.Decision{1: read, 2: {Kind: core.Return, Payload: "done"}})
	f.model.answer(2, 1, reply{status: 400, body: `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}`})
	if r := f.cli(nil, "run", "-once", "-goal", "Report what the notes say.", "-model", "claude-sonnet-5", "-max-tokens", "4096", "-effort", "low", "-structured=false",
		"-fallbacks", "-request-bytes", "131072",
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
	for _, format := range []string{"text", "tool"} {
		f = newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
		openaiKey := []string{"OPENAI_API_KEY=" + f.key}
		if format == "tool" {
			f.model.answer(1, 1, reply{status: http.StatusOK, body: responsesToolReply(core.Decision{Kind: core.Return, Purpose: "Report.", Payload: "done"})})
		}
		if r := f.cli(openaiKey, "run", "-once", "-goal", "Report done.", "-api", "openai-responses", "-effort", "high", "-decision-format", format); r.code != exitActive {
			t.Fatalf("run -once of the %s Responses run = %d\nstderr: %s", format, r.code, r.stderr)
		}
		if r := f.cli(openaiKey, "resume"); r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("resume of the %s Responses run = %d\nstdout: %s\nstderr: %s", format, r.code, r.stdout, r.stderr)
		}
		b += frozenRequests(t, f, "stored "+format+" Responses run")
	}
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
	return b + fmt.Sprintf("long receipt history: %d requests, sha256 %x\n", len(f.model.requests()), h.Sum(nil))
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
	if err != nil {
		t.Fatal(err)
	}
	var all []harness.Transition
	for after := int64(0); ; {
		page, err := store.Transitions(context.Background(), last.ID, after, 64)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page...)
		if len(page) < 64 {
			break
		}
		after = page[len(page)-1].Seq
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
