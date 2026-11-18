package harness_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

// corpusModel is a scripted Anthropic Messages endpoint. It records every
// request body it receives and answers with the next scripted decision, as
// a text block or, for the tool transport, as the canti_decision tool input,
// or with the body that follows "raw:".
type corpusModel struct {
	mu       sync.Mutex
	tool     bool
	replies  []string
	received [][]byte
}

func (m *corpusModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	m.mu.Lock()
	m.received = append(m.received, body)
	n := len(m.received)
	reply, scripted := "", err == nil && len(m.replies) > 0
	if scripted {
		reply, m.replies = m.replies[0], m.replies[1:]
	}
	m.mu.Unlock()
	if body, ok := strings.CutPrefix(reply, "raw:"); ok {
		w.Header().Set("content-type", "text/event-stream")
		streamMessage(w, body)
		return
	}
	if !scripted {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"unscripted request"}}`)
		return
	}
	content, stop := fmt.Sprintf(`[{"type":"tool_use","id":"toolu_%02d","name":"canti_decision","input":%s}]`, n, reply), "tool_use"
	if !m.tool {
		text, _ := json.Marshal(reply)
		content, stop = `[{"type":"text","text":`+string(text)+`}]`, "end_turn"
	}
	message := fmt.Sprintf(`{"id":"msg_%02d","type":"message","role":"assistant","model":"claude-opus-5","content":%s,"stop_reason":%q,`+
		`"usage":{"input_tokens":1000,"output_tokens":20}}`, n, content, stop)
	w.Header().Set("content-type", "text/event-stream")
	streamAssembledMessage(w, message)
}

// streamAssembledMessage turns a whole Messages object into the real block
// and delta events. The raw-envelope hook above remains only for the
// historical malformed-envelope boundary tests.
func streamAssembledMessage(w http.ResponseWriter, message string) {
	var full struct {
		ID           string            `json:"id"`
		Type         string            `json:"type"`
		Role         string            `json:"role"`
		Model        string            `json:"model"`
		Content      []json.RawMessage `json:"content"`
		StopReason   json.RawMessage   `json:"stop_reason"`
		StopSequence json.RawMessage   `json:"stop_sequence"`
		Usage        json.RawMessage   `json:"usage"`
	}
	if json.Unmarshal([]byte(message), &full) != nil {
		streamMessage(w, message)
		return
	}
	start := map[string]any{"id": full.ID, "type": full.Type, "role": full.Role, "model": full.Model, "content": []any{}, "stop_reason": nil}
	if len(full.Usage) > 0 {
		start["usage"] = full.Usage
	}
	startData, _ := json.Marshal(start)
	fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":%s}\n\n", startData)
	for index, raw := range full.Content {
		var block struct {
			Type      string          `json:"type"`
			Text      string          `json:"text"`
			Thinking  string          `json:"thinking"`
			Signature string          `json:"signature"`
			ID        string          `json:"id"`
			Name      string          `json:"name"`
			Input     json.RawMessage `json:"input"`
		}
		_ = json.Unmarshal(raw, &block)
		switch block.Type {
		case "text":
			corpusBlockStart(w, index, map[string]any{"type": "text", "text": ""})
			corpusTextDeltas(w, index, block.Text)
		case "thinking":
			corpusBlockStart(w, index, map[string]any{"type": "thinking", "thinking": "", "signature": ""})
			corpusThinkingDeltas(w, index, block.Thinking)
			if block.Signature != "" {
				signature, _ := json.Marshal(block.Signature)
				fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"signature_delta\",\"signature\":%s}}\n\n", index, signature)
			}
		case "tool_use":
			corpusBlockStart(w, index, map[string]any{"type": "tool_use", "id": block.ID, "name": block.Name, "input": map[string]any{}})
			input := block.Input
			if len(input) == 0 {
				input = json.RawMessage(`{}`)
			}
			corpusInputDeltas(w, index, string(input))
		default:
			fmt.Fprintf(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":%d,\"content_block\":%s}\n\n", index, raw)
		}
		fmt.Fprintf(w, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":%d}\n\n", index)
	}
	delta := map[string]any{"stop_reason": nil}
	if len(full.StopReason) > 0 && string(full.StopReason) != "null" {
		delta["stop_reason"] = full.StopReason
	}
	if len(full.StopSequence) > 0 && string(full.StopSequence) != "null" {
		delta["stop_sequence"] = full.StopSequence
	}
	messageDelta := map[string]any{"delta": delta}
	if len(full.Usage) > 0 {
		messageDelta["usage"] = full.Usage
	}
	deltaData, _ := json.Marshal(messageDelta)
	fmt.Fprintf(w, "event: message_delta\ndata: {\"type\":\"message_delta\",%s}\n\n", deltaData[1:len(deltaData)-1])
	io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func corpusBlockStart(w http.ResponseWriter, index int, block any) {
	raw, _ := json.Marshal(block)
	fmt.Fprintf(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":%d,\"content_block\":%s}\n\n", index, raw)
}

func corpusTextDeltas(w http.ResponseWriter, index int, text string) {
	runes := []rune(text)
	parts := []string{text}
	if len(runes) > 1 {
		middle := len(runes) / 2
		parts = []string{string(runes[:middle]), string(runes[middle:])}
	}
	for _, part := range parts {
		raw, _ := json.Marshal(part)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", index, raw)
	}
}

func corpusThinkingDeltas(w http.ResponseWriter, index int, text string) {
	runes := []rune(text)
	parts := []string{text}
	if len(runes) > 1 {
		middle := len(runes) / 2
		parts = []string{string(runes[:middle]), string(runes[middle:])}
	}
	for _, part := range parts {
		raw, _ := json.Marshal(part)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":%s}}\n\n", index, raw)
	}
}

func corpusInputDeltas(w http.ResponseWriter, index int, input string) {
	runes := []rune(input)
	parts := []string{input}
	if len(runes) > 1 {
		middle := len(runes) / 2
		parts = []string{string(runes[:middle]), string(runes[middle:])}
	}
	for _, part := range parts {
		raw, _ := json.Marshal(part)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":%s}}\n\n", index, raw)
	}
}

// streamMessage carries a whole Messages object in message_start and follows
// it with an empty message_delta and message_stop. The adapter returns the
// exact start bytes when no delta changes them, so the corpus keeps the raw
// envelopes it tests.
func streamMessage(w http.ResponseWriter, message string) {
	fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":%s}\n\n", message)
	io.WriteString(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":null}\n\n")
	io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
}

func (m *corpusModel) sent() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.received
}

// corpusExecutor finishes a started job at its first inspection with the
// scripted output of its command, which it writes as the job's artifact.
// A command in finals ends as that job instead, with its output as the
// preview, unless the job sets one, and as the artifact if it is available.
type corpusExecutor struct {
	artifacts string
	imports   string
	outputs   map[string]string
	jobs      map[string]*harness.Job
	commands  map[string]string
	finals    map[string]harness.Job
	changes   map[string]func(*os.Root) error
}

func (x *corpusExecutor) Specification(command string) string {
	return harness.ConfigDigest([]byte("corpus:" + command))
}
func (x *corpusExecutor) Drive(ctx context.Context, e harness.Effect) (harness.Job, error) {
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	if e.Spec != x.Specification(e.Command) {
		return harness.Job{}, errors.New("changed spec")
	}
	if x.jobs[e.ID] == nil {
		x.jobs[e.ID] = &harness.Job{ID: "job-" + strings.TrimPrefix(e.ID, "effect_")[:12], EffectID: e.ID, Spec: e.Spec, Status: harness.JobRunning, StartedAt: corpusCreated}
		x.commands[e.ID] = e.Command
	}
	return x.Inspect(ctx, e)
}

func (x *corpusExecutor) Inspect(_ context.Context, e harness.Effect) (harness.Job, error) {
	effectID := e.ID
	j := x.jobs[effectID]
	if j == nil {
		return harness.Job{EffectID: e.ID, Spec: e.Spec, Status: harness.JobAbsent}, nil
	}
	if final, scripted := x.finals[x.commands[effectID]]; scripted && j.Status == harness.JobRunning {
		output := x.outputs[x.commands[effectID]]
		final.ID, final.Preview = j.ID, cmp.Or(final.Preview, output)
		final.EffectID, final.Spec = e.ID, e.Spec
		if final.Status == harness.JobSucceeded || final.Status == harness.JobFailed {
			final.StartedAt, final.FinishedAt = corpusCreated, corpusCreated
		}
		if final.Status == harness.JobCancelled {
			final.Fenced = true
			if final.StartedAt.IsZero() {
				final.StartedAt = corpusCreated
			}
			if final.FinishedAt.IsZero() {
				final.FinishedAt = corpusCreated
			}
			if final.ExitCode == nil {
				code := 143
				final.ExitCode = &code
			}
		}
		if final.Output.Available {
			final.Output.Path, final.Output.Bytes = effectID+"/output", int64(len(output))
			if err := os.MkdirAll(filepath.Join(x.artifacts, effectID), 0o755); err != nil {
				return harness.Job{}, err
			}
			if err := os.WriteFile(filepath.Join(x.artifacts, final.Output.Path), []byte(output), 0o644); err != nil {
				return harness.Job{}, err
			}
		}
		*j = final
	}
	if change := x.changes[x.commands[effectID]]; change != nil && j.Status == harness.JobRunning {
		root, err := os.OpenRoot(x.imports)
		if err == nil {
			err = change(root)
			root.Close()
		}
		if err != nil {
			return harness.Job{}, err
		}
	}
	if j.Status == harness.JobRunning {
		output, code := x.outputs[x.commands[effectID]], 0
		if output == "" {
			output, code = "unscripted command\n", 127
		}
		rel := effectID + "/output"
		if err := os.MkdirAll(filepath.Join(x.artifacts, effectID), 0o755); err != nil {
			return harness.Job{}, err
		}
		if err := os.WriteFile(filepath.Join(x.artifacts, rel), []byte(output), 0o644); err != nil {
			return harness.Job{}, err
		}
		j.Status, j.ExitCode, j.Preview = harness.JobSucceeded, &code, output
		j.StartedAt, j.FinishedAt = corpusCreated, corpusCreated
		j.Output = harness.ArtifactRef{Path: rel, Bytes: int64(len(output)), Complete: true, Available: true}
	}
	j.EffectID, j.Spec = e.ID, e.Spec
	return *j, nil
}

func (x *corpusExecutor) Cancel(ctx context.Context, e harness.Effect) (harness.Job, error) {
	if j := x.jobs[e.ID]; j != nil && (j.Status == harness.JobPending || j.Status == harness.JobRunning) {
		j.Status, j.Fenced = harness.JobCancelled, true
	}
	return x.Inspect(ctx, e)
}

var corpusCreated = time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)

const corpusEnvironment = "Each command runs in a new Linux container from image debian:bookworm-slim with working directory /workspace, " +
	"which is read-write and persists between steps. Network: none. Per-command limits: memory 2g, CPUs unlimited, processes 512, run time 10m0s."

// corpusRig configures its loop as the CLI does, over a real SQLite journal
// and the Anthropic adapter.
type corpusRig struct {
	store *sqlite.Store
	model *corpusModel
	loop  *harness.Loop
	path  string
}

func newCorpusRig(t *testing.T, sc frozenScenario) *corpusRig {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "canti.db")
	store, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	model := &corpusModel{tool: sc.tool}
	server := httptest.NewServer(model)
	t.Cleanup(server.Close)
	format := ""
	if sc.tool {
		format = "tool"
	}
	m, err := anthropic.New(anthropic.Config{BaseURL: server.URL, APIKey: "corpus-credential-key", Model: "claude-opus-5", MaxTokens: 16000,
		StructuredOutput: true, DecisionFormat: format, Fallbacks: true})
	if err != nil {
		t.Fatal(err)
	}
	history, artifacts := filepath.Join(dir, "history"), filepath.Join(dir, "artifacts")
	x := &corpusExecutor{artifacts: artifacts, outputs: sc.outputs, jobs: map[string]*harness.Job{}, commands: map[string]string{}, finals: sc.finals, changes: sc.changes}
	reader := &archive.Reader{History: history, Artifacts: artifacts}
	if sc.imports != nil {
		reader.Imports, x.imports = filepath.Join(dir, "imports"), filepath.Join(dir, "imports")
		if err := os.Mkdir(reader.Imports, 0o755); err != nil {
			t.Fatal(err)
		}
		root, err := os.OpenRoot(reader.Imports)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		for name, data := range sc.imports {
			if err := root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := root.WriteFile(name, []byte(data), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	limits := harness.DefaultPromptLimits()
	limits.Total = cmp.Or(sc.contextBytes, 256<<10)
	environment := corpusEnvironment
	if sc.nativeArchive {
		environment = "Commands run in fresh shells in a persistent Linux environment with working directory /workspace. Files, installed tools, HOME and background services persist across commands, runs and context resets. Archive paths are logical native retrieval addresses, not shell mounts."
	}
	loop, err := harness.NewLoop(store, x, m, harness.LoopConfig{
		Environment: environment, NativeArchive: sc.nativeArchive, History: history,
		Limits: limits, Poll: time.Millisecond, Backoff: -1, Now: func() time.Time { return corpusCreated },
		Reader: reader, DecisionTool: sc.tool,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &corpusRig{store: store, model: model, loop: loop, path: path}
}

func (r *corpusRig) drive(t *testing.T, sc frozenScenario) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i, run := range sc.runs {
		state, err := core.NewState(run.id, sc.limits)
		if err != nil {
			t.Fatal(err)
		}
		record := harness.Run{ID: run.id, Goal: run.goal, Constraints: cmp.Or(run.constraints, "Do not modify files outside /workspace."), Budget: run.budget, Created: corpusCreated, Fresh: i == 0 || run.fresh}
		if err := r.store.CreateRunWithProject(ctx, record, state, run.project); err != nil {
			t.Fatal(err)
		}
		for _, text := range run.steers {
			if _, err := r.store.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityPrincipal, Delivery: harness.DeliverySteer, Text: text, Now: corpusCreated}); err != nil {
				t.Fatal(err)
			}
		}
		if i == 0 {
			for _, observation := range sc.observations {
				if _, err := r.store.Admit(ctx, observation); err != nil {
					t.Fatal(err)
				}
			}
		}
		r.model.mu.Lock()
		r.model.replies = append(r.model.replies, run.replies...)
		r.model.mu.Unlock()
		snap, err := r.loop.Run(ctx, run.id)
		if err == nil && run.resolve != nil && snap.State.Phase == core.PhaseNeedsResolution {
			if _, err = r.loop.Resolve(ctx, run.id, *run.resolve, "operator-owner"); err == nil {
				snap, err = r.loop.Run(ctx, run.id)
			}
		}
		phase := core.PhaseReturned
		if run.stop != "" {
			phase = core.PhaseStopped
		}
		if err != nil || snap.State.Phase != phase || snap.State.Reason != run.stop {
			t.Fatalf("run %s halted at %+v, %v", run.id, snap.State, err)
		}
	}
}

func (r *corpusRig) frozen(t *testing.T, runID string) []harness.Transition {
	t.Helper()
	all, err := r.store.Transitions(context.Background(), runID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []harness.Transition
	for _, tr := range all {
		if tr.Facts.Request != nil {
			out = append(out, tr)
		}
	}
	return out
}

func promptText(t *testing.T, body []byte) string {
	t.Helper()
	var b struct {
		System   string `json:"system"`
		Messages []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &b); err != nil || len(b.Messages) != 1 {
		t.Fatalf("request body is not one Messages request: %v", err)
	}
	var text strings.Builder
	text.WriteString(b.System)
	for _, c := range b.Messages[0].Content {
		text.WriteString(c.Text)
	}
	return text.String()
}
