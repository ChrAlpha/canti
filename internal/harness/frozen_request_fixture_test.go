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

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// corpusModel is a scripted Anthropic Messages endpoint. It records every
// request body it receives and answers with the next scripted decision, as
// a text block or, for the tool transport, as the canti_decision tool input,
// or with the body that follows "raw:".
type corpusModel struct {
	mu             sync.Mutex
	tool           bool
	replies        []string
	received       [][]byte
	summaries      [][]byte
	summaryReplies []string
}

func (m *corpusModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	var request struct {
		Messages []struct{ Content []struct{ Text string } }
	}
	_ = json.Unmarshal(body, &request)
	if len(request.Messages) == 2 && len(request.Messages[1].Content) > 0 && strings.HasPrefix(request.Messages[1].Content[0].Text, harness.CompactionPromptPrefix) {
		m.mu.Lock()
		m.summaries = append(m.summaries, body)
		reply := ""
		if len(m.summaryReplies) > 0 {
			reply, m.summaryReplies = m.summaryReplies[0], m.summaryReplies[1:]
		}
		m.mu.Unlock()
		if reply != "" {
			w.Header().Set("content-type", "application/json")
			io.WriteString(w, reply)
			return
		}
		w.Header().Set("content-type", "application/json")
		d := core.Decision{Kind: core.Return, Purpose: "Preserve continuation context.", Payload: "Continue the user task, preserve its constraints, and verify original evidence."}
		stop := "end_turn"
		raw, _ := json.Marshal(d)
		content := []map[string]any{{"type": "text", "text": string(raw)}}
		if m.tool {
			stop = "tool_use"
			content = []map[string]any{{"type": "tool_use", "id": "summary", "name": "canti_decision", "input": d}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"stop_reason": stop, "model": "claude-opus-5", "content": content, "usage": map[string]int{"input_tokens": 100, "output_tokens": 20, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}})
		return
	}
	m.mu.Lock()
	m.received = append(m.received, body)
	n := len(m.received)
	reply, scripted := "", err == nil && len(m.replies) > 0
	if scripted {
		reply, m.replies = m.replies[0], m.replies[1:]
	}
	m.mu.Unlock()
	w.Header().Set("content-type", "application/json")
	if body, ok := strings.CutPrefix(reply, "raw:"); ok {
		io.WriteString(w, body)
		return
	}
	if !scripted {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"unscripted request"}}`)
		return
	}
	content, stop := fmt.Sprintf(`[{"type":"tool_use","id":"toolu_%02d","name":"canti_decision","input":%s}]`, n, reply), "tool_use"
	if !m.tool {
		text, _ := json.Marshal(reply)
		content, stop = `[{"type":"text","text":`+string(text)+`}]`, "end_turn"
	}
	fmt.Fprintf(w, `{"id":"msg_%02d","type":"message","role":"assistant","model":"claude-opus-5","content":%s,"stop_reason":%q,`+
		`"usage":{"input_tokens":1000,"output_tokens":20}}`, n, content, stop)
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

func (x *corpusExecutor) Bind(_ context.Context, e harness.Effect) (harness.Job, error) {
	if j := x.jobs[e.ID]; j != nil {
		return *j, nil
	}
	x.jobs[e.ID] = &harness.Job{ID: "job-" + strings.TrimPrefix(e.ID, "effect_")[:12], Status: harness.JobPending}
	x.commands[e.ID] = e.Command
	return *x.jobs[e.ID], nil
}

func (x *corpusExecutor) Start(_ context.Context, effectID string) (harness.Job, error) {
	j := x.jobs[effectID]
	if j == nil {
		return harness.Job{}, errors.New("unbound effect")
	}
	if j.Status == harness.JobPending {
		j.Status = harness.JobRunning
	}
	return *j, nil
}

func (x *corpusExecutor) Inspect(_ context.Context, effectID string) (harness.Job, bool, error) {
	j := x.jobs[effectID]
	if j == nil {
		return harness.Job{}, false, nil
	}
	if final, scripted := x.finals[x.commands[effectID]]; scripted && j.Status == harness.JobRunning {
		output := x.outputs[x.commands[effectID]]
		final.ID, final.Preview = j.ID, cmp.Or(final.Preview, output)
		if final.Output.Available {
			final.Output.Path, final.Output.Bytes = effectID+"/output", int64(len(output))
			if err := os.MkdirAll(filepath.Join(x.artifacts, effectID), 0o755); err != nil {
				return harness.Job{}, false, err
			}
			if err := os.WriteFile(filepath.Join(x.artifacts, final.Output.Path), []byte(output), 0o644); err != nil {
				return harness.Job{}, false, err
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
			return harness.Job{}, false, err
		}
	}
	if j.Status == harness.JobRunning {
		output, code := x.outputs[x.commands[effectID]], 0
		if output == "" {
			output, code = "unscripted command\n", 127
		}
		rel := effectID + "/output"
		if err := os.MkdirAll(filepath.Join(x.artifacts, effectID), 0o755); err != nil {
			return harness.Job{}, false, err
		}
		if err := os.WriteFile(filepath.Join(x.artifacts, rel), []byte(output), 0o644); err != nil {
			return harness.Job{}, false, err
		}
		j.Status, j.ExitCode, j.Preview = harness.JobSucceeded, &code, output
		j.Output = harness.ArtifactRef{Path: rel, Bytes: int64(len(output)), Complete: true, Available: true}
	}
	return *j, true, nil
}

func (x *corpusExecutor) Cancel(_ context.Context, effectID string) error {
	if j := x.jobs[effectID]; j != nil && (j.Status == harness.JobPending || j.Status == harness.JobRunning) {
		j.Status = harness.JobCancelled
	}
	return nil
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
	model := &corpusModel{tool: sc.tool, summaryReplies: sc.summaryReplies}
	server := httptest.NewServer(model)
	t.Cleanup(server.Close)
	format := ""
	if sc.tool {
		format = "tool"
	}
	m, err := anthropic.New(anthropic.Config{BaseURL: server.URL, APIKey: "corpus-key", Model: "claude-opus-5", MaxTokens: 16000,
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
	loop, err := harness.NewLoop(store, x, m, harness.LoopConfig{
		Environment: corpusEnvironment, History: history,
		Limits: limits, Poll: time.Millisecond, Backoff: -1, Now: func() time.Time { return corpusCreated },
		Reader: reader, DecisionTool: sc.tool,
		CompactSteps: sc.windowSteps, CompactBytes: sc.windowBytes, SummaryBytes: 2048,
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
		record := harness.Run{ID: run.id, Goal: run.goal, Constraints: cmp.Or(run.constraints, "Do not modify files outside /workspace."), Budget: run.budget, Created: corpusCreated}
		if len(sc.runs) > 1 {
			record.SessionID, record.InputID = "session-1", fmt.Sprintf("input-%d", i+1)
			_, err = r.store.Submit(ctx, record, state)
		} else {
			err = r.store.CreateRun(ctx, record, state)
		}
		if err != nil {
			t.Fatal(err)
		}
		r.model.mu.Lock()
		r.model.replies = append(r.model.replies, run.replies...)
		r.model.mu.Unlock()
		snap, err := r.loop.Run(ctx, run.id)
		if err == nil && run.resolve != nil && snap.State.Phase == core.PhaseNeedsResolution {
			if _, err = r.loop.Resolve(ctx, run.id, *run.resolve); err == nil {
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

func (r *corpusRig) checkSteps(t *testing.T, runID string) {
	t.Helper()
	steps, err := r.store.Recent(context.Background(), runID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range steps {
		o := s.Observation
		if o.Execution != core.ExecutionExited || o.ExitCode == nil || *o.ExitCode != 0 {
			t.Errorf("run %s step %d did not succeed: %+v", runID, s.Step, o)
		}
		if q := o.Retrieval; q != nil && (q.Error != "" || len(q.Sources)+len(q.Entries) == 0) {
			t.Errorf("run %s step %d retrieval found nothing: %+v", runID, s.Step, q)
		}
	}
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
