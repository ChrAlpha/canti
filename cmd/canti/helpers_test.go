package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

// scriptedModel is an Anthropic Messages server that answers every request
// with the decision scripted for the step named in the prompt's progress
// line, and records every request body.
type scriptedModel struct {
	*httptest.Server
	key    string
	script map[int]core.Decision
	hold   chan struct{}
	mu     sync.Mutex
	bodies [][]byte
}

func newScriptedModel(t *testing.T, key string, script map[int]core.Decision) *scriptedModel {
	t.Helper()
	m := &scriptedModel{key: key, script: script}
	m.Server = httptest.NewServer(m)
	t.Cleanup(m.Close)
	return m
}

var progressLine = regexp.MustCompile(`(?m)^Step (\d+) of at most \d+, attempt \d+ of \d+\.$`)

func promptOf(body []byte) (step int, prompt string, err error) {
	var req struct {
		Messages []struct{ Content string } `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) != 1 {
		return 0, "", errors.New("request is not one user message")
	}
	prompt = req.Messages[0].Content
	m := progressLine.FindStringSubmatch(prompt)
	if m == nil {
		return 0, prompt, errors.New("prompt has no progress line")
	}
	step, err = strconv.Atoi(m[1])
	return step, prompt, err
}

func (m *scriptedModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	m.mu.Lock()
	m.bodies = append(m.bodies, body)
	hold := m.hold
	m.mu.Unlock()
	if hold != nil {
		select {
		case hold <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	w.Header().Set("content-type", "application/json")
	if r.Header.Get("x-api-key") != m.key || r.Header.Get("authorization") != "" {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"unexpected credentials"}}`)
		return
	}
	step, _, err := promptOf(body)
	d, ok := m.script[step]
	if err != nil || !ok {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"no scripted decision"}}`)
		return
	}
	text, _ := json.Marshal(d)
	json.NewEncoder(w).Encode(map[string]any{
		"id": "msg_test", "type": "message", "role": "assistant", "model": "claude-test", "stop_reason": "end_turn",
		"content": []map[string]string{{"type": "text", "text": string(text)}},
		"usage":   map[string]int{"input_tokens": 100, "output_tokens": 10},
	})
}

// holdRequests makes the model answer no request until its client goes
// away and report each one on the returned channel.
func (m *scriptedModel) holdRequests() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hold = make(chan struct{}, 1)
	return m.hold
}

func (m *scriptedModel) requests() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]byte(nil), m.bodies...)
}

// prompts returns the user prompt of every request for step, in order.
func (m *scriptedModel) prompts(step int) []string {
	var out []string
	for _, body := range m.requests() {
		if n, p, err := promptOf(body); err == nil && n == step {
			out = append(out, p)
		}
	}
	return out
}

type result struct {
	code           int
	stdout, stderr string
}

// runCLI runs the test binary as the CLI in a fresh process whose
// environment holds no model settings or credentials beyond env. A process
// killed by a signal reports code -1.
func runCLI(t *testing.T, env []string, args ...string) result {
	t.Helper()
	_, wait := startCLI(t, env, args...)
	return wait()
}

// startCLI starts the CLI process of runCLI and returns it with the wait
// for its result.
func startCLI(t *testing.T, env []string, args ...string) (*exec.Cmd, func() result) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	cmd := exec.CommandContext(ctx, exe, args...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if _, ok := os.LookupEnv("GORACE"); !ok {
		cmd.Env = append(cmd.Env, "GORACE=atexit_sleep_ms=0")
	}
	cmd.Env = append(append(cmd.Env, "CANTI_TEST_MAIN=1"), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("canti %q: %v", args, err)
	}
	return cmd, func() result {
		t.Helper()
		defer cancel()
		var exit *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exit) {
			t.Fatalf("canti %q: %v", args, err)
		}
		return result{cmd.ProcessState.ExitCode(), stdout.String(), stderr.String()}
	}
}

// fixture is one workspace and state directory driven against a scripted
// model with a fake API key.
type fixture struct {
	t                *testing.T
	workspace, state string
	key              string
	model            *scriptedModel
}

func newFixture(t *testing.T, script map[int]core.Decision) *fixture {
	t.Helper()
	root := t.TempDir()
	f := &fixture{t: t, workspace: filepath.Join(root, "workspace"), state: filepath.Join(root, "state"), key: "sk-test-" + rand.Text()}
	if err := os.Mkdir(f.workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	f.model = newScriptedModel(t, f.key, script)
	return f
}

// cli runs command with the fixture's workspace, state, credential, and
// model URL, plus env and args.
func (f *fixture) cli(env []string, command string, args ...string) result {
	f.t.Helper()
	_, wait := f.start(env, command, args...)
	return wait()
}

// start starts what cli runs.
func (f *fixture) start(env []string, command string, args ...string) (*exec.Cmd, func() result) {
	f.t.Helper()
	all := []string{command, "-workspace", f.workspace, "-state", f.state}
	if command == "run" || command == "resume" {
		all = append(all, "-model-url", f.model.URL)
	}
	return startCLI(f.t, append([]string{"ANTHROPIC_API_KEY=" + f.key}, env...), append(all, args...)...)
}

// status is the object the status command prints.
type status struct {
	Run struct {
		ID          string    `json:"id"`
		Goal        string    `json:"goal"`
		Constraints string    `json:"constraints"`
		Created     time.Time `json:"created"`
		Budget      struct {
			WallTime    string `json:"wall_time"`
			Tokens      int64  `json:"tokens"`
			OutputBytes int64  `json:"output_bytes"`
			JobTimeout  string `json:"job_timeout"`
		} `json:"budget"`
	} `json:"run"`
	Seq        int64           `json:"seq"`
	Phase      core.Phase      `json:"phase"`
	Intent     core.Intent     `json:"intent"`
	Step       int             `json:"step"`
	Attempt    int             `json:"attempt"`
	RequestRev int             `json:"request_rev"`
	Reason     core.StopReason `json:"reason"`
	Decision   *core.Decision  `json:"decision"`
	EffectID   string          `json:"effect_id"`
	JobID      string          `json:"job_id"`
	Totals     struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
		OutputBytes  int64 `json:"output_bytes"`
		Steps        int   `json:"steps"`
	} `json:"totals"`
}

// entry is one line the history command prints.
type entry struct {
	Seq     int64           `json:"seq"`
	EventID string          `json:"event_id"`
	Kind    string          `json:"kind"`
	Event   json.RawMessage `json:"event"`
	From    core.Phase      `json:"from"`
	To      core.Phase      `json:"to"`
	State   core.State      `json:"state"`
	Facts   harness.Facts   `json:"facts"`
}

func (f *fixture) status(args ...string) status {
	f.t.Helper()
	r := f.cli(nil, "status", args...)
	var s status
	if err := strict(r.stdout, &s); r.code != exitOK || err != nil {
		f.t.Fatalf("status = %d, %v\nstdout: %s\nstderr: %s", r.code, err, r.stdout, r.stderr)
	}
	return s
}

func (f *fixture) history(args ...string) []entry {
	f.t.Helper()
	r := f.cli(nil, "history", args...)
	if r.code != exitOK {
		f.t.Fatalf("history = %d\nstderr: %s", r.code, r.stderr)
	}
	var out []entry
	for line := range strings.Lines(r.stdout) {
		var e entry
		if err := strict(line, &e); err != nil {
			f.t.Fatalf("history line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

func kinds(h []entry) []string {
	out := make([]string, len(h))
	for i, e := range h {
		out[i] = e.Kind
	}
	return out
}

func strict(data string, v any) error {
	d := json.NewDecoder(strings.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.More() {
		return errors.New("trailing data")
	}
	return nil
}
