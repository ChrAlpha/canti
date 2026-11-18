package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// scriptedModel serves the Anthropic Messages API at /v1/messages and the
// Responses API at /responses. It answers every request with the decision
// scripted for the step named in the prompt's progress line, unless answer
// set a reply for it, and records every request body.
type scriptedModel struct {
	*httptest.Server
	key      string
	script   map[int]core.Decision
	decide   func(int, string) (core.Decision, error)
	hold     chan struct{}
	streamed chan struct{}
	mu       sync.Mutex
	bodies   [][]byte
	sent     map[int]int
	replies  map[[2]int]reply
	// unreported omits usage, as a provider that reports none would, so a
	// rejection the script injects regardless of size teaches no budget.
	unreported bool
}

// reply replaces the answer to one request. A non-zero status sends body
// with that status; otherwise the message carries text instead of the
// scripted decision when text is set, stops with stopReason instead of
// end_turn when it is set, and is cut in half by closing the connection
// when cut is set.
type reply struct {
	status     int
	body       string
	text       string
	stopReason string
	requestID  string
	cut        bool
	hang       bool // streams a partial answer, then waits for the client to go away
}

// answer makes the n-th request for step, counting from 1, answer with r.
func (m *scriptedModel) answer(step, n int, r reply) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.replies == nil {
		m.replies = map[[2]int]reply{}
	}
	m.replies[[2]int{step, n}] = r
}

func newScriptedModel(t *testing.T, key string, script map[int]core.Decision) *scriptedModel {
	t.Helper()
	m := &scriptedModel{key: key, script: script}
	m.Server = httptest.NewServer(m)
	t.Cleanup(m.Close)
	return m
}

// fixtureModel names the model API, model, and input window of a fixture's
// run, which a test can override with later flags; canti has no default for
// any of them.
var fixtureModel = []string{"-ephemeral", "-api", "anthropic-messages", "-model", "fixture-model", "-context-tokens", "200000"}

var progressLine = regexp.MustCompile(`(?m)^Step (\d+)(?: of at most \d+| \(unlimited steps\)), attempt \d+ of \d+\.$`)

// promptOf reads the user message of a Messages or a Responses request.
func promptOf(body []byte) (step int, prompt string, err error) {
	var req struct {
		Messages []struct{ Content json.RawMessage } `json:"messages"`
		Input    []struct{ Content json.RawMessage } `json:"input"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return 0, "", errors.New("request is not JSON")
	}
	blockType, messages := "text", req.Messages
	if req.Messages == nil {
		blockType, messages = "input_text", req.Input
	}
	if len(messages) != 1 {
		return 0, "", errors.New("request is not one user message")
	}
	var blocks []struct{ Type, Text string }
	if err := json.Unmarshal(messages[0].Content, &blocks); err != nil {
		return 0, "", errors.New("user message content is not content blocks")
	}
	for _, b := range blocks {
		if b.Type != blockType || b.Text == "" {
			return 0, "", errors.New("user message has an empty or non-text block")
		}
		prompt += b.Text
	}
	m := progressLine.FindStringSubmatch(prompt)
	if m == nil {
		return 0, prompt, errors.New("prompt has no progress line")
	}
	step, err = strconv.Atoi(m[1])
	return step, prompt, err
}

// decideWith makes decide answer each request in place of the script.
func (m *scriptedModel) decideWith(decide func(step int, prompt string) (core.Decision, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decide = decide
}

func (m *scriptedModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	m.mu.Lock()
	m.bodies = append(m.bodies, body)
	hold, decide, unreported := m.hold, m.decide, m.unreported
	m.mu.Unlock()
	if hold != nil {
		select {
		case hold <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	responses := r.URL.Path == "/responses"
	key, bearer := r.Header.Get("x-api-key"), r.Header.Get("authorization")
	if responses && (key != "" || bearer != "Bearer "+m.key) || !responses && (r.URL.Path != "/v1/messages" || key != m.key || bearer != "") {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"unexpected path or credentials"}}`)
		return
	}
	step, prompt, err := promptOf(body)
	var re reply
	if err == nil {
		m.mu.Lock()
		if m.sent == nil {
			m.sent = map[int]int{}
		}
		m.sent[step]++
		re = m.replies[[2]int{step, m.sent[step]}]
		m.mu.Unlock()
	}
	if re.status != 0 {
		if re.requestID != "" {
			w.Header().Set("x-request-id", re.requestID)
		}
		if re.status/100 == 2 && re.body != "" {
			if responses {
				writeResponsesStreamJSON(w, []byte(re.body))
			} else {
				writeMessagesStreamJSON(w, []byte(re.body))
			}
			return
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(re.status)
		io.WriteString(w, re.body)
		return
	}
	d, ok := m.script[step]
	if err == nil && decide != nil {
		d, err = decide(step, prompt)
		ok = err == nil
	}
	if err != nil || !ok && re.text == "" {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"no scripted decision"}}`)
		return
	}
	text := re.text
	if text == "" {
		b, _ := json.Marshal(d)
		text = string(b)
	}
	msg := map[string]any{
		"id": "msg_test", "type": "message", "role": "assistant", "model": "claude-test", "stop_reason": cmp.Or(re.stopReason, "end_turn"),
		"content": []map[string]string{{"type": "text", "text": text}},
		"usage":   map[string]int{"input_tokens": 100, "output_tokens": 10},
	}
	if unreported {
		delete(msg, "usage")
	}
	if responses {
		msg = responsesReply(text, re.stopReason)
	}
	if re.cut || re.hang {
		if responses {
			writePartialResponses(w, text)
		} else {
			writePartialMessage(w, text)
		}
		m.mu.Lock()
		streamed := m.streamed
		m.mu.Unlock()
		if streamed != nil {
			select {
			case streamed <- struct{}{}:
			default:
			}
		}
		if re.hang {
			<-r.Context().Done()
		}
		return
	}
	if responses {
		writeResponsesStream(w, msg)
	} else {
		writeMessagesStream(w, msg)
	}
}

// writeMessagesStream assembles a whole Messages object from real content
// blocks and deltas, as the streaming API does.
func writeMessagesStream(w http.ResponseWriter, message any) {
	data, _ := json.Marshal(message)
	writeMessagesStreamJSON(w, data)
}

func writeMessagesStreamJSON(w http.ResponseWriter, data []byte) {
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
	if json.Unmarshal(data, &full) != nil {
		w.Header().Set("content-type", "text/event-stream")
		fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":%s}\n\n", data)
		io.WriteString(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":null}\n\n")
		io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
		return
	}
	w.Header().Set("content-type", "text/event-stream")
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
			writeBlockStart(w, index, map[string]any{"type": "text", "text": ""})
			writeTextDeltas(w, index, block.Text)
		case "thinking":
			writeBlockStart(w, index, map[string]any{"type": "thinking", "thinking": "", "signature": ""})
			writeThinkingDeltas(w, index, block.Thinking)
			if block.Signature != "" {
				signature, _ := json.Marshal(block.Signature)
				fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"signature_delta\",\"signature\":%s}}\n\n", index, signature)
			}
		case "tool_use":
			writeBlockStart(w, index, map[string]any{"type": "tool_use", "id": block.ID, "name": block.Name, "input": map[string]any{}})
			input := block.Input
			if len(input) == 0 {
				input = json.RawMessage(`{}`)
			}
			writeInputDeltas(w, index, string(input))
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

func writeBlockStart(w http.ResponseWriter, index int, block any) {
	raw, _ := json.Marshal(block)
	fmt.Fprintf(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":%d,\"content_block\":%s}\n\n", index, raw)
}

func writeTextDeltas(w http.ResponseWriter, index int, text string) {
	for _, part := range splitText(text) {
		raw, _ := json.Marshal(part)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", index, raw)
	}
}

func writeThinkingDeltas(w http.ResponseWriter, index int, text string) {
	for _, part := range splitText(text) {
		raw, _ := json.Marshal(part)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":%s}}\n\n", index, raw)
	}
}

func writeInputDeltas(w http.ResponseWriter, index int, input string) {
	for _, part := range splitText(input) {
		raw, _ := json.Marshal(part)
		fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":%d,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":%s}}\n\n", index, raw)
	}
}

func splitText(text string) []string {
	runes := []rune(text)
	if len(runes) < 2 {
		return []string{text}
	}
	middle := len(runes) / 2
	return []string{string(runes[:middle]), string(runes[middle:])}
}

func writeResponsesStream(w http.ResponseWriter, response any) {
	data, _ := json.Marshal(response)
	writeResponsesStreamJSON(w, data)
}

func writeResponsesStreamJSON(w http.ResponseWriter, data []byte) {
	var envelope struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(data, &envelope)
	typ := "response.completed"
	switch envelope.Status {
	case "incomplete":
		typ = "response.incomplete"
	case "failed":
		typ = "response.failed"
	}
	w.Header().Set("content-type", "text/event-stream")
	fmt.Fprintf(w, "event: %s\ndata: {\"type\":\"%s\",\"response\":%s}\n\n", typ, typ, data)
}

// writePartialMessage streams one text block and stops before message_stop.
func writePartialMessage(w http.ResponseWriter, text string) {
	w.Header().Set("content-type", "text/event-stream")
	io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":100,\"output_tokens\":1}}}\n\n")
	io.WriteString(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
	payload, _ := json.Marshal(text)
	fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":%s}}\n\n", payload)
	w.(http.Flusher).Flush()
}

func writePartialResponses(w http.ResponseWriter, text string) {
	w.Header().Set("content-type", "text/event-stream")
	io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"model\":\"responses-test\",\"status\":\"in_progress\",\"output\":[],\"usage\":null}}\n\n")
	payload, _ := json.Marshal(text)
	fmt.Fprintf(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":%s}\n\n", payload)
	w.(http.Flusher).Flush()
}

// holdRequests makes the model answer no request until its client goes
// away and report each one on the returned channel.
func (m *scriptedModel) holdRequests() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hold = make(chan struct{}, 1)
	return m.hold
}

// watchStream reports that a partial stream has been flushed to the client.
func (m *scriptedModel) watchStream() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamed = make(chan struct{}, 1)
	return m.streamed
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

// hostCommands are the commands that take -host.
var hostCommands = map[string]bool{"run": true, "resume": true, "stop": true, "resolve": true, "observe": true, "withdraw": true, "cancel": true}

// pinHost uses -host embedded after
// a command that takes -host and names none, so that every test runs the
// command in its own process unless it names -host auto, or unpinned names
// the default.
func pinHost(args []string) []string {
	if len(args) == 0 || !hostCommands[args[0]] || slices.ContainsFunc(args, func(a string) bool {
		return a == "-host" || strings.HasPrefix(a, "-host=") || a == unpinned
	}) {
		return slices.DeleteFunc(slices.Clone(args), func(a string) bool { return a == unpinned })
	}
	mode := "embedded"

	return append([]string{args[0], "-host", mode}, args[1:]...)
}

// unpinned, among the arguments of a CLI process, makes pinHost leave its
// command on the default -host, and is removed.
const unpinned = "\x00default-host"

// startCLI starts the CLI process of runCLI and returns it with the wait
// for its result.
func startCLI(t *testing.T, env []string, args ...string) (*exec.Cmd, func() result) {
	t.Helper()
	return startCLIWith(t, nil, nil, env, args...)
}

// startCLIWith is startCLI with stdin as the standard input of the process
// and, unless it is nil, stdout as its standard output.
func startCLIWith(t *testing.T, stdin io.Reader, stdout io.Writer, env []string, args ...string) (*exec.Cmd, func() result) {
	t.Helper()
	return startCLIIn(t, "", stdin, stdout, env, args...)
}

// startCLIIn is startCLIWith in working directory dir, or the test's when
// dir is empty.
func startCLIIn(t *testing.T, dir string, stdin io.Reader, stdout io.Writer, env []string, args ...string) (*exec.Cmd, func() result) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	cmd := exec.CommandContext(ctx, exe, pinHost(args)...)
	cmd.Dir = dir
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	gorace := "atexit_sleep_ms=0"
	if v := os.Getenv("GORACE"); v != "" {
		gorace = v + " " + gorace
	}
	cmd.Env = append(cmd.Env, "GORACE="+gorace)
	cmd.Env = append(append(cmd.Env, "CANTI_TEST_MAIN=1"), env...)
	var out, stderr bytes.Buffer
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, cmp.Or(stdout, io.Writer(&out)), &stderr
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
		return result{cmd.ProcessState.ExitCode(), out.String(), stderr.String()}
	}
}

// shortSocketDir bounds only Unix transport paths. Workspace and other test
// data still use the caller's TMPDIR, including long-path verification runs.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "canti-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	return root
}

// fixture is one workspace and state directory driven against a scripted
// model with a fake API key. When the test ends it fails if the key appears
// in any CLI output or any file under the state directory.
type fixture struct {
	t                *testing.T
	workspace, state string
	key              string
	model            *scriptedModel
	mu               sync.Mutex // guards output for commands run concurrently
	output           strings.Builder
}

func newFixture(t *testing.T, script map[int]core.Decision) *fixture {
	t.Helper()
	root := t.TempDir()
	f := &fixture{t: t, workspace: filepath.Join(root, "workspace"), state: filepath.Join(root, "state"), key: "sk-test-" + rand.Text()}
	if err := os.Mkdir(f.workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	f.model = newScriptedModel(t, f.key, script)
	t.Cleanup(func() {
		if strings.Contains(f.output.String(), f.key) {
			t.Error("CLI output holds the credential")
		}
		filepath.WalkDir(f.state, func(path string, d fs.DirEntry, err error) error {
			if err == nil && d.Type().IsRegular() {
				if data, err := os.ReadFile(path); err != nil || bytes.Contains(data, []byte(f.key)) {
					t.Errorf("%s holds the credential: %v", path, err)
				}
			}
			return nil
		})
	})
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
	return f.startWith(nil, env, command, args...)
}

// startWith is start with stdin as the standard input of the process.
func (f *fixture) startWith(stdin io.Reader, env []string, command string, args ...string) (*exec.Cmd, func() result) {
	f.t.Helper()
	return f.startIO(stdin, nil, env, command, args...)
}

// startIO is startWith with stdout as the standard output unless it is nil.
func (f *fixture) startIO(stdin io.Reader, stdout io.Writer, env []string, command string, args ...string) (*exec.Cmd, func() result) {
	f.t.Helper()
	all := []string{command, "-workspace", f.workspace, "-state", f.state}
	if command == "run" || command == "resume" {
		all = append(all, "-model-url", f.model.URL)
	}
	if command == "run" {
		all = append(all, fixtureModel...)
	}
	cmd, wait := startCLIWith(f.t, stdin, stdout, append([]string{"ANTHROPIC_API_KEY=" + f.key}, env...), append(all, args...)...)
	return cmd, func() result {
		r := wait()

		f.mu.Lock()
		f.output.WriteString(r.stdout + r.stderr)
		f.mu.Unlock()
		return r
	}
}

// status is the object the status command prints.
type status struct {
	Execution struct {
		Ephemeral   bool                 `json:"ephemeral"`
		Environment *hostrpc.Environment `json:"environment"`
	} `json:"execution"`
	Run struct {
		Ordinal     int       `json:"ordinal"`
		InputID     string    `json:"input_id"`
		InputPos    int64     `json:"input_pos"`
		Fresh       bool      `json:"fresh"`
		ID          string    `json:"id"`
		Goal        string    `json:"goal"`
		Constraints string    `json:"constraints"`
		Created     time.Time `json:"created"`
		Budget      struct {
			WallTime      string `json:"wall_time"`
			Tokens        int64  `json:"tokens"`
			ContextTokens int64  `json:"context_tokens"`
			OutputBytes   int64  `json:"output_bytes"`
			JobTimeout    string `json:"job_timeout"`
		} `json:"budget"`
	} `json:"run"`
	Seq                int64                   `json:"seq"`
	Phase              core.Phase              `json:"phase"`
	Intent             core.Intent             `json:"intent"`
	Step               int                     `json:"step"`
	Attempt            int                     `json:"attempt"`
	RequestRev         int                     `json:"request_rev"`
	Reason             core.StopReason         `json:"reason"`
	Decision           *core.Decision          `json:"decision"`
	EffectID           string                  `json:"effect_id"`
	Inputs             int                     `json:"inputs"`
	Seen               int                     `json:"seen"`
	Project            *hostrpc.ProjectBinding `json:"project"`
	ProjectUnavailable string                  `json:"project_unavailable"`
	Totals             struct {
		Coverage     harness.UsageCoverage `json:"coverage"`
		InputTokens  int64                 `json:"input_tokens"`
		OutputTokens int64                 `json:"output_tokens"`
		OutputBytes  int64                 `json:"output_bytes"`
		Steps        int                   `json:"steps"`
	} `json:"totals"`
}

// entry is one line the history command prints.
type entry struct {
	Seq       int64           `json:"seq"`
	EventID   string          `json:"event_id"`
	Kind      string          `json:"kind"`
	Semantics int             `json:"semantics"`
	Event     json.RawMessage `json:"event"`
	From      core.Phase      `json:"from"`
	To        core.Phase      `json:"to"`
	State     core.State      `json:"state"`
	Facts     harness.Facts   `json:"facts"`
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

// fillPipe fills the pipe that w writes to, so the next write to it blocks.
func fillPipe(t *testing.T, w *os.File) {
	t.Helper()
	fd := int(w.Fd())
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range [][]byte{make([]byte, 4096), {0}} {
		for {
			if _, err := syscall.Write(fd, chunk); err == syscall.EAGAIN {
				break
			} else if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		t.Fatal(err)
	}
}

// toolReply is a Messages response that narrates in text and calls
// canti_decision once per decision.
// responsesReply is a Responses API response whose message holds text,
// incomplete for reason when it is set.
func responsesReply(text, reason string) map[string]any {
	msg := map[string]any{
		"id": "resp_test", "object": "response", "model": "responses-test", "status": "completed",
		"output": []any{map[string]any{"type": "message", "id": "msg_test", "role": "assistant", "status": "completed",
			"content": []map[string]any{{"type": "output_text", "text": text, "annotations": []any{}}}}},
		"usage": map[string]int{"input_tokens": 100, "output_tokens": 10, "total_tokens": 110},
	}
	if reason != "" {
		msg["status"], msg["incomplete_details"] = "incomplete", map[string]string{"reason": reason}
	}
	return msg
}

// responsesToolReply is a Responses API response that calls the decision
// tool once with d.
func responsesToolReply(d core.Decision) string {
	args, _ := json.Marshal(d)
	data, _ := json.Marshal(map[string]any{"id": "resp_test", "object": "response", "model": "responses-test", "status": "completed",
		"output": []any{map[string]any{"type": "function_call", "id": "fc_test", "call_id": "call_test", "name": "canti_decision", "arguments": string(args), "status": "completed"}},
		"usage":  map[string]int{"input_tokens": 100, "output_tokens": 10, "total_tokens": 110}})
	return string(data)
}

func toolReply(decisions ...core.Decision) string {
	blocks := []any{map[string]any{"type": "text", "text": "Narration is not a decision."}}
	for _, d := range decisions {
		blocks = append(blocks, map[string]any{"type": "tool_use", "id": "toolu_test", "name": "canti_decision", "input": d})
	}
	data, _ := json.Marshal(map[string]any{"model": "claude-test", "stop_reason": "tool_use", "content": blocks,
		"usage": map[string]int{"input_tokens": 100, "output_tokens": 10}})
	return string(data)
}

func checkFile(t *testing.T, path, want string) {
	t.Helper()
	if data, err := os.ReadFile(path); err != nil || string(data) != want {
		t.Errorf("%s = %q, %v; want %q", filepath.Base(path), data, err, want)
	}
}

func holdFirstAnswer(t *testing.T, f *fixture) (url string, release func()) {
	t.Helper()
	held := make(chan struct{})
	var releaseOnce, holdOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(held) }) }
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		holdOnce.Do(func() {
			select {
			case <-held:
			case <-r.Context().Done():
			}
		})
		f.model.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { release(); gate.Close() })
	return gate.URL, release
}
