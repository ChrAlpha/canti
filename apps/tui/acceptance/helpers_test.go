//go:build acceptance

package acceptance

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

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// scriptedModel serves the Anthropic Messages API at /v1/messages and the
// Responses API at /responses. It answers every request with the decision
// scripted for the step named in the prompt's progress line, unless answer
// set a reply for it, and records every request body.
type scriptedModel struct {
	*httptest.Server
	key     string
	script  map[int]decision
	decide  func(int, string) (decision, error)
	hold    chan struct{}
	mu      sync.Mutex
	bodies  [][]byte
	sent    map[int]int
	replies map[[2]int]reply
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
	cut        bool
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

func newScriptedModel(t *testing.T, key string, script map[int]decision) *scriptedModel {
	t.Helper()
	m := &scriptedModel{key: key, script: script}
	m.Server = httptest.NewServer(m)
	t.Cleanup(m.Close)
	return m
}

// fixtureModel names the model API and model of a fixture's run and chat,
// which a test can override with later flags; canti has neither default.
var fixtureModel = []string{"-ephemeral", "-api", "anthropic-messages", "-model", "fixture-model"}

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
func (m *scriptedModel) decideWith(decide func(step int, prompt string) (decision, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decide = decide
}

func (m *scriptedModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	m.mu.Lock()
	m.bodies = append(m.bodies, body)
	hold, decide := m.hold, m.decide
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
	if responses {
		msg = responsesReply(text, re.stopReason)
	}
	if re.cut {
		cutConnection(w, msg)
		return
	}
	json.NewEncoder(w).Encode(msg)
}

// cutConnection sends the first half of msg under a Content-Length of all of
// it and closes the connection.
func cutConnection(w http.ResponseWriter, msg any) {
	data, _ := json.Marshal(msg)
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(data), data[:len(data)/2])
	buf.Flush()
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

// hostCommands are the commands that take -host.
var hostCommands = map[string]bool{"run": true, tuiCommand: true, "resume": true, "stop": true, "resolve": true}

// pinHost uses -host embedded for operational commands and -host owned for
// the independent TUI after
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
	if args[0] == tuiCommand {
		mode = "owned"
	}
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
	exe, args := executable(t, env, args)
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

func newFixture(t *testing.T, script map[int]decision) *fixture {
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
	if command == "run" || command == "resume" || command == tuiCommand {
		all = append(all, "-model-url", f.model.URL)
	}
	if command == "run" || command == tuiCommand {
		all = append(all, fixtureModel...)
	}
	cmd, wait := startCLIWith(f.t, stdin, stdout, append([]string{"ANTHROPIC_API_KEY=" + f.key}, env...), append(all, args...)...)
	return cmd, func() result {
		r := wait()
		// A signal may kill the client before its separately owned runtime has
		// completed parent-death shutdown. Await that lifecycle before inspecting
		// SQLite files, whose WAL/SHM disappear as the runtime closes them.
		if command == tuiCommand && r.code < 0 && slices.Contains(cmd.Args, "owned") {
			waitFor(f.t, func() bool { return !locked(f.state) })
		}
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
		SessionID   string    `json:"session_id"`
		SessionSeq  int64     `json:"session_seq"`
		InputID     string    `json:"input_id"`
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
	Phase      string          `json:"phase"`
	Intent     json.RawMessage `json:"intent"`
	Step       int             `json:"step"`
	Attempt    int             `json:"attempt"`
	RequestRev int             `json:"request_rev"`
	Reason     string          `json:"reason"`
	Decision   *decision       `json:"decision"`
	EffectID   string          `json:"effect_id"`
	Totals     struct {
		Coverage     hostrpc.UsageCoverage `json:"coverage"`
		InputTokens  int64                 `json:"input_tokens"`
		OutputTokens int64                 `json:"output_tokens"`
		OutputBytes  int64                 `json:"output_bytes"`
		Steps        int                   `json:"steps"`
	} `json:"totals"`
}

// entry is a public history JSON record; raw preserves it for runtime-owned
// fault/receipt checks without importing journal implementation types here.
type entry struct {
	Kind  string `json:"kind"`
	State struct {
		RunID  string `json:"run_id"`
		Limits struct {
			MaxSteps int `json:"max_steps"`
		} `json:"limits"`
	} `json:"state"`
	Facts struct {
		Observation *struct {
			Execution string `json:"execution"`
		} `json:"observation"`
	} `json:"facts"`
	raw json.RawMessage
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
		e.raw = json.RawMessage(line)
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
func responsesToolReply(d decision) string {
	args, _ := json.Marshal(d)
	data, _ := json.Marshal(map[string]any{"id": "resp_test", "object": "response", "model": "responses-test", "status": "completed",
		"output": []any{map[string]any{"type": "function_call", "id": "fc_test", "call_id": "call_test", "name": "canti_decision", "arguments": string(args), "status": "completed"}},
		"usage":  map[string]int{"input_tokens": 100, "output_tokens": 10, "total_tokens": 110}})
	return string(data)
}

func toolReply(decisions ...decision) string {
	blocks := []any{map[string]any{"type": "text", "text": "Narration is not a decision."}}
	for _, d := range decisions {
		blocks = append(blocks, map[string]any{"type": "tool_use", "id": "toolu_test", "name": "canti_decision", "input": d})
	}
	data, _ := json.Marshal(map[string]any{"model": "claude-test", "stop_reason": "tool_use", "content": blocks,
		"usage": map[string]int{"input_tokens": 100, "output_tokens": 10}})
	return string(data)
}

// archiveText reads the published read-only archive layout through a bounded
// filesystem reader. Acceptance imports only the public SDK, never runtime internals.
func archiveText(t *testing.T, f *fixture, name string) string {
	t.Helper()
	relative, ok := strings.CutPrefix(name, "/canti/runs/")
	if !ok || filepath.Clean(relative) != relative || strings.HasPrefix(relative, "../") {
		t.Fatalf("invalid published archive path %q", name)
	}
	root, err := os.OpenRoot(filepath.Join(f.state, "history"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	file, err := root.Open(relative)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var result strings.Builder
	page := make([]byte, 8192)
	for n := 0; n < 128; n++ {
		count, err := file.Read(page)
		result.Write(page[:count])
		if err == io.EOF {
			return result.String()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("published archive exceeds 1MiB read bound")
	return ""
}

type archivedSession struct {
	SessionID     string `json:"session_id"`
	Sequence      int64  `json:"sequence"`
	InputID       string `json:"input_id"`
	PreviousRunID string `json:"previous_run_id"`
}

func archiveSession(t *testing.T, f *fixture, run string) archivedSession {
	t.Helper()
	var s archivedSession
	if err := json.Unmarshal([]byte(archiveText(t, f, "/canti/runs/"+run+"/session")), &s); err != nil {
		t.Fatal(err)
	}
	return s
}
func checkSessionLink(t *testing.T, f *fixture, prompt, current, previous, goal string, seq int64) {
	t.Helper()
	if !strings.Contains(prompt, "## Goal\n"+goal+"\n") || !strings.Contains(prompt, "/canti/runs/"+current+"/session") || !strings.Contains(prompt, "Previous run: /canti/runs/"+previous+"/.") {
		t.Fatalf("missing task/session navigation: %s", prompt)
	}
	link, prior := archiveSession(t, f, current), archiveSession(t, f, previous)
	if link.PreviousRunID != previous || link.Sequence != seq || prior.Sequence != seq-1 || link.SessionID == "" || prior.SessionID != link.SessionID || link.InputID == "" || prior.InputID == "" || link.InputID == prior.InputID {
		t.Fatalf("session chain identity: current=%+v prior=%+v", link, prior)
	}
}
func checkFile(t *testing.T, name, want string) {
	t.Helper()
	body, err := os.ReadFile(name)
	if err != nil || string(body) != want {
		t.Fatalf("%s=%q,%v; want%q", name, body, err, want)
	}
}
