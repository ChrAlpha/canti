//go:build legacygen

package harness_test

import (
	"archive/tar"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	legacyGenerate = flag.String("legacy-generate", "", "comma-separated legacy journal `cases` to generate, each <producer SHA>/<case>")
	legacyOut      = flag.String("legacy-out", legacyDir, "`directory` the generated cases are written under")
)

// legacyCredential is the fake model credential of every producer; no
// output of the generator may contain it.
const legacyCredential = "sk-ant-legacygen-not-a-credential-4b7e19c2d05f"

// legacyReserved holds every producer's workspace and state directory, at
// paths a later CLI consumer can recreate from the manifest.
const legacyReserved = "/tmp/canti-legacy-w0a"

const legacyInvocationTimeout = time.Minute

// legacyCapacityContext is the -context-bytes of the near-capacity case,
// tuned so that the paused window fits just below the capacity trigger.
const legacyCapacityContext = 100864

type legacyKey struct{ run, step, attempt int }

type legacyScripted struct {
	decision string // "{run}" stands for the run ID
	hold     bool   // never answered: the generator kills the client
}

type legacySpec struct {
	coverage []string
	imports  map[string]string
	replies  map[legacyKey]legacyScripted
	drive    func(g *legacyGen)
	probe    bool
}

func legacySpecs() map[string]legacySpec {
	unlimited := []string{"-wall-time", "0", "-job-timeout", "0"}
	notes := legacyText("notes", 1500)
	readNotes := legacyScripted{decision: legacyRetrieve("read", "/canti/imports/notes.txt", "", "Read the imported notes.")}
	return map[string]legacySpec{
		legacySchema1 + "/retrieval-awaiting-observation": {
			coverage: []string{"schema 1", "semantics 1", "built-in retrieval of an import and of a published step record",
				"paused in awaiting_observation with a read_ job"},
			imports: map[string]string{"notes.txt": notes},
			replies: map[legacyKey]legacyScripted{
				{1, 1, 1}: readNotes,
				{1, 2, 1}: {decision: legacyRetrieve("read", "/canti/runs/{run}/steps/00000001/preview", "", "Check what the first step recorded.")},
			},
			drive: func(g *legacyGen) {
				g.cli(append([]string{"run", "-goal", "Summarize the imported notes.", "-history-imports", g.imports, "-once"}, unlimited...)...)
				g.resumeUntil(1, func(v legacyView) bool { return v.Phase == "awaiting_observation" && v.Step == 2 })
			},
		},
		legacySemantics2 + "/request-sent-killed": {
			coverage: []string{"schema 2", "semantics 2", "two-run session", "built-in retrieval of an import",
				"killed with SIGKILL after AttemptStarted was durable and before the response"},
			imports: map[string]string{"notes.txt": notes},
			replies: map[legacyKey]legacyScripted{
				{1, 1, 1}: {decision: legacyDecision("return", "The imported notes file is numbered plain text.")},
				{2, 1, 1}: readNotes,
				{2, 2, 1}: {hold: true},
			},
			drive: func(g *legacyGen) {
				session := append([]string{"-session", "legacy-w0a", "-history-imports", g.imports, "-once"}, unlimited...)
				g.cli(append([]string{"run", "-input", "input-1", "-goal", "What kind of file is the imported notes file?"}, session...)...)
				g.resumeUntil(1, func(v legacyView) bool { return v.Phase == "returned" })
				g.cli(append([]string{"run", "-input", "input-2", "-goal", "Read the imported notes, then summarize them."}, session...)...)
				g.resumeUntil(2, func(v legacyView) bool { return v.Phase == "request_frozen" && v.Step == 2 })
				g.killDuringSend(2)
			},
		},
		legacySemantics2 + "/capacity-unrequested": {
			coverage: []string{"schema 2", "semantics 2", "built-in reads of a large import",
				"paused in idle with the window near the capacity trigger and no handover requested"},
			imports: map[string]string{"evidence.txt": legacyText("evidence", 32<<10)},
			replies: map[legacyKey]legacyScripted{
				{1, 1, 1}: {decision: legacyRetrieve("read", "/canti/imports/evidence.txt", `,"offset":0,"limit":8192`, "Read the first evidence range.")},
				{1, 2, 1}: {decision: legacyRetrieve("read", "/canti/imports/evidence.txt", `,"offset":8192,"limit":8192`, "Read the second evidence range.")},
			},
			probe: true,
			drive: func(g *legacyGen) {
				g.cli(append([]string{"run", "-goal", "Summarize the imported evidence.", "-history-imports", g.imports,
					"-context-bytes", strconv.Itoa(legacyCapacityContext), "-once"}, unlimited...)...)
				g.resumeUntil(1, func(v legacyView) bool { return v.Phase == "idle" && v.Step == 3 })
			},
		},
	}
}

// legacyDecision encodes a scripted decision in the protocol every producer
// decodes, independently of the current core types.
func legacyDecision(kind, payload string) string {
	b, err := json.Marshal(struct {
		Kind    string `json:"kind"`
		Payload string `json:"payload"`
	}{kind, payload})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func legacyRetrieve(operation, path, extra, reason string) string {
	return legacyDecision("exec", `canti:retrieve {"operation":"`+operation+`","path":"`+path+`"`+extra+`,"reason":"`+reason+`"}`)
}

func legacyText(name string, size int) string {
	var b strings.Builder
	for i := 1; b.Len() < size; i++ {
		fmt.Fprintf(&b, "%s line %05d: plain text evidence recorded for the legacy journal corpus.\n", name, i)
	}
	return b.String()
}

// TestLegacyJournalGenerate builds each named case's producer from git
// archive and writes the case under -legacy-out, refusing an existing one.
// It runs only with -tags legacygen and -legacy-generate.
func TestLegacyJournalGenerate(t *testing.T) {
	if *legacyGenerate == "" {
		t.Skip("set -legacy-generate to write new legacy journal cases")
	}
	specs, built := legacySpecs(), map[string]legacyBuilt{}
	for _, name := range strings.Split(*legacyGenerate, ",") {
		spec, ok := specs[name]
		if !ok {
			t.Fatalf("no legacy case %q is defined", name)
		}
		sha := path.Dir(name)
		if _, ok := built[sha]; !ok {
			built[sha] = legacyBuild(t, sha)
		}
		t.Run(name, func(t *testing.T) { legacyProduce(t, name, spec, built[sha]) })
	}
}

type legacyBuilt struct {
	bin      string
	producer legacyProducer
}

func legacyBuild(t *testing.T, sha string) legacyBuilt {
	t.Helper()
	git := func(args ...string) []byte {
		out, err := exec.Command("git", args...).Output()
		if err != nil {
			t.Fatalf("git %s: %v", strings.Join(args, " "), err)
		}
		return out
	}
	top := strings.TrimSpace(string(git("rev-parse", "--show-toplevel")))
	if got := strings.TrimSpace(string(git("rev-parse", "--verify", sha+"^{commit}"))); got != sha {
		t.Fatalf("%s resolves to %s", sha, got)
	}
	tree := strings.TrimSpace(string(git("rev-parse", sha+"^{tree}")))
	src, bin := t.TempDir(), filepath.Join(t.TempDir(), "canti")
	legacyExtract(t, git("-C", top, "archive", "--format=tar", sha), src)
	build := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-o", bin, "./cmd/canti")
	build.Dir, build.Env = src, legacyBuildEnv()
	start := time.Now()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", sha, err, out)
	}
	t.Logf("built %s in %v", sha, time.Since(start).Round(time.Millisecond))
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	info, err := exec.Command("go", "version", "-m", bin).Output()
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(info)), "\n")
	p := legacyProducer{SHA: sha, Tree: tree, Binary: legacyFileOf("cmd/canti", data),
		GoVersion: strings.TrimSpace(strings.TrimPrefix(lines[0], bin+":"))}
	for _, line := range lines[1:] {
		p.Build = append(p.Build, strings.Join(strings.Fields(line), " "))
	}
	return legacyBuilt{bin: bin, producer: p}
}

func legacyBuildEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "ANTHROPIC_") || slices.Contains([]string{"GOWORK", "GOFLAGS", "CGO_ENABLED", "GOTOOLCHAIN", "GOOS", "GOARCH"}, name) {
			continue
		}
		env = append(env, kv)
	}
	return append(env, "GOWORK=off", "GOFLAGS=-mod=readonly", "CGO_ENABLED=0", "GOTOOLCHAIN=local")
}

func legacyExtract(t *testing.T, archive []byte, dir string) {
	t.Helper()
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dir, filepath.FromSlash(h.Name))
		switch h.Typeflag {
		case tar.TypeXGlobalHeader:
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			var data []byte
			if data, err = io.ReadAll(r); err == nil {
				err = os.WriteFile(target, data, fs.FileMode(h.Mode)&0o755|0o600)
			}
		case tar.TypeSymlink:
			err = os.Symlink(h.Linkname, target)
		default:
			err = fmt.Errorf("archive entry %s has type %q", h.Name, h.Typeflag)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// legacyGen drives one producer binary through its CLI against a scripted
// Messages endpoint that answers by the committed run, step, and attempt.
type legacyGen struct {
	t                                         *testing.T
	spec                                      legacySpec
	bin                                       string
	root, workspace, state, imports, home, tm string
	shim                                      string
	server                                    *httptest.Server
	held                                      chan legacyKey
	release                                   chan struct{}
	released                                  sync.Once
	mu                                        sync.Mutex
	replies                                   []legacyReply
	invocations                               []legacyInvocation
	killed                                    *legacyKilled
	log                                       bytes.Buffer
}

func (g *legacyGen) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err == nil && (r.Method != http.MethodPost || r.URL.Path != "/v1/messages" ||
		r.Header.Get("x-api-key") != legacyCredential || r.Header.Get("anthropic-version") != "2023-06-01") {
		err = fmt.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	}
	var key legacyKey
	var runID string
	if err == nil {
		key, runID, err = g.sending(body)
	}
	scripted, ok := g.spec.replies[key]
	if err == nil && !ok {
		err = fmt.Errorf("no reply is scripted for run %d step %d attempt %d", key.run, key.step, key.attempt)
	}
	decision := strings.ReplaceAll(scripted.decision, "{run}", runID)
	g.mu.Lock()
	if err == nil {
		g.replies = append(g.replies, legacyReply{Run: key.run, Step: key.step, Attempt: key.attempt,
			Request: legacyHash(body), Decision: decision, Hold: scripted.hold})
	}
	fmt.Fprintf(&g.log, "model request run %d step %d attempt %d: %d bytes, sha256 %s, hold %v, error %v\n",
		key.run, key.step, key.attempt, len(body), legacyHash(body), scripted.hold, err)
	g.mu.Unlock()
	if err != nil {
		g.t.Errorf("model request: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"unscripted request"}}`)
		return
	}
	if scripted.hold {
		g.held <- key
		select {
		case <-r.Context().Done():
		case <-g.release:
		}
		return
	}
	text, _ := json.Marshal(decision)
	w.Header().Set("content-type", "application/json")
	fmt.Fprintf(w, `{"id":"msg_legacygen_%d_%d_%d","type":"message","role":"assistant","model":"claude-opus-5",`+
		`"content":[{"type":"text","text":%s}],"stop_reason":"end_turn","usage":{"input_tokens":1200,"output_tokens":30}}`,
		key.run, key.step, key.attempt, text)
}

// sending requires that the latest durable transition is the AttemptStarted
// of a send and that body is the request frozen for it, and returns the run
// ordinal, step, and attempt that key the scripted reply.
func (g *legacyGen) sending(body []byte) (legacyKey, string, error) {
	db, err := legacyRaw(filepath.Join(g.state, "canti.db"))
	if err != nil {
		return legacyKey{}, "", err
	}
	defer db.Close()
	var runID, kind, state, facts string
	var n int
	if err := db.QueryRow(`SELECT t.run_id, r.n, t.kind, t.to_state FROM transitions t JOIN runs r ON r.id = t.run_id
		ORDER BY t.rowid DESC LIMIT 1`).Scan(&runID, &n, &kind, &state); err != nil {
		return legacyKey{}, "", err
	}
	var v legacyView
	if err := json.Unmarshal([]byte(state), &v); err != nil {
		return legacyKey{}, "", err
	}
	if kind != "attempt_started" || v.Phase != "request_sent" {
		return legacyKey{}, "", fmt.Errorf("the latest durable transition is %s into %s, not a send", kind, v.Phase)
	}
	if err := db.QueryRow(`SELECT facts FROM transitions WHERE run_id = ? AND kind = 'request_frozen' AND step = ? AND rev = ?`,
		runID, v.Step, v.RequestRev).Scan(&facts); err != nil {
		return legacyKey{}, "", err
	}
	var f legacyFacts
	if err := json.Unmarshal([]byte(facts), &f); err != nil || f.Request == nil || !bytes.Equal(f.Request.Body, body) {
		return legacyKey{}, "", fmt.Errorf("the sent body is not the request frozen for step %d revision %d: %v", v.Step, v.RequestRev, err)
	}
	return legacyKey{n, v.Step, v.Attempt}, runID, nil
}

// legacyRaw opens a producer's journal read-only, so that closing it never
// checkpoints a WAL a killed producer left.
func legacyRaw(file string) (*sql.DB, error) {
	dsn := url.URL{Scheme: "file", OmitHost: true, Path: file, RawQuery: "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"}
	db, err := sql.Open("sqlite", dsn.String())
	if err == nil {
		db.SetMaxOpenConns(1)
	}
	return db, err
}

// latest returns the durable state of run ordinal n, or of the latest run
// when n is zero, with the kind and sequence of its last transition.
func (g *legacyGen) latest(state string, n int) (legacyView, string, int64) {
	g.t.Helper()
	db, err := legacyRaw(filepath.Join(state, "canti.db"))
	if err != nil {
		g.t.Fatal(err)
	}
	defer db.Close()
	var initial string
	var kind, to sql.NullString
	var seq sql.NullInt64
	if err := db.QueryRow(`SELECT r.initial, t.kind, t.to_state, t.seq FROM runs r
		LEFT JOIN transitions t ON t.run_id = r.id AND t.seq = (SELECT max(seq) FROM transitions WHERE run_id = r.id)
		WHERE r.n = ? OR ? = 0 ORDER BY r.n DESC LIMIT 1`, n, n).Scan(&initial, &kind, &to, &seq); err != nil {
		g.t.Fatalf("run %d: %v", n, err)
	}
	if to.Valid {
		initial = to.String
	}
	return legacyViewOf(g.t, initial), kind.String, seq.Int64
}

func (g *legacyGen) env(record bool) []string {
	credential := legacyCredential
	if record {
		credential = "(fake credential withheld)"
	}
	return []string{"PATH=" + g.shim, "HOME=" + g.home, "TMPDIR=" + g.tm, "ANTHROPIC_API_KEY=" + credential}
}

func (g *legacyGen) command(state string, args []string) (*exec.Cmd, []string, *bytes.Buffer) {
	full := append(slices.Clone(args), "-workspace", g.workspace, "-state", state, "-model-url", g.server.URL)
	cmd := exec.Command(g.bin, full...)
	out := &bytes.Buffer{}
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = g.workspace, g.env(false), out, out
	recorded := slices.Clone(full)
	recorded[len(recorded)-1] = "{model-url}"
	return cmd, recorded, out
}

// invoke runs one CLI command on state to its exit.
func (g *legacyGen) invoke(state string, args ...string) legacyInvocation {
	t := g.t
	t.Helper()
	cmd, recorded, out := g.command(state, args)
	start := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(legacyInvocationTimeout):
		cmd.Process.Kill()
		<-done
		t.Fatalf("canti %s did not exit within %v:\n%s", strings.Join(recorded, " "), legacyInvocationTimeout, out)
	}
	inv := legacyInvocation{Args: recorded, Exit: legacyExit(err)}
	g.logf("$ canti %s\n%s(exit %s after %v)\n", strings.Join(recorded, " "), out, inv.Exit, time.Since(start).Round(time.Millisecond))
	return inv
}

func (g *legacyGen) unhold() { g.released.Do(func() { close(g.release) }) }

func (g *legacyGen) text() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.log.String()
}

func (g *legacyGen) logf(format string, args ...any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fmt.Fprintf(&g.log, format, args...)
}

func legacyExit(err error) string {
	var exit *exec.ExitError
	switch {
	case err == nil:
		return "0"
	case errors.As(err, &exit):
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return "signal " + ws.Signal().String()
		}
		return strconv.Itoa(exit.ExitCode())
	}
	return err.Error()
}

// cli runs one command of the fixture's history and requires the exit
// status the latest run's durable phase implies.
func (g *legacyGen) cli(args ...string) {
	g.t.Helper()
	inv := g.invoke(g.state, args...)
	v, _, _ := g.latest(g.state, 0)
	inv.Phase = v.Phase
	g.invocations = append(g.invocations, inv)
	if want := map[bool]string{true: "0", false: "5"}[v.Phase == "returned"]; inv.Exit != want {
		g.t.Fatalf("canti %s exited %s in %s, want %s", strings.Join(inv.Args, " "), inv.Exit, v.Phase, want)
	}
}

func (g *legacyGen) resumeUntil(n int, done func(legacyView) bool) {
	g.t.Helper()
	for i := 0; ; i++ {
		v, _, _ := g.latest(g.state, n)
		if done(v) {
			return
		}
		if i == 32 {
			g.t.Fatalf("run %d did not reach its scripted pause: %+v", n, v)
		}
		g.cli("resume", "-once")
	}
}

// killDuringSend resumes run n into a send whose response the endpoint
// holds, confirms that the AttemptStarted of exactly that send is durable,
// and then kills the process with SIGKILL.
func (g *legacyGen) killDuringSend(n int) {
	t := g.t
	t.Helper()
	cmd, recorded, out := g.command(g.state, []string{"resume", "-once"})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var key legacyKey
	select {
	case key = <-g.held:
	case err := <-done:
		t.Fatalf("canti %s exited %s before its request was held:\n%s", strings.Join(recorded, " "), legacyExit(err), out)
	case <-time.After(legacyInvocationTimeout):
		cmd.Process.Kill()
		<-done
		t.Fatalf("canti %s sent no held request within %v:\n%s", strings.Join(recorded, " "), legacyInvocationTimeout, out)
	}
	v, kind, seq := g.latest(g.state, n)
	if kind != "attempt_started" || v.Phase != "request_sent" || key != (legacyKey{n, v.Step, v.Attempt}) {
		cmd.Process.Kill()
		t.Fatalf("held %+v while the durable state is %s into %+v", key, kind, v)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	inv := legacyInvocation{Args: recorded, Exit: legacyExit(<-done), Phase: v.Phase}
	g.unhold()
	g.invocations = append(g.invocations, inv)
	g.killed = &legacyKilled{run: n, kind: kind, seq: seq, view: v}
	g.logf("$ canti %s\n%s(%s at seq %d in %s, step %d, attempt %d)\n", strings.Join(recorded, " "), out, inv.Exit, seq, v.Phase, v.Step, v.Attempt)
	if inv.Exit != "signal killed" {
		t.Fatalf("the producer exited %s, not by SIGKILL", inv.Exit)
	}
}

// legacyKilled is the durable state a killed producer left, which only the
// checkpoint may read first: any other last connection would move the WAL.
type legacyKilled struct {
	run  int
	kind string
	seq  int64
	view legacyView
}
