package host

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// testHost opens a host on a fresh workspace and state directory.
func testHost(t *testing.T, opts Options) *Host {
	t.Helper()
	root := t.TempDir()
	opts.Workspace, opts.State = filepath.Join(root, "workspace"), filepath.Join(root, "state")
	for _, dir := range []string{opts.Workspace, opts.State} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	h, err := Open(context.Background(), opts, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Shutdown(context.Background()) })
	return h
}

func testSpec(h *Host) Spec {
	return Spec{
		Config: RunConfig{Workspace: h.opts.Workspace, Image: "debian:bookworm-slim", Network: "none", Memory: "2g", Pids: 512, OutputLimit: 8 << 20,
			API: APIMessages, Model: "test-model", MaxTokens: 1000, Structured: true, ContextBytes: 256 << 10, ContextVersion: ContextVersion, HandoverBytes: 2048},
		Limits: core.Limits{MaxSteps: 20, MaxAttempts: 2},
	}
}

// answering serves the Messages API, answering the n-th request with the
// n-th decision, and the last one after that.
func answering(t *testing.T, decisions ...core.Decision) string {
	t.Helper()
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		d := decisions[min(n, len(decisions)-1)]
		n++
		mu.Unlock()
		text, _ := json.Marshal(d)
		json.NewEncoder(w).Encode(map[string]any{"id": "msg", "type": "message", "role": "assistant", "model": "test-model", "stop_reason": "end_turn",
			"content": []map[string]string{{"type": "text", "text": string(text)}}, "usage": map[string]int{"input_tokens": 1, "output_tokens": 1}})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// holding serves a model that answers no request until its client goes away
// and reports each request on the returned channel.
func holding(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	got := make(chan struct{}, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		got <- struct{}{}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv.URL, got
}

func binding(url string) Binding { return Binding{Provider: url, Keys: Keys{AnthropicKey: "sk-test"}} }

func submit(t *testing.T, h *Host, s Submission) Ack {
	t.Helper()
	if s.Spec == (Spec{}) {
		s.Spec = testSpec(h)
	}
	if s.Goal == "" {
		s.Goal = "goal"
	}
	ack, err := h.Submit(context.Background(), s)
	if err != nil {
		t.Fatalf("Submit(%+v) = %v", s, err)
	}
	return ack
}

func kinds(t *testing.T, h *Host, id string) []string {
	t.Helper()
	ts, err := h.store.Transitions(context.Background(), id, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, tr := range ts {
		out = append(out, tr.Event.Kind())
	}
	return out
}

func TestSubmitAcknowledgesAdmissionAndNamesTheRunOfARetriedInput(t *testing.T) {
	var points []string
	h := testHost(t, Options{Checkpoint: func(p string) { points = append(points, p) }})
	url := "http://127.0.0.1:1"
	first := submit(t, h, Submission{InputID: "in-1", Goal: "g", Binding: binding(url)})
	if first.Replayed || first.Run.InputID != "in-1" || first.Run.SessionID != "" || first.Run.SessionSeq != 0 || first.Config.Provider != url {
		t.Fatalf("first ack = %+v", first)
	}
	if strings.Join(points, " ") != PointAdmitting+" "+PointSubmitted {
		t.Fatalf("checkpoints = %q, want the submission's admission and commit", points)
	}
	retry := submit(t, h, Submission{InputID: "in-1", Goal: "g", Binding: binding(url)})
	if !retry.Replayed || retry.Run.ID != first.Run.ID || retry.Snapshot.Seq != 0 {
		t.Fatalf("retry ack = %+v, want a replay of %s", retry, first.Run.ID)
	}
	if _, err := h.Submit(context.Background(), Submission{InputID: "in-1", Goal: "other", Spec: testSpec(h), Binding: binding(url)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("a retry of other content = %v, want ErrConflict", err)
	}
	if _, err := h.Submit(context.Background(), Submission{InputID: "in-2", Goal: "g", Spec: testSpec(h), Binding: binding(url)}); err == nil || !strings.Contains(err.Error(), "resume or stop it first") {
		t.Fatalf("a new input beside an open run = %v", err)
	}
	if _, err := h.Submit(context.Background(), Submission{SessionID: "s", InputID: "in-1", Goal: "g", Spec: testSpec(h), Binding: binding(url)}); err == nil {
		t.Fatal("a session input of the same ID replayed the standalone run instead of being refused beside it")
	}
}

func TestSessionAndStandaloneInputsAreDistinctKeys(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	for _, s := range []Submission{{InputID: "x"}, {SessionID: "s", InputID: "x"}} {
		s.Binding = binding(url)
		ack := submit(t, h, s)
		if ack.Replayed {
			t.Fatalf("%+v replayed %s", s, ack.Run.ID)
		}
		d, err := h.Drive(context.Background(), ack.Run.ID, binding(url), DriveOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseReturned {
			t.Fatalf("drive = %+v, %v", snap.State, err)
		}
		if again := submit(t, h, s); !again.Replayed || again.Run.ID != ack.Run.ID || again.Snapshot.State.Phase != core.PhaseReturned {
			t.Fatalf("retry of %+v = %+v", s, again)
		}
	}
}

func TestSubmitRefusesARunItsSubmitterCannotDrive(t *testing.T) {
	h := testHost(t, Options{})
	for _, tc := range []struct {
		b    Binding
		want string
	}{
		{Binding{Provider: "http://127.0.0.1:1"}, "to call the model"},
		{Binding{Provider: "http://127.0.0.1:1", Keys: Keys{OpenAIKey: "k"}}, "no credential is given or bound for anthropic-messages at http://127.0.0.1:1"},
		{Binding{Provider: "https://user:secret@model.test", Keys: Keys{AnthropicKey: "k"}}, "user information"},
		{Binding{Provider: "https://model.test/?key=1", Keys: Keys{AnthropicKey: "k"}}, "query"},
	} {
		if _, err := h.Submit(context.Background(), Submission{InputID: "i", Goal: "g", Spec: testSpec(h), Binding: tc.b}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Submit with %v = %v, want %q", tc.b, err, tc.want)
		}
		if strings.Contains(errString(h.Submit(context.Background(), Submission{Goal: "g", Spec: testSpec(h), Binding: tc.b})), "secret") {
			t.Errorf("an error shows the secret of %v", tc.b)
		}
	}
	if _, err := h.store.LastRun(context.Background()); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("a refused submission admitted a run: %v", err)
	}
	spec := testSpec(h)
	spec.Config.ContextBytes = 1024
	if _, err := h.Submit(context.Background(), Submission{Goal: "g", Spec: spec, Binding: binding("http://127.0.0.1:1")}); !errors.As(err, new(SpecError)) {
		t.Fatalf("a run that fits no context = %v, want a SpecError", err)
	}
}

func errString(_ Ack, err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestAKeyIsSentOnlyToTheProviderItIsBoundTo requires that the keys of a
// binding are used only when the provider it resolves to, the one it names,
// else its fallback, else the adapter's default, is the provider the run
// stores, that a run that stores none takes the binding's, and that a host's
// own credential is used only for its exact API and provider and only when
// the caller gives no key, whose own keys win.
func TestAKeyIsSentOnlyToTheProviderItIsBoundTo(t *testing.T) {
	stored := RunConfig{API: APIMessages, Provider: "https://stored.test"}
	own := Keys{AnthropicKey: "host-key"}
	given := Keys{AnthropicKey: "client-key"}
	for _, tc := range []struct {
		name  string
		c     RunConfig
		b     Binding
		host  map[Pair]Keys
		want  Keys
		fails string
	}{
		{"named", stored, Binding{Provider: "https://stored.test", Keys: given}, nil, given, ""},
		{"fallback", stored, Binding{Fallback: "https://stored.test", Keys: given}, nil, given, ""},
		{"default against a stored provider", stored, Binding{Keys: given}, nil, Keys{}, "credential given is for anthropic-messages at https://api.anthropic.com"},
		{"another fallback", stored, Binding{Fallback: "https://terminal.test", Keys: given}, nil, Keys{}, "credential given is for anthropic-messages at https://terminal.test"},
		{"another named provider", stored, Binding{Provider: "https://other.test", Keys: given}, nil, Keys{}, "credential given is for anthropic-messages at https://other.test"},
		{"keys of another API", stored, Binding{Provider: "https://stored.test", Keys: Keys{OpenAIKey: "o"}}, nil, Keys{}, "no credential is given or bound for anthropic-messages at https://stored.test"},
		{"host credential", stored, Binding{}, map[Pair]Keys{{APIMessages, "https://stored.test"}: own}, own, ""},
		{"host credential of another provider", stored, Binding{}, map[Pair]Keys{{APIMessages, "https://other.test"}: own}, Keys{}, "no credential is given or bound"},
		{"host credential of another API", stored, Binding{}, map[Pair]Keys{{APIResponses, "https://stored.test"}: {OpenAIKey: "o"}}, Keys{}, "no credential is given or bound"},
		{"a named other provider refuses the host credential too", stored, Binding{Provider: "https://other.test"}, map[Pair]Keys{{APIMessages, "https://stored.test"}: own}, Keys{}, "not https://other.test"},
		{"given keys win", stored, Binding{Fallback: "https://stored.test", Keys: given}, map[Pair]Keys{{APIMessages, "https://stored.test"}: own}, given, ""},
		{"a run storing no provider", RunConfig{API: APIMessages}, Binding{Fallback: "https://legacy.test", Keys: given}, nil, given, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &Host{opts: Options{Credentials: tc.host}}
			got, err := h.keysFor("r", tc.c, provider(tc.c, tc.b), tc.b)
			if got != tc.want || (err == nil) != (tc.fails == "") || err != nil && !strings.Contains(err.Error(), tc.fails) {
				t.Fatalf("keysFor = %v, %v; want %v, %q", got, err, tc.want, tc.fails)
			}
			if err != nil && strings.Contains(err.Error(), "key") && strings.Contains(err.Error(), "client-key") {
				t.Fatalf("the error shows a key: %v", err)
			}
		})
	}
	if p := provider(RunConfig{API: APIMessages}, Binding{}); p != "https://api.anthropic.com" {
		t.Fatalf("a run storing no provider and a binding naming none resolve to %q", p)
	}
}

// recording serves the Messages API, answering every request with decision
// and recording the key of each.
func recording(t *testing.T, decision core.Decision) (string, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		mu.Lock()
		keys = append(keys, r.Header.Get("x-api-key"))
		mu.Unlock()
		text, _ := json.Marshal(decision)
		json.NewEncoder(w).Encode(map[string]any{"id": "msg", "type": "message", "role": "assistant", "model": "test-model", "stop_reason": "end_turn",
			"content": []map[string]string{{"type": "text", "text": string(text)}}, "usage": map[string]int{"input_tokens": 1, "output_tokens": 1}})
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(keys) }
}

// TestADriveNeverSendsAKeyToAnUnboundProvider requires that a run stored
// against one provider is refused a drive whose caller's key is bound to
// another, before either provider gets a request, and that it is driven with
// the caller's key at its own provider once the caller names it.
func TestADriveNeverSendsAKeyToAnUnboundProvider(t *testing.T) {
	storedURL, storedKeys := recording(t, core.Decision{Kind: core.Return, Payload: "done"})
	otherURL, otherKeys := recording(t, core.Decision{Kind: core.Return, Payload: "elsewhere"})
	h := testHost(t, Options{})
	id := submit(t, h, Submission{Binding: binding(storedURL)}).Run.ID
	for _, b := range []Binding{{Fallback: otherURL, Keys: Keys{AnthropicKey: "sk-other"}}, {Keys: Keys{AnthropicKey: "sk-default"}}} {
		if _, err := h.Drive(context.Background(), id, b, DriveOptions{}); err == nil || !strings.Contains(err.Error(), "calls the model at "+storedURL) {
			t.Fatalf("Drive with %v = %v", b, err)
		}
	}
	d, err := h.Drive(context.Background(), id, Binding{Fallback: storedURL, Keys: Keys{AnthropicKey: "sk-stored"}}, DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("drive = %+v, %v", snap.State, err)
	}
	if got := storedKeys(); len(got) != 1 || got[0] != "sk-stored" || len(otherKeys()) != 0 {
		t.Fatalf("the stored provider got keys %q and the other %q", got, otherKeys())
	}
}

// TestAHostDrivesWithItsOwnCredentialOnlyForItsPair requires that a host's
// own credential continues an open run of its exact pair when no client
// gives a key, through ResumeOpen and through a keyless drive, that a run of
// another pair is not drivable, names the pair it needs, and is left
// untouched by ResumeOpen.
func TestAHostDrivesWithItsOwnCredentialOnlyForItsPair(t *testing.T) {
	url, keys := recording(t, core.Decision{Kind: core.Return, Payload: "done"})
	root := t.TempDir()
	opts := Options{Workspace: filepath.Join(root, "w"), State: filepath.Join(root, "s")}
	for _, dir := range []string{opts.Workspace, opts.State} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	h, err := Open(context.Background(), opts, true)
	if err != nil {
		t.Fatal(err)
	}
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	if v, err := h.View(context.Background(), id); err != nil || v.Drivable || !strings.Contains(v.Why, "credential for anthropic-messages at "+url) {
		t.Fatalf("view without a host credential = %+v, %v", v, err)
	}
	if err := h.ResumeOpen(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := kinds(t, h, id); len(got) != 0 || len(keys()) != 0 {
		t.Fatalf("ResumeOpen without a credential committed %q and sent %d requests", got, len(keys()))
	}
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	opts.Credentials = map[Pair]Keys{{APIMessages, "https://other.test"}: {AnthropicKey: "sk-other-pair"}, {APIMessages, url}: {AnthropicKey: "sk-host"}}
	h, err = Open(context.Background(), opts, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Shutdown(context.Background()) })
	if got := h.Bindings(); len(got) != 2 || got[0].Provider != "http"+strings.TrimPrefix(url, "http") && got[1].Provider != url {
		t.Fatalf("bindings = %v", got)
	}
	if v, err := h.View(context.Background(), id); err != nil || !v.Drivable || v.Why != "" {
		t.Fatalf("view with a host credential = %+v, %v", v, err)
	}
	if err := h.ResumeOpen(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		v, _ := h.View(context.Background(), id)
		return v.Snapshot.State.Phase == core.PhaseReturned && !v.Driving
	})
	if got := keys(); len(got) != 1 || got[0] != "sk-host" {
		t.Fatalf("the provider got keys %q, want the host's", got)
	}
	if err := h.ResumeOpen(context.Background()); err != nil || len(keys()) != 1 {
		t.Fatalf("ResumeOpen of a halted run = %v after %d requests", err, len(keys()))
	}
}

// gate holds every commit of the kinds it names until the commit's context
// ends, and reports on reached each kind it holds.
type gate struct {
	harness.Journal
	kinds   map[string]bool
	reached chan string
}

func (g gate) Commit(ctx context.Context, t harness.Transition) error {
	if g.kinds[t.Event.Kind()] {
		g.reached <- t.Event.Kind()
		<-ctx.Done()
		return ctx.Err()
	}
	return g.Journal.Commit(ctx, t)
}

// cancelling leaves the latest run of the state directory of opts, a new
// one of goal, cancelling: its stop is recorded and its cleanup held until
// the stop's deadline, and the host is shut down. It returns the run.
func cancelling(t *testing.T, opts Options, url, goal string) string {
	t.Helper()
	g := gate{kinds: map[string]bool{"performed": true, "observed": true}, reached: make(chan string, 4)}
	opts.WrapJournal = func(j harness.Journal) harness.Journal { g.Journal = j; return g }
	h, err := Open(context.Background(), opts, true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	id := submit(t, h, Submission{Binding: binding(url), Goal: goal}).Run.ID
	if _, err := h.Drive(context.Background(), id, binding(url), DriveOptions{}); err != nil {
		t.Fatal(err)
	}
	if k := <-g.reached; k != "performed" {
		t.Fatalf("held %s", k)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := h.Stop(ctx, id); err == nil {
		t.Fatal("the held stop finished")
	}
	if v, err := h.View(context.Background(), id); err != nil || v.Snapshot.State.Phase != core.PhaseCancelling || !v.Drivable {
		t.Fatalf("after the held stop: %+v, %v", v, err)
	}
	return id
}

// setContextVersion makes the stored config of run id in the state
// directory of opts record context version v.
func setContextVersion(t *testing.T, opts Options, id string, v int) {
	t.Helper()
	editConfig(t, opts, id, func(c map[string]any) { c["context_version"] = v })
}

// editConfig edits the stored config of run id in the state directory of
// opts with edit.
func editConfig(t *testing.T, opts Options, id string, edit func(map[string]any)) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(opts.State, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var data string
	if err := db.QueryRow(`SELECT run FROM runs WHERE id = ?`, id).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var run harness.Run
	if err := json.Unmarshal([]byte(data), &run); err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	if err := json.Unmarshal(run.Config, &c); err != nil {
		t.Fatal(err)
	}
	edit(c)
	config, _ := json.Marshal(c)
	if _, err := db.Exec(`UPDATE runs SET run = json_set(run, '$.Config', ?) WHERE id = ?`, base64.StdEncoding.EncodeToString(config), id); err != nil {
		t.Fatal(err)
	}
}

// TestResumeOpenFinishesAPendingStopAndLeavesOtherRuns requires that a host
// started without any credential finishes a stop that an earlier host left
// cancelling, of its own context version or of another that records its
// actions, that it leaves the pending stop of a run of an earlier version to
// an explicit stop, saying so, and that it commits nothing to an open run of
// another context version, which stays not drivable.
func TestResumeOpenFinishesAPendingStopAndLeavesOtherRuns(t *testing.T) {
	url := answering(t, core.Decision{Kind: core.Exec, Payload: "canti:nothing"})
	root := t.TempDir()
	opts := Options{Workspace: filepath.Join(root, "w"), State: filepath.Join(root, "s")}
	for _, dir := range []string{opts.Workspace, opts.State} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	restart := func() *Host {
		h, err := Open(context.Background(), opts, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Shutdown(context.Background()) })
		return h
	}
	for _, version := range []int{ContextVersion, ContextVersion + 1} {
		id := cancelling(t, opts, url, fmt.Sprint("version ", version))
		setContextVersion(t, opts, id, version)
		h := restart()
		if err := h.ResumeOpen(context.Background()); err != nil {
			t.Fatal(err)
		}
		v, err := h.View(context.Background(), id)
		if err != nil || v.Snapshot.State.Phase != core.PhaseStopped || v.Snapshot.State.Reason != core.StopCancelled || v.Drivable {
			t.Fatalf("a pending stop of version %d after ResumeOpen: %+v, %v", version, v.Snapshot.State, err)
		}
		h.Shutdown(context.Background())
	}

	legacy := cancelling(t, opts, url, "legacy")
	setContextVersion(t, opts, legacy, ActionsContextVersion-1)
	h := restart()
	before := kinds(t, h, legacy)
	if err := h.ResumeOpen(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := kinds(t, h, legacy); strings.Join(after, " ") != strings.Join(before, " ") {
		t.Fatalf("ResumeOpen committed %q to a run of an earlier version", after[len(before):])
	}
	if v, err := h.View(context.Background(), legacy); err != nil || v.Drivable || !strings.Contains(v.Why, "stop it with") {
		t.Fatalf("the pending stop of an earlier version: %+v, %v", v, err)
	}
	if snap, err := h.Stop(context.Background(), legacy); err != nil || snap.State.Phase != core.PhaseStopped {
		t.Fatalf("an explicit stop of it: %+v, %v", snap.State, err)
	}

	other := submit(t, h, Submission{Binding: binding(url), Goal: "other"}).Run.ID
	setContextVersion(t, opts, other, ContextVersion-1)
	if v, err := h.View(context.Background(), other); err != nil || v.Drivable || !strings.Contains(v.Why, "does not resume") {
		t.Fatalf("a run of another context version: %+v, %v", v, err)
	}
	before = kinds(t, h, other)
	if err := h.ResumeOpen(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := kinds(t, h, other); strings.Join(after, " ") != strings.Join(before, " ") {
		t.Fatalf("ResumeOpen committed %q to a run of another context version", after[len(before):])
	}
}

// TestADriveStartedAfterTheShutdownDoesNothing requires that a drive or a
// ResumeOpen asked of a host that began to shut down is refused as closing
// before it reads or prepares anything.
func TestADriveStartedAfterTheShutdownDoesNothing(t *testing.T) {
	h := testHost(t, Options{})
	id := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")}).Run.ID
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Drive(context.Background(), id, binding("http://127.0.0.1:1"), DriveOptions{}); !errors.Is(err, ErrClosing) {
		t.Fatalf("Drive after Shutdown = %v", err)
	}
	if err := h.ResumeOpen(context.Background()); !errors.Is(err, ErrClosing) {
		t.Fatalf("ResumeOpen after Shutdown = %v", err)
	}
}

func TestStopAndResolveNeedNoCredential(t *testing.T) {
	h := testHost(t, Options{})
	ack := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")})
	if _, err := h.ResolveJob(context.Background(), ack.Run.ID); err == nil || !strings.Contains(err.Error(), "no unknown outcome") {
		t.Fatalf("ResolveJob of an idle run = %v", err)
	}
	snap, err := h.Stop(context.Background(), ack.Run.ID)
	if err != nil || snap.State.Phase != core.PhaseStopped || snap.State.Reason != core.StopCancelled {
		t.Fatalf("Stop = %+v, %v", snap.State, err)
	}
}

func TestWaitSeesACommitThatLandsWhileItRegisters(t *testing.T) {
	var h *Host
	var once sync.Once
	var id string
	h = testHost(t, Options{Checkpoint: func(p string) {
		if p == PointWaiting {
			once.Do(func() {
				r, _ := harness.NewRunner(h.journal, noExecutor{})
				snap, _ := h.store.Latest(context.Background(), id)
				if _, err := r.Commit(context.Background(), snap, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
					t.Error(err)
				}
			})
		}
	}})
	id = submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")}).Run.ID
	h.drivers[id] = &driver{done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := h.Wait(ctx, Cursor{id, 0})
	if err != nil || r.Cursor.Seq != 1 || !r.Halted {
		t.Fatalf("Wait = %+v, %v, want the stop committed while it registered", r, err)
	}
	delete(h.drivers, id)
}

func TestWaitWakesForACommitAfterItsRecheck(t *testing.T) {
	h := testHost(t, Options{})
	id := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")}).Run.ID
	h.mu.Lock()
	h.drivers[id] = &driver{done: make(chan struct{})}
	h.mu.Unlock()
	got := make(chan WaitResult, 1)
	go func() {
		r, err := h.Wait(context.Background(), Cursor{id, 0})
		if err != nil {
			t.Error(err)
		}
		got <- r
	}()
	select {
	case r := <-got:
		t.Fatalf("Wait returned %+v before any commit", r)
	case <-time.After(50 * time.Millisecond):
	}
	r, _ := harness.NewRunner(h.journal, noExecutor{})
	snap, _ := h.store.Latest(context.Background(), id)
	if _, err := r.Commit(context.Background(), snap, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-got:
		if r.Cursor.Seq != 1 {
			t.Fatalf("Wait = %+v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait missed the commit")
	}
	h.mu.Lock()
	delete(h.drivers, id)
	h.mu.Unlock()
}

// noExecutor is the executor of a runner that only commits events.
type noExecutor struct{ harness.Executor }

func TestProgressPagesAreBoundedAndAdvancePastRowsThatShowNothing(t *testing.T) {
	h := testHost(t, Options{})
	big := "canti:x " + strings.Repeat("y", 100<<10)
	url := answering(t, core.Decision{Kind: core.Exec, Payload: big}, core.Decision{Kind: core.Exec, Payload: big},
		core.Decision{Kind: core.Exec, Payload: "canti:retrieve {}"}, core.Decision{Kind: core.Return, Payload: "done"})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	d, err := h.Drive(context.Background(), id, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := d.Result()
	if err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("drive = %+v, %v", snap.State, err)
	}
	ctx := context.Background()
	var all []Item
	pages := 0
	for after := (Cursor{id, 0}); ; pages++ {
		p, err := h.Progress(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if p.Next.Seq <= after.Seq && p.More {
			t.Fatalf("page after %d did not advance: %+v", after.Seq, p)
		}
		if again, _ := h.Progress(ctx, after, 1); len(again.Items) != len(p.Items) || again.Next != p.Next {
			t.Fatalf("the page after %d read again differs", after.Seq)
		}
		all = append(all, p.Items...)
		after = p.Next
		if !p.More {
			break
		}
	}
	var decided, completed, returned int
	for _, it := range all {
		switch it.Kind {
		case ItemDecided:
			decided++
		case ItemCompleted:
			completed++
			if !it.Local || it.Outcome == nil || it.ExitCode != nil {
				t.Errorf("local step %d = %+v, want an outcome and no exit code", it.Step, it)
			}
		case ItemReturned:
			returned++
		}
	}
	if decided != 3 || completed != 3 || returned != 1 || pages <= len(all) {
		t.Fatalf("%d pages of one row show %d decided, %d completed, %d returned items", pages, decided, completed, returned)
	}
	full, err := h.Progress(ctx, Cursor{id, 0}, 0)
	if err != nil {
		t.Fatal(err)
	}
	size, cut := 0, 0
	for _, it := range full.Items {
		size += it.bytes()
		if it.Truncated {
			cut++
		}
	}
	if full.More || size > PageBytes || len(full.Items) != len(all) || cut != 4 {
		t.Fatalf("a full page holds %d items of %d bytes, %d cut, more %v; want all %d, each of the two 100 KiB commands cut where it is shown twice",
			len(full.Items), size, cut, full.More, len(all))
	}
	if _, err := h.Progress(ctx, Cursor{id, snap.Seq + 1}, 0); !errors.Is(err, ErrCursor) {
		t.Fatal("a cursor past the run's last transition was accepted")
	}
	if _, err := h.Progress(ctx, Cursor{"run_other", 0}, 0); !errors.Is(err, ErrCursor) {
		t.Fatal("a cursor of an unknown run was accepted")
	}
}

func TestCompletedDrivesLeaveNothingInMemory(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	for i := range 20 {
		id := submit(t, h, Submission{Binding: binding(url), Goal: "goal " + string(rune('a'+i))}).Run.ID
		d, err := h.Drive(context.Background(), id, binding(url), DriveOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Result(); err != nil {
			t.Fatal(err)
		}
		if _, err := h.Stop(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.drivers) != 0 || len(h.runs) != 0 {
		t.Fatalf("after 20 runs the host holds %d drivers and %d run locks", len(h.drivers), len(h.runs))
	}
}

// blocking holds a commit of kind until release is closed, ignoring the
// context of the commit when stubborn is set, as a hung backend would.
type blocking struct {
	harness.Journal
	kind     string
	stubborn bool
	reached  chan struct{}
	release  chan struct{}
}

func (b blocking) Commit(ctx context.Context, t harness.Transition) error {
	if t.Event.Kind() == b.kind {
		close(b.reached)
		if b.stubborn {
			<-b.release
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-b.release:
			}
		}
	}
	return b.Journal.Commit(ctx, t)
}

func TestShutdownCommitsNothingOfItsOwnAndAdmitsNothingAfter(t *testing.T) {
	url, sent := holding(t)
	h := testHost(t, Options{})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	d, err := h.Drive(context.Background(), id, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-sent
	before := kinds(t, h, id)
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Result(); !errors.Is(err, context.Canceled) {
		t.Fatalf("the drive ended with %v, want its cancellation", err)
	}
	if _, err := h.Submit(context.Background(), Submission{Goal: "g", Spec: testSpec(h), Binding: binding(url)}); !errors.Is(err, ErrClosing) {
		t.Fatalf("Submit after Shutdown = %v", err)
	}
	if _, err := h.Stop(context.Background(), id); !errors.Is(err, ErrClosing) {
		t.Fatalf("Stop after Shutdown = %v", err)
	}
	reopened, err := Open(context.Background(), h.opts, false)
	if err != nil {
		t.Fatalf("the lock was not released: %v", err)
	}
	defer reopened.Close()
	after := kinds(t, reopened, id)
	if strings.Join(after, " ") != strings.Join(before, " ") || before[len(before)-1] != "attempt_started" {
		t.Fatalf("shutdown changed the journal from %q to %q", before, after)
	}
}

func TestShutdownWaitsForTheCommitInFlight(t *testing.T) {
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	b := blocking{kind: "response_saved", reached: make(chan struct{}), release: make(chan struct{})}
	h := testHost(t, Options{WrapJournal: func(j harness.Journal) harness.Journal { b.Journal = j; return b }})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	if _, err := h.Drive(context.Background(), id, binding(url), DriveOptions{}); err != nil {
		t.Fatal(err)
	}
	<-b.reached
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), h.opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := kinds(t, reopened, id); got[len(got)-1] != "attempt_started" {
		t.Fatalf("after shutdown the journal ends %q", got)
	}
}

func TestShutdownThatCannotQuiesceKeepsTheLock(t *testing.T) {
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	b := blocking{kind: "response_saved", stubborn: true, reached: make(chan struct{}), release: make(chan struct{})}
	h := testHost(t, Options{WrapJournal: func(j harness.Journal) harness.Journal { b.Journal = j; return b }})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	d, err := h.Drive(context.Background(), id, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-b.reached
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.Shutdown(ctx); !errors.Is(err, ErrNotQuiescent) {
		t.Fatalf("Shutdown = %v, want ErrNotQuiescent", err)
	}
	if _, err := Open(context.Background(), h.opts, false); !errors.Is(err, ErrLocked) {
		t.Fatalf("another host opened the state of a host that may still act: %v", err)
	}
	close(b.release)
	d.Result()
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestShutdownAwaitsASubmissionInItsAdmission requires that a submission
// held between its checks and its admission transaction keeps a shutdown
// from releasing the state directory until it has ended, that it is then
// either admitted or not, never after the release, and that no other host
// can open the state directory meanwhile.
func TestShutdownAwaitsASubmissionInItsAdmission(t *testing.T) {
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h := testHost(t, Options{Checkpoint: func(p string) {
		if p == PointAdmitting {
			once.Do(func() { close(reached); <-release })
		}
	}})
	submitted := make(chan error, 1)
	go func() {
		_, err := h.Submit(context.Background(), Submission{Goal: "g", Spec: testSpec(h), Binding: binding("http://127.0.0.1:1")})
		submitted <- err
	}()
	<-reached
	shut := make(chan error, 1)
	go func() { shut <- h.Shutdown(context.Background()) }()
	select {
	case err := <-shut:
		t.Fatalf("Shutdown returned %v while a submission was in its admission", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := Open(context.Background(), h.opts, false); !errors.Is(err, ErrLocked) {
		t.Fatalf("another host opened the state while a submission could still commit: %v", err)
	}
	close(release)
	if err := <-submitted; !errors.Is(err, context.Canceled) {
		t.Fatalf("the held submission ended with %v, want the shutdown's cancellation", err)
	}
	if err := <-shut; err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), h.opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.store.LastRun(context.Background()); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("a run was admitted after the shutdown began: %v", err)
	}
}

// TestCloseRefusesWhileAnOperationIsInFlight requires that Close never
// releases the state directory under an operation.
func TestCloseRefusesWhileAnOperationIsInFlight(t *testing.T) {
	h := testHost(t, Options{})
	_, leave, err := h.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); !errors.Is(err, ErrBusy) {
		t.Fatalf("Close with an operation in flight = %v", err)
	}
	leave()
	leave()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.Enter(context.Background()); !errors.Is(err, ErrClosing) {
		t.Fatalf("Enter after Close = %v", err)
	}
}

// TestShutdownEndsStopsInFlightAndQueued requires that a stop in its cleanup
// and a stop waiting for the run's lock both end when a shutdown begins: the
// first leaves the run with what it committed, the second commits nothing and
// is refused, and no row lands after the shutdown returned.
func TestShutdownEndsStopsInFlightAndQueued(t *testing.T) {
	b := blocking{kind: "stop_requested", reached: make(chan struct{}), release: make(chan struct{})}
	h := testHost(t, Options{WrapJournal: func(j harness.Journal) harness.Journal { b.Journal = j; return b }})
	id := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")}).Run.ID
	first := make(chan error, 1)
	go func() { _, err := h.Stop(context.Background(), id); first <- err }()
	<-b.reached
	second := make(chan error, 1)
	go func() { _, err := h.Stop(context.Background(), id); second <- err }()
	waitFor(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.runs[id] != nil && h.runs[id].n == 2
	})
	before := kinds(t, h, id)
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("the stop in flight ended with %v", err)
	}
	if err := <-second; !errors.Is(err, ErrClosing) {
		t.Fatalf("the queued stop ended with %v, want ErrClosing", err)
	}
	reopened, err := Open(context.Background(), h.opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	time.Sleep(50 * time.Millisecond)
	if after := kinds(t, reopened, id); strings.Join(after, " ") != strings.Join(before, " ") {
		t.Fatalf("the journal went from %q to %q", before, after)
	}
}

// TestKeyBearingValuesNeverShowTheirKeys requires that no formatting, JSON
// encoding, or structured log of keys or a binding shows a key.
func TestKeyBearingValuesNeverShowTheirKeys(t *testing.T) {
	k := Keys{AnthropicKey: "sk-secret-1", AnthropicToken: "sk-secret-2", OpenAIKey: "sk-secret-3"}
	b := Binding{Provider: "https://p.test", Keys: k}
	var out strings.Builder
	log := slog.New(slog.NewJSONHandler(&out, nil))
	log.Info("m", "keys", k, "binding", b, "wrapped", struct{ K Keys }{k})
	slog.New(slog.NewTextHandler(&out, nil)).Info("m", "keys", k, "binding", b)
	enc, err := json.Marshal(struct {
		K Keys
		B Binding
	}{k, b})
	if err != nil {
		t.Fatal(err)
	}
	all := out.String() + string(enc) + fmt.Sprintf("%v %+v %#v %v %+v %#v %s", k, k, k, b, b, b, fmt.Errorf("e: %v", b))
	if strings.Contains(all, "sk-secret") {
		t.Fatalf("a key shows in %s", all)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestPagesStayWithinTheirBoundsAtTheFactLimits requires that a run whose
// decisions hold texts of megabytes, of multi-byte runes, yields pages within
// PageBytes whose every text is within its cap, cut at a rune boundary, and
// marked Truncated, while View returns the full result of the return.
func TestPagesStayWithinTheirBoundsAtTheFactLimits(t *testing.T) {
	command := "canti:nothing " + strings.Repeat("é", 3<<19)
	result := strings.Repeat("€", 1<<20)
	url := answering(t, core.Decision{Kind: core.Exec, Payload: command, Purpose: strings.Repeat("ü", 4096)}, core.Decision{Kind: core.Return, Payload: result})
	h := testHost(t, Options{})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	d, err := h.Drive(context.Background(), id, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("drive = %+v, %v", snap.State, err)
	}
	if PageBytes < sqlite.ProgressRowBytes {
		t.Fatalf("an item of %d bytes may not fit a page of %d", sqlite.ProgressRowBytes, PageBytes)
	}
	var truncated []ItemKind
	for after := (Cursor{id, 0}); ; {
		p, err := h.Progress(context.Background(), after, 0)
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		for _, it := range p.Items {
			total += it.bytes()
			for _, f := range []struct {
				text  string
				limit int
			}{{it.Command, sqlite.ProgressCommandBytes}, {it.Purpose, sqlite.ProgressCommandBytes}, {it.Result, sqlite.ProgressResultBytes},
				{it.Preview, sqlite.ProgressPreviewBytes}, {it.Detail, sqlite.ProgressTextBytes}, {it.Failure, sqlite.ProgressTextBytes}} {
				if len(f.text) > f.limit || !utf8.ValidString(f.text) {
					t.Fatalf("item %+v has a text of %d bytes over %d, or cut inside a rune", it.Kind, len(f.text), f.limit)
				}
			}
			if it.Truncated {
				truncated = append(truncated, it.Kind)
			}
		}
		if total > PageBytes {
			t.Fatalf("a page holds %d bytes", total)
		}
		if !p.More {
			break
		}
		after = p.Next
	}
	if strings.Join(slices.Compact(slices.Sorted(slices.Values(truncatedNames(truncated)))), " ") != "completed decided returned" {
		t.Fatalf("truncated items %v", truncated)
	}
	if v, err := h.View(context.Background(), id); err != nil || v.Snapshot.State.Decision.Payload != result {
		t.Fatalf("View lost the full result: %d bytes, %v", len(v.Snapshot.State.Decision.Payload), err)
	}
}

func truncatedNames(kinds []ItemKind) []string {
	var out []string
	for _, k := range kinds {
		out = append(out, string(k))
	}
	return out
}

// TestACursorIsBoundToItsRun requires that Progress and Wait refuse a
// cursor of no run, or past the end of its run, as ErrCursor, and that a
// cursor of one run never reads another: the position of a run with more
// transitions is past the end of one with fewer.
func TestACursorIsBoundToItsRun(t *testing.T) {
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	h := testHost(t, Options{})
	first := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	d, err := h.Drive(context.Background(), first, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := d.Result()
	second := submit(t, h, Submission{Binding: binding(url), Goal: "second"}).Run.ID
	for _, c := range []Cursor{{second, snap.Seq}, {"run_none", 0}, {"", 0}, {first, -1}, {first, snap.Seq + 1}} {
		if _, err := h.Progress(context.Background(), c, 0); !errors.Is(err, ErrCursor) {
			t.Errorf("Progress(%+v) = %v", c, err)
		}
		if _, err := h.Wait(context.Background(), c); !errors.Is(err, ErrCursor) {
			t.Errorf("Wait(%+v) = %v", c, err)
		}
	}
	v, err := h.View(context.Background(), first)
	if err != nil || v.Cursor() != (Cursor{first, snap.Seq}) {
		t.Fatalf("view cursor = %+v, %v", v.Cursor(), err)
	}
	p, err := h.Progress(context.Background(), v.Cursor(), 0)
	if err != nil || len(p.Items) != 0 || p.More || !p.Halted || p.Next != v.Cursor() {
		t.Fatalf("the page after a halted run's last transition = %+v, %v", p, err)
	}
}

// TestAReplayNamesWhatTheRetryChanged requires that a replay names every
// setting in which the retried submission differs from the run first
// admitted for its input, and none when it repeats it.
func TestAReplayNamesWhatTheRetryChanged(t *testing.T) {
	url := "http://127.0.0.1:1"
	h := testHost(t, Options{})
	first := submit(t, h, Submission{InputID: "in", Binding: binding(url)})
	if same := submit(t, h, Submission{InputID: "in", Binding: binding(url)}); !same.Replayed || len(same.Differs) != 0 {
		t.Fatalf("a repeated submission replays as %+v", same)
	}
	spec := testSpec(h)
	spec.Config.Model, spec.Config.MaxTokens = "other-model", 2000
	spec.Budget.Tokens, spec.Limits.MaxSteps = 99, 7
	again := submit(t, h, Submission{InputID: "in", Spec: spec, Binding: binding("http://127.0.0.1:2")})
	if !again.Replayed || again.Run.ID != first.Run.ID || again.Config.Model != "test-model" ||
		strings.Join(again.Differs, " ") != "model provider max_tokens budget.tokens limits.max_steps" {
		t.Fatalf("a changed retry replays as %+v", again)
	}
}

// TestAURLItRefusesNeverShowsWhatMayHoldASecret requires that no error,
// formatting, or structured log of a refused or unvalidated provider URL
// shows its user information, query, fragment, or any of an input that does
// not parse, while a valid base URL, path included, stays shown whole.
func TestAURLItRefusesNeverShowsWhatMayHoldASecret(t *testing.T) {
	const marker = "DUMMYMARKER"
	bad := []string{
		"https://user" + marker + ":pw@h.test/v1", "https://user:pw" + marker + "@h.test/v1", "https://h.test/v1?key=" + marker,
		"https://h.test/v1#" + marker, "https://h.test/%zz" + marker, "h.test/v1?key=" + marker, "http://[::1" + marker + "/v1",
		"https://h.test/v1?" + marker, marker + "://x:y@h.test",
	}
	var out syncWriter
	logs := []*slog.Logger{slog.New(slog.NewJSONHandler(&out, nil)), slog.New(slog.NewTextHandler(&out, nil))}
	h := testHost(t, Options{})
	for _, u := range bad {
		err := CheckProvider(u)
		if err == nil {
			t.Fatalf("%q was accepted", u)
		}
		b := Binding{Provider: u, Fallback: u, Keys: Keys{AnthropicKey: "k"}}
		_, kerr := h.keysFor("r", RunConfig{API: APIMessages, Provider: "https://stored.test/v1"}, "https://stored.test/v1", Binding{Fallback: u, Keys: Keys{AnthropicKey: "k"}})
		_, nerr := h.keysFor("r", RunConfig{API: APIMessages, Provider: "https://stored.test/v1"}, "https://stored.test/v1", Binding{Provider: u})
		_, serr := h.Submit(context.Background(), Submission{Goal: "g", Spec: testSpec(h), Binding: Binding{Fallback: u, Keys: Keys{AnthropicKey: "k"}}})
		for _, l := range logs {
			l.Info("m", "binding", b, "wrapped", struct{ B Binding }{b}, "err", err)
		}
		all := fmt.Sprintf("%v %+v %#v %s %v %v %v", b, b, b, b, err, kerr, nerr) + errString(Ack{}, serr) + out.String()
		if strings.Contains(all, marker) || kerr == nil || nerr == nil || serr == nil {
			t.Fatalf("%q shows its secret, or was accepted: %s", u, all)
		}
	}
	for _, u := range []string{"https://gw.test/openai/v1", "http://127.0.0.1:8080/base/path"} {
		if err := CheckProvider(u); err != nil {
			t.Fatalf("a valid base URL %q = %v", u, err)
		}
		if d := DescribeURL(u); d != u {
			t.Fatalf("a valid base URL is described as %q", d)
		}
		_, err := h.keysFor("r", RunConfig{API: APIMessages, Provider: "https://stored.test/v1"}, "https://stored.test/v1", Binding{Fallback: u, Keys: Keys{AnthropicKey: "k"}})
		if err == nil || !strings.Contains(err.Error(), u) {
			t.Fatalf("a valid provider is not named in %v", err)
		}
	}
}

// syncWriter is a buffer that loggers write and a test reads.
type syncWriter struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncWriter) String() string { w.mu.Lock(); defer w.mu.Unlock(); return w.b.String() }

// TestHistoryImportsAreCanonicalAndNeverResolvedByTheHost requires that a
// submission naming a relative history imports directory is refused, that
// an absolute one through a symbolic link is stored canonical and reported
// as stored, and that an identical retry names no difference.
func TestHistoryImportsAreCanonicalAndNeverResolvedByTheHost(t *testing.T) {
	h := testHost(t, Options{})
	real := filepath.Join(t.TempDir(), "imports")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	canonical, _ := filepath.EvalSymlinks(real)
	spec := testSpec(h)
	spec.Config.HistoryImports = "imports"
	if _, err := h.Submit(context.Background(), Submission{InputID: "in", Goal: "g", Spec: spec, Binding: binding("http://127.0.0.1:1")}); !errors.As(err, new(SpecError)) ||
		!strings.Contains(err.Error(), "must be an absolute directory") {
		t.Fatalf("a relative history imports directory = %v", err)
	}
	spec.Config.HistoryImports = link
	first := submit(t, h, Submission{InputID: "in", Spec: spec, Binding: binding("http://127.0.0.1:1")})
	stored, err := DecodeConfig(first.Run, h.opts.Workspace)
	if err != nil || stored.HistoryImports != canonical || first.Config.HistoryImports != canonical {
		t.Fatalf("stored %q, acknowledged %q, want %q (%v)", stored.HistoryImports, first.Config.HistoryImports, canonical, err)
	}
	for _, dir := range []string{link, canonical} {
		spec.Config.HistoryImports = dir
		if again := submit(t, h, Submission{InputID: "in", Spec: spec, Binding: binding("http://127.0.0.1:1")}); !again.Replayed || len(again.Differs) != 0 {
			t.Fatalf("an identical retry naming %q replays as %+v", dir, again)
		}
	}
}

// TestAnItemIsTruncatedOnlyForATextItShows requires that a long purpose,
// which only a decided item shows, marks that item Truncated and neither the
// completed item of the same step nor a returned item, which show no purpose.
func TestAnItemIsTruncatedOnlyForATextItShows(t *testing.T) {
	long := strings.Repeat("p", 2*sqlite.ProgressCommandBytes)
	url := answering(t, core.Decision{Kind: core.Exec, Payload: "canti:nothing", Purpose: long}, core.Decision{Kind: core.Return, Payload: "done", Purpose: long})
	h := testHost(t, Options{})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	d, err := h.Drive(context.Background(), id, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("drive = %+v, %v", snap.State, err)
	}
	p, err := h.Progress(context.Background(), Cursor{id, 0}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[ItemKind]bool{}
	for _, it := range p.Items {
		got[it.Kind] = got[it.Kind] || it.Truncated
	}
	if !got[ItemDecided] || got[ItemCompleted] || got[ItemReturned] {
		t.Fatalf("truncated by kind %v, want only the decided item", got)
	}
}

// TestADriveOfARunStoringNoProviderChecksTheCallersProvider requires that a
// run stored before runs kept their provider, which takes its caller's, is
// refused a provider that admission would refuse, named or by fallback,
// before a model is built, a request sent, or anything committed or logged,
// with a description that shows none of its secret, and that a valid one
// still drives it.
func TestADriveOfARunStoringNoProviderChecksTheCallersProvider(t *testing.T) {
	const marker = "DUMMYMARKER"
	url, keys := recording(t, core.Decision{Kind: core.Return, Payload: "done"})
	var log syncWriter
	h := testHost(t, Options{Logger: slog.New(slog.NewTextHandler(&log, nil))})
	id := submit(t, h, Submission{Binding: binding(url)}).Run.ID
	editConfig(t, h.opts, id, func(c map[string]any) { delete(c, "provider") })
	if _, c, err := h.Find(context.Background(), id); err != nil || c.Provider != "" {
		t.Fatalf("the run stores provider %q, %v", c.Provider, err)
	}
	k := Keys{AnthropicKey: "sk-test"}
	for _, b := range []Binding{{Provider: url + "/v?key=" + marker, Keys: k}, {Fallback: url + "/v?key=" + marker, Keys: k},
		{Provider: "https://u" + marker + ":p@gw.test/v1", Keys: k}, {Fallback: "http://127.0.0.1:9/v#" + marker, Keys: k}, {Fallback: "%zz" + marker, Keys: k}} {
		_, err := h.Drive(context.Background(), id, b, DriveOptions{})
		if err == nil || strings.Contains(err.Error(), marker) {
			t.Fatalf("Drive with %v = %v", b, err)
		}
	}
	if got := kinds(t, h, id); len(got) != 0 || len(keys()) != 0 || strings.Contains(log.String(), marker) {
		t.Fatalf("refused drives committed %q, sent %d requests, or logged the marker: %s", got, len(keys()), log.String())
	}
	d, err := h.Drive(context.Background(), id, Binding{Fallback: url, Keys: k}, DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := d.Result(); err != nil || snap.State.Phase != core.PhaseReturned || len(keys()) != 1 {
		t.Fatalf("a valid drive = %+v, %v after %d requests", snap.State, err, len(keys()))
	}
}

// TestARetryNamesItsRunAfterItsImportsAreGone requires that an input whose
// history imports directory has since been removed still names the run first
// admitted for it, with no difference when it names the same directory,
// that a retry of other content still conflicts and writes nothing, and that
// a new input naming a missing or relative directory is refused admission.
func TestARetryNamesItsRunAfterItsImportsAreGone(t *testing.T) {
	h := testHost(t, Options{})
	dir := filepath.Join(t.TempDir(), "imports")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, _ := filepath.EvalSymlinks(dir)
	spec := testSpec(h)
	spec.Config.HistoryImports = canonical
	first := submit(t, h, Submission{InputID: "in", Spec: spec, Binding: binding("http://127.0.0.1:1")})
	if _, err := h.Stop(context.Background(), first.Run.ID); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(canonical); err != nil {
		t.Fatal(err)
	}
	again := submit(t, h, Submission{InputID: "in", Spec: spec, Binding: binding("http://127.0.0.1:1")})
	if !again.Replayed || again.Run.ID != first.Run.ID || len(again.Differs) != 0 {
		t.Fatalf("a retry after its imports were removed = %+v", again)
	}
	moved := spec
	moved.Config.HistoryImports = canonical + "-elsewhere"
	if other := submit(t, h, Submission{InputID: "in", Spec: moved, Binding: binding("http://127.0.0.1:1")}); !other.Replayed ||
		other.Run.ID != first.Run.ID || strings.Join(other.Differs, " ") != "history_imports" {
		t.Fatalf("a retry naming another missing directory = %+v", other)
	}
	if _, err := h.Submit(context.Background(), Submission{InputID: "in", Goal: "other", Spec: spec, Binding: binding("http://127.0.0.1:1")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("a retry of another goal = %v", err)
	}
	for _, dir := range []string{canonical, "imports"} {
		bad := spec
		bad.Config.HistoryImports = dir
		if _, err := h.Submit(context.Background(), Submission{InputID: "new-" + filepath.Base(dir), Goal: "g", Spec: bad, Binding: binding("http://127.0.0.1:1")}); !errors.As(err, new(SpecError)) {
			t.Fatalf("a new input naming %q = %v, want a spec error", dir, err)
		}
	}
	last, err := h.store.LastRun(context.Background())
	if err != nil || last.ID != first.Run.ID {
		t.Fatalf("the latest run is %s, %v; want only the first", last.ID, err)
	}
}
