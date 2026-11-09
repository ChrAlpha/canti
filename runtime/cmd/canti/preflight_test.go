package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func testPreflightValidation(t *testing.T) hostrpc.Validation {
	t.Helper()
	dir := t.TempDir()
	workspace := filepath.Join(dir, "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return hostrpc.Validation{
		State: filepath.Join(dir, "state"),
		Submission: hostrpc.Submission{
			SessionID: "session-qa", Goal: "qa preflight", ExplicitConstraints: true,
			Spec: hostrpc.Spec{Ephemeral: true,
				Workspace: workspace, Image: "debian:bookworm-slim", Network: "none", Memory: "2g",
				Pids: 512, OutputLimit: 8 << 20, API: "openai-responses", Model: "qa-model",
				MaxTokens: 16000, Structured: true, RequestBytes: 256 << 10,
				MaxSteps: 20, MaxAttempts: 3, WallTime: "30m",
				OutputBudget: 256 << 20, JobTimeout: "10m",
			},
		},
		Binding: hostrpc.Binding{OpenAIKey: "x"},
	}
}

func callPreflight(t *testing.T, body []byte) hostrpc.Response {
	t.Helper()
	var out bytes.Buffer
	if code := preflight(context.Background(), bytes.NewReader(body), &out); code != exitOK {
		t.Fatalf("preflight exit = %d, want %d", code, exitOK)
	}
	var response hostrpc.Response
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatalf("preflight response: %v", err)
	}
	if response.Protocol != hostrpc.Protocol {
		t.Fatalf("response protocol = %d, want %d", response.Protocol, hostrpc.Protocol)
	}
	return response
}

func preflightRequest(t *testing.T, validation hostrpc.Validation) []byte {
	t.Helper()
	body, err := json.Marshal(hostrpc.Request{Protocol: hostrpc.Protocol, Op: hostrpc.OpValidate, Validate: &validation})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertNoState(t *testing.T, state string) {
	t.Helper()
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preflight created state %s: %v", state, err)
	}
}

func TestPreflightRejectsInvalidSpecsWithoutCreatingState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*hostrpc.Validation)
		valid  bool
	}{
		{"valid", func(*hostrpc.Validation) {}, true},
		{"layout", func(v *hostrpc.Validation) { v.State = filepath.Join(v.Submission.Spec.Workspace, "state") }, false},
		{"CPU", func(v *hostrpc.Validation) { v.Submission.Spec.CPUs = "0.0000000001" }, false},
		{"memory", func(v *hostrpc.Validation) { v.Submission.Spec.Memory = "1x" }, false},
		{"provider", func(v *hostrpc.Validation) { v.Binding.Provider = "relative-provider" }, false},
		{"context", func(v *hostrpc.Validation) { v.Submission.Spec.RequestBytes = 1 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validation := testPreflightValidation(t)
			tc.change(&validation)
			response := callPreflight(t, preflightRequest(t, validation))
			if tc.valid && (response.Validated == nil || response.Error != nil) {
				t.Fatalf("valid preflight = %+v", response)
			}
			if !tc.valid && (response.Error == nil || response.Validated != nil) {
				t.Fatalf("invalid preflight = %+v", response)
			}
			assertNoState(t, validation.State)
		})
	}
}

func TestPreflightStandaloneKeepsSuppliedConstraintsWithoutCreatingState(t *testing.T) {
	validation := testPreflightValidation(t)
	validation.Submission.SessionID = ""
	validation.Submission.ExplicitConstraints = false
	validation.Submission.Constraints = "supplied standalone rules"
	response := callPreflight(t, preflightRequest(t, validation))
	if response.Error != nil || response.Validated == nil || response.Validated.Constraints != validation.Submission.Constraints {
		t.Fatalf("standalone preflight = %+v", response)
	}
	assertNoState(t, validation.State)
}

func TestPreflightRejectsTrailingDocumentBeforeValidation(t *testing.T) {
	validation := testPreflightValidation(t)
	body := append(preflightRequest(t, validation), []byte(" {}")...)
	response := callPreflight(t, body)
	if response.Error == nil || response.Validated != nil {
		t.Fatalf("trailing request accepted: %+v", response)
	}
	assertNoState(t, validation.State)
}

func TestCompiledPreflightRemovesProviderCredentialsBeforeDockerFailure(t *testing.T) {
	v := testPreflightValidation(t)
	c := &v.Submission.Spec
	c.Ephemeral, c.Environment = false, filepath.Join(t.TempDir(), "environment")
	c.Image, c.Network, c.Memory, c.CPUs, c.Pids = "", "", "", "", 0
	helperBytes := []byte("#!/bin/sh\necho 1\n")
	helper := filepath.Join(t.TempDir(), "helper")
	if err := os.WriteFile(helper, helperBytes, 0555); err != nil {
		t.Fatal(err)
	}
	if err := docker.InitEnvironment(c.Environment, helper); err != nil {
		t.Fatal(err)
	}
	authority, err := docker.EnvironmentIdentity(c.Environment)
	if err != nil {
		t.Fatal(err)
	}
	digest := harness.ConfigDigest(helperBytes)
	a := docker.Attachment{Format: 1, Directory: c.Environment, Endpoint: "unix:///synthetic-preflight.sock", DaemonID: "daemon", ContainerID: strings.Repeat("a", 64), AuthorityID: authority, Incarnation: 1,
		Helper: "/canti/bin/" + digest + "/canti-job", HelperDigest: digest, User: "0:0", Home: "/root", Path: "/usr/bin:/bin", AdmissionEpoch: "epoch"}
	a.Mounts = []docker.EnvironmentMount{{Type: "bind", Source: c.Workspace, Target: "/workspace", Writable: true}, {Type: "bind", Source: filepath.Join(a.Directory, "public"), Target: "/canti"}, {Type: "bind", Source: filepath.Join(a.Directory, "spool"), Target: "/var/lib/canti/jobs", Writable: true}}
	data, err := json.Marshal(a)
	if err == nil {
		err = os.WriteFile(filepath.Join(c.Environment, "authority", "environment.json"), data, 0600)
	}
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "canti")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public canti: %v %s", err, out)
	}
	commands := t.TempDir()
	observed := filepath.Join(commands, "observed")
	// Record presence only, never values. The first Docker subprocess fails
	// before transport I/O, exercising the public command's early failure path.
	script := "#!/bin/sh\nprintf 'called\\n' >\"$QA_OBSERVED\"\n" +
		"[ \"${OPENAI_API_KEY+x}\" != x ] || printf 'OPENAI_API_KEY\\n' >>\"$QA_OBSERVED\"\n" +
		"[ \"${ANTHROPIC_API_KEY+x}\" != x ] || printf 'ANTHROPIC_API_KEY\\n' >>\"$QA_OBSERVED\"\n" +
		"[ \"${ANTHROPIC_AUTH_TOKEN+x}\" != x ] || printf 'ANTHROPIC_AUTH_TOKEN\\n' >>\"$QA_OBSERVED\"\nexit 1\n"
	if err := os.WriteFile(filepath.Join(commands, "docker"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), binary, "preflight")
	cmd.Env = []string{"PATH=" + commands + ":/usr/bin:/bin", "QA_OBSERVED=" + observed,
		"OPENAI_API_KEY=synthetic-openai-sentinel", "ANTHROPIC_API_KEY=synthetic-anthropic-sentinel", "ANTHROPIC_AUTH_TOKEN=synthetic-token-sentinel"}
	cmd.Stdin = bytes.NewReader(preflightRequest(t, v))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("preflight transport response: %v %s", err, out)
	}
	var response hostrpc.Response
	if err := json.Unmarshal(out, &response); err != nil || response.Error == nil || response.Validated != nil {
		t.Fatalf("Docker failure response=%s: %v", out, err)
	}
	seen, err := os.ReadFile(observed)
	if err != nil || string(seen) != "called\n" {
		t.Fatalf("first Docker subprocess inherited credential names: %q: %v", seen, err)
	}
	assertNoState(t, v.State)
}

// The test binary acts as Docker's stdio transport, so this boundary test needs
// neither an installed Docker CLI nor a daemon. It never dispatches an exec.
func preflightDockerProxy() int {
	if len(os.Args) != 5 || os.Args[1] != "--host" || os.Args[3] != "system" || os.Args[4] != "dial-stdio" {
		return 2
	}
	conn, err := net.Dial("unix", strings.TrimPrefix(os.Args[2], "unix://"))
	if err != nil {
		return 1
	}
	defer conn.Close()
	go func() { _, _ = io.Copy(conn, os.Stdin) }()
	_, err = io.Copy(os.Stdout, conn)
	if err != nil {
		return 1
	}
	return 0
}

func TestPersistentPreflightChecksActualMountsBeforeCreatingState(t *testing.T) {
	v := testPreflightValidation(t)
	v.State = filepath.Join(t.TempDir(), "state")
	c := &v.Submission.Spec
	c.Ephemeral, c.Environment = false, filepath.Join(filepath.Dir(c.Workspace), "environment")
	c.Image, c.Network, c.Memory, c.CPUs, c.Pids = "", "", "", "", 0
	helperBytes := []byte("#!/bin/sh\necho 1\n")
	helper := filepath.Join(t.TempDir(), "helper")
	if err := os.WriteFile(helper, helperBytes, 0555); err != nil {
		t.Fatal(err)
	}
	if err := docker.InitEnvironment(c.Environment, helper); err != nil {
		t.Fatal(err)
	}
	authority, err := docker.EnvironmentIdentity(c.Environment)
	if err != nil {
		t.Fatal(err)
	}
	// Only the transport socket needs a short path; state, mounts and helper
	// fixtures still exercise the caller's original (possibly long) TMPDIR.
	socketDir := shortSocketDir(t)
	endpoint := filepath.Join(socketDir, "d.sock")
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	digest := harness.ConfigDigest(helperBytes)
	a := docker.Attachment{Format: 1, Directory: c.Environment, Endpoint: "unix://" + endpoint, DaemonID: "daemon", ContainerID: strings.Repeat("a", 64), AuthorityID: authority, Incarnation: 1,
		Helper: "/canti/bin/" + digest + "/canti-job", HelperDigest: digest, User: "1000", Home: "/home/developer", Path: "/usr/bin:/bin", AdmissionEpoch: "epoch"}
	a.Mounts = []docker.EnvironmentMount{{Type: "bind", Source: c.Workspace, Target: "/workspace", Writable: true}, {Type: "bind", Source: filepath.Join(a.Directory, "public"), Target: "/canti"}, {Type: "bind", Source: filepath.Join(a.Directory, "spool"), Target: "/var/lib/canti/jobs", Writable: true}}
	for name, value := range map[string]any{"environment.json": a, "state.json": map[string]any{"Format": 1, "Gate": "open", "AuthorityID": authority}} {
		data, err := json.Marshal(value)
		if err == nil {
			err = os.WriteFile(filepath.Join(c.Environment, "authority", name), data, 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	exposed := ""
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/version":
			io.WriteString(w, `{"ApiVersion":"1.47","MinAPIVersion":"1.30"}`)
		case strings.HasSuffix(r.URL.Path, "/info"):
			json.NewEncoder(w).Encode(map[string]string{"ID": "daemon", "OSType": "linux", "Architecture": runtime.GOARCH})
		case strings.HasSuffix(r.URL.Path, "/json"):
			mounts := []map[string]any{{"Type": "bind", "Source": c.Workspace, "Destination": "/workspace", "RW": true}, {"Type": "bind", "Source": filepath.Join(a.Directory, "public"), "Destination": "/canti", "RW": false}, {"Type": "bind", "Source": filepath.Join(a.Directory, "spool"), "Destination": "/var/lib/canti/jobs", "RW": true}}
			mu.Lock()
			extra := exposed
			mu.Unlock()
			if extra != "" {
				mounts = append(mounts, map[string]any{"Type": "bind", "Source": extra, "Destination": "/hidden-state", "RW": false})
			}
			json.NewEncoder(w).Encode(map[string]any{"ID": a.ContainerID, "Config": map[string]any{"Labels": map[string]string{"io.canti.control": a.Directory, "io.canti.authority": authority}}, "Mounts": mounts, "State": map[string]any{"Running": true}})
		default:
			http.Error(w, "unexpected mutation", http.StatusBadRequest)
		}
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	commands := t.TempDir()
	if err := os.Symlink(binary, filepath.Join(commands, "docker")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", commands+string(os.PathListSeparator)+os.Getenv("PATH"))
	response := callPreflight(t, preflightRequest(t, v))
	if response.Error != nil || response.Validated == nil {
		t.Fatalf("valid persistent preflight: %+v", response.Error)
	}
	assertNoState(t, v.State)

	for _, part := range []string{"public", "spool"} {
		for _, suffix := range []string{"state", "missing/nested/state"} {
			alias := filepath.Join(t.TempDir(), "environment-alias")
			if err := os.Symlink(filepath.Join(c.Environment, part), alias); err != nil {
				t.Fatal(err)
			}
			input := v
			input.State = filepath.Join(alias, suffix)
			result := callPreflight(t, preflightRequest(t, input))
			if result.Error == nil || !strings.Contains(result.Error.Message, "exposes run state") || result.Validated != nil {
				t.Fatalf("preflight admitted State alias into %s: %+v", part, result)
			}
			spec, err := hostserver.SpecFrom(input.Submission.Spec)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.ResolveExecution(t.Context(), spec.Config, input.State); err == nil || !strings.Contains(err.Error(), "exposes run state") {
				t.Fatalf("direct host admitted State alias: %v", err)
			}
			cli := runCLI(t, []string{"OPENAI_API_KEY=fixture"}, "run", "-workspace", c.Workspace, "-state", input.State, "-environment", c.Environment, "-api", "openai-responses", "-model", "fixture", "-model-url", "http://127.0.0.1:1", "-max-attempts", "1", "-goal", "reject exposed state")
			if cli.code != exitUsage || !strings.Contains(cli.stderr, "exposes run state") {
				t.Fatalf("CLI admitted State alias: %+v", cli)
			}
			assertNoState(t, filepath.Join(c.Environment, part, strings.Split(suffix, "/")[0]))
		}
	}
	outside := filepath.Join(t.TempDir(), "safe-alias")
	if err := os.Symlink(filepath.Dir(v.State), outside); err != nil {
		t.Fatal(err)
	}
	aliased := v
	aliased.State = filepath.Join(outside, "missing", "state")
	normalized := callPreflight(t, preflightRequest(t, aliased))
	if normalized.Error != nil || normalized.Validated == nil || normalized.Validated.State != filepath.Join(filepath.Dir(v.State), "missing", "state") {
		t.Fatalf("safe alias was not normalized: %+v", normalized)
	}
	assertNoState(t, filepath.Join(filepath.Dir(v.State), "missing"))
	alias := filepath.Join(t.TempDir(), "state-alias")
	if err := os.Symlink(filepath.Dir(v.State), alias); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ source, message string }{{filepath.Dir(v.State), "exposes run state"}, {alias, "symlink"}} {
		mu.Lock()
		exposed = tc.source
		mu.Unlock()
		response = callPreflight(t, preflightRequest(t, v))
		if response.Error == nil || !strings.Contains(response.Error.Message, tc.message) || response.Validated != nil {
			t.Fatalf("actual hidden mount %s was admitted: %+v", tc.source, response)
		}
		assertNoState(t, v.State)
	}
}
