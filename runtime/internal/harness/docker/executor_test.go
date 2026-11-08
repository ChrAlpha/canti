package docker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func newTestExecutor(t *testing.T, cfg Config) *Executor {
	t.Helper()
	if cfg.Image == "" {
		cfg.Image = "alpine:3.23"
	}
	if cfg.Workspace == "" {
		cfg.Workspace = t.TempDir()
	}
	if cfg.State == "" {
		cfg.State = t.TempDir()
	}
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestNewAppliesDefaults(t *testing.T) {
	x := newTestExecutor(t, Config{})
	want := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	c := x.cfg
	if c.Docker != "docker" || c.User != want || c.Network != "none" || c.StopGrace != 5*time.Second || c.OutputLimit != 8<<20 || c.PreviewLimit != 4<<10 {
		t.Fatalf("defaults = %+v", c)
	}
	for _, dir := range []string{"jobs", "artifacts"} {
		if fi, err := os.Stat(filepath.Join(c.State, dir)); err != nil || !fi.IsDir() {
			t.Fatalf("state %s: %v", dir, err)
		}
	}
}

func TestNewRejectsUnsafeConfig(t *testing.T) {
	root := t.TempDir()
	ws := filepath.Join(root, "ws")
	if err := os.Mkdir(ws, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(ws, link); err != nil {
		t.Fatal(err)
	}
	state, outer, records := filepath.Join(root, "state"), filepath.Join(root, "outer"), filepath.Join(root, "records")
	for _, dir := range []string{state, filepath.Join(ws, "state"), filepath.Join(root, "a:b"), outer, filepath.Join(outer, "state"), records} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	mount := func(source, target string) Config {
		return Config{Image: "i", Workspace: ws, State: state, Mounts: []Mount{{Source: records, Target: "/canti/runs"}, {Source: source, Target: target}}}
	}
	for _, tt := range []struct {
		name string
		cfg  Config
	}{
		{"no image", Config{Workspace: ws, State: state}},
		{"relative workspace", Config{Image: "i", Workspace: "ws", State: state}},
		{"relative state", Config{Image: "i", Workspace: ws, State: "state"}},
		{"missing workspace", Config{Image: "i", Workspace: filepath.Join(root, "none"), State: state}},
		{"missing state", Config{Image: "i", Workspace: ws, State: filepath.Join(root, "none")}},
		{"state file", Config{Image: "i", Workspace: ws, State: file}},
		{"workspace file", Config{Image: "i", Workspace: file, State: state}},
		{"state in workspace", Config{Image: "i", Workspace: ws, State: filepath.Join(ws, "state")}},
		{"state in linked workspace", Config{Image: "i", Workspace: ws, State: filepath.Join(link, "state")}},
		{"workspace in state", Config{Image: "i", Workspace: ws, State: root}},
		{"same dir", Config{Image: "i", Workspace: ws, State: link}},
		{"colon in path", Config{Image: "i", Workspace: ws, State: filepath.Join(root, "a:b")}},
		{"negative output limit", Config{Image: "i", Workspace: ws, State: state, OutputLimit: -1}},
		{"tiny preview", Config{Image: "i", Workspace: ws, State: state, PreviewLimit: 63}},
		{"negative grace", Config{Image: "i", Workspace: ws, State: state, StopGrace: -time.Second}},
		{"relative mount", mount("records", "/canti/run")},
		{"missing mount", mount(filepath.Join(root, "none"), "/canti/run")},
		{"mount file", mount(file, "/canti/run")},
		{"mount in workspace", mount(filepath.Join(ws, "state"), "/canti/run")},
		{"mount of linked workspace", mount(link, "/canti/run")},
		{"mount containing workspace", mount(root, "/canti/run")},
		{"mount containing state", Config{Image: "i", Workspace: ws, State: filepath.Join(outer, "state"), Mounts: []Mount{{Source: outer, Target: "/canti/run"}}}},
		{"mount with colon", mount(filepath.Join(root, "a:b"), "/canti/run")},
		{"relative target", mount(records, "canti/run")},
		{"unclean target", mount(records, "/canti/run/")},
		{"target with colon", mount(records, "/canti:run")},
		{"target at workspace", mount(records, WorkDir)},
		{"target in workspace", mount(records, WorkDir+"/records")},
		{"target at artifacts", mount(records, archive.ArtifactsRoot)},
		{"target over artifacts", mount(records, "/canti")},
		{"root target", mount(records, "/")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Fatalf("New(%+v) succeeded", tt.cfg)
			}
			for _, dir := range []string{filepath.Join(ws, "state"), state, filepath.Join(outer, "state")} {
				if _, err := os.Stat(filepath.Join(dir, "jobs")); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("rejected New() wrote into %s: %v", dir, err)
				}
			}
		})
	}
}

func TestCheckLayoutAgreesWithNewBeforeTheStateExists(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   func(root, ws, link string) string
		overlap bool
	}{
		{"apart", func(root, _, _ string) string { return filepath.Join(root, "a", "b", "state") }, false},
		{"apart with a shared prefix", func(root, _, _ string) string { return filepath.Join(root, "ws-state") }, false},
		{"inside", func(_, ws, _ string) string { return filepath.Join(ws, "a", "b", "state") }, true},
		{"the workspace", func(_, ws, _ string) string { return ws }, true},
		{"containing", func(root, _, _ string) string { return root }, true},
		{"inside through a link", func(_, _, link string) string { return filepath.Join(link, "a", "state") }, true},
		{"apart through a link named with a colon", func(root, _, _ string) string { return filepath.Join(root, "alias:x", "state") }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			ws, link := filepath.Join(root, "ws"), filepath.Join(root, "workspace-link")
			if err := os.Mkdir(ws, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(ws, link); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(root, "plain"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(root, "plain"), filepath.Join(root, "alias:x")); err != nil {
				t.Fatal(err)
			}
			tree := func() (paths []string) {
				if err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
					paths = append(paths, p)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return paths
			}
			state, before := tc.state(root, ws, link), tree()
			err := CheckLayout(link, state)
			if (err != nil) != tc.overlap || err != nil && !strings.Contains(err.Error(), "overlap") {
				t.Fatalf("CheckLayout(%s, %s) = %v, want an overlap %t", link, state, err, tc.overlap)
			}
			if after := tree(); !slices.Equal(after, before) {
				t.Fatalf("CheckLayout changed %q to %q", before, after)
			}
			if err := os.MkdirAll(state, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := New(Config{Image: "i", Workspace: link, State: state}); (err != nil) != tc.overlap {
				t.Fatalf("New() = %v once the state exists; CheckLayout predicted an overlap %t", err, tc.overlap)
			}
		})
	}
}

func TestCheckLayoutRefusesAStatePathItCannotResolve(t *testing.T) {
	root := t.TempDir()
	ws, file := filepath.Join(root, "real", "ws"), filepath.Join(root, "file")
	link, dangling := filepath.Join(root, "link"), filepath.Join(root, "dangling")
	if err := os.MkdirAll(ws, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "a:c"), 0o700); err != nil {
		t.Fatal(err)
	}
	for target, name := range map[string]string{ws: link, filepath.Join(ws, "missing"): dangling, filepath.Join(root, "a:c"): filepath.Join(root, "to-colon")} {
		if err := os.Symlink(target, name); err != nil {
			t.Fatal(err)
		}
	}
	tree := func() (paths []string) {
		if err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
			paths = append(paths, p)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return paths
	}
	for _, tc := range []struct{ name, state, want string }{
		{"relative", "state", "not a clean absolute path"},
		{"unclean, inside the workspace through a link", link + "/../ws/state", "not a clean absolute path"},
		{"below a file", filepath.Join(file, "state"), filepath.Join(file, "state")},
		{"below a link to a missing directory of the workspace", filepath.Join(dangling, "state"), filepath.Join(ws, "missing")},
		{"with a colon", filepath.Join(root, "a:b", "state"), "is not a directory that can be mounted"},
		{"below a link to a directory with a colon", filepath.Join(root, "to-colon", "state"), "is not a directory that can be mounted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tree()
			if err := CheckLayout(ws, tc.state); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckLayout(%s, %s) = %v, want an error naming %q", ws, tc.state, err, tc.want)
			}
			if after := tree(); !slices.Equal(after, before) {
				t.Fatalf("CheckLayout changed %q to %q", before, after)
			}
		})
	}
}

func TestCreateRequestIsolatesTheJob(t *testing.T) {
	runs, records := t.TempDir(), t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(records, link); err != nil {
		t.Fatal(err)
	}
	x := newTestExecutor(t, Config{User: "1234:5678", Memory: "256m", CPUs: "0.5", PidsLimit: 64,
		Mounts: []Mount{{Source: runs, Target: "/canti/runs"}, {Source: link, Target: "/canti/run"}}})
	spec := x.Specification("echo hi")
	r, err := x.request(harness.Effect{ID: "effect_1", Command: "echo hi"}, spec)
	if err != nil {
		t.Fatal(err)
	}
	env := []string{"TINI_KILL_PROCESS_GROUP=1"}
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "FTP_PROXY", "ALL_PROXY"} {
		env = append(env, k+"=", strings.ToLower(k)+"=")
	}
	labels := map[string]string{"canti.effect": "effect_1", "canti.state": x.cfg.State, "canti.spec": spec, "canti.exit": exitFile}
	if r.User != "1234:5678" || r.Image != "alpine:3.23" || r.WorkingDir != WorkDir || !r.AttachStdout || !r.AttachStderr ||
		!slices.Equal(r.Entrypoint, []string{"/bin/sh"}) || !slices.Equal(r.Cmd, []string{"-c", wrapper, "canti", "exec 2>&1\necho hi"}) ||
		!slices.Equal(r.Env, env) || !maps.Equal(r.Labels, labels) {
		t.Errorf("create request = %+v", r)
	}
	h := r.HostConfig
	binds := []string{x.cfg.Workspace + ":" + WorkDir, filepath.Join(x.cfg.State, "artifacts") + ":" + archive.ArtifactsRoot + ":ro",
		runs + ":/canti/runs:ro", records + ":/canti/run:ro"}
	if !slices.Equal(h.Binds, binds) || h.NetworkMode != "none" || !slices.Equal(h.CapDrop, []string{"ALL"}) ||
		!slices.Equal(h.SecurityOpt, []string{"no-new-privileges"}) || !h.Init || h.Memory != 256<<20 || h.NanoCpus != 5e8 ||
		h.PidsLimit == nil || *h.PidsLimit != 64 || h.LogConfig.Type != "json-file" ||
		!maps.Equal(h.LogConfig.Config, map[string]string{"max-size": "1p", "mode": "blocking"}) {
		t.Errorf("host config = %+v", h)
	}

	fields := []string{"AttachStderr", "AttachStdout", "Cmd", "Entrypoint", "Env", "HostConfig", "Image", "Labels", "User", "WorkingDir"}
	host := []string{"Binds", "CapDrop", "Init", "LogConfig", "NetworkMode", "SecurityOpt"}
	for cfg, limits := range map[*Config][]string{&x.cfg: {"Memory", "NanoCpus", "PidsLimit"}, &newTestExecutor(t, Config{}).cfg: nil} {
		r, err := (&Executor{cfg: *cfg}).request(harness.Effect{ID: "e", Command: "true"}, "spec")
		data, merr := json.Marshal(r)
		var sent map[string]json.RawMessage
		var sentHost map[string]json.RawMessage
		if err == nil {
			err = errors.Join(merr, json.Unmarshal(data, &sent), json.Unmarshal(sent["HostConfig"], &sentHost))
		}
		if err != nil || !slices.Equal(slices.Sorted(maps.Keys(sent)), fields) ||
			!slices.Equal(slices.Sorted(maps.Keys(sentHost)), slices.Sorted(slices.Values(append(host, limits...)))) {
			t.Errorf("create request sends %s, %v; want exactly %q and host config %q", data, err, fields, append(host, limits...))
		}
	}
	for _, bad := range []Config{{Memory: "1x"}, {CPUs: "0.0000000001"}} {
		if _, err := (&Executor{cfg: bad}).request(harness.Effect{ID: "e", Command: "true"}, "spec"); err == nil {
			t.Errorf("request(%+v) succeeded", bad)
		}
	}
}

func TestSpecHashCoversTheExecutionSpec(t *testing.T) {
	ws, state, records := t.TempDir(), t.TempDir(), t.TempDir()
	base := Config{Image: "alpine:3.23", Workspace: ws, State: state}
	hash := func(cfg Config, command string) string {
		return newTestExecutor(t, cfg).Specification(command)
	}
	want := hash(base, "echo hi")
	if again := hash(base, "echo hi"); again != want {
		t.Fatalf("hash is not deterministic: %s != %s", again, want)
	}
	policy := base
	policy.StopGrace, policy.OutputLimit, policy.PreviewLimit, policy.Docker = time.Second, 1, 64, "/usr/bin/docker"
	if got := hash(policy, "echo hi"); got == want {
		t.Fatalf("output and stop policy were omitted from specification")
	}
	seen := map[string]string{want: "base"}
	for name, change := range map[string]func(*Config){
		"image":     func(c *Config) { c.Image = "alpine:3.22" },
		"workspace": func(c *Config) { c.Workspace = t.TempDir() },
		"state":     func(c *Config) { c.State = t.TempDir() },
		"user":      func(c *Config) { c.User = "1:1" },
		"network":   func(c *Config) { c.Network = "bridge" },
		"memory":    func(c *Config) { c.Memory = "64m" },
		"cpus":      func(c *Config) { c.CPUs = "1" },
		"pids":      func(c *Config) { c.PidsLimit = 10 },
		"mount":     func(c *Config) { c.Mounts = []Mount{{Source: records, Target: "/canti/run"}} },
		"target":    func(c *Config) { c.Mounts = []Mount{{Source: records, Target: "/canti/runs"}} },
		"command":   nil,
	} {
		cfg, command := base, "echo hi"
		if change != nil {
			change(&cfg)
		} else {
			command = "echo bye"
		}
		got := hash(cfg, command)
		if other, dup := seen[got]; dup {
			t.Errorf("changing %s kept the %s hash", name, other)
		}
		seen[got] = name
	}
}

func TestCLIProcessesLeaveTheCallersProcessGroup(t *testing.T) {
	dir := t.TempDir()
	cli := filepath.Join(dir, "docker")
	if err := os.WriteFile(cli, fmt.Appendf(nil, "#!/bin/sh\nps -o pgid= -p $$ > %q/$1\n", dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := newTestExecutor(t, Config{Docker: cli}).pull(t.Context()); err != nil {
		t.Fatal(err)
	}
	conn, err := dialStdio(t.Context(), cli)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	for _, name := range []string{"pull", "system"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if pgid := strings.TrimSpace(string(b)); err != nil || pgid == "" || pgid == strconv.Itoa(syscall.Getpgrp()) {
			t.Errorf("docker %s ran in process group %q, %v; it shares the caller's, so a terminal interrupt can abort a launch", name, pgid, err)
		}
	}
}

func TestPullDeadlineTerminatesItsOwnedProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "child.pid")
	cli := filepath.Join(dir, "docker")
	script := fmt.Sprintf("#!/bin/sh\nsleep 300 &\necho $! > %q\nwait\n", pidfile)
	if err := os.WriteFile(cli, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	x := newTestExecutor(t, Config{Docker: cli})
	x.timeouts.transfer = 50 * time.Millisecond
	began := time.Now()
	if err := x.pull(t.Context()); err == nil || time.Since(began) > 2*time.Second {
		t.Fatalf("pull deadline: %v after %v", err, time.Since(began))
	}
	b, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 1 {
		t.Fatalf("owned child PID: %q %v", b, err)
	}
	cleanup := true
	t.Cleanup(func() {
		if cleanup {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	for deadline := time.Now().Add(time.Second); ; {
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		fields := strings.Fields(string(stat))
		if os.IsNotExist(err) || len(fields) > 2 && fields[2] == "Z" {
			cleanup = false
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("owned pull child %d remained running after deadline", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
