package docker

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/archive"
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
	for target, name := range map[string]string{ws: link, filepath.Join(ws, "missing"): dangling} {
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

func pair(args []string, flag, value string) bool {
	for i := range len(args) - 1 {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func TestCreateArgsIsolateTheJob(t *testing.T) {
	runs, records := t.TempDir(), t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(records, link); err != nil {
		t.Fatal(err)
	}
	x := newTestExecutor(t, Config{User: "1234:5678", Memory: "256m", CPUs: "0.5", PidsLimit: 64,
		Mounts: []Mount{{Source: runs, Target: "/canti/runs"}, {Source: link, Target: "/canti/run"}}})
	run := x.runArgs("echo hi")
	args := x.createArgs("effect_1", run)
	mounts := []string{"-v", x.cfg.Workspace + ":" + WorkDir, "-v", filepath.Join(x.cfg.State, "artifacts") + ":" + archive.ArtifactsRoot + ":ro",
		"-v", runs + ":/canti/runs:ro", "-v", records + ":/canti/run:ro", "alpine:3.23"}
	if i := slices.Index(args, "-v"); i < 0 || !slices.Equal(args[i:i+len(mounts)], mounts) {
		t.Errorf("create args do not mount %q in order: %q", mounts, args)
	}
	for _, p := range [][2]string{
		{"--name", "canti-effect_1"},
		{"--label", "canti.effect=effect_1"},
		{"--label", "canti.state=" + x.cfg.State},
		{"--label", "canti.spec=" + specHash(run)},
		{"--label", "canti.exit=" + exitFile},
		{"-e", "TINI_KILL_PROCESS_GROUP=1"},
		{"--entrypoint", "/bin/sh"},
		{"--workdir", WorkDir},
		{"--user", "1234:5678"},
		{"--network", "none"},
		{"--cap-drop", "ALL"},
		{"--security-opt", "no-new-privileges"},
		{"--memory", "256m"},
		{"--cpus", "0.5"},
		{"--pids-limit", "64"},
		{"--log-driver", "json-file"},
		{"--log-opt", "max-size=1p"},
		{"--log-opt", "mode=blocking"},
	} {
		if !pair(args, p[0], p[1]) {
			t.Errorf("create args lack %s %s: %q", p[0], p[1], args)
		}
	}
	if args[0] != "create" || !slices.Contains(args, "--init") {
		t.Errorf("create args = %q", args)
	}
	if tail := args[len(args)-5:]; !slices.Equal(tail, []string{"alpine:3.23", "-c", wrapper, "canti", "exec 2>&1\necho hi"}) {
		t.Errorf("create args end with %q", tail)
	}
	if tail := run[len(run)-3:]; !slices.Equal(tail, []string{"alpine:3.23", "-c", "exec 2>&1\necho hi"}) {
		t.Errorf("the hashed spec ends with %q, want the shape of earlier revisions", tail)
	}
	blanked := map[string]bool{"TINI_KILL_PROCESS_GROUP=1": true}
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "FTP_PROXY", "ALL_PROXY"} {
		blanked[k+"="], blanked[strings.ToLower(k)+"="] = true, true
	}
	for i, a := range args {
		switch {
		case a == "-e" && blanked[args[i+1]]:
			delete(blanked, args[i+1])
		case a == "-e", strings.HasPrefix(a, "--env"), a == "--privileged", strings.HasPrefix(a, "--cap-add"), a == "--rm", strings.HasPrefix(a, "--restart"):
			t.Errorf("create args pass %q: %q", a, args)
		}
	}
	if len(blanked) != 0 {
		t.Errorf("create args let the docker CLI inject %v", blanked)
	}
	for i, a := range args {
		if a == "-v" && strings.HasPrefix(args[i+1], x.cfg.State) && !strings.HasSuffix(args[i+1], ":ro") {
			t.Errorf("state is mounted writable: %q", args[i+1])
		}
	}
}

func TestSpecHashCoversTheExecutionSpec(t *testing.T) {
	ws, state, records := t.TempDir(), t.TempDir(), t.TempDir()
	base := Config{Image: "alpine:3.23", Workspace: ws, State: state}
	hash := func(cfg Config, command string) string {
		return specHash(newTestExecutor(t, cfg).runArgs(command))
	}
	want := hash(base, "echo hi")
	if again := hash(base, "echo hi"); again != want {
		t.Fatalf("hash is not deterministic: %s != %s", again, want)
	}
	policy := base
	policy.StopGrace, policy.OutputLimit, policy.PreviewLimit, policy.Docker = time.Second, 1, 64, "/usr/bin/docker"
	if got := hash(policy, "echo hi"); got != want {
		t.Fatalf("output and stop policy changed the spec hash")
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

func TestCommandsLeaveTheCallersProcessGroup(t *testing.T) {
	cmd := newTestExecutor(t, Config{}).command(t.Context(), "version")
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatal("the docker CLI shares the caller's process group, so a terminal interrupt can abort a launch")
	}
}
