package docker

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
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
	state := filepath.Join(root, "state")
	for _, tt := range []struct {
		name string
		cfg  Config
	}{
		{"no image", Config{Workspace: ws, State: state}},
		{"relative workspace", Config{Image: "i", Workspace: "ws", State: state}},
		{"relative state", Config{Image: "i", Workspace: ws, State: "state"}},
		{"missing workspace", Config{Image: "i", Workspace: filepath.Join(root, "none"), State: state}},
		{"workspace file", Config{Image: "i", Workspace: file, State: state}},
		{"state in workspace", Config{Image: "i", Workspace: ws, State: filepath.Join(ws, "state")}},
		{"state in linked workspace", Config{Image: "i", Workspace: ws, State: filepath.Join(link, "state")}},
		{"workspace in state", Config{Image: "i", Workspace: ws, State: root}},
		{"same dir", Config{Image: "i", Workspace: ws, State: link}},
		{"colon in path", Config{Image: "i", Workspace: ws, State: filepath.Join(root, "a:b")}},
		{"negative output limit", Config{Image: "i", Workspace: ws, State: state, OutputLimit: -1}},
		{"tiny preview", Config{Image: "i", Workspace: ws, State: state, PreviewLimit: 63}},
		{"negative grace", Config{Image: "i", Workspace: ws, State: state, StopGrace: -time.Second}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Fatalf("New(%+v) succeeded", tt.cfg)
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
	x := newTestExecutor(t, Config{User: "1234:5678", Memory: "256m", CPUs: "0.5", PidsLimit: 64})
	run := x.runArgs("echo hi")
	args := x.createArgs("effect_1", run)
	for _, p := range [][2]string{
		{"--name", "canti-effect_1"},
		{"--label", "canti.effect=effect_1"},
		{"--label", "canti.state=" + x.cfg.State},
		{"--label", "canti.spec=" + specHash(run)},
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
		{"-v", x.cfg.Workspace + ":" + WorkDir},
		{"-v", filepath.Join(x.cfg.State, "artifacts") + ":" + ArtifactDir + ":ro"},
	} {
		if !pair(args, p[0], p[1]) {
			t.Errorf("create args lack %s %s: %q", p[0], p[1], args)
		}
	}
	if args[0] != "create" || !slices.Contains(args, "--init") {
		t.Errorf("create args = %q", args)
	}
	if tail := args[len(args)-3:]; !slices.Equal(tail, []string{"alpine:3.23", "-c", "exec 2>&1\necho hi"}) {
		t.Errorf("create args end with %q", tail)
	}
	for _, a := range args {
		if a == "-e" || strings.HasPrefix(a, "--env") || a == "--privileged" || strings.HasPrefix(a, "--cap-add") {
			t.Errorf("create args pass %q: %q", a, args)
		}
	}
	for i, a := range args {
		if a == "-v" && strings.HasPrefix(args[i+1], x.cfg.State) && !strings.HasSuffix(args[i+1], ":ro") {
			t.Errorf("state is mounted writable: %q", args[i+1])
		}
	}
}

func TestSpecHashCoversTheExecutionSpec(t *testing.T) {
	ws, state := t.TempDir(), t.TempDir()
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
