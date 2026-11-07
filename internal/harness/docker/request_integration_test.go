//go:build integration

package docker

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// TestCreatedContainerIsTheOneTheDockerCLICreates compares the container
// that Bind creates with the one that "docker create" created from the same
// arguments in earlier revisions, also for the platform that
// DOCKER_DEFAULT_PLATFORM names.
func TestCreatedContainerIsTheOneTheDockerCLICreates(t *testing.T) {
	daemon, err := exec.Command("docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}").Output()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("default platform", func(t *testing.T) { compareCreated(t) })
	t.Run("DOCKER_DEFAULT_PLATFORM", func(t *testing.T) {
		t.Setenv("DOCKER_DEFAULT_PLATFORM", strings.TrimSpace(string(daemon)))
		compareCreated(t)
	})
}

func compareCreated(t *testing.T) {
	x, err := New(Config{Image: cmp.Or(os.Getenv("CANTI_TEST_IMAGE"), "alpine:3.23"), Workspace: t.TempDir(), State: t.TempDir(),
		Memory: "256m", CPUs: "0.5", PidsLimit: 64, Mounts: []Mount{{Source: t.TempDir(), Target: "/canti/runs"}}})
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("eq%d", time.Now().UnixNano())
	e := harness.Effect{ID: id, Command: "echo hi"}
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", "canti-"+id, "canti-"+id+"-cli").Run() })
	e.Spec = x.Specification(e.Command)
	if err := x.create(t.Context(), e, e.Spec); err != nil {
		t.Fatal(err)
	}
	args := []string{"create", "--name", "canti-" + id + "-cli", "--label", "canti.effect=" + id, "--label", "canti.state=" + x.cfg.State, "--label", "canti.spec=" + e.Spec, "--label", exitLabel + "=" + exitFile,
		"--network", x.cfg.Network, "--user", x.cfg.User, "--workdir", WorkDir, "--memory", "256m", "--cpus", "0.5", "--pids-limit", "64", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--init", "--log-driver", "json-file", "--log-opt", "max-size=1p", "--log-opt", "mode=blocking",
		"--volume", x.cfg.Workspace + ":" + WorkDir, "--volume", x.cfg.State + "/artifacts:/canti/artifacts:ro", "--volume", x.cfg.Mounts[0].Source + ":/canti/runs:ro", "--env", "TINI_KILL_PROCESS_GROUP=1"}
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "FTP_PROXY", "ALL_PROXY"} {
		args = append(args, "--env", key+"=", "--env", strings.ToLower(key)+"=")
	}
	args = append(args, "--entrypoint", "/bin/sh", x.cfg.Image, "-c", wrapper, "canti", "exec 2>&1\necho hi")
	cmd := exec.Command("docker", args...)
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "DOCKER_DEFAULT_PLATFORM=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker create: %v: %s", err, out)
	}

	api, cli := created(t, "canti-"+id), created(t, "canti-"+id+"-cli")
	if !bytes.Equal(api, cli) {
		t.Fatalf("container created through the API:\n%s\nthrough the docker CLI:\n%s", api, cli)
	}
}

// created is the inspection of a container without what identifies that
// one container. The docker CLI orders the environment and the binds as a
// map does, and sends an empty list where none is set.
func created(t *testing.T, name string) []byte {
	t.Helper()
	out, err := exec.Command("docker", "container", "inspect", name).Output()
	if err != nil {
		t.Fatal(err)
	}
	var c []map[string]any
	if err := json.Unmarshal(out, &c); err != nil || len(c) != 1 {
		t.Fatalf("docker inspect %s: %v", name, err)
	}
	for _, k := range []string{"Id", "Created", "Name", "ResolvConfPath", "HostnamePath", "HostsPath", "LogPath", "GraphDriver", "NetworkSettings"} {
		delete(c[0], k)
	}
	config, host := c[0]["Config"].(map[string]any), c[0]["HostConfig"].(map[string]any)
	delete(config, "Hostname")
	// Docker versions encode an empty port-binding map as null or {}.
	if ports, ok := host["PortBindings"].(map[string]any); ok && len(ports) == 0 {
		host["PortBindings"] = nil
	}
	for _, list := range []any{config["Env"], host["Binds"], c[0]["Mounts"]} {
		l, _ := list.([]any)
		slices.SortFunc(l, func(a, b any) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	}
	b, err := json.MarshalIndent(unlisted(c[0]), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.ReplaceAll(b, []byte(strings.TrimPrefix(name, "canti-")+"-cli"), []byte(strings.TrimPrefix(name, "canti-")))
}

func unlisted(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for k, e := range v {
			v[k] = unlisted(e)
		}
	case []any:
		if len(v) == 0 {
			return nil
		}
		for i, e := range v {
			v[i] = unlisted(e)
		}
	}
	return v
}
