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
// arguments in earlier revisions.
func TestCreatedContainerIsTheOneTheDockerCLICreates(t *testing.T) {
	x, err := New(Config{Image: cmp.Or(os.Getenv("CANTI_TEST_IMAGE"), "alpine:3.23"), Workspace: t.TempDir(), State: t.TempDir(),
		Memory: "256m", CPUs: "0.5", PidsLimit: 64, Mounts: []Mount{{Source: t.TempDir(), Target: "/canti/runs"}}})
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("eq%d", time.Now().UnixNano())
	e := harness.Effect{ID: id, Command: "echo hi"}
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", "canti-"+id, "canti-"+id+"-cli").Run() })
	run := x.runArgs(e.Command)
	if err := x.create(t.Context(), e, specHash(run)); err != nil {
		t.Fatal(err)
	}
	n := len(run) - 1
	args := append([]string{"create", "--name", "canti-" + id + "-cli", "--label", "canti.effect=" + id, "--label", "canti.state=" + x.cfg.State,
		"--label", "canti.spec=" + specHash(run), "--label", exitLabel + "=" + exitFile, "-e", "TINI_KILL_PROCESS_GROUP=1"}, run[:n]...)
	if out, err := exec.Command("docker", append(args, wrapper, "canti", run[n])...).CombinedOutput(); err != nil {
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
