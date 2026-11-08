//go:build acceptance && integration

package acceptance

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func dockerFixture(t *testing.T, script map[int]decision) (*fixture, string) {
	t.Helper()
	f := newFixture(t, script)
	t.Cleanup(func() { removeContainers(t, f.state) })
	return f, cmp.Or(os.Getenv("CANTI_TEST_IMAGE"), "alpine:3.23")
}
func removeContainers(t *testing.T, state string) {
	state, err := filepath.EvalSymlinks(state)
	if os.IsNotExist(err) {
		return
	}
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=canti.state="+state).Output()
	if err != nil {
		t.Errorf("list containers: %v", err)
		return
	}
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		if out, err := exec.Command("docker", append([]string{"rm", "-f"}, ids...)...).CombinedOutput(); err != nil {
			t.Errorf("remove containers: %v: %s", err, out)
		}
	}
}

func recoveryFixture(t *testing.T, script map[int]decision) (*fixture, string) {
	t.Helper()
	f, image := dockerFixture(t, script)
	t.Cleanup(func() {
		for _, c := range effectContainers(t, f.state) {
			if c.state == "running" {
				t.Errorf("container %s of effect %s is still running", c.id, c.effect)
			}
		}
	})
	return f, image
}

func effectContainers(t *testing.T, state string) []effectContainer {
	t.Helper()
	state, err := filepath.EvalSymlinks(state)
	if os.IsNotExist(err) {
		return nil
	}
	var out []byte
	if err == nil {
		out, err = exec.Command("docker", "ps", "-a", "--no-trunc", "--filter", "label=canti.state="+state,
			"--format", `{{.ID}} {{.Label "canti.effect"}} {{.State}}`).Output()
	}
	if err != nil {
		t.Errorf("list containers: %v", err)
		return nil
	}
	var cs []effectContainer
	for line := range strings.Lines(string(out)) {
		var c effectContainer
		if _, err := fmt.Sscan(line, &c.id, &c.effect, &c.state); err != nil {
			t.Errorf("container line %q: %v", line, err)
		}
		cs = append(cs, c)
	}
	return cs
}

type effectContainer struct{ id, effect, state string }

func checkReleased(t *testing.T, f *fixture, h []entry) {
	t.Helper()
	raw := make([]json.RawMessage, len(h))
	for i, e := range h {
		raw[i] = e.raw
	}
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary(t, "CANTI_ACCEPTANCE_RUNTIME_FIXTURE"), "-test.run=^TestAcceptanceReleasedResources$", "-test.v")
	cmd.Env = append(os.Environ(), "CANTI_TEST_MAIN=0", "CANTI_ACCEPTANCE_VERIFY_STATE="+f.state)
	cmd.Stdin = bytes.NewReader(data)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("runtime receipt verification: %v\n%s", err, output)
	}
}
