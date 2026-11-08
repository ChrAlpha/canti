//go:build acceptance

package acceptance

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const tuiCommand = "canti-tui"
const (
	exitOK         = 0
	exitError      = 1
	exitUsage      = 2
	exitStopped    = 3
	exitResolution = 4
	exitActive     = 5
	exitLocked     = 6
	stopCleanup    = 30 * time.Second
)

type decision struct {
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

func binary(t *testing.T, name string) string {
	t.Helper()
	path := os.Getenv(name)
	if !filepath.IsAbs(path) {
		t.Fatalf("root acceptance orchestration must supply absolute %s", name)
	}
	return path
}
func executable(t *testing.T, env []string, args []string) (string, []string) {
	t.Helper()
	runtimeName := "CANTI_ACCEPTANCE_RUNTIME"
	for _, v := range env {
		if strings.HasPrefix(v, "CANTI_TEST_") {
			runtimeName = "CANTI_ACCEPTANCE_RUNTIME_FIXTURE"
		}
	}
	if len(args) > 0 && args[0] == tuiCommand {
		return binary(t, "CANTI_ACCEPTANCE_TUI"), append([]string{"-runtime", binary(t, runtimeName)}, pinHost(args)[1:]...)
	}
	return binary(t, runtimeName), pinHost(args)
}
func locked(state string) bool {
	f, err := os.Open(filepath.Join(state, "lock"))
	if err != nil {
		return false
	}
	defer f.Close()
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}
func slicesContain(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
func waitFor(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (f *fixture) awaitReturned() {
	f.t.Helper()
	waitFor(f.t, func() bool {
		r := f.cli(nil, "status")
		return r.code == exitOK && strings.Contains(r.stdout, `"phase":"returned"`)
	})
}

func cliCommand(t *testing.T, env []string, args ...string) *exec.Cmd {
	t.Helper()
	exe, args := executable(t, env, args)
	cmd := exec.Command(exe, pinHost(args)...)
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
	return cmd
}
