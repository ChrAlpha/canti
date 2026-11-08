//go:build acceptance

package acceptance

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func holdWriteLock(t *testing.T, f *fixture) func() {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "locked")
	cmd := exec.Command(binary(t, "CANTI_ACCEPTANCE_RUNTIME_FIXTURE"), "fixture-write-lock", f.state, marker)
	cmd.Env = append(os.Environ(), "CANTI_TEST_MAIN=1")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			input.Close()
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				cmd.Process.Kill()
				<-done
				t.Error("write-lock fixture did not exit")
			}
		})
	}
	t.Cleanup(release)
	waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
	return release
}
