//go:build integration

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
)

func TestChatStopsDockerWithBlockedStderr(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(fmt.Sprintf("abandon=%v", abandon), func(t *testing.T) {
			f, image := recoveryFixture(t, map[int]core.Decision{
				1: {Kind: core.Exec, Payload: "echo started >> effects; sleep 60; echo finished >> effects"},
				2: {Kind: core.Return, Payload: "done"},
			})
			env := []string{"ANTHROPIC_API_KEY=" + f.key}
			blocked := filepath.Join(t.TempDir(), "cleanup")
			if abandon {
				env = append(env, "CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED="+blocked)
			}
			cmd, wait := startChatWithBlockedStderr(t, f, image, env)
			waitFor(t, func() bool { _, err := os.Stat(filepath.Join(f.workspace, "effects")); return err == nil })
			started := f.status()
			if started.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("unexpected execution before interrupt: %+v", started)
			}
			began := time.Now()
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if abandon {
				waitFor(t, func() bool { _, err := os.Stat(blocked); return err == nil })
				if err := cmd.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
			}
			r := wait()
			if elapsed := time.Since(began); elapsed > stopCleanup {
				t.Fatalf("stopping with unread stderr took %s", elapsed)
			}
			want := exitStopped
			if abandon {
				want = exitError
			}
			if r.code != want || r.stdout != "" {
				t.Fatalf("interrupted chat: %+v", r)
			}
			if s := f.status(); s.Run.ID != started.Run.ID || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("queued input was executed: %+v", s)
			}
			if !abandon {
				if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled {
					t.Fatalf("first signal did not finish cleanup: %+v", s)
				}
			}
			// Reopening proves the workspace lock was released, even after abandonment.
			if r := f.cli(nil, "stop"); r.code != exitStopped {
				t.Fatalf("finish cleanup: %+v", r)
			}
			if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled || s.Totals.Steps != 1 {
				t.Fatalf("stopped result: %+v", s)
			}
			if data, err := os.ReadFile(filepath.Join(f.workspace, "effects")); err != nil || string(data) != "started\n" {
				t.Fatalf("execution duplicated or continued: %q, %v", data, err)
			}
			h := f.history()
			observed := 0
			for _, e := range h {
				if e.Facts.Observation != nil {
					observed++
					if e.Facts.Observation.Execution != core.ExecutionCancelled {
						t.Fatalf("unexpected observation: %+v", e.Facts.Observation)
					}
				}
			}
			if observed != 1 {
				t.Fatalf("got %d observations, want one", observed)
			}
			checkReleased(t, f, h)
		})
	}
}

func startChatWithBlockedStderr(t *testing.T, f *fixture, image string, env []string) (*exec.Cmd, func() result) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	in, input, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { in.Close(); input.Close() })
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(); w.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	deadline, _ := ctx.Deadline()
	if err := r.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, pinHost(append([]string{"chat", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-image", image}, fixtureModel...))...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "CANTI_TEST_MAIN=1", "GORACE=atexit_sleep_ms=0")
	cmd.Env = append(cmd.Env, env...)
	var stdout bytes.Buffer
	cmd.Stdin, cmd.Stdout, cmd.Stderr = in, &stdout, w
	t.Cleanup(func() {
		cancel()
		if cmd.Process != nil && cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var prev byte
	var b [1]byte
	for {
		if _, err := r.Read(b[:]); err != nil {
			t.Fatal(err)
		}
		if prev == '>' && b[0] == ' ' {
			break
		}
		prev = b[0]
	}
	fillPipe(t, w)
	w.Close()
	if _, err := io.WriteString(input, "work\nnever\n"); err != nil {
		t.Fatal(err)
	}
	input.Close()
	return cmd, func() result {
		t.Helper()
		var exit *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exit) {
			t.Fatal(err)
		}
		return result{code: cmd.ProcessState.ExitCode(), stdout: stdout.String()}
	}
}
