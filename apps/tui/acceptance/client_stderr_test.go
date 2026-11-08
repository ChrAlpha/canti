//go:build acceptance

package acceptance

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestTUIRefusesUnboundedOutputBeforeStateWithBlockedStderr(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can reopen regardless of mode")
	}
	f := newFixture(t, nil)
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outR.Close()
	defer outW.Close()
	if err = unix.Fchmod(int(outW.Fd()), 0); err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer errR.Close()
	defer errW.Close()
	fillPipe(t, errW)
	cmd := cliCommand(t, []string{"ANTHROPIC_API_KEY=" + f.key}, tuiCommand, "-workspace", f.workspace, "-state", f.state)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = strings.NewReader("never\n"), outW, errW
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("setup error blocked on stderr")
	}
	if cmd.ProcessState.ExitCode() != exitError {
		t.Fatal(cmd.ProcessState)
	}
	if _, err = os.Stat(f.state); !os.IsNotExist(err) {
		t.Fatalf("setup created state: %v", err)
	}
}

func TestChatPipeOutputLossCancelsPendingStep(t *testing.T) {
	f := newFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"look"}`},
		2: {Kind: "return", Payload: "never"},
	})
	marker := filepath.Join(t.TempDir(), "performed")
	// Keep an accepted step pending independently of machine speed. Its
	// decided progress must reach the full pipe; no user signal is sent.
	_, _, wait := startChatWithStderr(t, f, []string{
		"ANTHROPIC_API_KEY=" + f.key,
		"CANTI_TEST_CRASH=block:performed", "CANTI_TEST_BLOCKED=" + marker,
	}, true)
	r := wait()
	s := f.status()
	h := f.history()
	t.Logf("no signal: exit=%d phase=%s reason=%s seq=%d history=%v", r.code, s.Phase, s.Reason, s.Seq, kinds(h))
	if r.code != exitError || r.stdout != "" || s.Phase != "stopped" || s.Reason != "cancelled" || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("output loss did not cancel the only admitted input: result=%+v status=%+v", r, s)
	}
	observations := 0
	for _, e := range h {
		if e.Kind == "performed" {
			t.Fatal("pending action was completed after output loss")
		}
		if e.Facts.Observation != nil {
			observations++
			if e.Facts.Observation.Execution != "cancelled" {
				t.Fatalf("unexpected observation: %+v", e.Facts.Observation)
			}
		}
	}
	if observations != 1 || locked(f.state) || len(f.hostProcesses()) != 0 {
		t.Fatalf("incomplete owned cleanup: observations=%d locked=%v hosts=%v", observations, locked(f.state), f.hostProcesses())
	}
}

func startChatWithStderr(t *testing.T, f *fixture, env []string, blockBeforeSubmit bool, args ...string) (*exec.Cmd, func(), func() result) {
	t.Helper()

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
	argv := append([]string{tuiCommand, "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL}, fixtureModel...)
	exe, argv := executable(t, env, append(argv, args...))
	cmd := exec.CommandContext(ctx, exe, argv...)
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
	blockOutput := func() {
		t.Helper()
		fillPipe(t, w)
		w.Close()
	}
	if blockBeforeSubmit {
		blockOutput()
	}
	if _, err := io.WriteString(input, "work\nnever\n"); err != nil {
		t.Fatal(err)
	}
	input.Close()
	return cmd, blockOutput, func() result {
		t.Helper()
		var exit *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exit) {
			t.Fatal(err)
		}
		return result{code: cmd.ProcessState.ExitCode(), stdout: stdout.String()}
	}
}
