package session

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func testExecutable(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRuntimeExecutableSelectsConfiguredPathBeforePATH(t *testing.T) {
	dir := t.TempDir()
	fromPath := testExecutable(t, dir, "canti", "exit 0")
	configured := testExecutable(t, dir, "chosen-runtime", "exit 0")
	t.Setenv("PATH", dir)
	for _, tc := range []struct {
		name, configured, want string
	}{
		{"PATH", "", fromPath},
		{"configured", configured, configured},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &runtimeClient{runtimePath: tc.configured}
			got, err := a.runtimeExecutable()
			if err != nil || got != tc.want {
				t.Fatalf("runtimeExecutable = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestRuntimePreflightRejectsMissingWrongAndMismatchedExecutablesWithoutState(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		protocol         bool
	}{
		{"missing", "", "find canti runtime", false},
		{"wrong binary", "printf 'not-json'", "runtime preflight response", false},
		{"wrong protocol", "printf '{\"protocol\":4}\\n'", "install matching canti and canti-tui", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			state := filepath.Join(dir, "state")
			path := filepath.Join(dir, "missing-runtime")
			if tc.body != "" {
				path = testExecutable(t, dir, strings.ReplaceAll(tc.name, " ", "-"), tc.body)
			}
			a := &runtimeClient{state: state, runtimePath: path}
			_, err := a.validate(context.Background(), hostrpc.Submission{}, hostrpc.Binding{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate error = %v; want %q", err, tc.want)
			}
			if tc.protocol && !errors.Is(err, hostrpc.ErrProtocol) {
				t.Fatalf("validate error = %v; want ErrProtocol", err)
			}
			if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("state after failed preflight: %v", err)
			}
		})
	}
}

func TestRuntimePreflightBoundsStdoutAndStderrWithoutState(t *testing.T) {
	for _, tc := range []struct {
		name, body string
	}{
		{"stdout", "printf '%s' '{\"protocol\":5,\"validated\":{\"state\":\"ok\"}}'\n" + testLargeOutput("", " ")},
		{"stderr", testLargeOutput(">&2", "x") + "\nexit 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			state := filepath.Join(dir, "state")
			a := &runtimeClient{state: state, runtimePath: testExecutable(t, dir, "oversized-runtime", tc.body)}
			start := time.Now()
			_, err := a.validate(context.Background(), hostrpc.Submission{}, hostrpc.Binding{})
			if !errors.Is(err, errPreflightOutput) || time.Since(start) > 3*time.Second {
				t.Fatalf("oversized preflight output = %v after %v; want bounded refusal", err, time.Since(start))
			}
			if len(err.Error()) > 64<<10 {
				t.Fatalf("preflight error retained %d bytes of output", len(err.Error()))
			}
			if _, statErr := os.Stat(state); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("state after oversized preflight: %v", statErr)
			}
		})
	}
}

func testLargeOutput(target, content string) string {
	return "i=0\nwhile [ \"$i\" -lt 2048 ]; do printf '%s' '" + strings.Repeat(content, 1024) + "' " + target + "; i=$((i+1)); done"
}

func TestRuntimePreflightCancellationBoundsChildWait(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	a := &runtimeClient{state: state, runtimePath: testExecutable(t, dir, "slow-runtime", "sleep 3 & wait")}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := a.validate(ctx, hostrpc.Submission{}, hostrpc.Binding{})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("preflight cancellation = %v after %v", err, time.Since(start))
	}
	if _, statErr := os.Stat(state); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("state after cancelled preflight: %v", statErr)
	}
}

func TestRuntimePreflightRejectsAmbiguousAndTrailingResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
	}{
		{"trailing", fmt.Sprintf(`{"protocol":%d,"validated":{"state":"ok"}} {}`, hostrpc.Protocol)},
		{"unknown field", fmt.Sprintf(`{"protocol":%d,"validated":{"state":"ok"},"unexpected":true}`, hostrpc.Protocol)},
		{"success and error", fmt.Sprintf(`{"protocol":%d,"validated":{"state":"ok"},"error":{"code":"spec","message":"refused"}}`, hostrpc.Protocol)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeValidation([]byte(tc.body), hostrpc.Binding{}); err == nil || errors.Is(err, hostrpc.ErrProtocol) {
				t.Fatalf("malformed current-protocol response bypassed its structural check: %v", err)
			}
		})
	}
}

func TestOwnedHostRefusesAlreadyLockedState(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(state, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	a := &runtimeClient{state: state, hostMode: Owned, hostWait: time.Second}
	_, err = a.connect(context.Background())
	if !errors.Is(err, ErrLocked) || a.child != nil {
		t.Fatalf("owned connect = %v, child %v; want locked without launch", err, a.child)
	}
	if _, statErr := os.Stat(filepath.Join(state, "host.log")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("owned connect launched a runtime: %v", statErr)
	}
}

func TestOwnedHostRejectsAnotherServingHost(t *testing.T) {
	for _, tc := range []struct {
		name  string
		child bool
	}{
		{"already serving", false},
		{"startup race", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				var request hostrpc.Request
				if json.NewDecoder(conn).Decode(&request) == nil {
					_ = json.NewEncoder(conn).Encode(hostrpc.Response{Protocol: hostrpc.Protocol, Hello: &hostrpc.Hello{PID: os.Getpid()}})
				}
			}()
			a := &runtimeClient{state: state, hostMode: Owned, hostWait: time.Second}
			if tc.child {
				a.child = &exec.Cmd{Process: &os.Process{Pid: os.Getpid() + 1}}
			}
			_, err = a.connect(context.Background())
			if !errors.Is(err, ErrLocked) {
				t.Fatalf("owned connect to another host = %v, want locked", err)
			}
		})
	}
}

func TestOwnedRuntimeReapingObservesForceCancellation(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "trap '' TERM; echo ready; exec sleep 60")
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	if line, err := bufio.NewReader(output).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("child readiness: %q %v", line, err)
	}
	r := &runtimeClient{child: cmd, childDone: exited}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.closeHost(ctx); close(done) }()
	select {
	case <-done:
		t.Fatal("child unexpectedly accepted TERM")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
		if r.child != nil {
			t.Fatal("child handle not retired")
		}
	case <-time.After(time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("force cancellation did not escape reaping")
	}
}
