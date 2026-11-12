package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestMain(m *testing.M) {
	mode := os.Getenv("CANTI_STARTUP_TEST_MODE")
	if mode != "" && len(os.Args) > 1 && (os.Args[1] == "preflight" || os.Args[1] == "host") {
		startupFixture(mode)
		os.Exit(0)
	}
	if os.Getenv("CANTI_STARTUP_TEST_CLI") == "1" {
		os.Exit(cli(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

func startupFixture(mode string) {
	marker := os.Getenv("CANTI_STARTUP_MARKER")
	mark := func() {
		if err := os.WriteFile(marker, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			panic(err)
		}
	}
	if os.Args[1] == "preflight" {
		if mode == "preflight" {
			signal.Ignore(os.Interrupt, syscall.SIGTERM)
			mark()
			select {}
		}
		var req hostrpc.Request
		if err := json.NewDecoder(os.Stdin).Decode(&req); err != nil {
			panic(err)
		}
		value := hostrpc.Validated{State: req.Validate.State, Spec: req.Validate.Submission.Spec, GoalLimit: 1024, ConstraintsLimit: 1024}
		if err := json.NewEncoder(os.Stdout).Encode(hostrpc.Response{Protocol: hostrpc.Protocol, Validated: &value}); err != nil {
			panic(err)
		}
		return
	}
	if mode == "host-startup" {
		signal.Ignore(os.Interrupt, syscall.SIGTERM)
		mark()
		select {}
	}
	term := make(chan os.Signal, 2)
	signal.Notify(term, syscall.SIGTERM)
	go func() {
		<-term
		mark()
		for range term {
		}
	}()
	var state string
	for i, arg := range os.Args {
		if arg == "-state" && i+1 < len(os.Args) {
			state = os.Args[i+1]
		}
	}
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		panic(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		var req hostrpc.Request
		if json.NewDecoder(conn).Decode(&req) == nil {
			json.NewEncoder(conn).Encode(hostrpc.Response{Protocol: hostrpc.Protocol, Hello: &hostrpc.Hello{Protocol: hostrpc.Protocol, PID: os.Getpid()}})
		}
		conn.Close()
	}
}

func TestStartupSignalsCancelPreflightAndEscalateOwnedChildren(t *testing.T) {
	for _, mode := range []string{"preflight", "host-startup", "owned-reaping"} {
		t.Run(mode, func(t *testing.T) {
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			workspace := filepath.Join(root, "workspace")
			state := filepath.Join(root, "state")
			marker := filepath.Join(root, "child")
			if err = os.Mkdir(workspace, 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-ephemeral", "-runtime", exe, "-workspace", workspace, "-state", state, "-host", "owned", "-host-wait", "20s")
			cmd.Env = append(runtimeEnvironment(), "CANTI_STARTUP_TEST_MODE="+mode, "CANTI_STARTUP_TEST_CLI=1", "CANTI_STARTUP_MARKER="+marker, "GORACE=atexit_sleep_ms=0")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			var pid int
			for deadline := time.Now().Add(2 * time.Second); pid == 0; {
				data, _ := os.ReadFile(marker)
				pid, _ = strconv.Atoi(string(data))
				if time.Now().After(deadline) {
					cmd.Process.Kill()
					<-done
					t.Fatalf("fixture did not reach %s: %s", mode, stderr.String())
				}
				if pid == 0 {
					time.Sleep(time.Millisecond)
				}
			}
			first := os.Signal(os.Interrupt)
			if mode == "owned-reaping" {
				first = syscall.SIGTERM
			}
			if err = cmd.Process.Signal(first); err != nil {
				t.Fatal(err)
			}
			if mode != "preflight" {
				select {
				case err := <-done:
					t.Fatalf("first signal abandoned owned cleanup: %v %s", err, stderr.String())
				case <-time.After(30 * time.Millisecond):
				}
				if err = syscall.Kill(pid, 0); err != nil {
					t.Fatalf("first signal killed the TERM-resistant child: %v", err)
				}
				if err = cmd.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-done:
				if cmd.ProcessState.ExitCode() != chat.Stopped {
					t.Fatalf("startup exit=%d stderr=%s", cmd.ProcessState.ExitCode(), stderr.String())
				}
			case <-time.After(2 * time.Second):
				cmd.Process.Kill()
				<-done
				t.Fatalf("startup signals stalled: %s", stderr.String())
			}
			if err = syscall.Kill(pid, 0); err != syscall.ESRCH {
				t.Fatalf("startup child %d was not reaped: %v", pid, err)
			}
			if mode == "preflight" {
				if _, err = os.Stat(state); !os.IsNotExist(err) {
					t.Fatalf("cancelled preflight created state: %v", err)
				}
			}
		})
	}
}

func TestChatRequiresExplicitExecutionModeAndRejectsProvisioningFlags(t *testing.T) {
	for _, args := range [][]string{nil, {"-environment", "/environment", "-ephemeral"}, {"-environment", "/environment", "-memory", "1g"}, {"-ephemeral", "-environment-workdir", "/workspace"}} {
		if _, code, err := parse(args, &bytes.Buffer{}); err == nil || code != chat.Usage {
			t.Fatalf("accepted %v: %d, %v", args, code, err)
		}
	}
	o, _, err := parse([]string{"-environment", "/environment", "-environment-workdir", "/workspace/project"}, &bytes.Buffer{})
	if err != nil || o.cfg.Spec.Environment != "/environment" || o.cfg.Spec.Image != "" || o.cfg.Spec.Pids != 0 {
		t.Fatalf("persistent flags = %+v, %v", o.cfg.Spec, err)
	}
}
