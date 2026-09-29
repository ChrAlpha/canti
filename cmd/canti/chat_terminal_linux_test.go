package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"golang.org/x/sys/unix"
)

func openPTY(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if err := unix.IoctlSetPointerInt(int(m.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	n, err := unix.IoctlGetInt(int(m.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	s, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return m, s
}

func TestChatTerminalDiscardsBusyInput(t *testing.T) {
	for _, tc := range []struct{ interrupted, canonical bool }{{false, true}, {true, true}, {false, false}, {true, false}} {
		t.Run(fmt.Sprintf("interrupt=%v/canonical=%v", tc.interrupted, tc.canonical), func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
			entered, release := make(chan struct{}, 4), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(body))
				entered <- struct{}{}
				select {
				case <-release:
					f.model.ServeHTTP(w, r)
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			defer unblock()
			master, slave := openPTY(t)
			if !tc.canonical {
				term, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
				if err != nil {
					t.Fatal(err)
				}
				term.Lflag &^= unix.ICANON
				term.Cc[unix.VMIN], term.Cc[unix.VTIME] = 1, 0
				if err := unix.IoctlSetTermios(int(slave.Fd()), unix.TCSETS, term); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, exe, "chat", "-workspace", f.workspace, "-state", f.state, "-model-url", server.URL)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "CANTI_TEST_MAIN=1", "GORACE=atexit_sleep_ms=0", "ANTHROPIC_API_KEY=" + f.key}
			cmd.Stdin, cmd.Stdout = slave, io.Discard
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
			stderr, err := cmd.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { cmd.Process.Kill(); cmd.Wait() }()
			prompts := make(chan struct{}, 16)
			go func() {
				var b [1]byte
				var prev byte
				for {
					if _, err := stderr.Read(b[:]); err != nil {
						return
					}
					if prev == '>' && b[0] == ' ' {
						prompts <- struct{}{}
					}
					prev = b[0]
				}
			}()
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			await(prompts)
			// One write also covers data read ahead alongside the accepted line.
			io.WriteString(master, "first\nread ahead\n")
			await(entered)
			io.WriteString(master, "busy complete\nbusy partial")
			if tc.interrupted {
				if _, err := io.WriteString(master, "\x03"); err != nil {
					t.Fatal(err)
				}
			} else {
				unblock()
			}
			await(prompts)
			select {
			case <-entered:
				t.Fatal("input typed while busy started another model request")
			case <-time.After(150 * time.Millisecond):
			}
			if s := f.status(); s.Run.SessionSeq != 1 {
				t.Fatalf("busy input created a run: %+v", s.Run)
			}
			if tc.interrupted {
				unblock()
			}
			io.WriteString(master, "fresh\n")
			await(entered)
			await(prompts)
			if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "fresh" || s.Phase != core.PhaseReturned {
				t.Fatalf("fresh input did not advance cleanly: %+v", s)
			}
			if tc.canonical {
				io.WriteString(master, "\x04")
			} else {
				cmd.Process.Signal(syscall.SIGTERM)
			}
			if err := cmd.Wait(); err != nil && tc.canonical {
				t.Fatal(err)
			}
		})
	}
}

func TestChatTerminalReopensPromptAfterStderrBackpressure(t *testing.T) {
	script := map[int]core.Decision{41: {Kind: core.Return, Payload: "answer"}}
	for step := 1; step <= 40; step++ {
		script[step] = core.Decision{Kind: core.Exec, Payload: `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"look"}`}
	}
	f := newFixture(t, script)
	master, slave := openPTY(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "chat", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-max-steps", "45")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "CANTI_TEST_MAIN=1", "GORACE=atexit_sleep_ms=0", "ANTHROPIC_API_KEY=" + f.key}
	cmd.Stdin = slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	deadline, _ := ctx.Deadline()
	if err := r.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = w
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	waitPrompt := func() {
		t.Helper()
		var prev byte
		var b [1]byte
		for {
			if _, err := r.Read(b[:]); err != nil {
				t.Fatal(err)
			}
			if prev == '>' && b[0] == ' ' {
				return
			}
			prev = b[0]
		}
	}
	waitPrompt()
	fillPipe(t, w)
	w.Close()
	if _, err := io.WriteString(master, "first\n"); err != nil {
		t.Fatal(err)
	}
	answer, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || answer != "answer\n" {
		t.Fatalf("answer = %q, %v", answer, err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := io.WriteString(master, "before reopened prompt\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if s := f.status(); s.Run.SessionSeq != 1 || len(f.model.requests()) != 41 {
		t.Fatalf("input admitted before reopening prompt: %+v", s)
	}
	waitPrompt()
	time.Sleep(100 * time.Millisecond)
	if s := f.status(); s.Run.SessionSeq != 1 || len(f.model.requests()) != 41 {
		t.Fatalf("busy input survived display recovery: %+v", s)
	}
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"fresh answer"}`})
	if _, err := io.WriteString(master, "fresh\n"); err != nil {
		t.Fatal(err)
	}
	waitPrompt()
	if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "fresh" || s.Phase != core.PhaseReturned {
		t.Fatalf("fresh input: %+v", s)
	}
	io.WriteString(master, "\x04")
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}
