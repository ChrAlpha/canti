//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/charmbracelet/x/ansi"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

type ptyChat struct {
	cmd     *exec.Cmd
	master  *os.File
	slave   *os.File
	slaveFD int
	base    *unix.Termios
	mu      sync.Mutex
	output  bytes.Buffer
	wake    chan struct{}
	done    chan struct{}
	waited  bool
}

func startPTYChat(t *testing.T, f *fixture, stderr *os.File, args ...string) *ptyChat {
	return startPTYChatWithEnv(t, f, stderr, nil, args...)
}

func startPTYChatWithEnv(t *testing.T, f *fixture, stderr *os.File, env []string, args ...string) *ptyChat {
	return startPTYChatWithMode(t, f, stderr, env, true, args...)
}

func startPTYChatWithMode(t *testing.T, f *fixture, stderr *os.File, env []string, canonical bool, args ...string) *ptyChat {
	return startPTYChatWithDeadline(t, f, stderr, env, canonical, 30*time.Second, args...)
}

func startPTYChatWithDeadline(t *testing.T, f *fixture, stderr *os.File, env []string, canonical bool, deadline time.Duration, args ...string) *ptyChat {
	t.Helper()
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 100}); err != nil {
		t.Fatal(err)
	}
	slaveFD := int(slave.Fd())
	base, err := unix.IoctlGetTermios(slaveFD, unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	if !canonical {
		base.Lflag &^= unix.ICANON
		base.Cc[unix.VMIN], base.Cc[unix.VTIME] = 1, 0
		if err := unix.IoctlSetTermios(slaveFD, unix.TCSETS, base); err != nil {
			t.Fatal(err)
		}
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	argv := append([]string{"chat", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL, "-session", "pty"}, args...)
	cmd := exec.CommandContext(ctx, exe, argv...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "ANTHROPIC_") && !strings.HasPrefix(kv, "CANTI_TEST_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "CANTI_TEST_MAIN=1", "ANTHROPIC_API_KEY="+f.key, "GORACE=atexit_sleep_ms=0")
	cmd.Env = append(cmd.Env, env...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	if stderr != nil {
		cmd.Stderr = stderr
	}
	p := &ptyChat{cmd: cmd, master: master, slave: slave, slaveFD: slaveFD, base: base, wake: make(chan struct{}, 1), done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		cancel()
		master.Close()
		slave.Close()
		t.Fatal(err)
	}
	go func() {
		defer close(p.done)
		buf := make([]byte, 4096)
		for {
			n, err := master.Read(buf)
			if n > 0 {
				p.mu.Lock()
				p.output.Write(buf[:n])
				p.mu.Unlock()
				select {
				case p.wake <- struct{}{}:
				default:
				}
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			if !p.waited {
				waited := make(chan struct{})
				go func() { _ = cmd.Wait(); close(waited) }()
				select {
				case <-waited:
				case <-time.After(2 * time.Second):
					t.Error("chat process did not reap after kill")
				}
			}
		}
		master.Close()
		slave.Close()
		<-p.done
	})
	return p
}

func (p *ptyChat) text() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.String()
}

func (p *ptyChat) await(t *testing.T, needle string, after int) int {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		output := ansi.Strip(p.text())
		if after <= len(output) {
			if i := strings.Index(output[after:], needle); i >= 0 {
				return after + i + len(needle)
			}
		}
		select {
		case <-p.wake:
		case <-deadline.C:
			t.Fatalf("terminal never showed %q after byte %d; tail: %q", needle, after, output[max(0, len(output)-500):])
		}
	}
}

func (p *ptyChat) send(t *testing.T, input string) {
	t.Helper()
	if _, err := io.WriteString(p.master, input); err != nil {
		t.Fatal(err)
	}
}

func (p *ptyChat) awaitRaw(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		flags, err := unix.IoctlGetTermios(p.slaveFD, unix.TCGETS)
		if err != nil {
			t.Fatal(err)
		}
		if flags.Lflag&unix.ICANON == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("chat did not put the PTY into raw mode")
}

func (p *ptyChat) finish(t *testing.T, want int) {
	t.Helper()
	p.waited = true
	ended := make(chan error, 1)
	go func() { ended <- p.cmd.Wait() }()
	select {
	case err := <-ended:
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Fatal(err)
		}
		if p.cmd.ProcessState.ExitCode() != want {
			t.Fatalf("chat exit %d, want %d; output tail: %q", p.cmd.ProcessState.ExitCode(), want, p.text()[max(0, len(p.text())-500):])
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		select {
		case <-ended:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("chat did not exit within five seconds")
	}
	got, err := unix.IoctlGetTermios(p.slaveFD, unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	if got.Lflag != p.base.Lflag || got.Iflag != p.base.Iflag || got.Oflag != p.base.Oflag || got.Cflag != p.base.Cflag {
		t.Fatalf("terminal flags not restored: before=%+v after=%+v", p.base, got)
	}
}

func TestChatPTYBusyInputDoesNotBecomeNextRun(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "returned"
		if interrupted {
			name = "interrupted"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
			f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second answer"}`})
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
			var once sync.Once
			served := make(chan struct{})
			gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(body))
				first := false
				once.Do(func() {
					first = true
					entered <- struct{}{}
					select {
					case <-release:
					case <-r.Context().Done():
					}
				})
				f.model.ServeHTTP(w, r)
				if first {
					close(served)
				}
			}))
			t.Cleanup(func() { releaseGate(); gate.Close() })
			p := startPTYChat(t, f, nil, "-model-url", gate.URL)
			pos := p.await(t, "ready", 0)
			p.send(t, "first request\r")
			pos = p.await(t, "running", pos)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the model did not receive the first run")
			}
			p.send(t, "discard this\r")
			if interrupted {
				p.send(t, "\x03")
				deadline := time.Now().Add(5 * time.Second)
				for f.status().Phase != core.PhaseStopped {
					if time.Now().After(deadline) {
						t.Fatal("Ctrl-C did not stop the active run")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			releaseGate()
			pos = p.await(t, "ready", pos)
			if s := f.status(); s.Run.SessionSeq != 1 || s.Run.Goal != "first request" {
				t.Fatalf("busy input became a run: %+v", s.Run)
			}
			time.Sleep(200 * time.Millisecond)
			if s := f.status(); s.Run.SessionSeq != 1 {
				t.Fatalf("buffered busy input became a run: %+v", s.Run)
			}
			select {
			case <-served:
			case <-time.After(5 * time.Second):
				t.Fatal("the model did not answer the first run")
			}
			p.send(t, "second request\r")
			p.await(t, "second answer", pos)
			if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "second request" || s.Phase != core.PhaseReturned {
				t.Fatalf("explicit input did not finish: %+v", s)
			}
			prompts := f.model.prompts(1)
			if len(prompts) != 2 || !strings.Contains(prompts[1], "first request") || !strings.Contains(prompts[1], "second request") || strings.Contains(prompts[1], "discard this") {
				t.Fatalf("wrong session history: %q", prompts)
			}
			if interrupted && !strings.Contains(prompts[1], "stopped, reason cancelled") {
				t.Fatalf("stopped run missing from context: %q", prompts[1])
			}
			p.send(t, "\x03")
			p.finish(t, exitOK)
		})
	}
}

func TestChatPTYStderrBackpressureCancelsActiveRun(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "unused"}})
	held := f.model.holdRequests()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD := int(w.Fd())
	p := startPTYChat(t, f, w)
	p.awaitRaw(t)
	p.send(t, "busy request\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the model did not receive the active run")
	}
	flags, err := unix.FcntlInt(uintptr(writeFD), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_NONBLOCK == 0 {
		t.Fatalf("chat output is not nonblocking: flags=%x, error=%v", flags, err)
	}
	fillLivePipe(t, writeFD)
	p.send(t, "\x03")
	p.finish(t, exitError)
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled || s.Run.SessionSeq != 1 {
		t.Fatalf("output failure left an active or repeated run: %+v", s)
	}
}

func fillLivePipe(t *testing.T, fd int) {
	t.Helper()
	for _, chunk := range [][]byte{make([]byte, 4096), {0}} {
		for {
			if _, err := unix.Write(fd, chunk); errors.Is(err, unix.EAGAIN) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestChatPTYResolutionGuidanceSurvivesAltScreen(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: "true"}})
	t.Setenv("PATH", t.TempDir())
	p := startPTYChat(t, f, nil)
	p.await(t, "ready", 0)
	p.send(t, "needs inspection\r")
	p.finish(t, exitResolution)
	s := f.status()
	if s.Phase != core.PhaseNeedsResolution || s.Run.SessionSeq != 1 {
		t.Fatalf("missing durable resolution state: %+v", s)
	}
	out := p.text()
	altEnd := strings.LastIndex(out, "\x1b[?1049l")
	if altEnd < 0 || !strings.Contains(out[altEnd:], "canti resolve -run "+s.Run.ID) || !strings.Contains(out[altEnd:], "canti chat -session pty") {
		t.Fatalf("resolution and session guidance absent after alt-screen exit: alt-end=%d, output tail=%q", altEnd, out[max(0, len(out)-1200):])
	}
}

func TestChatPTYSecondInterruptExitsBlockedCleanupWithFullStderr(t *testing.T) {
	f := newFixture(t, nil)
	held := f.model.holdRequests()
	marker := filepath.Join(t.TempDir(), "stopping")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD := int(w.Fd())
	p := startPTYChatWithEnv(t, f, w, []string{"CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED=" + marker})
	p.awaitRaw(t)
	p.send(t, "request needing cleanup\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	flags, err := unix.FcntlInt(uintptr(writeFD), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_NONBLOCK == 0 {
		t.Fatalf("chat output is not nonblocking: flags=%x, error=%v", flags, err)
	}
	fillLivePipe(t, writeFD)
	p.send(t, "\x03")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first Ctrl-C never entered blocked stop commit")
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.send(t, "\x03")
	p.finish(t, exitError)
	s := f.status()
	if s.Run.SessionSeq != 1 || s.Run.Goal != "request needing cleanup" || s.Phase == core.PhaseStopped {
		t.Fatalf("blocked cleanup unexpectedly committed or started another run: %+v", s)
	}
	if result := f.cli(nil, "stop", "-run", s.Run.ID); result.code != exitStopped {
		t.Fatalf("retrying stop after abandoned cleanup: %+v", result)
	}
	if final := f.status(); final.Phase != core.PhaseStopped || final.Reason != core.StopCancelled || final.Run.SessionSeq != 1 {
		t.Fatalf("manual stop did not finish original run: %+v", final)
	}
}
