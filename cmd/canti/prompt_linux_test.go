package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"golang.org/x/sys/unix"
)

type fillOnAnswer struct {
	io.Writer
	t      *testing.T
	filled bool
}

func TestPromptFlushBarrierAndImmediateInput(t *testing.T) {
	master, slave := openPTY(t)
	r, out, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer out.Close()
	if err := r.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	w := newStderrWriter(out)
	defer w.Close()
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for round := range 20 {
		fillPipe(t, out)
		flags, err := unix.FcntlInt(out.Fd(), unix.F_GETFL, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		finished := make(chan error, 1)
		go func() { finished <- w.Prompt(int(slave.Fd())) }()
		select {
		case err := <-finished:
			t.Fatalf("prompt escaped a full pipe: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
		if _, err := io.WriteString(master, "stale\npartial"); err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		var prev byte
		for {
			if _, err := r.Read(b[:]); err != nil {
				t.Fatal(err)
			}
			if prev == '>' && b[0] == ' ' {
				break
			}
			prev = b[0]
		}
		fresh := "fresh-" + strconv.Itoa(round)
		if _, err := io.WriteString(master, fresh+"\n"); err != nil {
			t.Fatal(err)
		}
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if flagsAfter, err := unix.FcntlInt(out.Fd(), unix.F_GETFL, 0); err != nil || flagsAfter != flags {
			t.Fatalf("output flags changed: before=%x after=%x err=%v", flags, flagsAfter, err)
		}
		if got, err := readInput(bufio.NewReader(slave), io.Discard); err != nil || got != fresh {
			t.Fatalf("round %d input = %q, %v", round, got, err)
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil || len(after) != len(before) {
		t.Fatalf("descriptor count %d -> %d, %v", len(before), len(after), err)
	}
}

func TestPromptOutputBoundaries(t *testing.T) {
	_, slave := openPTY(t)
	t.Run("file offset", func(t *testing.T) {
		out, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		io.WriteString(out, "prefix")
		w := newStderrWriter(out)
		defer w.Close()
		if err := w.Prompt(int(slave.Fd())); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(out.Name())
		if err != nil || string(got) != "prefix> " {
			t.Fatalf("output = %q, %v", got, err)
		}
	})
	t.Run("closed reader", func(t *testing.T) {
		r, out, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		r.Close()
		defer out.Close()
		w := newStderrWriter(out)
		defer w.Close()
		if err := w.Prompt(int(slave.Fd())); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("prompt error = %v", err)
		}
	})
	t.Run("close blocked prompt", func(t *testing.T) {
		r, out, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		defer out.Close()
		fillPipe(t, out)
		w := newStderrWriter(out)
		defer w.Close()
		finished := make(chan error, 1)
		go func() { finished <- w.Prompt(int(slave.Fd())) }()
		time.Sleep(20 * time.Millisecond)
		w.Close()
		if err := <-finished; !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("prompt error = %v", err)
		}
		select {
		case <-w.done:
		case <-time.After(time.Second):
			t.Fatal("prompt writer did not release its descriptor")
		}
	})
	t.Run("socket", func(t *testing.T) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if err != nil {
			t.Fatal(err)
		}
		r, out := os.NewFile(uintptr(fds[0]), "reader"), os.NewFile(uintptr(fds[1]), "writer")
		defer r.Close()
		defer out.Close()
		w := newStderrWriter(out)
		defer w.Close()
		if err := w.Prompt(int(slave.Fd())); err != nil {
			t.Fatal(err)
		}
		var p [2]byte
		if _, err := io.ReadFull(r, p[:]); err != nil || string(p[:]) != "> " {
			t.Fatalf("prompt = %q, %v", p, err)
		}
	})
	for _, closed := range []bool{false, true} {
		t.Run("socket blocked/closed="+strconv.FormatBool(closed), func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
			if err != nil {
				t.Fatal(err)
			}
			r, out := os.NewFile(uintptr(fds[0]), "reader"), os.NewFile(uintptr(fds[1]), "writer")
			defer r.Close()
			defer out.Close()
			if err := r.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			fillPipe(t, out)
			w := newStderrWriter(out)
			defer w.Close()
			finished := make(chan error, 1)
			go func() { finished <- w.Prompt(int(slave.Fd())) }()
			select {
			case err := <-finished:
				t.Fatalf("prompt escaped full socket: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if closed {
				r.Close()
			} else {
				reader := bufio.NewReader(r)
				if _, err := reader.ReadString('>'); err != nil {
					t.Fatal(err)
				}
				if b, err := reader.ReadByte(); err != nil || b != ' ' {
					t.Fatalf("prompt suffix = %q, %v", b, err)
				}
			}
			select {
			case err := <-finished:
				if closed && err == nil || !closed && err != nil {
					t.Fatalf("closed=%v prompt error = %v", closed, err)
				}
			case <-time.After(time.Second):
				t.Fatal("socket prompt did not finish")
			}
		})
	}
}

func (w *fillOnAnswer) Write(p []byte) (int, error) {
	if !w.filled {
		fillPipe(w.t, os.Stderr)
		w.filled = true
	}
	return w.Writer.Write(p)
}

func TestChatPromptWriteBackpressure(t *testing.T) {
	if os.Getenv("CANTI_TEST_PROMPT_PIPE") == "1" {
		os.Exit(cli(context.Background(), os.Args[3:], &fillOnAnswer{Writer: os.Stdout, t: t}, os.Stderr))
	}
	for _, tc := range []struct {
		canonical bool
		signal    syscall.Signal
	}{{true, 0}, {false, 0}, {true, syscall.SIGINT}, {true, syscall.SIGTERM}} {
		t.Run("canonical="+strconv.FormatBool(tc.canonical)+"/signal="+strconv.Itoa(int(tc.signal)), func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
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
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestChatPromptWriteBackpressure$", "--", "chat", "-workspace", f.workspace, "-state", f.state, "-model-url", f.model.URL)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "CANTI_TEST_PROMPT_PIPE=1", "GORACE=atexit_sleep_ms=0", "ANTHROPIC_API_KEY=" + f.key}
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
			if _, err := io.WriteString(master, "first\n"); err != nil {
				t.Fatal(err)
			}
			if answer, err := bufio.NewReader(out).ReadString('\n'); err != nil || answer != "answer\n" {
				t.Fatalf("answer = %q, %v", answer, err)
			}
			// A return-only run emits no progress. Its stdout sink fills stderr
			// after the first prompt: the empty Flush succeeds, then '> ' waits.
			time.Sleep(100 * time.Millisecond)
			if tc.signal != 0 {
				start := time.Now()
				if err := cmd.Process.Signal(tc.signal); err != nil {
					t.Fatal(err)
				}
				if err := cmd.Wait(); err == nil || time.Since(start) > 2*time.Second {
					t.Fatalf("blocked prompt signal exit: %v after %s", err, time.Since(start))
				}
				if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
					t.Fatalf("signal changed completed run: %+v", s)
				}
				return
			}
			if _, err := io.WriteString(master, "before visible\npartial"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
			waitPrompt()
			if _, err := io.WriteString(master, "fresh\n"); err != nil {
				t.Fatal(err)
			}
			waitPrompt()
			if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "fresh" || s.Phase != core.PhaseReturned || len(f.model.requests()) != 2 {
				t.Fatalf("prompt handoff submitted stale input: state=%+v, requests=%d", s, len(f.model.requests()))
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
