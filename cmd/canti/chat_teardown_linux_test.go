package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"golang.org/x/sys/unix"
)

type teardownTerminal struct {
	*os.File
	fd     uintptr
	broken atomic.Bool
	mu     sync.Mutex
	output string
}

func (w *teardownTerminal) Fd() uintptr {
	if w.broken.Load() {
		return ^uintptr(0)
	}
	return w.fd
}

func (w *teardownTerminal) WriteString(p string) (int, error) { return w.Write([]byte(p)) }

func (w *teardownTerminal) Write(p []byte) (int, error) {
	n, err := w.File.Write(p)
	w.mu.Lock()
	w.output += string(p[:n])
	w.mu.Unlock()
	return n, err
}

func (w *teardownTerminal) await(t *testing.T, needle string, after int) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		w.mu.Lock()
		output := ansi.Strip(w.output)
		w.mu.Unlock()
		if after <= len(output) {
			if i := strings.Index(output[after:], needle); i >= 0 {
				return after + i + len(needle)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("terminal never showed %q after byte %d: %q", needle, after, output)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestChatTUITeardownCancellation(t *testing.T) {
	for _, priorRun := range []bool{false, true} {
		name := "already interrupted"
		if priorRun {
			name = "next run after interruption"
		}
		t.Run(name, func(t *testing.T) {
			master, slave := openPTY(t)
			fd := int(slave.Fd())
			base, err := unix.IoctlGetTermios(fd, unix.TCGETS)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.IoctlSetWinsize(fd, unix.TIOCSWINSZ, &unix.Winsize{Row: 24, Col: 100}); err != nil {
				t.Fatal(err)
			}
			terminal := &teardownTerminal{File: slave, fd: uintptr(fd)}
			drained := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, master); close(drained) }()
			entered, interrupted := make(chan int, 2), make(chan int, 4)
			firstStopped, release := make(chan struct{}), make(chan struct{})
			var firstOnce, releaseOnce sync.Once
			var round atomic.Int32
			finishFirst := func() { firstOnce.Do(func() { close(firstStopped) }) }
			finishActive := func() { releaseOnce.Do(func() { close(release) }) }
			submit := func(string) (int, error, os.Signal) {
				n := int(round.Add(1))
				entered <- n
				if priorRun && n == 1 {
					<-firstStopped
				} else {
					<-release
				}
				return exitStopped, nil, os.Interrupt
			}
			interrupt := func(os.Signal) {
				n := int(round.Load())
				interrupted <- n
				if priorRun && n == 1 {
					finishFirst()
				}
			}
			finished := make(chan struct{})
			var result chatDone
			go func() {
				result.code, result.err = runChatTUI("teardown", nil, submit, interrupt, nil, nil, new(chatDisplay), nil, slave, terminal)
				close(finished)
			}()
			t.Cleanup(func() {
				finishFirst()
				finishActive()
				terminal.broken.Store(true)
				_ = syscall.Kill(os.Getpid(), syscall.SIGWINCH)
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("chat teardown did not finish")
				}
				slave.Close()
				master.Close()
				select {
				case <-drained:
				case <-time.After(time.Second):
					t.Error("terminal output reader did not finish")
				}
			})
			send := func(s string) {
				t.Helper()
				if _, err := io.WriteString(master, s); err != nil {
					t.Fatal(err)
				}
			}
			await := func(ch <-chan int, want int, event string) {
				t.Helper()
				select {
				case got := <-ch:
					if got != want {
						t.Fatalf("%s affected run %d, want %d", event, got, want)
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("run %d never received %s", want, event)
				}
			}
			pos := terminal.await(t, "ready", 0)
			send("first\r")
			await(entered, 1, "submission")
			pos = terminal.await(t, "running", pos)
			send("\x03")
			await(interrupted, 1, "user interruption")
			if priorRun {
				pos = terminal.await(t, "ready", pos)
				send("second\r")
				await(entered, 2, "submission")
				terminal.await(t, "running", pos)
			}
			terminal.broken.Store(true)
			if err := syscall.Kill(os.Getpid(), syscall.SIGWINCH); err != nil {
				t.Fatal(err)
			}
			if priorRun {
				await(interrupted, 2, "teardown cancellation")
			} else {
				deadline := time.Now().Add(3 * time.Second)
				for {
					flags, err := unix.IoctlGetTermios(fd, unix.TCGETS)
					if err != nil {
						t.Fatal(err)
					}
					if flags.Lflag == base.Lflag {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("resize failure did not restore terminal input")
					}
					time.Sleep(5 * time.Millisecond)
				}
				select {
				case n := <-interrupted:
					t.Fatalf("teardown repeated run %d cancellation", n)
				case <-time.After(100 * time.Millisecond):
				}
			}
			finishActive()
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("chat did not exit after cleanup finished")
			}
			if result.code != exitError || !errors.Is(result.err, syscall.EBADF) {
				t.Fatalf("terminal resize failure was lost: %+v", result)
			}
			select {
			case n := <-interrupted:
				t.Fatalf("teardown repeated run %d cancellation", n)
			default:
			}
		})
	}
}

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
