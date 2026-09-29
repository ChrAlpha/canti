package main

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"golang.org/x/sys/unix"
)

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

func TestChatPromptWriteBackpressure(t *testing.T) {
	t.Run("short congestion recovery", testChatTerminalReopensPromptAfterStderrBackpressure)
	t.Run("display deadline", testChatPromptDisplayDeadline)
	for _, signal := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run("returned signal "+signal.String(), func(t *testing.T) {
			testChatPromptReturnedSignal(t, signal)
		})
	}
}

func testChatPromptReturnedSignal(t *testing.T, signal syscall.Signal) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
	entered, release := make(chan struct{}, 1), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			f.model.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD := int(w.Fd())
	p := startPTYChat(t, f, w, "-model-url", server.URL)
	p.awaitRaw(t)
	p.send(t, "first\r")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	fillLivePipe(t, writeFD)
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for f.status().Phase != core.PhaseReturned {
		if time.Now().After(deadline) {
			t.Fatal("first run did not return while output was blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := p.cmd.Process.Signal(signal); err != nil {
		t.Fatal(err)
	}
	p.finish(t, exitError)
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("signal changed completed run: %+v, requests=%d", s, len(f.model.requests()))
	}
}

func testChatPromptDisplayDeadline(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
	held := f.model.holdRequests()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD := int(w.Fd())
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	p := startPTYChat(t, f, w)
	p.awaitRaw(t)
	p.send(t, "first\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	if flags, err := unix.FcntlInt(uintptr(writeFD), unix.F_GETFL, 0); err != nil || flags&unix.O_NONBLOCK == 0 {
		t.Fatalf("stderr pipe not nonblocking: flags=%x err=%v", flags, err)
	}
	fillLivePipe(t, writeFD)
	p.send(t, "before visible\rpartial")
	start := time.Now()
	p.send(t, "\x03")
	p.finish(t, exitError)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("full terminal output took %s to terminate", elapsed)
	}
	if s := f.status(); s.Run.SessionSeq != 1 || s.Run.Goal != "first" || s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled || len(f.model.requests()) != 1 {
		t.Fatalf("output failure replayed busy input or lost cancellation: %+v, requests=%d", s, len(f.model.requests()))
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
