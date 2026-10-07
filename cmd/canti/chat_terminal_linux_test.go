package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
	"github.com/charmbracelet/x/ansi"
	"golang.org/x/sys/unix"
)

func TestChatTerminalDiscardsBusyInput(t *testing.T) {
	for _, tc := range []struct{ interrupted, canonical bool }{{false, true}, {true, true}, {false, false}, {true, false}} {
		t.Run(fmt.Sprintf("interrupt=%v/canonical=%v", tc.interrupted, tc.canonical), func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
			f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"fresh answer"}`})
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(body))
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
			defer unblock()
			p := startPTYChatWithMode(t, f, nil, nil, tc.canonical, "-model-url", server.URL)
			pos := p.await(t, "ready", 0)
			p.awaitRaw(t)
			p.send(t, "first\rread ahead\r")
			pos = p.await(t, "running", pos)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first request did not reach provider")
			}
			p.send(t, "busy complete\rbusy partial")
			if tc.interrupted {
				p.send(t, "\x03")
				deadline := time.Now().Add(5 * time.Second)
				for f.status().Phase != core.PhaseStopped {
					if time.Now().After(deadline) {
						t.Fatal("Ctrl-C did not stop the active run")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			unblock()
			pos = p.await(t, "ready", pos)
			time.Sleep(150 * time.Millisecond)
			wantRequests := 1
			if tc.interrupted {
				wantRequests = 0
			}
			if s := f.status(); s.Run.SessionSeq != 1 || s.Run.Goal != "first" || len(f.model.requests()) != wantRequests {
				t.Fatalf("busy or read-ahead input created another run: %+v", s)
			}
			p.send(t, "fresh\r")
			answer := "fresh answer"
			if tc.interrupted {
				answer = "first answer"
			}
			p.await(t, answer, pos)
			if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "fresh" || s.Phase != core.PhaseReturned {
				t.Fatalf("fresh input did not advance cleanly: %+v", s)
			}
			prompts := f.model.prompts(1)
			if len(prompts) != wantRequests+1 || strings.Contains(prompts[len(prompts)-1], "busy complete") || strings.Contains(prompts[len(prompts)-1], "read ahead") {
				t.Fatalf("stale input entered model context: %q", prompts)
			}
			p.send(t, "\x03")
			p.finish(t, exitOK)
		})
	}
}

func TestChatTerminalReopensPromptAfterStderrBackpressure(t *testing.T) {
	testChatTerminalReopensPromptAfterStderrBackpressure(t)
}

func testChatTerminalReopensPromptAfterStderrBackpressure(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"fresh answer"}`})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
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
	defer unblock()
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
	p := startPTYChat(t, f, w, "-model-url", server.URL)
	p.awaitRaw(t)
	readFD := int(r.Fd())
	if err := unix.SetNonblock(readFD, true); err != nil {
		t.Fatal(err)
	}
	var initial bytes.Buffer
	var chunk [4096]byte
	until := time.Now().Add(5 * time.Second)
	for !strings.Contains(ansi.Strip(initial.String()), "ready") {
		n, readErr := unix.Read(readFD, chunk[:])
		if n > 0 {
			initial.Write(chunk[:n])
		}
		if readErr != nil && readErr != unix.EAGAIN {
			t.Fatal(readErr)
		}
		if time.Now().After(until) {
			t.Fatalf("initial TUI ready frame missing: %q", initial.String())
		}
		if n <= 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	for {
		n, readErr := unix.Read(readFD, chunk[:])
		if readErr == unix.EAGAIN {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		if n == 0 {
			t.Fatal("terminal output closed before pressure test")
		}
	}
	p.send(t, "first\r")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	store, err := sqlite.Open(t.Context(), filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, err := store.LastRun(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	initial.Reset()
	until = time.Now().Add(5 * time.Second)
	for !strings.Contains(ansi.Strip(initial.String()), "running") {
		n, readErr := unix.Read(readFD, chunk[:])
		if n > 0 {
			initial.Write(chunk[:n])
		}
		if readErr != nil && readErr != unix.EAGAIN {
			t.Fatal(readErr)
		}
		if time.Now().After(until) {
			t.Fatalf("TUI running frame missing before pressure: %q", initial.String())
		}
		if n <= 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	for {
		n, readErr := unix.Read(readFD, chunk[:])
		if readErr == unix.EAGAIN {
			break
		}
		if readErr != nil || n == 0 {
			t.Fatalf("terminal output closed before pressure: n=%d err=%v", n, readErr)
		}
	}
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	if flags, err := unix.FcntlInt(uintptr(writeFD), unix.F_GETFL, 0); err != nil || flags&unix.O_NONBLOCK == 0 {
		t.Fatalf("stderr pipe not nonblocking: flags=%x err=%v", flags, err)
	}
	fillLivePipe(t, writeFD)
	unblock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, err := store.Latest(t.Context(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Phase == core.PhaseReturned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first run did not return before output pressure was released")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	p.send(t, "stale during blocked display\r")
	time.Sleep(50 * time.Millisecond)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			n, readErr := unix.Read(readFD, chunk[:])
			if n > 0 {
				p.mu.Lock()
				p.output.Write(chunk[:n])
				p.mu.Unlock()
				select {
				case p.wake <- struct{}{}:
				default:
				}
			}
			if readErr == nil && n == 0 || readErr != nil && readErr != unix.EAGAIN {
				return
			}
			if n <= 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	defer func() {
		r.Close()
		<-drained
	}()
	deadline = time.Now().Add(5 * time.Second)
	for !strings.Contains(ansi.Strip(p.text()), "ready") {
		if time.Now().After(deadline) {
			err := p.cmd.Process.Signal(syscall.Signal(0))
			t.Fatalf("TUI did not redraw ready after stderr recovered: child signal(0)=%v, output tail=%q", err, p.text()[max(0, len(p.text())-500):])
		}
		select {
		case <-p.wake:
		case <-time.After(10 * time.Millisecond):
		}
	}
	if s := f.status(); s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("busy input survived output recovery: %+v, requests=%d, output tail=%q", s, len(f.model.requests()), p.text()[max(0, len(p.text())-500):])
	}
	p.send(t, "fresh\r")
	deadline = time.Now().Add(5 * time.Second)
	for {
		if s := f.status(); s.Run.SessionSeq == 2 && s.Phase == core.PhaseReturned {
			if s.Run.Goal != "fresh" || len(f.model.requests()) != 2 {
				t.Fatalf("fresh input did not advance cleanly: %+v, requests=%d", s, len(f.model.requests()))
			}
			break
		}
		if time.Now().After(deadline) {
			err := p.cmd.Process.Signal(syscall.Signal(0))
			t.Fatalf("TUI did not accept fresh input after output recovery: %+v, child signal(0)=%v, output tail=%q", f.status(), err, p.text()[max(0, len(p.text())-500):])
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
	w.Close()
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
	readFD := int(r.Fd())
	if err := unix.SetNonblock(readFD, true); err != nil {
		t.Fatal(err)
	}
	awaitFrame := func(needle string) {
		t.Helper()
		var output []byte
		var chunk [4096]byte
		deadline := time.Now().Add(5 * time.Second)
		for !bytes.Contains([]byte(ansi.Strip(string(output))), []byte(needle)) {
			n, readErr := unix.Read(readFD, chunk[:])
			if n > 0 {
				output = append(output, chunk[:n]...)
			}
			if readErr != nil && readErr != unix.EAGAIN {
				t.Fatal(readErr)
			}
			if time.Now().After(deadline) {
				t.Fatalf("TUI never showed %q before pressure: %q", needle, output)
			}
			if n <= 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
		for {
			n, readErr := unix.Read(readFD, chunk[:])
			if readErr == unix.EAGAIN {
				return
			}
			if readErr != nil || n == 0 {
				t.Fatalf("terminal output closed before pressure: n=%d err=%v", n, readErr)
			}
		}
	}
	awaitFrame("ready")
	p.send(t, "first\r")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	awaitFrame("running")
	store, err := sqlite.Open(t.Context(), filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, err := store.LastRun(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	fillLivePipe(t, writeFD)
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := store.Latest(t.Context(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.State.Phase == core.PhaseReturned {
			break
		}
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
