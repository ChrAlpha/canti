package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
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
	p.send(t, "first\r")
	select {
	case <-entered:
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
	unblock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if s := f.status(); s.Phase == core.PhaseReturned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first run did not return before output pressure was released")
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.send(t, "stale during blocked display\r")
	time.Sleep(50 * time.Millisecond)
	drained := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, r); close(drained) }()
	time.Sleep(150 * time.Millisecond)
	if s := f.status(); s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
		t.Fatalf("busy input survived output recovery: %+v, requests=%d", s, len(f.model.requests()))
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
			t.Fatalf("TUI did not accept fresh input after output recovery: %+v", f.status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
	w.Close()
	<-drained
}
