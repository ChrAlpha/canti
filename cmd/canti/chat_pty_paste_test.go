//go:build linux

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"golang.org/x/sys/unix"
)

func (p *ptyChat) awaitInputRead(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		queued, err := unix.IoctlGetInt(p.slaveFD, unix.TIOCINQ)
		if err != nil {
			t.Fatal(err)
		}
		if queued == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("PTY input reader left %d bytes queued", queued)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChatPTYSplitBracketedPasteSubmitsOneMultilineGoal(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "paste accepted"}})
	p := startPTYChat(t, f, nil)
	pos := p.await(t, "ready", 0)
	p.send(t, "\x1b[20")
	p.awaitInputRead(t)
	p.send(t, "0~line one\nline two\x1b[201~")
	p.await(t, "line two", pos)
	if n := len(f.model.requests()); n != 0 {
		t.Fatalf("multiline paste submitted before Enter: %d model requests", n)
	}
	p.send(t, "\r")
	awaitPTYPhase(t, f, core.PhaseReturned)
	p.await(t, "paste accepted", pos)
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 1 || s.Run.Goal != "line one\nline two" {
		t.Fatalf("paste changed the submitted goal: %+v", s)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func TestChatPTYBusyPasteTailAfterReturnIsDiscarded(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second answer"}`})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		f.model.ServeHTTP(w, r)
	}))
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() { releaseGate(); gate.Close() })
	p := startPTYChat(t, f, nil, "-model-url", gate.URL)
	pos := p.await(t, "ready", 0)
	p.send(t, "first request\r")
	pos = p.await(t, "running", pos)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	p.send(t, "\x1b[200~discarded prefix\n")
	p.awaitInputRead(t)
	releaseGate()
	awaitPTYPhase(t, f, core.PhaseReturned)
	pos = p.await(t, "discarding incomplete terminal input", pos)
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 1 {
		t.Fatalf("first request did not finish before paste suffix: %+v", s)
	}
	p.send(t, "discarded suffix\r\x1b[201~")
	p.awaitInputRead(t)
	pos = p.await(t, "ready", pos)
	if s := f.status(); s.Run.SessionSeq != 1 {
		t.Fatalf("busy paste tail created a run: %+v", s.Run)
	}
	p.send(t, "explicit next\r")
	p.await(t, "second answer", pos)
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 2 || s.Run.Goal != "explicit next" {
		t.Fatalf("next input after busy paste did not finish: %+v", s)
	}
	if prompts := f.model.prompts(1); len(prompts) != 2 || strings.Contains(prompts[1], "discarded") {
		t.Fatalf("discarded paste entered model context: %q", prompts)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func awaitPTYPhase(t *testing.T, f *fixture, want core.Phase) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := f.status()
		if s.Phase == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run never reached %s: %+v, model requests=%d", want, s, len(f.model.requests()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
