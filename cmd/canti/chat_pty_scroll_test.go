//go:build linux

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/charmbracelet/x/ansi"
)

func TestChatPTYBusyScrollDoesNotQueueInput(t *testing.T) {
	line := func(i int) string { return strings.Repeat(string(rune('A'+i)), 30) }
	var history strings.Builder
	for i := 0; i < 40; i++ {
		history.WriteString(line(i) + "\n")
	}
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: history.String()}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second answer"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"third answer"}`})
	entered := make(chan struct{})
	firstRelease := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int32
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			select {
			case <-firstRelease:
			case <-r.Context().Done():
			}
		case 2:
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		f.model.ServeHTTP(w, r)
	}))
	var once sync.Once
	var firstOnce sync.Once
	unblockFirst := func() { firstOnce.Do(func() { close(firstRelease) }) }
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblockFirst(); unblock(); gate.Close() })
	p := startPTYChat(t, f, nil, "-model-url", gate.URL)
	pos := p.await(t, "ready", 0)
	p.send(t, "first request\r")
	pos = p.await(t, "new input discarded", pos)
	unblockFirst()
	p.await(t, line(39), pos)
	pos = p.await(t, "eady", pos)
	p.send(t, "second request\r")
	p.await(t, "new input discarded", pos)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second request did not reach provider")
	}
	pos = len(ansi.Strip(p.text()))
	p.send(t, "discarded text\r\x1b[200~discarded paste\x1b[5~\x1b[201~\x1b[5")
	p.awaitInputRead(t)
	p.send(t, "~")
	p.await(t, line(12), pos)
	pos = len(ansi.Strip(p.text()))
	p.send(t, "\x1b[6~")
	pos = p.await(t, line(39), pos)
	if s := f.status(); s.Run.SessionSeq != 2 || s.Run.Goal != "second request" || s.Phase != core.PhaseRequestSent {
		t.Fatalf("navigation changed the active run: %+v", s)
	}
	unblock()
	p.await(t, "second answer", pos)
	pos = p.await(t, "eady", pos)
	p.send(t, "explicit next\r")
	p.await(t, "third answer", pos)
	if s := f.status(); s.Run.SessionSeq != 3 || s.Run.Goal != "explicit next" || s.Phase != core.PhaseReturned {
		t.Fatalf("busy input leaked into the next run: %+v", s)
	}
	if prompts := f.model.prompts(1); len(prompts) != 3 || strings.Contains(strings.Join(prompts, "\n"), "discarded") {
		t.Fatalf("busy input entered model context: %q", prompts)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
