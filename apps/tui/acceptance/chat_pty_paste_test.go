//go:build acceptance && linux

package acceptance

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "paste accepted"}})
	p := startPTYChat(t, f, nil)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "\x1b[20")
	p.awaitInputRead(t)
	p.send(t, "0~line one\nline two\x1b[201~")
	p.await(t, "line two", pos)
	if n := len(f.model.requests()); n != 0 {
		t.Fatalf("multiline paste submitted before Enter: %d model requests", n)
	}
	p.send(t, "\r")
	awaitPTYPhase(t, f, "returned")
	p.await(t, "paste accepted", pos)
	if s := f.status(); s.Phase != "returned" || s.Run.Ordinal != 1 || s.Run.Goal != "line one\nline two" {
		t.Fatalf("paste changed the submitted goal: %+v", s)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func TestChatPTYPasteSpansCompletionAndRequiresManualSend(t *testing.T) {
	t.Setenv("TERM", "dumb")
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "first answer"}})
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
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "first request\r")
	pos = p.awaitStatus(t, "running", pos)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	p.send(t, "\x1b[200~draft prefix\n")
	p.awaitInputRead(t)
	releaseGate()
	awaitPTYPhase(t, f, "returned")
	pos = p.awaitStatus(t, "ready", pos)
	if s := f.status(); s.Phase != "returned" || s.Run.Ordinal != 1 {
		t.Fatalf("first request did not finish before paste suffix: %+v", s)
	}
	p.send(t, "draft suffix\r\x1b[201~")
	p.awaitInputRead(t)
	pos = p.awaitStatus(t, "ready", pos)
	if s := f.status(); s.Run.Ordinal != 1 {
		t.Fatalf("busy paste tail created a run: %+v", s.Run)
	}
	p.send(t, "\r")
	p.await(t, "second answer", pos)
	if s := f.status(); s.Phase != "returned" || s.Run.Ordinal != 2 || s.Run.Goal != "draft prefix\ndraft suffix" {
		t.Fatalf("next input after busy paste did not finish: %+v", s)
	}
	if prompts := f.model.prompts(1); len(prompts) != 2 || !strings.Contains(prompts[1], "draft prefix\ndraft suffix") {
		t.Fatalf("retained paste missing from model context: %q", prompts)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func awaitPTYPhase(t *testing.T, f *fixture, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r := f.cli(nil, "status")
		var s status
		if r.code == exitOK && strict(r.stdout, &s) == nil && s.Phase == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run never reached %s: status = %d\nstdout: %s\nstderr: %s\nmodel requests=%d", want, r.code, r.stdout, r.stderr, len(f.model.requests()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
