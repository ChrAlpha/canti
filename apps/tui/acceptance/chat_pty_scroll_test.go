//go:build acceptance && linux

package acceptance

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChatPTYBusyScrollDoesNotQueueInput(t *testing.T) {
	line := func(i int) string {
		var b [30]byte
		for j := range b {
			b[j] = byte('!' + (i*31+j)%94)
		}
		return string(b[:])
	}
	var history strings.Builder
	for i := 0; i < 40; i++ {
		history.WriteString(line(i) + "\n")
	}
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: history.String()}})
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
	p := startPTYChatWithEnv(t, f, nil, []string{"TERM=tmux-256color"}, "-model-url", gate.URL)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "first request\r")
	pos = p.awaitStatus(t, "running", pos)
	unblockFirst()
	p.await(t, line(39), pos)
	pos = p.awaitStatus(t, "ready", pos)
	p.send(t, "second request\r")
	p.awaitStatus(t, "running", pos)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second request did not reach provider")
	}
	pos = p.mark()
	p.send(t, "draft text\x1b[200~draft paste\x1b[5~\x1b[201~\x1b[5")
	p.awaitInputRead(t)
	p.send(t, "~")
	// The 24-row layout reserves nine rows. The completed turn's reported
	// usage adds one transcript row, so page up shows history rows 15..29.
	// Assert the complete window and retained
	// draft, not a substring of incremental terminal output.
	p.await(t, line(15), pos)
	p.mu.Lock()
	screen := p.screen.String()
	p.mu.Unlock()
	for i := 15; i <= 29; i++ {
		if !strings.Contains(screen, line(i)) {
			t.Fatalf("page up lost history row %d: %q", i, screen)
		}
	}
	if strings.Contains(screen, line(14)) || strings.Contains(screen, line(30)) || strings.Contains(screen, line(39)) || !strings.Contains(screen, "draft textdraft paste") {
		t.Fatalf("page up changed viewport or draft incorrectly: %q", screen)
	}
	pos = p.mark()
	p.send(t, "\x1b[6~")
	pos = p.await(t, line(39), pos)
	if s := f.status(); s.Run.Ordinal != 2 || s.Run.Goal != "second request" || s.Phase != "request_sent" {
		t.Fatalf("navigation changed the active run: %+v", s)
	}
	unblock()
	p.await(t, "second answer", pos)
	pos = p.awaitStatus(t, "ready", pos)
	p.send(t, "\r")
	p.await(t, "third answer", pos)
	if s := f.status(); s.Run.Ordinal != 3 || s.Run.Goal != "draft textdraft paste" || s.Phase != "returned" {
		t.Fatalf("busy input leaked into the next run: %+v", s)
	}
	if prompts := f.model.prompts(1); len(prompts) != 3 || !strings.Contains(prompts[2], "draft textdraft paste") {
		t.Fatalf("busy input entered model context: %q", prompts)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
