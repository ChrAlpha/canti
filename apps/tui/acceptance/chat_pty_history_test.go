//go:build acceptance && linux

package acceptance

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
)

func TestChatPTYReopensTheLineAndFreshStartsEmpty(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"second answer"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"fresh answer"}`})
	if r := f.cli(nil, "run", "-goal", "first goal"); r.code != exitOK || r.stdout != "first answer\n" {
		t.Fatalf("first recorded run: %+v", r)
	}
	if r := f.cli(nil, "run", "-goal", "second goal"); r.code != exitOK || r.stdout != "second answer\n" {
		t.Fatalf("second recorded run: %+v", r)
	}
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	if locked(f.state) {
		t.Fatal("owned editor held the state lock while idle")
	}
	p.await(t, "── earlier history ──", 0)
	p.await(t, "› first goal", 0)
	p.await(t, "first answer", 0)
	p.await(t, "› second goal", 0)
	p.await(t, "second answer", 0)
	p.await(t, "── this session ──", 0)
	p.send(t, "\x03")
	p.finish(t, exitOK)

	fresh := startPTYChat(t, f, nil, "-fresh")
	fresh.awaitStatus(t, "ready", 0)
	if locked(f.state) {
		t.Fatal("owned -fresh editor held the state lock while idle")
	}
	screen := fresh.screen.String()
	if strings.Contains(screen, "first goal") || strings.Contains(screen, "first answer") ||
		strings.Contains(screen, "second goal") || strings.Contains(screen, "second answer") || strings.Contains(screen, "earlier history") {
		t.Fatalf("-fresh showed earlier history: %q", screen)
	}
	fresh.send(t, "fresh goal\r")
	answer := fresh.await(t, "fresh answer", 0)
	fresh.awaitStatus(t, "ready", answer)
	fresh.send(t, "\x03")
	fresh.finish(t, exitOK)

	after := startPTYChat(t, f, nil)
	after.awaitStatus(t, "ready", 0)
	after.await(t, "› fresh goal", 0)
	answer = after.await(t, "fresh answer", 0)
	after.awaitStatus(t, "ready", answer)
	afterScreen := after.screen.String()
	if strings.Contains(afterScreen, "first goal") || strings.Contains(afterScreen, "first answer") ||
		strings.Contains(afterScreen, "second goal") || strings.Contains(afterScreen, "second answer") {
		t.Fatalf("reopening after -fresh crossed the new line boundary: %q", afterScreen)
	}
	after.send(t, "\x03")
	after.finish(t, exitOK)
}

func TestChatPTYReopenFollowsADetachedRunOnce(t *testing.T) {
	f := newFixture(t, nil)
	f.autoHost()
	gate := make(chan struct{})
	f.model.decideWith(func(step int, _ string) (decision, error) {
		switch step {
		case 1:
			return decision{Kind: "exec", Payload: "canti:handover first step"}, nil
		case 2:
			<-gate
			return decision{Kind: "return", Payload: "busy answer"}, nil
		}
		return decision{}, errors.New("unexpected step")
	})
	first := startPTYChatWithAttr(t, f, nil, nil, true, 30*time.Second, &syscall.SysProcAttr{Setsid: true, Setctty: true}, "-host", "auto")
	first.awaitStatus(t, "ready", 0)
	first.send(t, "busy goal\r")
	first.await(t, "$ canti:handover first step", 0)
	waitFor(t, func() bool { return len(f.model.requests()) == 2 })
	first.waited = true
	ended := make(chan error, 1)
	go func() { ended <- first.cmd.Wait() }()
	if err := first.master.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("chat did not exit after its terminal hung up")
	}

	second := startPTYChat(t, f, nil, "-host", "auto")
	running := second.awaitStatus(t, "running", 0)
	second.await(t, "› busy goal", 0)
	second.await(t, "$ canti:handover first step", 0)
	close(gate)
	second.await(t, "busy answer", running)
	second.awaitStatus(t, "ready", running)
	second.send(t, "\x03")
	second.finish(t, exitOK)

	out := second.text()
	end := strings.LastIndex(out, "\x1b[?1049l")
	if end < 0 {
		t.Fatal("chat did not leave the alternate screen")
	}
	transcript := strings.ReplaceAll(ansi.Strip(out[end:]), "\r\n", "\n")
	for _, once := range []string{"› busy goal", "$ canti:handover first step", "busy answer"} {
		if n := strings.Count(transcript, once); n != 1 {
			t.Fatalf("%q appears %d times in the reopened transcript:\n%s", once, n, transcript)
		}
	}
}

func waitModelRequests(t *testing.T, f *fixture, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for len(f.model.requests()) < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %d model requests (have %d)", timeout, want, len(f.model.requests()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestChatPTYReopenShowsAQueuedGoalOpenedBetweenHistoryPages(t *testing.T) {
	f := newFixture(t, nil)
	f.autoHost()
	gate := make(chan struct{})
	f.model.decideWith(func(step int, prompt string) (decision, error) {
		switch {
		case strings.Contains(prompt, "C goal"):
			return decision{Kind: "return", Payload: "C answer"}, nil
		case strings.Contains(prompt, "B goal"):
			return decision{Kind: "return", Payload: "B answer"}, nil
		default:
			<-gate
			return decision{Kind: "return", Payload: "A answer"}, nil
		}
	})
	_, hostWait := f.start(nil, "host")
	waitFor(t, func() bool { _, err := os.Stat(f.socket()); return err == nil })
	t.Cleanup(func() { _ = hostWait })
	_, aWait := f.start(nil, "run", "-host", "auto", "-goal", "A goal", "-max-steps", "64")
	waitModelRequests(t, f, 1, 30*time.Second)
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "B goal", "-delivery", "follow_up", "-fresh"); r.code != exitOK {
		t.Fatalf("queue B: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "C goal", "-delivery", "follow_up"); r.code != exitOK {
		t.Fatalf("queue C: %+v", r)
	}
	hold := f.holdFirstHistory(t)
	p := startPTYChatWithDeadline(t, f, nil, nil, true, 30*time.Second, "-host", "auto")
	<-hold.held
	close(gate)
	if r := aWait(); r.code != exitOK {
		t.Fatalf("A run: %+v", r)
	}
	waitModelRequests(t, f, 3, 10*time.Second)
	hold.Release()
	p.awaitStatus(t, "ready", 0)
	goal := p.await(t, "B goal", 0)
	p.await(t, "B answer", goal)
	cgoal := p.await(t, "C goal", 0)
	p.await(t, "C answer", cgoal)
	if screen := p.screen.String(); strings.Count(screen, "B goal") != 1 || strings.Count(screen, "B answer") != 1 ||
		strings.Count(screen, "C goal") != 1 || strings.Count(screen, "C answer") != 1 {
		t.Fatalf("goal/answer occurrences B=%d/%d C=%d/%d in the screen:\n%s", strings.Count(screen, "B goal"), strings.Count(screen, "B answer"), strings.Count(screen, "C goal"), strings.Count(screen, "C answer"), screen)
	} else if a, sep, b := strings.Index(screen, "A answer"), strings.Index(screen, "a new line started"), strings.Index(screen, "B answer"); a < 0 || sep < 0 || b < 0 || !(a < sep && sep < b) {
		t.Fatalf("new-line order A answer=%d separator=%d B answer=%d in:\n%s", a, sep, b, screen)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func TestChatPTYReopenMarksFreshThenNonFreshBeforeTheFirstFrame(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "before answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"fresh answer"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"after answer"}`})
	f.autoHost()
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "before goal"); r.code != exitOK {
		t.Fatalf("before run: %+v", r)
	}
	hold := f.holdFirstHistory(t)
	p := startPTYChatWithDeadline(t, f, nil, nil, true, 30*time.Second, "-host", "auto")
	<-hold.held
	if r := f.cli(nil, "run", "-host", "auto", "-fresh", "-goal", "fresh goal"); r.code != exitOK {
		t.Fatalf("fresh run: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "after goal"); r.code != exitOK {
		t.Fatalf("non-fresh successor: %+v", r)
	}
	hold.Release()
	p.awaitStatus(t, "ready", 0)
	p.await(t, "a new line started", 0)
	p.await(t, "› after goal", 0)
	p.await(t, "after answer", 0)
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func TestChatPTYReopenFollowsARunStartedAfterHistory(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "started answer"}})
	f.autoHost()
	gate := make(chan struct{})
	f.model.decideWith(func(int, string) (decision, error) {
		<-gate
		return decision{Kind: "return", Payload: "started answer"}, nil
	})
	marker := filepath.Join(t.TempDir(), "history")
	p := startPTYChatWithEnv(t, f, nil, []string{"CANTI_TEST_SETUP=point:history.read", "CANTI_TEST_BLOCKED=" + marker}, "-host", "auto")
	waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
	_, runWait := f.start(nil, "run", "-host", "auto", "-fresh", "-goal", "fresh start")
	waitFor(t, func() bool { return len(f.model.requests()) >= 1 })
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	running := p.awaitStatus(t, "running", 0)
	p.await(t, "a new line started", 0)
	p.await(t, "› fresh start", 0)
	close(gate)
	p.await(t, "started answer", running)
	p.awaitStatus(t, "ready", running)
	p.send(t, "\x03")
	p.finish(t, exitOK)
	if r := runWait(); r.code != exitOK {
		t.Fatalf("run started after history: %+v", r)
	}
}

func TestChatPTYStoppedRunWithHeldInputsStartsAndWithdraws(t *testing.T) {
	f := newFixture(t, nil)
	gate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	f.model.decideWith(func(int, string) (decision, error) {
		<-gate
		return decision{Kind: "return", Payload: "new answer"}, nil
	})
	first := startPTYChat(t, f, nil)
	ready := first.awaitStatus(t, "ready", 0)
	first.send(t, "first request\r")
	running := first.awaitStatus(t, "running", ready)
	first.send(t, "held follow-up\x06")
	first.await(t, "follow-up queued", running)
	first.send(t, "\x03")
	first.await(t, "held input", 0)
	first.send(t, "\x03")
	first.finish(t, exitOK)

	second := startPTYChat(t, f, nil)
	ready = second.awaitStatus(t, "ready", 0)
	second.await(t, "held", ready)
	second.send(t, "\x12")
	second.await(t, "Withdrew 1 held input(s) into the draft", ready)
	close(gate)
	second.send(t, "new goal\r")
	answer := second.await(t, "new answer", ready)
	second.awaitStatus(t, "ready", answer)
	second.send(t, "\x03")
	second.finish(t, exitOK)
}

func TestChatPTYReopenKeepsANonFreshPredecessorAboveTheSeparator(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "before answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"D answer"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"B answer"}`})
	f.autoHost()
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "before goal"); r.code != exitOK {
		t.Fatalf("before run: %+v", r)
	}
	hold := f.holdFirstHistory(t)
	p := startPTYChatWithDeadline(t, f, nil, nil, true, 30*time.Second, "-host", "auto")
	<-hold.held
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "D goal"); r.code != exitOK {
		t.Fatalf("D run: %+v", r)
	}
	if r := f.cli(nil, "run", "-host", "auto", "-fresh", "-goal", "B goal"); r.code != exitOK {
		t.Fatalf("B run: %+v", r)
	}
	hold.Release()
	p.awaitStatus(t, "ready", 0)
	p.await(t, "D answer", 0)
	goal := p.await(t, "B goal", 0)
	p.await(t, "B answer", goal)
	screen := p.screen.String()
	d, sep, b := strings.Index(screen, "D answer"), strings.Index(screen, "a new line started"), strings.Index(screen, "B answer")
	if d < 0 || sep < 0 || b < 0 || !(d < sep && sep < b) {
		t.Fatalf("predecessor order D=%d separator=%d B=%d in:\n%s", d, sep, b, screen)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
