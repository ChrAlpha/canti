//go:build acceptance && linux

package acceptance

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
)

func TestChatPTYReopensAStoppedRunWithItsReason(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "exec", Payload: "canti:handover step one"}})
	r := f.cli(nil, "run", "-goal", "stopped goal", "-max-steps", "1")
	s := f.status()
	if r.code != exitStopped || s.Phase != "stopped" || s.Reason != "steps_exhausted" {
		t.Fatalf("stopped run: %+v, %+v", r, s)
	}
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	p.await(t, "canti: run "+s.Run.ID+" stopped: steps_exhausted", 0)
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func TestChatPTYReopensAllEightRunsWhole(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "answer-01"}})
	for i := 2; i <= 8; i++ {
		f.model.answer(1, i, reply{text: fmt.Sprintf(`{"kind":"return","payload":"answer-%02d"}`, i)})
	}
	for i := 1; i <= 8; i++ {
		if r := f.cli(nil, "run", "-goal", fmt.Sprintf("run-%02d", i)); r.code != exitOK {
			t.Fatalf("run %d: %+v", i, r)
		}
	}
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	p.send(t, "\x03")
	p.finish(t, exitOK)
	transcript := finalTranscript(t, p)
	for i := 1; i <= 8; i++ {
		for _, want := range []string{fmt.Sprintf("› run-%02d", i), fmt.Sprintf("answer-%02d", i)} {
			if !strings.Contains(transcript, want) {
				t.Fatalf("reopened transcript lacks %q:\n%s", want, transcript)
			}
		}
	}
}

func TestChatPTYReopenShowsATruncatedAnswerNotice(t *testing.T) {
	answer := strings.Repeat("x", 8982) + "-tail"
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: answer}})
	if r := f.cli(nil, "run", "-goal", "long answer"); r.code != exitOK || r.stdout != answer+"\n" {
		t.Fatalf("long run: code %d, stdout %d bytes", r.code, len(r.stdout))
	}
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	p.await(t, "answer truncated", 0)
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func finalTranscript(t *testing.T, p *ptyChat) string {
	t.Helper()
	_ = p.slave.Close()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("PTY master reader did not drain")
	}
	_ = p.master.Close()
	out := p.text()
	end := strings.LastIndex(out, "\x1b[?1049l")
	if end < 0 {
		t.Fatal("chat did not leave the alternate screen")
	}
	return strings.ReplaceAll(ansi.Strip(out[end:]), "\r\n", "\n")
}

func TestChatPTYReopenedHistoryOmitsAFragment(t *testing.T) {
	f := newFixture(t, nil)
	f.model.decideWith(func(step int, _ string) (decision, error) {
		if step >= 256 {
			return decision{Kind: "return", Payload: "long answer"}, nil
		}
		return decision{Kind: "exec", Payload: "canti:handover step"}, nil
	})
	if r := f.cli(nil, "run", "-goal", "long goal"); r.code != exitOK || r.stdout != "long answer\n" {
		t.Fatalf("long run: %+v", r)
	}
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	p.await(t, "canti history", 0)
	p.send(t, "\x03")
	p.finish(t, exitOK)
	transcript := finalTranscript(t, p)
	if strings.Contains(transcript, "long answer") && !strings.Contains(transcript, "› long goal") {
		t.Fatalf("reopened transcript kept an answer without its goal:\n%s", transcript)
	}
	if !strings.Contains(transcript, "… earlier transcript omitted") {
		t.Fatalf("a fully omitted history lacks its notice:\n%s", transcript)
	}
}

func TestChatPTYAttachedTruncatedAnswerShowsTheNotice(t *testing.T) {
	answer := strings.Repeat("x", 8982) + "-tail"
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: answer}})
	p := startPTYChat(t, f, nil)
	ready := p.awaitStatus(t, "ready", 0)
	p.send(t, "long answer\r")
	answerFrame := p.await(t, "answer truncated", ready)
	p.awaitStatus(t, "ready", answerFrame)
	p.send(t, "\x03")
	p.finish(t, exitOK)
}

func TestChatPTYReopenShowsAQueuedFollowUpBeforeItOpens(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "exec", Payload: "canti:handover step one"}})
	f.autoHost()
	gate := make(chan struct{})
	f.model.decideWith(func(step int, _ string) (decision, error) {
		if step >= 2 {
			<-gate
			return decision{Kind: "return", Payload: "queued answer"}, nil
		}
		return decision{Kind: "exec", Payload: "canti:handover step one"}, nil
	})
	first := startPTYChatWithAttr(t, f, nil, nil, true, 30*time.Second, &syscall.SysProcAttr{Setsid: true, Setctty: true}, "-host", "auto")
	first.awaitStatus(t, "ready", 0)
	first.send(t, "working goal\r")
	first.await(t, "$ canti:handover step one", 0)
	waitFor(t, func() bool { return len(f.model.requests()) == 2 })
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "queued next", "-delivery", "follow_up"); r.code != exitOK {
		t.Fatalf("queue follow-up: %+v", r)
	}
	first.waited = true
	ended := make(chan error, 1)
	go func() { ended <- first.cmd.Wait() }()
	if err := first.master.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("first chat did not detach")
	}

	second := startPTYChat(t, f, nil, "-host", "auto")
	running := second.awaitStatus(t, "running", 0)
	second.await(t, "» queued next", 0)
	close(gate)
	second.await(t, "queued answer", running)
	second.awaitStatus(t, "ready", running)
	second.send(t, "\x03")
	second.finish(t, exitOK)
}

func TestChatPTYReopenNineRunsShowsOmission(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "answer-01"}})
	for i := 2; i <= 9; i++ {
		f.model.answer(1, i, reply{text: fmt.Sprintf(`{"kind":"return","payload":"answer-%02d"}`, i)})
	}
	for i := 1; i <= 9; i++ {
		if r := f.cli(nil, "run", "-goal", fmt.Sprintf("run-%02d", i)); r.code != exitOK {
			t.Fatalf("run %d: %+v", i, r)
		}
	}
	p := startPTYChat(t, f, nil)
	p.awaitStatus(t, "ready", 0)
	p.await(t, "canti history", 0)
	if screen := p.screen.String(); strings.Contains(screen, "run-01") {
		t.Fatalf("nine-run history kept the run beyond the summary bound:\n%s", screen)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
