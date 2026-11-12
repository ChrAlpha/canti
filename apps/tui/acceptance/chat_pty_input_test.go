//go:build acceptance && linux

package acceptance

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
)

// TestChatPTYFollowUpCtrlFQueuesAndContinues requires that Ctrl+F submits the
// draft as a follow-up while a run is active, and that the chat follows the
// agent log into the run the follow-up opens.
func TestChatPTYFollowUpCtrlFQueuesAndContinues(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "first answer"}})
	f.model.answer(1, 2, reply{text: `{"kind":"return","payload":"follow-up answer"}`})
	url, release := holdFirstAnswer(t, f)
	p := startPTYChat(t, f, nil, "-model-url", url)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "first request\r")
	pos = p.awaitStatus(t, "running", pos)
	p.send(t, "follow up\x06")
	pos = p.await(t, "follow-up queued", pos)
	release()
	pos = p.await(t, "first answer", pos)
	pos = p.await(t, "follow-up answer", pos)
	deadline := time.Now().Add(5 * time.Second)
	for f.status().Phase != "returned" {
		if time.Now().After(deadline) {
			t.Fatal("the follow-up run did not return")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := f.status()
	if s.Phase != "returned" || s.Run.Ordinal != 2 || s.Run.Goal != "follow up" {
		t.Fatalf("the follow-up did not open and finish the next run: %+v", s)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
	out := p.text()
	end := strings.LastIndex(out, "\x1b[?1049l")
	if end < 0 {
		t.Fatal("chat did not leave the alternate screen")
	}
	transcript := strings.ReplaceAll(ansi.Strip(out[end:]), "\r\n", "\n")
	if strings.Count(transcript, "first answer\n") != 1 || strings.Count(transcript, "follow-up answer\n") != 1 {
		t.Fatalf("follow-up line did not show each answer once: %q", transcript)
	}
	if strings.Index(transcript, "first answer\n") > strings.Index(transcript, "follow-up answer\n") {
		t.Fatalf("follow-up answers are out of order: %q", transcript)
	}
}

// TestChatPTYHeldInputCtrlRRestores requires that a stop with a queued
// follow-up shows a held notice and that Ctrl+R withdraws the held input into
// the draft.
func TestChatPTYHeldInputCtrlRRestores(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "unused"}})
	f.model.holdRequests()
	p := startPTYChat(t, f, nil)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "first request\r")
	pos = p.awaitStatus(t, "running", pos)
	p.send(t, "held restore text\x06")
	pos = p.await(t, "follow-up queued", pos)
	p.send(t, "\x03")
	pos = p.await(t, "held input", pos)
	p.send(t, "\x12")
	p.await(t, "Withdrew 1 held input(s) into the draft", pos)
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
