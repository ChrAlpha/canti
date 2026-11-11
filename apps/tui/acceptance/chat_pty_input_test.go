//go:build acceptance && linux

package acceptance

import (
	"testing"
	"time"
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
