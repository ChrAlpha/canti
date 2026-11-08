//go:build linux

package main

import (
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/charmbracelet/x/ansi"
)

func TestChatPTYExitPreservesCompleteTranscript(t *testing.T) {
	answer := "answer-start\n" + strings.Repeat("0123456789abcdef0123456789abcdef\n", 200) + "answer-complete"
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: answer}})
	p := startPTYChat(t, f, nil)
	pos := p.await(t, "ready", 0)
	p.send(t, "keep the complete answer\r")
	p.await(t, "answer-complete", pos)
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 1 {
		t.Fatalf("answer did not return: %+v", s)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
	p.await(t, "new runs use the current launch flags.", 0)
	out := p.text()
	end := strings.LastIndex(out, "\x1b[?1049l")
	if end < 0 {
		t.Fatal("chat did not leave the alternate screen")
	}
	transcript := strings.ReplaceAll(ansi.Strip(out[end:]), "\r\n", "\n")
	if !strings.Contains(transcript, answer+"\n") {
		t.Fatalf("complete answer missing after alternate screen exit: transcript bytes=%d, tail=%q", len(transcript), transcript[max(0, len(transcript)-200):])
	}
}
