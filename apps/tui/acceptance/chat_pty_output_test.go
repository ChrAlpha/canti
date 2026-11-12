//go:build acceptance && linux

package acceptance

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestChatPTYExitPreservesCompleteTranscript(t *testing.T) {
	answer := "answer-start\n" + strings.Repeat("0123456789abcdef0123456789abcdef\n", 200) + "answer-complete"
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: answer}})
	p := startPTYChat(t, f, nil)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "keep the complete answer\r")
	p.await(t, "answer-complete", pos)
	if s := f.status(); s.Phase != "returned" || s.Run.Ordinal != 1 {
		t.Fatalf("answer did not return: %+v", s)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
	p.awaitFinalOutput(t, "the current launch flags apply to inputs that open a run.")
	out := p.text()
	end := strings.LastIndex(out, "\x1b[?1049l")
	if end < 0 {
		t.Fatal("chat did not leave the alternate screen")
	}
	transcript := strings.ReplaceAll(ansi.Strip(out[end:]), "\r\n", "\n")
	if strings.Count(transcript, answer+"\n") != 1 {
		t.Fatalf("complete answer is not shown exactly once after alternate screen exit: transcript bytes=%d, tail=%q", len(transcript), transcript[max(0, len(transcript)-200):])
	}
}
