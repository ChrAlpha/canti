package present

import (
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestItemShowsEveryRunTerminalOutcome(t *testing.T) {
	var b strings.Builder
	Item(&b, hostrpc.Item{Kind: "returned", RunID: "run_first", Result: "first answer"})
	Item(&b, hostrpc.Item{Kind: "stopped", RunID: "run_second", StopReason: "cancelled"})
	if got, want := b.String(), "first answer\ncanti: run run_second stopped: cancelled\n"; got != want {
		t.Fatalf("terminal items = %q, want %q", got, want)
	}
}

func TestResultAfterTerminalDoesNotRepeatTheAnswerOrStopLine(t *testing.T) {
	returned := chat.Result{Code: chat.OK, Run: hostrpc.Run{RunID: "run", Status: hostrpc.StatusReturned, Result: "answer"}}
	if answer, _ := ResultAfterTerminal(returned); answer != "" {
		t.Fatalf("returned answer repeated: %q", answer)
	}
	stopped := chat.Result{Code: chat.Stopped, Run: hostrpc.Run{RunID: "run", Status: hostrpc.StatusStopped, StopReason: "cancelled"}}
	if _, diagnostic := ResultAfterTerminal(stopped); strings.Contains(diagnostic, "stopped:") {
		t.Fatalf("stop line repeated: %q", diagnostic)
	}
}
