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

func TestItemShowsHandoverRecordedWithOrWithoutANote(t *testing.T) {
	for _, tc := range []struct {
		name string
		o    hostrpc.Outcome
		want string
	}{
		{"with a note", hostrpc.Outcome{Status: "done", HandoverBytes: 12}, "│ handover recorded: 12 bytes\n└ done\n"},
		{"without a note", hostrpc.Outcome{Status: "done"}, "│ handover recorded: 0 bytes\n└ done\n"},
		{"a failed handover", hostrpc.Outcome{Status: "failed", Detail: "harness: handover note is not valid UTF-8"}, "│ harness: handover note is not valid UTF-8\n└ failed\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			Item(&b, hostrpc.Item{Kind: "completed", Outcome: &tc.o})
			if got := b.String(); got != tc.want {
				t.Fatalf("item = %q, want %q", got, tc.want)
			}
			if tc.o.Status == "failed" && strings.Contains(b.String(), "handover recorded") {
				t.Fatal("a failed handover was shown as recorded")
			}
		})
	}
}
