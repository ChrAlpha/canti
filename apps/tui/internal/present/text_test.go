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

func TestInputShowsOnlyPrincipalInputsWithTheirDelivery(t *testing.T) {
	var b strings.Builder
	Input(&b, hostrpc.Input{Authority: "principal", Text: "goal"})
	Input(&b, hostrpc.Input{Authority: "principal", Delivery: "steer", Text: "steer text"})
	Input(&b, hostrpc.Input{Authority: "principal", Delivery: "follow_up", Text: "later", Truncated: true})
	Input(&b, hostrpc.Input{Authority: "observation", Text: "evidence"})
	if got, want := b.String(), "› goal\n› steer text\n» later …\n"; got != want {
		t.Fatalf("inputs = %q, want %q", got, want)
	}
}

func TestWithdrawalStatesTheTotalAndOmittedNames(t *testing.T) {
	var b strings.Builder
	Item(&b, hostrpc.Item{Kind: "withdrawn", Withdrawn: []string{"in_1", "pos:5"}, WithdrawnTotal: 70, Truncated: true})
	if got, want := b.String(), "↩ withdrew 2+ input(s) (names omitted): in_1, pos:5\n"; got != want {
		t.Fatalf("withdrawal = %q, want %q", got, want)
	}
	b.Reset()
	Item(&b, hostrpc.Item{Kind: "withdrawn", Withdrawn: []string{"in_1"}})
	if got, want := b.String(), "↩ withdrew 1 input(s): in_1\n"; got != want {
		t.Fatalf("small withdrawal = %q, want %q", got, want)
	}
}

func TestItemShowsADecidedStepsPurposeBeforeItsCommand(t *testing.T) {
	var b strings.Builder
	Item(&b, hostrpc.Item{Kind: "decided", Purpose: "Inspect the build\nbefore changing it", Command: "go test ./..."})
	if got, want := b.String(), "# Inspect the build\n# before changing it\n$ go test ./...\n"; got != want {
		t.Fatalf("decided item = %q, want %q", got, want)
	}
	b.Reset()
	Item(&b, hostrpc.Item{Kind: "decided", Command: "true"})
	if got, want := b.String(), "$ true\n"; got != want {
		t.Fatalf("purpose-free item = %q, want %q", got, want)
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
