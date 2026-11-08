package core_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

var (
	purposeBody       = []byte(`{"kind":"exec","purpose":"check","payload":"printf ok"}`)
	returnPurposeBody = []byte(`{"kind":"return","purpose":"explain","payload":"done"}`)
	blankBody         = []byte(`{"kind":"exec","payload":" "}`)
)

var recordSemantics = flag.Bool("record-semantics", false, "record the transitions of a semantics version that has none")

// candidates returns every event a harness could report from s: the
// identities s holds, a next attempt, each decision body, execution, and
// reportable stop reason.
func candidates(s core.State) []core.Event {
	out := []core.Event{core.RequestFrozen{}}
	for _, at := range [][2]int{{s.Step, s.Attempt}, {s.Step, s.Attempt + 1}, {s.Step + 1, s.Attempt}} {
		out = append(out, core.AttemptStarted{Step: at[0], Attempt: at[1]}, core.ResponseSaved{Step: at[0], Attempt: at[1]},
			core.ResponseIncomplete{Step: at[0], Attempt: at[1]}, core.RequestRejected{Step: at[0], Attempt: at[1]}, core.ResponseLoaded{Step: at[0], Attempt: at[1], Body: execBody})
	}
	for _, body := range [][]byte{returnBody, invalidBody, purposeBody, returnPurposeBody, blankBody} {
		out = append(out, core.ResponseLoaded{Step: s.Step, Attempt: s.Attempt, Body: body})
	}
	for _, effect := range []string{s.EffectID, "other"} {
		out = append(out, core.EffectRecovered{EffectID: effect}, core.OutcomeUnknown{EffectID: effect},
			core.Performed{EffectID: effect})
	}
	out = append(out, core.Observed{EffectID: "other", Execution: core.ExecutionExited}, core.Resolved{EffectID: "other", Execution: core.ExecutionExited})
	for _, e := range []core.ExecutionState{core.ExecutionExited, core.ExecutionTimedOut, core.ExecutionCancelled, core.ExecutionFailedToStart, core.ExecutionUnknown} {
		out = append(out, core.Observed{EffectID: s.EffectID, Execution: e}, core.Resolved{EffectID: s.EffectID, Execution: e})
	}
	for _, r := range []core.StopReason{core.StopCancelled, core.StopTimedOut, core.StopBudgetExhausted, core.StopUnrecoverable} {
		out = append(out, core.StopRequested{Reason: r})
	}
	return out
}

func label(e core.Event) string {
	switch e := e.(type) {
	case core.ResponseLoaded:
		switch string(e.Body) {
		case string(execBody):
			return "response_loaded(exec)"
		case string(returnBody):
			return "response_loaded(return)"
		case string(purposeBody):
			return "response_loaded(exec with purpose)"
		case string(returnPurposeBody):
			return "response_loaded(return with purpose)"
		case string(blankBody):
			return "response_loaded(blank exec)"
		}
		return "response_loaded(invalid)"
	case core.Observed:
		return "observed(" + string(e.Execution) + ")"
	case core.Resolved:
		return "resolved(" + string(e.Execution) + ")"
	case core.StopRequested:
		return "stop_requested(" + string(e.Reason) + ")"
	}
	return e.Kind()
}

// reachable explores every state Next reaches from a new run under limits,
// following at most depth events, and returns the transitions it took,
// labelled, and the digest of every event it tried in full: its state and
// intent, the event, and the next state and intent, or whether Next refused
// it as illegal or invalid.
func reachable(t *testing.T, limits core.Limits, depth int) (map[string]bool, string) {
	t.Helper()
	start, err := core.NewState("run-1", limits)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[core.State]int{start: 0}
	table := map[string]bool{}
	var full []string
	for queue := []core.State{start}; len(queue) > 0; queue = queue[1:] {
		s := queue[0]
		if err := s.Validate(); err != nil {
			t.Fatalf("limits %+v reach an invalid state %+v: %v", limits, s, err)
		}
		if seen[s] == depth {
			continue
		}
		for _, e := range candidates(s) {
			next, err := s.Next(e)
			from, _ := json.Marshal(s)
			event, _ := json.Marshal(e)
			if err != nil {
				if !errors.Is(err, core.ErrIllegalTransition) && !errors.Is(err, core.ErrInvalidEvent) {
					t.Fatalf("Next(%+v, %+v) = %v", s, e, err)
				}
				if next != s {
					t.Fatalf("Next(%+v, %+v) failed and changed the state to %+v", s, e, next)
				}
				full = append(full, fmt.Sprintf("%s %s %s %s refused: illegal %v", from, s.Intent(), e.Kind(), event, errors.Is(err, core.ErrIllegalTransition)))
				continue
			}
			line := fmt.Sprintf("%s %s -> %s", s.Phase, label(e), next.Phase)
			if next.Reason != "" {
				line += " " + string(next.Reason)
			}
			table[line] = true
			to, _ := json.Marshal(next)
			full = append(full, fmt.Sprintf("%s %s %s %s -> %s %s", from, s.Intent(), e.Kind(), event, to, next.Intent()))
			if _, ok := seen[next]; !ok {
				seen[next] = seen[s] + 1
				queue = append(queue, next)
			}
		}
	}
	slices.Sort(full)
	sum := sha256.Sum256([]byte(strings.Join(full, "\n")))
	return table, fmt.Sprintf("%d states, %d events tried, sha256 %x", len(seen), len(full), sum)
}

// TestReachableStatesAreValidAndPinTheirSemantics requires that every state
// a run reaches from NewState under small, many-attempt, and unlimited limits
// passes Validate, that the explored legal pairs are exactly the transition
// table's, and that the transitions, in full per limit set and as a labelled
// table, are those recorded for SemanticsVersion: changing any requires a new
// semantics version, recorded once with -record-semantics.
func TestReachableStatesAreValidAndPinTheirSemantics(t *testing.T) {
	table, digests := map[string]bool{}, []string{}
	for _, limits := range []core.Limits{{MaxSteps: 1, MaxAttempts: 1}, {MaxSteps: 2, MaxAttempts: 2}, {MaxSteps: 3, MaxAttempts: 3},
		{MaxSteps: 1, MaxAttempts: 5}, {MaxSteps: 0, MaxAttempts: 2}} {
		lines, digest := reachable(t, limits, 40)
		for line := range lines {
			table[line] = true
		}
		digests = append(digests, fmt.Sprintf("limits %+v, depth 40: %s", limits, digest))
	}
	pairs := map[string]bool{}
	for line := range table {
		fields := strings.Fields(line)
		pairs[fields[0]+" "+strings.Split(fields[1], "(")[0]] = true
	}
	want := map[string]bool{}
	for _, tt := range legalTransitions() {
		want[string(tt.from.Phase)+" "+tt.event.Kind()] = true
	}
	if len(want) != legalPairs || !reflect.DeepEqual(pairs, want) {
		t.Errorf("explored pairs %d differ from the %d of the transition table:\n%v\n%v", len(pairs), len(want), sorted(pairs), sorted(want))
	}
	got := fmt.Sprintf("semantics version %d\n%s\n%s\n", core.SemanticsVersion, strings.Join(digests, "\n"), strings.Join(sorted(table), "\n"))
	file := filepath.Join("testdata", fmt.Sprintf("semantics-v%d.txt", core.SemanticsVersion))
	recorded, err := os.ReadFile(file)
	switch {
	case *recordSemantics && errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	case err != nil:
		t.Fatalf("semantics version %d has no recorded transitions: %v", core.SemanticsVersion, err)
	}
	if got != string(recorded) {
		t.Fatalf("the transitions are not those of semantics version %d; bump SemanticsVersion and record its transitions with -record-semantics.\ngot:\n%s", core.SemanticsVersion, got)
	}
}

func sorted(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}
