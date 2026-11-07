package harness

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

// TestNormalizeActionReadsTheReservedNamespace pins the reading of every
// form of payload: a command outside the reserved namespace, each action
// after any leading whitespace and with any delimiter, an unknown name, a
// disabled retrieval, and a refusal under a request that requires a
// handover, deterministically.
func TestNormalizeActionReadsTheReservedNamespace(t *testing.T) {
	retrieve := func(args int) Action { return Action{Name: ActionRetrieve, Recovery: RecoveryRead, Args: args} }
	handover := func(args int) Action { return Action{Name: ActionHandover, Recovery: RecoveryLocal, Args: args} }
	unknown := func(args int) Action {
		return Action{Name: ActionUnknown, Recovery: RecoveryLocal, Args: args, Invalid: codeUnknownAction}
	}
	command := Action{Name: ActionCommand, Recovery: RecoveryExternal}
	for _, tc := range []struct {
		payload string
		want    Action
	}{
		{"go test ./...", command},
		{`canti:retrieve {"operation":"read"}`, retrieve(15)},
		{"canti:retrieve\t{}", retrieve(15)},
		{"canti:retrieve\n{}", retrieve(15)},
		{"canti:retrieve\r\n{}", retrieve(15)},
		{"canti:retrieve", retrieve(14)},
		{" \t\ncanti:retrieve {}", retrieve(18)},
		{strings.Repeat(" ", 100) + "canti:retrieve {}", retrieve(115)},
		{"canti:handover", handover(14)},
		{"canti:handover Next: report.", handover(15)},
		{"canti:handover\tNext", handover(15)},
		{"\rcanti:handover Next", handover(16)},
		{"canti:", unknown(6)},
		{"canti:retrieve{}", unknown(16)},
		{"canti:retreive {}", unknown(15)},
		{"canti:Retrieve {}", unknown(15)},
		{"canti:handover2 x", unknown(16)},
		{"CANTI:retrieve {}", command},
		{"xcanti:retrieve {}", command},
		{"echo canti:retrieve", command},
		{"# canti:discovery\nfind /canti/runs", command},
		{"\vcanti:retrieve {}", command},
		{"\u00a0canti:retrieve {}", command},
	} {
		got := NormalizeAction(tc.payload, false, true)
		if got != tc.want || NormalizeAction(tc.payload, false, true) != got {
			t.Errorf("NormalizeAction(%q) = %+v, want %+v", tc.payload, got, tc.want)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("NormalizeAction(%q) is invalid: %v", tc.payload, err)
		}
		if got.Name != ActionCommand && tc.payload[got.Args-1] != ':' && !strings.ContainsAny(tc.payload[got.Args-1:got.Args], ActionSpace) &&
			got.Args != len(tc.payload) && got.Name != ActionUnknown {
			t.Errorf("NormalizeAction(%q) places its arguments at %d", tc.payload, got.Args)
		}
		refused := NormalizeAction(tc.payload, true, true)
		if want := (got.Name != ActionHandover); (refused.Refused == codeHandoverRequired) != want || want && refused.Recovery != RecoveryLocal {
			t.Errorf("NormalizeAction(%q) under a required handover = %+v", tc.payload, refused)
		}
	}
	if got := NormalizeAction("canti:retrieve {}", false, false); got.Invalid != codeRetrievalDisabled || got.Validate() != nil {
		t.Errorf("a retrieval without a reader = %+v", got)
	}
}

// TestActionValidateAcceptsOnlyNormalizedReadings requires that Validate
// refuses every action normalization never yields.
func TestActionValidateAcceptsOnlyNormalizedReadings(t *testing.T) {
	for _, a := range []Action{
		{},
		{Name: ActionLegacy},
		{Name: ActionCommand, Recovery: RecoveryLocal},
		{Name: ActionCommand, Recovery: RecoveryExternal, Args: 3},
		{Name: ActionCommand, Recovery: RecoveryExternal, Invalid: codeUnknownAction},
		{Name: ActionUnknown, Recovery: RecoveryLocal, Args: 6, Invalid: unknownAction},
		{Name: ActionRetrieve, Recovery: RecoveryLocal, Args: 15},
		{Name: ActionRetrieve, Recovery: RecoveryRead},
		{Name: ActionRetrieve, Recovery: RecoveryRead, Args: 15, Invalid: "decode failed"},
		{Name: ActionRetrieve, Recovery: RecoveryRead, Args: 15, Refused: codeHandoverRequired},
		{Name: ActionRetrieve, Recovery: RecoveryLocal, Args: 15, Refused: handoverRequired},
		{Name: ActionHandover, Recovery: RecoveryLocal, Args: 15, Refused: codeHandoverRequired},
		{Name: ActionHandover, Recovery: RecoveryLocal, Args: 15, Invalid: codeRetrievalDisabled},
		{Name: ActionUnknown, Recovery: RecoveryLocal, Args: 6},
		{Name: ActionUnknown, Recovery: RecoveryLocal, Args: -1, Invalid: codeUnknownAction},
		{Name: ActionCommand, Recovery: RecoveryLocal, Refused: "another refusal"},
	} {
		if err := a.Validate(); err == nil {
			t.Errorf("Validate(%+v) accepted it", a)
		}
	}
}

// TestActionEncodingIsBounded requires that every valid action, at the
// largest offset, encodes within MaxActionBytes: its strings come from a
// closed set of ASCII texts that JSON does not escape.
func TestActionEncodingIsBounded(t *testing.T) {
	largest := 0
	for _, name := range []ActionName{ActionCommand, ActionRetrieve, ActionHandover, ActionUnknown} {
		for _, recovery := range []Recovery{RecoveryExternal, RecoveryRead, RecoveryLocal} {
			for _, invalid := range []string{"", codeRetrievalDisabled, codeUnknownAction} {
				for _, refused := range []string{"", codeHandoverRequired} {
					for _, args := range []int{0, math.MaxInt} {
						a := Action{Name: name, Recovery: recovery, Args: args, Invalid: invalid, Refused: refused}
						if a.Validate() != nil {
							continue
						}
						data, err := json.Marshal(a)
						if err != nil {
							t.Fatal(err)
						}
						largest = max(largest, len(data))
					}
				}
			}
		}
	}
	if largest == 0 || largest > MaxActionBytes {
		t.Fatalf("the largest valid action encodes in %d bytes, bound %d", largest, MaxActionBytes)
	}
}

// TestOutcomeDetailEncodingIsBounded requires that a failure's detail, of
// any length and content, is clipped to maxDetailBytes, and that its JSON
// encoding, which escapes a control byte, an angle bracket, or an
// ampersand as six bytes, stays within six times that.
func TestOutcomeDetailEncodingIsBounded(t *testing.T) {
	for _, filler := range []string{"\x01", "<", "&", "\"", "\\", " ", "界"} {
		o := failed(errors.New(strings.Repeat(filler, 1<<20)))
		if err := o.Validate(); err != nil {
			t.Fatalf("%q: %v", filler, err)
		}
		data, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		if len(o.Detail) > maxDetailBytes || len(data) > 6*maxDetailBytes+64 {
			t.Fatalf("%q: detail %d bytes encodes in %d", filler, len(o.Detail), len(data))
		}
	}
}

// TestOutcomePresentationIsTheVersion12Observation requires that each local
// outcome presents as the observation context version 12 recorded for the
// same action, and that no other step changes its observation.
func TestOutcomePresentationIsTheVersion12Observation(t *testing.T) {
	zero, one := 0, 1
	read := &archive.Retrieval{Query: archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/a", Reason: "check"},
		Sources: []archive.SourceRange{{Path: "/canti/imports/a", Length: 3, SHA256: strings.Repeat("a", 64)}}, Evidence: []string{"abc"}}
	refs := *read
	refs.Evidence = nil
	encoded, _ := json.Marshal(refs)
	broken := &archive.Retrieval{Query: read.Query, Error: "no such file"}
	brokenJSON, _ := json.Marshal(broken)
	h := &Handover{RunID: "run-1", FirstStep: 1, Text: "next"}
	for _, tc := range []struct {
		o    Outcome
		want Observation
	}{
		{Outcome{Status: OutcomeDone, Retrieval: read}, Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: string(encoded), Retrieval: read}},
		{Outcome{Status: OutcomeFailed, Retrieval: broken}, Observation{Execution: core.ExecutionExited, ExitCode: &one, Preview: string(brokenJSON), Retrieval: broken}},
		{Outcome{Status: OutcomeFailed, Detail: "invalid character"}, Observation{Execution: core.ExecutionExited, ExitCode: &one, Preview: "Context action failed: invalid character"}},
		{Outcome{Status: OutcomeDone, Handover: h}, Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: handoverNotice, Handover: h}},
		{Outcome{Status: OutcomeRefused, Detail: handoverRequired}, Observation{Execution: core.ExecutionFailedToStart, Preview: "Context action failed: " + handoverRequired}},
	} {
		o := tc.o
		got := StepRecord{Outcome: &o}.presented()
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(tc.want)
		if string(gotJSON) != string(wantJSON) {
			t.Errorf("%+v presents as %s, want %s", tc.o, gotJSON, wantJSON)
		}
	}
	obs := Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: "ok"}
	if got := (StepRecord{Observation: obs}).presented(); got.Preview != "ok" || got.ExitCode != &zero {
		t.Errorf("an observation presents as %+v", got)
	}
}

// TestAcceptanceIsPositivelyLegacyOrRecorded requires that only an
// acceptance before ActionSemantics without an action is legacy, that one of
// ActionSemantics or later needs a valid action, and that ActionSemantics is
// the version that added Performed.
func TestAcceptanceIsPositivelyLegacyOrRecorded(t *testing.T) {
	if core.CompatibleEvent(ActionSemantics, core.Performed{}) != nil || core.CompatibleEvent(ActionSemantics-1, core.Performed{}) == nil {
		t.Fatal("ActionSemantics is not the version that added Performed")
	}
	command := Action{Name: ActionCommand, Recovery: RecoveryExternal}
	for _, tc := range []struct {
		semantics int
		action    *Action
		want      Provenance
	}{
		{1, nil, ProvenanceLegacy}, {3, nil, ProvenanceLegacy}, {4, &command, ProvenanceRecorded},
		{3, &command, ""}, {4, nil, ""}, {4, &Action{Name: ActionCommand}, ""}, {0, nil, ""}, {core.SemanticsVersion + 1, &command, ""},
	} {
		a, err := Acceptance(tc.semantics, tc.action)
		if (err == nil) != (tc.want != "") || a.Provenance != tc.want {
			t.Errorf("Acceptance(%d, %+v) = %+v, %v; want %q", tc.semantics, tc.action, a, err, tc.want)
		}
		if tc.want == ProvenanceLegacy && (a.Action != Action{Name: ActionLegacy} || a.Local()) {
			t.Errorf("legacy acceptance %+v asserts an intent", a)
		}
	}
	if !LegacyInProcess("effect-1", "read_effect-1") || LegacyInProcess("effect-1", "read_effect-2") || LegacyInProcess("effect-1", "") {
		t.Fatal("LegacyInProcess misreads a job")
	}
}

// TestCheckEffectProvesEachCompletionFromItsAcceptance runs every rule of
// CheckEffect, each transition once agreeing with its acceptance and source
// window and once contradicting them.
func TestCheckEffectProvesEachCompletionFromItsAcceptance(t *testing.T) {
	state := func(phase core.Phase, payload, job string) core.State {
		return core.State{RunID: "run-1", Step: 2, Attempt: 1, RequestRev: 1, Phase: phase, Limits: core.Limits{MaxSteps: 5, MaxAttempts: 3},
			Decision: core.Decision{Kind: core.Exec, Payload: payload}, EffectID: "effect-2", JobID: job}
	}
	recorded := func(payload string, requested bool) Accepted {
		return Accepted{Semantics: core.SemanticsVersion, Provenance: ProvenanceRecorded, Action: NormalizeAction(payload, requested, true)}
	}
	legacy := Accepted{Semantics: 3, Provenance: ProvenanceLegacy, Action: Action{Name: ActionLegacy}}
	requested := &Window{FirstStep: 1, Requested: true}
	open := &Window{FirstStep: 1}
	text := "canti:handover Next: report."
	opened := &Window{FirstStep: 3, SourceStep: 2, Handover: "Next: report."}
	failedWindow := &Window{FirstStep: 1, Requested: true, Failures: 1}
	done := &Outcome{Status: OutcomeDone, Handover: &Handover{RunID: "run-1", FirstStep: 1, Text: "Next: report."}}
	read := &Outcome{Status: OutcomeDone, Retrieval: &archive.Retrieval{}}
	performed := func(payload string, o *Outcome, w *Window) Transition {
		return Transition{From: state(core.PhaseEffectSaved, payload, ""), Event: core.Performed{EffectID: "effect-2"}, Facts: Facts{Outcome: o, Window: w}}
	}
	bound := Transition{From: state(core.PhaseEffectSaved, "make", ""), Event: core.JobBound{EffectID: "effect-2", JobID: "job"}}
	jobless := Transition{From: state(core.PhaseEffectSaved, "make", ""), Event: core.Observed{EffectID: "effect-2", Execution: core.ExecutionFailedToStart}}
	cancelled := Transition{From: state(core.PhaseCancelling, "canti:retrieve {}", ""), Event: core.Observed{EffectID: "effect-2", Execution: core.ExecutionCancelled}}
	accepting := func(payload string, a Action) Transition {
		return Transition{To: state(core.PhaseEffectSaved, payload, ""), Event: core.ResponseLoaded{Step: 2, Attempt: 1, Body: []byte("{}")}, Facts: Facts{Action: &a}}
	}
	for _, tc := range []struct {
		name     string
		t        Transition
		accepted Accepted
		source   *Window
		ok       bool
	}{
		{"acceptance as normalized", accepting("make", NormalizeAction("make", false, true)), Accepted{}, open, true},
		{"acceptance read without a reader", accepting("canti:retrieve {}", NormalizeAction("canti:retrieve {}", false, false)), Accepted{}, open, true},
		{"acceptance ignoring a required handover", accepting("make", NormalizeAction("make", false, true)), Accepted{}, requested, false},
		{"acceptance of another reading", accepting("make", NormalizeAction("canti:x", false, true)), Accepted{}, open, false},
		{"handover opening its window", performed(text, done, opened), recorded(text, true), requested, true},
		{"handover without a window", performed(text, done, nil), recorded(text, true), requested, false},
		{"handover opening another text", performed(text, done, &Window{FirstStep: 3, SourceStep: 2, Handover: "x"}), recorded(text, true), requested, false},
		{"handover proposing another text", performed(text, &Outcome{Status: OutcomeDone, Handover: &Handover{RunID: "run-1", FirstStep: 1, Text: "x"}}, opened), recorded(text, true), requested, false},
		{"handover unrequested but done", performed(text, done, opened), recorded(text, false), open, false},
		{"handover failing in its window", performed(text, &Outcome{Status: OutcomeFailed, Detail: "too long"}, failedWindow), recorded(text, true), requested, true},
		{"handover failing without a failure", performed(text, &Outcome{Status: OutcomeFailed, Detail: "too long"}, nil), recorded(text, true), requested, false},
		{"unrequested handover failing", performed(text, &Outcome{Status: OutcomeFailed, Detail: "not requested"}, nil), recorded(text, false), open, true},
		{"refusal counting a failure", performed("make", &Outcome{Status: OutcomeRefused, Detail: handoverRequired}, failedWindow), recorded("make", true), requested, true},
		{"refusal as a failure", performed("make", &Outcome{Status: OutcomeFailed, Detail: handoverRequired}, failedWindow), recorded("make", true), requested, false},
		{"read", performed("canti:retrieve {}", read, nil), recorded("canti:retrieve {}", false), open, true},
		{"read opening a window", performed("canti:retrieve {}", read, opened), recorded("canti:retrieve {}", false), open, false},
		{"read with a handover", performed("canti:retrieve {}", done, nil), recorded("canti:retrieve {}", false), open, false},
		{"unknown action failing", performed("canti:x", &Outcome{Status: OutcomeFailed, Detail: unknownAction}, nil), recorded("canti:x", false), open, true},
		{"unknown action done", performed("canti:x", read, nil), recorded("canti:x", false), open, false},
		{"command performed", performed("make", read, nil), recorded("make", false), open, false},
		{"legacy performed", performed("canti:retrieve {}", read, nil), legacy, open, false},
		{"performed without outcome", performed("canti:x", nil, nil), recorded("canti:x", false), open, false},
		{"command bound", bound, recorded("make", false), open, true},
		{"local action bound", bound, recorded("canti:retrieve {}", false), open, false},
		{"legacy bound", bound, legacy, open, true},
		{"command failing jobless", jobless, recorded("make", false), open, false},
		{"legacy failing jobless", jobless, legacy, open, true},
		{"local action cancelled", cancelled, recorded("canti:retrieve {}", false), open, true},
		{"command cancelled jobless", cancelled, recorded("make", false), open, true},
	} {
		if err := CheckEffect(tc.t, tc.accepted, tc.source); (err == nil) != tc.ok {
			t.Errorf("%s: CheckEffect = %v, want ok %v", tc.name, err, tc.ok)
		}
	}
	if !EffectTransition(bound) || EffectTransition(Transition{Event: core.StopRequested{Reason: core.StopCancelled}, From: bound.From}) {
		t.Fatal("EffectTransition misclassifies a bind or a stop")
	}
}

// TestDiscoverySkipsExactlyTheHarnessActions requires, for every payload of
// the parity table, that discovery with the exclusions the loop configures
// skips the step that published it exactly when normalization reads it as
// a harness action or it is a marked discovery command.
func TestDiscoverySkipsExactlyTheHarnessActions(t *testing.T) {
	payloads := []string{
		`canti:retrieve {"operation":"read"}`, "canti:retrieve\t{}", "canti:retrieve\n{}", "canti:retrieve\r\n{}", "canti:retrieve",
		" \t\ncanti:retrieve {}", strings.Repeat(" ", 100) + "canti:retrieve {}",
		"canti:handover", "canti:handover text", "canti:handover\ttext", "\rcanti:handover text",
		"canti:", "canti:retrieve{}", "canti:retreive {}", "canti:Retrieve {}", "canti:handover2 x",
		"# canti:discovery\nfind /canti/runs", "  # canti:discovery\nfind /canti/runs",
		"CANTI:retrieve {}", "xcanti:retrieve", "echo canti:retrieve", "#canti:discovery", "\vcanti:retrieve", "\u00a0canti:retrieve",
		strings.Repeat(" ", archive.MaxBytes) + "canti:retrieve {}", strings.Repeat(" ", archive.MaxBytes) + "ls",
	}
	// Discovery cannot classify a command whose leading whitespace fills
	// MaxBytes; it skips it and reports the scan incomplete.
	unclassified := map[int]bool{len(payloads) - 1: true, len(payloads): true}
	history := t.TempDir()
	cfg, err := normalize(LoopConfig{Environment: "test", Reader: &archive.Reader{History: history}})
	if err != nil {
		t.Fatal(err)
	}
	steps := filepath.Join(history, "run-1", archive.StepsDir)
	zero := 0
	for i, p := range payloads {
		dir := filepath.Join(steps, archive.StepDir(i+1))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		r := StepRecord{Step: i + 1, Command: p, Observation: Observation{Execution: core.ExecutionExited, ExitCode: &zero, Preview: fmt.Sprintf("needle %d.", i+1)}}
		if err := writeStep(dir, r); err != nil {
			t.Fatal(err)
		}
	}
	result, err := cfg.Reader.Retrieve(t.Context(), archive.RetrievalQuery{Operation: "discover", Path: archive.RunsRoot + "/run-1/steps", Query: "needle", Reason: "parity"})
	if err != nil || result.Error != "" || result.More || !result.Incomplete || result.SkippedCount != len(unclassified) {
		t.Fatalf("discover: %+v, %v", result, err)
	}
	for _, skip := range result.Skipped {
		if !unclassified[skip.Step] || skip.Reason != archive.SkipUnclassified {
			t.Fatalf("skipped %+v, want only the unclassified steps", skip)
		}
	}
	scanned := map[int]bool{}
	for _, s := range result.Sources {
		var step int
		if _, err := fmt.Sscanf(strings.TrimPrefix(s.Path, archive.RunsRoot+"/run-1/steps/"), "%08d/", &step); err == nil {
			scanned[step] = true
		}
	}
	for i, p := range payloads {
		reserved := NormalizeAction(p, false, true).Name != ActionCommand
		marked := strings.HasPrefix(strings.TrimLeft(p, ActionSpace), DiscoveryMarker)
		if skipped := !scanned[i+1]; skipped != (reserved || marked || unclassified[i+1]) {
			t.Errorf("payload %q: reserved %v, marked %v, skipped %v", p, reserved, marked, skipped)
		}
	}
}

// TestCheckEffectIsTotalOverValidOutcomes requires that CheckEffect returns,
// never panics, for every valid action crossed with every valid outcome
// shape, window, and source window, as the integrity gate of every writer.
func TestCheckEffectIsTotalOverValidOutcomes(t *testing.T) {
	payloads := []string{"make", "canti:retrieve {}", "canti:handover next", "canti:x"}
	h := &Handover{RunID: "run-1", FirstStep: 1, Text: "next"}
	r := &archive.Retrieval{}
	broken := &archive.Retrieval{Error: "missing"}
	outcomes := []Outcome{
		{Status: OutcomeDone, Retrieval: r}, {Status: OutcomeDone, Handover: h},
		{Status: OutcomeFailed, Retrieval: broken}, {Status: OutcomeFailed, Detail: "x"},
		{Status: OutcomeRefused, Detail: handoverRequired},
	}
	windows := []*Window{nil, {FirstStep: 3, SourceStep: 2, Handover: "next"}, {FirstStep: 1, Requested: true, Failures: 1}}
	for _, p := range payloads {
		for _, requested := range []bool{false, true} {
			for _, reader := range []bool{false, true} {
				a := NormalizeAction(p, requested, reader)
				accepted := Accepted{Semantics: ActionSemantics, Provenance: ProvenanceRecorded, Action: a}
				for _, o := range outcomes {
					if o.Validate() != nil {
						t.Fatalf("outcome %+v is not valid", o)
					}
					for _, w := range windows {
						for _, source := range []*Window{nil, {FirstStep: 1}, {FirstStep: 1, Requested: true}} {
							o := o
							tr := Transition{Event: core.Performed{EffectID: "effect-2"}, Facts: Facts{Outcome: &o, Window: w},
								From: core.State{RunID: "run-1", Step: 2, Phase: core.PhaseEffectSaved, Decision: core.Decision{Kind: core.Exec, Payload: p}, EffectID: "effect-2"}}
							_ = CheckEffect(tr, accepted, source)
						}
					}
				}
			}
		}
	}
}

// TestActionMatchesItsPayloadByStableRules requires that a recorded action
// matches its decision when it is a command exactly outside the namespace,
// places its arguments after the word, and names the word of a retrieval or
// a handover, and that an unknown action matches any word of the namespace,
// including one a later version learns.
func TestActionMatchesItsPayloadByStableRules(t *testing.T) {
	retrieve := NormalizeAction("canti:retrieve {}", false, true)
	for _, tc := range []struct {
		a       Action
		payload string
		ok      bool
	}{
		{NormalizeAction("make", false, true), "make", true},
		{NormalizeAction("make", true, true), "make", true},
		{retrieve, "canti:retrieve {}", true},
		{retrieve, "\tcanti:retrieve {}", false},
		{retrieve, "canti:handover {}", false},
		{Action{Name: ActionCommand, Recovery: RecoveryExternal}, "canti:retrieve {}", false},
		{Action{Name: ActionCommand, Recovery: RecoveryExternal}, " canti:x", false},
		{NormalizeAction("canti:x", false, true), "make", false},
		{Action{Name: ActionUnknown, Recovery: RecoveryLocal, Args: len("canti:handover-carry "), Invalid: codeUnknownAction}, "canti:handover-carry text", true},
		{Action{Name: ActionUnknown, Recovery: RecoveryLocal, Args: len(RetrievePrefix), Invalid: codeUnknownAction}, "canti:retrieve {}", true},
		{Action{Name: ActionRetrieve, Recovery: RecoveryRead, Args: 99}, "canti:retrieve {}", false},
	} {
		if err := tc.a.Matches(tc.payload); (err == nil) != tc.ok {
			t.Errorf("%+v.Matches(%q) = %v, want ok %v", tc.a, tc.payload, err, tc.ok)
		}
	}
}

// TestActionProblemsAreStableCodes requires that an action records codes,
// whose texts its outcome shows, so that rewording a text changes no
// recorded action.
func TestActionProblemsAreStableCodes(t *testing.T) {
	for _, tc := range []struct {
		a          Action
		code, text string
	}{
		{NormalizeAction("canti:retrieve {}", false, false), "retrieval_disabled", "retrieval is disabled for this run"},
		{NormalizeAction("canti:x", false, true), "unknown_action", unknownAction},
		{NormalizeAction("make", true, true), "handover_required", handoverRequired},
	} {
		if code := cmp.Or(tc.a.Refused, tc.a.Invalid); code != tc.code || tc.a.Problem() != tc.text {
			t.Errorf("%+v records %q with %q, want %q with %q", tc.a, code, tc.a.Problem(), tc.code, tc.text)
		}
	}
}
