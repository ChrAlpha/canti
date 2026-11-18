package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

// TestNormalizeActionReadsTheReservedNamespace pins the reading of every
// form of payload: a command outside the reserved namespace, each action
// after any leading whitespace and with any delimiter, an unknown name, a
// disabled retrieval, and a refusal under a request that requires a
// handover, deterministically.
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
func TestAcceptanceRequiresCurrentRecordedIdentity(t *testing.T) {
	command := Action{Name: ActionCommand, Recovery: RecoveryExternal}
	local := NormalizeAction("canti:x", true)
	execution := &ExecutionIdentity{Spec: ConfigDigest([]byte("command")), ConfigDigest: ConfigDigest(nil)}
	for _, tc := range []struct {
		version   int
		action    *Action
		execution *ExecutionIdentity
		ok        bool
	}{
		{core.SemanticsVersion, &command, execution, true}, {core.SemanticsVersion, &local, nil, true},
		{core.SemanticsVersion - 1, &command, execution, false}, {core.SemanticsVersion + 1, &command, execution, false},
		{core.SemanticsVersion, nil, nil, false}, {core.SemanticsVersion, &command, nil, false},
		{core.SemanticsVersion, &command, &ExecutionIdentity{Spec: "invalid", ConfigDigest: ConfigDigest(nil)}, false},
		{core.SemanticsVersion, &local, execution, false},
	} {
		a, err := Acceptance(tc.version, tc.action, tc.execution)
		if (err == nil) != tc.ok {
			t.Errorf("Acceptance(%+v)=%+v,%v", tc, a, err)
		}
	}
}

// TestCheckEffectProvesEachCompletionFromItsAcceptance runs every rule of
// CheckEffect, each transition once agreeing with its acceptance and source
// window and once contradicting them.
func TestDiscoverySkipsExactlyTheHarnessActions(t *testing.T) {
	payloads := []string{
		`canti:retrieve {"operation":"read"}`, "canti:retrieve\t{}", "canti:retrieve\n{}", "canti:retrieve\r\n{}", "canti:retrieve",
		" \t\ncanti:retrieve {}", strings.Repeat(" ", 100) + "canti:retrieve {}",
		"canti:handover", "canti:handover text", "canti:handover\ttext", "\rcanti:handover text",
		`canti:handover-carry {"text":"x","carry":[{"step":1}]}`, "canti:handover-carry\n{}",
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
		reserved := NormalizeAction(p, true).Name != ActionCommand
		marked := strings.HasPrefix(strings.TrimLeft(p, ActionSpace), DiscoveryMarker)
		if skipped := !scanned[i+1]; skipped != (reserved || marked || unclassified[i+1]) {
			t.Errorf("payload %q: reserved %v, marked %v, skipped %v", p, reserved, marked, skipped)
		}
	}
}

// TestCheckEffectIsTotalOverValidOutcomes requires that CheckEffect returns,
// never panics, for every valid action crossed with every valid outcome
// shape, window, and source window, as the integrity gate of every writer.
func TestActionMatchesItsPayloadByStableRules(t *testing.T) {
	retrieve := NormalizeAction("canti:retrieve {}", true)
	for _, tc := range []struct {
		a       Action
		payload string
		ok      bool
	}{
		{NormalizeAction("make", true), "make", true},
		{NormalizeAction("make", true), "make", true},
		{retrieve, "canti:retrieve {}", true},
		{retrieve, "\tcanti:retrieve {}", false},
		{retrieve, "canti:handover {}", false},
		{Action{Name: ActionCommand, Recovery: RecoveryExternal}, "canti:retrieve {}", false},
		{Action{Name: ActionCommand, Recovery: RecoveryExternal}, " canti:x", false},
		{NormalizeAction("canti:x", true), "make", false},
		{NormalizeAction("canti:handover text", true), "canti:handover text", true},
		{NormalizeAction("canti:handover text", true), "canti:handover", false},
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

func TestNormalizeActionReadsTheReservedNamespace(t *testing.T) {
	for _, payload := range []string{"go test ./...", "echo canti:focus", "CANTI:focus {}", "# canti:discovery\nfind /canti/runs", "canti:focus {\"sources\":[]}", " canti:retrieve {}", "canti:handover next", "canti:handover\nnext", "canti:handover-carry {}", "canti:unknown", "canti:retrieve{}"} {
		a := NormalizeAction(payload, true)
		if err := a.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := a.Matches(payload); err != nil {
			t.Fatal(err)
		}
		reserved := strings.HasPrefix(strings.TrimLeft(payload, ActionSpace), ActionNamespace)
		if reserved == (a.Recovery == RecoveryExternal) {
			t.Fatalf("namespace escaped: %q %+v", payload, a)
		}
		if strings.Contains(payload, "canti:handover") && a.Name != ActionHandover && (a.Name != ActionUnknown || a.Invalid != codeUnknownAction) {
			t.Fatalf("retired handover word active: %+v", a)
		}
	}
	a := NormalizeAction("canti:handover next", false)
	if a.Name != ActionHandover || a.Recovery != RecoveryLocal {
		t.Fatalf("handover=%+v", a)
	}
	if a := NormalizeAction("canti:focus {}", false); a.Name != ActionUnknown || a.Invalid != codeUnknownAction {
		t.Fatalf("focus=%+v", a)
	}
	if a := NormalizeAction("canti:retrieve {}", false); a.Invalid != codeRetrievalDisabled {
		t.Fatal("disabled retrieval admitted")
	}
}
func TestActionValidateAcceptsOnlyNormalizedReadings(t *testing.T) {
	for _, a := range []Action{{}, {Name: ActionCommand, Recovery: RecoveryLocal}, {Name: ActionHandover, Recovery: RecoveryExternal, Args: 12}, {Name: ActionRetrieve, Recovery: RecoveryRead}, {Name: ActionUnknown, Recovery: RecoveryLocal, Args: 6}, {Name: ActionUnknown, Recovery: RecoveryLocal, Args: -1, Invalid: codeUnknownAction}} {
		if a.Validate() == nil {
			t.Fatalf("invalid action accepted: %+v", a)
		}
	}
}
func TestActionEncodingIsBounded(t *testing.T) {
	for _, name := range []string{"make", "canti:retrieve {}", "canti:handover text", "canti:unknown"} {
		a := NormalizeAction(name, true)
		if a.Args > 0 {
			a.Args = math.MaxInt
		}
		data, err := json.Marshal(a)
		if err != nil || len(data) > MaxActionBytes {
			t.Fatalf("action=%s error=%v", data, err)
		}
	}
}
func TestCheckEffectProvesEachCompletionFromItsAcceptance(t *testing.T) {
	for _, payload := range []string{"canti:retrieve {}", "canti:handover text", "canti:unknown", "make"} {
		action := NormalizeAction(payload, true)
		accepted := Accepted{Semantics: core.SemanticsVersion, Action: action}
		if action.Recovery == RecoveryExternal {
			accepted.Execution = &ExecutionIdentity{Spec: ConfigDigest([]byte("spec")), ConfigDigest: ConfigDigest(nil)}
		}
		for _, o := range []Outcome{
			{Status: OutcomeDone, Retrieval: &archive.Retrieval{}},
			{Status: OutcomeDone},
			{Status: OutcomeFailed, Detail: unknownAction},
			{Status: OutcomeFailed, Retrieval: &archive.Retrieval{Error: "unreadable"}},
		} {
			tr := Transition{From: core.State{RunID: "r", Step: 2, Phase: core.PhaseEffectSaved, EffectID: "effect", Decision: core.Decision{Kind: core.Exec, Payload: payload}}, Event: core.Performed{EffectID: "effect"}, Facts: Facts{Outcome: &o}}
			_, handoverOutcome := handoverClaim(payload, &o)
			if handoverOutcome {
				tr.Facts.ContextState = &ContextState{AnchorSeq: 3, FirstStep: 3}
			}
			ok := false
			switch {
			case action.Invalid != "":
				ok = o.Status == OutcomeFailed && o.Detail == problemText(action.Invalid) && o.Retrieval == nil
			case action.Name == ActionRetrieve:
				ok = (o.Status == OutcomeFailed || o.Retrieval != nil) && tr.Facts.ContextState == nil
			case action.Name == ActionHandover:
				ok = handoverOutcome || o.Status == OutcomeFailed && o.Detail != "" && o.Retrieval == nil && tr.Facts.ContextState == nil
			}
			if err := CheckEffect(tr, accepted, nil); (err == nil) != ok {
				t.Fatalf("%s outcome=%+v error=%v", payload, o, err)
			}
		}
	}
}
