package harness_test

import (
	"bytes"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func TestContextEventsPreserveAttemptIdentity(t *testing.T) {
	state, err := core.NewState("inspection", core.Limits{MaxSteps: 3, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	seq := int64(0)
	commit := func(event core.Event, facts harness.Facts) harness.Transition {
		t.Helper()
		next, err := state.Next(event)
		if err != nil {
			t.Fatal(err)
		}
		seq++
		tr := harness.Transition{Seq: seq, From: state, To: next, Event: event, Facts: facts}
		state = next
		return tr
	}
	commit(core.RequestFrozen{}, harness.Facts{})
	commit(core.AttemptStarted{Step: 1, Attempt: 1}, harness.Facts{})
	partial := []byte{0xff, '{', '"'}
	tr := commit(core.ResponseIncomplete{Step: 1, Attempt: 1}, harness.Facts{Failure: "truncated", Response: &harness.Response{Body: partial, Usage: harness.Usage{InputTokens: 11}}})
	v := harness.DescribeContextEvent(tr, true)
	if v.Attempt != 1 || v.Revision != 1 || state.Attempt != 2 || v.Failure != "truncated" || v.Response.Complete || !bytes.Equal(v.Response.Body, partial) || v.Response.Usage.InputTokens != 11 {
		t.Fatalf("partial attributed to wrong attempt: %+v", v)
	}
	if hidden := harness.DescribeContextEvent(tr, false); hidden.Response.Body != nil || hidden.Response.BodyBytes != len(partial) {
		t.Fatal("summary copied body or lost size")
	}
	commit(core.AttemptStarted{Step: 1, Attempt: 2}, harness.Facts{})
	commit(core.ResponseSaved{Step: 1, Attempt: 2}, harness.Facts{})
	tr = commit(core.ResponseLoaded{Step: 1, Attempt: 2, Body: []byte("malformed")}, harness.Facts{})
	v = harness.DescribeContextEvent(tr, false)
	if v.Attempt != 2 || v.Revision != 1 || v.Outcome != "protocol_rejected" || v.Failure == "" || state.Attempt != 3 {
		t.Fatalf("repair misattributed: %+v", v)
	}
	commit(core.RequestFrozen{}, harness.Facts{})
	tr = commit(core.AttemptStarted{Step: 1, Attempt: 3}, harness.Facts{})
	if v = harness.DescribeContextEvent(tr, false); v.Attempt != 3 || v.Revision != 2 {
		t.Fatalf("repair start: %+v", v)
	}
	tr = commit(core.AttemptStarted{Step: 1, Attempt: 4}, harness.Facts{})
	v = harness.DescribeContextEvent(tr, false)
	if v.Attempt != 3 || v.RequestedAttempt != 4 || v.LostAttempt != 3 || v.Outcome != "attempt_not_started" || v.Reason != core.StopAttemptsExhausted {
		t.Fatalf("invented a fourth attempt: %+v", v)
	}
}

func TestContextRequestComparisonKeepsMissingMetadataUnknown(t *testing.T) {
	frozen := func(seq int64, step, revision int, req *harness.Request) harness.ContextRequest {
		t.Helper()
		v, err := harness.DescribeContextRequest(harness.Transition{Seq: seq, Event: core.RequestFrozen{}, To: core.State{Step: step, RequestRev: revision}, Facts: harness.Facts{Request: req}})
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	old := frozen(1, 1, 1, &harness.Request{Body: []byte("old"), Window: &harness.Window{FirstStep: 1}})
	current := frozen(5, 1, 2, &harness.Request{Body: []byte("current"), Window: &harness.Window{FirstStep: 1}, Context: &harness.ContextCapture{Bytes: harness.ContextBytes{Total: 2}}})
	delta := harness.CompareContextRequests(old, current)
	if old.CaptureAvailable || delta.PromptBytes != nil || delta.WindowChanged == nil || *delta.WindowChanged || delta.BodyBytes != 4 || old.BodyBytes != 3 {
		t.Fatalf("invented optional metadata: %+v %+v", old, delta)
	}
	next := frozen(9, 2, 1, &harness.Request{Body: []byte("next"), Window: &harness.Window{FirstStep: 2}, Context: &harness.ContextCapture{Bytes: harness.ContextBytes{Total: 1}}})
	delta = harness.CompareContextRequests(current, next)
	if delta.WindowChanged == nil || !*delta.WindowChanged || delta.PromptBytes == nil || *delta.PromptBytes != -1 || delta.BodyBytes != -3 {
		t.Fatalf("wrong next-window delta: %+v", delta)
	}
	for _, pair := range [][2]harness.ContextRequest{{{}, current}, {current, {}}, {{}, {}}} {
		if d := harness.CompareContextRequests(pair[0], pair[1]); d.WindowChanged != nil {
			t.Fatalf("invented a window change from missing optional metadata: %+v", d)
		}
	}
}

// TestContextEventsReportTheCommittedDecodeOutcome requires that a loaded
// response reports its committed outcome and current decoder diagnostic.
func TestContextEventsReportTheCommittedDecodeOutcome(t *testing.T) {
	saved := core.State{RunID: "inspection", Step: 1, Attempt: 1, RequestRev: 1, Phase: core.PhaseResponseSaved, Limits: core.Limits{MaxSteps: 3, MaxAttempts: 2}}
	retried, stopped, effect, returned := saved, saved, saved, saved
	retried.Phase, retried.Attempt = core.PhaseIdle, 2
	stopped.Phase, stopped.Reason = core.PhaseStopped, core.StopProtocolFailure
	effect.Phase, effect.Decision, effect.EffectID = core.PhaseEffectSaved, core.Decision{Kind: core.Exec, Payload: "true"}, "effect"
	returned.Phase, returned.Decision = core.PhaseReturned, core.Decision{Kind: core.Return, Payload: "done"}
	for _, tc := range []struct {
		name, body       string
		to               core.State
		outcome, failure string
	}{
		{"exec accepted", `{"kind":"exec","payload":"true"}`, effect, "response_loaded", ""},
		{"rejected", `{"kind":"exec"}`, retried, "protocol_rejected", "core: invalid decision: expected kind and payload"},
		{"rejected at the last attempt", `not json`, stopped, "protocol_rejected", "core: invalid decision: invalid character 'o' in literal null (expecting 'u')"},
		{"returned", `{"kind":"return","payload":"done"}`, returned, "returned", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := harness.Transition{Seq: 4, From: saved, To: tc.to, Event: core.ResponseLoaded{Step: 1, Attempt: 1, Body: []byte(tc.body)}}
			if v := harness.DescribeContextEvent(tr, false); v.Outcome != tc.outcome || v.Failure != tc.failure {
				t.Fatalf("event = %+v, want outcome %q and failure %q", v, tc.outcome, tc.failure)
			}
		})
	}
}
