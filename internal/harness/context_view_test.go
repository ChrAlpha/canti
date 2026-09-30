package harness_test

import (
	"bytes"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
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

func TestContextRequestComparisonKeepsLegacyUnknown(t *testing.T) {
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
	if old.CaptureAvailable || delta.PromptBytes != nil || delta.WindowChanged || delta.BodyBytes != 4 || old.BodyBytes != 3 {
		t.Fatalf("invented legacy metadata: %+v %+v", old, delta)
	}
	next := frozen(9, 2, 1, &harness.Request{Body: []byte("next"), Window: &harness.Window{FirstStep: 2}, Context: &harness.ContextCapture{Bytes: harness.ContextBytes{Total: 1}}})
	delta = harness.CompareContextRequests(current, next)
	if !delta.WindowChanged || delta.PromptBytes == nil || *delta.PromptBytes != -1 || delta.BodyBytes != -3 {
		t.Fatalf("wrong next-window delta: %+v", delta)
	}
}
