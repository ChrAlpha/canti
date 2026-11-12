package harness

import (
	"crypto/sha256"
	"fmt"

	"github.com/ChrAlpha/canti/runtime/core"
)

type ContextRequest struct {
	Seq              int64           `json:"seq"`
	Step             int             `json:"step"`
	Revision         int             `json:"revision"`
	Model            string          `json:"model"`
	Protocol         string          `json:"protocol"`
	Adapter          string          `json:"adapter"`
	BodyBytes        int             `json:"body_bytes"`
	SHA256           string          `json:"sha256"`
	Anchor           *ContextState   `json:"anchor,omitempty"`
	CaptureAvailable bool            `json:"capture_available"`
	Capture          *ContextCapture `json:"capture,omitempty"`
}

type ContextDelta struct {
	BodyBytes     int   `json:"body_bytes"`
	PromptBytes   *int  `json:"prompt_bytes,omitempty"`
	AnchorChanged *bool `json:"anchor_changed"`
}

type ContextResponse struct {
	Complete  bool   `json:"complete"`
	BodyBytes int    `json:"body_bytes"`
	SHA256    string `json:"sha256"`
	Usage     Usage  `json:"usage"`
	Body      []byte `json:"body_base64,omitempty"`
}

type ContextEvent struct {
	Seq              int64              `json:"seq"`
	Kind             string             `json:"kind"`
	Step             int                `json:"step"`
	Revision         int                `json:"revision"`
	Attempt          int                `json:"attempt"`
	LostAttempt      int                `json:"lost_attempt,omitempty"`
	RequestedAttempt int                `json:"requested_attempt,omitempty"`
	Phase            core.Phase         `json:"phase"`
	Outcome          string             `json:"outcome"`
	Reason           core.StopReason    `json:"reason,omitempty"`
	Failure          string             `json:"failure,omitempty"`
	Reset            *ContextState      `json:"reset,omitempty"`
	Signal           *ContextLimitError `json:"signal,omitempty"`
	Response         *ContextResponse   `json:"response,omitempty"`
}

func DescribeContextRequest(t Transition) (ContextRequest, error) {
	r := t.Facts.Request
	if r == nil || t.Event == nil || t.Event.Kind() != "request_frozen" {
		return ContextRequest{}, fmt.Errorf("harness: transition %d is not a frozen request", t.Seq)
	}
	return ContextRequest{
		Seq: t.Seq, Step: t.To.Step, Revision: t.To.RequestRev,
		Model: r.Model, Protocol: r.Protocol, Adapter: r.Adapter,
		BodyBytes: len(r.Body), SHA256: fmt.Sprintf("%x", sha256.Sum256(r.Body)),
		Anchor: r.ContextState, CaptureAvailable: r.Context != nil, Capture: r.Context,
	}, nil
}

func CompareContextRequests(previous, current ContextRequest) ContextDelta {
	d := ContextDelta{BodyBytes: current.BodyBytes - previous.BodyBytes}
	if previous.Capture != nil && current.Capture != nil {
		delta := current.Capture.Bytes.Total - previous.Capture.Bytes.Total
		d.PromptBytes = &delta
	}
	if previous.Anchor != nil && current.Anchor != nil {
		changed := previous.Anchor.AnchorSeq != current.Anchor.AnchorSeq
		d.AnchorChanged = &changed
	}
	return d
}

// DescribeContextEvent describes t, with the body of its response when
// bodies is set. Current-format rejected responses carry the decoder diagnostic.
func DescribeContextEvent(t Transition, bodies bool) ContextEvent {
	v := ContextEvent{
		Seq: t.Seq, Kind: t.Event.Kind(), Step: t.From.Step, Revision: t.From.RequestRev,
		Attempt: t.From.Attempt, Phase: t.To.Phase, Outcome: t.Event.Kind(),
		Reason: t.To.Reason, Failure: t.Facts.Failure,
	}
	if t.Facts.Rejection != nil {
		v.Signal = &t.Facts.Rejection.Signal
	}
	v.Reset = t.Facts.ContextState
	if r := t.Facts.Response; r != nil {
		v.Response = &ContextResponse{Complete: r.Complete, BodyBytes: len(r.Body), SHA256: fmt.Sprintf("%x", sha256.Sum256(r.Body)), Usage: r.Usage}
		if bodies {
			v.Response.Body = r.Body
		}
	}
	switch e := t.Event.(type) {
	case core.AttemptStarted:
		if t.From.Phase == core.PhaseRequestSent {
			v.LostAttempt = t.From.Attempt
		}
		if t.To.Phase == core.PhaseRequestSent {
			v.Attempt = e.Attempt
		} else {
			v.RequestedAttempt, v.Outcome = e.Attempt, "attempt_not_started"
		}
	case core.RequestRejected:
		v.Outcome = "context_rejected"
		if t.Facts.ContextState != nil {
			v.Outcome = "context_reset"
		}
	case core.Performed:
		if t.Facts.Outcome != nil && t.Facts.Outcome.Handover != nil {
			v.Outcome = "context_handover"
		}
	case core.ResponseLoaded:
		switch t.To.Phase {
		case core.PhaseReturned:
			v.Outcome = "returned"
		case core.PhaseEffectSaved:
		default:
			v.Outcome = "protocol_rejected"
			if _, err := core.Decode(e.Body); err != nil {
				v.Failure = err.Error()
			}
		}
	}
	return v
}
