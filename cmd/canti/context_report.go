package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/internal/host"
)

type contextReport struct {
	RunID          string                  `json:"run_id"`
	SessionID      string                  `json:"session_id,omitempty"`
	SessionSeq     int64                   `json:"session_seq,omitempty"`
	AsOfSeq        int64                   `json:"as_of_seq"`
	Request        harness.ContextRequest  `json:"request"`
	Previous       *harness.ContextRequest `json:"previous,omitempty"`
	Next           *harness.ContextRequest `json:"next,omitempty"`
	Delta          *harness.ContextDelta   `json:"delta,omitempty"`
	Events         []harness.ContextEvent  `json:"events"`
	More           bool                    `json:"more"`
	NextAfter      int64                   `json:"next_after,omitempty"`
	EventBytes     int64                   `json:"event_bytes"`
	EventByteLimit int                     `json:"event_byte_limit"`
}

type contextFlags struct {
	step, revision, limit int
	after                 int64
	raw, text, bodies     bool
}

func contextCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	var f contextFlags
	fs.IntVar(&f.step, "step", 0, "request `step`; 0 selects the latest frozen request")
	fs.IntVar(&f.revision, "revision", 0, "request `revision` at -step; 0 selects its latest revision")
	fs.Int64Var(&f.after, "after", 0, "read events after this journal `sequence`; requires explicit -run, -step and -revision")
	fs.IntVar(&f.limit, "limit", 20, "maximum event `count` per page (1..100); also capped at 16 MiB persisted event JSON, except one oversized event")
	fs.BoolVar(&f.raw, "raw", false, "write exact frozen request bytes without an added newline")
	fs.BoolVar(&f.text, "text", false, "print a short text report instead of JSON")
	fs.BoolVar(&f.bodies, "responses", false, "include recorded response bodies as base64 in JSON")
	return func(ctx context.Context) (int, error) {
		if err := f.check(a.runID); err != nil {
			return exitUsage, err
		}
		store, run, c, err := a.read(ctx)
		if err != nil {
			return exitError, err
		}
		defer store.Close()
		if f.raw {
			return a.writeRequest(ctx, store, run.ID, f.step, f.revision)
		}
		page, err := store.ContextRecords(ctx, run.ID, f.step, f.revision, f.after, f.limit)
		if err != nil {
			return exitError, err
		}
		report, err := describeContext(page, f.bodies, c.ContextVersion == host.ContextVersion)
		if err != nil {
			return exitError, err
		}
		if f.text {
			return exitOK, report.writeText(a.stdout)
		}
		return exitOK, a.encoder().Encode(report)
	}
}

func (f contextFlags) check(runID string) error {
	if f.step < 0 || f.revision < 0 || f.revision > 0 && f.step == 0 || f.after < 0 || f.limit < 1 || f.limit > 100 {
		return errors.New("step/revision/after must be nonnegative, revision requires step, and limit must be 1..100")
	}
	if f.after > 0 && (runID == "" || f.step == 0 || f.revision == 0) {
		return errors.New("after requires explicit -run, -step and -revision to keep pagination on the same request")
	}
	if f.raw && (f.text || f.bodies || f.after != 0) || f.text && f.bodies {
		return errors.New("raw cannot be combined with text/responses/after; responses requires JSON")
	}
	return nil
}

func (a *app) writeRequest(ctx context.Context, store *sqlite.Store, id string, step, revision int) (int, error) {
	request, err := store.ContextRequest(ctx, id, step, revision)
	if err != nil {
		return exitError, err
	}
	body := request.Facts.Request.Body
	n, err := a.stdout.Write(body)
	if err == nil && n != len(body) {
		err = io.ErrShortWrite
	}
	return exitOK, err
}

func describeContext(p sqlite.ContextPage, bodies, currentRules bool) (contextReport, error) {
	r := contextReport{RunID: p.Run.ID, SessionID: p.Run.SessionID, SessionSeq: p.Run.SessionSeq, AsOfSeq: p.AsOfSeq, More: p.More, Events: []harness.ContextEvent{}, EventBytes: p.EventBytes, EventByteLimit: sqlite.ContextEventPageBytes}
	var err error
	if r.Request, err = harness.DescribeContextRequest(p.Request); err != nil {
		return r, err
	}
	for _, pair := range []struct {
		record *harness.Transition
		out    **harness.ContextRequest
	}{{p.Previous, &r.Previous}, {p.Next, &r.Next}} {
		if pair.record != nil {
			v, err := harness.DescribeContextRequest(*pair.record)
			if err != nil {
				return r, err
			}
			*pair.out = &v
		}
	}
	if r.Previous != nil {
		delta := harness.CompareContextRequests(*r.Previous, r.Request)
		r.Delta = &delta
	}
	for _, event := range p.Events {
		r.Events = append(r.Events, harness.DescribeContextEvent(event, bodies, currentRules))
		r.NextAfter = event.Seq
	}
	if !r.More {
		r.NextAfter = 0
	}
	return r, nil
}

type textLines struct {
	w   io.Writer
	err error
}

func (t *textLines) line(format string, args ...any) {
	if t.err == nil {
		_, t.err = fmt.Fprintf(t.w, format+"\n", args...)
	}
}

func (r contextReport) writeText(w io.Writer) error {
	t := &textLines{w: w}
	q := r.Request
	t.line("Run %s, step %d, revision %d (request seq %d; as of %d)", r.RunID, q.Step, q.Revision, q.Seq, r.AsOfSeq)
	t.line("Model %s; protocol %s; adapter %s", q.Model, q.Protocol, q.Adapter)
	t.line("Frozen request bytes: %d; sha256 %s", q.BodyBytes, q.SHA256)
	if q.Window != nil {
		t.line("Window begins at step %d; handover source step %d; requested %t", q.Window.FirstStep, q.Window.SourceStep, q.Window.Requested)
	}
	if q.Capture == nil {
		t.line("Context capture unavailable for this historical request.")
	} else {
		t.capture(q.Capture)
	}
	if r.Previous != nil {
		t.line("Previous: step %d revision %d (seq %d); request bytes delta %+d", r.Previous.Step, r.Previous.Revision, r.Previous.Seq, r.Delta.BodyBytes)
		if r.Delta.WindowChanged == nil {
			t.line("Window change unavailable: historical window metadata is missing.")
		} else {
			t.line("Window changed: %t", *r.Delta.WindowChanged)
		}
		if r.Delta.PromptBytes != nil {
			t.line("Rendered prompt bytes delta: %+d", *r.Delta.PromptBytes)
		}
	}
	if r.Next != nil {
		t.line("Next: step %d revision %d (seq %d)", r.Next.Step, r.Next.Revision, r.Next.Seq)
	}
	t.line("Event page: %d persisted JSON bytes (limit %d; a single oversized event is returned intact)", r.EventBytes, r.EventByteLimit)
	for _, e := range r.Events {
		t.event(e)
	}
	if r.More {
		t.line("More events: use -run %s -step %d -revision %d -after %d", r.RunID, q.Step, q.Revision, r.NextAfter)
	}
	return t.err
}

func (t *textLines) capture(c *harness.ContextCapture) {
	t.line("Rendered prompt bytes: %d / %d", c.Bytes.Total, c.Bytes.Limit)
	b := c.Bytes
	t.line("Composition bytes: system %d; goal %d; constraints %d; environment %d; earlier dialogue %d; handover %d", b.System, b.Goal, b.Constraints, b.Environment, b.Session, b.Handover)
	if c.Handover.Status == "evaluated" {
		t.line("Handover check: %s; reasons %v; used %d + growth %d, work capacity %d", c.Handover.Status, c.Handover.Reasons, c.Handover.Used, c.Handover.Growth, c.Handover.Capacity)
	} else {
		t.line("Handover check not evaluated: %s", c.Handover.Status)
	}
	for _, s := range c.Session {
		handover := ""
		if s.HandoverStep != 0 {
			handover = fmt.Sprintf(", latest handover step %d", s.HandoverStep)
		}
		t.line("Earlier dialogue: run %s sequence %d, %s, goal included %t%s", s.RunID, s.SessionSeq, s.Status, s.IncludedGoal, handover)
	}
	for _, retrieval := range c.Retrievals {
		t.line("Retrieval at step %d: %s; original query in observation; more %t; incomplete %t", retrieval.Step, retrieval.OperationName(), retrieval.More, retrieval.Incomplete)
		if retrieval.Error != "" {
			t.line("  error: %s", retrieval.Error)
		}
	}
	for _, s := range c.Sources {
		t.line("Source at step %d: %s [%d,%d) sha256 %s; available when frozen: %t", s.Step, s.Source.Path, s.Source.Offset, s.Source.Offset+int64(s.Source.Length), s.Source.SHA256, s.Available)
		if s.Error != "" {
			t.line("  error: %s", s.Error)
		}
	}
}

func (t *textLines) event(e harness.ContextEvent) {
	t.line("Event %d: step %d revision %d attempt %d %s; phase %s", e.Seq, e.Step, e.Revision, e.Attempt, e.Outcome, e.Phase)
	if e.Reason != "" {
		t.line("  stop reason: %s", e.Reason)
	}
	if e.RequestedAttempt != 0 {
		t.line("  requested attempt %d was not started", e.RequestedAttempt)
	}
	if e.Failure != "" {
		t.line("  failure: %s", e.Failure)
	}
	if e.LostAttempt != 0 {
		t.line("  attempt %d has no committed response", e.LostAttempt)
	}
	if e.Response != nil {
		t.line("  response bytes: %d; complete %t; recorded tokens input %d output %d", e.Response.BodyBytes, e.Response.Complete, e.Response.Usage.InputTokens, e.Response.Usage.OutputTokens)
	}
}
