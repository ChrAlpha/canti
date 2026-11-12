package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
)

type contextReport struct {
	RunID          string                  `json:"run_id"`
	InputID        string                  `json:"input_id,omitempty"`
	InputPos       int64                   `json:"input_pos,omitempty"`
	Fresh          bool                    `json:"fresh,omitempty"`
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
		store, run, _, err := a.read(ctx)
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
		report, err := describeContext(page, f.bodies)
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

func describeContext(p sqlite.ContextPage, bodies bool) (contextReport, error) {
	r := contextReport{RunID: p.Run.ID, InputID: p.Run.InputID, InputPos: p.Run.InputPos, Fresh: p.Run.Fresh, AsOfSeq: p.AsOfSeq, More: p.More, Events: []harness.ContextEvent{}, EventBytes: p.EventBytes, EventByteLimit: sqlite.ContextEventPageBytes}
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
		r.Events = append(r.Events, harness.DescribeContextEvent(event, bodies))
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
	if q.Anchor != nil {
		t.line("Anchor sequence %d; first step %d; minimal %t; carried handover %t; rejection %t", q.Anchor.AnchorSeq, q.Anchor.FirstStep, q.Anchor.Minimal, q.Anchor.Handover != nil, q.Anchor.Rejection != nil)
	}
	if q.Capture == nil {
		t.line("Context capture unavailable for this recorded request.")
	} else {
		t.capture(q.Capture)
	}
	if r.Previous != nil {
		t.line("Previous: step %d revision %d (seq %d); request bytes delta %+d", r.Previous.Step, r.Previous.Revision, r.Previous.Seq, r.Delta.BodyBytes)
		if r.Delta.AnchorChanged == nil {
			t.line("Anchor change unavailable: recorded anchor metadata is missing.")
		} else {
			t.line("Anchor changed: %t", *r.Delta.AnchorChanged)
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
	t.line("Composition bytes: system %d; goal %d; constraints %d; head instructions %d; environment %d; anchor %d; previous %d; handover %d; index %d; notice %d; epoch header %d; epoch instructions %d; observations %d; latest %d",
		b.System, b.Goal, b.Constraints, b.HeadInstructions, b.Environment, b.Anchor, b.Previous, b.Handover, b.Index, b.Notice, b.EpochHeader, b.EpochInstructions, b.Observations, b.Latest)
	t.line("Preparation: %s; latest step %d sequence %d; resident sources %d; bound instructions %d; observation keys %d; latest observation position %d",
		c.Preparation, c.LatestStep, c.LatestSeq, len(c.Sources), c.InstructionCount, c.ObservationKeys, c.ObservationPos)
	if c.PreviousRun != "" {
		t.line("Previous run: %s", c.PreviousRun)
	}
	for _, retrieval := range c.Retrievals {
		t.line("Retrieval at step %d: %s; original query in observation; more %t; incomplete %t", retrieval.Step, retrieval.OperationName(), retrieval.More, retrieval.Incomplete)
		if retrieval.Error != "" {
			t.line("  error: %s", retrieval.Error)
		}
	}
	for _, s := range c.Sources {
		t.line("Resident source sequence %d source %d, step %d: %s [%d,%d) sha256 %s", s.Seq, s.Index, s.Step, s.Source.Path, s.Source.Offset, s.Source.Offset+int64(s.Source.Length), s.Source.SHA256)
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
		t.line("  response bytes: %d; complete %t; reported tokens input %s output %s", e.Response.BodyBytes, e.Response.Complete, reportedCount(e.Response.Usage.InputTokens, e.Response.Usage.InputReported), reportedCount(e.Response.Usage.OutputTokens, e.Response.Usage.OutputReported))
	}
}

func reportedCount(count int64, reported bool) string {
	if !reported {
		return "unknown"
	}
	return fmt.Sprint(count)
}
