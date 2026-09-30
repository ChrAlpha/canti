package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"

	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

type contextReport struct {
	RunID      string                  `json:"run_id"`
	SessionID  string                  `json:"session_id,omitempty"`
	SessionSeq int64                   `json:"session_seq,omitempty"`
	AsOfSeq    int64                   `json:"as_of_seq"`
	Request    harness.ContextRequest  `json:"request"`
	Previous   *harness.ContextRequest `json:"previous,omitempty"`
	Next       *harness.ContextRequest `json:"next,omitempty"`
	Delta      *harness.ContextDelta   `json:"delta,omitempty"`
	Events     []harness.ContextEvent  `json:"events"`
	More       bool                    `json:"more"`
	NextAfter  int64                   `json:"next_after,omitempty"`
}

func contextCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	step := fs.Int("step", 0, "request `step`; 0 selects the latest frozen request")
	revision := fs.Int("revision", 0, "request `revision` at -step; 0 selects its latest revision")
	after := fs.Int64("after", 0, "read events after this journal `sequence`; requires explicit -run, -step and -revision")
	limit := fs.Int("limit", 20, "maximum event `count` per page (1..100)")
	raw := fs.Bool("raw", false, "write exact frozen request bytes without an added newline")
	text := fs.Bool("text", false, "print a short text report instead of JSON")
	bodies := fs.Bool("responses", false, "include recorded response bodies as base64 in JSON")
	return func(ctx context.Context) (int, error) {
		if *step < 0 || *revision < 0 || *revision > 0 && *step == 0 || *after < 0 || *limit < 1 || *limit > 100 {
			return exitUsage, errors.New("step/revision/after must be nonnegative, revision requires step, and limit must be 1..100")
		}
		if *after > 0 && (a.runID == "" || *step == 0 || *revision == 0) {
			return exitUsage, errors.New("after requires explicit -run, -step and -revision to keep pagination on the same request")
		}
		if *raw && (*text || *bodies || *after != 0) || *text && *bodies {
			return exitUsage, errors.New("raw cannot be combined with text/responses/after; responses requires JSON")
		}
		store, err := sqlite.OpenReadOnly(ctx, filepath.Join(a.state, "canti.db"))
		if err != nil {
			return exitError, err
		}
		defer store.Close()
		id := a.runID
		if id == "" {
			run, err := store.LastRun(ctx)
			if err != nil {
				return exitError, err
			}
			id = run.ID
		}
		if *raw {
			request, err := store.ContextRequest(ctx, id, *step, *revision)
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
		page, err := store.ContextRecords(ctx, id, *step, *revision, *after, *limit)
		if err != nil {
			return exitError, err
		}
		report, err := describeContext(page, *bodies)
		if err != nil {
			return exitError, err
		}
		if *text {
			return exitOK, report.writeText(a.stdout)
		}
		return exitOK, a.encoder().Encode(report)
	}
}

func describeContext(p sqlite.ContextPage, bodies bool) (contextReport, error) {
	r := contextReport{RunID: p.Run.ID, SessionID: p.Run.SessionID, SessionSeq: p.Run.SessionSeq, AsOfSeq: p.AsOfSeq, More: p.More, Events: []harness.ContextEvent{}}
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

func (r contextReport) writeText(w io.Writer) error {
	var err error
	line := func(format string, args ...any) {
		if err == nil {
			_, err = fmt.Fprintf(w, format+"\n", args...)
		}
	}
	q := r.Request
	line("Run %s, step %d, revision %d (request seq %d; as of %d)", r.RunID, q.Step, q.Revision, q.Seq, r.AsOfSeq)
	line("Model %s; protocol %s; adapter %s", q.Model, q.Protocol, q.Adapter)
	line("Frozen request bytes: %d; sha256 %s", q.BodyBytes, q.SHA256)
	if q.Window != nil {
		line("Window begins at step %d; handover source step %d; requested %t", q.Window.FirstStep, q.Window.SourceStep, q.Window.Requested)
	}
	if q.Capture == nil {
		line("Context capture unavailable for this historical request.")
	} else {
		c := q.Capture
		line("Rendered prompt bytes: %d / %d", c.Bytes.Total, c.Bytes.Limit)
		b := c.Bytes
		line("Composition bytes: system %d; goal %d; constraints %d; environment %d; earlier dialogue %d; handover %d", b.System, b.Goal, b.Constraints, b.Environment, b.Session, b.Handover)
		if c.Handover.Status == "evaluated" {
			line("Handover check: %s; reasons %v; used %d + growth %d, work capacity %d", c.Handover.Status, c.Handover.Reasons, c.Handover.Used, c.Handover.Growth, c.Handover.Capacity)
		} else {
			line("Handover check not evaluated: %s", c.Handover.Status)
		}
		for _, s := range c.Session {
			line("Earlier dialogue: run %s sequence %d, %s, goal included %t", s.RunID, s.SessionSeq, s.Status, s.IncludedGoal)
		}
		for _, retrieval := range c.Retrievals {
			line("Retrieval at step %d: %s; original query in observation; more %t; incomplete %t", retrieval.Step, retrieval.OperationName(), retrieval.More, retrieval.Incomplete)
			if retrieval.Error != "" {
				line("  error: %s", retrieval.Error)
			}
		}
		for _, s := range c.Sources {
			line("Source at step %d: %s [%d,%d) sha256 %s; available when frozen: %t", s.Step, s.Source.Path, s.Source.Offset, s.Source.Offset+int64(s.Source.Length), s.Source.SHA256, s.Available)
			if s.Error != "" {
				line("  error: %s", s.Error)
			}
		}
	}
	if r.Previous != nil {
		line("Previous: step %d revision %d (seq %d); request bytes delta %+d", r.Previous.Step, r.Previous.Revision, r.Previous.Seq, r.Delta.BodyBytes)
		if r.Delta.WindowChanged == nil {
			line("Window change unavailable: historical window metadata is missing.")
		} else {
			line("Window changed: %t", *r.Delta.WindowChanged)
		}
		if r.Delta.PromptBytes != nil {
			line("Rendered prompt bytes delta: %+d", *r.Delta.PromptBytes)
		}
	}
	if r.Next != nil {
		line("Next: step %d revision %d (seq %d)", r.Next.Step, r.Next.Revision, r.Next.Seq)
	}
	for _, e := range r.Events {
		line("Event %d: step %d revision %d attempt %d %s; phase %s", e.Seq, e.Step, e.Revision, e.Attempt, e.Outcome, e.Phase)
		if e.Reason != "" {
			line("  stop reason: %s", e.Reason)
		}
		if e.RequestedAttempt != 0 {
			line("  requested attempt %d was not started", e.RequestedAttempt)
		}
		if e.Failure != "" {
			line("  failure: %s", e.Failure)
		}
		if e.LostAttempt != 0 {
			line("  attempt %d has no committed response", e.LostAttempt)
		}
		if e.Response != nil {
			line("  response bytes: %d; complete %t; recorded tokens input %d output %d", e.Response.BodyBytes, e.Response.Complete, e.Response.Usage.InputTokens, e.Response.Usage.OutputTokens)
		}
	}
	if r.More {
		line("More events: use -run %s -step %d -revision %d -after %d", r.RunID, q.Step, q.Revision, r.NextAfter)
	}
	return err
}
