package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// progressSource is what a follower reads a run's progress from: a host in
// this process or one that a client reaches.
type progressSource interface {
	Progress(ctx context.Context, runID, after string, limit int) (hostrpc.Page, error)
	Wait(ctx context.Context, runID, after string) (hostrpc.Wait, error)
}

// follower shows the progress of one run on w as its transitions are
// committed: each committed command, and each completed step. It reads the
// journal through its source, so a display that is slow or blocked delays
// only itself, never a commit.
type follower struct {
	src    progressSource
	next   string
	runID  string
	w      io.Writer
	cancel context.CancelFunc
	done   chan struct{}
}

// follow prepares to show on a.steps the progress of run id that the host
// commits after now; start begins showing it. It shows nothing without
// a.steps.
func (a *app) follow(h *host.Host, id string) (*follower, error) {
	f := &follower{src: embeddedProgress{h}, runID: id, w: a.steps, done: make(chan struct{})}
	if a.steps == nil {
		return f, nil
	}
	v, err := h.View(context.Background(), id)
	if err != nil {
		return nil, err
	}
	f.next = hostserver.CursorOf(v.Cursor())
	return f, nil
}

// start shows the progress of a drive that has started, until the host no
// longer drives the run or finish is called.
func (f *follower) start() {
	if f.w == nil || f.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go func() {
		defer close(f.done)
		for {
			more, err := f.page(ctx)
			if err != nil {
				return
			}
			if more {
				continue
			}
			r, err := f.src.Wait(ctx, f.runID, f.next)
			if err != nil || r.Cursor == f.next && (r.Halted || r.Driver != hostrpc.DriverDriving) {
				return
			}
		}
	}()
}

// finish stops following and shows what remains to the run's latest
// transition.
func (f *follower) finish() {
	if f.w == nil {
		return
	}
	if f.cancel != nil {
		f.cancel()
		<-f.done
	}
	for {
		if more, err := f.page(context.Background()); err != nil || !more {
			return
		}
	}
}

// page shows one page of progress and reports whether more follows.
func (f *follower) page(ctx context.Context) (bool, error) {
	p, err := f.src.Progress(ctx, f.runID, f.next, 0)
	if err != nil {
		return false, err
	}
	for _, it := range p.Items {
		render(f.w, it)
	}
	f.next = p.Next
	return p.More, nil
}

// render shows item it: the command of an accepted exec decision, and the
// result of a completed step.
func render(w io.Writer, it hostrpc.Item) {
	switch {
	case it.Kind == "context_reset":
		fmt.Fprintf(w, "↳ context reset at sequence %d; continuing step %d from archive navigation\n", it.AnchorSeq, it.Step)
	case it.Kind == "decided":
		fmt.Fprintf(w, "$ %s\n", strings.Join(excerpt(it.Command, false), "\n  "))
	case it.Kind == "completed" && it.Outcome != nil:
		for _, line := range excerpt(outcomeSummary(*it.Outcome), true) {
			fmt.Fprintf(w, "│ %s\n", line)
		}
		fmt.Fprintf(w, "└ %s\n", it.Outcome.Status)
	case it.Kind == "completed":
		status := string(it.Execution)
		if (it.Execution == "cancelled" || it.Execution == "timed_out") && it.ExitCode == nil {
			status += " (exit code unavailable)"
		}
		if it.Execution == "exited" && it.ExitCode != nil {
			status = fmt.Sprintf("exit %d", *it.ExitCode)
		}
		if d := excerpt(it.Detail, false); len(d) > 0 {
			status += " (" + strings.Join(d, " ") + ")"
		}
		for _, line := range excerpt(it.Preview, true) {
			fmt.Fprintf(w, "│ %s\n", line)
		}
		fmt.Fprintf(w, "└ %s\n", status)
	}
}

// outcomeSummary describes the outcome of an action the harness performed,
// which ran no process.
func outcomeSummary(o hostrpc.Outcome) string {
	switch {
	case o.Detail != "":
		return o.Detail
	case o.Retrieval && o.RetrievalError != "":
		return "retrieval error: " + o.RetrievalError
	case o.Retrieval:
		return fmt.Sprintf("retrieved %d sources and %d directory entries", o.Sources, o.Entries)
	case o.Focus:
		return fmt.Sprintf("focus replaced with %d frozen sources", o.FocusSources)
	}
	return ""
}

// embeddedProgress projects an in-process host through the same presentation
// contract as the SDK; it performs no socket or protocol client operation.
type embeddedProgress struct{ h *host.Host }

func embeddedCursor(id, after string) (host.Cursor, error) {
	if after == "" {
		return host.Cursor{RunID: id}, nil
	}
	return hostserver.ParseCursor(after)
}
func (p embeddedProgress) Progress(ctx context.Context, id, after string, limit int) (hostrpc.Page, error) {
	c, err := embeddedCursor(id, after)
	if err != nil {
		return hostrpc.Page{}, err
	}
	page, err := p.h.Progress(ctx, c, limit)
	return hostserver.PageOf(page), err
}
func (p embeddedProgress) Wait(ctx context.Context, id, after string) (hostrpc.Wait, error) {
	c, err := embeddedCursor(id, after)
	if err != nil {
		return hostrpc.Wait{}, err
	}
	w, err := p.h.Wait(ctx, c)
	driver := hostrpc.DriverIdle
	if w.Driving {
		driver = hostrpc.DriverDriving
	}
	return hostrpc.Wait{Cursor: hostserver.CursorOf(w.Cursor), Halted: w.Halted, Driver: driver}, err
}
