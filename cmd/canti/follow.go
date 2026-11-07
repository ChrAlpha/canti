package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/host"
)

// progressSource is what a follower reads a run's progress from: a host in
// this process or one that a client reaches.
type progressSource interface {
	Progress(ctx context.Context, after host.Cursor, limit int) (host.Page, error)
	Wait(ctx context.Context, after host.Cursor) (host.WaitResult, error)
}

// follower shows the progress of one run on w as its transitions are
// committed: each committed command, and each completed step. It reads the
// journal through its source, so a display that is slow or blocked delays
// only itself, never a commit.
type follower struct {
	src    progressSource
	next   host.Cursor
	w      io.Writer
	cancel context.CancelFunc
	done   chan struct{}
}

// follow prepares to show on a.steps the progress of run id that the host
// commits after now; start begins showing it. It shows nothing without
// a.steps.
func (a *app) follow(h *host.Host, id string) (*follower, error) {
	f := &follower{src: h, next: host.Cursor{RunID: id}, w: a.steps, done: make(chan struct{})}
	if a.steps == nil {
		return f, nil
	}
	v, err := h.View(context.Background(), id)
	if err != nil {
		return nil, err
	}
	f.next = v.Cursor()
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
			r, err := f.src.Wait(ctx, f.next)
			if err != nil || r.Cursor.Seq <= f.next.Seq && (r.Halted || !r.Driving) {
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
	p, err := f.src.Progress(ctx, f.next, 0)
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
func render(w io.Writer, it host.Item) {
	switch {
	case it.Kind == host.ItemDecided:
		fmt.Fprintf(w, "$ %s\n", strings.Join(excerpt(it.Command, false), "\n  "))
	case it.Kind == host.ItemCompleted && it.Outcome != nil:
		for _, line := range excerpt(outcomeSummary(*it.Outcome), true) {
			fmt.Fprintf(w, "│ %s\n", line)
		}
		fmt.Fprintf(w, "└ %s\n", it.Outcome.Status)
	case it.Kind == host.ItemCompleted:
		status := string(it.Execution)
		if it.Execution == core.ExecutionExited && it.ExitCode != nil {
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
func outcomeSummary(o host.Outcome) string {
	switch {
	case o.Detail != "":
		return o.Detail
	case o.Retrieval && o.RetrievalError != "":
		return "retrieval error: " + o.RetrievalError
	case o.Retrieval:
		return fmt.Sprintf("retrieved %d sources and %d directory entries", o.Sources, o.Entries)
	case o.Handover:
		return "handover recorded"
	}
	return ""
}
