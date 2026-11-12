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

// progressSource is what a follower reads the agent log from: a host in this
// process or one that a client reaches.
type progressSource interface {
	Progress(ctx context.Context, after string, limit int) (hostrpc.Page, error)
	Wait(ctx context.Context, after string) (hostrpc.Wait, error)
}

// follower shows the agent log on w as its facts are committed: each
// admitted input, each committed command, and each completed step. It reads
// the journal through its source, so a display that is slow or blocked delays
// only itself, never a commit.
type follower struct {
	src    progressSource
	next   string
	w      io.Writer
	cancel context.CancelFunc
	done   chan struct{}
}

// follow prepares to show on a.steps the agent log after the position of run
// id; start begins showing it. It shows nothing without a.steps.
func (a *app) follow(h *host.Host, id string) (*follower, error) {
	f := &follower{src: embeddedProgress{h}, w: a.steps, done: make(chan struct{})}
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

// start shows the log of a drive that has started, until the agent is
// quiescent with nothing owned or finish is called.
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
			if err != nil || r.Cursor == f.next && r.Quiescent && r.Driver != hostrpc.DriverDriving {
				return
			}
		}
	}()
}

// finish stops following and shows what remains to the log's latest row.
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

// render shows item it: an admitted input, the start of a run, the command of
// an accepted exec decision, and the result of a completed step.
func render(w io.Writer, it hostrpc.Item) {
	switch {
	case it.Kind == "input":
		renderInput(w, it)
	case it.Kind == "run_started":
		fmt.Fprintf(w, "▶ run %s started\n", it.RunID)
	case it.Kind == "inputs_bound":
		fmt.Fprintf(w, "↳ %d instruction(s) bound before step %d\n", it.Inputs, it.Step)
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

func renderInput(w io.Writer, it hostrpc.Item) {
	in := it.Input
	if in == nil {
		return
	}
	label := string(in.Authority)
	switch {
	case in.Authority == "principal" && in.Disposition != "":
		label = "instruction " + in.Disposition
	case in.Authority == "observation":
		label = "observation " + in.Source + "/" + in.Subject
	}
	text := strings.Join(excerpt(in.Text, false), " ")
	fmt.Fprintf(w, "› %s at %d: %s\n", label, it.Pos, text)
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

func embeddedCursor(after string) (host.Cursor, error) {
	if after == "" {
		return 0, nil
	}
	return hostserver.ParseCursor(after)
}

func (p embeddedProgress) Progress(ctx context.Context, after string, limit int) (hostrpc.Page, error) {
	c, err := embeddedCursor(after)
	if err != nil {
		return hostrpc.Page{}, err
	}
	page, err := p.h.Progress(ctx, c, limit)
	return hostserver.PageOf(page), err
}

func (p embeddedProgress) Wait(ctx context.Context, after string) (hostrpc.Wait, error) {
	c, err := embeddedCursor(after)
	if err != nil {
		return hostrpc.Wait{}, err
	}
	w, err := p.h.Wait(ctx, c)
	driver := hostrpc.DriverIdle
	if w.Driving {
		driver = hostrpc.DriverDriving
	}
	return hostrpc.Wait{Cursor: hostserver.CursorOf(w.Cursor), Quiescent: w.Quiescent, Driver: driver}, err
}
