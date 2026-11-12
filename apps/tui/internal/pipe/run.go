// Package pipe presents the same chat facts as the UI, one input at a time.
package pipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/apps/tui/internal/present"
)

type input struct {
	text string
	err  error
}

func Run(ctx context.Context, c *chat.Controller, in *os.File, out, diagnostic io.Writer, signals <-chan os.Signal, failures <-chan error) (int, error) {
	reader, err := newInputReader(in, diagnostic, c.GoalLimit)
	if err != nil {
		return chat.Error, err
	}
	defer reader.Close()
	if _, err = fmt.Fprint(diagnostic, "canti: each input drives the agent in this workspace; the agent keeps one line.\n"); err != nil {
		return chat.Error, err
	}
	for {
		if _, err = fmt.Fprint(diagnostic, "> "); err != nil {
			return chat.Error, fmt.Errorf("showing chat prompt: %w", err)
		}
		entry, code, err := reader.Next(ctx, signals, failures)
		if code != chat.OK || err != nil {
			return code, err
		}
		var opener *openerError
		var overflow *overflowError
		if errors.As(entry.err, &opener) || errors.As(entry.err, &overflow) {
			if _, e := fmt.Fprintf(diagnostic, "canti-tui: %v\n", entry.err); e != nil {
				return chat.Error, e
			}
			if errors.Is(entry.err, io.EOF) {
				return chat.OK, nil
			}
			continue
		}
		if entry.text != "" {
			if err = c.Check(entry.text); err != nil {
				fmt.Fprintf(diagnostic, "canti-tui: %v\n", err)
			} else {
				code, err, stop := runInput(ctx, c, entry.text, out, diagnostic, signals, failures)
				if stop {
					return code, err
				}
			}
		}
		if errors.Is(entry.err, io.EOF) {
			fmt.Fprintln(diagnostic)
			return chat.OK, nil
		}
		if entry.err != nil {
			return chat.Error, entry.err
		}
	}
}
func runInput(ctx context.Context, c *chat.Controller, goal string, out, diagnostic io.Writer, signals <-chan os.Signal, failures <-chan error) (int, error, bool) {
	turn, err := c.Start(ctx, chat.NewSnapshot(goal))
	if err != nil {
		return chat.Error, err, true
	}
	events, closeEvents := turnEvents(turn)
	defer closeEvents()
	interrupted, explicitStop := false, false
	var displayErr error
	for {
		select {
		case sig := <-signals:
			interrupted = true
			explicitStop = explicitStop || sig != syscall.SIGHUP
			signalTurn(turn, sig)
		case e := <-failures:
			displayErr = e
			turn.InputLost()
		case e := <-events:
			if e.Result != nil {
				code, err, stop := finishInput(*e.Result, turn.Snapshot.InputID, out, diagnostic, displayErr, explicitStop)
				return code, err, stop || interrupted
			}
			if displayErr == nil {
				displayErr = show(diagnostic, e)
				if displayErr != nil {
					turn.InputLost()
				}
			}
		}
	}
}
func finishInput(result chat.Result, inputID string, out, diagnostic io.Writer, displayErr error, explicitStop bool) (int, error, bool) {
	answer, note := present.Result(result)
	if result.Uncertain {
		note += present.Reconciliation(inputID)
	}
	_, err := io.WriteString(out, answer)
	if err != nil {
		return chat.Error, err, true
	}
	_, err = io.WriteString(diagnostic, note)
	if err = errors.Join(displayErr, err); err != nil {
		// A confirmed explicit cancellation is still Stopped when its diagnostic
		// cannot be displayed. Keep the error for the caller, but preserve the
		// durable outcome. Answers and unresolved recovery output must succeed.
		if explicitStop && confirmedCancellation(result) {
			return chat.Stopped, err, true
		}
		return chat.Error, err, true
	}
	stop := explicitStop || result.NoInputFits || result.Code != chat.OK && result.Code != chat.Stopped && !(result.Rejected && result.Code == chat.Usage)
	return result.Code, nil, stop
}

func confirmedCancellation(result chat.Result) bool {
	return result.Code == chat.Stopped && result.Run.StopReason == "cancelled" &&
		result.Err == nil && !result.Uncertain && !result.Interrupted
}
func turnEvents(turn *chat.Turn) (<-chan chat.Event, func()) {
	events := make(chan chat.Event)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			e, err := turn.Next(ctx)
			if err != nil {
				return
			}
			select {
			case events <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, func() { cancel(); <-done }
}

func show(w io.Writer, e chat.Event) error {
	var b strings.Builder
	if e.Diagnostic != "" {
		fmt.Fprintln(&b, present.Clean(e.Diagnostic))
	}
	if e.Item != nil {
		present.Item(&b, *e.Item)
	}
	if e.Omitted > 0 {
		fmt.Fprintf(&b, "… %d progress entries omitted\n", e.Omitted)
	}
	if e.Ack != nil && len(e.Ack.Differs) > 0 {
		fmt.Fprintf(&b, "canti: input replay keeps its original settings: %s\n", strings.Join(e.Ack.Differs, ", "))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func signalTurn(turn *chat.Turn, sig os.Signal) {
	if sig == syscall.SIGHUP {
		turn.InputLost()
	} else {
		turn.Interrupt()
	}
}
