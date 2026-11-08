// Package pipe presents the same session facts as the UI, one input at a time.
package pipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/ChrAlpha/canti/apps/tui/internal/present"
	"github.com/ChrAlpha/canti/apps/tui/internal/session"
)

type input struct {
	text string
	err  error
}

func Run(ctx context.Context, c *session.Controller, in *os.File, out, diagnostic io.Writer, signals <-chan os.Signal, failures <-chan error) (int, error) {
	reader, err := newInputReader(in, diagnostic, c.GoalLimit)
	if err != nil {
		return session.Error, err
	}
	defer reader.Close()
	if _, err = fmt.Fprintf(diagnostic, "canti: session %s; continue it with \"canti-tui -session %[1]s\".\n", c.ID()); err != nil {
		return session.Error, err
	}
	for {
		if _, err = fmt.Fprint(diagnostic, "> "); err != nil {
			return session.Error, fmt.Errorf("showing chat prompt: %w", err)
		}
		entry, code, err := reader.Next(ctx, signals, failures)
		if code != session.OK || err != nil {
			return code, err
		}
		var opener *openerError
		var overflow *overflowError
		if errors.As(entry.err, &opener) || errors.As(entry.err, &overflow) {
			if _, e := fmt.Fprintf(diagnostic, "canti-tui: %v\n", entry.err); e != nil {
				return session.Error, e
			}
			if errors.Is(entry.err, io.EOF) {
				return session.OK, nil
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
			return session.OK, nil
		}
		if entry.err != nil {
			return session.Error, entry.err
		}
	}
}
func runInput(ctx context.Context, c *session.Controller, goal string, out, diagnostic io.Writer, signals <-chan os.Signal, failures <-chan error) (int, error, bool) {
	turn, err := c.Start(ctx, session.NewSnapshot(goal))
	if err != nil {
		return session.Error, err, true
	}
	events, closeEvents := turnEvents(turn)
	defer closeEvents()
	interrupted := false
	var displayErr error
	for {
		select {
		case sig := <-signals:
			interrupted = true
			signalTurn(turn, sig)
		case e := <-failures:
			displayErr = e
			turn.InputLost()
		case e := <-events:
			if e.Result != nil {
				return finishInput(*e.Result, turn.Snapshot.InputID, c.ID(), out, diagnostic, displayErr, interrupted)
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
func finishInput(result session.Result, inputID, sessionID string, out, diagnostic io.Writer, displayErr error, interrupted bool) (int, error, bool) {
	answer, note := present.Result(result)
	if result.Uncertain {
		note += present.Reconciliation(inputID, sessionID)
	}
	_, err := io.WriteString(out, answer)
	if err == nil {
		_, err = io.WriteString(diagnostic, note)
	}
	if displayErr != nil {
		return session.Error, displayErr, true
	}
	if err != nil {
		return session.Error, err, true
	}
	stop := interrupted || result.NoInputFits || result.Code != session.OK && result.Code != session.Stopped && !(result.Rejected && result.Code == session.Usage)
	return result.Code, nil, stop
}
func turnEvents(turn *session.Turn) (<-chan session.Event, func()) {
	events := make(chan session.Event)
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

func show(w io.Writer, e session.Event) error {
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

func signalTurn(turn *session.Turn, sig os.Signal) {
	if sig == syscall.SIGHUP {
		turn.InputLost()
	} else {
		turn.Interrupt()
	}
}
