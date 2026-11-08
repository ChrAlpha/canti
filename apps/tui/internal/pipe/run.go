// Package pipe presents the same session facts as the UI, one input at a time.
package pipe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/ChrAlpha/canti/apps/tui/internal/present"
	"github.com/ChrAlpha/canti/apps/tui/internal/session"
	"github.com/muesli/cancelreader"
)

type input struct {
	text string
	err  error
}

func Run(ctx context.Context, c *session.Controller, in *os.File, out, diagnostic io.Writer, signals <-chan os.Signal, failures <-chan error) (int, error) {
	reader, err := cancelreader.NewReader(in)
	if err != nil {
		return session.Error, err
	}
	requests := make(chan struct{})
	inputs := make(chan input)
	closing := make(chan struct{})
	var reads sync.WaitGroup
	reads.Add(1)
	go func() {
		defer reads.Done()
		buf := bufio.NewReader(reader)
		for {
			select {
			case <-closing:
				return
			case <-requests:
				text, err := readInput(buf, diagnostic, c.GoalLimit)
				select {
				case inputs <- input{text, err}:
				case <-closing:
					return
				}
			}
		}
	}()
	defer func() { close(closing); reader.Cancel(); reads.Wait(); reader.Close() }()
	if _, err = fmt.Fprintf(diagnostic, "canti: session %s; continue it with \"canti-tui -session %[1]s\".\n", c.ID()); err != nil {
		return session.Error, err
	}
	for {
		if _, err = fmt.Fprint(diagnostic, "> "); err != nil {
			return session.Error, fmt.Errorf("showing chat prompt: %w", err)
		}
		select {
		case requests <- struct{}{}:
		case <-ctx.Done():
			return session.Error, ctx.Err()
		}
		var entry input
		select {
		case entry = <-inputs:
		case <-signals:
			return session.Stopped, nil
		case err := <-failures:
			return session.Error, err
		case <-ctx.Done():
			return session.Error, ctx.Err()
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
	events := make(chan session.Event)
	relayCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var relay sync.WaitGroup
	relay.Add(1)
	go func() {
		defer relay.Done()
		for {
			e, err := turn.Next(relayCtx)
			if err != nil {
				return
			}
			select {
			case events <- e:
			case <-relayCtx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); relay.Wait() }()
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
				answer, note := present.Result(*e.Result)
				if _, err = io.WriteString(out, answer); err == nil {
					_, err = io.WriteString(diagnostic, note)
				}
				if displayErr != nil {
					return session.Error, displayErr, true
				}
				if err != nil {
					return session.Error, err, true
				}
				result := e.Result
				stop := interrupted || result.NoInputFits || result.Code != session.OK && result.Code != session.Stopped && !result.Rejected
				return result.Code, nil, stop
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
