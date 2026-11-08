package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/apps/tui/internal/session"
)

// Run gives native Tea exclusive input and terminal-mode ownership. The relay
// carries only OS signals and output faults, never bytes or decoded key events.
func Run(ctx context.Context, c *session.Controller, input *os.File, output io.Writer, signals <-chan os.Signal, failures <-chan error) (int, error) {
	m := New(ctx, c)
	p := tea.NewProgram(m, tea.WithInput(input), tea.WithOutput(output), tea.WithInputLimits(64<<10, c.GoalLimit), tea.WithWindowSize(80, 24), tea.WithoutSignalHandler())
	relayCtx, cancel := context.WithCancel(context.Background())
	var relay sync.WaitGroup
	relay.Add(1)
	go func() {
		defer relay.Done()
		for {
			select {
			case sig := <-signals:
				p.Send(signalMsg{sig})
			case err := <-failures:
				p.Send(outputError{err})
			case <-ctx.Done():
				p.Send(outputError{ctx.Err()})
				return
			case <-relayCtx.Done():
				return
			}
		}
	}()
	final, runErr := p.Run()
	cancel()
	relay.Wait()
	if final != nil {
		m = final.(Model)
	}
	if m.turn != nil {
		m.turn.InputLost()
		// A second external interrupt still escapes stalled owned cleanup after the
		// renderer has failed. Rendering never owns this wait.
		done := m.turn.Done()
		for done != nil {
			select {
			case <-done:
				done = nil
			case <-signals:
				m.turn.Interrupt()
			}
		}
		out := m.turn.Outcome()
		updated, _ := m.complete(out)
		m = updated.(Model)
	}
	if s := m.transcript(); s != "" {
		if _, err := io.WriteString(output, s); runErr == nil {
			runErr = err
		}
	}
	if m.final != "" && !stringsContainsFinal(m.transcript(), m.final) {
		_, _ = io.WriteString(output, m.final)
	}
	_, _ = fmt.Fprintf(output, "canti: continue session with \"canti-tui -session %s\"; new runs use the current launch flags.\n", c.ID())
	if runErr != nil && !errors.Is(runErr, tea.ErrProgramKilled) {
		return session.Error, runErr
	}
	if m.err != nil {
		return session.Error, m.err
	}
	return m.code, runErr
}
func stringsContainsFinal(transcript, final string) bool {
	return len(final) <= len(transcript) && transcript[len(transcript)-len(final):] == final
}
