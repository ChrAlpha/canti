package ui

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/apps/tui/internal/present"
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
	if err := writeExit(output, m, c.ID()); runErr == nil {
		runErr = err
	}
	return m.result(runErr)
}
func (m Model) result(runErr error) (int, error) {
	if runErr != nil {
		return session.Error, runErr
	}
	if m.err != nil {
		return max(m.code, session.Error), m.err
	}
	return m.code, nil
}

func writeExit(output io.Writer, m Model, id string) error {
	transcript := m.transcript()
	if transcript != "" {
		if _, err := io.WriteString(output, transcript); err != nil {
			return err
		}
	}
	if m.final != "" && !strings.HasSuffix(transcript, m.final) {
		if _, err := io.WriteString(output, m.final); err != nil {
			return err
		}
	}
	if m.pending != nil {
		_, err := io.WriteString(output, present.Reconciliation(m.pending.InputID, id))
		return err
	}
	_, err := fmt.Fprintf(output, "canti: continue session with \"canti-tui -session %s\"; new runs use the current launch flags.\n", id)
	return err
}
