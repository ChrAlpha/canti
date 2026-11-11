package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
)

// setup consumes signals until the presenter takes over the same channel.
// First interrupt cancels admission/preflight; the second forces owned reaping.
func setup(ctx context.Context, c *chat.Controller, signals <-chan os.Signal) (int, error) {
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	shutdown, force := context.WithCancel(context.Background())
	defer force()
	done := make(chan error, 1)
	go func() { done <- c.Setup(work, shutdown) }()
	interrupted := false
	parentDone := ctx.Done()
	for {
		select {
		case err := <-done:
			if interrupted {
				return chat.Stopped, fmt.Errorf("startup interrupted")
			}
			if err != nil {
				return errorCode(err), err
			}
			return chat.OK, nil
		case <-signals:
			if interrupted {
				force()
			} else {
				interrupted = true
				cancel()
			}
		case <-parentDone:
			parentDone = nil
			cancel()
			force()
		}
	}
}
