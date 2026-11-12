package pipe

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/muesli/cancelreader"
)

// inputReader owns a single cancelable reader. It reads only when the pipe
// presenter requests the next input, and joins on every exit path.
type inputReader struct {
	reader        cancelreader.CancelReader
	requests      chan struct{}
	inputs        chan input
	closing, done chan struct{}
}

func newInputReader(in *os.File, prompt io.Writer, limit int) (*inputReader, error) {
	reader, err := cancelreader.NewReader(in)
	if err != nil {
		return nil, err
	}
	r := &inputReader{reader: reader, requests: make(chan struct{}), inputs: make(chan input), closing: make(chan struct{}), done: make(chan struct{})}
	go r.read(prompt, limit)
	return r, nil
}
func (r *inputReader) read(prompt io.Writer, limit int) {
	defer close(r.done)
	buf := bufio.NewReader(r.reader)
	for {
		select {
		case <-r.closing:
			return
		case <-r.requests:
			text, err := readInput(buf, prompt, limit)
			select {
			case r.inputs <- input{text, err}:
			case <-r.closing:
				return
			}
		}
	}
}
func (r *inputReader) Close() { close(r.closing); r.reader.Cancel(); <-r.done; r.reader.Close() }
func (r *inputReader) Next(ctx context.Context, signals <-chan os.Signal, failures <-chan error) (input, int, error) {
	if pendingSignal(signals) {
		return input{}, chat.Stopped, nil
	}
	select {
	case r.requests <- struct{}{}:
	case <-ctx.Done():
		return input{}, chat.Error, ctx.Err()
	}
	return r.await(ctx, signals, failures)
}

// await arbitrates a completed read against exit requests. A signal that was
// queued before the completed input is handled takes precedence even when the
// select chooses the input branch.
func (r *inputReader) await(ctx context.Context, signals <-chan os.Signal, failures <-chan error) (input, int, error) {
	select {
	case entry := <-r.inputs:
		if pendingSignal(signals) {
			return input{}, chat.Stopped, nil
		}
		return entry, chat.OK, nil
	case <-signals:
		return input{}, chat.Stopped, nil
	case err := <-failures:
		return input{}, chat.Error, err
	case <-ctx.Done():
		return input{}, chat.Error, ctx.Err()
	}
}

func pendingSignal(signals <-chan os.Signal) bool {
	select {
	case <-signals:
		return true
	default:
		return false
	}
}
