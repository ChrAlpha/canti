package main

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	stderrQueueEntries = 64
	stderrEntryBytes   = 4096
	stderrDrainTime    = 100 * time.Millisecond
)

var stderrTruncated = []byte("... [stderr entry truncated]\n")
var errStderrEntryTooLarge = errors.New("stderr entry exceeds 4096 bytes")

type stderrEntry struct {
	data    []byte
	ack     chan error
	inputFD *int
}

type stderrWriter struct {
	mu      sync.Mutex
	queue   chan stderrEntry
	closing chan struct{}
	done    chan struct{}
	dropped atomic.Uint64
	closed  bool
	err     error
}

func newStderrWriter(out io.Writer) *stderrWriter {
	w := &stderrWriter{
		queue: make(chan stderrEntry, stderrQueueEntries), closing: make(chan struct{}), done: make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		write := func(entry stderrEntry) bool {
			var err error
			if entry.inputFD != nil {
				err = writePrompt(out, entry.data, *entry.inputFD, w.closing)
			} else if len(entry.data) > 0 {
				_, err = out.Write(entry.data)
			}
			if err == nil {
				if lost := w.dropped.Swap(0); lost != 0 {
					_, err = fmt.Fprintf(out, "canti: stderr dropped %d entries\n", lost)
				}
			}
			if entry.ack != nil {
				entry.ack <- err
			}
			if err != nil {
				w.mu.Lock()
				w.err = err
				w.mu.Unlock()
				return false
			}
			return true
		}
		for {
			select {
			case entry := <-w.queue:
				if !write(entry) {
					return
				}
			case <-w.closing:
				for {
					select {
					case entry := <-w.queue:
						if !write(entry) {
							return
						}
					default:
						return
					}
				}
			}
		}
	}()
	return w
}

func stderrBytes(p []byte) []byte {
	if len(p) <= stderrEntryBytes {
		return append([]byte(nil), p...)
	}
	end := stderrEntryBytes - len(stderrTruncated)
	for end > 0 && !utf8.RuneStart(p[end]) {
		end--
	}
	return append(append(make([]byte, 0, stderrEntryBytes), p[:end]...), stderrTruncated...)
}

func (w *stderrWriter) Write(p []byte) (int, error) {
	entry := stderrEntry{data: stderrBytes(p)}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	select {
	case <-w.done:
		w.dropped.Add(1)
	case w.queue <- entry:
	default:
		w.dropped.Add(1)
	}
	return len(p), nil
}

func (w *stderrWriter) WriteSync(p []byte) error {
	return w.writeSync(p, nil)
}

func (w *stderrWriter) Prompt(inputFD int) error {
	return w.writeSync([]byte("> "), &inputFD)
}

func (w *stderrWriter) writeSync(p []byte, inputFD *int) error {
	if len(p) > stderrEntryBytes {
		return errStderrEntryTooLarge
	}
	entry := stderrEntry{data: append([]byte(nil), p...), ack: make(chan error, 1), inputFD: inputFD}
	select {
	case <-w.closing:
		return io.ErrClosedPipe
	case <-w.done:
		return w.outputError()
	case w.queue <- entry:
	}
	select {
	case err := <-entry.ack:
		return err
	case <-w.closing:
		return io.ErrClosedPipe
	case <-w.done:
		return w.outputError()
	}
}

func (w *stderrWriter) Flush() error { return w.WriteSync(nil) }

type syncStderr struct{ w *stderrWriter }

func (s syncStderr) Write(p []byte) (int, error) {
	if err := s.w.WriteSync(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *stderrWriter) outputError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	return errors.New("stderr output stopped")
}

func (w *stderrWriter) Dropped() uint64 { return w.dropped.Load() }

func (w *stderrWriter) Close() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.closing)
	}
	w.mu.Unlock()
	select {
	case <-w.done:
	case <-time.After(stderrDrainTime):
	}
}
