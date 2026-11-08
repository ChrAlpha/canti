package uv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInputRecoveryBounds(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		want        error
	}{
		{"paste discard bytes", "\x1b[200~" + strings.Repeat("a", DefaultDiscardLimit+20), ErrInputLimit},
		{"paste recovery deadline", "\x1b[200~123456789", ErrInputTimeout},
		{"unterminated paste", "\x1b[200~ok", ErrInputTimeout},
		{"unterminated OSC", "\x1b]title", ErrInputTimeout},
		{"unterminated CSI", "\x1b[123;", ErrInputTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			defer w.Close()
			cr, err := NewCancelReader(r)
			if err != nil {
				t.Fatal(err)
			}
			defer cr.Close()
			reader := NewTerminalReader(cr, "xterm")
			reader.MaxPasteBytes = 8
			reader.IncompleteTimeout = DefaultInputTimeout
			events := make(chan Event, 16)
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { done <- reader.StreamEvents(ctx, events) }()
			go func() { _, _ = io.WriteString(w, tt.input) }()
			select {
			case err := <-done:
				if !errors.Is(err, tt.want) {
					t.Fatalf("got %v want %v", err, tt.want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("input did not terminate within bound")
			}
			for len(events) > 0 {
				e := <-events
				if _, ok := e.(KeyPressEvent); ok {
					t.Fatalf("malformed input escaped as key: %#v", e)
				}
			}
		})
	}
}

func TestInputReadPreservesBytesWithEOF(t *testing.T) {
	reader := NewTerminalReader(&lastRead{}, "xterm")
	reader.ReportEOF = true
	events := make(chan Event, 8)
	if err := reader.StreamEvents(context.Background(), events); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if e := <-events; e.(KeyPressEvent).Text != "x" {
		t.Fatal(e)
	}
}

type lastRead struct{}

func (*lastRead) Read(p []byte) (int, error) { p[0] = 'x'; return 1, io.EOF }

func TestPasteOverflowScannerBoundsAndRecovers(t *testing.T) {
	s := newEventScanner()
	s.maxPasteBytes = 8
	buf := []byte("\x1b[200~123456789\x1b[2")
	n, e := s.scanEvents(buf, false)
	if len(s.paste) > 8 || !s.pasteOverflow || len(e) != 1 {
		t.Fatalf("overflow: %d %#v", len(s.paste), e)
	}
	tail := append(append([]byte{}, buf[n:]...), []byte("01~y")...)
	_, e = s.scanEvents(tail, false)
	if len(e) != 2 {
		t.Fatalf("events %#v", e)
	}
	if _, ok := e[0].(PasteOverflowEvent); !ok {
		t.Fatal(e)
	}
	if k, ok := e[1].(KeyPressEvent); !ok || k.Text != "y" {
		t.Fatal(e)
	}
}

func TestBoundedInputPreservesAltControlIntroducers(t *testing.T) {
	for _, input := range []string{"\x1b]", "\x1b^", "\x1b]\x1b'", "\x1b^\x1b&"} {
		reader := NewTerminalReader(strings.NewReader(input), "xterm")
		reader.IncompleteTimeout = DefaultInputTimeout
		events := make(chan Event, 8)
		if err := reader.StreamEvents(context.Background(), events); err != nil {
			t.Fatalf("%q: %v", input, err)
		}
		if len(events) != len(input)/2 {
			t.Fatalf("%q: %d events", input, len(events))
		}
		for len(events) > 0 {
			if e := <-events; e.(KeyPressEvent).Mod != ModAlt {
				t.Fatal(e)
			}
		}
	}
}

func overflowReader(t *testing.T, input io.Reader) *TerminalReader {
	t.Helper()
	reader := NewTerminalReader(input, "xterm")
	reader.MaxPasteBytes = 8
	reader.ReportEOF = true
	reader.eventScanner.maxPasteBytes = 8
	reader.eventScanner.scanEvents([]byte("\x1b[200~123456789"), false)
	if !reader.eventScanner.pasteOverflow || reader.eventScanner.paste == nil {
		t.Fatal("scanner did not enter active overflow")
	}
	return reader
}
func TestEOFInConfirmedPasteOverflow(t *testing.T) {
	reader := overflowReader(t, strings.NewReader(""))
	if err := reader.StreamEvents(context.Background(), make(chan Event, 8)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

type heldInput struct {
	entered, release, exited chan struct{}
	once                     sync.Once
}

func (r *heldInput) Read([]byte) (int, error) {
	close(r.entered)
	<-r.release
	close(r.exited)
	return 0, io.EOF
}
func (r *heldInput) Cancel() bool { r.once.Do(func() { close(r.release) }); return true }
func (r *heldInput) Close() error { r.Cancel(); return nil }
func TestCancelInConfirmedPasteOverflowJoinsReader(t *testing.T) {
	input := &heldInput{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	reader := overflowReader(t, input)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- reader.StreamEvents(ctx, make(chan Event, 8)) }()
	select {
	case <-input.entered:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		input.Cancel()
		t.Fatal("overflow cancellation blocked")
	}
	select {
	case <-input.exited:
	default:
		t.Fatal("native read was not joined")
	}
}

type byteInput struct{ data []byte }

func (r *byteInput) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}
func TestSplitUTF8PasteAndKeys(t *testing.T) {
	for _, paste := range []bool{false, true} {
		input := "日🙂"
		if paste {
			input = "\x1b[200~" + input + "\x1b[201~"
		}
		reader := NewTerminalReader(&byteInput{[]byte(input)}, "xterm")
		reader.MaxPasteBytes = 64
		reader.ReportEOF = true
		reader.IncompleteTimeout = DefaultInputTimeout
		events := make(chan Event, 16)
		if err := reader.StreamEvents(context.Background(), events); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		var keys, content string
		for len(events) > 0 {
			switch e := (<-events).(type) {
			case KeyPressEvent:
				keys += e.Text
			case PasteEvent:
				content = e.Content
			}
		}
		if paste && (content != "日🙂" || keys != "") || !paste && (keys != "日🙂" || content != "") {
			t.Fatalf("paste=%v keys=%q content=%q", paste, keys, content)
		}
	}
}

func TestPasteDiscardBudgetIncludesUnfinishedDelimiter(t *testing.T) {
	for _, suffix := range []string{"", "\x1b[20"} {
		d := newEventScanner()
		d.maxPasteBytes = 8
		d.scanEvents([]byte("\x1b[200~123456789"), false)
		if !d.pasteOverflow {
			t.Fatal("missing overflow")
		}
		d.scanEvents([]byte(strings.Repeat("x", DefaultDiscardLimit-len(suffix))+suffix), false)
		if !errors.Is(d.limitErr, ErrInputLimit) {
			t.Fatalf("suffix=%q budget exhaustion did not terminate", suffix)
		}
	}
}

func TestContinuousInputCannotExtendAbsoluteRecoveryDeadline(t *testing.T) {
	for _, initial := range []string{"\x1b[200~123456789", "\x1b]11;"} {
		t.Run(fmt.Sprintf("%q", initial), func(t *testing.T) {
			r, w := io.Pipe()
			defer r.Close()
			defer w.Close()
			reader := NewTerminalReader(r, "xterm")
			reader.MaxPasteBytes = 8
			reader.IncompleteTimeout = DefaultInputTimeout
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			events := make(chan Event, 8)
			start := time.Now()
			go func() { done <- reader.StreamEvents(ctx, events) }()
			sent := make(chan struct{})
			go func() {
				defer close(sent)
				if _, err := io.WriteString(w, initial); err != nil {
					return
				}
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-tick.C:
						if _, err := io.WriteString(w, "x"); err != nil {
							return
						}
					}
				}
			}()
			select {
			case err := <-done:
				if !errors.Is(err, ErrInputTimeout) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("arriving bytes extended absolute deadline")
			}
			if elapsed := time.Since(start); elapsed < DefaultInputTimeout || elapsed > DefaultInputTimeout+500*time.Millisecond {
				t.Fatalf("recovery ended after %v", elapsed)
			}
			cancel()
			r.Close()
			w.Close()
			<-sent
		})
	}
}
