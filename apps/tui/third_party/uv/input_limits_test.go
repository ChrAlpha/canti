package uv

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
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
