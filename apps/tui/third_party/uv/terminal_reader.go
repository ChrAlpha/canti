package uv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"
	"github.com/muesli/cancelreader"
	"github.com/rivo/uniseg"
)

// ErrReaderNotStarted is returned when the reader has not been started yet.
var ErrReaderNotStarted = fmt.Errorf("reader not started")

// DefaultEscTimeout is the default timeout at which the [TerminalReader] will
// process ESC sequences. It is set to 50 milliseconds.
const DefaultEscTimeout = 50 * time.Millisecond

// DefaultInputLimit bounds native pending sequences and completed paste data.
const DefaultInputLimit = 64 << 10

// ErrInputLimit reports an input sequence that exceeds the configured bound.
var ErrInputLimit = errors.New("terminal input sequence exceeds limit")

// ErrInputTimeout reports an unterminated sequence or paste.
var ErrInputTimeout = errors.New("terminal input recovery deadline exceeded")

// DefaultDiscardLimit bounds additional bytes consumed after paste overflow.
const DefaultDiscardLimit = 64 << 10

// DefaultInputTimeout bounds incomplete input and overflow recovery.
const DefaultInputTimeout = 2 * time.Second

// PasteOverflowEvent rejects a whole paste after its closing delimiter.
type PasteOverflowEvent struct{ Limit int }

// TerminalReader represents an input event loop that reads input events from
// a reader and parses them into human-readable events. It supports
// reading escape sequences, mouse events, and bracketed paste mode.
type TerminalReader struct {
	EventDecoder

	// MouseMode determines whether mouse events are enabled or not. This is a
	// platform-specific feature and is only available on Windows. When this is
	// true, the reader will be initialized to read mouse events using the
	// Windows Console API.
	MouseMode *MouseMode

	// EscTimeout is the escape character timeout duration. Most escape
	// sequences start with an escape character [ansi.ESC] and are followed by
	// one or more characters. If the next character is not received within
	// this timeout, the reader will assume that the escape sequence is
	// complete and will process the received characters as a complete escape
	// sequence.
	//
	// By default, this is set to [DefaultEscTimeout] (50 milliseconds).
	EscTimeout time.Duration

	// MaxSequenceBytes and MaxPasteBytes bound native accumulation. Zero uses defaults.
	MaxSequenceBytes int
	MaxPasteBytes    int

	// IncompleteTimeout retains incomplete control strings for this duration, then
	// returns ErrInputTimeout. Zero preserves the escape ambiguity timeout.
	IncompleteTimeout time.Duration

	// ReportEOF lets the owner distinguish input closure from cancellation.
	ReportEOF bool

	r     io.Reader
	table map[string]Key // table is a lookup table for key sequences.

	term string // term is the terminal name $TERM.

	// paste is the bracketed paste mode buffer.
	// When nil, bracketed paste mode is disabled.
	paste []byte

	lookup bool // lookup indicates whether to use the lookup table for key sequences.

	// vtInput indicates whether we're using Windows Console API VT input mode.
	//nolint:unused,nolintlint
	vtInput bool

	eventScanner *eventScanner

	// We use these buffers to decode UTF-16 sequences and graphemes from the
	// Windows Console API and Win32-Input-Mode events.
	utf16Half   [2]bool    // 0 key up, 1 key down
	utf16Buf    [2][2]rune // 0 key up, 1 key down
	graphemeBuf [2][]rune  // 0 key up, 1 key down
	//nolint:unused,nolintlint
	lastMouseBtns uint32 // the last mouse button state for the previous event
	//nolint:unused,nolintlint
	lastWinsizeX, lastWinsizeY int16 // the last window size for the previous event to prevent multiple size events from firing

	logger Logger // The logger to use for debugging.
}

// NewTerminalReader returns a new input event reader. The reader streams input
// events from the terminal and parses escape sequences into human-readable
// events. It supports reading Terminfo databases.
//
// Use [TerminalReader.UseTerminfo] to use Terminfo defined key sequences.
// Use [TerminalReader.Legacy] to control legacy key encoding behavior.
//
// Example:
//
//	```go
//	var cr cancelreader.CancelReader
//	var evc chan Event
//	sc := NewTerminalReader(cr, os.Getenv("TERM"))
//	go sc.StreamEvents(ctx, evc)
//	```
func NewTerminalReader(r io.Reader, termType string) *TerminalReader {
	d := &TerminalReader{
		EscTimeout: DefaultEscTimeout,
		r:          r,
		term:       termType,
		lookup:     true, // Use lookup table by default.
	}
	d.r = r
	if d.table == nil {
		d.table = buildKeysTable(d.Legacy, d.term, d.UseTerminfo)
	}
	evs := newEventScanner()
	evs.EventDecoder = d.EventDecoder
	evs.table = d.table
	evs.lookup = d.lookup
	evs.setLogger(d.logger)
	d.eventScanner = evs
	return d
}

// readBufSize is the size of the read buffer used to read input events at a time.
const readBufSize = 4096

// sendBytes reads data from the reader and sends it to the provided channel.
// It stops when an error occurs or when the context is closed.
func (d *TerminalReader) sendBytes(ctx context.Context, readc chan []byte) error {
	for {
		var readBuf [readBufSize]byte
		n, err := d.r.Read(readBuf[:])
		if n > 0 {
			select {
			case <-ctx.Done():
				return nil
			case readc <- readBuf[:n]:
			}
		}
		if err != nil {
			return err
		}
	}
}

// StreamEvents sends events to the provided channel. It stops when the context
// is closed or when an error occurs.
func (d *TerminalReader) StreamEvents(ctx context.Context, eventc chan<- Event) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if d.MaxSequenceBytes <= 0 {
		d.MaxSequenceBytes = DefaultInputLimit
	}
	if d.MaxPasteBytes <= 0 {
		d.MaxPasteBytes = DefaultInputLimit
	}
	d.eventScanner.maxPasteBytes = d.MaxPasteBytes
	errc := make(chan error, 1)
	readc := make(chan []byte)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errc <- d.streamData(ctx, readc)
	}()
	defer func() {
		cancel()
		// Arbitrary io.Readers cannot always be interrupted. Native terminal readers
		// can, and their read goroutine must finish before its descriptors close.
		if reader, ok := d.r.(cancelreader.CancelReader); ok && reader.Cancel() {
			<-done
		}
	}()
	return d.readEvents(ctx, eventc, readc, errc)
}

func (d *TerminalReader) readEvents(ctx context.Context, eventc chan<- Event, readc <-chan []byte, errc <-chan error) error {
	var buf bytes.Buffer
	var pendingSince, lastRead time.Time
	interval := d.EscTimeout
	if interval <= 0 {
		interval = DefaultEscTimeout
	}
	tick := time.NewTimer(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			if ctx.Err() != nil || errors.Is(err, cancelreader.ErrCanceled) {
				return nil
			}
			if d.eventScanner.paste != nil || d.IncompleteTimeout > 0 && pendingControl(buf.Bytes()) {
				return io.ErrUnexpectedEOF
			}
			d.sendEvents(ctx, eventc, buf.Bytes(), true)
			if errors.Is(err, io.EOF) && !d.ReportEOF {
				return nil
			}
			return err
		case now := <-tick.C:
			if d.eventScanner.pasteOverflow && now.Sub(d.eventScanner.overflowAt) >= DefaultInputTimeout {
				return ErrInputTimeout
			}
			if d.IncompleteTimeout > 0 && !pendingSince.IsZero() && now.Sub(pendingSince) >= d.IncompleteTimeout {
				return ErrInputTimeout
			}
			if d.eventScanner.paste != nil && now.Sub(lastRead) >= DefaultInputTimeout {
				return ErrInputTimeout
			}
			if buf.Len() > 0 && now.Sub(lastRead) >= interval && (d.IncompleteTimeout <= 0 || !pendingControl(buf.Bytes())) && d.eventScanner.paste == nil {
				buf.Next(d.sendEvents(ctx, eventc, buf.Bytes(), true))
				if buf.Len() == 0 {
					pendingSince = time.Time{}
				}
			}
			tick.Reset(min(interval, 50*time.Millisecond))
		case read := <-readc:
			tick.Reset(interval)
			lastRead = time.Now()
			for len(read) > 0 {
				remaining := d.MaxSequenceBytes - buf.Len()
				if remaining <= 0 {
					return ErrInputLimit
				}
				take := min(len(read), remaining)
				buf.Write(read[:take])
				read = read[take:]
				n := d.sendEvents(ctx, eventc, buf.Bytes(), false)
				buf.Next(n)
				if d.eventScanner.limitErr != nil {
					return d.eventScanner.limitErr
				}
				if ctx.Err() != nil {
					return nil
				}
				if buf.Len() == 0 {
					pendingSince = time.Time{}
				} else if pendingSince.IsZero() || n > 0 {
					pendingSince = lastRead
				}
			}
		}
	}
}

// A control string or partial CSI is never converted to editable text merely
// because the escape-key ambiguity timer expired.
func pendingControl(b []byte) bool {
	if len(b) < 2 || b[0] != 0x1b {
		return false
	}
	switch b[1] {
	case ']', 'P', '_', '^', 'X':
		return len(b) > 2 && b[2] != 0x1b
	case '[':
		return len(b) > 2
	}
	return false
}

// SetLogger sets the logger to use for debugging. If nil, no logging will be
// performed.
func (d *TerminalReader) SetLogger(logger Logger) {
	d.logger = logger
}

func (d *TerminalReader) sendEvents(ctx context.Context, eventc chan<- Event, buf []byte, expired bool) int {
	n, events := d.eventScanner.scanEvents(buf, expired)
	for _, event := range events {
		select {
		case eventc <- event:
		case <-ctx.Done():
			return n
		}
	}
	return n
}

// eventScanner scans the buffer for events and sends them to the event channel.
type eventScanner struct {
	EventDecoder

	utf16Half     [2]bool    // 0 key up, 1 key down
	utf16Buf      [2][2]rune // 0 key up, 1 key down
	graphemeBuf   [2][]rune  // 0 key up, 1 key down
	paste         []byte
	maxPasteBytes int
	pasteOverflow bool
	discardBytes  int
	overflowAt    time.Time
	limitErr      error
	table         map[string]Key
	lookup        bool
	logger        Logger
}

// newEventScanner creates a new event scanner.
func newEventScanner() *eventScanner {
	return &eventScanner{maxPasteBytes: DefaultInputLimit}
}

// setLogger sets the logger to use for debugging. If nil, no logging will be
// performed.
func (d *eventScanner) setLogger(logger Logger) {
	d.logger = logger
}

func (d *eventScanner) logf(format string, v ...interface{}) {
	logf(d.logger, format, v...)
}

func (d *eventScanner) scanEvents(buf []byte, expired bool) (total int, events []Event) {
	if len(buf) == 0 {
		return 0, nil
	}

	var dn int
	d.logf("processing buf %d %q", len(buf), buf)
	dn, buf = d.deserializeWin32Input(buf)
	total += dn

	// Lookup table first
	if d.lookup && len(buf) > 2 && buf[0] == ansi.ESC {
		if k, ok := d.table[string(buf)]; ok {
			return len(buf), []Event{KeyPressEvent(k)}
		}
	}

	// total is the total number of bytes processed
	for len(buf) > 0 {
		if d.limitErr != nil {
			return total, events
		}
		if d.pasteOverflow {
			n, complete := d.discardPaste(buf)
			total += n
			buf = buf[n:]
			if !complete {
				return total, events
			}
			events = append(events, PasteOverflowEvent{Limit: d.maxPasteBytes})
			continue
		}
		if !utf8.FullRune(buf) {
			return total, events
		}
		// A paste delimiter may span slow reads; it is not an ambiguous Escape key.
		if d.paste != nil && len(buf) < len("\x1b[201~") && bytes.HasPrefix([]byte("\x1b[201~"), buf) {
			return total, events
		}
		esc := buf[0] == ansi.ESC
		n, event := d.Decode(buf)

		// Handle bracketed-paste
		if d.paste != nil { //nolint:nestif
			if _, ok := event.(PasteEndEvent); !ok {
				switch event := event.(type) {
				case KeyPressEvent:
					if len(event.Text) > 0 {
						d.appendPaste(event.Text)
					} else {
						seq := string(buf[:n])
						isWin32 := strings.HasPrefix(seq, "\x1b[") && strings.HasSuffix(seq, "_")
						switch {
						case isWin32 && event.Code == KeyEnter && event.Code == event.BaseCode:
							// This handles special cases where
							// win32-input-mode encodes newlines and other keys
							// as keypress events. We need to encode them as
							// their respective values.
							d.appendPaste("\n")
						case isWin32 && unicode.IsControl(event.Code) && event.Code == event.BaseCode:
							// This handles other cases such as tabs, escapes, etc.
							d.appendPaste(string(event.Code))
						case !isWin32:
							// We ignore all other non-text win32-input-mode events.
							if esc && n <= 2 && !expired {
								// If the event is an escape sequence and we
								// are not expired, we need to wait for more
								// input.
								return total, events
							}
							d.appendPaste(seq)
						}
					}
				case UnknownEvent:
					if !expired {
						// If the event is unknown and we are not expired, we
						// return need to try to decode the buffer again.
						return total, events
					}
				default:
					// Everything else is ignored...
				}
				buf = buf[n:]
				total += n
				continue
			}
		}

		var isUnknown bool
		switch event.(type) {
		case ignoredEvent:
			// ignore this event
			event = nil
		case UnknownEvent:
			isUnknown = true
			// Try to look up the event in the table.
			if !expired {
				return total, events
			}

			if k, ok := d.table[string(buf[:n])]; ok {
				events = append(events, KeyPressEvent(k))
				return total + n, events
			}

			events = append(events, event)
		case PasteStartEvent:
			d.paste = []byte{} // reset the paste buffer
			d.pasteOverflow = false
		case PasteEndEvent:
			if d.pasteOverflow {
				d.paste = nil
				d.pasteOverflow = false
				events = append(events, PasteOverflowEvent{Limit: d.maxPasteBytes})
				break
			}
			var paste []rune
			for len(d.paste) > 0 {
				r, w := utf8.DecodeRune(d.paste)
				if r != utf8.RuneError {
					paste = append(paste, r)
				}
				d.paste = d.paste[w:]
			}
			d.paste = nil // reset the paste buffer
			events = append(events, PasteEvent{string(paste)})
		}

		if !isUnknown && event != nil {
			if esc && n <= 2 && !expired {
				// Wait for more input
				return total, events
			}

			if m, ok := event.(MultiEvent); ok {
				// If the event is a MultiEvent, append all events to the queue.
				events = append(events, m...)
			} else {
				// Otherwise, just append the event to the queue.
				events = append(events, event)
			}
		}

		buf = buf[n:]
		total += n
	}

	return total, events
}

func (d *eventScanner) appendPaste(text string) {
	if d.pasteOverflow {
		return
	}
	if len(text) > d.maxPasteBytes-len(d.paste) {
		d.paste = []byte{}
		d.pasteOverflow = true
		d.discardBytes = 0
		d.overflowAt = time.Now()
		return
	}
	d.paste = append(d.paste, text...)
}

// discardPaste retains at most a split end marker, and counts raw bytes rather
// than decoded characters. No escape sequence inside rejected text can escape
// into the application as a key or interrupt.
func (d *eventScanner) discardPaste(buf []byte) (int, bool) {
	const end = "\x1b[201~"
	n := bytes.Index(buf, []byte(end))
	complete := n >= 0
	if complete {
		n += len(end)
	} else {
		n = len(buf)
		for tail := min(len(buf), len(end)-1); tail > 0; tail-- {
			if bytes.Equal(buf[len(buf)-tail:], []byte(end[:tail])) {
				n -= tail
				break
			}
		}
	}
	if n > DefaultDiscardLimit-d.discardBytes {
		d.limitErr = ErrInputLimit
		return 0, false
	}
	d.discardBytes += n
	if complete {
		d.paste = nil
		d.pasteOverflow = false
	}
	return n, complete
}

func (d *eventScanner) encodeGraphemeBufs() []byte {
	var b []byte
	for kd := range d.graphemeBuf {
		if len(d.graphemeBuf[kd]) > 0 {
			switch kd {
			case 1:
				b = append(b, string(d.graphemeBuf[kd])...)
			case 0:
				// Encode the release grapheme as Kitty Keyboard to get the release event.
				grs := uniseg.NewGraphemes(string(d.graphemeBuf[kd]))
				for grs.Next() {
					var codepoints string
					gr := grs.Str()
					for i, r := range gr {
						if r == 0 {
							continue
						}
						if i > 0 {
							codepoints += ":"
						}
						codepoints += strconv.FormatInt(int64(r), 10)
					}
					// This is dark :)
					// During serializing/deserializing of win32 input events, the
					// API will split a grapheme into runes and send them as a
					// keydown/keyup sequences. We collect the runes, decoded them
					// from UTF-16 just fine. However, [EventDecoder.Decode] will
					// always decode graphemes as [KeyPressEvent]s. Thus, to
					// workaround that and the existing API, while we are
					// intercepting win32 input events, we encode the grapheme
					// release events as Kitty Keyboard sequences so that
					// [EventDecoder.Decode] can properly decode them as
					// [KeyReleaseEvent]s.
					seq := fmt.Sprintf("\x1b[%d;1:3;%su", d.graphemeBuf[kd][0], codepoints)
					b = append(b, seq...)
				}
			}
			d.graphemeBuf[kd] = d.graphemeBuf[kd][:0] // reset the buffer
		}
	}
	return b
}

func (d *eventScanner) storeGraphemeRune(kd int, r rune) {
	if d.utf16Half[kd] {
		// We have a half pair that needs to be decoded.
		d.utf16Half[kd] = false
		d.utf16Buf[kd][1] = r
		r := utf16.DecodeRune(d.utf16Buf[kd][0], d.utf16Buf[kd][1])
		d.graphemeBuf[kd] = append(d.graphemeBuf[kd], r)
	} else if utf16.IsSurrogate(r) {
		// This is the first half of a UTF-16 surrogate pair.
		d.utf16Half[kd] = true
		d.utf16Buf[kd][0] = r
	} else {
		// This should be a single UTF-16 that can be converted
		// to UTF-8.
		d.graphemeBuf[kd] = append(d.graphemeBuf[kd], r)
	}
}

// deserializeWin32Input deserializes the Win32 input events converting
// KeyEventRecrods to bytes. Before returning the bytes, it will also try to
// decode any UTF-16 pairs that might be present in the input buffer.
func (d *eventScanner) deserializeWin32Input(buf []byte) (int, []byte) {
	p := ansi.GetParser()
	defer ansi.PutParser(p)

	var processed int
	var state byte
	des := make([]byte, 0, len(buf))

	for len(buf) > 0 {
		seq, width, n, newState := ansi.DecodeSequence(buf, state, p)
		switch width {
		case 0:
			if p.Command() == '_' { // Win32 Input Mode
				vk, _ := p.Param(0, 0)
				if vk == 0 {
					// This is either a serialized KeyEventRecord or a UTF-16
					// pair.
					uc, _ := p.Param(2, 0)
					kd, _ := p.Param(3, 0)
					kd = clamp(kd, 0, 1) // kd is the key down state (0 or 1)
					d.storeGraphemeRune(kd, rune(uc))
					processed += n
					break
				}
			}
			fallthrough
		default:
			des = append(des, d.encodeGraphemeBufs()...)
			des = append(des, seq...)
		}

		state = newState
		buf = buf[n:]
	}

	des = append(des, d.encodeGraphemeBufs()...)

	return processed, des
}

func (d *TerminalReader) logf(format string, v ...interface{}) {
	logf(d.logger, format, v...)
}

func logf(logger Logger, format string, v ...interface{}) {
	if logger == nil {
		return
	}
	logger.Printf(format, v...)
}
