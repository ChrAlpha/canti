package main

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

func tuiKey(s string) tea.KeyPressMsg { return tea.KeyPressMsg(tea.Key{Text: s, Code: []rune(s)[0]}) }

func TestChatTUIBusyReadOnlyScroll(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		for _, decoded := range []bool{false, true} {
			g := new(chatInputGate)
			g.enterBusy()
			m := newChatTUI("session", func() { t.Fatal("busy input prepared a run") }, func(string) (int, error, os.Signal) {
				t.Fatal("busy input submitted a run")
				return exitError, nil, nil
			}, nil, nil, nil, new(chatDisplay))
			m.inputGate, m.busy, m.stopping = g, true, stopping
			m.editor.SetValue("unchanged")
			m.addTranscript(strings.Repeat("history\n", 80))
			bottom := m.steps.YOffset()
			update := func(msg tea.Msg) {
				updated, cmd := m.Update(msg)
				m = updated.(chatTUI)
				if cmd != nil {
					t.Fatal("read-only navigation or discarded input produced a command")
				}
			}
			send := func(code rune, raw string) {
				if decoded {
					for _, b := range []byte(raw) {
						update(g.decode([]byte{b}))
					}
				} else {
					update(tea.KeyPressMsg(tea.Key{Code: code}))
				}
			}
			send(tea.KeyPgUp, "\x1b[5~")
			if m.steps.YOffset() >= bottom {
				t.Fatalf("busy PgUp did not scroll: decoded=%v stopping=%v", decoded, stopping)
			}
			scrolled := m.steps.YOffset()
			m.display.Write([]byte("new output\n"))
			m.drainOutput()
			if m.steps.YOffset() != scrolled {
				t.Fatal("new output moved the reader to the bottom")
			}
			for _, msg := range []tea.Msg{tuiKey("x"), tea.PasteMsg{Content: "queued\x1b[6~"}, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}), tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter, Mod: tea.ModAlt})} {
				update(msg)
			}
			update(g.decode([]byte("discard\r\x1b[200~paste\x1b[6~\x1b[201~")))
			if m.editor.Value() != "unchanged" || m.steps.YOffset() != scrolled || !m.busy || m.stopping != stopping {
				t.Fatal("busy input changed the editor, viewport, or run state")
			}
			update(chatKeys{epoch: g.currentEpoch() + 1, events: []tea.Msg{tea.KeyPressMsg(tea.Key{Code: tea.KeyPgDown})}})
			if m.steps.YOffset() != scrolled {
				t.Fatal("stale navigation changed the viewport")
			}
			send(tea.KeyPgDown, "\x1b[6~")
			if m.steps.YOffset() <= scrolled {
				t.Fatalf("busy PgDown did not scroll: decoded=%v stopping=%v", decoded, stopping)
			}
		}
	}
}

func TestChatTUIBusyDiscardsInputAndStopsOnce(t *testing.T) {
	var stopped atomic.Int32
	m := newChatTUI("session", nil, nil, func(os.Signal) { stopped.Add(1) }, nil, nil, new(chatDisplay))
	m.editor.SetValue("first")
	updated, cmd := m.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	m = updated.(chatTUI)
	if !m.busy || cmd == nil || m.editor.Value() != "" {
		t.Fatalf("submit: busy=%v, cmd=%v, editor=%q", m.busy, cmd, m.editor.Value())
	}
	for _, msg := range []tea.Msg{tuiKey("x"), tea.PasteMsg{Content: "queued"}, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter})} {
		updated, _ = m.Update(msg)
		m = updated.(chatTUI)
	}
	if m.editor.Value() != "" || strings.Contains(m.transcript, "queued") {
		t.Fatalf("busy input was retained: %q, %q", m.editor.Value(), m.transcript)
	}
	for i := 0; i < 2; i++ {
		updated, _ = m.Update(tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}))
		m = updated.(chatTUI)
	}
	if stopped.Load() != 2 || !m.stopping || !m.busy {
		t.Fatalf("stop: calls=%d busy=%v stopping=%v", stopped.Load(), m.busy, m.stopping)
	}
	updated, _ = m.Update(chatDone{code: exitStopped})
	m = updated.(chatTUI)
	if m.busy || m.stopping || m.editor.Value() != "" {
		t.Fatalf("after stop: %+v", m)
	}
}

func TestChatTUIOutputAndTranscriptStayBounded(t *testing.T) {
	d := new(chatDisplay)
	big := strings.Repeat("A", chatDisplayLimit+100) + "\x1b[31mEND\x1b[0m\x00"
	if n, err := d.Write([]byte(big)); n != len(big) || err != nil {
		t.Fatalf("write: %d, %v", n, err)
	}
	chunk := d.Drain()
	if len(chunk) > chatDisplayLimit+64 || !strings.Contains(chunk, "END") || strings.Contains(chunk, "\x1b") || strings.Contains(chunk, "[31m") {
		t.Fatalf("unsafe or unbounded display: len=%d tail=%q", len(chunk), chunk[max(0, len(chunk)-100):])
	}
	if d.Drain() != "" {
		t.Fatal("display was not drained")
	}
	m := newChatTUI("session", nil, nil, nil, nil, nil, d)
	m.addTranscript(strings.Repeat("B", chatTranscriptLimit*2))
	if len(m.transcript) > chatTranscriptLimit || !strings.HasSuffix(m.transcript, "BBB") || !strings.Contains(m.steps.View(), "earlier transcript omitted") {
		t.Fatalf("transcript: len=%d", len(m.transcript))
	}
}

func TestChatTUIFailureStopsThenExits(t *testing.T) {
	var stopped atomic.Int32
	m := newChatTUI("session", nil, nil, func(os.Signal) { stopped.Add(1) }, nil, nil, new(chatDisplay))
	m.busy = true
	updated, _ := m.Update(chatOutputFailure{err: errors.New("terminal closed")})
	m = updated.(chatTUI)
	if !m.busy || !m.stopping || stopped.Load() != 1 {
		t.Fatalf("output failure did not stop active run: %+v", m)
	}
	updated, cmd := m.Update(chatDone{code: exitStopped})
	m = updated.(chatTUI)
	if cmd == nil || m.err == nil || m.busy {
		t.Fatalf("completion did not exit: %+v", m)
	}
}

func TestChatTUIPreparesEachAcceptedInputBeforeSubmit(t *testing.T) {
	var prepared atomic.Int32
	m := newChatTUI("session", func() { prepared.Add(1) }, nil, nil, nil, nil, new(chatDisplay))
	m.editor.SetValue("first")
	updated, _ := m.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	m = updated.(chatTUI)
	if prepared.Load() != 1 || !m.busy {
		t.Fatalf("first submit: prepared=%d busy=%v", prepared.Load(), m.busy)
	}
	updated, _ = m.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	m = updated.(chatTUI)
	if prepared.Load() != 1 {
		t.Fatal("busy Enter prepared queued input")
	}
	updated, _ = m.Update(chatDone{code: exitOK})
	m = updated.(chatTUI)
	m.editor.SetValue("second")
	updated, _ = m.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	m = updated.(chatTUI)
	if prepared.Load() != 2 {
		t.Fatalf("second submit reused control channel: %d", prepared.Load())
	}
}

func TestChatTUIRejectsQueuedPreviousEpoch(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	fd := int(slave.Fd())
	if err := unix.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	g := &chatInputGate{fd: fd}
	first := g.decode([]byte("first\r"))
	queued := g.decode([]byte("stale\r"))
	m := newChatTUI("session", nil, nil, nil, nil, nil, new(chatDisplay))
	m.inputGate = g
	updated, _ := m.Update(first)
	m = updated.(chatTUI)
	if !m.busy || !strings.Contains(m.transcript, "first") {
		t.Fatalf("first input not submitted: %+v", m)
	}
	updated, _ = m.Update(chatDone{code: exitOK})
	m = updated.(chatTUI)
	if m.busy || g.currentEpoch() != 1 {
		t.Fatalf("input not reopened: %+v", m)
	}
	updated, _ = m.Update(queued)
	m = updated.(chatTUI)
	if m.editor.Value() != "" || m.busy || strings.Contains(m.transcript, "stale") {
		t.Fatalf("old batch became new input: %+v", m)
	}
	updated, _ = m.Update(g.decode([]byte("fresh\r")))
	m = updated.(chatTUI)
	if !m.busy || !strings.Contains(m.transcript, "fresh") {
		t.Fatalf("new epoch did not advance: %+v", m)
	}
}

func TestChatInputDecoderKeepsSplitSequencesAndRejectsLongPaste(t *testing.T) {
	g := new(chatInputGate)
	if got := g.decode([]byte("\xe4")); len(got.events) != 0 {
		t.Fatalf("partial UTF-8 emitted: %+v", got.events)
	}
	if got := g.decode([]byte("\xbd\xa0")); len(got.events) != 1 || got.events[0].(tea.KeyPressMsg).Key().Text != "你" {
		t.Fatalf("UTF-8 split: %+v", got.events)
	}
	if got := g.decode([]byte("\x1b")); len(got.events) != 0 {
		t.Fatalf("partial Alt emitted: %+v", got.events)
	}
	if got := g.decode([]byte("\r")); len(got.events) != 1 || got.events[0].(tea.KeyPressMsg).String() != "alt+enter" {
		t.Fatalf("Alt+Enter split: %+v", got.events)
	}
	if got := g.decode([]byte("\x1b[")); len(got.events) != 0 {
		t.Fatalf("partial CSI emitted: %+v", got.events)
	}
	if got := g.decode([]byte("A")); len(got.events) != 1 || got.events[0].(tea.KeyPressMsg).String() != "up" {
		t.Fatalf("arrow split: %+v", got.events)
	}
	g.decode([]byte("\x1b[200~"))
	for i := 0; i < 12; i++ {
		g.decode([]byte(strings.Repeat("x", 4096)))
	}
	if got := g.decode([]byte("\x1b[201~")); len(got.events) != 1 {
		t.Fatalf("long paste result: %+v", got.events)
	} else if _, ok := got.events[0].(chatPasteOverflow); !ok {
		t.Fatalf("long paste was silently truncated: %T", got.events[0])
	}
}

func TestChatInputSplitPasteAndControlC(t *testing.T) {
	for split := 1; split < len("\x1b[200~"); split++ {
		g := new(chatInputGate)
		input := "\x1b[200~line one\nline two\r\x1b[201~"
		events := g.decode([]byte(input[:split])).events
		for i := split; i < len(input); i++ {
			events = append(events, g.decode([]byte{input[i]}).events...)
		}
		if len(events) != 1 {
			t.Fatalf("paste opener split at %d emitted %d events: %v", split, len(events), events)
		}
		paste, ok := events[0].(tea.PasteMsg)
		if !ok || paste.Content != "line one\nline two\r" {
			t.Fatalf("paste opener split at %d: %#v", split, events[0])
		}
	}
	for _, prefix := range []string{"\x1b", "\x1b["} {
		g := new(chatInputGate)
		g.enterBusy()
		g.decode([]byte(prefix))
		got := g.decode([]byte{3})
		if len(got.events) != 1 || got.events[0].(tea.KeyPressMsg).String() != "ctrl+c" {
			t.Fatalf("busy Ctrl-C after %q: %+v", prefix, got.events)
		}
	}
}

func TestChatInputContinuesDroppingPasteAcrossRunCompletion(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	fd := int(slave.Fd())
	if err := unix.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	g := &chatInputGate{fd: fd}
	g.enterBusy()
	g.decode([]byte("\x1b[200~old paste"))
	if err := g.reopen(); err != nil || !g.discarding() {
		t.Fatalf("did not preserve paste boundary: %v", err)
	}
	if got := g.decode([]byte(" tail\n\x1b[201~stale\r")); len(got.events) != 0 || g.discarding() {
		t.Fatalf("paste tail leaked into new input: %+v", got)
	}
	if got := g.decode([]byte("fresh\r")); len(got.events) != 6 {
		t.Fatalf("fresh input unavailable: %+v", got.events)
	}
}

func TestChatInputContinuesDroppingPageKeyAcrossRunCompletion(t *testing.T) {
	for _, page := range []string{"\x1b[5~", "\x1b[6~"} {
		for split := 1; split < len(page); split++ {
			master, slave, err := pty.Open()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { master.Close(); slave.Close() })
			fd := int(slave.Fd())
			if err := unix.SetNonblock(fd, true); err != nil {
				t.Fatal(err)
			}
			g := &chatInputGate{fd: fd}
			g.enterBusy()
			if got := g.decode([]byte(page[:split])); len(got.events) != 0 {
				t.Fatalf("partial page key emitted: %+v", got)
			}
			if err := g.reopen(); err != nil || !g.discarding() {
				t.Fatalf("did not preserve page key boundary: %v", err)
			}
			if got := g.decode([]byte(page[split:] + "stale\r")); len(got.events) != 0 || g.discarding() {
				t.Fatalf("old page key or batch tail crossed reopening: %+v", got)
			}
			if got := g.decode([]byte("fresh\r")); len(got.events) != 6 {
				t.Fatalf("fresh input unavailable: %+v", got)
			}
		}
	}
}

func TestChatInputPasteControlByteDoesNotStopRun(t *testing.T) {
	for _, parts := range [][]string{
		{"\x1b[200~a\x03b\n\x1b[201~"},
		{"\x1b[20", "0~a\x03b\n\x1b[201~"},
	} {
		g := new(chatInputGate)
		var events []tea.Msg
		for _, part := range parts {
			events = append(events, g.decode([]byte(part)).events...)
		}
		if len(events) != 1 {
			t.Fatalf("paste emitted %d events: %v", len(events), events)
		}
		paste, ok := events[0].(tea.PasteMsg)
		if !ok || paste.Content != "a\x03b\n" {
			t.Fatalf("paste control byte changed meaning: %#v", events[0])
		}
	}
}

func TestChatInputOverflowCannotBeLostAtRunCompletion(t *testing.T) {
	g := new(chatInputGate)
	g.enterBusy()
	g.decode([]byte("\x1b["))
	var failure chatKeys
	for i := 0; i < 20; i++ {
		failure = g.decode([]byte(strings.Repeat("1", 4096)))
	}
	if !g.failed || len(failure.events) != 0 {
		t.Fatalf("overflow state or subsequent data: failed=%v events=%v", g.failed, failure.events)
	}
	m := newChatTUI("session", nil, nil, nil, nil, nil, new(chatDisplay))
	m.inputGate = g
	m.busy = true
	updated, cmd := m.Update(chatDone{code: exitOK})
	m = updated.(chatTUI)
	if m.code != exitError || m.err == nil || cmd == nil || !strings.Contains(m.err.Error(), "exceeds limit") {
		t.Fatalf("overflow lost after completion: code=%d err=%v cmd=%v", m.code, m.err, cmd)
	}
}
