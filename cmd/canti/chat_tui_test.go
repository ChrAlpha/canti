package main

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	tea "charm.land/bubbletea/v2"
)

func tuiKey(s string) tea.KeyPressMsg { return tea.KeyPressMsg(tea.Key{Text: s, Code: []rune(s)[0]}) }

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
