//go:build linux

package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"github.com/ChrAlpha/canti/internal/harness"
	uv "github.com/charmbracelet/ultraviolet"
	"github.com/charmbracelet/x/ansi"
	ansiparser "github.com/charmbracelet/x/ansi/parser"
	"github.com/charmbracelet/x/term"
	"golang.org/x/sys/unix"
)

const chatPasteEnd = "\x1b[201~"
const chatInputBufferLimit = 64 << 10

type chatKeys struct {
	epoch  uint64
	events []tea.Msg
}

type chatInputFailure struct{ err error }
type chatPasteOverflow struct{}

type chatInputGate struct {
	mu            sync.Mutex
	fd            int
	epoch         uint64
	busy          bool
	decoder       uv.EventDecoder
	pending       []byte
	paste         []byte
	inPaste       bool
	pasteOverflow bool
	draining      bool
	failed        bool
	framer        *ansi.Parser
}

func (g *chatInputGate) enterBusy() {
	g.mu.Lock()
	g.busy = true
	g.mu.Unlock()
}

func (g *chatInputGate) currentEpoch() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch
}

func (g *chatInputGate) discarding() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.draining
}

func (g *chatInputGate) reopen() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.busy = true
	g.draining = false
	if err := g.drainAvailableLocked(); err != nil {
		return err
	}
	g.epoch++
	if g.inPaste || len(g.pending) > 0 {
		g.draining = true
		g.paste = nil
		return nil
	}
	g.busy = false
	return nil
}

func (g *chatInputGate) finishDrainLocked() error {
	g.pending, g.paste, g.inPaste, g.pasteOverflow = nil, nil, false, false
	g.draining = false
	if err := g.drainAvailableLocked(); err != nil {
		return err
	}
	g.epoch++
	g.draining = g.inPaste || len(g.pending) > 0
	g.busy = g.draining
	return nil
}

func (g *chatInputGate) drainAvailableLocked() error {
	buf := make([]byte, 4096)
	total := 0
	for {
		n, err := unix.Read(g.fd, buf)
		if errors.Is(err, syscall.EAGAIN) {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.EOF
		}
		total += n
		if total > chatInputBufferLimit {
			return errors.New("terminal input drain exceeds limit")
		}
		batch := g.decodeLocked(buf[:n])
		for _, event := range batch.events {
			if failure, ok := event.(chatInputFailure); ok {
				return failure.err
			}
		}
	}
}

func (g *chatInputGate) decode(read []byte) chatKeys {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.decodeLocked(read)
}

func (g *chatInputGate) decodeLocked(read []byte) chatKeys {
	batch := chatKeys{epoch: g.epoch}
	if g.failed {
		return batch
	}
	if len(g.pending)+len(read) > chatInputBufferLimit {
		g.failed = true
		batch.events = append(batch.events, chatInputFailure{err: errors.New("terminal input sequence exceeds limit")})
		return batch
	}
	g.pending = append(g.pending, read...)
	for len(g.pending) > 0 {
		if g.inPaste {
			i := bytes.Index(g.pending, []byte(chatPasteEnd))
			if i < 0 {
				keep := len(chatPasteEnd) - 1
				consume := max(0, len(g.pending)-keep)
				g.appendPaste(g.pending[:consume])
				g.pending = g.pending[consume:]
				break
			}
			g.appendPaste(g.pending[:i])
			g.pending = g.pending[i+len(chatPasteEnd):]
			if g.draining {
				if err := g.finishDrainLocked(); err != nil {
					batch.events = append(batch.events, chatInputFailure{err: err})
				}
				break
			}
			if g.pasteOverflow {
				batch.events = append(batch.events, chatPasteOverflow{})
			} else {
				batch.events = append(batch.events, tea.PasteMsg{Content: string(g.paste)})
			}
			g.paste, g.inPaste, g.pasteOverflow = nil, false, false
			continue
		}
		if g.pending[0] == '\x1b' {
			if i := bytes.IndexByte(g.pending, 3); i >= 0 {
				g.pending = g.pending[i+1:]
				batch.events = append(batch.events, tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}))
				continue
			}
		}
		n := g.frameLength(g.pending)
		if n == 0 {
			if i := bytes.IndexByte(g.pending, 3); i >= 0 {
				g.pending = g.pending[i+1:]
				batch.events = append(batch.events, tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}))
				continue
			}
			break
		}
		_, ev := g.decoder.Decode(g.pending[:n])
		g.pending = g.pending[n:]
		if g.draining {
			if _, ok := ev.(uv.PasteStartEvent); ok {
				g.inPaste = true
				continue
			}
			if err := g.finishDrainLocked(); err != nil {
				batch.events = append(batch.events, chatInputFailure{err: err})
			}
			break
		}
		switch e := ev.(type) {
		case uv.PasteStartEvent:
			g.inPaste = true
		case uv.KeyPressEvent:
			key := tea.KeyPressMsg(e)
			if !g.busy || key.String() == "ctrl+c" {
				batch.events = append(batch.events, key)
			}
		case uv.KeyReleaseEvent:
			if !g.busy {
				batch.events = append(batch.events, tea.KeyReleaseMsg(e))
			}
		}
	}
	return batch
}

func (g *chatInputGate) appendPaste(p []byte) {
	if g.draining {
		return
	}
	limit := harness.DefaultPromptLimits().Goal
	if len(g.paste)+len(p) > limit {
		g.pasteOverflow = true
	}
	if len(g.paste) < limit {
		g.paste = append(g.paste, p[:min(len(p), limit-len(g.paste))]...)
	}
}

func (g *chatInputGate) frameLength(p []byte) int {
	if len(p) == 0 {
		return 0
	}
	if p[0] != '\x1b' {
		if p[0] >= utf8.RuneSelf && !utf8.FullRune(p) {
			return 0
		}
		if p[0] >= utf8.RuneSelf {
			_, n := utf8.DecodeRune(p)
			return n
		}
		return 1
	}
	if len(p) == 1 {
		return 0
	}
	switch p[1] {
	case '[', 'O', ']', 'P', '_', '^', 'X':
	default:
		if p[1] >= utf8.RuneSelf && !utf8.FullRune(p[1:]) {
			return 0
		}
		if p[1] >= utf8.RuneSelf {
			_, n := utf8.DecodeRune(p[1:])
			return n + 1
		}
		return 2
	}
	if p[1] == 'O' {
		if len(p) < 3 {
			return 0
		}
		return 3
	}
	if g.framer == nil {
		g.framer = ansi.NewParser()
	}
	g.framer.Reset()
	for i, b := range p {
		g.framer.Advance(b)
		if i > 0 && g.framer.State() == ansiparser.GroundState {
			return i + 1
		}
	}
	return 0
}

func startChatInput(f *os.File, send func(tea.Msg)) (*chatInputGate, func() error, error) {
	previous, err := term.MakeRaw(f.Fd())
	if err != nil {
		return nil, nil, err
	}
	fd := int(f.Fd())
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err == nil {
		err = unix.SetNonblock(fd, true)
	}
	if err != nil {
		_ = term.Restore(f.Fd(), previous)
		return nil, nil, err
	}
	g := &chatInputGate{fd: fd}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			n, err := unix.Poll(fds, 50)
			if errors.Is(err, syscall.EINTR) || n == 0 {
				continue
			}
			if err != nil {
				send(chatInputFailure{err: err})
				return
			}
			g.mu.Lock()
			read, err := unix.Read(fd, buf)
			batch := chatKeys{}
			if err == nil && read > 0 {
				batch = g.decodeLocked(buf[:read])
			}
			g.mu.Unlock()
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR) {
				continue
			}
			if err != nil || read == 0 {
				if err == nil {
					err = io.EOF
				}
				send(chatInputFailure{err: err})
				return
			}
			if len(batch.events) > 0 {
				send(batch)
			}
		}
	}()
	cleanup := func() error {
		close(stop)
		<-done
		_, flagErr := unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags)
		return errors.Join(flagErr, term.Restore(f.Fd(), previous))
	}
	return g, cleanup, nil
}
