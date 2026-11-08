package main

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/charmbracelet/x/ansi"
	"golang.org/x/sys/unix"
)

func TestChatPTYDisplayFailureWaitsForCancellation(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "answer"}})
	held := f.model.holdRequests()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writeFD, readFD := int(w.Fd()), int(r.Fd())
	p := startPTYChat(t, f, w)
	p.awaitRaw(t)
	if err := unix.SetNonblock(readFD, true); err != nil {
		t.Fatal(err)
	}
	awaitFrame := func(needle string) {
		t.Helper()
		var output []byte
		var chunk [4096]byte
		deadline := time.Now().Add(5 * time.Second)
		for !bytes.Contains([]byte(ansi.Strip(string(output))), []byte(needle)) {
			n, err := unix.Read(readFD, chunk[:])
			if n > 0 {
				output = append(output, chunk[:n]...)
			}
			if err != nil && err != unix.EAGAIN {
				t.Fatal(err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("TUI never showed %q: %q", needle, output)
			}
			if n <= 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	awaitFrame("ready")
	p.send(t, "first\r")
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach provider")
	}
	awaitFrame("running")
	db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(t.Context(), "ROLLBACK")
	if err := unix.SetNonblock(writeFD, true); err != nil {
		t.Fatal(err)
	}
	fillLivePipe(t, writeFD)
	p.send(t, "discard while busy\rpartial\x03")
	time.Sleep(3 * 250 * time.Millisecond)
	if _, err := conn.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	p.finish(t, exitError)
	if s := f.status(); s.Phase != core.PhaseStopped || s.Reason != core.StopCancelled || s.Run.SessionSeq != 1 || s.Run.Goal != "first" || len(f.model.requests()) != 1 {
		t.Fatalf("display failure abandoned cancellation or admitted busy input: %+v, requests=%d", s, len(f.model.requests()))
	}
	f.model.mu.Lock()
	f.model.hold = nil
	f.model.mu.Unlock()
	if r := f.cli(nil, "run", "-session", "pty", "-input", "next", "-goal", "fresh"); r.code != exitOK {
		t.Fatalf("next session input failed: %+v", r)
	}
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 2 || s.Run.Goal != "fresh" || len(f.model.requests()) != 2 {
		t.Fatalf("session did not continue after cancellation: %+v", s)
	}
}
