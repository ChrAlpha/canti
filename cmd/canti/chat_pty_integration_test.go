//go:build integration && linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
)

func TestChatPTYInterruptsDockerJobAndKeepsSession(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo started > started; sleep 60"},
	})
	f.model.answer(1, 2, reply{text: `{"kind":"exec","payload":"echo second > second-started; sleep 60"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"after interrupts"}`})
	p := startPTYChat(t, f, nil, "-image", image)
	ready := p.await(t, "ready", 0)
	p.send(t, "start job\r")
	ready = p.await(t, "running", ready)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(f.workspace, "started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Docker command never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.send(t, "\x03")
	ready = p.await(t, "ready", ready)
	stopped := f.status()
	if stopped.Phase != core.PhaseStopped || stopped.Reason != core.StopCancelled || stopped.Run.SessionSeq != 1 {
		t.Fatalf("PTY Ctrl-C did not stop the Docker run: %+v", stopped)
	}
	firstHistory := f.history("-run", stopped.Run.ID)
	checkReleased(t, f, firstHistory)
	p.send(t, "second job\r")
	ready = p.await(t, "running", ready)
	deadline = time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(f.workspace, "second-started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second Docker command never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.send(t, "\x03")
	ready = p.await(t, "ready", ready)
	stopped = f.status()
	if stopped.Phase != core.PhaseStopped || stopped.Reason != core.StopCancelled || stopped.Run.SessionSeq != 2 {
		t.Fatalf("second PTY Ctrl-C did not stop the Docker run: %+v", stopped)
	}
	checkReleased(t, f, append(firstHistory, f.history("-run", stopped.Run.ID)...))
	p.send(t, "continue session\r")
	p.await(t, "after interrupts", ready)
	deadline = time.Now().Add(5 * time.Second)
	for f.status().Phase != core.PhaseReturned {
		if time.Now().After(deadline) {
			t.Fatal("third run did not return")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := f.status(); s.Phase != core.PhaseReturned || s.Run.SessionSeq != 3 || s.Run.Goal != "continue session" {
		t.Fatalf("next input did not complete: %+v", s)
	}
	prompts := f.model.prompts(1)
	if len(prompts) != 3 || !strings.Contains(prompts[2], "start job") || !strings.Contains(prompts[2], "second job") || strings.Count(prompts[2], "stopped, reason cancelled") != 2 {
		t.Fatalf("next model request lost stopped session context: %q", prompts)
	}
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
