//go:build acceptance && integration && linux

package acceptance

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestChatPTYHangupCancelsTheDockerJob(t *testing.T) {
	f, image := recoveryFixture(t, map[int]decision{1: {Kind: "exec", Payload: "echo started > started; sleep 60"}})
	p := startPTYChatWithAttr(t, f, nil, nil, true, time.Minute, &syscall.SysProcAttr{Setsid: true, Setctty: true}, "-image", image)
	p.awaitStatus(t, "ready", 0)
	p.send(t, "start job\r")
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
	p.waited = true
	ended := make(chan error, 1)
	go func() { ended <- p.cmd.Wait() }()
	if err := p.master.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-time.After(stopCleanup):
		t.Fatal("chat did not exit after its terminal hung up")
	}
	s := f.status()
	if code := p.cmd.ProcessState.ExitCode(); code != exitError || s.Phase != "stopped" || s.Reason != "cancelled" || s.Run.Ordinal != 1 {
		t.Fatalf("hangup: %v, exit %d, run %+v", p.cmd.ProcessState, code, s)
	}
	checkReleased(t, f, f.history("-run", s.Run.ID))
}

func TestChatPTYInterruptsDockerJobAndKeepsLine(t *testing.T) {
	f, image := recoveryFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: "echo started > started; sleep 60"},
	})
	f.model.answer(1, 2, reply{text: `{"kind":"exec","payload":"echo second > second-started; sleep 60"}`})
	f.model.answer(1, 3, reply{text: `{"kind":"return","payload":"after interrupts"}`})
	p := startPTYChat(t, f, nil, "-image", image)
	ready := p.awaitStatus(t, "ready", 0)
	p.send(t, "start job\r")
	ready = p.awaitStatus(t, "running", ready)
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
	ready = p.awaitStatus(t, "ready", ready)
	stopped := f.status()
	if stopped.Phase != "stopped" || stopped.Reason != "cancelled" || stopped.Run.Ordinal != 1 {
		t.Fatalf("PTY Ctrl-C did not stop the Docker run: %+v", stopped)
	}
	firstHistory := f.history("-run", stopped.Run.ID)
	checkReleased(t, f, firstHistory)
	p.send(t, "second job\r")
	ready = p.awaitStatus(t, "running", ready)
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
	ready = p.awaitStatus(t, "ready", ready)
	stopped = f.status()
	if stopped.Phase != "stopped" || stopped.Reason != "cancelled" || stopped.Run.Ordinal != 2 {
		t.Fatalf("second PTY Ctrl-C did not stop the Docker run: %+v", stopped)
	}
	checkReleased(t, f, append(firstHistory, f.history("-run", stopped.Run.ID)...))
	p.send(t, "continue line\r")
	p.await(t, "after interrupts", ready)
	deadline = time.Now().Add(5 * time.Second)
	for f.status().Phase != "returned" {
		if time.Now().After(deadline) {
			t.Fatal("third run did not return")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s := f.status(); s.Phase != "returned" || s.Run.Ordinal != 3 || s.Run.Goal != "continue line" {
		t.Fatalf("next input did not complete: %+v", s)
	}
	prompts := f.model.prompts(1)
	if len(prompts) != 3 {
		t.Fatalf("%d requests, want exactly three line inputs", len(prompts))
	}
	checkLineLink(t, f, prompts[2], f.status().Run.ID, stopped.Run.ID, "continue line")
	firstID := archivePrevious(t, f, stopped.Run.ID)
	if firstID == "" || archivePrevious(t, f, firstID) != "" {
		t.Fatalf("previous run chain: %q", firstID)
	}
	for path, want := range map[string]string{
		firstID + "/goal": "start job", stopped.Run.ID + "/goal": "second job",
		firstID + "/result": "phase: stopped\nreason: cancelled\n", stopped.Run.ID + "/result": "phase: stopped\nreason: cancelled\n",
	} {
		if got := archiveText(t, f, "/canti/runs/"+path); got != want {
			t.Fatalf("archive %s=%q, want %q", path, got, want)
		}
	}

	p.send(t, "\x03")
	p.finish(t, exitOK)
}

// TestChatPTYCtrlXCancelsTheEffect requires that Ctrl+X cancels the accepted
// effect, that the run continues to its next step with a cancelled
// observation, and that a stop is not recorded.
func TestChatPTYCtrlXCancelsTheEffect(t *testing.T) {
	f, image := recoveryFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: "echo started > started; sleep 60"},
		2: {Kind: "return", Payload: "after cancel"},
	})
	p := startPTYChat(t, f, nil, "-image", image)
	pos := p.awaitStatus(t, "ready", 0)
	p.send(t, "start job\r")
	pos = p.awaitStatus(t, "running", pos)
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
	p.send(t, "\x18")
	pos = p.await(t, "└ cancelled", pos)
	deadline = time.Now().Add(30 * time.Second)
	for f.status().Phase != "returned" {
		if time.Now().After(deadline) {
			t.Fatalf("the run did not continue after cancel: %+v history=%q", f.status(), kinds(f.history("-run", f.status().Run.ID)))
		}
		time.Sleep(50 * time.Millisecond)
	}
	s := f.status()
	if s.Phase != "returned" || s.Reason != "" {
		t.Fatalf("cancel stopped the run or lost its return: %+v", s)
	}
	history := f.history("-run", s.Run.ID)
	cancelled := false
	for _, e := range history {
		if e.Kind == "observed" && e.Facts.Observation != nil && e.Facts.Observation.Execution == "cancelled" {
			cancelled = true
		}
	}
	if !cancelled {
		t.Fatalf("no cancelled observation in %q", kinds(history))
	}
	checkReleased(t, f, history)
	pos = p.awaitStatus(t, "ready", pos)
	p.send(t, "\x03")
	p.finish(t, exitOK)
}
