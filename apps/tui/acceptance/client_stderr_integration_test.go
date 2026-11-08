//go:build acceptance && integration

package acceptance

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestChatStopsDockerWithBlockedStderr(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(fmt.Sprintf("abandon=%v", abandon), func(t *testing.T) {
			f, image := recoveryFixture(t, map[int]decision{
				1: {Kind: "exec", Payload: "echo started >> effects; sleep 60; echo finished >> effects"},
				2: {Kind: "return", Payload: "done"},
			})
			env := []string{"ANTHROPIC_API_KEY=" + f.key}
			blocked := filepath.Join(t.TempDir(), "cleanup")
			if abandon {
				env = append(env, "CANTI_TEST_CRASH=block:stop_requested", "CANTI_TEST_BLOCKED="+blocked)
			}
			cmd, blockOutput, wait := startChatWithStderr(t, f, env, false, "-image", image)
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("runtime evidence: status=%+v history=%v requests=%d", f.status(), kinds(f.history()), len(f.model.requests()))
				}
			})
			waitFor(t, func() bool {
				data, err := os.ReadFile(filepath.Join(f.workspace, "effects"))
				return err == nil && string(data) == "started\n"
			})
			started := f.status()
			if started.Phase != "effect_saved" || started.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("unexpected execution before interrupt: %+v", started)
			}
			t.Logf("before backpressure: phase=%s seq=%d session_seq=%d; effect wrote started", started.Phase, started.Seq, started.Run.SessionSeq)
			// Establish a running effect before applying backpressure. Filling
			// stderr before Submit races automatic owned output-loss cancellation
			// against Docker launch, so the effects marker may never be written.
			blockOutput()
			began := time.Now()
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if abandon {
				waitFor(t, func() bool { _, err := os.Stat(blocked); return err == nil })
				if err := cmd.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
			}
			r := wait()
			if elapsed := time.Since(began); elapsed > stopCleanup {
				t.Fatalf("stopping with unread stderr took %s", elapsed)
			}
			want := exitStopped
			if abandon {
				want = exitError
			}
			if r.code != want || r.stdout != "" {
				t.Fatalf("interrupted chat: %+v", r)
			}
			if s := f.status(); s.Run.ID != started.Run.ID || s.Run.SessionSeq != 1 || len(f.model.requests()) != 1 {
				t.Fatalf("queued input was executed: %+v", s)
			}
			if !abandon {
				if s := f.status(); s.Phase != "stopped" || s.Reason != "cancelled" {
					t.Fatalf("first signal did not finish cleanup: %+v", s)
				}
			}
			// Reopening proves the workspace lock was released, even after abandonment.
			if r := f.cli(nil, "stop"); r.code != exitStopped {
				t.Fatalf("finish cleanup: %+v", r)
			}
			if s := f.status(); s.Phase != "stopped" || s.Reason != "cancelled" || s.Totals.Steps != 1 {
				t.Fatalf("stopped result: %+v", s)
			}
			if data, err := os.ReadFile(filepath.Join(f.workspace, "effects")); err != nil || string(data) != "started\n" {
				t.Fatalf("execution duplicated or continued: %q, %v", data, err)
			}
			h := f.history()
			observed := 0
			for _, e := range h {
				if e.Facts.Observation != nil {
					observed++
					if e.Facts.Observation.Execution != "cancelled" {
						t.Fatalf("unexpected observation: %+v", e.Facts.Observation)
					}
				}
			}
			if observed != 1 {
				t.Fatalf("got %d observations, want one", observed)
			}
			checkReleased(t, f, h)
		})
	}
}
