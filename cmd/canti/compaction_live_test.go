//go:build integration && live

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

// Three inputs continue one real file task. Every transition is driven by a
// fresh CLI process; the provider and Docker executor are both real. Each run
// has 12 business steps, 600k budget tokens, and a four-minute wall-time bound.
func TestLiveCompactionContinuation(t *testing.T) {
	if os.Getenv("CANTI_LIVE_COMPACTION") != "1" {
		t.Skip("set CANTI_LIVE_COMPACTION=1 for bounded provider acceptance")
	}
	if os.Getenv("ANTHROPIC_API_KEY") == "" && os.Getenv("ANTHROPIC_AUTH_TOKEN") == "" {
		t.Skip("model authentication is not configured")
	}
	root := t.TempDir()
	if evidence := os.Getenv("CANTI_LIVE_EVIDENCE_DIR"); evidence != "" {
		if err := os.MkdirAll(evidence, 0700); err != nil {
			t.Fatal(err)
		}
		var err error
		root, err = os.MkdirTemp(evidence, "continuation-")
		if err != nil {
			t.Fatal(err)
		}
	}
	workspace, state := filepath.Join(root, "workspace"), filepath.Join(root, "state")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte("customer,amount,status\nalpha,10,paid\nbeta,25,paid\nalpha,5,refunded\ngamma,100,cancelled\nbeta,7,paid\ngamma,3,paid\n")
	rules := []byte("Create a reusable POSIX report.sh that reads orders.csv and writes report.txt. Exclude cancelled orders, aggregate integer amounts by customer, sort customer ascending, and use the exact header customer,total. Inspect the inputs before implementing. Preserve orders.csv and this requirements.md. Initialize effects.log with exactly one line initialized when implementing, and preserve that line across later changes. Verify the result by running report.sh.\n")
	for name, content := range map[string][]byte{"orders.csv": data, "requirements.md": rules} {
		if err := os.WriteFile(filepath.Join(workspace, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var env []string
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "ANTHROPIC_") {
			env = append(env, value)
		}
	}
	// Only the CLI receives these; direct Docker inspection helpers must not.
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "")
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = "claude-opus-5"
	}
	image := os.Getenv("CANTI_TEST_IMAGE")
	if image == "" {
		image = "alpine:3.23"
	}
	location := []string{"-workspace", workspace, "-state", state}
	defer func() {
		if _, err := os.Stat(filepath.Join(state, "canti.db")); err == nil {
			_ = runCLI(t, nil, append([]string{"stop"}, location...)...)
			for _, c := range effectContainers(t, state) {
				if c.state == "running" {
					t.Errorf("owned effect %s remains running", c.effect)
				}
			}
		}
	}()
	goals := []string{
		"Inspect requirements.md and orders.csv, then return a concise implementation plan. Make no file changes yet.",
		"Implement the report task we planned. Read the original requirements as needed, create report.sh and report.txt, initialize the single audit line, run the script, and verify its result.",
		"Correction: refunded orders must now also be excluded, and report rows must be sorted by total descending, then customer ascending for ties. Update and rerun report.sh. Preserve the input files and the existing audit line; do not initialize it again.",
	}
	var runs []string
	deadline := time.Now().Add(13 * time.Minute)
	for i, goal := range goals {
		args := append([]string{"run"}, location...)
		args = append(args, "-once", "-session", "live-compact", "-input", fmt.Sprintf("input-%d", i+1), "-goal", goal,
			"-constraints", "Never modify orders.csv or requirements.md. Keep the existing effects.log line unchanged after initialization. A later explicit user correction overrides the initial report rules.",
			"-model", model, "-image", image, "-max-tokens", "4096", "-max-steps", "12", "-max-attempts", "3",
			"-token-budget", "600000", "-wall-time", "4m", "-job-timeout", "15s", "-context-bytes", "131072",
			"-summary-bytes", "4096", "-summary-tokens", "2048", "-compact-steps", "2", "-retain-bytes", "1024", "-fallbacks=false")
		if os.Getenv("CANTI_LIVE_STRUCTURED") == "0" {
			args = append(args, "-structured=false")
		}
		if r := runCLI(t, env, args...); r.code != exitActive {
			t.Fatalf("input %d admission code=%d: %s", i+1, r.code, r.stderr)
		}
		var current status
		for n := 0; ; n++ {
			if n >= 200 || time.Now().After(deadline) {
				t.Fatal("live task exceeded its process/time bound")
			}
			r := runCLI(t, nil, append([]string{"status"}, location...)...)
			if r.code != exitOK || json.Unmarshal([]byte(r.stdout), &current) != nil {
				t.Fatal("could not inspect live state")
			}
			if current.Phase == core.PhaseReturned {
				break
			}
			if current.Phase == core.PhaseStopped || current.Phase == core.PhaseNeedsResolution {
				t.Fatalf("live task stopped: phase=%s reason=%s state=%s", current.Phase, current.Reason, state)
			}
			r = runCLI(t, env, append(append([]string{"resume"}, location...), "-once")...)
			if r.code != exitActive && r.code != exitOK {
				t.Fatalf("live resume code=%d: %s", r.code, r.stderr)
			}
		}
		runs = append(runs, current.Run.ID)
		for name, want := range map[string][]byte{"orders.csv": data, "requirements.md": rules} {
			got, err := os.ReadFile(filepath.Join(workspace, name))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("modified protected input %s", name)
			}
		}
		if i == 0 {
			files, err := os.ReadDir(workspace)
			if err != nil || len(files) != 2 {
				t.Fatal("the inspection-only input changed workspace files")
			}
		}
		if i > 0 {
			want := "customer,total\nalpha,15\nbeta,32\ngamma,3\n"
			if i == 2 {
				want = "customer,total\nbeta,32\nalpha,10\ngamma,3\n"
			}
			got, err := os.ReadFile(filepath.Join(workspace, "report.txt"))
			if err != nil || string(got) != want {
				t.Fatalf("input %d report mismatch: %q (%v)", i+1, got, err)
			}
			log, err := os.ReadFile(filepath.Join(workspace, "effects.log"))
			if err != nil || string(log) != "initialized\n" {
				t.Fatalf("initialization repeated or missing: %q (%v)", log, err)
			}
		}
		t.Logf("LIVE_CONTINUATION input=%d run=%s steps=%d summary_input=%d summary_output=%d unknown_reserved=%d", i+1, current.Run.ID, current.Totals.Steps, current.Totals.SummaryInputTokens, current.Totals.SummaryOutputTokens, current.Totals.ReservedUnknownTokens)
	}
	store, err := sqlite.OpenReadOnly(context.Background(), filepath.Join(state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	compactions, derived := 0, 0
	for _, id := range runs {
		operations, err := store.Compactions(context.Background(), id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range operations {
			if record.Operation.Accepted {
				compactions++
			}
			if record.Operation.Plan.Prefix != nil {
				derived++
			}
		}
	}
	if compactions < 2 || derived == 0 {
		t.Fatalf("insufficient real compaction coverage: adopted=%d derived=%d", compactions, derived)
	}
	t.Logf("LIVE_CONTINUATION evidence=%s adopted=%d derived=%d", root, compactions, derived)
}
