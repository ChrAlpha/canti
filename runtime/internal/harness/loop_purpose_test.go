package harness_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func purposeReply(kind core.Kind, purpose, payload string) reply {
	b, _ := json.Marshal(core.Decision{Kind: kind, Purpose: purpose, Payload: payload})
	return reply{body: string(b), usage: usage}
}

func TestLoopShowsCommittedPurposesAcrossRestart(t *testing.T) {
	long := "read it back\n" + strings.Repeat("é", 300)
	f := newFixture(t, limits, harness.Budget{},
		purposeReply(core.Exec, "list the tree, then count Go files", "ls -la /workspace"),
		reply{body: "not a decision", usage: usage},
		purposeReply(core.Exec, long, "find . -name '*.go' | wc -l"),
		purposeReply(core.Return, "the count is known", "There are 3 Go files."))
	snap := f.steps(f.loop(), 7)
	if s := snap.State; s.Phase != core.PhaseIdle || s.Step != 2 || s.Attempt != 2 || s.RequestRev != 1 {
		t.Fatalf("before restart = %+v, want a rejected first response of step 2", s)
	}
	wantState(t, f.run(f.fresh()), core.PhaseReturned, "")
	f.wantKinds(cycle, closing, cycle, closing)

	p := f.model.prompts
	if len(p) != 4 {
		t.Fatalf("prompts = %d, want 4", len(p))
	}
	first := "### Step 1\nPurpose: list the tree, then count Go files\nCommand:\n```\nls -la /workspace\n```\n"
	if !strings.HasPrefix(p[1].Blocks[1], first) {
		t.Fatalf("step 1 block =\n%q\nwant prefix\n%q", p[1].Blocks[1], first)
	}
	if p[2].System != p[1].System || p[2].Stable != p[1].Stable || !slices.Equal(p[2].Blocks[:p[2].Stable], p[1].Blocks[:p[1].Stable]) {
		t.Fatalf("revision 2 of step 2, rendered after a restart, changed the stable blocks:\n%q\n%q", p[2].Blocks[:p[2].Stable], p[1].Blocks[:p[1].Stable])
	}
	second := "### Step 2\nPurpose: read it back " + strings.Repeat("é", 249) + "…[truncated 102 bytes]\nCommand:\n"
	if p[3].Blocks[1] != p[1].Blocks[1] || !strings.HasPrefix(p[3].Blocks[2], second) {
		t.Fatalf("step 3 blocks =\n%q\nwant step 1 unchanged and step 2 starting\n%q", p[3].Blocks[1:3], second)
	}
	for _, rev := range [][2]int{{2, 1}, {2, 2}, {3, 1}} {
		if body := string(f.frozen(rev[0], rev[1])); !strings.Contains(body, first) {
			t.Fatalf("frozen request %v lacks the step 1 purpose:\n%s", rev, body)
		}
	}
	if !strings.Contains(string(f.frozen(3, 1)), second) {
		t.Fatal("frozen request of step 3 lacks the clipped step 2 purpose")
	}
}

func TestLoopFreezesTheSystemOfItsDecisionTransport(t *testing.T) {
	for _, tool := range []bool{false, true} {
		f := newFixture(t, limits, harness.Budget{}, returnReply("done"))
		f.cfg.DecisionTool = tool
		wantState(t, f.run(f.loop()), core.PhaseReturned, "")
		p := f.model.prompts[0]
		if strings.Contains(p.System, "Call the canti_decision tool exactly once per reply") != tool || strings.Contains(p.System, "escape quotes") == tool {
			t.Fatalf("tool %v: system =\n%s", tool, p.System)
		}
		req, err := f.store.Request(t.Context(), runID, 1, 1)
		if err != nil || req.Context == nil || req.Context.Bytes.System != len(p.System) || string(req.Body) != p.System+"\n\n"+p.User() {
			t.Fatalf("tool %v: frozen request does not match the rendered system: %v", tool, err)
		}
	}
}
