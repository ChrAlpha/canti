package harness_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func testProject() project.Snapshot {
	content := "Use tabs.\n"
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
	skillPath := "/workspace/.agents/skills/go/SKILL.md"
	index := project.Index{Entries: []project.IndexEntry{{Name: "go", Description: "Go conventions.", Path: skillPath}}}
	return project.Snapshot{Root: "/workspace",
		Instructions: []project.Instruction{{Path: "/workspace/AGENTS.md", Size: len(content), SHA256: sum, Content: content}},
		Skills:       []project.Skill{{Name: "go", Description: "Go conventions.", Path: skillPath}},
		SkillsTotal:  1, SkillsIndex: index.Encode(), SkillsIndexSHA256: digestIndex(index.Text())}
}

func projectSection(t *testing.T, body []byte) string {
	t.Helper()
	text := string(body)
	start := strings.Index(text, "## Project instructions")
	if start < 0 {
		t.Fatalf("request has no project section: %s", text)
	}
	end := strings.Index(text[start:], "## Environment")
	if end < 0 {
		t.Fatalf("request has no environment after its project section: %s", text[start:])
	}
	return text[start : start+end]
}

func TestProjectSectionSurvivesARestartAndAnEditedFile(t *testing.T) {
	f := newProjectFixture(t, limits, harness.Budget{}, testProject(), execReply("ls"), execReply("ls"), returnReply("done"))
	l := f.loop()
	f.steps(l, 1)
	first := projectSection(t, f.frozen(1, 1))
	if !strings.Contains(first, "/workspace/AGENTS.md (sha256 ") || !strings.Contains(first, "Use tabs.") || !strings.Contains(first, "## Skills") {
		t.Fatalf("project section = %q", first)
	}
	if err := os.WriteFile(filepath.Join(f.ws, "AGENTS.md"), []byte("Use spaces.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A new loop on a newly opened store is a restarted controller; four
	// intents complete the first step and freeze the second.
	f.steps(f.fresh(), 4)
	afterEdit := projectSection(t, f.frozen(2, 1))
	if afterEdit != first {
		t.Fatalf("restart rendered a changed project section:\nfirst: %s\nafter: %s", first, afterEdit)
	}
	if strings.Contains(afterEdit, "Use spaces.") {
		t.Fatal("the edited file entered the run's requests")
	}
	if err := os.Remove(filepath.Join(f.ws, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	f.steps(f.fresh(), 4)
	afterDelete := projectSection(t, f.frozen(3, 1))
	if afterDelete != first {
		t.Fatalf("deletion changed the project section:\nfirst: %s\nafter: %s", first, afterDelete)
	}
}

func TestProjectSectionIsStableAcrossSteps(t *testing.T) {
	f := newProjectFixture(t, limits, harness.Budget{}, testProject(), execReply("ls"), execReply("ls"), returnReply("done"))
	l := f.loop()
	f.steps(l, 1)
	first := projectSection(t, f.frozen(1, 1))
	f.steps(l, 4)
	second := projectSection(t, f.frozen(2, 1))
	if first != second {
		t.Fatalf("project section changed across steps:\n%s\n%s", first, second)
	}
}

func TestProjectContextPassesAConfiguredBudget(t *testing.T) {
	f := newProjectFixture(t, limits, harness.Budget{ContextTokens: 32768}, testProject(), execReply("ls"), returnReply("done"))
	if snap := f.run(f.loop()); snap.State.Phase != core.PhaseReturned {
		t.Fatalf("configured budget phase = %s", snap.State.Phase)
	}
}

func TestProjectContextPassesALearnedBudget(t *testing.T) {
	overflow := reply{body: "{}", usage: harness.Usage{InputTokens: 100, OutputTokens: 10, InputReported: true, OutputReported: true},
		err: &harness.ContextLimitError{Status: 400, Code: "context_length_exceeded"}}
	f := newProjectFixture(t, limits, harness.Budget{}, testProject(), execReply("ls"), overflow, returnReply("done"))
	if snap := f.run(f.loop()); snap.State.Phase != core.PhaseReturned {
		t.Fatalf("learned budget phase = %s", snap.State.Phase)
	}
}

func TestProjectContextPassesABudgetAnchor(t *testing.T) {
	f := newProjectFixture(t, limits, harness.Budget{ContextTokens: 100}, testProject(), execReply("ls"), execReply("ls"), returnReply("done"))
	if snap := f.run(f.loop()); snap.State.Phase != core.PhaseReturned {
		t.Fatalf("budget anchor phase = %s", snap.State.Phase)
	}
	anchored := false
	for _, tr := range f.transitions() {
		if tr.Facts.ContextState != nil && tr.Facts.ContextState.AnchorSeq > 0 {
			anchored = true
		}
	}
	if !anchored {
		t.Fatal("the budgeted project run opened no anchor")
	}
}
