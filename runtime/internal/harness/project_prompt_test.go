package harness

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func TestPromptRendersProjectInstructionsAndSkills(t *testing.T) {
	content := "Use tabs.\n"
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
	skillPath := "/workspace/.agents/skills/go/SKILL.md"
	f := testFrame()
	f.capture = &ContextCapture{}
	index := project.Index{Entries: []project.IndexEntry{{Name: "go", Description: "Go conventions.", Path: skillPath}}}
	f.Project = project.Snapshot{Root: "/workspace",
		Instructions: []project.Instruction{{Path: "/workspace/AGENTS.md", ResolvedPath: "/workspace/SHARED.md", Size: len(content), SHA256: sum, Content: content}},
		Skills:       []project.Skill{{Name: "go", Description: "Go conventions.", Path: skillPath}},
		SkillsTotal:  1, SkillsIndex: index.Encode(), SkillsIndexSHA256: digestIndex(index.Text())}
	p := mustPrompt(t, f, DefaultPromptLimits())
	head := p.Blocks[0]
	for _, want := range []string{
		"## Project instructions",
		"### /workspace/AGENTS.md (sha256 " + sum[:12] + ")",
		"Resolved path: /workspace/SHARED.md",
		"Use tabs.",
		"## Skills",
		"- go: Go conventions.",
		"Path: " + skillPath,
	} {
		if !strings.Contains(head, want) {
			t.Errorf("head lacks %q:\n%s", want, head)
		}
	}
	if f.capture.Bytes.Project == 0 || f.capture.Bytes.Skills == 0 {
		t.Fatalf("capture did not account for project context: %+v", f.capture.Bytes)
	}
	if p.Stable < 1 || !strings.Contains(p.Blocks[0], "## Project instructions") {
		t.Fatal("project instructions are not in the stable head")
	}
	if !strings.Contains(p.System, "Project instructions bound from the workspace are standing instructions") ||
		!strings.Contains(p.System, "a later principal instruction") {
		t.Fatalf("system prompt lacks the project instruction rule:\n%s", p.System)
	}
}

// baseSystemSHA256 is the digest of system at 3dfeed6, before project context.
const (
	baseSystemSHA256     = "b2d8119e0ad6b6eb14a4407f8252f67d8803f871f29c168ee5824e9fe5fb8248"
	baseToolSystemSHA256 = "6157a06a140cfef6cbecc0ede1dd8ea797f0ca80f41b8413f6fe59b58434b52a"
)

func TestSystemWithoutProjectContextIsTheBasePrompt(t *testing.T) {
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(system))); got != baseSystemSHA256 {
		t.Fatalf("base system digest = %s, want %s", got, baseSystemSHA256)
	}
	f := testFrame()
	if got := f.contextSystem(); got != system || strings.Contains(got, "Project instructions bound") {
		t.Fatalf("system without project context changed:\n%s", got)
	}
	f.DecisionTool = true
	if got := f.contextSystem(); got != toolSystem || strings.Contains(got, "Project instructions bound") {
		t.Fatalf("tool system without project context changed:\n%s", got)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(toolSystem))); got != baseToolSystemSHA256 {
		t.Fatalf("base tool system digest = %s, want %s", got, baseToolSystemSHA256)
	}
}

func digestIndex(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }
