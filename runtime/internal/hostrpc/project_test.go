package hostrpc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/host"
	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func TestProjectProjectionStaysBelowTheTUIValidationCap(t *testing.T) {
	long := "/workspace/" + strings.Repeat("deep/", 690) + "AGENTS.md"
	index := project.Index{Entries: []project.IndexEntry{{Name: "s", Description: "d", Path: long}}, Omitted: 1023}
	snapshot := project.Snapshot{Root: "/workspace", SkillsTotal: 1024, SkippedTotal: 1024,
		SkillsIndex: index.Encode()}
	for i := 0; i < project.MaxSnapshotSkills; i++ {
		snapshot.Skills = append(snapshot.Skills, project.Skill{Name: "skill-" + string(rune('a'+i%26)), Description: strings.Repeat("d", project.MaxSkillDescription), Path: long})
	}
	for i := 0; i < project.MaxSnapshotSkipped; i++ {
		snapshot.Skipped = append(snapshot.Skipped, project.Skip{Path: long, Reason: strings.Repeat("r", project.MaxSkipReasonBytes)})
	}
	for i := 0; i < project.MaxInstructionFiles; i++ {
		snapshot.Instructions = append(snapshot.Instructions, project.Instruction{Path: long, Size: 1, SHA256: "sum", Content: "x"})
	}
	binding, err := ProjectOf(snapshot, false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > MaxProjectBindingBytes {
		t.Fatalf("project projection is %d bytes, over %d", len(data), MaxProjectBindingBytes)
	}
	if len(binding.Instructions)+len(binding.Skills)+len(binding.Skipped) == 0 {
		t.Fatal("projection dropped every entry that could show provenance")
	}
	if len(data) >= 1<<20 {
		t.Fatalf("project projection is %d bytes, over the 1 MiB TUI preflight cap", len(data))
	}
	for _, value := range []any{
		Validated{Project: binding, Spec: Spec{Model: "model", Workspace: "/workspace", MaxTokens: 16000, RequestBytes: 256 << 10}},
		Run{Project: binding, Goal: strings.Repeat("g", 32<<10), Constraints: strings.Repeat("c", 16<<10)},
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) >= 1<<20 {
			t.Fatalf("%T is %d bytes, over the 1 MiB TUI preflight cap", value, len(encoded))
		}
	}
}

func TestProjectionDegradesOversizedMandatoryMetadata(t *testing.T) {
	meta := project.Meta{Root: "/" + strings.Repeat("a", MaxProjectBindingBytes)}
	if _, err := ProjectOfMeta(meta, false); err == nil {
		t.Fatal("oversized metadata projected without an error")
	}
	run := RunOf(host.View{Project: meta}, nil)
	if run.Project != nil || run.ProjectUnavailable == "" {
		t.Fatalf("run projection = %+v", run.Project)
	}
}

func TestProjectUnavailableIsBoundedBeforeTheProjection(t *testing.T) {
	run := RunOf(host.View{ProjectUnavailable: strings.Repeat("x", 1<<20)}, nil)
	if len(run.ProjectUnavailable) > project.MaxDiagnosticBytes {
		t.Fatalf("project diagnostic is %d bytes", len(run.ProjectUnavailable))
	}
}
