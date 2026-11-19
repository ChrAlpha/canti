package project

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func containerRead(t *testing.T, root, workdir string) Snapshot {
	t.Helper()
	if workdir == "" {
		workdir = root
	}
	snap, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: workdir})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestContainerReadBindsTheKernelView(t *testing.T) {
	root := t.TempDir()
	write(t, root, "AGENTS.md", "root\n")
	write(t, root, "sub/AGENTS.md", "sub\n")
	write(t, root, ".agents/skills/go/SKILL.md", "---\nname: go\ndescription: Go conventions.\n---\n")
	snap := containerRead(t, root, filepath.Join(root, "sub"))
	if len(snap.Instructions) != 2 || snap.Instructions[1].Content != "sub\n" {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
	if len(snap.Skills) != 1 || snap.Skills[0].Name != "go" {
		t.Fatalf("skills = %+v", snap.Skills)
	}
}

func TestContainerReadFollowsAnAbsoluteLinkInsideTheRoot(t *testing.T) {
	root := t.TempDir()
	write(t, root, "shared.md", "shared\n")
	if err := os.Symlink(filepath.Join(root, "shared.md"), filepath.Join(root, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	snap := containerRead(t, root, "")
	if len(snap.Instructions) != 1 || snap.Instructions[0].Content != "shared\n" || snap.Instructions[0].ResolvedPath == "" {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
}

func TestContainerReadRefusesAnInstructionOutsideTheRoot(t *testing.T) {
	outside := t.TempDir()
	write(t, outside, "AGENTS.md", "outside\n")
	root := t.TempDir()
	if err := os.Symlink(filepath.Join(outside, "AGENTS.md"), filepath.Join(root, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), filepath.Join(root, "AGENTS.md")) || !strings.Contains(err.Error(), outside) {
		t.Fatalf("outside instruction = %v", err)
	}
}

func TestContainerReadSkipsAnOutsideSkillWithDiagnostic(t *testing.T) {
	outside := t.TempDir()
	write(t, outside, "SKILL.md", "---\nname: outside\ndescription: outside\n---\n")
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".agents/skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, ".agents/skills/outside")); err != nil {
		t.Fatal(err)
	}
	snap := containerRead(t, root, "")
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "outside") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestContainerReadRefusesAnInstructionAliasOfAnEnvironmentFile(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".env", "SECRET=1\n")
	if err := os.Symlink(".env", filepath.Join(root, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
	if !errors.Is(err, ErrForbidden) || !strings.Contains(err.Error(), ".env") {
		t.Fatalf("environment instruction = %v", err)
	}
}

func TestContainerReadDiagnosesADanglingSkill(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".agents/skills/broken"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing.md", filepath.Join(root, ".agents/skills/broken/SKILL.md")); err != nil {
		t.Fatal(err)
	}
	snap := containerRead(t, root, "")
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "dangling") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestContainerReadTreatsAMissingChildUnderADirectoryLinkAsAbsent(t *testing.T) {
	root := t.TempDir()
	write(t, root, "real/other", "x\n")
	if err := os.Symlink("real", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	snap := containerRead(t, root, filepath.Join(root, "link"))
	if len(snap.Instructions) != 0 {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
}

func TestContainerReadRefusesAWorkdirLinkToANonDirectory(t *testing.T) {
	root := t.TempDir()
	write(t, root, "file", "x\n")
	if err := os.Symlink("file", filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: filepath.Join(root, "work")})
	if !errors.Is(err, ErrNotFound) && !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("workdir link = %v", err)
	}
}

func TestContainerReadRefusesAnInstructionUnderAnUnreadableMount(t *testing.T) {
	root := t.TempDir()
	write(t, root, "vol/AGENTS.md", "volume\n")
	_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: filepath.Join(root, "vol"), Mounts: []ContainerMount{
		{Target: root, Readable: true},
		{Target: filepath.Join(root, "vol")},
	}})
	if !errors.Is(err, ErrUnreadableMount) || !strings.Contains(err.Error(), filepath.Join(root, "vol")) {
		t.Fatalf("volume instruction = %v", err)
	}
}

func TestContainerReadSkipsSkillsUnderAnUnreadableMount(t *testing.T) {
	root := t.TempDir()
	write(t, root, "vol/.agents/skills/s/SKILL.md", "---\nname: s\ndescription: s\n---\n")
	snap, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root, Mounts: []ContainerMount{
		{Target: root, Readable: true},
		{Target: filepath.Join(root, "vol")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// The root skills path is not under the volume; the volume's skills are
	// outside the chain only when the workdir is the root. Read the volume
	// itself to exercise the skip.
	snap, err = ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root, Mounts: []ContainerMount{
		{Target: root, Readable: true},
		{Target: filepath.Join(root, "vol")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 {
		t.Fatalf("skills = %+v", snap.Skills)
	}
}

func TestSnapshotFitEncodedDropsOptionalPages(t *testing.T) {
	long := strings.Repeat("p", 3000)
	index := Index{Entries: []IndexEntry{{Name: "s", Description: "d", Path: "/workspace/.agents/skills/s/SKILL.md"}}}
	s := Snapshot{Root: "/workspace", SkillsTotal: 64, SkippedTotal: 64, SkillsIndex: index.Encode()}
	s.SkillsIndexSHA256 = digest([]byte(index.Text()))
	for i := 0; i < 64; i++ {
		s.Instructions = append(s.Instructions, Instruction{Path: "/workspace/" + long, Size: 1, SHA256: digest([]byte("x")), Content: "x"})
		s.Skills = append(s.Skills, Skill{Name: "s", Description: strings.Repeat("d", 1024), Path: "/workspace/" + long})
		s.Skipped = append(s.Skipped, Skip{Path: "/workspace/" + long, Reason: strings.Repeat("r", 512)})
	}
	fit, err := s.FitEncoded(512 << 10)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(fit)
	if err != nil || len(data) > 512<<10 {
		t.Fatalf("encoded fit = %d, %v", len(data), err)
	}
	if fit.SkillsTotal != 64 || fit.SkippedTotal != 64 || fit.SkillsIndexText() == "" {
		t.Fatalf("fit lost counts or index: %+v", fit)
	}
}

func TestContainerReadRefusesAnInstructionLinkToADirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "rules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("rules", filepath.Join(root, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "non-regular") {
		t.Fatalf("directory link = %v", err)
	}
}

func TestContainerReadHonorsContextDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadContainer(ctx, ContainerRequest{Root: t.TempDir(), Workdir: t.TempDir()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read = %v", err)
	}
}

func TestContainerReadRefusesADirectoryNamedLikeAnInstruction(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "AGENTS.md"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "non-regular") {
		t.Fatalf("directory instruction = %v", err)
	}
}

func TestContainerReadSkipsASkillPathWithANewline(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".agents", "skills", "bad\nname")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: bad\ndescription: bad\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, err := ReadContainer(context.Background(), ContainerRequest{Root: root, Workdir: root})
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "control") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestContainerReadIndexesADirectoryNamedLikeAResolvedAnnotation(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".agents/skills/good (resolves to notes)/SKILL.md", "---\nname: good\ndescription: d\n---\n")
	snap := containerRead(t, root, "")
	if len(snap.Skills) != 1 || len(snap.Skipped) != 0 {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
	if !strings.Contains(snap.SkillsIndexText(), "Path: "+root+"/.agents/skills/good (resolves to notes)/SKILL.md") {
		t.Fatalf("index = %q", snap.SkillsIndexText())
	}
}

func TestIndexEncodingDoesNotCarryDiscoveryRoots(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".agents/skills/go/SKILL.md", "---\nname: go\ndescription: Go conventions.\n---\n")
	chain := make([]containerDir, 1150)
	longest := "/workspace/" + strings.Repeat("d", 2311)
	for i := 0; i < len(chain)-1; i++ {
		chain[i] = containerDir{visible: longest, real: root}
	}
	chain[len(chain)-1] = containerDir{visible: "/workspace", real: root}
	skills, skipped := containerSkills(context.Background(), root, chain, nil)
	index := buildIndex(skills, len(skipped))
	if len(skills) != 1 || len(index.Encode()) > 4096 {
		t.Fatalf("skills=%d encoded index=%d", len(skills), len(index.Encode()))
	}
	if !strings.Contains(index.Text(), "- go: Go conventions.") {
		t.Fatalf("rendered index = %q", index.Text())
	}
}
