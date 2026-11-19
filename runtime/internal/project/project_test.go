package project

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func workspace(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func write(t *testing.T, root, rel, body string) string {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func request(ws, visibleWorkdir string) Request {
	if visibleWorkdir == "" {
		visibleWorkdir = "/workspace"
	}
	return Request{Workspace: ws, Root: "/workspace", Workdir: visibleWorkdir}
}

func read(t *testing.T, ws, workdir string) Snapshot {
	t.Helper()
	visible := "/workspace"
	if workdir != "" {
		if rel, err := filepath.Rel(ws, workdir); err == nil && rel != "." {
			visible += "/" + filepath.ToSlash(rel)
		}
	}
	snap, err := Read(request(ws, visible))
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestReadBindsInstructionFilesFromWorkspaceRootToWorkdir(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "AGENTS.md", "root rules\n")
	write(t, ws, "sub/AGENTS.md", "sub rules\n")
	write(t, ws, "sub/deep/AGENTS.md", "deep rules\n")
	snap := read(t, ws, filepath.Join(ws, "sub/deep"))
	if len(snap.Instructions) != 3 {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
	want := []string{"/workspace/AGENTS.md", "/workspace/sub/AGENTS.md", "/workspace/sub/deep/AGENTS.md"}
	for i, in := range snap.Instructions {
		if in.Path != want[i] || in.Content != map[int]string{0: "root rules\n", 1: "sub rules\n", 2: "deep rules\n"}[i] {
			t.Errorf("instruction %d = %+v", i, in)
		}
		if in.ResolvedPath != "" || in.Size != len(in.Content) || in.SHA256 != digest([]byte(in.Content)) {
			t.Errorf("instruction %d provenance = %+v", i, in)
		}
	}
}

func TestReadUsesPiOneFilePerDirectoryOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"override", map[string]string{"AGENTS.override.md": "override\n", "AGENTS.md": "agents\n", "CLAUDE.md": "claude\n"}, "override\n"},
		{"agents", map[string]string{"AGENTS.md": "agents\n", "CLAUDE.md": "claude\n"}, "agents\n"},
		{"uppercase", map[string]string{"AGENTS.MD": "upper\n", "CLAUDE.md": "claude\n"}, "upper\n"},
		{"claude", map[string]string{"CLAUDE.md": "claude\n"}, "claude\n"},
		{"claude uppercase", map[string]string{"CLAUDE.MD": "claude upper\n"}, "claude upper\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := workspace(t)
			for name, body := range tc.files {
				write(t, ws, name, body)
			}
			snap := read(t, ws, "")
			if len(snap.Instructions) != 1 || snap.Instructions[0].Content != tc.want {
				t.Fatalf("instructions = %+v, want %q", snap.Instructions, tc.want)
			}
		})
	}
}

func TestReadFollowsSymlinksInsideTheWorkspaceAndRecordsBoth(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "AGENTS.md", "shared rules\n")
	if err := os.Symlink("AGENTS.md", filepath.Join(ws, "CLAUDE.md")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(ws, "linked"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(".", filepath.Join(ws, "linked", "back")); err != nil {
		t.Fatal(err)
	}
	snap := read(t, ws, filepath.Join(ws, "linked", "back"))
	if len(snap.Instructions) != 1 {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
	in := snap.Instructions[0]
	if in.Path != "/workspace/AGENTS.md" || in.Content != "shared rules\n" {
		t.Fatalf("instruction = %+v", in)
	}
	linked := workspace(t)
	write(t, linked, "SHARED.md", "shared rules\n")
	if err := os.Symlink("SHARED.md", filepath.Join(linked, "CLAUDE.md")); err != nil {
		t.Fatal(err)
	}
	snap = read(t, linked, "")
	if len(snap.Instructions) != 1 {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
	in = snap.Instructions[0]
	if in.Path != "/workspace/CLAUDE.md" || in.ResolvedPath != "/workspace/SHARED.md" {
		t.Fatalf("symlink provenance = %+v", in)
	}
}

func TestReadRefusesSymlinksLeavingTheWorkspace(t *testing.T) {
	outside := t.TempDir()
	write(t, outside, "AGENTS.md", "outside rules\n")
	ws := workspace(t)
	if err := os.Symlink(filepath.Join(outside, "AGENTS.md"), filepath.Join(ws, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, ""))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "/workspace/AGENTS.md") {
		t.Fatalf("escaping instruction symlink = %v", err)
	}
}

func TestReadRefusesNonRegularInstructionSymlink(t *testing.T) {
	ws := workspace(t)
	if err := os.MkdirAll(filepath.Join(ws, "rules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("rules", filepath.Join(ws, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, ""))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "non-regular") {
		t.Fatalf("non-regular instruction symlink = %v", err)
	}
}

func TestReadRefusesInstructionBytesOverTheBoundNamingFiles(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "AGENTS.md", strings.Repeat("a", 40<<10))
	write(t, ws, "sub/AGENTS.md", strings.Repeat("b", 40<<10))
	_, err := Read(request(ws, "/workspace/sub"))
	if err == nil {
		t.Fatal("over-bound instructions were accepted")
	}
	for _, want := range []string{"/workspace/AGENTS.md (40960 bytes)", "/workspace/sub/AGENTS.md (40960 bytes)", "81920 bytes", "65536"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func skill(t *testing.T, root, dir, body string) {
	t.Helper()
	write(t, root, filepath.Join(dir, "SKILL.md"), body)
}

func TestReadIndexesSkillsWithFrontmatter(t *testing.T) {
	ws := workspace(t)
	skill(t, ws, ".agents/skills/pdf-tools", "---\nname: pdf-tools\ndescription: Extract text from PDFs.\n---\n# PDF tools\n")
	skill(t, ws, "sub/.agents/skills/go", "---\ndescription: 'Go conventions.'\n---\n")
	snap := read(t, ws, filepath.Join(ws, "sub"))
	if len(snap.Skills) != 2 || snap.SkillsOmitted != 0 {
		t.Fatalf("skills = %+v omitted=%d", snap.Skills, snap.SkillsOmitted)
	}
	if snap.Skills[0].Name != "go" || snap.Skills[0].Description != "Go conventions." || snap.Skills[0].Path != "/workspace/sub/.agents/skills/go/SKILL.md" {
		t.Errorf("skill 0 = %+v", snap.Skills[0])
	}
	if snap.Skills[1].Name != "pdf-tools" || snap.Skills[1].Path != "/workspace/.agents/skills/pdf-tools/SKILL.md" {
		t.Errorf("skill 1 = %+v", snap.Skills[1])
	}
	if !strings.Contains(snap.SkillsIndexText(), "- go: Go conventions.") || !strings.Contains(snap.SkillsIndexText(), "Path: /workspace/sub/.agents/skills/go/SKILL.md") {
		t.Fatalf("index = %q", snap.SkillsIndexText())
	}
}

func TestReadSkipsInvalidSkillsWithDiagnostics(t *testing.T) {
	ws := workspace(t)
	skill(t, ws, ".agents/skills/no-description", "---\nname: no-description\n---\n")
	skill(t, ws, ".agents/skills/long-name", "---\nname: "+strings.Repeat("a", 65)+"\ndescription: x\n---\n")
	skill(t, ws, ".agents/skills/long-description", "---\nname: long-description\ndescription: "+strings.Repeat("x", 1025)+"\n---\n")
	skill(t, ws, ".agents/skills/bad-yaml", "---\nname: bad-yaml\ndescription: >2\n  x\n---\n")
	skill(t, ws, ".agents/skills/bad-name", "---\nname: Bad_Name\ndescription: x\n---\n")
	skill(t, ws, ".agents/skills/good", "---\nname: good\ndescription: Good.\n---\n")
	snap := read(t, ws, "")
	if len(snap.Skills) != 1 || snap.Skills[0].Name != "good" {
		t.Fatalf("skills = %+v", snap.Skills)
	}
	if len(snap.Skipped) != 5 {
		t.Fatalf("skipped = %+v", snap.Skipped)
	}
	for _, skip := range snap.Skipped {
		if skip.Reason == "" || !strings.HasPrefix(skip.Path, "/workspace/.agents/skills/") {
			t.Errorf("skip = %+v", skip)
		}
	}
}

func TestReadSkipsSkillsLeavingTheWorkspace(t *testing.T) {
	outside := t.TempDir()
	write(t, outside, "SKILL.md", "---\nname: outside\ndescription: outside\n---\n")
	ws := workspace(t)
	if err := os.MkdirAll(filepath.Join(ws, ".agents/skills"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(ws, ".agents/skills/outside")); err != nil {
		t.Fatal(err)
	}
	snap := read(t, ws, "")
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "leaves the workspace") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadResolvesSkillNameCollisionsNearestFirst(t *testing.T) {
	ws := workspace(t)
	skill(t, ws, ".agents/skills/same", "---\nname: same\ndescription: root\n---\n")
	skill(t, ws, "sub/.agents/skills/same", "---\nname: same\ndescription: nested\n---\n")
	snap := read(t, ws, filepath.Join(ws, "sub"))
	if len(snap.Skills) != 1 || snap.Skills[0].Description != "nested" || snap.Skills[0].Path != "/workspace/sub/.agents/skills/same/SKILL.md" {
		t.Fatalf("skills = %+v", snap.Skills)
	}
	if len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "duplicate name") {
		t.Fatalf("skipped = %+v", snap.Skipped)
	}
}

func TestReadBoundsSkillsIndexAndCountsTheRest(t *testing.T) {
	ws := workspace(t)
	for i := 0; i < 40; i++ {
		body := fmt.Sprintf("---\nname: skill-%02d\ndescription: %s\n---\n", i, strings.Repeat("x", 900))
		skill(t, ws, fmt.Sprintf(".agents/skills/skill-%02d", i), body)
	}
	snap := read(t, ws, "")
	if len(snap.Skills) != 40 || snap.SkillsOmitted == 0 {
		t.Fatalf("skills=%d omitted=%d", len(snap.Skills), snap.SkillsOmitted)
	}
	if len(snap.SkillsIndexText()) > MaxSkillsIndexBytes {
		t.Fatalf("index %d bytes over %d", len(snap.SkillsIndexText()), MaxSkillsIndexBytes)
	}
	if !strings.Contains(snap.SkillsIndexText(), fmt.Sprintf("%d more skills are available in the workspace", snap.SkillsOmitted)) {
		t.Fatalf("index lacks the overflow notice: %q", snap.SkillsIndexText())
	}
	if snap.SkillsOmitted >= len(snap.Skills) {
		t.Fatalf("omitted=%d of %d", snap.SkillsOmitted, len(snap.Skills))
	}
}

func TestReadSkipsASkillThatDisablesModelInvocation(t *testing.T) {
	ws := workspace(t)
	skill(t, ws, ".agents/skills/hidden", "---\nname: hidden\ndescription: hidden\ndisable-model-invocation: true\n---\n")
	snap := read(t, ws, "")
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "disable-model-invocation") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

// Pi 1.5.0 dist/utils/frontmatter.js parses this frontmatter with yaml.parse,
// and dist/core/skills.js then validates the string name and description.
// These cases pin the subset canti parses without that dependency.
func TestParseFrontmatterAcceptsTheSubset(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantName, wantDescription string
		disabled                              bool
	}{
		{
			"plain",
			"---\nname: pdf-tools\ndescription: Extract PDFs.\n---\n",
			"pdf-tools", "Extract PDFs.", false,
		},
		{
			"single quoted",
			"---\nname: 'pdf-tools'\ndescription: 'Use when: PDFs.'\n---\n",
			"pdf-tools", "Use when: PDFs.", false,
		},
		{
			"double quoted",
			"---\nname: \"pdf-tools\"\ndescription: \"Use \\\"PDFs\\\".\"\n---\n",
			"pdf-tools", `Use "PDFs".`, false,
		},
		{
			"folded",
			"---\nname: pdf-tools\ndescription: >\n  Extract PDFs.\n  Use for documents.\n---\n",
			"pdf-tools", "Extract PDFs. Use for documents.\n", false,
		},
		{
			"folded more-indented",
			"---\nname: pdf-tools\ndescription: >\n  foo\n    bar\n  baz\n---\n",
			"pdf-tools", "foo\n  bar\nbaz\n", false,
		},
		{
			"literal stripped",
			"---\nname: pdf-tools\ndescription: |-\n  First line.\n  Second line.\n---\n",
			"pdf-tools", "First line.\nSecond line.", false,
		},
		{
			"literal kept",
			"---\nname: pdf-tools\ndescription: |+\n  Line.\n---\n",
			"pdf-tools", "Line.\n", false,
		},
		{
			"closing trailing whitespace",
			"---\nname: pdf-tools\ndescription: PDFs.\n---  \n",
			"pdf-tools", "PDFs.", false,
		},
		{
			"literal with indented marker",
			"---\nname: pdf-tools\ndescription: |\n  line\n  ---\n  tail\ndisable-model-invocation: true\n---\n",
			"pdf-tools", "line\n---\ntail\n", true,
		},
		{
			"literal keeps trailing blanks",
			"---\nname: pdf-tools\ndescription: |+\n  line\n\n---\n",
			"pdf-tools", "line\n\n", false,
		},
		{
			"nested ignored",
			"---\nmetadata:\n  owner: x\nname: pdf-tools\ndescription: PDFs.\n---\n",
			"pdf-tools", "PDFs.", false,
		},
		{
			"disabled",
			"---\ndescription: PDFs.\ndisable-model-invocation: true\n---\n",
			"", "PDFs.", true,
		},
		{
			"inline comment",
			"---\nname: pdf-tools\ndescription: PDFs. # routing\n---\n",
			"pdf-tools", "PDFs.", false,
		},
		{
			"escaped slash",
			"---\nname: pdf-tools\ndescription: \"path\\/example\"\n---\n",
			"pdf-tools", "path/example", false,
		},
		{
			"indented comment",
			"---\nname: pdf-tools\ndescription: PDFs.\n  # not content\n---\n",
			"pdf-tools", "PDFs.", false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta, err := parseFrontmatter([]byte(tc.body), false)
			if err != nil {
				t.Fatal(err)
			}
			if meta.Name != tc.wantName || meta.Description != tc.wantDescription || meta.DisableModelInvocation != tc.disabled {
				t.Fatalf("meta = %+v", meta)
			}
		})
	}
}

// The same Pi 1.5.0 parser rejects a malformed document rather than guessing;
// canti's subset refuses anything it cannot read as one of the accepted forms.
func TestParseFrontmatterRefusesAmbiguousValues(t *testing.T) {
	for _, tc := range []struct {
		name, body string
	}{
		{"unterminated", "---\nname: pdf-tools\ndescription: PDFs.\n"},
		{"duplicate", "---\nname: one\nname: two\ndescription: PDFs.\n---\n"},
		{"unterminated quote", "---\nname: \"pdf-tools\ndescription: PDFs.\n---\n"},
		{"flow sequence", "---\nname: pdf-tools\ndescription: [PDFs]\n---\n"},
		{"block header", "---\nname: pdf-tools\ndescription: >2\n  PDFs.\n---\n"},
		{"tab indent", "---\nname: pdf-tools\ndescription: >\n\tPDFs.\n---\n"},
		{"nested description", "---\nname: pdf-tools\ndescription:\n  nested: value\n---\n"},
		{"missing key", "---\njust a value\n---\n"},
		{"bad disable", "---\nname: pdf-tools\ndescription: PDFs.\ndisable-model-invocation: yes\n---\n"},
		{"quoted disable", "---\nname: pdf-tools\ndescription: PDFs.\ndisable-model-invocation: \"true\"\n---\n"},
		{"closing typo", "---\nname: pdf-tools\ndescription: PDFs.\n---typo\n"},
		{"no space after colon", "---\nname:pdf-tools\ndescription: PDFs.\n---\n"},
		{"plain mapping", "---\nname: pdf-tools\ndescription: test: nested\n---\n"},
		{"multiline plain", "---\nname: pdf-tools\ndescription: PDFs.\n  continued\n---\n"},
		{"multiline quoted", "---\nname: pdf-tools\ndescription: \"PDFs.\n  continued\"\n---\n"},
		{"closing with text", "---\nname: pdf-tools\ndescription: PDFs.\n--- more\n"},
		{"out of range escape", "---\nname: pdf-tools\ndescription: \"\\U00110000\"\n---\n"},
		{"boolean description", "---\nname: pdf-tools\ndescription: true\n---\n"},
		{"numeric name", "---\nname: 123\ndescription: PDFs.\n---\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseFrontmatter([]byte(tc.body), false); err == nil {
				t.Fatalf("accepted %q", tc.body)
			}
		})
	}
}

func TestSnapshotValidateRejectsInconsistentRecords(t *testing.T) {
	good := Snapshot{Root: "/workspace", Instructions: []Instruction{{Path: "/workspace/AGENTS.md", Size: 1, SHA256: digest([]byte("a")), Content: "a"}}}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.Instructions = []Instruction{{Path: "/workspace/AGENTS.md", Size: 2, SHA256: good.Instructions[0].SHA256, Content: "a"}}
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted an inconsistent size")
	}
	bad = good
	bad.Instructions[0].Content = "b"
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted an inconsistent digest")
	}
	bad = good
	bad.Instructions[0].Path = "/elsewhere/AGENTS.md"
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted a path outside the root")
	}
}

func TestReadOrdersSkillsByName(t *testing.T) {
	ws := workspace(t)
	for _, name := range []string{"zebra", "alpha", "middle"} {
		skill(t, ws, ".agents/skills/"+name, "---\nname: "+name+"\ndescription: d\n---\n")
	}
	snap := read(t, ws, "")
	var names []string
	for _, skill := range snap.Skills {
		names = append(names, skill.Name)
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("skills are not sorted: %v", names)
	}
}

func TestReadRefusesAWorkdirSymlinkToANonDirectory(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "file", "x\n")
	if err := os.Symlink("file", filepath.Join(ws, "work")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, "/workspace/work"))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("workdir symlink = %v", err)
	}
}

func TestReadCountsAWorkdirAliasOfTheRootOnce(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "AGENTS.md", strings.Repeat("a", 40<<10))
	if err := os.Symlink(".", filepath.Join(ws, "alias")); err != nil {
		t.Fatal(err)
	}
	snap, err := Read(request(ws, "/workspace/alias"))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Instructions) != 1 || snap.Instructions[0].Size != 40<<10 {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
}

func TestReadRefusesAnIntermediateSymlinkLeavingTheMount(t *testing.T) {
	outside := t.TempDir()
	write(t, outside, "AGENTS.md", "outside\n")
	ws := workspace(t)
	if err := os.Symlink(outside, filepath.Join(ws, "sub")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, "/workspace/sub"))
	if !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("intermediate symlink = %v", err)
	}
}

func TestReadDiagnosesADanglingSkillLink(t *testing.T) {
	ws := workspace(t)
	if err := os.MkdirAll(filepath.Join(ws, ".agents/skills/broken"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing.md", filepath.Join(ws, ".agents/skills/broken/SKILL.md")); err != nil {
		t.Fatal(err)
	}
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "dangling") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadTruncatesFortyNestedSkillsWithinTheBound(t *testing.T) {
	ws := workspace(t)
	for i := 0; i < 40; i++ {
		dir := fmt.Sprintf(".agents/skills/skill-%02d", i)
		write(t, ws, dir+"/SKILL.md", fmt.Sprintf("---\nname: skill-%02d\ndescription: %s\n---\n", i, strings.Repeat("x", 900)))
	}
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.SkillsIndexText()) > MaxSkillsIndexBytes {
		t.Fatalf("index = %d bytes, over %d", len(snap.SkillsIndexText()), MaxSkillsIndexBytes)
	}
	if snap.SkillsOmitted == 0 || !strings.Contains(snap.SkillsIndexText(), "more skills are available") {
		t.Fatalf("index did not state omissions: %d bytes", len(snap.SkillsIndexText()))
	}
	if !strings.Contains(snap.SkillsIndexText(), "- skill-00:") {
		t.Fatalf("footer displaced every skill: %q", snap.SkillsIndexText())
	}
}

func TestReadStoresABoundedPageOfSkillsAndSkips(t *testing.T) {
	ws := workspace(t)
	for i := 0; i < 100; i++ {
		body := fmt.Sprintf("---\nname: skill-%03d\ndescription: %s\n---\n", i, strings.Repeat("x", 32))
		skill(t, ws, fmt.Sprintf(".agents/skills/skill-%03d", i), body)
		skill(t, ws, fmt.Sprintf(".agents/skills/bad-%03d", i), "---\nname: bad\n---\n")
	}
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != MaxSnapshotSkills || snap.SkillsTotal != 100 {
		t.Fatalf("skills page = %d of %d", len(snap.Skills), snap.SkillsTotal)
	}
	if len(snap.Skipped) != MaxSnapshotSkipped || snap.SkippedTotal != 100 {
		t.Fatalf("skipped page = %d of %d", len(snap.Skipped), snap.SkippedTotal)
	}
	if len(snap.SkillsIndexText()) > MaxSkillsIndexBytes || snap.SkillsIndexSHA256 == "" {
		t.Fatalf("index = %d bytes, hash=%q", len(snap.SkillsIndexText()), snap.SkillsIndexSHA256)
	}
	if err := snap.Meta().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTruncateNeverExceedsItsBound(t *testing.T) {
	text := strings.Repeat("x", 100) + "…"
	for _, limit := range []int{0, 1, 2, 3, 4, 5, 10, 100, 102} {
		if got := truncate(text, limit); len(got) > limit {
			t.Errorf("truncate(%d) = %d bytes, over %d", limit, len(got), limit)
		}
	}
}

func TestReadRefusesAnInstructionAliasOfAnEnvironmentFile(t *testing.T) {
	ws := workspace(t)
	write(t, ws, ".env", "SECRET=1\n")
	if err := os.Symlink(".env", filepath.Join(ws, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, ""))
	if !errors.Is(err, ErrForbidden) || !strings.Contains(err.Error(), ".env") {
		t.Fatalf("environment instruction = %v", err)
	}
}

func TestReadSkipsASkillAliasOfAnEnvironmentFile(t *testing.T) {
	ws := workspace(t)
	write(t, ws, ".env", "SECRET=1\n")
	if err := os.MkdirAll(filepath.Join(ws, ".agents/skills/leak"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../../.env", filepath.Join(ws, ".agents/skills/leak/SKILL.md")); err != nil {
		t.Fatal(err)
	}
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "environment file") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadSkipsAnInvalidUTF8SkillDirectoryName(t *testing.T) {
	ws := workspace(t)
	dir := filepath.Join(ws, ".agents/skills", string([]byte{'b', 0xff}))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: bad\ndescription: bad\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "not UTF-8") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadSkipsAnInvalidUTF8Description(t *testing.T) {
	ws := workspace(t)
	write(t, ws, ".agents/skills/bad/SKILL.md", "---\nname: bad\ndescription: "+string([]byte{'x', 0xff})+"\n---\n")
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "UTF-8") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadRecordsADerivedSkillName(t *testing.T) {
	ws := workspace(t)
	skill(t, ws, ".agents/skills/derived", "---\ndescription: A skill.\n---\n")
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 1 || snap.Skills[0].Name != "derived" || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "derived") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadBoundsTheOmissionFooterAcrossManyNestedRoots(t *testing.T) {
	ws := workspace(t)
	workdir := ws
	visible := "/workspace"
	for i := 0; i < 40; i++ {
		workdir = filepath.Join(workdir, fmt.Sprintf("d%02d", i))
		visible += fmt.Sprintf("/d%02d", i)
		write(t, ws, filepath.ToSlash(strings.TrimPrefix(workdir, ws))+fmt.Sprintf("/.agents/skills/s%02d/SKILL.md", i),
			fmt.Sprintf("---\nname: s%02d\ndescription: %s\n---\n", i, strings.Repeat("x", 900)))
	}
	snap, err := Read(request(ws, visible))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.SkillsIndexText()) > MaxSkillsIndexBytes {
		t.Fatalf("index = %d bytes, over %d", len(snap.SkillsIndexText()), MaxSkillsIndexBytes)
	}
	if snap.SkillsOmitted == 0 || !strings.Contains(snap.SkillsIndexText(), "more skills are available") {
		t.Fatalf("index did not state omissions: %d bytes", len(snap.SkillsIndexText()))
	}
	if !strings.Contains(snap.SkillsIndexText(), "- s00:") {
		t.Fatalf("footer displaced every skill: %q", snap.SkillsIndexText())
	}
}

func TestReadTreatsAMissingChildUnderADirectoryLinkAsAbsent(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "real/other", "x\n")
	if err := os.Symlink("real", filepath.Join(ws, "link")); err != nil {
		t.Fatal(err)
	}
	snap, err := Read(request(ws, "/workspace/link"))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Instructions) != 0 {
		t.Fatalf("instructions = %+v", snap.Instructions)
	}
}

func TestReadRefusesAnAbsoluteLinkInsideTheEphemeralWorkspace(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "shared.md", "shared\n")
	if err := os.Symlink(filepath.Join(ws, "shared.md"), filepath.Join(ws, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, ""))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "absolute link") {
		t.Fatalf("absolute ephemeral link = %v", err)
	}
}

func TestReadBoundsASymlinkCycle(t *testing.T) {
	ws := workspace(t)
	if err := os.Symlink("b", filepath.Join(ws, "a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a", filepath.Join(ws, "b")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, "/workspace/a"))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "too many symbolic links") {
		t.Fatalf("cycle = %v", err)
	}
}

func TestReadRefusesADirectoryNamedLikeAnInstruction(t *testing.T) {
	ws := workspace(t)
	if err := os.MkdirAll(filepath.Join(ws, "AGENTS.md"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, ""))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "non-regular") {
		t.Fatalf("directory instruction = %v", err)
	}
}

func TestReadSkipsASkillPathWithANewline(t *testing.T) {
	ws := workspace(t)
	dir := filepath.Join(ws, ".agents", "skills", "bad\nname")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: bad\ndescription: bad\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, err := Read(request(ws, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "control") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestReadRefusesATrailingSlashLinkToARegularFile(t *testing.T) {
	ws := workspace(t)
	write(t, ws, "rules", "not a directory\n")
	if err := os.Symlink("rules/", filepath.Join(ws, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := Read(request(ws, ""))
	if !errors.Is(err, ErrUnsafePath) || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("trailing slash link = %v", err)
	}
}

func TestReadIndexesADirectoryNamedLikeAResolvedAnnotation(t *testing.T) {
	ws := workspace(t)
	skill(t, ws, ".agents/skills/good (resolves to notes)", "---\nname: good\ndescription: d\n---\n")
	snap := read(t, ws, "")
	if len(snap.Skills) != 1 || len(snap.Skipped) != 0 {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
	if !strings.Contains(snap.SkillsIndexText(), "Path: /workspace/.agents/skills/good (resolves to notes)/SKILL.md") {
		t.Fatalf("index = %q", snap.SkillsIndexText())
	}
}

func TestReadSkipsASkillWhoseResolvedPathHasAControlCharacter(t *testing.T) {
	ws := workspace(t)
	target := filepath.Join(ws, "notes\nfile.md")
	if err := os.WriteFile(target, []byte("---\nname: linked\ndescription: d\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(ws, ".agents/skills/linked")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(dir, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, filepath.Join(dir, "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	snap := read(t, ws, "")
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "control character") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestContainerReadSkipsASkillWhoseResolvedPathHasAControlCharacter(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "notes\nfile.md")
	write(t, root, "notes\nfile.md", "---\nname: linked\ndescription: d\n---\n")
	dir := filepath.Join(root, ".agents/skills/linked")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(dir, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, filepath.Join(dir, "SKILL.md")); err != nil {
		t.Fatal(err)
	}
	snap := containerRead(t, root, "")
	if len(snap.Skills) != 0 || len(snap.Skipped) != 1 || !strings.Contains(snap.Skipped[0].Reason, "control character") {
		t.Fatalf("skills = %+v skipped = %+v", snap.Skills, snap.Skipped)
	}
}

func TestSnapshotValidateRejectsAMalformedStructuredIndex(t *testing.T) {
	entry := IndexEntry{Name: "s", Description: "d", Path: "/workspace/.agents/skills/s/SKILL.md"}
	index := Index{Entries: []IndexEntry{entry}}
	good := Snapshot{Root: "/workspace", SkillsTotal: 1,
		Skills:      []Skill{{Name: "s", Description: "d", Path: entry.Path}},
		SkillsIndex: index.Encode(), SkillsIndexSHA256: digest([]byte(index.Text()))}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(index.Encode(), "\n")
	for _, tc := range []struct {
		name  string
		index string
	}{
		{"missing trailer", lines[0]},
		{"repeated trailer", index.Encode() + "\n" + lines[len(lines)-1]},
		{"trailing data", index.Encode() + "\n{}"},
		{"wrong count", strings.Replace(index.Encode(), `"entries":1`, `"entries":2`, 1)},
		{"wrong omitted", strings.Replace(index.Encode(), `"omitted":0`, `"omitted":1`, 1)},
		{"wrong skipped", strings.Replace(index.Encode(), `"skipped":0`, `"skipped":1`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := good
			bad.SkillsIndex = tc.index
			bad.SkillsIndexSHA256 = digest([]byte(good.SkillsIndexText()))
			if err := bad.Validate(); err == nil {
				t.Fatal("accepted a malformed structured index")
			}
		})
	}
}

func TestDecodeIndexRejectsNonStrictObjects(t *testing.T) {
	entry := `{"name":"s","description":"d","path":"/workspace/.agents/skills/s/SKILL.md"}`
	trailer := `{"entries":1,"omitted":0,"skipped":0}`
	if _, err := DecodeIndex(entry + "\n" + trailer); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"null", "null"},
		{"missing fields", `{"entries":1}`},
		{"null count", `{"entries":1,"omitted":null,"skipped":0}`},
		{"duplicate trailer field", `{"entries":1,"entries":1,"omitted":0,"skipped":0}`},
		{"unknown trailer field", `{"entries":1,"omitted":0,"skipped":0,"extra":0}`},
		{"missing entry field", `{"description":"d","path":"/workspace/.agents/skills/s/SKILL.md"}` + "\n" + trailer},
		{"duplicate entry field", `{"name":"s","name":"s","description":"d","path":"/workspace/.agents/skills/s/SKILL.md"}` + "\n" + trailer},
		{"null entry field", `{"name":"s","description":"d","path":null}` + "\n" + trailer},
		{"unknown entry field", `{"name":"s","description":"d","path":"/workspace/.agents/skills/s/SKILL.md","extra":0}` + "\n" + trailer},
		{"non-object entry", `null` + "\n" + trailer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeIndex(tc.raw); err == nil {
				t.Fatal("accepted a non-strict index object")
			}
		})
	}
}
