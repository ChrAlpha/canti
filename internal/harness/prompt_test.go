package harness

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/core"
)

func exitedStep(n int, command, preview string) StepRecord {
	code := 0
	return StepRecord{Step: n, Command: command, Observation: Observation{
		Execution: core.ExecutionExited, ExitCode: &code, Preview: preview,
		Output: ArtifactRef{Path: fmt.Sprintf("%d/output", n), Bytes: int64(len(preview)), Complete: true, Available: true},
	}}
}

func promptFrame() Frame { return testFrame() }

func fullFrame() Frame {
	f := promptFrame()
	f.Constraints = "do not touch vendor/"
	f.Step, f.Attempt = 3, 2
	f.Steps = []StepRecord{exitedStep(1, "ls", "go.mod\n"), exitedStep(2, "go test ./...", "FAIL")}
	f.Rejected = &Rejection{Error: `core: unknown field "cmd"`, Response: `{"cmd":"ls"}`}
	return f
}

func unlimited() PromptLimits { l := DefaultPromptLimits(); l.Total = 1 << 30; return l }

func mustPrompt(t *testing.T, f Frame, l PromptLimits) Prompt {
	t.Helper()
	p, err := f.Prompt(l)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(p.System) + len(p.User()); n > l.Total {
		t.Fatalf("prompt %d > %d", n, l.Total)
	}
	if !utf8.ValidString(p.System) || !utf8.ValidString(p.User()) {
		t.Fatal("invalid UTF-8")
	}
	if p.Stable != len(p.Blocks)-1 || p.Stable != len(f.Steps)+1 {
		t.Fatalf("stable blocks = %d, steps = %d", p.Stable, len(f.Steps))
	}
	return p
}

func TestPromptRendersCompleteCurrentPage(t *testing.T) {
	f := fullFrame()
	p := mustPrompt(t, f, DefaultPromptLimits())
	want := []string{"## Goal\n", "## Constraints\n", "## Environment\n", "## Context window\n", "## Current window steps\n", "### Step 1\n", "### Step 2\n", "## Progress\n", "## Rejected response\n"}
	last := -1
	for _, section := range want {
		at := strings.Index(p.User(), section)
		if at <= last {
			t.Fatalf("missing or reordered %q in %s", section, p.User())
		}
		last = at
	}
	if strings.Contains(p.User(), "## Notes") {
		t.Fatal("notes leaked into prompt")
	}
	if !strings.Contains(p.User(), "/canti/runs/run_x/steps/<step number in 8 digits>/") {
		t.Fatal("history reference missing")
	}
	if !strings.Contains(p.System, "canti:handover") {
		t.Fatal("handover protocol missing")
	}
}

func TestPromptDoesNotDropCurrentPageOnOverflow(t *testing.T) {
	f := promptFrame()
	f.Step, f.MaxSteps = 13, 20
	for n := 1; n <= 12; n++ {
		f.Steps = append(f.Steps, exitedStep(n, "echo", strings.Repeat("o", 200)))
	}
	full := mustPrompt(t, f, unlimited())
	l := unlimited()
	l.Total = len(full.System) + len(full.User()) - 1
	if p, err := f.Prompt(l); err == nil || len(p.Blocks) != 0 || !strings.Contains(err.Error(), "current context page exceeds") {
		t.Fatalf("overflow: %+v, %v", p, err)
	}
	l.Total++
	if p := mustPrompt(t, f, l); strings.Count(p.User(), "### Step ") != 12 {
		t.Fatalf("missing step: %s", p.User())
	}
}

func TestPromptPageBoundaryAndStablePrefix(t *testing.T) {
	f := promptFrame()
	f.Step, f.MaxSteps = 4, 10
	f.Earlier = 2
	f.Window = &Window{FirstStep: 3, SourceStep: 2, Handover: "inspect source"}
	f.Steps = []StepRecord{exitedStep(3, "pwd", "/workspace")}
	p := mustPrompt(t, f, DefaultPromptLimits())
	if strings.Contains(p.User(), "### Step 2") || !strings.Contains(p.User(), "2 earlier steps are not shown") || !strings.Contains(p.User(), "inspect source") {
		t.Fatalf("page boundary: %s", p.User())
	}
	if p.Stable != 2 {
		t.Fatalf("stable=%d", p.Stable)
	}
	f.Steps = nil
	f.Step = 3
	if p := mustPrompt(t, f, DefaultPromptLimits()); !strings.Contains(p.User(), "None.\n\n## Progress") {
		t.Fatalf("empty page: %s", p.User())
	}
}

func TestPromptHandoverInstructionAndRepair(t *testing.T) {
	f := fullFrame()
	f.Window.Requested = true
	f.HandoverBytes = 2048
	p := mustPrompt(t, f, DefaultPromptLimits())
	for _, s := range []string{"Handover required", "canti:handover", `run_id "run_x"`, "first_step 1", "2048 UTF-8 bytes", "Rejected response"} {
		if !strings.Contains(p.User(), s) {
			t.Fatalf("missing %q", s)
		}
	}
}

func TestPromptLimitsAndUTF8(t *testing.T) {
	f := fullFrame()
	l := DefaultPromptLimits()
	for _, edit := range []func(*PromptLimits){
		func(l *PromptLimits) { l.Goal = 0 }, func(l *PromptLimits) { l.Constraints = 0 },
		func(l *PromptLimits) { l.Environment = 0 }, func(l *PromptLimits) { l.Command = 0 },
		func(l *PromptLimits) { l.Preview = 0 }, func(l *PromptLimits) { l.Rejected = 0 },
		func(l *PromptLimits) { l.Total = 0 },
	} {
		bad := l
		edit(&bad)
		if _, err := f.Prompt(bad); err == nil {
			t.Fatalf("accepted limits %+v", bad)
		}
	}
	f.Goal = strings.Repeat("界", l.Goal/3+1)
	if _, err := f.Prompt(l); err == nil {
		t.Fatal("accepted oversized goal")
	}
	f = fullFrame()
	f.Environment = "ok\xff\xfe"
	f.Steps[0].Observation.Preview = "ok\xff\xfe"
	p := mustPrompt(t, f, l)
	if !strings.Contains(p.User(), "ok\uFFFD\uFFFD") {
		t.Fatal("invalid UTF-8 not replaced")
	}
}

func TestClip(t *testing.T) {
	for _, tc := range []struct {
		in    string
		limit int
		want  string
	}{
		{"abcd", 3, "abc…[truncated 1 bytes]"}, {"a日b", 4, "a日…[truncated 1 bytes]"},
		{"ok\xff\xfe", 8, "ok\uFFFD\uFFFD"}, {"🙂🙂", 7, "🙂…[truncated 4 bytes]"},
	} {
		if got := clip(tc.in, tc.limit); got != tc.want || !utf8.ValidString(got) {
			t.Fatalf("clip(%q,%d)=%q", tc.in, tc.limit, got)
		}
	}
}

func TestPromptDeterministicAndRejectsInvalidFrame(t *testing.T) {
	f := fullFrame()
	a := mustPrompt(t, f, DefaultPromptLimits())
	b := mustPrompt(t, f, DefaultPromptLimits())
	if !reflect.DeepEqual(a, b) {
		t.Fatal("equal frames differed")
	}
	f.Window = nil
	if _, err := f.Prompt(DefaultPromptLimits()); err == nil {
		t.Fatal("missing window accepted")
	}
}
