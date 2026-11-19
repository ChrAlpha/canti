package host

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	projectctx "github.com/ChrAlpha/canti/runtime/internal/project"
)

func writeWorkspace(t *testing.T, h *Host, rel, body string) string {
	t.Helper()
	path := filepath.Join(h.opts.Workspace, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func projectOf(t *testing.T, h *Host, id string) projectctx.Snapshot {
	t.Helper()
	p, err := h.store.Project(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOpenBindsProjectInstructionsAndSkills(t *testing.T) {
	h := testHost(t, Options{})
	writeWorkspace(t, h, "AGENTS.md", "Use tabs.\n")
	writeWorkspace(t, h, ".agents/skills/go/SKILL.md", "---\nname: go\ndescription: Go conventions.\n---\n")
	writeWorkspace(t, h, ".agents/skills/bad/SKILL.md", "---\nname: bad\n---\n")
	ack := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")})
	p := projectOf(t, h, ack.Run.ID)
	if len(p.Instructions) != 1 || p.Instructions[0].Path != "/workspace/AGENTS.md" || p.Instructions[0].Content != "Use tabs.\n" {
		t.Fatalf("instructions = %+v", p.Instructions)
	}
	if len(p.Skills) != 1 || p.Skills[0].Name != "go" || len(p.Skipped) != 1 || !strings.Contains(p.Skipped[0].Reason, "description") {
		t.Fatalf("skills = %+v skipped = %+v", p.Skills, p.Skipped)
	}
	c, err := DecodeConfig(ack.Run, h.opts.Workspace)
	if err != nil || c.NoProjectContext {
		t.Fatalf("config = %+v, %v", c, err)
	}
}

func TestOpenFollowsAnInstructionSymlinkInsideTheWorkspace(t *testing.T) {
	h := testHost(t, Options{})
	writeWorkspace(t, h, "SHARED.md", "shared\n")
	if err := os.Symlink("SHARED.md", filepath.Join(h.opts.Workspace, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	ack := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")})
	p := projectOf(t, h, ack.Run.ID)
	if len(p.Instructions) != 1 || p.Instructions[0].Path != "/workspace/AGENTS.md" || p.Instructions[0].ResolvedPath != "/workspace/SHARED.md" {
		t.Fatalf("instructions = %+v", p.Instructions)
	}
}

func TestAdmissionRefusesAnInstructionSymlinkOutsideTheWorkspace(t *testing.T) {
	h := testHost(t, Options{})
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "AGENTS.md")
	if err := os.WriteFile(outsideFile, []byte("outside\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideFile, filepath.Join(h.opts.Workspace, "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	_, err := h.Submit(context.Background(), Submission{Goal: "g", Spec: testSpec(h), Binding: binding("http://127.0.0.1:1")})
	var spec SpecError
	if !errors.As(err, &spec) || !strings.Contains(err.Error(), "/workspace/AGENTS.md") || !strings.Contains(err.Error(), "leaves the workspace") {
		t.Fatalf("escaping instruction = %v", err)
	}
	if _, err := h.store.LastRun(context.Background()); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("a refused admission created a run: %v", err)
	}
}

func TestAdmissionRefusesOversizeProjectInstructionsWithFilesAndSizes(t *testing.T) {
	h := testHost(t, Options{})
	writeWorkspace(t, h, "AGENTS.md", strings.Repeat("a", 40<<10))
	writeWorkspace(t, h, "sub/AGENTS.md", strings.Repeat("b", 40<<10))
	// The ephemeral working directory is the workspace root, so bind the sub
	// directory through the project reader rather than the host map.
	_, err := projectctx.Read(projectctx.Request{Workspace: h.opts.Workspace, Root: "/workspace", Workdir: "/workspace/sub"})
	if err == nil || !strings.Contains(err.Error(), "81920 bytes") || !strings.Contains(err.Error(), "/workspace/AGENTS.md (40960 bytes)") || !strings.Contains(err.Error(), "/workspace/sub/AGENTS.md (40960 bytes)") {
		t.Fatalf("oversize instructions = %v", err)
	}
	writeWorkspace(t, h, "sub/AGENTS.md", "small\n")
	writeWorkspace(t, h, "AGENTS.md", strings.Repeat("a", (64<<10)+1))
	_, err = h.Submit(context.Background(), Submission{Goal: "g", Spec: testSpec(h), Binding: binding("http://127.0.0.1:1")})
	var spec SpecError
	if !errors.As(err, &spec) || !strings.Contains(err.Error(), "/workspace/AGENTS.md (65537 bytes)") || !strings.Contains(err.Error(), "65536") {
		t.Fatalf("oversize admission = %v", err)
	}
}

func TestNoProjectContextIsRecordedAndBindsNothing(t *testing.T) {
	h := testHost(t, Options{})
	writeWorkspace(t, h, "AGENTS.md", "Use tabs.\n")
	spec := testSpec(h)
	spec.Config.NoProjectContext = true
	ack := submit(t, h, Submission{Spec: spec, Binding: binding("http://127.0.0.1:1")})
	c, err := DecodeConfig(ack.Run, h.opts.Workspace)
	if err != nil || !c.NoProjectContext {
		t.Fatalf("config = %+v, %v", c, err)
	}
	if p := projectOf(t, h, ack.Run.ID); p.Root != "" || len(p.Instructions) != 0 {
		t.Fatalf("opt-out bound project context: %+v", p)
	}
}

func TestAQueuedFollowUpBindsProjectContextWhenItOpens(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "first"})
	writeWorkspace(t, h, "AGENTS.md", "first rules\n")
	first := submit(t, h, Submission{Goal: "first", Binding: binding(url)})
	queued := submit(t, h, Submission{Goal: "second", Delivery: harness.DeliveryFollowUp, Binding: binding(url)})
	if queued.Opened() {
		t.Fatalf("a follow-up opened before the first run returned: %+v", queued)
	}
	writeWorkspace(t, h, "AGENTS.md", "second rules\n")
	drive, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := drive.Result()
	if err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("first run = %+v, %v", snap.State, err)
	}
	if err := h.startFollowUp(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	last, err := h.store.LastRun(context.Background())
	if err != nil || last.ID == first.Run.ID {
		t.Fatalf("second run = %+v, %v", last, err)
	}
	p := projectOf(t, h, last.ID)
	if len(p.Instructions) != 1 || p.Instructions[0].Content != "second rules\n" {
		t.Fatalf("follow-up instructions = %+v", p.Instructions)
	}
	if p := projectOf(t, h, first.Run.ID); p.Instructions[0].Content != "first rules\n" {
		t.Fatalf("first run changed: %+v", p.Instructions)
	}
}

func TestValidateBindsNoProjectContextForASteer(t *testing.T) {
	h := testHost(t, Options{})
	// A quiescent steer opens a run, so preflight binds the project context
	// and refuses the same oversize file Submit would.
	writeWorkspace(t, h, "AGENTS.md", strings.Repeat("a", (64<<10)+1))
	if _, _, err := Validate(context.Background(), h.opts.State, Submission{Goal: "steer", Delivery: harness.DeliverySteer, Spec: testSpec(h)}); err == nil {
		t.Fatal("a quiescent steer skipped project context")
	}
	// A steer beside an open run does not open one and must not read it.
	h2 := testHost(t, Options{})
	submit(t, h2, Submission{Binding: binding("http://127.0.0.1:1")})
	writeWorkspace(t, h2, "AGENTS.md", strings.Repeat("a", (64<<10)+1))
	if _, _, err := Validate(context.Background(), h2.opts.State, Submission{Goal: "steer", Delivery: harness.DeliverySteer, Spec: testSpec(h2)}); err != nil {
		t.Fatalf("a working steer read project context: %v", err)
	}
}

func TestViewDoesNotReadInstructionContent(t *testing.T) {
	h := testHost(t, Options{})
	writeWorkspace(t, h, "AGENTS.md", "Use tabs.\n")
	ack := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")})
	db, err := sql.Open("sqlite", filepath.Join(h.opts.State, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE runs SET project = '{' WHERE id = ?`, ack.Run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.View(context.Background(), ack.Run.ID); err != nil {
		t.Fatalf("View decoded instruction content: %v", err)
	}
}

func writeFile(t *testing.T, root, rel, body string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestResumeOpenDoesNotDecodeAClosedRunsProjectContent(t *testing.T) {
	h := testHost(t, Options{})
	url := answering(t, core.Decision{Kind: core.Return, Payload: "done"})
	first := submit(t, h, Submission{Goal: "first", Binding: binding(url)})
	submit(t, h, Submission{Goal: "second", Delivery: harness.DeliveryFollowUp, Binding: binding(url)})
	drive, err := h.Drive(context.Background(), first.Run.ID, binding(url), DriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := drive.Result()
	if err != nil || snap.State.Phase != core.PhaseReturned {
		t.Fatalf("first run = %+v, %v", snap.State, err)
	}
	db, err := sql.Open("sqlite", filepath.Join(h.opts.State, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE runs SET project = '{' WHERE id = ?`, first.Run.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.ResumeOpen(context.Background()); err != nil {
		t.Fatalf("ResumeOpen decoded a closed run's project content: %v", err)
	}
	last, err := h.store.LastRun(context.Background())
	if err != nil || last.ID == first.Run.ID {
		t.Fatalf("queued follow-up did not open: %+v, %v", last, err)
	}
}

func TestViewDegradesACorruptProjectMetadata(t *testing.T) {
	h := testHost(t, Options{})
	writeWorkspace(t, h, "AGENTS.md", "Use tabs.\n")
	ack := submit(t, h, Submission{Binding: binding("http://127.0.0.1:1")})
	db, err := sql.Open("sqlite", filepath.Join(h.opts.State, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE runs SET project_meta = '{' WHERE id = ?`, ack.Run.ID); err != nil {
		t.Fatal(err)
	}
	v, err := h.View(context.Background(), ack.Run.ID)
	if err != nil {
		t.Fatalf("View failed on corrupt metadata: %v", err)
	}
	if v.ProjectUnavailable == "" || v.Project.Root != "" {
		t.Fatalf("view = %+v", v)
	}
	if err := h.ResumeOpen(context.Background()); err != nil {
		t.Fatalf("ResumeOpen failed on corrupt metadata: %v", err)
	}
}

func TestValidateDoesNotBindAWorkingFollowUp(t *testing.T) {
	h := testHost(t, Options{})
	submit(t, h, Submission{Goal: "first", Binding: binding("http://127.0.0.1:1")})
	writeWorkspace(t, h, "AGENTS.md", strings.Repeat("a", (64<<10)+1))
	sub := Submission{Goal: "follow", Delivery: harness.DeliveryFollowUp, Spec: testSpec(h), Binding: binding("http://127.0.0.1:1")}
	if _, _, err := Validate(context.Background(), h.opts.State, sub); err != nil {
		t.Fatalf("working follow-up validation read project context: %v", err)
	}
	ack, err := h.Submit(context.Background(), sub)
	if err != nil {
		t.Fatalf("working follow-up submit = %v", err)
	}
	if ack.Input.Disposition != harness.DispositionQueued {
		t.Fatalf("disposition = %s", ack.Input.Disposition)
	}
	// A working follow-up with a prospective persistent environment must not
	// contact or resolve that environment during validation or queueing.
	h2 := testHost(t, Options{})
	submit(t, h2, Submission{Goal: "first", Binding: binding("http://127.0.0.1:1")})
	persistent := testSpec(h2)
	persistent.Config.Ephemeral = false
	persistent.Config.Environment = filepath.Join(t.TempDir(), "missing-environment")
	persistent.Config.Attachment = nil
	persistent.Config.Image, persistent.Config.Network, persistent.Config.Memory = "", "", ""
	persistent.Config.CPUs, persistent.Config.Pids = "", 0
	sub2 := Submission{Goal: "persistent follow", Delivery: harness.DeliveryFollowUp, Spec: persistent, Binding: binding("http://127.0.0.1:1")}
	if _, _, err := Validate(context.Background(), h2.opts.State, sub2); err != nil {
		t.Fatalf("persistent working follow-up validation = %v", err)
	}
	if ack, err := h2.Submit(context.Background(), sub2); err != nil || ack.Input.Disposition != harness.DispositionQueued {
		t.Fatalf("persistent working follow-up submit = %+v, %v", ack, err)
	}
}
