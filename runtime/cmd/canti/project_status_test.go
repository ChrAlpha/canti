package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	projectctx "github.com/ChrAlpha/canti/runtime/internal/project"
)

func TestStatusReportsTheBoundProjectContext(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if err := os.WriteFile(filepath.Join(f.workspace, "AGENTS.md"), []byte("Use tabs.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.workspace, ".agents/skills/go"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.workspace, ".agents/skills/go/SKILL.md"), []byte("---\nname: go\ndescription: Go conventions.\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := f.cli(nil, "run", "-goal", "inspect project context"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	s := f.status()
	if s.Project == nil || len(s.Project.Instructions) != 1 || s.Project.Instructions[0].Path != "/workspace/AGENTS.md" ||
		len(s.Project.Skills) != 1 || s.Project.Skills[0].Name != "go" {
		t.Fatalf("status project = %+v", s.Project)
	}
}

func TestStatusReportsTheProjectContextOptOut(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if err := os.WriteFile(filepath.Join(f.workspace, "AGENTS.md"), []byte("Use tabs.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := f.cli(nil, "run", "-goal", "inspect opt out", "-no-project-context"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	s := f.status()
	if s.Project == nil || !s.Project.Disabled || len(s.Project.Instructions) != 0 {
		t.Fatalf("status project = %+v", s.Project)
	}
}

func TestStatusDegradesACorruptProjectMetadata(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "inspect corrupt metadata"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	run := f.status()
	db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE runs SET project_meta = '{' WHERE id = ?`, run.Run.ID); err != nil {
		t.Fatal(err)
	}
	status := f.status()
	if status.ProjectUnavailable == "" || status.Project != nil {
		t.Fatalf("status = %+v", status)
	}
	if len(status.ProjectUnavailable) > projectctx.MaxDiagnosticBytes {
		t.Fatalf("project diagnostic is %d bytes", len(status.ProjectUnavailable))
	}
}

func TestStatusDegradesAnOversizedProjectMetadata(t *testing.T) {
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "inspect oversized metadata"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	run := f.status()
	db, err := sql.Open("sqlite", filepath.Join(f.state, "canti.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	meta := projectctx.Meta{Root: "/workspace", Instructions: []projectctx.MetaFile{{
		Path: "/outside/" + strings.Repeat("a", 1<<20), SHA256: strings.Repeat("a", 64)}}}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE runs SET project_meta = ? WHERE id = ?`, string(data), run.Run.ID); err != nil {
		t.Fatal(err)
	}
	status := f.status()
	if status.ProjectUnavailable == "" || status.Project != nil {
		t.Fatalf("status = %+v", status)
	}
	if len(status.ProjectUnavailable) > projectctx.MaxDiagnosticBytes {
		t.Fatalf("project diagnostic is %d bytes", len(status.ProjectUnavailable))
	}
}
