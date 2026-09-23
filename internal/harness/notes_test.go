package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const notesName = ".canti/notes.md"

func writeFile(t *testing.T, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, name string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, name); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
}

func TestReadNotes(t *testing.T) {
	tests := []struct {
		name, content string
		limit         int
		want          string
		truncated     bool
	}{
		{name: "small", content: "plan: fix tests\n", limit: 64, want: "plan: fix tests\n"},
		{name: "empty", content: "", limit: 64},
		{name: "exactly the limit", content: "abcd", limit: 4, want: "abcd"},
		{name: "one byte over", content: "abcde", limit: 4, want: "abcd", truncated: true},
		{name: "huge", content: strings.Repeat("x", 1<<20), limit: 10, want: strings.Repeat("x", 10), truncated: true},
		{name: "cut inside a rune", content: "ab日本", limit: 4, want: "ab", truncated: true},
		{name: "zero limit", content: "x", limit: 0, truncated: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := t.TempDir()
			writeFile(t, filepath.Join(ws, notesName), tt.content)
			got, truncated, err := ReadNotes(ws, notesName, tt.limit)
			if err != nil || got != tt.want || truncated != tt.truncated {
				t.Fatalf("ReadNotes() = %q, %v, %v; want %q, %v, nil", got, truncated, err, tt.want, tt.truncated)
			}
		})
	}
}

func TestReadNotesMissing(t *testing.T) {
	ws := t.TempDir()
	check := func() {
		t.Helper()
		got, truncated, err := ReadNotes(ws, notesName, 64)
		if err != nil || got != "" || truncated {
			t.Fatalf("ReadNotes() = %q, %v, %v; want empty notes", got, truncated, err)
		}
	}
	check()
	if err := os.Mkdir(filepath.Join(ws, ".canti"), 0o755); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestReadNotesFollowsSymlinkInsideWorkspace(t *testing.T) {
	ws := t.TempDir()
	writeFile(t, filepath.Join(ws, "docs", "plan.md"), "inside")
	symlink(t, filepath.Join("..", "docs", "plan.md"), filepath.Join(ws, notesName))
	got, truncated, err := ReadNotes(ws, notesName, 64)
	if err != nil || got != "inside" || truncated {
		t.Fatalf("ReadNotes() = %q, %v, %v; want the linked notes", got, truncated, err)
	}
}

func TestReadNotesRejects(t *testing.T) {
	const secret = "host secret"
	tests := []struct {
		name  string
		setup func(t *testing.T, ws, outside string) string
	}{
		{name: "directory", setup: func(t *testing.T, ws, _ string) string {
			if err := os.MkdirAll(filepath.Join(ws, notesName), 0o755); err != nil {
				t.Fatal(err)
			}
			return notesName
		}},
		{name: "symlink to an outside file", setup: func(t *testing.T, ws, outside string) string {
			symlink(t, filepath.Join(outside, "secret"), filepath.Join(ws, notesName))
			return notesName
		}},
		{name: "relative symlink out of the workspace", setup: func(t *testing.T, ws, outside string) string {
			rel, err := filepath.Rel(filepath.Join(ws, ".canti"), filepath.Join(outside, "secret"))
			if err != nil {
				t.Fatal(err)
			}
			symlink(t, rel, filepath.Join(ws, notesName))
			return notesName
		}},
		{name: "symlinked directory outside", setup: func(t *testing.T, ws, outside string) string {
			writeFile(t, filepath.Join(outside, "notes.md"), secret)
			symlink(t, outside, filepath.Join(ws, ".canti"))
			return notesName
		}},
		{name: "parent path", setup: func(t *testing.T, ws, outside string) string {
			rel, err := filepath.Rel(ws, filepath.Join(outside, "secret"))
			if err != nil {
				t.Fatal(err)
			}
			return rel
		}},
		{name: "absolute path", setup: func(t *testing.T, _, outside string) string {
			return filepath.Join(outside, "secret")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws, outside := t.TempDir(), t.TempDir()
			writeFile(t, filepath.Join(outside, "secret"), secret)
			got, _, err := ReadNotes(ws, tt.setup(t, ws, outside), 64)
			if err == nil || got != "" {
				t.Fatalf("ReadNotes() = %q, %v; want an error and no content", got, err)
			}
		})
	}
}

func TestReadNotesRejectsBadArguments(t *testing.T) {
	ws := t.TempDir()
	if _, _, err := ReadNotes(filepath.Join(ws, "missing"), notesName, 64); err == nil {
		t.Fatal("ReadNotes() on a missing workspace succeeded")
	}
	if _, _, err := ReadNotes(ws, notesName, -1); err == nil {
		t.Fatal("ReadNotes() with a negative limit succeeded")
	}
}
