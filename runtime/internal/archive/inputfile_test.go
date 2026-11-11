package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func inputFileRoot(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, InputDir(1))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, MetaFile), []byte("meta"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, dir
}

func TestInputFileReadsBoundedRegularFiles(t *testing.T) {
	root, dir := inputFileRoot(t)
	want := "exactly expected"
	if err := os.WriteFile(filepath.Join(dir, "text"), []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := (Reader{Inputs: root}).InputFile(context.Background(), 1, "text", len(want))
	if err != nil || string(got) != want {
		t.Fatalf("InputFile = %q, %v", got, err)
	}
}

func TestInputFileRejectsSymlinkSibling(t *testing.T) {
	root, dir := inputFileRoot(t)
	if err := os.Symlink(filepath.Join(dir, MetaFile), filepath.Join(dir, "text")); err != nil {
		t.Fatal(err)
	}
	if _, err := (Reader{Inputs: root}).InputFile(context.Background(), 1, "text", 4); err == nil {
		t.Fatal("symlinked input file was accepted")
	}
}

func TestInputFileRejectsOversizedSibling(t *testing.T) {
	root, dir := inputFileRoot(t)
	want := "over the expected record"
	if err := os.WriteFile(filepath.Join(dir, "text"), []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (Reader{Inputs: root}).InputFile(context.Background(), 1, "text", len(want)-1); err == nil {
		t.Fatal("oversized input file was accepted")
	}
}
