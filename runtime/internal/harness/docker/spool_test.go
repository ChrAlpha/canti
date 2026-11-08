package docker

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestSpoolEvidenceRejectsSpecialFilesWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	command := filepath.Join(dir, "spool", "effect")
	if err := os.MkdirAll(command, 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(command, "output"), 0600); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := spoolEvidence(dir, "effect", "output", 1024); err == nil {
		t.Fatal("accepted FIFO as command output")
	}
	if time.Since(started) > time.Second {
		t.Fatal("host blocked opening mutable FIFO")
	}
}
func TestSpoolEvidenceCannotEscapeThroughAnEditableDirectory(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "output"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "spool"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "spool", "effect")); err != nil {
		t.Fatal(err)
	}
	if _, err := spoolEvidence(dir, "effect", "output", 1024); err == nil {
		t.Fatal("followed command directory outside spool")
	}
}
func TestSpoolEvidenceRejectsReplacedAndOversizedFiles(t *testing.T) {
	dir := t.TempDir()
	command := filepath.Join(dir, "spool", "effect")
	if err := os.MkdirAll(command, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(command, "output"), []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := spoolEvidence(dir, "effect", "output", 4); err == nil {
		t.Fatal("accepted oversized file")
	}
	if err := os.Symlink("output", filepath.Join(command, "completion.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := spoolEvidence(dir, "effect", "completion.json", 1024); err == nil {
		t.Fatal("accepted symlink metadata")
	}
	data, err := spoolEvidence(dir, "effect", "output", 5)
	if err != nil || string(data) != "12345" {
		t.Fatalf("bounded regular evidence=%q: %v", data, err)
	}
}
