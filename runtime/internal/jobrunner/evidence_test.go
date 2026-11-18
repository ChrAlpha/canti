package jobrunner

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("destination full") }

func TestCopyEvidenceSeparatesSourceAndDestinationFailures(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "output"), []byte("hello"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := CopyEvidence(root, "output", 1024, failingWriter{}, nil); !errors.Is(err, ErrEvidenceDestination) || errors.Is(err, ErrEvidenceSource) {
		t.Fatalf("destination failure = %v", err)
	}
	if _, err := CopyEvidence(root, "missing", 1024, failingWriter{}, nil); !errors.Is(err, ErrEvidenceSource) || errors.Is(err, ErrEvidenceDestination) {
		t.Fatalf("source failure = %v", err)
	}
}
