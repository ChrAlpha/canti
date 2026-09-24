package harness_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness"
)

func TestSuccessfulRetrievalDoesNotReportMissingIncompleteOutput(t *testing.T) {
	archive := t.TempDir()
	const evidence = "complete original source evidence"
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"verify earlier evidence"}`),
		returnReply("done"))
	f.cfg.Reader = &harness.Reader{Imports: archive}
	if _, err := f.loop().Run(f.bounded(), runID); err != nil {
		t.Fatal(err)
	}
	if len(f.model.sent) != 2 {
		t.Fatalf("sent %d requests", len(f.model.sent))
	}
	request := string(f.model.sent[1])
	if !strings.Contains(request, evidence) || !strings.Contains(request, `"more":false`) {
		t.Fatal("successful retrieval result did not reach the model")
	}
	if strings.Contains(request, "Output: 0 bytes, incomplete, unavailable") {
		t.Fatal("successful retrieval was presented as missing incomplete command output")
	}
}
