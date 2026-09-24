package harness_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/internal/harness"
)

func TestRetrievedFencedSourceReachesFrozenRequestWithoutLostBytes(t *testing.T) {
	archive := t.TempDir()
	const evidence = "Original evidence:\n```go\nvalue := 42\n```\nFINAL_SOURCE_BYTE_Z"
	if err := os.WriteFile(filepath.Join(archive, "evidence"), []byte(evidence), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, limits, harness.Budget{},
		execReply(`canti:retrieve {"operation":"read","path":"/canti/imports/evidence","reason":"read exact original evidence"}`),
		returnReply("done"))
	f.cfg.Reader = &harness.Reader{Imports: archive}
	if _, err := f.loop().Run(f.bounded(), runID); err != nil {
		t.Fatal(err)
	}
	req, err := f.store.Request(f.bounded(), runID, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(req.Body), evidence) {
		t.Fatal("already-bounded original evidence lost source bytes while adding presentation fences")
	}
}
