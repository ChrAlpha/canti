package archive

import (
	"context"
	"strings"
	"testing"
)

func TestDirectorySearchTextOnlyFailureAndIndividualRecovery(t *testing.T) {
	for _, invalid := range []string{"needle\x00binary", "needle\xffinvalid"} {
		r := directoryFixture(t, map[string]string{
			"a.txt": "needle first evidence", "b.bin": invalid, "c.txt": "needle later evidence",
		})
		q := RetrievalQuery{Operation: "search", Path: "/canti/imports", Query: "needle", Reason: "inspect mixed archive"}
		result, err := r.Retrieve(context.Background(), q)
		if err != nil || !strings.Contains(result.Error, "directory search b.bin:") || len(result.Sources) != 0 || result.More || result.NextCursor != "" {
			t.Fatalf("failed batch presented partial success or lost diagnostic: %+v %v", result, err)
		}
		for _, name := range []string{"a.txt", "c.txt"} {
			read := retrieveOK(t, r, RetrievalQuery{Operation: "read", Path: "/canti/imports/" + name, Reason: "read original file after directory failure"})
			if len(read.Sources) != 1 || read.More {
				t.Fatalf("individual recovery: %+v", read)
			}
			text, err := r.expand(context.Background(), read.Sources[0])
			if err != nil || !strings.Contains(text, "evidence") {
				t.Fatalf("individual evidence unavailable: %q %v", text, err)
			}
		}
	}
}
