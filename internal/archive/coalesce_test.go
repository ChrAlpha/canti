package archive

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestRetrievalSearchCoalescesSelectedFileRanges(t *testing.T) {
	body := strings.Repeat("needle ", 30) + strings.Repeat("x", 400) + "needle"
	r, path := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "search", Path: path, Reason: "find evidence", Query: "needle", Limit: 320}
	selected := Retrieval{Query: q}
	searchRetrieval([]byte(body), q.Limit, &selected)
	if len(selected.Sources) != 1 {
		t.Fatalf("dense matches did not select one range: %+v", selected)
	}
	result := retrieveOK(t, r, q)
	if result.NextOffset != selected.NextOffset || result.More != (selected.NextOffset < int64(len(body))) {
		t.Fatalf("selection cursor changed: selected=%+v result=%+v", selected, result)
	}
	if len(result.Sources) != 1 || result.Sources[0] != selected.Sources[0] {
		t.Fatalf("selection changed after delivery: selected=%+v result=%+v", selected.Sources, result.Sources)
	}
	first := result.Sources[0]
	content, err := r.Expand(context.Background(), first)
	if err != nil || content != body[first.Offset:first.Offset+int64(first.Length)] {
		t.Fatalf("merged bytes or SHA mismatch: %q %v", content, err)
	}
}

func TestRetrievalSearchCoalescesDirectoryWithoutChangingCursor(t *testing.T) {
	r := directoryFixture(t, map[string]string{"a": strings.Repeat("needle ", 30), "b": "needle"})
	q := RetrievalQuery{Operation: "search", Path: "/canti/imports", Reason: "find evidence", Query: "needle", Limit: 320}
	dir, err := r.open(q.Path)
	if err != nil {
		t.Fatal(err)
	}
	selected := Retrieval{Query: q}
	err = r.searchDirectory(context.Background(), dir, q.Limit, &selected)
	dir.Close()
	if err != nil {
		t.Fatal(err)
	}
	result := retrieveOK(t, r, q)
	if result.NextCursor != selected.NextCursor || result.More != selected.More || len(result.Sources) != len(selected.Sources) {
		t.Fatalf("directory selection changed: selected=%+v result=%+v", selected, result)
	}
	total := 0
	for _, source := range result.Sources {
		if _, err := r.Expand(context.Background(), source); err != nil {
			t.Fatal(err)
		}
		total += source.Length
	}
	encoded, err := json.Marshal(result.Sources)
	if err != nil || total > q.Limit || len(encoded) > MaxBytes {
		t.Fatalf("coalesced result exceeded budget: bytes=%d references=%d err=%v", total, len(encoded), err)
	}
}

func TestRetrievalSearchSelectionAlreadyCoalesced(t *testing.T) {
	r := directoryFixture(t, map[string]string{
		"a": strings.Repeat("needle ", 30) + strings.Repeat("x", 400) + "needle",
		"b": "needle" + strings.Repeat("x", 400) + "needle",
		"c": strings.Repeat("needle ", 30),
	})
	q := RetrievalQuery{Operation: "search", Path: "/canti/imports", Reason: "check selected ranges", Query: "needle", Limit: MaxBytes}
	result := retrieveOK(t, r, q)
	if len(result.Sources) < 4 {
		t.Fatalf("fixture did not exercise separate ranges: %+v", result)
	}
	for i, source := range result.Sources {
		if _, err := r.Expand(context.Background(), source); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			continue
		}
		previous := result.Sources[i-1]
		if previous.Path > source.Path || previous.Path == source.Path && previous.Offset+int64(previous.Length) >= source.Offset {
			t.Fatalf("ranges are out of order, overlapping, or adjacent: %+v", result.Sources)
		}
	}
}

func BenchmarkRetrievalReaderCoalesce(b *testing.B) {
	root := b.TempDir()
	if err := os.WriteFile(root+"/evidence", []byte(strings.Repeat("needle ", 1000)), 0o600); err != nil {
		b.Fatal(err)
	}
	r := Reader{Imports: root}
	for _, path := range []string{"/canti/imports/evidence", "/canti/imports"} {
		b.Run(path, func(b *testing.B) {
			q := RetrievalQuery{Operation: "search", Path: path, Reason: "benchmark overlap", Query: "needle", Limit: MaxBytes}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := r.Retrieve(context.Background(), q)
				if err != nil || result.Error != "" {
					b.Fatalf("Retrieve: %+v %v", result, err)
				}
			}
		})
	}
}
