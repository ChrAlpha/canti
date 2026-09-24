package harness

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
	if len(selected.Sources) < 2 {
		t.Fatalf("fixture has no overlapping selected ranges: %+v", selected)
	}
	result := retrieveOK(t, r, q)
	if result.NextOffset != selected.NextOffset || result.More != (selected.NextOffset < int64(len(body))) {
		t.Fatalf("selection cursor changed: selected=%+v result=%+v", selected, result)
	}
	if len(result.Sources) >= len(selected.Sources) {
		t.Fatalf("overlapping references were not coalesced: selected=%d result=%d", len(selected.Sources), len(result.Sources))
	}
	start, end := selected.Sources[0].Offset, selected.Sources[0].Offset+int64(selected.Sources[0].Length)
	group := 1
	for group < len(selected.Sources) && selected.Sources[group].Offset < end {
		end = max(end, selected.Sources[group].Offset+int64(selected.Sources[group].Length))
		group++
	}
	if group < 2 || len(result.Sources) != len(selected.Sources)-group+1 {
		t.Fatalf("unexpected coalescing: selected=%+v result=%+v", selected.Sources, result.Sources)
	}
	first := result.Sources[0]
	if first.Offset != start || first.Offset+int64(first.Length) != end {
		t.Fatalf("merged range does not cover selected bytes: %+v", result.Sources)
	}
	content, err := r.Expand(context.Background(), first)
	if err != nil || content != body[start:end] {
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
	if result.NextCursor != selected.NextCursor || result.More != selected.More || len(result.Sources) >= len(selected.Sources) {
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
	if err != nil || total > q.Limit || len(encoded) > RetrievalMaxBytes {
		t.Fatalf("coalesced result exceeded budget: bytes=%d references=%d err=%v", total, len(encoded), err)
	}
}

func TestRetrievalCoalesceFailsClosedOnChangedSource(t *testing.T) {
	body := strings.Repeat("needle ", 20)
	r, path := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "search", Path: path, Reason: "find evidence", Query: "needle", Limit: 320}
	selected := Retrieval{Query: q}
	searchRetrieval([]byte(body), q.Limit, &selected)
	if len(selected.Sources) < 2 {
		t.Fatal("fixture has no overlap")
	}
	if err := os.WriteFile(r.Imports+"/evidence", []byte(strings.Repeat("change ", 20)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.coalesceSources(context.Background(), selected.Sources); err == nil {
		t.Fatal("changed original source was accepted")
	}
	if err := os.Remove(r.Imports + "/evidence"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/null", r.Imports+"/evidence"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.coalesceSources(context.Background(), selected.Sources); err == nil {
		t.Fatal("symlink replacement was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.coalesceSources(ctx, selected.Sources); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	if got, err := r.coalesceSources(context.Background(), selected.Sources[:1]); err != nil || len(got) != 1 || got[0] != selected.Sources[0] {
		t.Fatalf("singleton was re-read: %+v %v", got, err)
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
			q := RetrievalQuery{Operation: "search", Path: path, Reason: "benchmark overlap", Query: "needle", Limit: RetrievalMaxBytes}
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
