package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func retrievalFixture(t *testing.T, body string) (Reader, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "evidence"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Reader{Imports: root}, "/canti/imports/evidence"
}

func retrieveOK(t *testing.T, r Reader, q RetrievalQuery) Retrieval {
	t.Helper()
	result, err := r.Retrieve(context.Background(), q)
	if err != nil || result.Error != "" {
		t.Fatalf("Retrieve: result=%+v err=%v", result, err)
	}
	return result
}

func TestRetrievalReadContinuationAndNoBody(t *testing.T) {
	body := strings.Repeat("unique original evidence ", 900)
	r, name := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "read", Path: name, Reason: "check original evidence"}
	var got strings.Builder
	for {
		result := retrieveOK(t, r, q)
		if len(result.Sources) != 1 || result.Sources[0].Length > RetrievalDefaultBytes {
			t.Fatalf("unbounded read: %+v", result)
		}
		encoded, err := json.Marshal(result)
		if err != nil || strings.Contains(string(encoded), "unique original") {
			t.Fatalf("body persisted: %s %v", encoded, err)
		}
		text, err := r.Expand(context.Background(), result.Sources[0])
		if err != nil {
			t.Fatal(err)
		}
		got.WriteString(text)
		if !result.More {
			break
		}
		if result.NextOffset <= q.Offset {
			t.Fatal("cursor did not advance")
		}
		q.Offset = result.NextOffset
	}
	if got.String() != body {
		t.Fatal("continuation lost or duplicated bytes")
	}
}

func TestRetrievalUnicodeAndInvalidText(t *testing.T) {
	r, name := retrievalFixture(t, "A中文B")
	q := RetrievalQuery{Operation: "read", Path: name, Reason: "read text", Limit: 5}
	result := retrieveOK(t, r, q)
	if result.NextOffset != 4 || result.Sources[0].Length != 4 {
		t.Fatalf("UTF-8 endpoint not aligned: %+v", result)
	}
	q.Offset = result.NextOffset
	result = retrieveOK(t, r, q)
	text, err := r.Expand(context.Background(), result.Sources[0])
	if err != nil || text != "文B" || result.More {
		t.Fatalf("continuation: %q %+v %v", text, result, err)
	}
	for _, q := range []RetrievalQuery{
		{Operation: "read", Path: name, Reason: "split start", Offset: 2},
		{Operation: "read", Path: name, Reason: "too small", Offset: 1, Limit: 1},
	} {
		got, err := r.Retrieve(context.Background(), q)
		if err != nil || got.Error == "" {
			t.Fatalf("accepted invalid text range: %+v %v", got, err)
		}
	}
	for _, body := range []string{"a\x00b", "a\xffb", "a\xe4"} {
		r, name := retrievalFixture(t, body)
		got, err := r.Retrieve(context.Background(), RetrievalQuery{Operation: "read", Path: name, Reason: "text"})
		if err != nil || got.Error == "" {
			t.Fatalf("accepted binary: %+v %v", got, err)
		}
	}
}

func TestRetrievalSearchScanAndOutputContinuation(t *testing.T) {
	body := strings.Repeat("x", retrievalScanBytes-2) + "needle" + strings.Repeat("y", retrievalScanBytes) + "needle"
	r, name := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "search", Path: name, Reason: "locate earlier decision", Query: "needle", Limit: 6}
	first := retrieveOK(t, r, q)
	if len(first.Sources) != 1 || first.Sources[0].Offset != retrievalScanBytes-2 || !first.More {
		t.Fatalf("missed scan boundary match: %+v", first)
	}
	q.Offset = first.NextOffset
	second := retrieveOK(t, r, q)
	if len(second.Sources) != 0 || !second.More || second.NextOffset != q.Offset+retrievalScanBytes {
		t.Fatalf("empty bounded scan must continue: %+v", second)
	}
	q.Offset = second.NextOffset
	third := retrieveOK(t, r, q)
	if len(third.Sources) != 1 || third.More {
		t.Fatalf("final match: %+v", third)
	}
	for _, result := range []Retrieval{first, third} {
		text, err := r.Expand(context.Background(), result.Sources[0])
		if text != "needle" || err != nil {
			t.Fatalf("match %q %v", text, err)
		}
	}
	r, name = retrievalFixture(t, strings.Repeat("a", 10000))
	result := retrieveOK(t, r, RetrievalQuery{Operation: "search", Path: name, Reason: "many hits", Query: "a", Limit: RetrievalMaxBytes})
	total := 0
	for _, source := range result.Sources {
		total += source.Length
	}
	if total > RetrievalMaxBytes || len(result.Sources) > 32 || !result.More || result.NextOffset == 0 {
		t.Fatalf("unbounded search: %d %+v", total, result)
	}
}

func TestRetrievalListContinuation(t *testing.T) {
	r, _ := retrievalFixture(t, "text")
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(r.Imports, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	q := RetrievalQuery{Operation: "list", Path: "/canti/imports", Reason: "find evidence", Limit: 25}
	var paths []string
	for {
		result := retrieveOK(t, r, q)
		if len(result.Sources) != 0 || len(result.Entries) != 1 {
			t.Fatalf("list bound: %+v", result)
		}
		paths = append(paths, result.Entries...)
		if !result.More {
			break
		}
		q.Offset = result.NextOffset
	}
	if strings.Join(paths, ",") != "/canti/imports/a,/canti/imports/b,/canti/imports/c,/canti/imports/evidence" {
		t.Fatalf("list continuation: %v", paths)
	}
}

func TestRetrievalRejectsInvalidQueriesAndPaths(t *testing.T) {
	r, name := retrievalFixture(t, "text")
	base := RetrievalQuery{Operation: "read", Path: name, Reason: "verify"}
	for _, mutate := range []func(*RetrievalQuery){
		func(q *RetrievalQuery) { q.Reason = " " },
		func(q *RetrievalQuery) { q.Reason = strings.Repeat("a", 1025) },
		func(q *RetrievalQuery) { q.Offset = -1 },
		func(q *RetrievalQuery) { q.Offset = 100 },
		func(q *RetrievalQuery) { q.Limit = -1 },
		func(q *RetrievalQuery) { q.Limit = RetrievalMaxBytes + 1 },
		func(q *RetrievalQuery) { q.Operation = "execute" },
		func(q *RetrievalQuery) { q.Operation = "search" },
		func(q *RetrievalQuery) { q.Operation, q.Query = "search", strings.Repeat("x", RetrievalMaxBytes+1) },
		func(q *RetrievalQuery) { q.Path = "/etc/passwd" },
		func(q *RetrievalQuery) { q.Path = "/canti/imports/../outside" },
		func(q *RetrievalQuery) { q.Path = "/canti/imports/.env" },
		func(q *RetrievalQuery) { q.Path = "/canti/imports/.env.local/file" },
		func(q *RetrievalQuery) { q.Path = "/canti/imports2/evidence" },
		func(q *RetrievalQuery) { q.Path = "/canti/runs/evidence" },
		func(q *RetrievalQuery) { q.Path = "/canti/imports" },
		func(q *RetrievalQuery) { q.Operation = "list" },
	} {
		q := base
		mutate(&q)
		result, err := r.Retrieve(context.Background(), q)
		if err != nil || result.Error == "" || len(result.Sources) != 0 {
			t.Fatalf("invalid query accepted: %+v => %+v %v", q, result, err)
		}
	}
}

func TestRetrievalRejectsSymlinksAndSpecialFiles(t *testing.T) {
	r, _ := retrievalFixture(t, "text")
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "external"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"escape": outside, "internal": "evidence"} {
		if err := os.Symlink(target, filepath.Join(r.Imports, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mkfifo(filepath.Join(r.Imports, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"escape/external", "internal", "pipe"} {
		result, err := r.Retrieve(context.Background(), RetrievalQuery{Operation: "read", Path: "/canti/imports/" + name, Reason: "inspect"})
		if err != nil || result.Error == "" {
			t.Fatalf("unsafe path accepted: %+v %v", result, err)
		}
	}
}

func TestRetrievalExpandVerifiesSourceAndDoesNotRecurse(t *testing.T) {
	body := `{"sources":[{"path":"/canti/imports/missing","offset":0,"length":4}]}`
	r, name := retrievalFixture(t, body)
	result := retrieveOK(t, r, RetrievalQuery{Operation: "read", Path: name, Reason: "inspect prior retrieval metadata"})
	text, err := r.Expand(context.Background(), result.Sources[0])
	if err != nil || text != body {
		t.Fatalf("metadata was not treated as plain text: %q %v", text, err)
	}
	if err := os.WriteFile(filepath.Join(r.Imports, "evidence"), []byte(strings.Repeat("x", len(body))), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Expand(context.Background(), result.Sources[0]); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("changed content accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(r.Imports, "evidence")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Expand(context.Background(), result.Sources[0]); err == nil {
		t.Fatal("missing source accepted")
	}
}

func TestRetrievalCancellation(t *testing.T) {
	r, name := retrievalFixture(t, "text")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Retrieve(ctx, RetrievalQuery{Operation: "read", Path: name, Reason: "inspect"}); err != context.Canceled {
		t.Fatalf("Retrieve: %v", err)
	}
	if _, err := r.Expand(ctx, SourceRange{}); err != context.Canceled {
		t.Fatalf("Expand: %v", err)
	}
}
