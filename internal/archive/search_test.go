package archive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func directoryFixture(t *testing.T, files map[string]string) Reader {
	t.Helper()
	r := Reader{Imports: t.TempDir()}
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(r.Imports, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func directoryQuery() RetrievalQuery {
	return RetrievalQuery{Operation: "search", Path: "/canti/imports", Reason: "locate original evidence", Query: "needle"}
}

func TestRetrievalDirectorySearchAcrossFiles(t *testing.T) {
	r := directoryFixture(t, map[string]string{"c": "needle", "a": "needle", "b": "no needle end"})
	q := directoryQuery()
	q.Limit = len(q.Query)
	var sources []SourceRange
	for attempts := 0; attempts < 10; attempts++ {
		result := retrieveOK(t, r, q)
		if len(result.Sources) > 1 || result.NextOffset != 0 {
			t.Fatalf("directory result exceeded bound or used offset: %+v", result)
		}
		for _, source := range result.Sources {
			text, err := r.expand(context.Background(), source)
			if err != nil || text != "needle" {
				t.Fatalf("bad source: %q %v", text, err)
			}
		}
		sources = append(sources, result.Sources...)
		if !result.More {
			if result.NextCursor != "" {
				t.Fatal("exhausted search retained cursor")
			}
			break
		}
		if result.NextCursor == "" || result.NextCursor == q.Cursor {
			t.Fatal("directory continuation did not advance")
		}
		q.Cursor = result.NextCursor
	}
	if len(sources) != 3 || sources[0].Path != "/canti/imports/a" || sources[1].Path != "/canti/imports/b" || sources[2].Path != "/canti/imports/c" {
		t.Fatalf("directory skipped or duplicated hits: %+v", sources)
	}
}

func TestRetrievalDirectorySearchAggregateScanBudget(t *testing.T) {
	r := directoryFixture(t, map[string]string{
		"a": strings.Repeat("x", scanBytes-10),
		"b": "xxxxxxxneedle",
	})
	q := directoryQuery()
	first := retrieveOK(t, r, q)
	if len(first.Sources) != 0 || !first.More {
		t.Fatalf("scan budget did not bound aggregate files: %+v", first)
	}
	q.Cursor = first.NextCursor
	cursor, err := decodeDirectoryCursor(q)
	if err != nil || cursor.File != "b" || cursor.Offset != 5 {
		t.Fatalf("boundary overlap cursor: %+v %v", cursor, err)
	}
	second := retrieveOK(t, r, q)
	if len(second.Sources) != 1 || second.More || second.Sources[0].Offset > 7 {
		t.Fatalf("cross-scan match lost: %+v", second)
	}
	text, err := r.expand(context.Background(), second.Sources[0])
	if err != nil || !strings.Contains(text, "needle") {
		t.Fatalf("cross-scan evidence: %q %v", text, err)
	}
}

func TestRetrievalDirectorySearchUnicodeBoundary(t *testing.T) {
	r := directoryFixture(t, map[string]string{
		"a": strings.Repeat("x", scanBytes-2) + "中文证据",
	})
	q := directoryQuery()
	q.Query = "中文"
	first := retrieveOK(t, r, q)
	if len(first.Sources) != 0 || !first.More {
		t.Fatalf("partial Unicode scan: %+v", first)
	}
	q.Cursor = first.NextCursor
	second := retrieveOK(t, r, q)
	if len(second.Sources) != 1 || second.More {
		t.Fatalf("Unicode continuation: %+v", second)
	}
	text, err := r.expand(context.Background(), second.Sources[0])
	if err != nil || !strings.Contains(text, q.Query) {
		t.Fatalf("Unicode evidence: %q %v", text, err)
	}
}

func TestRetrievalDirectorySearchNoHitsAndFlatIsolation(t *testing.T) {
	r := directoryFixture(t, map[string]string{"a": "NEEDLE", "empty": ""})
	if err := os.Mkdir(filepath.Join(r.Imports, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.Imports, "child/evidence"), []byte("needle"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("child/evidence", filepath.Join(r.Imports, "link")); err != nil {
		t.Fatal(err)
	}
	result := retrieveOK(t, r, directoryQuery())
	if result.More || result.NextCursor != "" || len(result.Sources) != 0 {
		t.Fatalf("search must be case-sensitive, flat, and ignore symlinks: %+v", result)
	}
}

func TestRetrievalDirectorySearchCursorValidation(t *testing.T) {
	r := directoryFixture(t, map[string]string{"a": "needle needle"})
	q := directoryQuery()
	q.Limit = 6
	first := retrieveOK(t, r, q)
	q.Cursor = first.NextCursor
	if q.Cursor == "" {
		t.Fatal("expected continuation")
	}
	q.Limit, q.Reason = 100, "finish changed output budget"
	if result := retrieveOK(t, r, q); result.More || len(result.Sources) != 1 {
		t.Fatalf("limit and reason may change: %+v", result)
	}
	for _, mutate := range []func(*RetrievalQuery){
		func(q *RetrievalQuery) { q.Query = "different" },
		func(q *RetrievalQuery) { q.Operation = "read" },
		func(q *RetrievalQuery) { q.Path += "/a" },
		func(q *RetrievalQuery) { q.Offset = 1 },
		func(q *RetrievalQuery) { q.Cursor = "not base64" },
		func(q *RetrievalQuery) { q.Cursor = strings.Repeat("a", 1025) },
		func(q *RetrievalQuery) {
			cursor, _ := decodeDirectoryCursor(*q)
			cursor.File = "../outside"
			data, _ := json.Marshal(cursor)
			q.Cursor = base64.RawURLEncoding.EncodeToString(data)
		},
		func(q *RetrievalQuery) {
			cursor, _ := decodeDirectoryCursor(*q)
			cursor.Offset = -1
			data, _ := json.Marshal(cursor)
			q.Cursor = base64.RawURLEncoding.EncodeToString(data)
		},
	} {
		bad := q
		mutate(&bad)
		result, err := r.Retrieve(context.Background(), bad)
		if err != nil || result.Error == "" || result.More || result.NextCursor != "" {
			t.Fatalf("invalid cursor accepted: %+v %v", result, err)
		}
	}
	if err := os.Remove(filepath.Join(r.Imports, "a")); err != nil {
		t.Fatal(err)
	}
	result, err := r.Retrieve(context.Background(), q)
	if err != nil || !strings.Contains(result.Error, "no longer available") {
		t.Fatalf("removed cursor source accepted: %+v %v", result, err)
	}
}

func TestRetrievalDirectorySearchReferencesAndBytesBounded(t *testing.T) {
	r := Reader{Imports: t.TempDir()}
	rel := filepath.Join(strings.Repeat("a", 240), strings.Repeat("b", 240), strings.Repeat("c", 240))
	if err := os.MkdirAll(filepath.Join(r.Imports, rel), 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		name := string(rune('a'+i)) + strings.Repeat("x", 200)
		if err := os.WriteFile(filepath.Join(r.Imports, rel, name), []byte("needle"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	q := directoryQuery()
	q.Path += "/" + filepath.ToSlash(rel)
	q.Limit = MaxBytes
	var found int
	for attempts := 0; attempts < 10; attempts++ {
		result := retrieveOK(t, r, q)
		encoded, err := json.Marshal(result.Sources)
		if err != nil || len(encoded) > MaxBytes || len(result.Sources) > 32 {
			t.Fatalf("reference presentation exceeded budget: %d %v", len(encoded), err)
		}
		total := 0
		for _, source := range result.Sources {
			total += source.Length
		}
		if total > q.Limit {
			t.Fatal("raw bytes exceeded budget")
		}
		found += len(result.Sources)
		if !result.More {
			break
		}
		if len(result.Sources) == 0 || q.Cursor == result.NextCursor {
			t.Fatal("reference budget blocked progress")
		}
		q.Cursor = result.NextCursor
	}
	if found != 12 {
		t.Fatalf("reference-limited continuation lost matches: %d", found)
	}
}

func TestRetrievalDirectorySearchSourceCountBounded(t *testing.T) {
	files := make(map[string]string)
	for i := 0; i < 40; i++ {
		files[fmt.Sprintf("%03d", i)] = "needle"
	}
	r := directoryFixture(t, files)
	q := directoryQuery()
	first := retrieveOK(t, r, q)
	if len(first.Sources) != 32 || !first.More {
		t.Fatalf("source count was not bounded: %+v", first)
	}
	q.Cursor = first.NextCursor
	second := retrieveOK(t, r, q)
	if len(second.Sources) != 8 || second.More || second.Sources[0].Path != "/canti/imports/032" {
		t.Fatalf("source count continuation: %+v", second)
	}
}

func TestRetrievalDirectorySearchCancellation(t *testing.T) {
	r := directoryFixture(t, map[string]string{"a": "needle"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Retrieve(ctx, directoryQuery()); err != context.Canceled {
		t.Fatalf("directory search cancellation: %v", err)
	}
}

func TestRetrievalDirectorySearchEmptyFileCountBounded(t *testing.T) {
	files := make(map[string]string)
	for i := 0; i < 300; i++ {
		files[fmt.Sprintf("%03d", i)] = ""
	}
	r := directoryFixture(t, files)
	q := directoryQuery()
	first := retrieveOK(t, r, q)
	if len(first.Sources) != 0 || !first.More {
		t.Fatalf("empty file scan was not bounded: %+v", first)
	}
	q.Cursor = first.NextCursor
	cursor, err := decodeDirectoryCursor(q)
	if err != nil || cursor.File != "256" || cursor.Offset != 0 {
		t.Fatalf("empty file continuation: %+v %v", cursor, err)
	}
	second := retrieveOK(t, r, q)
	if len(second.Sources) != 0 || second.More {
		t.Fatalf("empty file search did not terminate: %+v", second)
	}
}
