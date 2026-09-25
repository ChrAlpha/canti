package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFoldedLiteralMatchesEqualFold(t *testing.T) {
	random := rand.New(rand.NewSource(20260924))
	alphabet := []rune("aAbBkKKſSσΣςßẞ中 �")
	for sample := 0; sample < 2000; sample++ {
		body := make([]rune, 40)
		for i := range body {
			body[i] = alphabet[random.Intn(len(alphabet))]
		}
		query := make([]rune, 1+random.Intn(6))
		for i := range query {
			query[i] = alphabet[random.Intn(len(alphabet))]
		}
		if sample%2 == 0 {
			start := random.Intn(len(body) - len(query) + 1)
			copy(query, body[start:start+len(query)])
		}
		wantStart, wantLength := -1, 0
		for i := 0; i+len(query) <= len(body); i++ {
			if strings.EqualFold(string(body[i:i+len(query)]), string(query)) {
				wantStart = len(string(body[:i]))
				wantLength = len(string(body[i : i+len(query)]))
				break
			}
		}
		start, length := newFoldedLiteral(string(query)).index([]byte(string(body)))
		if start != wantStart || length != wantLength {
			t.Fatalf("sample %d: %q in %q: got [%d,%d], want [%d,%d]", sample, string(query), string(body), start, length, wantStart, wantLength)
		}
		if length > queryMatchBytes(RetrievalQuery{Query: string(query), IgnoreCase: true}) {
			t.Fatal("folded match exceeded preflight bound")
		}
	}
}

func BenchmarkRetrievalCasefold(b *testing.B) {
	for _, tc := range []struct {
		name, body, query string
		hits              int
	}{
		{"sparse", strings.Repeat("-", 240000) + "HTTP deadline is 30 seconds.", "HTTP", 1},
		{"near-miss-512", strings.Repeat("A", 240000), strings.Repeat("a", 511) + "b", 0},
		{"near-miss-8192", strings.Repeat("A", 240000), strings.Repeat("a", 8191) + "b", 0},
	} {
		root := b.TempDir()
		if err := os.WriteFile(filepath.Join(root, "evidence"), []byte(tc.body), 0o600); err != nil {
			b.Fatal(err)
		}
		r := Reader{Imports: root}
		for _, fold := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/ignore_case=%t", tc.name, fold), func(b *testing.B) {
				q := RetrievalQuery{Operation: "search", Path: "/canti/imports/evidence", Query: tc.query, Reason: "benchmark", IgnoreCase: fold, Limit: RetrievalMaxBytes}
				b.ReportAllocs()
				for b.Loop() {
					result, err := r.Retrieve(context.Background(), q)
					if err != nil || result.Error != "" || len(result.Sources) != tc.hits {
						b.Fatalf("unexpected retrieval: %+v %v", result, err)
					}
				}
			})
		}
	}
}

func TestRetrievalCasefoldQuality(t *testing.T) {
	filename := os.Getenv("CANTI_RETRIEVAL_QUALITY_CORPUS")
	if filename == "" {
		t.Skip("external frozen corpus was not supplied")
	}
	c, err := readQualityCorpus(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !validQualityCorpusShape(c) {
		t.Fatal("invalid quality corpus")
	}
	r := qualityReader(t, c)
	var rows []qualityMetrics
	for _, tc := range c.Cases {
		for _, budget := range []int{512, 4096} {
			for _, calls := range []int{1, 4} {
				rows = append(rows, qualityRun(t, r, tc, budget, calls, func(ctx context.Context, q RetrievalQuery) (Retrieval, error) {
					q.IgnoreCase = true
					return r.Retrieve(ctx, q)
				}))
			}
		}
	}
	encoded, _ := json.Marshal(c)
	report := map[string]any{"format": "retrieval-casefold-quality/1", "corpus_sha256": digest(encoded), "rows": rows, "aggregate": qualitySummarize(rows), "ignore_case": true, "evidence_level": "offline Reader; forced case folding includes identifier negatives; no automatic mode selection or provider"}
	output, _ := json.MarshalIndent(report, "", "  ")
	if filename := os.Getenv("CANTI_RETRIEVAL_QUALITY_OUTPUT"); filename != "" {
		if err := validQualityIOPath(filename); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, output, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range rows {
		if row.Error != "" {
			t.Errorf("%s: %s", row.ID, row.Error)
		}
	}
}

func casefoldQuery(t *testing.T, query string, limit int) RetrievalQuery {
	t.Helper()
	var q RetrievalQuery
	err := decodeAction(fmt.Sprintf(`{"operation":"search","path":"/canti/imports/evidence","query":%q,"reason":"locate text with uncertain capitalization","ignore_case":true,"limit":%d}`, query, limit), &q)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestRetrievalCasefoldOriginalBytesAndContinuation(t *testing.T) {
	for _, directory := range []bool{false, true} {
		r, _ := retrievalFixture(t, "K K k")
		q := casefoldQuery(t, "k", 3)
		if directory {
			q.Path = "/canti/imports"
		}
		var found []SourceRange
		for calls := 0; ; calls++ {
			if calls == 10 {
				t.Fatal("case-fold continuation did not terminate")
			}
			result := retrieveOK(t, r, q)
			bytes := 0
			for _, s := range result.Sources {
				text, err := r.Expand(context.Background(), s)
				if err != nil || digest([]byte(text)) != s.SHA256 {
					t.Fatalf("changed original evidence: %q %v", text, err)
				}
				bytes += len(text)
			}
			if bytes > 3 {
				t.Fatal("folded character exceeded original-byte limit")
			}
			found = append(found, result.Sources...)
			if !result.More {
				break
			}
			if directory {
				if result.NextCursor == q.Cursor {
					t.Fatal("cursor did not advance")
				}
				q.Cursor = result.NextCursor
			} else {
				if result.NextOffset <= q.Offset {
					t.Fatal("offset did not advance")
				}
				q.Offset = result.NextOffset
			}
		}
		for _, span := range []qualitySpan{{"/canti/imports/evidence", 0, 3}, {"/canti/imports/evidence", 4, 1}, {"/canti/imports/evidence", 6, 1}} {
			if !qualityScore(qualityCase{Gold: []qualitySpan{span}}, found).AllGoldCovered {
				t.Fatalf("original match lost: %+v", span)
			}
		}
	}
}

func TestRetrievalCasefoldValidationAndLiteralDefault(t *testing.T) {
	r, name := retrievalFixture(t, "HTTP K Straße")
	q := casefoldQuery(t, "http", 128)
	if result := retrieveOK(t, r, q); len(result.Sources) == 0 {
		t.Fatal("case folding did not match HTTP")
	}
	for _, query := range []string{"http", "k", "STRASSE"} {
		result := retrieveOK(t, r, RetrievalQuery{Operation: "search", Path: name, Query: query, Reason: "literal identity"})
		if len(result.Sources) != 0 {
			t.Fatal("default literal matching changed")
		}
	}
	if result := retrieveOK(t, r, casefoldQuery(t, "STRASSE", 128)); len(result.Sources) != 0 {
		t.Fatal("simple case folding must not perform linguistic normalization")
	}
	for _, limit := range []int{1, 2} {
		result, err := r.Retrieve(context.Background(), casefoldQuery(t, "k", limit))
		if err != nil || result.Error == "" || result.More || len(result.Sources) != 0 {
			t.Fatalf("undersized Unicode fold limit accepted: %+v %v", result, err)
		}
	}
	q.Operation, q.Query = "read", ""
	if result, err := r.Retrieve(context.Background(), q); err != nil || result.Error == "" {
		t.Fatalf("ignore_case accepted outside search: %+v %v", result, err)
	}
}

func TestRetrievalCasefoldDirectoryCursorAndScanBoundary(t *testing.T) {
	r, _ := retrievalFixture(t, strings.Repeat("-", retrievalScanBytes-2)+"K")
	q := casefoldQuery(t, "k", 3)
	q.Path = "/canti/imports"
	first := retrieveOK(t, r, q)
	if !first.More || len(first.Sources) != 0 {
		t.Fatalf("expected scan-boundary continuation: %+v", first)
	}
	q.Cursor = first.NextCursor
	second := retrieveOK(t, r, q)
	if second.More || len(second.Sources) != 1 || second.Sources[0].Offset != retrievalScanBytes-2 || second.Sources[0].Length != 3 {
		t.Fatalf("folded match split by scan boundary: %+v", second)
	}
	var changed RetrievalQuery
	if err := decodeAction(fmt.Sprintf(`{"operation":"search","path":"/canti/imports","query":"k","reason":"literal continuation","cursor":%q}`, q.Cursor), &changed); err != nil {
		t.Fatal(err)
	}
	if result, err := r.Retrieve(context.Background(), changed); err != nil || result.Error == "" {
		t.Fatalf("case-fold cursor accepted by literal search: %+v %v", result, err)
	}
}
