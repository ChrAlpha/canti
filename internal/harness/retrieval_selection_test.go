package harness

import (
	"context"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRetrievalSearchDenseHitsReachTailInOneCall(t *testing.T) {
	body := strings.Repeat("needle ", 215) + "answer=42 needle"
	r, path := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "search", Path: path, Reason: "find final answer", Query: "needle", Limit: RetrievalDefaultBytes}
	result := retrieveOK(t, r, q)
	if result.More || result.NextOffset != int64(len(body)) || len(result.Sources) != 1 {
		t.Fatalf("dense hits stopped before tail: %+v", result)
	}
	content, err := r.Expand(context.Background(), result.Sources[0])
	if err != nil || !strings.Contains(content, "answer=42") {
		t.Fatalf("tail evidence unavailable: %q %v", content, err)
	}
}

func TestRetrievalSearchDenseHitsSmallBudgetContinues(t *testing.T) {
	body := strings.Repeat("needle ", 60) + "answer=42 needle"
	r, path := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "search", Path: path, Reason: "find final answer", Query: "needle", Limit: 160}
	previous := int64(-1)
	found := false
	for page := 0; page < 20; page++ {
		result := retrieveOK(t, r, q)
		if result.NextOffset <= previous || len(result.Sources) != 1 || result.Sources[0].Length > q.Limit {
			t.Fatalf("invalid continuation at page %d: %+v", page, result)
		}
		content, err := r.Expand(context.Background(), result.Sources[0])
		if err != nil {
			t.Fatal(err)
		}
		found = found || strings.Contains(content, "answer=42")
		if !result.More {
			if !found {
				t.Fatal("tail evidence was skipped")
			}
			return
		}
		previous = result.NextOffset
		q.Offset = result.NextOffset
	}
	t.Fatal("dense search did not terminate")
}

func TestSearchRangesMergedReferencesAndSlots(t *testing.T) {
	data := []byte(strings.Repeat("needle ", 30) + strings.Repeat("x", 400) + "needle")
	q := RetrievalQuery{Operation: "search", Path: "/canti/imports/evidence", Reason: "find evidence", Query: "needle", Limit: 500}
	result := Retrieval{Query: q}
	_, remaining := searchRanges(data, q.Limit, len(data), 1, RetrievalMaxBytes-2, &result)
	if len(result.Sources) != 1 || result.NextOffset <= int64(len("needle ")) || result.Sources[0].Length > q.Limit {
		t.Fatalf("overlap consumed source slot or byte budget: %+v", result)
	}
	encoded, err := json.Marshal(result.Sources)
	single, _ := json.Marshal(result.Sources[0])
	if err != nil || len(encoded) > RetrievalMaxBytes || remaining != RetrievalMaxBytes-2-len(single)-1 {
		t.Fatalf("reference accounting drifted: size=%d remaining=%d err=%v", len(encoded), remaining, err)
	}
	withSecond := Retrieval{Query: q}
	searchRanges(data, q.Limit, len(data), 2, RetrievalMaxBytes-2, &withSecond)
	if len(withSecond.Sources) != 2 || withSecond.Sources[1].Offset <= withSecond.Sources[0].Offset+int64(withSecond.Sources[0].Length) {
		t.Fatalf("independent range did not use second slot: %+v", withSecond)
	}
}

func TestSearchRangesKeepsSecondHitRightContext(t *testing.T) {
	body := "needle" + strings.Repeat("x", 124) + "needle" + strings.Repeat("x", 104) + "answer=42"
	r, path := retrievalFixture(t, body)
	q := RetrievalQuery{Operation: "search", Path: path, Reason: "read right context", Query: "needle", Limit: 320}
	result := retrieveOK(t, r, q)
	if len(result.Sources) != 1 || result.More {
		t.Fatalf("second hit was not merged: %+v", result)
	}
	content, err := r.Expand(context.Background(), result.Sources[0])
	if err != nil || !strings.Contains(content, "answer=42") {
		t.Fatalf("second hit lost right context: %q %v", content, err)
	}
}

func TestSearchRangesMergesAdjacentIntervalsWithoutShrinking(t *testing.T) {
	data := []byte("a" + strings.Repeat("x", 256) + "a" + strings.Repeat("x", 128))
	q := RetrievalQuery{Operation: "search", Path: "/canti/imports/evidence", Reason: "find evidence", Query: "a", Limit: 400}
	result := Retrieval{Query: q}
	searchRanges(data, q.Limit, len(data), 1, -1, &result)
	if len(result.Sources) != 1 || result.Sources[0].Length != len(data) || result.NextOffset != int64(len(data)) {
		t.Fatalf("adjacent ranges were split or shrunk: %+v", result)
	}
}

func TestSearchRangesProperties(t *testing.T) {
	random := rand.New(rand.NewSource(20260926))
	tokens := []string{"a", "A", "x", "K", "k", "K", "é", "É", "漢", " "}
	queries := []string{"a", "A", "k", "K", "é", "漢", "xx"}
	paths := []string{"/canti/imports/a", "/canti/imports/<&\"\\é"}
	offsets := []int64{0, 9, 10, 99, 100, 999, 1000, 9999, 10000}
	for sample := 0; sample < 2000; sample++ {
		var body strings.Builder
		for i := 0; i < random.Intn(100)+1; i++ {
			body.WriteString(tokens[random.Intn(len(tokens))])
		}
		data := []byte(body.String())
		q := RetrievalQuery{Operation: "search", Path: paths[random.Intn(len(paths))], Query: queries[random.Intn(len(queries))], IgnoreCase: random.Intn(2) == 0, Offset: offsets[random.Intn(len(offsets))]}
		limit := len(q.Query) + random.Intn(90)
		references := -1
		if random.Intn(2) == 0 {
			references = random.Intn(600)
		}
		end := random.Intn(len(data) + 1)
		for end < len(data) && !utf8.RuneStart(data[end]) {
			end++
		}
		result := Retrieval{Query: q}
		left, remaining := searchRanges(data, limit, end, random.Intn(6)+1, references, &result)
		if result.NextOffset < q.Offset || result.NextOffset > q.Offset+int64(len(data)) {
			t.Fatalf("sample=%d invalid cursor: %+v", sample, result)
		}
		total, spent := 0, 0
		for i, source := range result.Sources {
			if i > 0 && result.Sources[i-1].Offset+int64(result.Sources[i-1].Length) >= source.Offset {
				t.Fatalf("sample=%d ranges overlap or touch: %+v", sample, result.Sources)
			}
			start := int(source.Offset - q.Offset)
			if start < 0 || start+source.Length > len(data) || source != sourceRange(q.Path, source.Offset, data[start:start+source.Length]) {
				t.Fatalf("sample=%d source bytes or hash changed: %+v", sample, source)
			}
			total += source.Length
			encoded, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			spent += len(encoded) + 1
		}
		if left != limit-total || references >= 0 && (remaining != references-spent || spent > references) {
			t.Fatalf("sample=%d budget drift: left=%d total=%d remaining=%d spent=%d", sample, left, total, remaining, spent)
		}
	}
}
