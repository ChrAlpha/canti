package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type qualitySpan struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Length int    `json:"length"`
}

type qualityCase struct {
	ID    string        `json:"id"`
	Split string        `json:"split"`
	Path  string        `json:"path"`
	Query string        `json:"query"`
	Terms string        `json:"terms,omitempty"`
	Gold  []qualitySpan `json:"gold"`
}

type qualityCorpus struct {
	Format string            `json:"format"`
	Root   string            `json:"root,omitempty"`
	Files  map[string]string `json:"files,omitempty"`
	Hashes map[string]string `json:"hashes,omitempty"`
	Cases  []qualityCase     `json:"cases"`
}

type qualityMetrics struct {
	ID                string   `json:"id"`
	Split             string   `json:"split"`
	Budget            int      `json:"budget"`
	CallLimit         int      `json:"call_limit"`
	Calls             int      `json:"calls"`
	ReturnedBytes     int      `json:"returned_bytes"`
	UniqueBytes       int      `json:"unique_bytes"`
	ReferenceBytes    int      `json:"reference_bytes"`
	SourcePrecision   float64  `json:"source_precision"`
	SourceRecall      float64  `json:"source_recall"`
	GoldSpanRecall    float64  `json:"gold_span_recall"`
	GoldBytePrecision float64  `json:"gold_byte_precision"`
	DuplicateRatio    float64  `json:"duplicate_ratio"`
	NoGold            bool     `json:"no_gold"`
	FalsePositive     bool     `json:"false_positive"`
	AllGoldCovered    bool     `json:"all_gold_covered"`
	More              bool     `json:"more"`
	Missing           []string `json:"missing,omitempty"`
	Error             string   `json:"error,omitempty"`
}

type qualityAggregate struct {
	Split             string   `json:"split"`
	Budget            int      `json:"budget"`
	CallLimit         int      `json:"call_limit"`
	Cases             int      `json:"cases"`
	PositiveCases     int      `json:"positive_cases"`
	SourcePrecision   float64  `json:"source_precision_macro"`
	SourceRecall      float64  `json:"source_recall_macro"`
	GoldSpanRecall    float64  `json:"gold_span_recall_macro"`
	GoldBytePrecision float64  `json:"gold_byte_precision_macro"`
	DuplicateRatio    float64  `json:"duplicate_ratio_macro"`
	NegativeCases     int      `json:"negative_cases"`
	FalsePositives    int      `json:"false_positives"`
	FailedCases       []string `json:"failed_cases"`
}

func TestRetrievalQuality(t *testing.T) {
	var c qualityCorpus
	synthetic := os.Getenv("CANTI_RETRIEVAL_QUALITY_CORPUS") == ""
	if filename := os.Getenv("CANTI_RETRIEVAL_QUALITY_CORPUS"); filename != "" {
		var err error
		c, err = readQualityCorpus(filename)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		c = qualitySynthetic()
	}
	if !validQualityCorpusShape(c) {
		t.Fatal("invalid quality corpus")
	}
	encoded, _ := json.Marshal(c)
	corpusHash := digest(encoded)
	r := qualityReader(t, c)
	var rows []qualityMetrics
	for _, tc := range c.Cases {
		for _, budget := range []int{512, 4096} {
			for _, calls := range []int{1, 4} {
				rows = append(rows, qualityRun(t, r, tc, budget, calls, r.Retrieve))
			}
		}
	}
	report := struct {
		Format        string             `json:"format"`
		Revision      string             `json:"revision"`
		CorpusSHA256  string             `json:"corpus_sha256"`
		EvidenceLevel string             `json:"evidence_level"`
		HashVerified  bool               `json:"original_hash_verified"`
		Rows          []qualityMetrics   `json:"rows"`
		Aggregate     []qualityAggregate `json:"aggregate"`
	}{"retrieval-quality-result/1", os.Getenv("CANTI_RETRIEVAL_QUALITY_REVISION"), corpusHash,
		"offline Reader retrieval; source precision labels whole paths, gold-byte precision labels exact spans; no provider or rendered request measurement", true, rows, qualitySummarize(rows)}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if filename := os.Getenv("CANTI_RETRIEVAL_QUALITY_OUTPUT"); filename != "" {
		if err := validQualityIOPath(filename); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, append(data, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	compact, _ := json.Marshal(report)
	t.Logf("RETRIEVAL_QUALITY %s", compact)
	for _, row := range rows {
		if row.Error != "" {
			t.Errorf("retrieval error retained in report: %s: %s", row.ID, row.Error)
		}
		if !synthetic || row.Budget != 4096 || row.CallLimit != 4 {
			continue
		}
		switch row.ID {
		case "sparse", "correction", "no-approved-duration", "case-sensitive", "dense-checkpoint-100":
			if !row.AllGoldCovered || row.SourceRecall != 1 {
				t.Errorf("stable synthetic evidence regressed: %+v", row)
			}
		case "absent":
			if row.FalsePositive {
				t.Errorf("stable synthetic absence regressed: %+v", row)
			}
		}
	}
}

func qualityReader(t *testing.T, c qualityCorpus) Reader {
	t.Helper()
	root := c.Root
	if root != "" && len(c.Hashes) == 0 {
		t.Fatal("external corpus requires frozen source hashes")
	}
	if root == "" {
		root = t.TempDir()
		for name, body := range c.Files {
			if err := validQualityPath(name); err != nil {
				t.Fatal(err)
			}
			name = filepath.Join(root, name)
			if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if root == c.Root && len(c.Files) > 0 {
		t.Fatal("external corpus cannot include inline files")
	}
	if len(c.Hashes) > 0 {
		if err := verifyQualityCorpus(root, c.Hashes); err != nil {
			t.Fatal(err)
		}
		if c.Root != "" {
			frozen := t.TempDir()
			if err := copyQualityCorpus(root, frozen, c.Hashes); err != nil {
				t.Fatal(err)
			}
			if err := verifyQualityCorpus(frozen, c.Hashes); err != nil {
				t.Fatal(err)
			}
			root = frozen
		}
	}
	r := Reader{Imports: root}
	for _, tc := range c.Cases {
		if tc.ID == "" || tc.Split == "" || tc.Query == "" {
			t.Fatal("quality case requires id, split, and frozen query")
		}
		for _, gold := range tc.Gold {
			if gold.Length <= 0 || gold.Offset < 0 {
				t.Fatalf("invalid gold span: %+v", gold)
			}
			f, err := r.open(gold.Path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := f.Stat()
			f.Close()
			if err != nil || gold.Offset+int64(gold.Length) > info.Size() {
				t.Fatalf("gold span outside source: %+v", gold)
			}
		}
	}
	return r
}

func qualityRun(t *testing.T, r Reader, tc qualityCase, budget, calls int, retrieve func(context.Context, RetrievalQuery) (Retrieval, error)) qualityMetrics {
	t.Helper()
	q := RetrievalQuery{Operation: "search", Path: tc.Path, Query: tc.Query, Reason: "frozen quality evaluation", Limit: budget}
	var found []SourceRange
	var actual, referenceBytes int
	var more bool
	var failure string
	for actual < calls {
		result, err := retrieve(context.Background(), q)
		actual++
		if err != nil {
			failure = err.Error()
			break
		}
		if result.Error != "" {
			failure = result.Error
			break
		}
		bytes := 0
		for _, source := range result.Sources {
			text, err := r.expand(context.Background(), source)
			if err != nil || len(text) != source.Length || digest([]byte(text)) != source.SHA256 {
				t.Fatalf("original source expansion/hash: %s: %v", tc.ID, err)
			}
			bytes += source.Length
		}
		if bytes > budget || len(result.Sources) > 32 {
			t.Fatalf("retrieval exceeded budget: %s", tc.ID)
		}
		encoded, _ := json.Marshal(result.Sources)
		referenceBytes += len(encoded)
		found = append(found, result.Sources...)
		more = result.More
		if !more {
			break
		}
		if result.NextCursor != "" {
			if result.NextCursor == q.Cursor {
				t.Fatalf("directory cursor stalled: %s", tc.ID)
			}
			q.Cursor = result.NextCursor
		} else {
			if result.NextOffset <= q.Offset {
				t.Fatalf("file offset stalled: %s", tc.ID)
			}
			q.Offset = result.NextOffset
		}
	}
	m := qualityScore(tc, found)
	m.Budget, m.CallLimit, m.Calls, m.ReferenceBytes = budget, calls, actual, referenceBytes
	m.More, m.Error = more, failure
	return m
}

func qualityScore(tc qualityCase, found []SourceRange) qualityMetrics {
	m := qualityMetrics{ID: tc.ID, Split: tc.Split, NoGold: len(tc.Gold) == 0}
	got := make(map[string][][2]int64)
	gold := make(map[string][][2]int64)
	for _, s := range found {
		got[s.Path] = append(got[s.Path], [2]int64{s.Offset, s.Offset + int64(s.Length)})
		m.ReturnedBytes += s.Length
	}
	for _, s := range tc.Gold {
		gold[s.Path] = append(gold[s.Path], [2]int64{s.Offset, s.Offset + int64(s.Length)})
	}
	covered, relevantBytes, relevantSources := 0, int64(0), 0
	for name, spans := range got {
		got[name] = qualityUnion(spans)
		if len(gold[name]) > 0 {
			relevantSources++
		}
		for _, s := range got[name] {
			m.UniqueBytes += int(s[1] - s[0])
			for _, g := range qualityUnion(gold[name]) {
				relevantBytes += max(int64(0), min(s[1], g[1])-max(s[0], g[0]))
			}
		}
	}
	for _, s := range tc.Gold {
		hit := false
		for _, g := range got[s.Path] {
			if g[0] <= s.Offset && g[1] >= s.Offset+int64(s.Length) {
				hit = true
			}
		}
		if hit {
			covered++
		} else {
			m.Missing = append(m.Missing, fmt.Sprintf("%s:%d+%d", s.Path, s.Offset, s.Length))
		}
	}
	m.SourcePrecision = qualityRatio(relevantSources, len(got))
	m.SourceRecall = qualityRatio(relevantSources, len(gold))
	m.GoldSpanRecall = qualityRatio(covered, len(tc.Gold))
	m.GoldBytePrecision = qualityRatio(int(relevantBytes), m.ReturnedBytes)
	if m.ReturnedBytes > 0 {
		m.DuplicateRatio = 1 - qualityRatio(m.UniqueBytes, m.ReturnedBytes)
	}
	m.FalsePositive = m.NoGold && m.ReturnedBytes > 0
	m.AllGoldCovered = !m.NoGold && covered == len(tc.Gold)
	return m
}

func qualityUnion(spans [][2]int64) [][2]int64 {
	spans = append([][2]int64(nil), spans...)
	sort.Slice(spans, func(i, j int) bool { return spans[i][0] < spans[j][0] })
	var merged [][2]int64
	for _, s := range spans {
		if len(merged) == 0 || s[0] > merged[len(merged)-1][1] {
			merged = append(merged, s)
		} else {
			merged[len(merged)-1][1] = max(merged[len(merged)-1][1], s[1])
		}
	}
	return merged
}

func qualityRatio(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d)
}

func qualitySummarize(rows []qualityMetrics) []qualityAggregate {
	groups := make(map[string]*qualityAggregate)
	for _, r := range rows {
		key := fmt.Sprintf("%s/%d/%d", r.Split, r.Budget, r.CallLimit)
		a := groups[key]
		if a == nil {
			a = &qualityAggregate{Split: r.Split, Budget: r.Budget, CallLimit: r.CallLimit}
			groups[key] = a
		}
		a.Cases++
		a.DuplicateRatio += r.DuplicateRatio
		if r.NoGold {
			a.NegativeCases++
			if r.FalsePositive {
				a.FalsePositives++
			}
		} else {
			a.PositiveCases++
			a.SourcePrecision += r.SourcePrecision
			a.SourceRecall += r.SourceRecall
			a.GoldSpanRecall += r.GoldSpanRecall
			a.GoldBytePrecision += r.GoldBytePrecision
		}
		if r.Error != "" || r.FalsePositive || !r.NoGold && !r.AllGoldCovered {
			a.FailedCases = append(a.FailedCases, r.ID)
		}
	}
	var out []qualityAggregate
	for _, a := range groups {
		if a.PositiveCases > 0 {
			d := float64(a.PositiveCases)
			a.SourcePrecision, a.SourceRecall = a.SourcePrecision/d, a.SourceRecall/d
			a.GoldSpanRecall, a.GoldBytePrecision = a.GoldSpanRecall/d, a.GoldBytePrecision/d
		}
		a.DuplicateRatio /= float64(a.Cases)
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprintf("%s/%04d/%d", out[i].Split, out[i].Budget, out[i].CallLimit) < fmt.Sprintf("%s/%04d/%d", out[j].Split, out[j].Budget, out[j].CallLimit)
	})
	return out
}

func qualitySynthetic() qualityCorpus {
	c := qualityCorpus{Format: "retrieval-quality/1", Files: make(map[string]string)}
	add := func(id, split, query string, bodies []string, answers map[int][]string) {
		tc := qualityCase{ID: id, Split: split, Path: "/canti/imports/" + id, Query: query}
		for i, body := range bodies {
			name := fmt.Sprintf("%s/%03d.txt", id, i)
			c.Files[name] = body
			for _, answer := range answers[i] {
				offset := strings.Index(body, answer)
				if offset < 0 {
					panic("synthetic gold absent")
				}
				tc.Gold = append(tc.Gold, qualitySpan{"/canti/imports/" + name, int64(offset), len(answer)})
			}
		}
		c.Cases = append(c.Cases, tc)
	}
	for i, query := range []string{"checkpoint", "冻结", "resume"} {
		cluster := strings.Repeat(query+" ", 12)
		body := cluster + strings.Repeat("-", 380) + query + " preserves approved state."
		add(fmt.Sprintf("dense-%d", i), "synthetic-dev", query, []string{body}, map[int][]string{0: {"preserves approved state."}})
		add(fmt.Sprintf("dense-files-%d", i), "synthetic-dev", query, []string{cluster, query + " preserves approved state."}, map[int][]string{1: {"preserves approved state."}})
	}
	prefix, answer := strings.Repeat("checkpoint ", 100), "checkpoint preserves approved state."
	add("dense-checkpoint-100", "synthetic-dev", "checkpoint",
		[]string{prefix + strings.Repeat("-", 1516-len(prefix)-len(answer)) + answer}, map[int][]string{0: {"preserves approved state."}})
	add("sparse", "synthetic-control", "freeze", []string{"freeze requests before sending.", strings.Repeat("-", 1600) + "freeze provenance before retrying."}, map[int][]string{0: {"before sending"}, 1: {"before retrying"}})
	add("correction", "synthetic-control", "Orchid timeout", []string{"Orchid timeout originally used 15 seconds.", "Orchid timeout correction: use 30 seconds."}, map[int][]string{0: {"15 seconds"}, 1: {"30 seconds"}})
	add("absent", "synthetic-negative", "unsupported archive mode", []string{"archive lists and reads; no extra modes are documented."}, nil)
	add("hard-negative", "synthetic-negative", "Orchid timeout", []string{"Orchid timeout is the title of a music track."}, nil)
	add("no-approved-duration", "synthetic-control", "Orchid timeout", []string{"Orchid timeout planning discussion; no approved duration was recorded."}, map[int][]string{0: {"no approved duration was recorded."}})
	add("case-sensitive", "synthetic-control", "HTTP", []string{"http lower case is a distinct identifier.", "HTTP deadline: 20 seconds."}, map[int][]string{1: {"20 seconds"}})
	add("second-hit-right-context", "synthetic-adversarial", "checkpoint", []string{"checkpoint" + strings.Repeat("-", 100) + "checkpoint" + strings.Repeat("-", 40) + "approved answer: durable."}, map[int][]string{0: {"durable"}})
	add("scan-boundary", "synthetic-control", "中文", []string{strings.Repeat("-", scanBytes-2) + "中文恢复已验证。"}, map[int][]string{0: {"恢复已验证"}})
	for _, answer := range []int{0, 99} {
		bodies := make([]string, 100)
		for i := range bodies {
			bodies[i] = "checkpoint typography sample " + strings.Repeat("-", 80)
		}
		bodies[answer] = "checkpoint preserves approved state."
		add(fmt.Sprintf("filename-order-%02d", answer), "synthetic-adversarial", "checkpoint", bodies,
			map[int][]string{answer: {"preserves approved state."}})
	}
	return c
}

func TestRetrievalQualityScoring(t *testing.T) {
	tc := qualityCase{ID: "scorer", Gold: []qualitySpan{{"a", 2, 5}, {"b", 0, 3}}}
	m := qualityScore(tc, []SourceRange{{Path: "a", Offset: 0, Length: 5}, {Path: "a", Offset: 4, Length: 4}, {Path: "c", Offset: 0, Length: 2}})
	if m.UniqueBytes != 10 || m.ReturnedBytes != 11 || m.SourcePrecision != .5 || m.SourceRecall != .5 || m.GoldSpanRecall != .5 || m.AllGoldCovered || len(m.Missing) != 1 {
		t.Fatalf("scorer lost overlap, source precision, or required gold: %+v", m)
	}
	if math.Abs(m.GoldBytePrecision-5.0/11) > 1e-12 || math.Abs(m.DuplicateRatio-(1-10.0/11)) > 1e-12 {
		t.Fatalf("scorer counted repeated gold bytes: %+v", m)
	}
	negative := qualityScore(qualityCase{ID: "negative"}, []SourceRange{{Path: "a", Length: 4}})
	if !negative.NoGold || !negative.FalsePositive || negative.AllGoldCovered {
		t.Fatalf("negative case treated as successful recall: %+v", negative)
	}
	for _, tc := range []struct {
		name    string
		spans   []SourceRange
		covered bool
	}{
		{"adjacent", []SourceRange{{Path: "a", Offset: 2, Length: 2}, {Path: "a", Offset: 4, Length: 3}, {Path: "b", Length: 3}}, true},
		{"gap", []SourceRange{{Path: "a", Offset: 2, Length: 2}, {Path: "a", Offset: 5, Length: 2}, {Path: "b", Length: 3}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := qualityScore(qualityCase{Gold: []qualitySpan{{"a", 2, 5}, {"b", 0, 3}}}, tc.spans)
			if m.AllGoldCovered != tc.covered {
				t.Fatalf("multi-source contiguous coverage: %+v", m)
			}
		})
	}
}
