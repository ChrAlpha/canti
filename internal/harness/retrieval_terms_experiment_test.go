package harness

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRetrievalTermsCorpusExperiment(t *testing.T) {
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
	variants := []string{"anchor-literal", "anchor-fold", "terms-literal", "any-sensitive", "any-fold", "all-sensitive", "all-fold"}
	report := map[string][]qualityMetrics{}
	for _, tc := range c.Cases {
		if tc.Terms == "" {
			continue
		}
		dir, err := r.open(tc.Path)
		if err != nil {
			t.Fatal(err)
		}
		names, err := retrievalDirectory(context.Background(), dir, true)
		dir.Close()
		if err != nil || len(names) > 256 {
			t.Fatalf("experiment exceeds flat directory budget: %v", err)
		}
		found := map[string][]SourceRange{}
		remaining := retrievalScanBytes
		for _, name := range names {
			file := path.Join(tc.Path, name)
			f, err := r.open(file)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(io.LimitReader(f, int64(remaining)+1))
			f.Close()
			if err != nil || len(body) > remaining {
				t.Fatalf("experiment scope exceeds scan budget: %v", err)
			}
			remaining -= len(body)
			for _, variant := range variants {
				spans := experimentTermRanges(string(body), tc.Terms, strings.HasPrefix(variant, "all-"), strings.HasSuffix(variant, "-fold"))
				if variant == "anchor-fold" {
					spans = nil
					for _, match := range regexp.MustCompile("(?i)"+regexp.QuoteMeta(tc.Query)).FindAllIndex(body, -1) {
						spans = append(spans, [2]int{match[0], match[1]})
					}
				}
				if strings.HasSuffix(variant, "literal") {
					query := tc.Terms
					if variant == "anchor-literal" {
						query = tc.Query
					}
					spans = nil
					for offset := 0; offset < len(body); {
						index := strings.Index(string(body[offset:]), query)
						if index < 0 {
							break
						}
						spans = append(spans, [2]int{offset + index, offset + index + len(query)})
						offset += index + len(query)
					}
				}
				for _, span := range spans {
					start, stop := max(0, span[0]-128), min(len(body), span[1]+128)
					for start < span[0] && !utf8.RuneStart(body[start]) {
						start++
					}
					for stop < len(body) && !utf8.RuneStart(body[stop]) {
						stop--
					}
					found[variant] = append(found[variant], sourceRange(file, int64(start), body[start:stop]))
				}
			}
		}
		for _, variant := range variants {
			report[variant] = append(report[variant], qualityScore(tc, found[variant]))
		}
	}
	if len(report) == 0 {
		t.Fatal("no frozen term queries in corpus")
	}
	corpus, _ := json.Marshal(c)
	output := map[string]any{"evidence": "offline candidate generation only; no output budget or ranking; not production or provider evidence", "corpus_sha256": digest(corpus), "rows": report}
	aggregate := map[string][]qualityAggregate{}
	for variant, rows := range report {
		aggregate[variant] = qualitySummarize(rows)
	}
	output["aggregate"] = aggregate
	encoded, _ := json.MarshalIndent(output, "", "  ")
	if filename := os.Getenv("CANTI_RETRIEVAL_TERMS_OUTPUT"); filename != "" {
		if err := validQualityIOPath(filename); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	compact, _ := json.Marshal(aggregate)
	t.Log(string(compact))
}

func experimentTermRanges(body, query string, all, fold bool) [][2]int {
	var patterns []*regexp.Regexp
	seen := map[string]bool{}
	for _, term := range strings.Fields(query) {
		if seen[term] {
			continue
		}
		seen[term] = true
		expression := regexp.QuoteMeta(term)
		if fold {
			expression = "(?i)" + expression
		}
		patterns = append(patterns, regexp.MustCompile(expression))
	}
	var ranges [][2]int
	if len(patterns) == 0 {
		return ranges
	}
	for offset := 0; offset < len(body); {
		span := experimentTermMatch(body[offset:], patterns, all)
		if span == nil {
			break
		}
		ranges = append(ranges, [2]int{offset + span[0], offset + span[1]})
		offset += span[1]
	}
	return ranges
}

func experimentTermMatch(body string, patterns []*regexp.Regexp, all bool) []int {
	next := make([][]int, len(patterns))
	for i, pattern := range patterns {
		next[i] = pattern.FindStringIndex(body)
		if all && next[i] == nil {
			return nil
		}
	}
	for {
		first, last := -1, 0
		for i, span := range next {
			if span == nil {
				continue
			}
			if first == -1 || span[0] < next[first][0] {
				first = i
			}
			last = max(last, span[1])
		}
		if first == -1 {
			return nil
		}
		if !all {
			return next[first]
		}
		if last-next[first][0] <= 256 {
			return []int{next[first][0], last}
		}
		_, width := utf8.DecodeRuneInString(body[next[first][0]:])
		offset := next[first][0] + width
		span := patterns[first].FindStringIndex(body[offset:])
		if span == nil {
			return nil
		}
		next[first] = []int{offset + span[0], offset + span[1]}
	}
}

func TestRetrievalTermsExperiment(t *testing.T) {
	type topic struct {
		query string
		gold  []string
		noise []string
	}
	topics := []topic{
		{"request frozen", []string{"The request remains frozen across retries.", "A frozen provider request is reused on resume."}, []string{"request logging is enabled", "frozen container image", "request " + strings.Repeat("unrelated ", 40) + "frozen"}},
		{"HTTP timeout", []string{"HTTP timeout closes the call.", "The http client has a TIMEOUT of 30 seconds."}, []string{"http routing is healthy", "timeout of the shell job", "HTTP cache is enabled"}},
		{"handover capacity", []string{"The handover has limited capacity.", "Capacity reserved for HANDOVER includes escaping."}, []string{"handover completed yesterday", "capacity of the artifact disk", "window capacity available"}},
		{"恢复 取消", []string{"恢复运行前必须检查取消状态。", "取消状态要在恢复之后核实。"}, []string{"恢复备份成功。", "取消订单后生成报表。", "恢复" + strings.Repeat("无关", 100) + "取消"}},
		{"sha256 original", []string{"SHA256 verifies original byte ranges.", "Original evidence is verified using sha256."}, []string{"sha256 of build binary", "original plan was changed", "CRC verifies evidence"}},
		{"cache hit", []string{"Cache HIT was confirmed by usage counters."}, []string{"The cache missed; no hit was recorded.", "cache hit rate is an unverified guess", "uncached weather warning"}},
		{"limit update", []string{"The update raised the output limit."}, []string{"unlimited automatic updater", "The old limit and unrelated package update are documented together."}},
		{"unsupported archive mode", nil, []string{"archive mode supports text", "unsupported file type", "mode controls the window"}},
	}
	variants := []struct {
		name      string
		all, fold bool
	}{
		{"literal", false, false}, {"any-sensitive", false, false},
		{"any-fold", false, true}, {"all-sensitive", true, false}, {"all-fold", true, true},
	}
	for _, variant := range variants {
		tp, fp, gold := 0, 0, 0
		for _, topic := range topics {
			for _, document := range append(append([]string{}, topic.gold...), topic.noise...) {
				matched := len(experimentTermRanges(document, topic.query, variant.all, variant.fold)) > 0
				if variant.name == "literal" {
					matched = strings.Contains(document, topic.query)
				}
				relevant := false
				for _, g := range topic.gold {
					relevant = relevant || g == document
				}
				if relevant {
					gold++
				}
				if matched && relevant {
					tp++
				} else if matched {
					fp++
				}
			}
		}
		row, _ := json.Marshal(map[string]any{"variant": variant.name, "gold": gold, "tp": tp, "fp": fp, "precision": float64(tp) / float64(max(1, tp+fp)), "recall": float64(tp) / float64(gold), "evidence": "synthetic development topics, no provider"})
		t.Log(string(row))
	}
}
