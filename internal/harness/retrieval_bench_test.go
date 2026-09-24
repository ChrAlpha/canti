package harness

import (
	"strings"
	"testing"
)

func BenchmarkRetrievalSearch(b *testing.B) {
	for _, tc := range []struct{ name, body, query string }{
		{"dense", strings.Repeat("checkpoint ", 24000), "checkpoint"},
		{"sparse", strings.Repeat(strings.Repeat("-", 30000)+"checkpoint ", 8), "checkpoint"},
		{"unicode", strings.Repeat("恢复取消核实证据 ", 12000), "证据"},
	} {
		for _, mode := range []string{"file", "directory-metadata"} {
			b.Run(tc.name+"/"+mode, func(b *testing.B) {
				data := []byte(tc.body)
				q := RetrievalQuery{Operation: "search", Path: "/canti/imports/evidence", Query: tc.query}
				if mode == "directory-metadata" {
					q.Path = "/canti/imports/" + strings.Repeat("archive/", 100) + "evidence"
				}
				b.ReportAllocs()
				var result Retrieval
				for b.Loop() {
					result = Retrieval{Query: q}
					if mode == "directory-metadata" {
						searchRanges(data, RetrievalDefaultBytes, min(len(data), retrievalScanBytes), 32, RetrievalMaxBytes-2, &result)
					} else {
						searchRetrieval(data, RetrievalDefaultBytes, &result)
					}
				}
				bytes := 0
				for _, source := range result.Sources {
					bytes += source.Length
				}
				b.ReportMetric(float64(result.NextOffset), "scanned-offset/op")
				b.ReportMetric(float64(bytes), "evidence-B/op")
				b.ReportMetric(float64(len(result.Sources)), "ranges/op")
			})
		}
	}
}
