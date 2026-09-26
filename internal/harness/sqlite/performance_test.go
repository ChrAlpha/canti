package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func BenchmarkEncode(b *testing.B) {
	for _, size := range []int{16, 128 << 10} {
		b.Run(fmt.Sprintf("request_%dB", size), func(b *testing.B) {
			state := fresh(b, "benchmark", limits)
			tr := derive(b, harness.Snapshot{State: state}, core.RequestFrozen{}, harness.Facts{
				Request: &harness.Request{Body: make([]byte, size), Model: "m", Protocol: "p", Adapter: "a"},
			})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := encode(tr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCommitFile(b *testing.B) {
	for _, size := range []int{16, 128 << 10} {
		b.Run(fmt.Sprintf("request_%dB", size), func(b *testing.B) {
			ctx := context.Background()
			s := open(b, tempPath(b))
			body := make([]byte, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				id := fmt.Sprintf("benchmark-%d", i)
				state := fresh(b, id, limits)
				if err := s.CreateRun(ctx, testRun(id), state); err != nil {
					b.Fatal(err)
				}
				tr := derive(b, harness.Snapshot{State: state}, core.RequestFrozen{}, harness.Facts{
					Request: &harness.Request{Body: body, Model: "m", Protocol: "p", Adapter: "a"},
				})
				b.StartTimer()
				if err := s.Commit(ctx, tr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
