package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// initialContext is the context state that a run's first request freezes.
var initialContext = harness.InitialContext()

func BenchmarkEncode(b *testing.B) {
	for _, size := range []int{16, 128 << 10} {
		b.Run(fmt.Sprintf("request_%dB", size), func(b *testing.B) {
			state := fresh(b, "benchmark", limits)
			tr := derive(b, harness.Snapshot{State: state}, core.RequestFrozen{}, harness.Facts{
				Request: &harness.Request{ContextState: &initialContext, Body: make([]byte, size), Model: "m", Protocol: "p", Adapter: "a"},
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
					Request: &harness.Request{ContextState: &initialContext, Body: body, Model: "m", Protocol: "p", Adapter: "a"},
				})
				b.StartTimer()
				if err := s.Commit(ctx, tr); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				// Close the run so that admission accepts the next one.
				stop := derive(b, harness.Snapshot{Seq: tr.Seq, State: tr.To}, core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
				if err := s.Commit(ctx, stop); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkCompletion reads, by its sequence, the performed step of a
// retrieval that committed 8 KiB of evidence, under a decision of a typical
// size and of 1 MiB: the point read a carried reference costs at every
// preparation of the window that carries it.
func BenchmarkCompletion(b *testing.B) {
	for _, size := range []int{128, 1 << 20} {
		b.Run(fmt.Sprintf("decision_%dB", size), func(b *testing.B) {
			ctx := context.Background()
			s := open(b, tempPath(b))
			d := newDriver(b, s, createRun(b, s, "run-1", limits))
			d.decide(execBody(fmt.Sprintf(`canti:retrieve {"operation":"read","path":"/canti/imports/x","reason":%q}`, strings.Repeat("r", size))), harness.Usage{})
			evidence := strings.Repeat("e", archive.MaxBytes)
			d.do(core.Performed{EffectID: d.snap.State.EffectID}, harness.Facts{Outcome: &harness.Outcome{Status: harness.OutcomeDone, Retrieval: &archive.Retrieval{
				Query:   archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/x", Reason: "r"},
				Sources: []archive.SourceRange{{Path: "/canti/imports/x", Length: len(evidence), SHA256: strings.Repeat("a", 64)}}, Evidence: []string{evidence}}}})
			seq := d.snap.Seq
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if r, err := s.Completion(ctx, "run-1", seq); err != nil || r.Outcome == nil {
					b.Fatal(r, err)
				}
			}
		})
	}
}

// decisionSizes are the payloads of an exec decision that a snapshot holds
// from its acceptance to its completion: a typical command, a file written
// through a heredoc, and the largest the benchmarks cover.
var decisionSizes = []int{128, 64 << 10, 1 << 20}

// BenchmarkLatestDecision reads the latest snapshot of a run that holds an
// accepted decision of each size, as a commit, a context read, and every
// reader of the run's position do.
func BenchmarkLatestDecision(b *testing.B) {
	for _, size := range decisionSizes {
		b.Run(fmt.Sprintf("decision_%dB", size), func(b *testing.B) {
			ctx := context.Background()
			s := open(b, tempPath(b))
			d := newDriver(b, s, createRun(b, s, "run-1", limits))
			d.decide(execBody("echo "+strings.Repeat("x", size)), harness.Usage{})
			b.ReportAllocs()
			for b.Loop() {
				if _, err := s.Latest(ctx, "run-1"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCommitDecision commits the observation that completes a step
// whose accepted decision has each size: the commit that carries the
// decision in its from state and checks it against the latest snapshot.
func BenchmarkCommitDecision(b *testing.B) {
	for _, size := range decisionSizes {
		b.Run(fmt.Sprintf("decision_%dB", size), func(b *testing.B) {
			s := open(b, tempPath(b))
			command := "echo " + strings.Repeat("x", size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				d := newDriver(b, s, createRun(b, s, fmt.Sprintf("run-%d", i), limits))
				d.decide(execBody(command), harness.Usage{})
				b.StartTimer()
				d.observe(core.ExecutionExited, 1)
				b.StopTimer()
				// Close the run so that admission accepts the next one.
				d.do(core.StopRequested{Reason: core.StopCancelled}, harness.Facts{})
				b.StartTimer()
			}
		})
	}
}
