package harness

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func BenchmarkPrompt(b *testing.B) {
	for _, n := range []int{0, 5, 20} {
		b.Run(fmt.Sprintf("steps=%d", n), func(b *testing.B) {
			f := testFrame()
			f.Step, f.MaxSteps = n+1, 1000
			f.RetrievalEnabled = true
			for i := range n {
				code := 0
				f.Steps = append(f.Steps, StepRecord{Seq: int64((i + 1) * 5), Step: i + 1, Purpose: strings.Repeat("check ", 180),
					Observation: Observation{Execution: core.ExecutionExited, ExitCode: &code, Preview: strings.Repeat("out ", 64)}})
			}
			l := DefaultPromptLimits()
			l.Total = 256 << 10
			b.ReportAllocs()
			for b.Loop() {
				if _, err := f.Prompt(l); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkClip(b *testing.B) {
	for _, tc := range []struct{ name, text string }{
		{"ascii", strings.Repeat("command output\n", 1000)},
		{"unicode", strings.Repeat("恢复取消核实证据\n", 1000)},
		{"invalid", strings.Repeat("a\xffb\n", 1000)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = clip(tc.text, 2048)
			}
		})
	}
}
