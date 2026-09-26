package harness

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkPrompt(b *testing.B) {
	for _, n := range []int{0, 5, 20} {
		b.Run(fmt.Sprintf("steps=%d", n), func(b *testing.B) {
			f := testFrame()
			f.Step, f.MaxSteps, f.HandoverBytes = n+1, 1000, 2048
			f.RetrievalEnabled = true
			for i := range n {
				f.Steps = append(f.Steps, exitedStep(i+1, strings.Repeat("go test ./...; ", 180), strings.Repeat("package: ok\n", 400)))
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
