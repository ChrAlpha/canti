package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func growthFixture(t *testing.T, groups, perGroup int, body, filename string) (*Loop, Frame, []string) {
	t.Helper()
	r := &Reader{Imports: t.TempDir()}
	limits := DefaultPromptLimits()
	limits.Total = 256 << 10
	l := &Loop{cfg: LoopConfig{Reader: r, Limits: limits, WindowSteps: 16}}
	f := budgetFrame()
	f.ImportsEnabled, f.Step = true, groups+1
	var files []string
	for group := 0; group < groups; group++ {
		q := RetrievalQuery{Operation: "search", Path: "/canti/imports", Query: "x", Reason: "recover original evidence"}
		result := &Retrieval{Query: q}
		for item := 0; item < perGroup; item++ {
			name := fmt.Sprintf("source-%03d-%s", len(files), filename)
			file := filepath.Join(r.Imports, name)
			if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			files = append(files, file)
			result.Sources = append(result.Sources, sourceRange("/canti/imports/"+name, 0, []byte(body)))
		}
		encoded, _ := json.Marshal(result.Sources)
		if len(encoded) > RetrievalMaxBytes || perGroup*len(body) > RetrievalMaxBytes {
			t.Fatal("fixture exceeds legal retrieval source budgets")
		}
		f.Steps = append(f.Steps, StepRecord{Step: group + 1, Command: `canti:retrieve {"operation":"search","path":"/canti/imports","query":"x","reason":"recover original evidence"}`,
			Observation: Observation{Execution: "exited", Retrieval: result}})
	}
	return l, f, files
}

func renderedGrowth(t *testing.T, l *Loop, f *Frame) (int, int) {
	t.Helper()
	growth, err := l.expandRetrieval(t.Context(), f)
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.Prompt(l.cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	return len(p.System) + len(p.User()), growth
}

func TestRetrievalGrowthKeepsManyShortSourcesInCurrentWindow(t *testing.T) {
	l, f, files := growthFixture(t, 4, 32, strings.Repeat("x", 95)+"\n", strings.Repeat("p", 40))
	f.Goal = strings.Repeat("g", 12<<10)
	before, growth := renderedGrowth(t, l, &f)
	if before > 56<<10 {
		t.Fatalf("fixture is not a small rendered window: %d bytes", before)
	}
	if err := l.requestHandover(&f, growth); err != nil {
		t.Fatal(err)
	}
	if f.Window.Requested {
		t.Fatalf("premature handover: %d rendered bytes, %d growth bytes for 128 ranges in 4 blocks", before, growth)
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := renderedGrowth(t, l, &f)
	if after > before+growth {
		t.Fatalf("future failure exceeded reserved render: %d > %d + %d", after, before, growth)
	}
	t.Logf("rendered=%d ranges=128 retrieval_blocks=4 future_growth=%d failed_render=%d", before, growth, after)
}

func TestRetrievalGrowthBoundsFailureAndRestoration(t *testing.T) {
	for _, scenario := range []string{"success-to-error", "error-to-original", "mixed", "long-headers", "backtick-root-error", "maximum-source-restoration"} {
		t.Run(scenario, func(t *testing.T) {
			body, filename, count := strings.Repeat("`", 2047)+"\n", "ordinary", 4
			if scenario == "long-headers" {
				filename = strings.Repeat("`", 200) + "\n"
			}
			if scenario == "backtick-root-error" {
				body = "x\n"
			}
			if scenario == "maximum-source-restoration" {
				body, count = strings.Repeat("`", RetrievalMaxBytes), 1
			}
			l, f, files := growthFixture(t, 2, count, body, filename)
			if scenario == "backtick-root-error" {
				old := l.cfg.Reader.Imports
				root := filepath.Join(t.TempDir(), strings.Repeat("`", 240))
				if err := os.Rename(old, root); err != nil {
					t.Fatal(err)
				}
				l.cfg.Reader.Imports = root
				before, growth := renderedGrowth(t, l, &f)
				if err := os.Rename(root, root+"-missing"); err != nil {
					t.Fatal(err)
				}
				after, _ := renderedGrowth(t, l, &f)
				if after > before+growth {
					t.Fatalf("backtick error exceeded bound: %d > %d + %d", after, before, growth)
				}
				return
			}
			for i, file := range files {
				if scenario == "error-to-original" || scenario == "mixed" && i%2 == 0 || scenario == "long-headers" || scenario == "maximum-source-restoration" {
					if err := os.Remove(file); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, growth := renderedGrowth(t, l, &f)
			for i, file := range files {
				if scenario == "error-to-original" || scenario == "mixed" && i%2 == 0 || scenario == "long-headers" || scenario == "maximum-source-restoration" {
					if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			}
			after, _ := renderedGrowth(t, l, &f)
			if after > before+growth {
				t.Fatalf("future render exceeded bound: %d > %d + %d", after, before, growth)
			}
		})
	}
}
