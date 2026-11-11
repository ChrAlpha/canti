//go:build integration

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestLineWorkspaceCorrectionAndOriginalRead(t *testing.T) {
	f, image := recoveryFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "printf async > mode.txt"},
		2: {Kind: core.Return, Payload: "Implemented async. Alternatives: async or sync."},
		3: {Kind: core.Return, Payload: "Corrected to sync and checked the original answer."},
	})
	if r := f.cli(nil, "run", "-input", "first", "-goal", "Implement async", "-image", image); r.code != exitOK {
		t.Fatal(r)
	}
	first := f.status().Run.ID
	correction, _ := json.Marshal(core.Decision{Kind: core.Exec, Payload: "test \"$(cat mode.txt)\" = async && printf sync > mode.txt"})
	read, _ := json.Marshal(core.Decision{Kind: core.Exec, Payload: `canti:retrieve {"operation":"read","path":"/canti/runs/` + first + `/result","reason":"verify original answer","limit":4096}`})
	f.model.answer(1, 2, reply{text: string(correction)})
	f.model.answer(2, 2, reply{text: string(read)})
	if r := f.cli(nil, "run", "-input", "second", "-goal", "Use sync instead; check your previous answer", "-image", image); r.code != exitOK {
		t.Fatal(r)
	}
	second := f.status().Run.ID
	if second == first {
		t.Fatal("new input reused execution identity")
	}
	if b, err := os.ReadFile(filepath.Join(f.workspace, "mode.txt")); err != nil || string(b) != "sync" {
		t.Fatalf("workspace correction: %q %v", b, err)
	}
	if p := f.model.prompts(3)[0]; !strings.Contains(p, "Retrieved original evidence") || !strings.Contains(p, "/"+first+"/result") || !strings.Contains(p, "Implemented async") {
		t.Fatalf("missing original evidence: %s", p)
	}
	var executed []entry
	for _, id := range []string{first, second} {
		for _, e := range f.history("-run", id) {
			if !strings.HasPrefix(e.State.Decision.Payload, "canti:retrieve") {
				executed = append(executed, e)
			}
		}
	}
	checkReleased(t, f, executed)
}
