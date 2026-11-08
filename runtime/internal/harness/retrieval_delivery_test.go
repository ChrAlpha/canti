package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

func TestListCursorOnlyPassesEntriesShownInPrompt(t *testing.T) {
	root := t.TempDir()
	for i := range 140 {
		name := fmt.Sprintf("%08d", i)
		if i%4 == 0 {
			name += strings.Repeat(`\"`, 50)
		}
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r := archive.Reader{Imports: root}
	for _, limit := range []int{archive.DefaultBytes, archive.MaxBytes} {
		q := archive.RetrievalQuery{Operation: "list", Path: "/canti/imports", Reason: "locate a step", Limit: limit}
		var shown []string
		for {
			result := retrieveOK(t, r, q)
			body, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			f := testFrame()
			f.Step = 2
			f.Steps = []StepRecord{{Step: 1, Command: "canti:retrieve", Observation: Observation{
				Execution: core.ExecutionExited, Preview: string(body), Retrieval: &result,
			}}}
			prompt := mustPrompt(t, f, DefaultPromptLimits())
			_, after, ok := strings.Cut(prompt.User(), "Entries: ")
			if !ok {
				t.Fatal("no entries in prompt")
			}
			line, _, _ := strings.Cut(after, "\n")
			var entries []string
			if err := json.Unmarshal([]byte(line), &entries); err != nil {
				t.Fatalf("entries were not complete JSON: %v", err)
			}
			if !reflect.DeepEqual(entries, result.Entries) || int(result.NextOffset) != len(shown)+len(entries) {
				t.Fatalf("cursor %d passed undisplayed entries: got %d, previous %d, result %d", result.NextOffset, len(entries), len(shown), len(result.Entries))
			}
			encoded, err := json.Marshal(entries)
			if err != nil || len(encoded) > limit {
				t.Fatalf("encoded entries use %d bytes over %d: %v", len(encoded), limit, err)
			}
			if !strings.Contains(prompt.User(), fmt.Sprintf(`"next_offset":%d`, result.NextOffset)) {
				t.Fatal("continuation offset missing from prompt")
			}
			shown = append(shown, entries...)
			if !result.More {
				break
			}
			q.Offset = result.NextOffset
		}
		if len(shown) != 140 {
			t.Fatalf("limit %d displayed %d of 140 paths", limit, len(shown))
		}
	}
}

func TestAcceptedHandoverPreservedWithInnerFence(t *testing.T) {
	handover := "Current work:\n```sh\ngo test ./...\n```\nEvidence: /canti/runs/R/steps/00000041/meta"
	f := testFrame()
	f.Step, f.Earlier = 2, 1
	f.Window = &Window{FirstStep: 2, SourceStep: 1, Handover: handover}
	p := mustPrompt(t, f, DefaultPromptLimits())
	if !strings.Contains(p.User(), "````\n"+handover+"\n````") || strings.Contains(p.User(), "[truncated") {
		t.Fatalf("handover changed in new request:\n%s", p.User())
	}
}

func TestEmptyListEncodingFitsLimit(t *testing.T) {
	r := archive.Reader{Imports: t.TempDir()}
	q := archive.RetrievalQuery{Operation: "list", Path: "/canti/imports", Reason: "check empty directory", Limit: 2}
	result := retrieveOK(t, r, q)
	encoded, err := json.Marshal(result.Entries)
	if err != nil || string(encoded) != "[]" || result.More {
		t.Fatalf("empty list encoding: %s, %+v, %v", encoded, result, err)
	}
	q.Limit = 1
	result, err = r.Retrieve(context.Background(), q)
	if err != nil || result.Error == "" {
		t.Fatalf("accepted a list limit below array framing: %+v, %v", result, err)
	}
}

func retrieveOK(t *testing.T, r archive.Reader, q archive.RetrievalQuery) archive.Retrieval {
	t.Helper()
	result, err := r.Retrieve(t.Context(), q)
	if err != nil || result.Error != "" {
		t.Fatalf("Retrieve(%+v) = %+v, %v", q, result, err)
	}
	return result
}

func TestStepRendersDiscoveryCoverage(t *testing.T) {
	r := archive.Retrieval{Query: archive.RetrievalQuery{Operation: "discover", Path: "/canti/runs/run/steps", Query: "needle", Reason: "recover"},
		Incomplete: true, SkippedCount: 2, Skipped: []archive.Skip{{Step: 257, Field: "artifact", Reason: "nul"}, {Step: 2, Field: "preview", Reason: "utf8"}}}
	rendered := budgetFrame().step(StepRecord{Step: 1, Command: "canti:retrieve {}", Observation: Observation{Retrieval: &r}}, DefaultPromptLimits())
	for _, want := range []string{`"skipped_count":2`, `"field":"preview"`, "candidate ranges", `"incomplete":true`, `"more":false`} {
		if !strings.Contains(rendered, want) {
			t.Errorf("coverage lacks %s:\n%s", want, rendered)
		}
	}
}
