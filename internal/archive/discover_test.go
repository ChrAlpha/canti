package archive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

func discoveryFixture(t *testing.T) Reader {
	t.Helper()
	r := Reader{History: t.TempDir(), Artifacts: t.TempDir(), Excluded: []string{"canti:retrieve ", "canti:handover ", "# canti:discovery"}}
	if err := os.MkdirAll(filepath.Join(r.History, "run", "steps"), 0o700); err != nil {
		t.Fatal(err)
	}
	return r
}

func discoveryStep(t *testing.T, r Reader, step int, command, preview, output string, complete bool) string {
	t.Helper()
	dir := filepath.Join(r.History, "run", "steps", StepDir(step))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	artifact := ""
	if output != "" {
		name := fmt.Sprintf("step-%d", step)
		if err := os.WriteFile(filepath.Join(r.Artifacts, name), []byte(output), 0o600); err != nil {
			t.Fatal(err)
		}
		artifact = "/canti/artifacts/" + name
	}
	exit := 0
	meta := StepMeta{Step: step, Execution: "exited", ExitCode: &exit, Output: artifact, OutputBytes: int64(len(output)), OutputComplete: complete}.Encode()
	for name, body := range map[string]string{CommandFile: command, PreviewFile: preview, MetaFile: meta} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return meta
}

func discoveryQuery() RetrievalQuery {
	return RetrievalQuery{Operation: "discover", Path: "/canti/runs/run/steps", Query: "needle", Reason: "recover original evidence"}
}

func discoveryTexts(t *testing.T, r Reader, result Retrieval) map[string]string {
	t.Helper()
	texts := map[string]string{}
	for _, source := range result.Sources {
		body, err := r.expand(t.Context(), source)
		if err != nil {
			t.Fatal(err)
		}
		texts[source.Path] += body
	}
	return texts
}

func TestDiscoveryOriginalSourcesAndEchoExclusion(t *testing.T) {
	r := discoveryFixture(t)
	discoveryStep(t, r, 1, "run original job", "needle preview duplicate", "needle original approval", true)
	discoveryStep(t, r, 2, `canti:retrieve {"query":"needle"}`, "needle retrieval echo", "", false)
	discoveryStep(t, r, 3, "canti:handover {}", "needle handover echo", "", false)
	discoveryStep(t, r, 4, "# canti:discovery\nsearch history", "needle shell echo", "needle shell echo", true)
	result := retrieveOK(t, r, discoveryQuery())
	texts := discoveryTexts(t, r, result)
	if result.More || result.Incomplete || len(texts) != 2 || !strings.Contains(texts["/canti/artifacts/step-1"], "original approval") || !strings.Contains(texts["/canti/runs/run/steps/00000001/meta"], "output_complete: true") {
		t.Fatalf("wrong original candidates: %+v %v", result, texts)
	}
}

func TestDiscoveryPartialArtifactAndTrueTailPreview(t *testing.T) {
	r := discoveryFixture(t)
	discoveryStep(t, r, 1, "execute original", "needle true tail beyond retained output", "needle retained head", false)
	discoveryStep(t, r, 2, "execute other", "needle available preview", "", false)
	result := retrieveOK(t, r, discoveryQuery())
	texts := discoveryTexts(t, r, result)
	for path, want := range map[string]string{
		"/canti/runs/run/steps/00000001/preview": "true tail",
		"/canti/artifacts/step-1":                "retained head",
		"/canti/runs/run/steps/00000002/preview": "available preview",
	} {
		if !strings.Contains(texts[path], want) {
			t.Fatalf("missing %q from %q: %v", want, path, texts)
		}
	}
}

func TestDiscoveryLateArtifactAndFoldedUnicode(t *testing.T) {
	r := discoveryFixture(t)
	discoveryStep(t, r, 1, "execute original", "unrelated preview", strings.Repeat("x", 512<<10)+"中文K approval", true)
	q := discoveryQuery()
	q.Query, q.IgnoreCase = "中文k", true
	result := retrieveOK(t, r, q)
	texts := discoveryTexts(t, r, result)
	if result.More || !strings.Contains(texts["/canti/artifacts/step-1"], "中文K approval") {
		t.Fatalf("missed deep folded original: %+v %v", result, texts)
	}
}

func TestDiscoveryDenseRecentMatchesLeaveRoomForOlderEvidence(t *testing.T) {
	r := discoveryFixture(t)
	discoveryStep(t, r, 1, "old work", "unrelated preview", "needle original approval", true)
	discoveryStep(t, r, 2, "needle new command", "needle new preview", strings.Repeat("needle noise ", 50000), false)
	result := retrieveOK(t, r, discoveryQuery())
	texts := discoveryTexts(t, r, result)
	if result.More || !strings.Contains(texts["/canti/artifacts/step-1"], "original approval") {
		t.Fatalf("dense recent matches concealed older evidence: %+v", result)
	}
	for _, path := range []string{"/canti/runs/run/steps/00000002/command", "/canti/runs/run/steps/00000002/preview", "/canti/artifacts/step-2"} {
		if !strings.Contains(texts[path], "needle") {
			t.Fatalf("missing field opportunity %s: %+v", path, result)
		}
		count := 0
		for _, source := range result.Sources {
			if source.Path == path {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("expected one candidate from %s, got %d", path, count)
		}
	}
}

func TestDiscoveryFrozenCursorAndBudgets(t *testing.T) {
	r := discoveryFixture(t)
	meta := discoveryStep(t, r, 1, "work", "needle", "", false)
	discoveryStep(t, r, 2, "work", "needle", "", false)
	q := discoveryQuery()
	q.Limit = len(meta) + len(q.Query)
	first := retrieveOK(t, r, q)
	if !first.More || first.NextCursor == "" {
		t.Fatalf("expected continuation: %+v", first)
	}
	discoveryStep(t, r, 3, "work", "needle new history after snapshot", "", false)
	q.Cursor = first.NextCursor
	second := retrieveOK(t, r, q)
	if second.More {
		t.Fatalf("cursor chased new history: %+v", second)
	}
	for _, result := range []Retrieval{first, second} {
		bytes := 0
		for _, source := range result.Sources {
			bytes += source.Length
			if strings.Contains(source.Path, "00000003") {
				t.Fatal("included history newer than frozen discovery fence")
			}
		}
		refs, _ := json.Marshal(result.Sources)
		if bytes > q.Limit || len(refs) > MaxBytes || len(result.Sources) > 32 {
			t.Fatalf("exceeded source or reference bounds: %d %d %+v", bytes, len(refs), result)
		}
	}
	stale := q
	stale.Query = "changed"
	if got, err := r.Retrieve(t.Context(), stale); err != nil || !strings.HasSuffix(got.Error, "; start a new discover without cursor") {
		t.Fatalf("a cursor of another query does not say how to restart: %+v %v", got, err)
	}
	for _, mutate := range []func(*RetrievalQuery){
		func(q *RetrievalQuery) { q.Query = "changed" },
		func(q *RetrievalQuery) { q.IgnoreCase = true },
		func(q *RetrievalQuery) { q.Offset = 1 },
		func(q *RetrievalQuery) { q.Cursor = "invalid" },
	} {
		changed := q
		mutate(&changed)
		got, err := r.Retrieve(t.Context(), changed)
		if err != nil || got.Error == "" || len(got.Sources) != 0 {
			t.Fatalf("invalid cursor accepted: %+v %v", got, err)
		}
	}
}

func TestDiscoveryScanAndStepBoundaries(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		r := discoveryFixture(t)
		discoveryStep(t, r, 1, "work", "irrelevant", strings.Repeat("x", (8<<20)-2)+"中文needle", true)
		q := discoveryQuery()
		q.Query = "中文needle"
		first := retrieveOK(t, r, q)
		if !first.More || len(first.Sources) != 0 {
			t.Fatalf("scan exceeded bound: %+v", first)
		}
		q.Cursor = first.NextCursor
		second := retrieveOK(t, r, q)
		if second.More || !strings.Contains(discoveryTexts(t, r, second)["/canti/artifacts/step-1"], q.Query) {
			t.Fatalf("lost scan-boundary match: %+v", second)
		}
	})
	t.Run("steps", func(t *testing.T) {
		r := discoveryFixture(t)
		for i := 1; i <= 257; i++ {
			preview := "irrelevant"
			if i == 1 {
				preview = "needle"
			}
			discoveryStep(t, r, i, "work", preview, "", false)
		}
		q := discoveryQuery()
		first := retrieveOK(t, r, q)
		if !first.More || len(first.Sources) != 0 {
			t.Fatalf("step bound: %+v", first)
		}
		q.Cursor = first.NextCursor
		second := retrieveOK(t, r, q)
		if second.More || len(second.Sources) != 2 {
			t.Fatalf("step continuation: %+v", second)
		}
	})
}

func TestDiscoveryFailuresNeverReturnPartialEvidence(t *testing.T) {
	for _, kind := range []string{"missing", "size", "meta", "outside", "environment", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			r := discoveryFixture(t)
			meta := discoveryStep(t, r, 1, "work", "needle", "needle original", true)
			discoveryStep(t, r, 2, "work", "needle valid newer source", "", false)
			artifact := filepath.Join(r.Artifacts, "step-1")
			metaPath := filepath.Join(r.History, "run", "steps", "00000001", "meta")
			var err error
			switch kind {
			case "missing":
				err = os.Remove(artifact)
			case "size":
				err = os.WriteFile(artifact, []byte("short"), 0o600)
			case "meta":
				err = os.WriteFile(metaPath, []byte(strings.Replace(meta, "output_complete: true", "output_complete: maybe", 1)), 0o600)
			case "outside":
				err = os.WriteFile(metaPath, []byte(strings.Replace(meta, "/canti/artifacts/step-1", "/canti/runs/run/goal", 1)), 0o600)
			case "environment":
				err = os.WriteFile(metaPath, []byte(strings.Replace(meta, "/canti/artifacts/step-1", "/canti/artifacts/.env", 1)), 0o600)
			case "symlink":
				err = os.Remove(artifact)
				if err == nil {
					err = os.Symlink(metaPath, artifact)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.Retrieve(t.Context(), discoveryQuery())
			if err != nil || got.Error == "" || len(got.Sources) != 0 || got.More || got.NextCursor != "" {
				t.Fatalf("partial failure looked successful: %+v %v", got, err)
			}
		})
	}
}

func TestDiscoverySkipsNonTextSourcesAcrossPages(t *testing.T) {
	for _, bad := range []string{"needle\x00noise", "needle\xffnoise"} {
		t.Run(fmt.Sprintf("%x", bad[len("needle")]), func(t *testing.T) {
			r := discoveryFixture(t)
			discoveryStep(t, r, 1, "work", "needle older proof", "", false)
			for i := 2; i <= 258; i++ {
				preview := "unrelated"
				if i == 258 {
					preview = "needle recent proof"
				}
				discoveryStep(t, r, i, "work", preview, "", false)
			}
			path := filepath.Join(r.History, "run", "steps", "00000002", "preview")
			if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(r.History, "run", "steps", "00000257", "preview"), []byte(bad), 0o600); err != nil {
				t.Fatal(err)
			}
			q := discoveryQuery()
			first := retrieveOK(t, r, q)
			if !first.More || first.SkippedCount != 1 || len(first.Skipped) != 1 || first.Skipped[0].Step != 257 ||
				!strings.Contains(discoveryTexts(t, r, first)["/canti/runs/run/steps/00000258/preview"], "recent proof") {
				t.Fatalf("lost recent source: %+v", first)
			}
			q.Cursor = first.NextCursor
			last := retrieveOK(t, r, q)
			if last.More || !last.Incomplete || last.SkippedCount != 2 || len(last.Skipped) != 2 ||
				last.Skipped[0].Step != 257 || last.Skipped[1].Step != 2 || last.Skipped[1].Field != "preview" ||
				!strings.Contains(discoveryTexts(t, r, last)["/canti/runs/run/steps/00000001/preview"], "older proof") {
				t.Fatalf("lost valid source or gap: %+v", last)
			}
			encoded, err := json.Marshal(last)
			if err != nil {
				t.Fatal(err)
			}
			var persisted Retrieval
			if err := json.Unmarshal(encoded, &persisted); err != nil || persisted.SkippedCount != 2 || len(persisted.Skipped) != 2 {
				t.Fatalf("lost durable gap: %+v %v", persisted, err)
			}
		})
	}
}

func TestDiscoverySkipIdentifiesArtifactNamedPreview(t *testing.T) {
	r := discoveryFixture(t)
	meta := discoveryStep(t, r, 1, "work", "needle valid preview", "needle\x00artifact", false)
	if err := os.Rename(filepath.Join(r.Artifacts, "step-1"), filepath.Join(r.Artifacts, "preview")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.History, "run", "steps", "00000001", "meta"), []byte(strings.Replace(meta, "/canti/artifacts/step-1", "/canti/artifacts/preview", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	result := retrieveOK(t, r, discoveryQuery())
	if result.More || !result.Incomplete || result.SkippedCount != 1 || len(result.Skipped) != 1 || result.Skipped[0].Field != "artifact" ||
		!strings.Contains(discoveryTexts(t, r, result)["/canti/runs/run/steps/00000001/preview"], "valid preview") {
		t.Fatalf("wrong source identity or lost valid preview: %+v", result)
	}
}

func TestDiscoveryThreeTextFieldsSkippedAcrossPage(t *testing.T) {
	r := discoveryFixture(t)
	for i := 1; i <= 257; i++ {
		discoveryStep(t, r, i, "work", "unrelated", "", false)
	}
	discoveryStep(t, r, 257, "needle\x00command", "needle\x00preview", "needle\x00artifact", false)
	q := discoveryQuery()
	first := retrieveOK(t, r, q)
	if !first.More || !first.Incomplete || first.SkippedCount != 3 || len(first.Skipped) != 2 || first.Skipped[0].Field != "command" || first.Skipped[1].Field != "artifact" {
		t.Fatalf("wrong first-page gaps: %+v", first)
	}
	q.Cursor = first.NextCursor
	last := retrieveOK(t, r, q)
	if last.More || !last.Incomplete || last.SkippedCount != 3 || len(last.Skipped) != 2 {
		t.Fatalf("lost gaps across page: %+v", last)
	}
}

func TestDiscoveryFatalMetadataAfterSkippedText(t *testing.T) {
	r := discoveryFixture(t)
	meta := discoveryStep(t, r, 1, "work", "needle older", "", false)
	discoveryStep(t, r, 2, "work", "needle recent", "", false)
	if err := os.WriteFile(filepath.Join(r.History, "run", "steps", "00000002", "preview"), []byte("needle\x00bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.History, "run", "steps", "00000001", "meta"), []byte(strings.Replace(meta, "output_complete: false", "output_complete: maybe", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := r.Retrieve(t.Context(), discoveryQuery())
	if err != nil || !strings.Contains(got.Error, "invalid original step metadata") || len(got.Sources) != 0 || got.More || got.NextCursor != "" || got.SkippedCount != 0 || len(got.Skipped) != 0 {
		t.Fatalf("fatal error lost: %+v %v", got, err)
	}
}

func TestDiscoveryScopeAndCancellation(t *testing.T) {
	r := discoveryFixture(t)
	discoveryStep(t, r, 1, "work", "needle", "", false)
	for _, target := range []string{"/canti/runs", "/canti/runs/run", "/canti/runs/run/steps/00000001", "/canti/artifacts", "/canti/runs/run/steps/../steps"} {
		q := discoveryQuery()
		q.Path = target
		got, err := r.Retrieve(t.Context(), q)
		if err != nil || got.Error == "" {
			t.Fatalf("invalid scope accepted: %s %+v %v", target, got, err)
		}
	}
	q := discoveryQuery()
	q.Limit = len(q.Query)
	got, err := r.Retrieve(t.Context(), q)
	if err != nil || got.Error == "" || got.More {
		t.Fatalf("impossible metadata budget: %+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Retrieve(ctx, discoveryQuery()); err != context.Canceled {
		t.Fatalf("cancel: %v", err)
	}
}

func TestDiscoveryRejectsMalformedCursorAndMissingSteps(t *testing.T) {
	r := discoveryFixture(t)
	meta := discoveryStep(t, r, 1, "work", "needle", "", false)
	discoveryStep(t, r, 2, "work", "needle", "", false)
	q := discoveryQuery()
	q.Limit = len(meta) + len(q.Query)
	first := retrieveOK(t, r, q)
	data, err := base64.RawURLEncoding.DecodeString(first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	var original map[string]any
	if err := json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]any{"binding": nil, "step": 0, "through": 3, "field": 3, "offset": -1, "unknown": true, "skipped_count": 7} {
		changed := make(map[string]any)
		for k, v := range original {
			changed[k] = v
		}
		changed[key] = value
		encoded, _ := json.Marshal(changed)
		q.Cursor = base64.RawURLEncoding.EncodeToString(encoded)
		got, err := r.Retrieve(t.Context(), q)
		if err != nil || got.Error == "" || got.More {
			t.Fatalf("malformed cursor accepted: %s %+v %v", key, got, err)
		}
	}
	gone := make(map[string]any)
	for k, v := range original {
		gone[k] = v
	}
	gone["step"], gone["field"], gone["offset"] = 1, 2, 0
	encoded, _ := json.Marshal(gone)
	q.Cursor = base64.RawURLEncoding.EncodeToString(encoded)
	if got, err := r.Retrieve(t.Context(), q); err != nil || !strings.HasSuffix(got.Error, "discovery cursor field is no longer available; start a new discover without cursor") {
		t.Fatalf("a cursor past the fields of its step does not say how to restart: %+v %v", got, err)
	}
	for label, mutate := range map[string]func(map[string]any){
		"false_incomplete": func(c map[string]any) {
			c["step"], c["field"], c["skipped_count"] = 1, 0, 1
			c["skipped"] = []Skip{{Step: 2, Field: "preview", Reason: "nul"}}
			c["incomplete"] = false
		},
		"missing_sample": func(c map[string]any) {
			c["step"], c["field"], c["skipped_count"] = 1, 0, 2
			c["skipped"] = []Skip{{Step: 2, Field: "preview", Reason: "nul"}}
			c["incomplete"] = true
		},
	} {
		changed := make(map[string]any)
		for k, v := range original {
			changed[k] = v
		}
		mutate(changed)
		encoded, _ := json.Marshal(changed)
		q.Cursor = base64.RawURLEncoding.EncodeToString(encoded)
		got, err := r.Retrieve(t.Context(), q)
		if err != nil || got.Error == "" || got.More {
			t.Fatalf("malformed cursor accepted: %s %+v %v", label, got, err)
		}
	}
	q.Cursor = ""
	last := filepath.Join(r.History, "run", "steps", "00000002")
	if err := os.RemoveAll(last); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(r.History, "run", "steps", "00000001"), last); err != nil {
		t.Fatal(err)
	}
	got, err := r.Retrieve(t.Context(), q)
	if err != nil || got.Error == "" || got.More || len(got.Sources) != 0 {
		t.Fatalf("symlink step silently omitted: %+v %v", got, err)
	}
	if err := os.Remove(last); err != nil {
		t.Fatal(err)
	}
	discoveryStep(t, r, 3, "work", "needle", "", false)
	got, err = r.Retrieve(t.Context(), q)
	if err != nil || got.Error == "" || got.More || len(got.Sources) != 0 {
		t.Fatalf("missing step silently omitted: %+v %v", got, err)
	}
}

func TestDiscoveryNoMatchRetainsIncompleteCoverageAcrossPages(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(unavailable), func(t *testing.T) {
			r := discoveryFixture(t)
			for step := 1; step <= 257; step++ {
				output, complete := "retained unrelated output", true
				if step == 257 {
					complete = false
					if unavailable {
						output = ""
					}
				}
				discoveryStep(t, r, step, "work", "unrelated preview", output, complete)
			}
			q := discoveryQuery()
			first := retrieveOK(t, r, q)
			if !first.More || !first.Incomplete || len(first.Sources) != 0 {
				t.Fatalf("no-hit coverage hidden: %+v", first)
			}
			q.Cursor = first.NextCursor
			last := retrieveOK(t, r, q)
			if last.More || !last.Incomplete || len(last.Sources) != 0 {
				t.Fatalf("terminal cursor lost earlier output gap: %+v", last)
			}
		})
	}
}

// TestDiscoveryExcludesAfterLeadingWhitespaceWithinItsBound requires that
// discovery skips a step whose command begins with an exclusion after
// leading spaces, tabs, line feeds, or carriage returns, reading past the
// first 64 bytes only to decide one that they leave open; that it skips a
// command whose leading whitespace exceeds MaxBytes as unclassified,
// reporting the scan incomplete; and that it scans a command that only
// begins like an exclusion.
func TestDiscoveryExcludesAfterLeadingWhitespaceWithinItsBound(t *testing.T) {
	r := discoveryFixture(t)
	r.Excluded = []string{"canti:", "# canti:discovery"}
	commands := map[int]string{
		1: "run original job",
		2: "\t\ncanti:retrieve {}",
		3: strings.Repeat(" ", 100) + "canti:handover Next.",
		4: strings.Repeat(" ", MaxBytes) + "canti:retrieve {}",
		5: "  # canti:discovery\nfind /canti/runs",
		6: strings.Repeat(" ", 60) + "cantiX run",
		7: "\r\ncanti:",
		8: "#canti:discovery",
	}
	for step := 1; step <= len(commands); step++ {
		discoveryStep(t, r, step, commands[step], fmt.Sprintf("needle %d", step), "", false)
	}
	result := retrieveOK(t, r, discoveryQuery())
	var found []int
	for step := range len(commands) + 1 {
		if strings.Contains(strings.Join(slices.Collect(maps.Values(discoveryTexts(t, r, result))), "\n"), fmt.Sprintf("needle %d", step)) {
			found = append(found, step)
		}
	}
	if result.More || !slices.Equal(found, []int{1, 6, 8}) {
		t.Fatalf("discovery scanned steps %v, want 1, 6, and 8: %+v", found, result)
	}
	if !result.Incomplete || result.SkippedCount != 1 || !slices.Equal(result.Skipped, []Skip{{Step: 4, Field: CommandFile, Reason: SkipUnclassified}}) {
		t.Fatalf("the unclassified step 4 is not reported: %+v", result)
	}
}

// TestDiscoveryChargesEveryByteItReadsOfACommand requires that deciding a
// command charges the first read and the whole second read to the scan
// budget, and that an unclassified command is charged and skipped.
func TestDiscoveryChargesEveryByteItReadsOfACommand(t *testing.T) {
	r := discoveryFixture(t)
	r.Excluded = []string{"canti:"}
	for _, tc := range []struct {
		command         string
		charge          int
		excluded, skips bool
	}{
		{"run original job", len("run original job"), false, false},
		{strings.Repeat(" ", 100) + "canti:x", 64 + 107, true, false},
		{strings.Repeat(" ", 100) + "ls", 64 + 102, false, false},
		{strings.Repeat(" ", MaxBytes+1) + "canti:x", 64 + MaxBytes, true, true},
	} {
		discoveryStep(t, r, 1, tc.command, "preview", "", false)
		d := discovery{r: r, q: discoveryQuery(), cursor: discoveryCursor{Step: 1, Through: 1}, result: &Retrieval{}, remaining: DiscoveryScanBytes}
		excluded, stop, err := d.excluded(t.Context(), filepath.Join(RunsRoot, "run", "steps", StepDir(1), CommandFile))
		if err != nil || stop || excluded != tc.excluded || DiscoveryScanBytes-d.remaining != tc.charge || (d.result.SkippedCount == 1) != tc.skips {
			t.Errorf("%d-byte command: excluded %v, stop %v, charged %d of %d, skipped %+v, %v",
				len(tc.command), excluded, stop, DiscoveryScanBytes-d.remaining, tc.charge, d.result.Skipped, err)
		}
	}
}

// TestDiscoveryStopsBeforeAnUndecidedCommandItCannotPayFor requires that a
// discovery whose scan budget cannot pay for deciding a command with long
// leading whitespace stops before that step, reporting more, and that its
// continuation, with a fresh budget, decides and skips it.
func TestDiscoveryStopsBeforeAnUndecidedCommandItCannotPayFor(t *testing.T) {
	r := discoveryFixture(t)
	r.Excluded = []string{"canti:"}
	discoveryStep(t, r, 1, "run original job", "needle 1", "", false)
	discoveryStep(t, r, 2, strings.Repeat(" ", 100)+"canti:retrieve {}", "needle 2", "", false)
	meta := discoveryStep(t, r, 3, "fill", "x", "", false)
	q := discoveryQuery()
	reserve := MaxBytes + 65 + queryMatchBytes(q) + utf8.UTFMax
	fill := DiscoveryScanBytes - len("fill") - len(meta) - (reserve + MaxBytes/2)
	discoveryStep(t, r, 3, "fill", strings.Repeat("x", fill), "", false)
	first := retrieveOK(t, r, q)
	if !first.More || len(first.Sources) != 0 {
		t.Fatalf("the first discovery did not stop before step 2: %+v", first)
	}
	q.Cursor = first.NextCursor
	next := retrieveOK(t, r, q)
	texts := strings.Join(slices.Collect(maps.Values(discoveryTexts(t, r, next))), "\n")
	if next.More || strings.Contains(texts, "needle 2") || !strings.Contains(texts, "needle 1") {
		t.Fatalf("the continuation did not skip step 2 and scan step 1: %+v", next)
	}
}
