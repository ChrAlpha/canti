package jobrunner

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

const previewArtifact = "/canti/artifacts/effect_test/output"

func previewOf(t *testing.T, data string, limit int, stored int64) string {
	t.Helper()
	buffer := NewPreviewBuffer(limit)
	for i := 0; i < len(data); {
		n := min(len(data)-i, 7)
		buffer.Observe([]byte(data[i : i+n]))
		i += n
	}
	return buffer.String(previewOptions(data, stored))
}

func previewOptions(data string, stored int64) PreviewOptions {
	reader := strings.NewReader(data)
	pageEnd := func(start, max int64) int64 {
		if end, err := pageEndFromReaderAt(reader, int64(len(data)), start, max, MaxPreviewBytes); err == nil {
			return end
		}
		return max
	}
	if stored < 0 {
		stored = 0
	}
	if stored > int64(len(data)) {
		stored = int64(len(data))
	}
	prefix := data[:stored]
	lines := int64(0)
	if len(prefix) > 0 {
		lines = int64(PreviewLines(prefix))
	}
	limit := int64(0)
	if stored < int64(len(data)) {
		limit = stored
	}
	return PreviewOptions{Artifact: previewArtifact, Stored: stored, ArtifactLimit: limit, StoredLines: lines, PreviewLimit: MaxPreviewBytes, PageEnd: pageEnd}
}

func TestPreviewShowsOutputWithinBothLimitsWhole(t *testing.T) {
	for name, data := range map[string]string{
		"empty":      "",
		"one line":   "hello\n",
		"no newline": "hello",
		"many lines": strings.Repeat("alpha\n", MaxPreviewLines-1),
		"near bytes": strings.Repeat("x", MaxPreviewBytes),
	} {
		t.Run(name, func(t *testing.T) {
			if got := previewOf(t, data, MaxPreviewBytes, int64(len(data))); got != data {
				t.Fatalf("preview of %d bytes changed: %d bytes, marker %q", len(data), len(got), got[len(got)-min(80, len(got)):])
			}
		})
	}
}

func TestPreviewCutsLongOutputOnLineBoundariesWithExactRanges(t *testing.T) {
	lines := make([]string, 3000)
	for i := range lines {
		lines[i] = "line-" + strings.Repeat("0", 4) + string(rune('a'+i%26))
	}
	data := strings.Join(lines, "\n") + "\n"
	got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	if len(got) > MaxPreviewBytes || PreviewLines(got) > MaxPreviewLines {
		t.Fatalf("preview = %d bytes, %d lines", len(got), PreviewLines(got))
	}
	head := strings.Join(lines[:999], "\n") + "\n"
	if !strings.HasPrefix(got, head) {
		t.Fatalf("preview does not start with lines 1-999")
	}
	tail := strings.Join(lines[2000:], "\n")
	if !strings.HasSuffix(got, tail+"\n") {
		t.Fatalf("preview does not end with lines 2001-3000")
	}
	marker := got[len(head) : len(got)-len(tail)-2]
	for _, want := range []string{
		"lines 1-999 and 2001-3000 of 3000",
		"omitted bytes ",
		" (lines 1000-2000)",
		"3000 lines and " + strconv.Itoa(len(data)) + " bytes observed",
		"artifact complete;",
		"page omitted bytes ",
	} {
		if !strings.Contains(marker, want) {
			t.Errorf("marker does not hold %q: %q", want, marker)
		}
	}
	if !regexp.MustCompile(`page omitted bytes [0-9]+-[0-9]+: dd if=` + regexp.QuoteMeta(previewArtifact)).MatchString(marker) {
		t.Errorf("marker has no bounded page: %q", marker)
	}
}

func TestPreviewLineLimitAndByteLimitTriggerIndependently(t *testing.T) {
	t.Run("lines", func(t *testing.T) {
		data := strings.Repeat("a\n", 2500)
		got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
		if len(got) > MaxPreviewBytes || PreviewLines(got) > MaxPreviewLines {
			t.Fatalf("preview = %d bytes, %d lines", len(got), PreviewLines(got))
		}
		for _, want := range []string{"lines 1-999 and 1501-2500 of 2500", "omitted bytes 1999-3000 (lines 1000-1500)", "2500 lines and 5000 bytes observed"} {
			if !strings.Contains(got, want) {
				t.Errorf("marker lacks %q: %q", want, got)
			}
		}
	})
	t.Run("bytes", func(t *testing.T) {
		line := strings.Repeat("b", 199) + "\n"
		data := strings.Repeat(line, 300)
		got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
		if len(got) > MaxPreviewBytes || PreviewLines(got) > MaxPreviewLines {
			t.Fatalf("preview = %d bytes, %d lines", len(got), PreviewLines(got))
		}
		for _, want := range []string{"lines 1-121 and 181-300 of 300", "head line 121 cut after 31 bytes", "omitted bytes 24032-36000 (lines 121-180)", "300 lines and 60000 bytes observed"} {
			if !strings.Contains(got, want) {
				t.Errorf("marker lacks %q: %q", want, got)
			}
		}
	})
}

func TestPreviewCutsASingleOverlongLineAtBothEnds(t *testing.T) {
	data := strings.Repeat("x", 200000)
	got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	if len(got) > MaxPreviewBytes || !utf8.ValidString(got) {
		t.Fatalf("preview = %d bytes, valid %v", len(got), utf8.ValidString(got))
	}
	for _, want := range []string{
		"lines 1-1 and 1-1 of 1 (line 1 is split)",
		"head line 1 cut after ",
		"tail line 1 cut to its last ",
		"omitted bytes 24033-175968 on line 1",
		"page omitted bytes 24033-75232: dd if=" + previewArtifact,
		"1 lines and 200000 bytes observed",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("preview lacks %q: %q", want, got)
		}
	}
	if !strings.HasPrefix(got, strings.Repeat("x", 1000)) || !strings.HasSuffix(got, strings.Repeat("x", 1000)) {
		t.Fatal("preview lost the head or tail of the line")
	}
}

func TestPreviewCountsCRLFLines(t *testing.T) {
	data := strings.Repeat("a\r\n", 2500)
	got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	if len(got) > MaxPreviewBytes || PreviewLines(got) > MaxPreviewLines {
		t.Fatalf("preview = %d bytes, %d lines", len(got), PreviewLines(got))
	}
	for _, want := range []string{"lines 1-999 and 1501-2500 of 2500", "2500 lines and 7500 bytes observed"} {
		if !strings.Contains(got, want) {
			t.Errorf("marker lacks %q: %q", want, got)
		}
	}
	if !strings.HasPrefix(got, strings.Repeat("a\r\n", 999)) || !strings.HasSuffix(got, strings.Repeat("a\r\n", 1000)) {
		t.Fatalf("CRLF lines were not kept: %q", got[:20])
	}
}

func TestPreviewKeepsInvalidUTF8WithinItsLimit(t *testing.T) {
	for _, data := range []string{
		strings.Repeat("\xff", 100),
		strings.Repeat("\xff\xfe\xfd", 20000),
	} {
		got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
		if len(got) > MaxPreviewBytes || !utf8.ValidString(got) {
			t.Fatalf("preview of %d bytes = %d bytes, valid %v", len(data), len(got), utf8.ValidString(got))
		}
	}
}

func TestPreviewReportsAnArtifactCutByOutputLimit(t *testing.T) {
	data := strings.Repeat("kept\n", 100)
	got := previewOf(t, data, MaxPreviewBytes, 120)
	if len(got) > MaxPreviewBytes {
		t.Fatalf("preview = %d bytes", len(got))
	}
	if !strings.HasPrefix(got, data) {
		t.Fatalf("the whole observed output inside the preview limits was not shown: %q", got)
	}
	for _, want := range []string{
		"artifact keeps 120 of 500 bytes (-output-limit);",
		"page stored artifact bytes 1-120: dd if=" + previewArtifact,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("preview lacks %q: %q", want, got)
		}
	}
}

func TestPreviewIsIndependentOfChunking(t *testing.T) {
	data := strings.Repeat("chunk-0123456789\n", 4000)
	want := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	for _, size := range []int{1, 2, 31, 4096, len(data)} {
		var buffer PreviewBuffer
		buffer = NewPreviewBuffer(MaxPreviewBytes)
		for i := 0; i < len(data); i += size {
			buffer.Observe([]byte(data[i:min(i+size, len(data))]))
		}
		if got := buffer.String(previewOptions(data, int64(len(data)))); got != want {
			t.Fatalf("chunk size %d changed the preview", size)
		}
	}
}

func TestPreviewKeepsTailLinesAfterAnOverlongHeadLine(t *testing.T) {
	var b strings.Builder
	b.WriteString(strings.Repeat("x", 60000))
	b.WriteByte('\n')
	for range 1000 {
		b.WriteString("ok\n")
	}
	data := b.String()
	got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	if !strings.Contains(got, "ok\nok\n") || !strings.HasSuffix(got, "ok\n") {
		t.Fatalf("tail lines disappeared: %q", got[max(0, len(got)-200):])
	}
	if !strings.Contains(got, "lines 1-1 and 2-1001 of 1001") {
		t.Fatalf("marker lost the selected tail range: %q", got)
	}
}

func TestPreviewReportsCaptureProvenanceInsteadOfComplete(t *testing.T) {
	data := "kept\n"
	for name, o := range map[string]PreviewOptions{
		"incomplete":  {Artifact: previewArtifact, Stored: int64(len(data)), StoredLines: 1, Incomplete: true},
		"write error": {Artifact: previewArtifact, Stored: 2, StoredLines: 1, CaptureError: "capture failed"},
	} {
		t.Run(name, func(t *testing.T) {
			buffer := NewPreviewBuffer(MaxPreviewBytes)
			buffer.Observe([]byte(data))
			got := buffer.String(o)
			if !strings.Contains(got, "capture ") || strings.Contains(got, "artifact complete") {
				t.Fatalf("preview claimed complete capture: %q", got)
			}
			if name == "write error" && (!strings.Contains(got, "capture error: capture failed") || !strings.Contains(got, "artifact keeps 2 of 5 bytes (capture error)") || strings.Contains(got, "-output-limit")) {
				t.Fatalf("write failure was labelled as an output-limit cut: %q", got)
			}
			if name == "incomplete" && !strings.Contains(got, "artifact keeps all 5 observed bytes, but capture was not complete") {
				t.Fatalf("incomplete capture claimed a complete artifact: %q", got)
			}
		})
	}
}

func TestPreviewCountsTrailingEmptyLinesInTheMarker(t *testing.T) {
	for _, lines := range []int{2001, 3000} {
		data := strings.Repeat("\n", lines)
		got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
		wantRange := fmt.Sprintf("lines 1-999 and %d-%d of %d", lines-999, lines, lines)
		for _, want := range []string{wantRange, fmt.Sprintf("%d lines and %d bytes observed", lines, lines)} {
			if !strings.Contains(got, want) {
				t.Fatalf("lines=%d: preview lacks %q: %q", lines, want, got)
			}
		}
		if n := strings.Count(got, "\n"); n != 2000 {
			t.Fatalf("lines=%d: preview has %d newlines, want 2,000 for 999 head + marker + 1,000 tail lines", lines, n)
		}
	}
}

func TestPreviewKeepsTailAfterALongFirstLineAndNewlineTail(t *testing.T) {
	data := strings.Repeat("x", 52000) + strings.Repeat("\nend", 1000)
	got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	if !strings.HasSuffix(got, "end\nend") || !strings.Contains(got, "lines 1-1 and 2-1001 of 1001") {
		t.Fatalf("tail after a long first line was lost: %q", got[max(0, len(got)-120):])
	}
}

func TestPreviewPagesOnlyOmittedDataTheArtifactStored(t *testing.T) {
	t.Run("line range", func(t *testing.T) {
		data := strings.Repeat("a\n", 3000)
		got := previewOf(t, data, MaxPreviewBytes, 100)
		for _, want := range []string{"omitted bytes 1999-4000 (lines 1000-2000)", "artifact stores only the first 100 bytes", "omitted data is unavailable"} {
			if !strings.Contains(got, want) {
				t.Errorf("preview lacks %q: %q", want, got)
			}
		}
		if strings.Contains(got, "dd if=") {
			t.Fatalf("paging names an omitted range the artifact does not store: %q", got)
		}
	})
	t.Run("overlong line", func(t *testing.T) {
		data := strings.Repeat("x", 100000)
		got := previewOf(t, data, MaxPreviewBytes, 30000)
		for _, want := range []string{"omitted bytes 24033-75968 on line 1", "dd if=" + previewArtifact + " bs=1 skip=24032"} {
			if !strings.Contains(got, want) {
				t.Errorf("preview lacks %q: %q", want, got)
			}
		}
	})
	t.Run("overlong line not stored", func(t *testing.T) {
		data := strings.Repeat("x", 100000)
		got := previewOf(t, data, MaxPreviewBytes, 100)
		for _, want := range []string{"omitted bytes 24033-75968 on line 1", "artifact stores only the first 100 bytes", "omitted data is unavailable"} {
			if !strings.Contains(got, want) {
				t.Errorf("preview lacks %q: %q", want, got)
			}
		}
		if strings.Contains(got, "dd if=") {
			t.Fatalf("paging names bytes the artifact does not store: %q", got)
		}

	})
}

func TestPreviewPagingFitsTheNextPreview(t *testing.T) {
	data := strings.Repeat("a\n", 10000)
	path := filepath.Join(t.TempDir(), "output")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	reader := strings.NewReader(data)
	got := NewPreviewBuffer(MaxPreviewBytes)
	got.Observe([]byte(data))
	preview := got.String(PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: 10000,
		PageEnd: func(start, max int64) int64 {
			if end, err := pageEndFromReaderAt(reader, int64(len(data)), start, max, MaxPreviewBytes); err == nil {
				return end
			}
			return max
		}})
	_, command := pageCommandForTest(t, preview)
	out, err := exec.Command("sh", "-c", command).CombinedOutput()
	if err != nil {
		t.Fatalf("page %q: %v: %s", command, err, out)
	}
	if len(out) > MaxPreviewBytes || PreviewLines(string(out)) > MaxPreviewLines {
		t.Fatalf("page = %d bytes and %d lines", len(out), PreviewLines(string(out)))
	}
	if next := previewOf(t, string(out), MaxPreviewBytes, int64(len(out))); next != string(out) {
		t.Fatalf("the next preview truncates the page: %d bytes -> %d bytes", len(out), len(next))
	}
}

func TestPreviewContentDoesNotDependOnArtifactPathLength(t *testing.T) {
	data := strings.Repeat("x", 50979)
	short := NewPreviewBuffer(MaxPreviewBytes)
	long := NewPreviewBuffer(MaxPreviewBytes)
	short.Observe([]byte(data))
	long.Observe([]byte(data))
	a := short.String(PreviewOptions{Artifact: "/canti/artifacts/e/output", Stored: 100})
	b := long.String(PreviewOptions{Artifact: "/" + strings.Repeat("long-path/", 50) + "output", Stored: 100})
	if len(a) == 0 || len(b) == 0 {
		t.Fatal("empty preview")
	}
	if got, want := strings.ReplaceAll(b, "/"+strings.Repeat("long-path/", 50)+"output", "/canti/artifacts/e/output"), a; got != want {
		t.Fatalf("path length changed the selected content:\nshort %q\nlong  %q", a, got)
	}
}

func TestPreviewStatesAllCaptureAndArtifactCauses(t *testing.T) {
	data := strings.Repeat("x", 1000)
	buffer := NewPreviewBuffer(MaxPreviewBytes)
	buffer.Observe([]byte(data))
	got := buffer.String(PreviewOptions{Artifact: previewArtifact, Stored: 100, ArtifactLimit: 100, StoredLines: 1, Incomplete: true})
	for _, want := range []string{"capture incomplete", "artifact keeps 100 of 1000 bytes (-output-limit; capture incomplete)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("preview lacks %q: %q", want, got)
		}
	}
}

func TestPreviewCutsInvalidRunsLikeWholeLines(t *testing.T) {
	data := strings.Repeat("\xff", 100000) + strings.Repeat("x", 100000)
	got := previewOf(t, data, MaxPreviewBytes, int64(len(data)))
	if n := strings.Count(got, "\uFFFD"); n > 2 {
		t.Fatalf("preview has %d replacement runes, want one for each cut end: %q", n, got[len(got)-80:])
	}
}

func TestPreviewPagingCommandsReadRealArtifacts(t *testing.T) {
	t.Run("lines", func(t *testing.T) {
		data := strings.Repeat("a\n", 3000)
		dir := t.TempDir()
		path := filepath.Join(dir, "output")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		buffer := NewPreviewBuffer(MaxPreviewBytes)
		buffer.Observe([]byte(data))
		got := buffer.String(PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: 3000})
		command := pagingCommand(t, got, "page omitted bytes 1999-4000: ")
		out, err := exec.Command("sh", "-c", command).CombinedOutput()
		if err != nil || string(out) != strings.Repeat("a\n", 1001) {
			t.Fatalf("line page = %d bytes, %v: %q", len(out), err, out[:min(len(out), 80)])
		}
	})
	t.Run("bytes", func(t *testing.T) {
		data := strings.Repeat("x", 60000)
		dir := t.TempDir()
		path := filepath.Join(dir, "output")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		buffer := NewPreviewBuffer(MaxPreviewBytes)
		buffer.Observe([]byte(data))
		got := buffer.String(PreviewOptions{Artifact: path, Stored: 60000, ArtifactLimit: 60000, StoredLines: 1})
		command := pagingCommand(t, got, "page omitted bytes 24033-35968: ")
		out, err := exec.Command("sh", "-c", command).CombinedOutput()
		if err != nil || string(out) != data[24032:35968] {
			t.Fatalf("full-artifact byte page = %d bytes, %v", len(out), err)
		}
	})
	t.Run("byte page is capped by a shorter stored artifact", func(t *testing.T) {
		data := strings.Repeat("x", 60000)
		dir := t.TempDir()
		path := filepath.Join(dir, "output")
		if err := os.WriteFile(path, []byte(data[:30000]), 0600); err != nil {
			t.Fatal(err)
		}
		buffer := NewPreviewBuffer(MaxPreviewBytes)
		buffer.Observe([]byte(data))
		got := buffer.String(PreviewOptions{Artifact: path, Stored: 30000, ArtifactLimit: 30000, StoredLines: 1})
		command := pagingCommand(t, got, "page omitted bytes 24033-30000: ")
		out, err := exec.Command("sh", "-c", command).CombinedOutput()
		if err != nil || string(out) != data[24032:30000] {
			t.Fatalf("stored byte page = %d bytes, %v", len(out), err)
		}
	})
	t.Run("two lines with only the shown prefix stored", func(t *testing.T) {
		data := strings.Repeat("x", 30000) + "\n" + strings.Repeat("y", 30000)
		got := previewOf(t, data, MaxPreviewBytes, 100)
		if !strings.Contains(got, "omitted data is unavailable") || strings.Contains(got, "page omitted") {
			t.Fatalf("line page claimed shown bytes are retrievable: %q", got)
		}
	})
}

func pagingCommand(t *testing.T, preview, marker string) string {
	t.Helper()
	_, after, ok := strings.Cut(preview, marker)
	if !ok {
		t.Fatalf("preview has no %q: %q", marker, preview)
	}
	command, _, ok := strings.Cut(after, " ...")
	if !ok {
		t.Fatalf("paging command is unfinished: %q", after)
	}
	return command
}

func TestPreviewPagingKeepsTerminatingLineBytesExact(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"unterminated", strings.Repeat("x", 60000)},
		{"lf", strings.Repeat("x", 60000) + "\n"},
		{"crlf", strings.Repeat("x", 60000) + "\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "output")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			buffer := NewPreviewBuffer(MaxPreviewBytes)
			buffer.Observe([]byte(tc.data))
			reader := strings.NewReader(tc.data)
			got := buffer.String(PreviewOptions{Artifact: path, Stored: int64(len(tc.data)), ArtifactLimit: int64(len(tc.data)), StoredLines: int64(PreviewLines(tc.data)), PreviewLimit: MaxPreviewBytes,
				PageEnd: func(start, max int64) int64 {
					if end, err := pageEndFromReaderAt(reader, int64(len(tc.data)), start, max, MaxPreviewBytes); err == nil {
						return end
					}
					return max
				}})
			span, command := pageCommandForTest(t, got)
			var start, end int
			if _, err := fmt.Sscanf(span, "%d-%d", &start, &end); err != nil || start < 1 || end < start || end > len(tc.data) {
				t.Fatalf("page span %q: %v", span, err)
			}
			out, err := exec.Command("sh", "-c", command).CombinedOutput()
			if err != nil {
				t.Fatal(err)
			}
			if want := tc.data[start-1 : end]; string(out) != want {
				t.Fatalf("page = %d bytes, want %d (span %s)", len(out), len(want), span)
			}
		})
	}
}

func pageCommandForTest(t *testing.T, preview string) (string, string) {
	t.Helper()
	_, after, ok := strings.Cut(preview, "page omitted bytes ")
	if !ok {
		t.Fatalf("preview has no page: %q", preview)
	}
	span, command, ok := strings.Cut(after, ": ")
	if !ok {
		t.Fatalf("preview page has no separator: %q", after)
	}
	command, _, ok = strings.Cut(command, " ...")
	if !ok {
		t.Fatalf("preview page is unterminated: %q", after)
	}
	return span, command
}

func TestPreviewPagingUsesTheConfiguredPreviewLimit(t *testing.T) {
	data := strings.Repeat("a\n", 10000)
	path := filepath.Join(t.TempDir(), "output")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	limit := 4096
	buffer := NewPreviewBuffer(limit)
	buffer.Observe([]byte(data))
	reader := strings.NewReader(data)
	preview := buffer.String(PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: 10000, PreviewLimit: limit,
		PageEnd: func(start, max int64) int64 {
			if end, err := pageEndFromReaderAt(reader, int64(len(data)), start, max, limit); err == nil {
				return end
			}
			return max
		}})
	_, command := pageCommandForTest(t, preview)
	out, err := exec.Command("sh", "-c", command).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if len(out) > limit {
		t.Fatalf("configured page = %d bytes, limit %d", len(out), limit)
	}
	next := NewPreviewBuffer(limit)
	next.Observe(out)
	if got := next.String(PreviewOptions{Artifact: path, Stored: int64(len(out)), ArtifactLimit: int64(len(out)), StoredLines: int64(PreviewLines(string(out))), PreviewLimit: limit}); got != string(out) {
		t.Fatalf("configured page was truncated: %d -> %d bytes", len(out), len(got))
	}
}

func TestPreviewPagingAccountsForInvalidUTF8Expansion(t *testing.T) {
	for _, data := range [][]byte{
		bytes.Repeat([]byte{0xff, 'x'}, 60000),
		append(bytes.Repeat([]byte{0xff}, 60000), []byte("é")...),
		bytes.Repeat([]byte{0xff}, 120000),
	} {
		path := filepath.Join(t.TempDir(), "output")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		const limit = 50 << 10
		buffer := NewPreviewBuffer(limit)
		buffer.Observe(data)
		reader := bytes.NewReader(data)
		preview := buffer.String(PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: int64(PreviewLines(string(data))), PreviewLimit: limit,
			PageEnd: func(start, max int64) int64 {
				if end, err := pageEndFromReaderAt(reader, int64(len(data)), start, max, limit); err == nil {
					return end
				}
				return max
			}})
		command := pageCommandAnyForTest(t, preview)
		out, err := exec.Command("sh", "-c", command).CombinedOutput()
		if err != nil {
			t.Fatal(err)
		}
		next := NewPreviewBuffer(limit)
		next.Observe(out)
		if got := next.String(PreviewOptions{Artifact: path, Stored: int64(len(out)), ArtifactLimit: int64(len(out)), StoredLines: int64(PreviewLines(string(out))), PreviewLimit: limit}); got != renderRaw(out) {
			t.Fatalf("invalid-byte page was truncated: %d raw bytes -> %d rendered bytes", len(out), len(got))
		}
	}
}

func pageCommandAnyForTest(t *testing.T, preview string) string {
	t.Helper()
	for _, marker := range []string{"page omitted bytes ", "page stored artifact bytes "} {
		if _, after, ok := strings.Cut(preview, marker); ok {
			_, command, ok := strings.Cut(after, ": ")
			if !ok {
				continue
			}
			command, _, ok = strings.Cut(command, " ...")
			if ok {
				return command
			}
		}
	}
	t.Fatalf("preview has no page: %q", preview)
	return ""
}

func TestPreviewUnavailableArtifactNamesTheRetainedPathOnce(t *testing.T) {
	path := "/" + strings.Repeat("a", 1023)
	data := strings.Repeat("x\n", 3000)
	got := previewOf(t, data, MaxPreviewBytes, 100)
	// previewOf uses the synthetic shared path; render again with this path.
	buffer := NewPreviewBuffer(MaxPreviewBytes)
	buffer.Observe([]byte(data))
	got = buffer.String(PreviewOptions{Artifact: path, Stored: 100, ArtifactLimit: 100, StoredLines: 1, PreviewLimit: MaxPreviewBytes})
	if !strings.Contains(got, "omitted data is unavailable") || strings.Contains(got, "dd if=") || strings.Count(got, path) != 1 {
		t.Fatalf("unavailable artifact did not name its retained path once: %q", got)
	}
}
