package docker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

const tapArtifact = "/canti/artifacts/effect_test/output"

func capture(t *testing.T, r io.Reader, w io.Writer, limit int64, previewLimit int) *tap {
	t.Helper()
	c := &tap{w: w, limit: limit, artifact: tapArtifact, preview: jobrunner.NewPreviewBuffer(previewLimit)}
	if _, err := io.Copy(c, r); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestTapKeepsSmallOutput(t *testing.T) {
	for n := 0; n <= 70; n++ {
		in := strings.Repeat("a", n)
		var out bytes.Buffer
		c := capture(t, iotest.OneByteReader(strings.NewReader(in)), &out, 1<<20, 64)
		if c.werr != nil || c.stored != int64(n) || c.total != int64(n) || out.String() != in {
			t.Fatalf("n=%d: stored %d total %d err %v out %q", n, c.stored, c.total, c.werr, out.String())
		}
		if n <= 64 && c.output() != in {
			t.Fatalf("n=%d: preview %q, want the whole output", n, c.output())
		}
		if len(c.output()) > 64 {
			t.Fatalf("n=%d: preview has %d bytes", n, len(c.output()))
		}
	}
}

func TestTapTruncatesAndKeepsTrueTail(t *testing.T) {
	in := "HEAD\n" + strings.Repeat("0123456789\n", 5000) + "TAIL\n"
	for _, r := range []io.Reader{strings.NewReader(in), iotest.HalfReader(strings.NewReader(in)), iotest.OneByteReader(strings.NewReader(in))} {
		var out bytes.Buffer
		c := capture(t, r, &out, 1000, 4096)
		if c.werr != nil || c.stored != 1000 || c.total != int64(len(in)) || out.String() != in[:1000] {
			t.Fatalf("stored %d total %d err %v, kept %d bytes", c.stored, c.total, c.werr, out.Len())
		}
		p := c.output()
		if len(p) > 4096 || !strings.HasPrefix(p, "HEAD\n") || !strings.HasSuffix(p, "TAIL\n") {
			t.Fatalf("preview = %q", p)
		}
		for _, want := range []string{"output truncated", "omitted bytes", "(lines 45-4958)", "artifact keeps 1000 of "} {
			if !strings.Contains(p, want) {
				t.Fatalf("preview lacks %q: %q", want, p)
			}
		}
	}
}

func TestPreviewCutsOnRuneBoundaries(t *testing.T) {
	for shift := range 4 {
		in := strings.Repeat("x", shift) + strings.Repeat("é世🙂", 400)
		for limit := jobrunner.MinPreviewBytes; limit < jobrunner.MinPreviewBytes+32; limit++ {
			c := capture(t, iotest.HalfReader(strings.NewReader(in)), io.Discard, 1<<20, limit)
			p := c.output()
			if len(p) > limit || !utf8.ValidString(p) || !strings.HasSuffix(p, "🙂") {
				t.Fatalf("shift %d limit %d: preview %q (%d bytes)", shift, limit, p, len(p))
			}
		}
	}
}

type failingWriter struct {
	room int
	err  error
}

func (w *failingWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.room)
	w.room -= n
	if n < len(p) {
		return n, w.err
	}
	return n, nil
}

func TestTapRecordsWriteFailureAndKeepsReading(t *testing.T) {
	in := "HEAD" + strings.Repeat("z", 10000) + "TAIL"
	for _, werr := range []error{syscall.ENOSPC, nil} {
		c := capture(t, iotest.HalfReader(strings.NewReader(in)), &failingWriter{room: 100, err: werr}, 1<<20, 4096)
		want := werr
		if want == nil {
			want = io.ErrShortWrite
		}
		if !errors.Is(c.werr, want) || c.total != int64(len(in)) {
			t.Fatalf("werr %v total %d, want %v and %d", c.werr, c.total, want, len(in))
		}
		if p := c.output(); !strings.HasPrefix(p, "HEAD") || !strings.HasSuffix(p, "TAIL") {
			t.Fatalf("preview = %q", p)
		}

	}
}

func TestDescribe(t *testing.T) {
	for _, tt := range []struct {
		name   string
		stored int64
		total  int64
		logs   error
		status harness.JobStatus
		want   harness.ArtifactRef
	}{
		{"complete success", 5, 5, nil, harness.JobSucceeded, harness.ArtifactRef{Path: "e/output", Bytes: 5, Complete: true, Available: true}},
		{"complete failure", 5, 5, nil, harness.JobFailed, harness.ArtifactRef{Path: "e/output", Bytes: 5, Complete: true, Available: true}},
		{"truncated", 5, 9, nil, harness.JobSucceeded, harness.ArtifactRef{Path: "e/output", Bytes: 5, Available: true, Detail: "kept 5 of 9 bytes"}},
		{"cancelled", 5, 5, nil, harness.JobCancelled, harness.ArtifactRef{Path: "e/output", Bytes: 5, Available: true, Detail: "job was cancelled; output is partial"}},
		{"cancelled and truncated", 5, 9, nil, harness.JobCancelled, harness.ArtifactRef{Path: "e/output", Bytes: 5, Available: true, Detail: "kept 5 of 9 bytes; job was cancelled; output is partial"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if ref := describe("e/output", &tap{stored: tt.stored, total: tt.total}, harness.Job{Status: tt.status}); ref != tt.want {
				t.Fatalf("describe() = %+v, want %+v", ref, tt.want)
			}
		})
	}
}

func TestPartialUnavailablePreviewStatesCancellationWithinBounds(t *testing.T) {
	got := partialUnavailablePreview("/canti/artifacts/e/output")
	if !strings.Contains(got, "command was cancelled; output is partial") || strings.Contains(got, "artifact complete") ||
		len(got) > jobrunner.MaxPreviewBytes || jobrunner.PreviewLines(got) > jobrunner.MaxPreviewLines {
		t.Fatalf("partial preview = %q", got)
	}
}

func TestCancelledPreviewEvidenceKeepsStdoutAndRespectsLimits(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	render := func(data []byte, limit int) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "output"), data, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := jobrunner.PreviewEvidence(root, "output", 1<<20, limit, "/canti/artifacts/e/output", 1<<20, true)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	literal := []byte("output truncated: literal\n")
	if got := render(literal, 50<<10); !strings.HasPrefix(got, string(literal)) || !strings.Contains(got, "command was cancelled") {
		t.Fatalf("literal stdout was changed: %q", got)
	}
	if got := render([]byte(strings.Repeat("line\n", 2000)), 50<<10); jobrunner.PreviewLines(got) > jobrunner.MaxPreviewLines || len(got) > 50<<10 {
		t.Fatalf("a full preview exceeded its line bound: %d lines, %d bytes", jobrunner.PreviewLines(got), len(got))
	}
	if got := render([]byte(strings.Repeat("x", 4096)), 4096); len(got) > 4096 || strings.Contains(got, "artifact complete") {
		t.Fatalf("a full byte preview exceeded its limit: %q", got)
	}
}

func TestCancelledProjectionUsesRecordedCaptureFacts(t *testing.T) {
	rec, err := jobrunner.Run(jobrunner.Config{Directory: t.TempDir(), EffectID: "effect_seq", Spec: strings.Repeat("a", 64), Epoch: "epoch", Generation: 1,
		Command: "seq 3000", Workdir: "/", Artifact: "/canti/artifacts/e/output", OutputLimit: 100, PreviewLimit: 50 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.PreviewMarked || rec.Bytes != 13893 || rec.Stored != 100 {
		t.Fatalf("fixture record = %+v", rec)
	}
	got := cancelledProjection(rec.Preview, rec, "/canti/artifacts/e/output", 50<<10)
	if strings.Contains(got, "preview unavailable") || !strings.Contains(got, "13893 bytes") || !strings.Contains(got, "-output-limit") ||
		!strings.Contains(got, "2999\n3000") || !strings.Contains(got, "command was cancelled") || strings.Contains(got, "artifact complete") {
		t.Fatalf("recorded facts were lost: %q", got)
	}
	literal := "ordinary artifact complete: example\n"
	got = cancelledProjection(literal, jobrunner.Record{Bytes: int64(len(literal)), Stored: int64(len(literal))}, "/canti/artifacts/e/output", 50<<10)
	if !strings.HasPrefix(got, literal) || !strings.Contains(got, "command was cancelled") {
		t.Fatalf("literal output was changed: %q", got)
	}
}
func TestCancelledProjectionLongestPathKeepsACompleteArtifact(t *testing.T) {
	path := t.TempDir()
	for len(path)+1+200 < 1023 {
		path = filepath.Join(path, strings.Repeat("a", 200))
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	remain := 1024 - len(path) - 1
	if remain < 1 {
		t.Fatalf("path base leaves no file name: %d", len(path))
	}
	path = filepath.Join(path, strings.Repeat("b", remain))
	if len(path) != 1024 {
		t.Fatalf("artifact path length = %d", len(path))
	}
	data := "HEAD!" + strings.Repeat("x", 60000) + "TAIL-FINAL-XYZ"
	page := func(t *testing.T, preview string) (int, int, string) {
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
		var start, end int
		if _, err := fmt.Sscanf(span, "%d-%d", &start, &end); err != nil || start < 1 || end < start || end > len(data) {
			t.Fatalf("page span %q: %v", span, err)
		}
		return start, end, command
	}
	run := func(t *testing.T, command string) string {
		t.Helper()
		out, err := exec.Command("sh", "-c", command).Output()
		if err != nil {
			t.Fatalf("page command: %v", err)
		}
		return string(out)
	}
	for _, limit := range []int{4096, 50 << 10} {
		rec, err := jobrunner.Run(jobrunner.Config{Directory: t.TempDir(), EffectID: "effect_longest", Spec: strings.Repeat("a", 64), Epoch: "epoch", Generation: 1,
			Command: "printf 'HEAD!'; head -c 60000 /dev/zero | tr '\\0' x; printf 'TAIL-FINAL-XYZ'", Workdir: "/", Artifact: path,
			OutputLimit: int64(len(data)), PreviewLimit: limit})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		preview := rec.Preview
		if !strings.Contains(preview, "TAIL-FINAL-XYZ") || !strings.Contains(preview, "HEAD!") || strings.Count(preview, path) != 1 {
			t.Fatalf("limit %d: helper preview lost its sentinels or repeated the path", limit)
		}
		start, end, command := page(t, preview)
		if got, want := run(t, command), data[start-1:end]; got != want {
			t.Fatalf("limit %d: uncancelled page = %d bytes, want %d", limit, len(got), len(want))
		}
		cancelled := cancelledProjection(preview, rec, path, limit)
		if !strings.Contains(cancelled, "TAIL-FINAL-XYZ") || !strings.Contains(cancelled, "HEAD!") || !strings.Contains(cancelled, "command was cancelled") || !strings.Contains(cancelled, "output is partial") || strings.Contains(cancelled, "artifact complete") || len(cancelled) > limit || strings.Count(cancelled, path) != 1 {
			t.Fatalf("limit %d: cancelled preview lost sentinels, repeated the path, retained a completion claim or exceeded bounds", limit)
		}
		cStart, cEnd, cCommand := page(t, cancelled)
		if got, want := run(t, cCommand), data[cStart-1:cEnd]; got != want {
			t.Fatalf("limit %d: cancelled page = %d bytes, want %d", limit, len(got), len(want))
		}
	}
}

func TestCancelledProjectionKeepsAJobRunnerRunFixture(t *testing.T) {
	path := "/" + strings.Repeat("a", 901)
	dir := t.TempDir()
	rec, err := jobrunner.Run(jobrunner.Config{Directory: dir, EffectID: "effect_fixture", Spec: strings.Repeat("a", 64), Epoch: "epoch", Generation: 1,
		Command: "head -c 60000 /dev/zero | tr '\\0' x", Workdir: "/", Artifact: path, OutputLimit: 100, PreviewLimit: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.PreviewMarked || rec.Bytes != 60000 || rec.Stored != 100 {
		t.Fatalf("fixture record = %+v", rec)
	}
	got := cancelledProjection(rec.Preview, rec, path, 4096)
	if strings.Contains(got, "preview unavailable") || !strings.Contains(got, "60000 bytes") ||
		!strings.Contains(got, "-output-limit") || !strings.Contains(got, "command was cancelled") ||
		!strings.Contains(got, "xxx") || len(got) > 4096 {
		t.Fatalf("jobrunner fixture lost facts: %d bytes, %d lines", len(got), jobrunner.PreviewLines(got))
	}
}

func TestCancelledProjectionRejectsAFullRecordWithoutReservedHeadroom(t *testing.T) {
	path := "/" + strings.Repeat("a", 901)
	marker := "... output truncated: lines 1-1 and 1-1 of 2, 6000 lines and 6000 bytes observed; artifact keeps 100 of 6000 bytes (-output-limit): " + path +
		"; page omitted bytes 24033-35968: dd if=" + path + " bs=1 skip=24032 count=11936 2>/dev/null ..."
	const limit = 4096
	content := limit - len(marker) - 2
	head := "HEAD" + strings.Repeat("h", content/2-4)
	tail := strings.Repeat("t", content-content/2-4) + "TAIL"
	preview := head + "\n" + marker + "\n" + tail
	if len(preview) != limit {
		t.Fatalf("fixture preview = %d bytes", len(preview))
	}
	rec := jobrunner.Record{Bytes: 6000, Stored: 100, PreviewMarked: true, PreviewMarkerStart: len(head) + 1, PreviewMarkerLength: len(marker)}
	got := cancelledProjection(preview, rec, path, limit)
	if !strings.Contains(got, "preview unavailable") || strings.Count(got, path) != 1 {
		t.Fatalf("a full record without reserved headroom did not take the unavailable path exactly once: %q", got)
	}
}

func TestCancelledProjectionFullArtifactHasNoCompletionClaim(t *testing.T) {
	rec, err := jobrunner.Run(jobrunner.Config{Directory: t.TempDir(), EffectID: "effect_full_seq", Spec: strings.Repeat("a", 64), Epoch: "epoch", Generation: 1,
		Command: "seq 3000", Workdir: "/", Artifact: "/canti/artifacts/e/output", OutputLimit: 13893, PreviewLimit: 50 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.PreviewMarked || rec.Bytes != 13893 || rec.Stored != 13893 || rec.Incomplete || rec.CaptureError != "" {
		t.Fatalf("full artifact fixture = %+v", rec)
	}
	got := cancelledProjection(rec.Preview, rec, "/canti/artifacts/e/output", 50<<10)
	if strings.Contains(got, "artifact complete") || !strings.Contains(got, "artifact retained") ||
		!strings.Contains(got, "13893 bytes") || !strings.Contains(got, "2999\n3000") ||
		!strings.Contains(got, "command was cancelled") || len(got) > 50<<10 {
		t.Fatalf("full artifact cancellation claimed completion: %q", got)
	}
}
