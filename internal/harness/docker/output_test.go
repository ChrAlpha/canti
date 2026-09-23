package docker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/internal/harness"
)

func capture(t *testing.T, r io.Reader, w io.Writer, limit int64, previewLimit int) *tap {
	t.Helper()
	c := &tap{w: w, limit: limit, previewLimit: previewLimit}
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
		if n <= 64 && c.preview() != in {
			t.Fatalf("n=%d: preview %q, want the whole output", n, c.preview())
		}
		if len(c.preview()) > 64 {
			t.Fatalf("n=%d: preview has %d bytes", n, len(c.preview()))
		}
	}
}

func TestTapTruncatesAndKeepsTrueTail(t *testing.T) {
	in := "HEAD" + strings.Repeat("0123456789", 5000) + "TAIL"
	for _, r := range []io.Reader{strings.NewReader(in), iotest.HalfReader(strings.NewReader(in)), iotest.OneByteReader(strings.NewReader(in))} {
		var out bytes.Buffer
		c := capture(t, r, &out, 1000, 128)
		if c.werr != nil || c.stored != 1000 || c.total != int64(len(in)) || out.String() != in[:1000] {
			t.Fatalf("stored %d total %d err %v, kept %d bytes", c.stored, c.total, c.werr, out.Len())
		}
		p := c.preview()
		if len(p) > 128 || !strings.HasPrefix(p, "HEAD") || !strings.HasSuffix(p, "TAIL") {
			t.Fatalf("preview = %q", p)
		}
		var omitted int
		head, rest, _ := strings.Cut(p, "\n...")
		if _, err := fmt.Sscanf(rest, " %d bytes omitted ...\n", &omitted); err != nil {
			t.Fatalf("preview %q has no omission marker: %v", p, err)
		}
		_, tail, _ := strings.Cut(rest, "omitted ...\n")
		if omitted != len(in)-len(head)-len(tail) || !strings.HasPrefix(in, head) || !strings.HasSuffix(in, tail) {
			t.Fatalf("preview %q omits %d of %d bytes", p, omitted, len(in))
		}
	}
}

func TestPreviewCutsOnRuneBoundaries(t *testing.T) {
	for shift := range 4 {
		in := strings.Repeat("x", shift) + strings.Repeat("é世🙂", 400)
		for limit := 64; limit < 96; limit++ {
			c := capture(t, iotest.HalfReader(strings.NewReader(in)), io.Discard, 1<<20, limit)
			p := c.preview()
			head, _, _ := strings.Cut(p, "\n...")
			if len(p) > limit || len(p) < limit-2*(utf8.UTFMax-1) || !utf8.ValidString(p) || !strings.HasPrefix(in, head) || !strings.HasSuffix(p, "🙂") {
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
		c := capture(t, iotest.HalfReader(strings.NewReader(in)), &failingWriter{room: 100, err: werr}, 1<<20, 64)
		want := werr
		if want == nil {
			want = io.ErrShortWrite
		}
		if !errors.Is(c.werr, want) || c.total != int64(len(in)) {
			t.Fatalf("werr %v total %d, want %v and %d", c.werr, c.total, want, len(in))
		}
		if p := c.preview(); !strings.HasPrefix(p, "HEAD") || !strings.HasSuffix(p, "TAIL") {
			t.Fatalf("preview = %q", p)
		}
		ref := describe("e/output", c, nil, harness.Job{Status: harness.JobSucceeded})
		if ref.Available || ref.Complete || ref.Bytes != 0 || !strings.Contains(ref.Detail, want.Error()) {
			t.Fatalf("describe() = %+v", ref)
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
		{"logs unavailable", 5, 5, errors.New("exit status 1"), harness.JobSucceeded, harness.ArtifactRef{Path: "e/output", Detail: "logs unavailable: exit status 1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if ref := describe("e/output", &tap{stored: tt.stored, total: tt.total}, tt.logs, harness.Job{Status: tt.status}); ref != tt.want {
				t.Fatalf("describe() = %+v, want %+v", ref, tt.want)
			}
		})
	}
}

func TestVerifyRechecksArtifact(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "e"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "e", "output"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok := harness.ArtifactRef{Path: "e/output", Bytes: 5, Complete: true, Available: true}
	if got := verify(root, ok); got != ok {
		t.Fatalf("verify() = %+v, want %+v", got, ok)
	}
	resized := ok
	resized.Bytes = 6
	if got := verify(root, resized); got.Available || got.Complete || !strings.Contains(got.Detail, "artifact has 5 bytes") {
		t.Fatalf("verify(resized) = %+v", got)
	}
	unavailable := harness.ArtifactRef{Path: "e/output", Detail: "logs unavailable: x"}
	if got := verify(root, unavailable); got != unavailable {
		t.Fatalf("verify(unavailable) = %+v", got)
	}
	if err := os.Remove(filepath.Join(root, "e", "output")); err != nil {
		t.Fatal(err)
	}
	if got := verify(root, ok); got.Available || got.Complete || !strings.Contains(got.Detail, "artifact missing") {
		t.Fatalf("verify(missing) = %+v", got)
	}
}
