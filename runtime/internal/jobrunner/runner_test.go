package jobrunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf8"
)

func config(t *testing.T, command string) Config {
	t.Helper()
	return Config{Directory: t.TempDir(), EffectID: "effect_test", Spec: strings.Repeat("a", 64), Generation: 1, Workdir: t.TempDir(), Command: command, OutputLimit: 1024, PreviewLimit: 128}
}
func TestCompletionKeepsCommandExitAndImmutableCapture(t *testing.T) {
	c := config(t, "printf hello; printf error >&2; exit 7")
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ExitCode == nil || *rec.ExitCode != 7 || rec.Bytes != 10 || rec.Stored != 10 || rec.Incomplete || rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt) {
		t.Fatalf("record = %+v", rec)
	}
	out, err := os.ReadFile(filepath.Join(c.Directory, "output"))
	if err != nil || string(out) != "helloerror" {
		t.Fatalf("output=%q: %v", out, err)
	}
	data, err := os.ReadFile(filepath.Join(c.Directory, "completion.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Record
	if err = json.Unmarshal(data, &saved); err != nil || saved.Spec != c.Spec || saved.Digest != rec.Digest {
		t.Fatalf("saved = %+v: %v", saved, err)
	}
	if _, err = Run(c); err == nil {
		t.Fatal("a second launcher reused existing command evidence")
	}
}
func TestCaptureBoundsOutputWithoutStoppingCommand(t *testing.T) {
	c := config(t, "i=0; while [ $i -lt 10000 ]; do printf 0123456789; i=$((i+1)); done; printf done > finished")
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Bytes != 100000 || rec.Stored != c.OutputLimit || *rec.ExitCode != 0 {
		t.Fatalf("record=%+v", rec)
	}
	info, err := os.Stat(filepath.Join(c.Directory, "output"))
	if err != nil || info.Size() != c.OutputLimit {
		t.Fatalf("output=%v: %v", info, err)
	}
	if _, err = os.Stat(filepath.Join(c.Workdir, "finished")); err != nil {
		t.Fatal(err)
	}
}
func TestInheritedBackgroundPipeCannotHoldCompletion(t *testing.T) {
	c := config(t, "sleep 1 & printf foreground")
	start := time.Now()
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 800*time.Millisecond || !rec.Incomplete || rec.CaptureError != "" || *rec.ExitCode != 0 || rec.Preview != "foreground" {
		t.Fatalf("elapsed=%v record=%+v", time.Since(start), rec)
	}
}

func TestClosedPipeWaitsForCollectorPastTheLingerGrace(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = w.Write([]byte("0\n")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	captured, result := make(chan capture, 1), make(chan capture, 1)
	release := make(chan struct{})
	go func() { <-release; captured <- collect(r, &out, 1024, 128) }()
	grace := 5 * time.Millisecond
	go func() { result <- awaitCapture(r, captured, grace) }()
	select {
	case got := <-result:
		close(release)
		t.Fatalf("returned before collector: %+v", got)
	case <-time.After(4 * grace):
	}
	close(release)
	select {
	case got := <-result:
		if !got.eof || got.err != nil || got.total != 2 || got.stored != 2 || out.String() != "0\n" {
			t.Fatalf("closed pipe lost complete capture: %+v output=%q", got, out.String())
		}
	case <-time.After(time.Second):
		t.Fatal("closed pipe did not finish after collector was released")
	}
}

func TestPipeWriterObservationKeepsBufferedEOFAndUnknownStatesDistinct(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if !writersRemain(r) {
		t.Fatal("empty pipe lost its live writer")
	}
	if _, err = w.Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	if !writersRemain(r) {
		t.Fatal("buffered bytes concealed a live writer")
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if writersRemain(r) {
		t.Fatal("buffered bytes concealed confirmed EOF")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if !writersRemain(r) {
		t.Fatal("unavailable pipe manufactured confirmed EOF")
	}
}

func TestCaptureDeadlineRetainsPendingBytesWithinItsOutputLimit(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	text := strings.Repeat("pending-", 128)
	if _, err = w.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err = r.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	result := make(chan capture, 1)
	go func() { result <- collect(r, &out, 16, 128) }()
	var got capture
	select {
	case got = <-result:
	case <-time.After(time.Second):
		r.Close()
		w.Close()
		t.Fatal("deadline drain waited for the live writer")
	}
	if got.eof || got.err != nil || got.total != int64(len(text)) || got.stored != 16 || out.String() != text[:16] || len(got.preview()) > 128 {
		t.Fatalf("deadline lost pending bytes or limits: %+v output=%q", got, out.String())
	}
}

func TestContinuousInheritedWriterCannotExtendCapturePastItsGrace(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	writerReady, writerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(writerDone)
		chunk := bytes.Repeat([]byte("x"), 4096)
		if _, err := w.Write(chunk); err != nil {
			return
		}
		close(writerReady)
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}()
	select {
	case <-writerReady:
	case <-time.After(time.Second):
		t.Fatal("writer did not publish initial bytes")
	}
	var out bytes.Buffer
	captured, result := make(chan capture, 1), make(chan capture, 1)
	go func() { captured <- collect(r, &out, 64, 128) }()
	start := time.Now()
	go func() { result <- awaitCapture(r, captured, 10*time.Millisecond) }()
	var got capture
	select {
	case got = <-result:
	case <-time.After(time.Second):
		r.Close()
		w.Close()
		t.Fatal("continuous writer extended capture past its bound")
	}
	if time.Since(start) > time.Second || got.eof || got.err != nil || got.total < got.stored || got.stored != 64 || out.Len() != 64 {
		t.Errorf("live writer escaped bounded incomplete capture: %+v elapsed=%v", got, time.Since(start))
	}
	select {
	case <-writerDone:
		t.Error("writer ceased before capture stopped waiting")
	default:
	}
	r.Close()
	w.Close()
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("writer did not exit after pipe cleanup")
	}
}

func TestCollectorReadFailureDoesNotManufactureEOF(t *testing.T) {
	failure := errors.New("read failed")
	got := collect(iotest.ErrReader(failure), io.Discard, 1024, 128)
	if got.eof || !errors.Is(got.err, failure) || got.total != 0 || got.stored != 0 {
		t.Fatalf("read failure became EOF: %+v", got)
	}
}
func TestInitializationAndWorkingDirectoryAreExplicit(t *testing.T) {
	c := config(t, "printf '%s:%s' \"$TOOL_PATH\" \"$PWD\"")
	c.Workdir = filepath.Join(c.Workdir, strings.Repeat("long-directory-", 12))
	if err := os.MkdirAll(c.Workdir, 0700); err != nil {
		t.Fatal(err)
	}
	c.Init = filepath.Join(t.TempDir(), "init.sh")
	if err := os.WriteFile(c.Init, []byte("export TOOL_PATH=persistent\ncd /\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(filepath.Join(c.Directory, "output"))
	if err != nil || *rec.ExitCode != 0 || string(output) != "persistent:"+c.Workdir {
		t.Fatalf("retained output=%q: %v", output, err)
	}
}

func TestMissingWorkingDirectoryPublishesCommandFailure(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "initialized"}[initialized], func(t *testing.T) {
			c := config(t, "printf should-not-run")
			c.Workdir = filepath.Join(c.Workdir, "missing")
			if initialized {
				c.Init = filepath.Join(t.TempDir(), "init.sh")
				if err := os.WriteFile(c.Init, []byte("export INITIALIZED=yes\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			rec, err := Run(c)
			if err != nil || rec.ExitCode == nil || *rec.ExitCode == 0 || rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt) || rec.Incomplete {
				t.Fatalf("cwd failure must have ordinary completion: %+v: %v", rec, err)
			}
			output, err := os.ReadFile(filepath.Join(c.Directory, "output"))
			if err != nil || !strings.Contains(string(output), "cd") || !strings.Contains(string(output), c.Workdir) || strings.Contains(string(output), "should-not-run") {
				t.Fatalf("cwd stderr=%q: %v", output, err)
			}
			data, err := os.ReadFile(filepath.Join(c.Directory, "completion.json"))
			var saved Record
			if err != nil || json.Unmarshal(data, &saved) != nil || saved.ExitCode == nil || *saved.ExitCode != *rec.ExitCode || saved.Digest != rec.Digest || saved.EffectID != c.EffectID {
				t.Fatalf("durable cwd completion=%+v: %v", saved, err)
			}
		})
	}
}
func TestEvidenceDoesNotFollowAnOutputSymlink(t *testing.T) {
	c := config(t, "touch should-not-run")
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(c.Directory, "output")); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(c); err == nil {
		t.Fatal("followed output link")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "untouched" {
		t.Fatal("modified symlink target")
	}
	if _, err := os.Stat(filepath.Join(c.Workdir, "should-not-run")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("command ran")
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }
func TestCaptureFailureStillDrainsTheChild(t *testing.T) {
	c := collect(strings.NewReader(strings.Repeat("x", 10000)), brokenWriter{}, 1024, 128)
	if c.total != 10000 || c.err == nil || c.stored != 0 {
		t.Fatalf("capture=%+v", c)
	}
}
func TestSignalExitRetainsTheShellStatus(t *testing.T) {
	c := config(t, "kill -TERM $$")
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if *rec.ExitCode != 143 {
		t.Fatalf("exit=%d", *rec.ExitCode)
	}
}

func TestArbitraryBytePreviewRespectsTheFinalUTF8ByteLimit(t *testing.T) {
	data := make([]byte, 0, 4096)
	for i := 0; i < 4096; i++ {
		data = append(data, byte(i%256))
	}
	for _, size := range []int{64, 128, 4096} {
		got := collect(bytes.NewReader(data), io.Discard, 4096, size).preview()
		if !utf8.ValidString(got) || len(got) > size {
			t.Fatalf("limit=%d bytes=%d valid=%v", size, len(got), utf8.ValidString(got))
		}
	}
}

func TestControlByteOutputCannotOverflowCompletionMetadata(t *testing.T) {
	c := config(t, "printf '"+strings.Repeat("\\001", 4096)+"'")
	c.OutputLimit = 4096
	c.PreviewLimit = 4096
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if *rec.ExitCode != 0 || rec.Bytes != 4096 || rec.Stored != 4096 || len(rec.Preview) > 4096 {
		t.Fatalf("record=%+v", rec)
	}
	data, err := os.ReadFile(filepath.Join(c.Directory, "completion.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > MetadataLimit {
		t.Fatalf("metadata=%d", len(data))
	}
	var saved Record
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Preview, "preview shortened for metadata") {
		t.Fatal("metadata truncation was silent")
	}
	if saved.Preview != rec.Preview || saved.Digest != rec.Digest {
		t.Fatal("returned and published records disagree")
	}
}

func TestAnUnknownWaitFailureIsNotCommandExit125(t *testing.T) {
	if _, err := exitCode(errors.New("wait status unavailable")); err == nil {
		t.Fatal("wait error became command exit")
	}
	c := config(t, "exit 125")
	rec, err := Run(c)
	if err != nil || rec.ExitCode == nil || *rec.ExitCode != 125 {
		t.Fatalf("real exit125 = %+v: %v", rec, err)
	}
}

func TestReplacingOutputWithAFIFOCannotBlockCompletionOrEvidenceRead(t *testing.T) {
	c := config(t, "rm output; mkfifo output; printf captured")
	c.Workdir = c.Directory
	began := time.Now()
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(began) > time.Second || *rec.ExitCode != 0 || rec.Preview != "captured" {
		t.Fatalf("completion=%+v elapsed=%v", rec, time.Since(began))
	}
	root, err := os.OpenRoot(c.Directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	began = time.Now()
	if _, err = ReadEvidence(root, "output", c.OutputLimit); err == nil {
		t.Fatal("accepted FIFO evidence")
	}
	if time.Since(began) > time.Second {
		t.Fatal("evidence read blocked on FIFO")
	}
}

func TestEvidenceRejectsLinksOversizeAndEscape(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err = os.WriteFile(filepath.Join(dir, "large"), []byte("oversized"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("large", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"link", "large", "../outside", "."} {
		if _, err = ReadEvidence(root, name, 4); err == nil {
			t.Fatalf("accepted %s", name)
		}
	}
}
