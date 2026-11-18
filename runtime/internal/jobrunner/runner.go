// Package jobrunner captures one foreground shell independently of its controller.
// It knows no journal, Docker API, model, or run transitions.
package jobrunner

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	"unicode/utf8"
)

const Protocol = 3
const MetadataLimit = 16 << 10

// lingerGrace bounds how long capture waits, once the foreground shell has
// exited, for descendants that still hold its inherited output pipe.
const lingerGrace = 100 * time.Millisecond

// Config is supplied in the bound exec's arguments, never read from job files.
// Workdir is entered by the command's shell, so it need not exist at launch.
type Config struct {
	Directory, EffectID, Spec, Command, Workdir, Init string
	Artifact                                          string
	Generation                                        uint64
	OutputLimit                                       int64
	PreviewLimit                                      int
}

// Record is mutable-environment evidence. Only the external controller can turn
// it, together with independent process evidence, into an authoritative receipt.
type Record struct {
	Protocol                      int
	EffectID, Spec                string
	Generation                    uint64
	StartedAt, FinishedAt         time.Time
	ExitCode                      *int
	Bytes, Stored                 int64
	Digest, Preview, CaptureError string
	PreviewDigest                 string
	PreviewBytes, PreviewLines    int
	PreviewMarked                 bool
	PreviewMarkerStart            int
	PreviewMarkerLength           int
	PreviewCancelledMarker        string
	Incomplete                    bool
}

func (c Config) validate() error {
	if !filepath.IsAbs(c.Directory) || !filepath.IsAbs(c.Workdir) || !filepath.IsAbs(c.Artifact) || len(c.Artifact) > MaxArtifactPath || !safeArtifactPath(c.Artifact) || c.EffectID == "" || len(c.EffectID) > 128 || !utf8.ValidString(c.EffectID) || c.Generation == 0 || c.OutputLimit < 1 || c.PreviewLimit < MinPreviewBytes || c.PreviewLimit > MaxPreviewBytes {
		return errors.New("job runner: invalid identity, directory or capture limits")
	}
	b, err := hex.DecodeString(c.Spec)
	if err != nil || len(b) != sha256.Size || hex.EncodeToString(b) != c.Spec {
		return errors.New("job runner: invalid specification")
	}
	if c.Init != "" && !filepath.IsAbs(c.Init) {
		return errors.New("job runner: init must be absolute")
	}
	return nil
}

// Run launches a fresh shell, bounds retained output, and publishes a synced
// completion. An error never represents a command's exit code.
func Run(c Config) (Record, error) {
	if err := c.validate(); err != nil {
		return Record{}, err
	}
	root, err := os.OpenRoot(c.Directory)
	if err != nil {
		return Record{}, err
	}
	defer root.Close()
	out, err := root.OpenFile("output", os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return Record{}, err
	}
	defer out.Close()
	// The collector can have a different host UID (for example root commands
	// inside Docker). The private spool directory controls access; evidence files
	// themselves must remain readable by that external collector.
	if err = out.Chmod(0644); err != nil {
		return Record{}, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return Record{}, err
	}
	defer reader.Close()
	defer writer.Close()
	// The shell, not the helper's launch, enters the workdir, before and after
	// the optional init: a missing or inaccessible directory is the command's
	// own failure, captured with its output and a completion, never a launch
	// that leaves no evidence. The helper itself runs from the root directory.
	script := "cd -- \"$2\" || exit $?\n"
	if c.Init != "" {
		script += ". \"$1\" || exit $?\ncd -- \"$2\" || exit $?\n"
	}
	cmd := exec.Command("/bin/sh", "-c", script+c.Command, "canti-job", c.Init, c.Workdir)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = "/", nil, writer, writer
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	rec := Record{Protocol: Protocol, EffectID: c.EffectID, Spec: c.Spec, Generation: c.Generation}
	if err := cmd.Start(); err != nil {
		return rec, err
	}
	rec.StartedAt = time.Now().UTC() // The helper's observation, not a Docker PID1 timestamp.
	stopSignals := forwardSignals(cmd.Process.Pid)
	defer stopSignals()
	writer.Close()
	captured := make(chan capture, 1)
	go func() { captured <- collect(reader, out, c.OutputLimit, c.PreviewLimit) }()
	waitErr := cmd.Wait()
	code, statusErr := exitCode(waitErr)
	if statusErr != nil {
		return rec, statusErr
	}
	rec.ExitCode = &code
	rec.FinishedAt = time.Now().UTC()
	got := awaitCapture(reader, captured, lingerGrace)
	rec.Bytes, rec.Stored = got.total, got.stored
	rec.Incomplete = !got.eof || got.err != nil
	if got.err != nil {
		rec.CaptureError = "capture failed"
	}
	if err := out.Sync(); err != nil {
		return rec, fmt.Errorf("job runner: sync output: %w", err)
	}
	rec.Digest = hex.EncodeToString(got.digest.Sum(nil))
	if err := verifyFile(out, rec.Digest, rec.Stored, c.OutputLimit); err != nil {
		return rec, err
	}
	rec.Preview, rec.PreviewMarked, rec.PreviewMarkerStart, rec.PreviewMarkerLength, rec.PreviewCancelledMarker = got.renderPreview(PreviewOptions{Artifact: c.Artifact, Stored: got.stored, ArtifactLimit: c.OutputLimit, StoredLines: got.storedLines(), Incomplete: rec.Incomplete, CaptureError: rec.CaptureError, PreviewLimit: c.PreviewLimit,
		PageEnd: func(start, max int64) int64 {
			if end, err := pageEndFromFile(out, start, max, c.PreviewLimit); err == nil {
				return end
			}
			return min(max, start+int64(c.PreviewLimit))
		}})
	rec.PreviewDigest = digestString(rec.Preview)
	rec.PreviewBytes = len(rec.Preview)
	rec.PreviewLines = PreviewLines(rec.Preview)
	if rec.PreviewBytes > c.PreviewLimit || rec.PreviewLines > MaxPreviewLines {
		return rec, errors.New("job runner: preview exceeds its bound")
	}
	if err := publishPreview(root, rec.Preview); err != nil {
		// The preview is an optional projection; its publication failure must
		// not turn a completed command into an unknown outcome. The collector
		// reports the missing preview as unavailable.
		rec.PreviewDigest, rec.PreviewBytes, rec.PreviewLines = "", 0, 0
	}
	if err := publish(root, &rec); err != nil {
		return rec, err
	}
	return rec, nil
}

func safeArtifactPath(path string) bool {
	if !utf8.ValidString(path) || !filepath.IsAbs(path) {
		return false
	}
	for _, r := range path {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '/' || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func verifyFile(f *os.File, digest string, size, limit int64) error {
	before, err := f.Stat()
	if err != nil {
		return err
	}
	if before.Size() > limit || before.Size() != size {
		return errors.New("job runner: output changed during capture")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, size))
	if err != nil {
		return err
	}
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if n != size || after.Size() != size || hex.EncodeToString(h.Sum(nil)) != digest {
		return errors.New("job runner: output changed during capture")
	}
	return nil
}

// pageEndFromFile finds the longest recommended page under the configured
// preview limits, using the same rendered-size accounting as PreviewBuffer.
func pageEndFromFile(f *os.File, start, max int64, previewLimit int) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return start, err
	}
	return pageEndFromReaderAt(f, info.Size(), start, max, previewLimit)
}

func pageEndFromReaderAt(r io.ReaderAt, size, start, max int64, previewLimit int) (int64, error) {
	if start < 0 {
		start = 0
	}
	if max > size {
		max = size
	}
	if previewLimit <= 0 {
		return start, nil
	}
	rawEnd := min(max, start+int64(previewLimit))
	if rawEnd <= start {
		return start, nil
	}
	raw := make([]byte, rawEnd-start)
	n, err := r.ReadAt(raw, start)
	if err != nil && err != io.EOF {
		return start, err
	}
	raw = raw[:n]
	return start + int64(pageEndForRaw(raw, previewLimit)), nil
}

func exitCode(err error) (int, error) {
	if err == nil {
		return 0, nil
	}
	var exited *exec.ExitError
	if errors.As(err, &exited) {
		if status, ok := exited.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal()), nil
		}
		if code := exited.ExitCode(); code >= 0 {
			return code, nil
		}
		return 0, errors.New("job runner: unavailable child status")
	}
	return 0, fmt.Errorf("job runner: wait did not establish child status: %w", err)
}

func forwardSignals(pid int) func() {
	signals := make(chan os.Signal, 4)
	done := make(chan struct{})
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-done:
				return
			case s := <-signals:
				if sig, ok := s.(syscall.Signal); ok {
					_ = syscall.Kill(-pid, sig)
				}
			}
		}
	}()
	return func() { signal.Stop(signals); close(done) }
}

func publishPreview(root *os.Root, preview string) error {
	f, err := root.OpenFile(PreviewFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, err = io.WriteString(f, preview)
	err = errors.Join(err, f.Chmod(0644))
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func publish(root *os.Root, rec *Record) error {
	// The preview is separate mutable evidence; the completion record keeps
	// only facts whose JSON remains within MetadataLimit.
	published := *rec
	published.Preview = ""
	data, err := json.Marshal(published)
	if err != nil {
		return err
	}
	if len(data) > MetadataLimit {
		return errors.New("job runner: completion exceeds metadata limit")
	}
	f, err := root.OpenFile("completion.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Chmod(0644))
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = root.Rename("completion.tmp", "completion.json"); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// awaitCapture returns the collector's capture once the foreground shell has
// exited. While a descendant still holds an inherited write end, it waits at
// most grace and then stops the collector, which keeps the bytes already in
// the pipe. Once every write end is closed, EOF is certain and it waits for the
// collector however late either is scheduled: a helper descheduled past grace
// must not report a capture that reached EOF as incomplete.
func awaitCapture(r *os.File, captured <-chan capture, grace time.Duration) capture {
	if writersRemain(r) {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case got := <-captured:
			return got
		case <-timer.C:
		}
		if writersRemain(r) && r.SetReadDeadline(time.Now()) != nil {
			r.Close()
		}
	}
	return <-captured
}

type capture struct {
	total, stored  int64
	storedNewlines int64
	storedLast     byte
	buffer         PreviewBuffer
	digest         hash.Hash
	eof            bool
	err            error
}

func collect(r io.Reader, w io.Writer, limit int64, preview int) capture {
	c := capture{buffer: NewPreviewBuffer(preview), digest: sha256.New()}
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			c.write(w, buf[:n], limit)
		}
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				c.eof = true
			case errors.Is(err, os.ErrDeadlineExceeded):
				c.drain(r, w, limit, buf)
			default:
				c.err = errors.Join(c.err, err)
			}
			return c
		}
	}
}
func (c *capture) write(w io.Writer, p []byte, limit int64) {
	c.total += int64(len(p))
	c.buffer.Observe(p)
	if c.err == nil && c.stored < limit {
		wanted := min(int64(len(p)), limit-c.stored)
		n, err := w.Write(p[:wanted])
		if n > 0 {
			c.storedNewlines += int64(bytes.Count(p[:n], []byte{'\n'}))
			c.storedLast = p[n-1]
		}
		c.stored += int64(n)
		if n > 0 && c.digest != nil {
			_, _ = c.digest.Write(p[:n])
		}
		if err == nil && int64(n) != wanted {
			err = io.ErrShortWrite
		}
		c.err = err
	}
}
func (c capture) preview(o PreviewOptions) string {
	return c.buffer.String(o)
}

func (c *capture) renderPreview(o PreviewOptions) (string, bool, int, int, string) {
	preview := c.buffer.String(o)
	return preview, c.buffer.marked, c.buffer.markerStart, c.buffer.markerLength, c.buffer.cancelledMarker
}

func (c capture) storedLines() int64 {
	if c.stored == 0 {
		return 0
	}
	if c.storedLast == '\n' {
		return c.storedNewlines
	}
	return c.storedNewlines + 1
}

func digestString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func utf8Prefix(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:max(0, limit)]
}
