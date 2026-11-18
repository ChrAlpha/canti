package jobrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"syscall"
)

var (
	// ErrEvidenceSource marks a missing, damaged or changing source file.
	ErrEvidenceSource = errors.New("job evidence: source")
	// ErrEvidenceDestination marks a failure writing a verified source to its
	// local destination. It is retryable and is not an unknown job outcome.
	ErrEvidenceDestination = errors.New("job evidence: destination")
)

// ReadEvidence opens mutable job evidence without following its final symlink
// or blocking on a FIFO. The caller supplies a rooted spool directory; bounded
// regular files are evidence, never authority to launch or settle an effect.
func ReadEvidence(root *os.Root, name string, limit int64) ([]byte, error) {
	if limit < 0 {
		return nil, errors.New("job evidence: invalid read limit")
	}
	entry, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !entry.Mode().IsRegular() || entry.Size() > limit {
		return nil, errors.New("job evidence: not a bounded regular file")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > limit || !os.SameFile(entry, before) {
		return nil, errors.New("job evidence: not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return nil, errors.New("job evidence: file changed during read")
	}
	entry, err = root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !entry.Mode().IsRegular() || !os.SameFile(entry, after) {
		return nil, errors.New("job evidence: file replaced during read")
	}
	return data, nil
}

// CopyEvidence streams bounded regular-file evidence into dst while hashing
// exactly the bytes written. It never follows a final symlink or blocks on a
// FIFO, and detects a file that changes or is replaced during the copy.
func CopyEvidence(root *os.Root, name string, limit int64, dst io.Writer, digest hash.Hash) (int64, error) {
	if limit < 0 {
		return 0, errors.New("job evidence: invalid read limit")
	}
	entry, err := root.Lstat(name)
	if err != nil {
		return 0, errors.Join(ErrEvidenceSource, err)
	}
	if !entry.Mode().IsRegular() || entry.Size() > limit {
		return 0, errors.Join(ErrEvidenceSource, errors.New("job evidence: not a bounded regular file"))
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return 0, errors.Join(ErrEvidenceSource, err)
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return 0, errors.Join(ErrEvidenceSource, err)
	}
	if !before.Mode().IsRegular() || before.Size() > limit || !os.SameFile(entry, before) {
		return 0, errors.Join(ErrEvidenceSource, errors.New("job evidence: not a bounded regular file"))
	}
	total, err := copyBounded(f, before.Size(), dst, digest)
	if err != nil {
		return total, err
	}

	after, err := f.Stat()
	if err != nil {
		return total, errors.Join(ErrEvidenceSource, err)
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return total, errors.Join(ErrEvidenceSource, errors.New("job evidence: file changed during read"))
	}
	entry, err = root.Lstat(name)
	if err != nil {
		return total, errors.Join(ErrEvidenceSource, err)
	}
	if !entry.Mode().IsRegular() || !os.SameFile(entry, after) {
		return total, errors.Join(ErrEvidenceSource, errors.New("job evidence: file replaced during read"))
	}
	return total, nil
}

func copyBounded(f *os.File, size int64, dst io.Writer, digest hash.Hash) (int64, error) {
	w := dst
	if digest != nil {
		w = io.MultiWriter(dst, digest)
	}
	buf := make([]byte, 32<<10)
	var total int64
	for total < size {
		want := min(int64(len(buf)), size-total)
		read, readErr := f.Read(buf[:want])
		if read > 0 {
			written, writeErr := w.Write(buf[:read])
			total += int64(written)
			if writeErr != nil {
				return total, errors.Join(ErrEvidenceDestination, writeErr)
			}
			if written != read {
				return total, errors.Join(ErrEvidenceDestination, io.ErrShortWrite)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, errors.Join(ErrEvidenceSource, errors.New("job evidence: file changed during read"))
			}
			return total, errors.Join(ErrEvidenceSource, readErr)
		}
		if read == 0 {
			return total, errors.Join(ErrEvidenceSource, errors.New("job evidence: file changed during read"))
		}
	}
	return total, nil
}

// HelperDigest returns the SHA-256 of a helper executable.
func HelperDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// PreviewEvidence renders a preview from bounded evidence without retaining
// the whole file. It is used for a cancelled command, whose projection must be
// generated from the captured bytes and structured cancellation state rather
// than by searching the command's text for a marker.
func PreviewEvidence(root *os.Root, name string, limit int64, previewLimit int, artifact string, artifactLimit int64, partial bool) (string, error) {
	if limit < 0 || previewLimit < MinPreviewBytes || previewLimit > MaxPreviewBytes {
		return "", errors.New("job evidence: invalid preview limit")
	}
	entry, err := root.Lstat(name)
	if err != nil {
		return "", err
	}
	if !entry.Mode().IsRegular() || entry.Size() > limit {
		return "", errors.New("job evidence: not a bounded regular file")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !before.Mode().IsRegular() || before.Size() > limit || !os.SameFile(entry, before) {
		return "", errors.New("job evidence: not a bounded regular file")
	}
	buffer := NewPreviewBuffer(previewLimit)
	total, newlines, last, err := readPreviewEvidence(f, before.Size(), &buffer)
	if err != nil {
		return "", err
	}

	after, err := f.Stat()
	if err != nil {
		return "", err
	}
	entry, err = root.Lstat(name)
	if err != nil {
		return "", err
	}
	if total != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !entry.Mode().IsRegular() || !os.SameFile(entry, after) {
		return "", errors.New("job evidence: file changed during read")
	}
	lines := newlines
	if total > 0 && last != '\n' {
		lines++
	}
	pageEnd := func(start, max int64) int64 {
		if end, pageErr := pageEndFromReaderAt(f, before.Size(), start, max, previewLimit); pageErr == nil {
			return end
		}
		return min(max, start+int64(previewLimit))
	}
	return buffer.String(PreviewOptions{Artifact: artifact, Stored: total, ArtifactLimit: artifactLimit, StoredLines: lines, Partial: partial, PreviewLimit: previewLimit, PageEnd: pageEnd}), nil
}

func readPreviewEvidence(f *os.File, size int64, buffer *PreviewBuffer) (total, newlines int64, last byte, err error) {
	maxRead := size
	if maxRead < 1<<63-1 {
		maxRead++
	}
	buf := make([]byte, 32<<10)
	for total < maxRead {
		want := min(int64(len(buf)), maxRead-total)
		n, readErr := f.Read(buf[:want])
		if n > 0 {
			total += int64(n)
			if total > size {
				return total, newlines, last, errors.New("job evidence: file changed during read")
			}
			newlines += int64(bytesCount(buf[:n], '\n'))
			last = buf[n-1]
			buffer.Observe(buf[:n])
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return total, newlines, last, readErr
		}
		if n == 0 {
			return total, newlines, last, errors.New("job evidence: file changed during read")
		}
	}
	return total, newlines, last, nil
}

func bytesCount(b []byte, c byte) int {
	n := 0
	for _, v := range b {
		if v == c {
			n++
		}
	}
	return n
}
