package jobrunner

import (
	"errors"
	"io"
	"os"
	"syscall"
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
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
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
