package harness

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"syscall"
	"unicode/utf8"
)

// ReadNotes reads at most limit bytes of the agent-writable notes file name
// inside workspace. Resolution stays inside the workspace, so a symlink that
// escapes it is an error, as is anything but a regular file; opening never
// blocks. A missing file is empty notes. truncated reports that the file is
// longer than limit; the text then ends before any rune the limit splits.
func ReadNotes(workspace, name string, limit int) (text string, truncated bool, err error) {
	if limit < 0 || limit == math.MaxInt {
		return "", false, fmt.Errorf("harness: notes limit %d is out of range", limit)
	}
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return "", false, err
	}
	defer root.Close()
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", false, err
	}
	if !info.Mode().IsRegular() {
		return "", false, fmt.Errorf("harness: notes %s is not a regular file (%s)", name, info.Mode().Type())
	}
	b, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return "", false, err
	}
	if len(b) <= limit {
		return string(b), false, nil
	}
	b = b[:limit]
	for i := len(b) - 1; i >= 0 && i >= len(b)-utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i]) {
			if !utf8.FullRune(b[i:]) {
				b = b[:i]
			}
			break
		}
	}
	return string(b), true, nil
}
