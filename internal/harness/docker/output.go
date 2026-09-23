package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/internal/harness"
)

type record struct {
	Preview string              `json:"preview"`
	Output  harness.ArtifactRef `json:"output"`
}

// tap keeps the first limit bytes of a stream in w, counts every byte, and
// retains the head and true tail for a preview. It never fails a write, so
// the stream is always read to its end.
type tap struct {
	w             io.Writer
	limit         int64
	previewLimit  int
	stored, total int64
	head, tail    []byte
	werr          error
}

func (t *tap) Write(p []byte) (int, error) {
	t.total += int64(len(p))
	if n := min(len(p), t.previewLimit-len(t.head)); n > 0 {
		t.head = append(t.head, p[:n]...)
	}
	t.tail = append(t.tail, p[max(0, len(p)-t.previewLimit):]...)
	if over := len(t.tail) - t.previewLimit; over > 0 {
		t.tail = t.tail[:copy(t.tail, t.tail[over:])]
	}
	if t.werr == nil && t.stored < t.limit {
		k := min(int64(len(p)), t.limit-t.stored)
		n, err := t.w.Write(p[:k])
		t.stored += int64(n)
		if err == nil && int64(n) < k {
			err = io.ErrShortWrite
		}
		t.werr = err
	}
	return len(p), nil
}

func (t *tap) preview() string {
	if t.total <= int64(t.previewLimit) {
		return string(t.head)
	}
	room := t.previewLimit - len(omitted(t.total))
	head := t.head[:room/2]
	for i := len(head) - 1; i >= max(0, len(head)-utf8.UTFMax); i-- {
		if utf8.RuneStart(head[i]) {
			if !utf8.FullRune(head[i:]) {
				head = head[:i]
			}
			break
		}
	}
	tail := t.tail[len(t.tail)-(room-room/2):]
	for i := 0; i < min(len(tail), utf8.UTFMax); i++ {
		if utf8.RuneStart(tail[i]) {
			tail = tail[i:]
			break
		}
	}
	return string(head) + omitted(t.total-int64(len(head)+len(tail))) + string(tail)
}

func omitted(n int64) string {
	return fmt.Sprintf("\n... %d bytes omitted ...\n", n)
}

func describe(path string, t *tap, logsErr error, status harness.JobStatus) (harness.ArtifactRef, bool) {
	ref := harness.ArtifactRef{Path: path}
	switch {
	case logsErr != nil:
		ref.Detail = "logs unavailable: " + logsErr.Error()
		return ref, false
	case t.werr != nil:
		ref.Detail = "artifact write failed: " + t.werr.Error()
		return ref, false
	}
	var notes []string
	if t.stored < t.total {
		notes = append(notes, fmt.Sprintf("kept %d of %d bytes", t.stored, t.total))
	}
	if status == harness.JobCancelled {
		notes = append(notes, "job was cancelled; output is partial")
	}
	ref.Bytes, ref.Available, ref.Complete, ref.Detail = t.stored, true, len(notes) == 0, strings.Join(notes, "; ")
	return ref, true
}

func verify(root string, ref harness.ArtifactRef) harness.ArtifactRef {
	if !ref.Available {
		return ref
	}
	fi, err := os.Stat(filepath.Join(root, ref.Path))
	switch {
	case err != nil:
		ref.Detail = "artifact missing: " + err.Error()
	case fi.Size() != ref.Bytes:
		ref.Detail = fmt.Sprintf("artifact has %d bytes, recorded %d", fi.Size(), ref.Bytes)
	default:
		return ref
	}
	ref.Available, ref.Complete = false, false
	return ref
}

// output returns the retained output of a finished job, capturing it on the
// first report. Only a stored artifact is recorded; a failed capture is
// retried on the next report.
func (x *Executor) output(ctx context.Context, id, containerID string, status harness.JobStatus) (string, harness.ArtifactRef, error) {
	meta := filepath.Join(x.dir(id), "output.json")
	var rec record
	data, err := os.ReadFile(meta)
	switch {
	case err == nil:
		err = json.Unmarshal(data, &rec)
	case errors.Is(err, fs.ErrNotExist):
		var persist bool
		if rec, persist, err = x.collect(ctx, id, containerID, status); err == nil && persist {
			data, _ = json.Marshal(rec)
			err = publish(meta, data, false)
		}
	}
	if err != nil {
		return "", harness.ArtifactRef{}, err
	}
	return rec.Preview, verify(x.artifacts(), rec.Output), nil
}

func (x *Executor) collect(ctx context.Context, id, containerID string, status harness.JobStatus) (record, bool, error) {
	rel := id + "/output"
	path := filepath.Join(x.artifacts(), rel)
	t := &tap{w: io.Discard, limit: x.cfg.OutputLimit, previewLimit: x.cfg.PreviewLimit}
	f, err := x.artifactFile(path)
	if err != nil {
		t.werr = err
	} else {
		t.w = f
	}
	cmd := exec.CommandContext(ctx, x.cfg.Docker, "logs", containerID)
	cmd.Stdout, cmd.Stderr = t, t
	logsErr := cmd.Run()
	if f != nil {
		if err := install(f, path, false, errors.Join(t.werr, logsErr)); logsErr == nil {
			t.werr = errors.Join(err, syncDir(x.artifacts()))
		}
	}
	if err := ctx.Err(); err != nil {
		return record{}, false, err
	}
	ref, persist := describe(rel, t, logsErr, status)
	return record{Preview: t.preview(), Output: ref}, persist, nil
}

func (x *Executor) artifactFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(filepath.Dir(path), ".tmp-*")
}
