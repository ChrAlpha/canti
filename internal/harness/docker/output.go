package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/internal/harness"
)

// record is the final report of a finished job and the hash of the spec it
// ran under. Once published it is the authority for the effect.
type record struct {
	harness.Job
	Spec string
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

// describe reports the artifact captured for job, keeping the detail job was
// derived with, such as why it never started.
func describe(path string, t *tap, logsErr error, job harness.Job) harness.ArtifactRef {
	ref := harness.ArtifactRef{Path: path}
	switch {
	case logsErr != nil:
		ref.Detail = "logs unavailable: " + logsErr.Error()
		return ref
	case t.werr != nil:
		ref.Detail = "artifact write failed: " + t.werr.Error()
		return ref
	}
	var notes []string
	if job.Output.Detail != "" {
		notes = append(notes, job.Output.Detail)
	}
	if t.stored < t.total {
		notes = append(notes, fmt.Sprintf("kept %d of %d bytes", t.stored, t.total))
	}
	if job.Status == harness.JobCancelled {
		notes = append(notes, "job was cancelled; output is partial")
	}
	ref.Bytes, ref.Available, ref.Complete, ref.Detail = t.stored, true, len(notes) == 0, strings.Join(notes, "; ")
	return ref
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

// finish captures the output of a finished job and records the job, with an
// unavailable artifact if the capture failed, because the report is final.
// Only once it is recorded is its container removed. A capture interrupted
// because ctx is done, or a record that cannot be published, reports an
// error and leaves the capture to the next report.
func (x *Executor) finish(ctx context.Context, id string, job harness.Job, spec string) (harness.Job, error) {
	job, err := x.collect(ctx, id, job)
	if err == nil {
		data, _ := json.Marshal(record{job, spec})
		err = publish(x.record(id), data, false)
	}
	if err != nil {
		return harness.Job{}, err
	}
	x.release(ctx, job.ID)
	job.Output = verify(x.artifacts(), job.Output)
	return job, nil
}

// load reads the effect's final record, if any. A record that is not exactly
// a complete record of a finished job is an error, never missing or final.
func (x *Executor) load(id string) (record, bool, error) {
	data, err := os.ReadFile(x.record(id))
	if errors.Is(err, fs.ErrNotExist) {
		return record{}, false, nil
	}
	var rec record
	if err == nil {
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err = d.Decode(&rec); err == nil {
			if _, end := d.Token(); end != io.EOF {
				err = errors.New("trailing data")
			}
		}
	}
	if err == nil && (rec.ID == "" || rec.Spec == "" || !terminal(rec.Status)) {
		err = errors.New("incomplete")
	}
	if err != nil {
		return record{}, false, fmt.Errorf("docker: record of effect %s: %w", id, err)
	}
	return rec, true, nil
}

// recorded returns the effect's final record, if any, retrying the removal
// of its container.
func (x *Executor) recorded(ctx context.Context, id string) (record, bool, error) {
	rec, done, err := x.load(id)
	if err != nil || !done {
		return record{}, false, err
	}
	x.release(ctx, rec.ID)
	rec.Output = verify(x.artifacts(), rec.Output)
	return rec, true, nil
}

// release removes a recorded job's container by the ID it was inspected
// under, never a container that later took its name. A failure leaves it to
// the next report.
func (x *Executor) release(ctx context.Context, containerID string) {
	_, _ = x.docker(ctx, "rm", "-v", containerID)
}

// sweep releases every container of this state whose effect already has a
// record naming it, so a removal that failed is retried by the next Bind even
// though the finished effect is never reported again.
func (x *Executor) sweep(ctx context.Context) {
	out, err := x.docker(ctx, "ps", "-aq", "--no-trunc", "--filter", "label=canti.state="+x.cfg.State,
		"--format", `{{.ID}} {{.Label "canti.effect"}}`)
	if err != nil {
		return
	}
	for line := range strings.Lines(string(out)) {
		id, effect, _ := strings.Cut(strings.TrimSpace(line), " ")
		if checkID(effect) != nil {
			continue
		}
		if rec, done, err := x.load(effect); err == nil && done && rec.ID == id {
			x.release(ctx, id)
		}
	}
}

func (x *Executor) record(id string) string { return filepath.Join(x.dir(id), "output.json") }

func (x *Executor) collect(ctx context.Context, id string, job harness.Job) (harness.Job, error) {
	rel := id + "/output"
	path := filepath.Join(x.artifacts(), rel)
	t := &tap{w: io.Discard, limit: x.cfg.OutputLimit, previewLimit: x.cfg.PreviewLimit}
	f, err := x.artifactFile(path)
	if err != nil {
		t.werr = err
	} else {
		t.w = f
	}
	call, cancel := context.WithTimeout(ctx, x.timeouts.transfer)
	defer cancel()
	cmd := x.command(call, "logs", job.ID)
	cmd.Stdout, cmd.Stderr = t, t
	logsErr := cmd.Run()
	if logsErr != nil && call.Err() != nil {
		logsErr = fmt.Errorf("no response within %v", x.timeouts.transfer)
	}
	if f != nil {
		if err := install(f, path, false, errors.Join(t.werr, logsErr)); logsErr == nil {
			t.werr = errors.Join(err, syncDir(x.artifacts()))
		}
	}
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	job.Output = describe(rel, t, logsErr, job)
	job.Preview = t.preview()
	return job, nil
}

func (x *Executor) artifactFile(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err == nil {
		err = f.Chmod(0o644)
	}
	return f, err
}
