package docker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// record is the final report of a finished job and the hash of the spec it
// ran under. Once published it is the authority for the effect.
type record struct {
	Format int
	harness.Job
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

// demux copies the standard output and standard error frames of a job's log
// stream to w in their order, as docker logs writes them; the daemon reports
// a failure while streaming in a frame of its own.
func demux(w io.Writer, r io.Reader) error {
	var h [8]byte
	for {
		if _, err := io.ReadFull(r, h[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		n := int64(binary.BigEndian.Uint32(h[4:]))
		switch h[0] {
		case 0, 1, 2:
			if _, err := io.CopyN(w, r, n); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return err
			}
		case 3:
			msg, _ := io.ReadAll(io.LimitReader(r, n))
			return errors.New(string(msg))
		default:
			return fmt.Errorf("log stream %d is not known", h[0])
		}
	}
}

// describe reports the artifact captured for job, keeping the detail job was
// derived with, such as why it never started.
func describe(path string, t *tap, job harness.Job) harness.ArtifactRef {
	ref := harness.ArtifactRef{Path: path}
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

// finish publishes output and the terminal receipt before releasing physical
// evidence. Any capture or publication failure leaves the owner recoverable.
func (x *Executor) finish(ctx context.Context, effect harness.Effect, job harness.Job) (harness.Job, error) {
	if err := job.Validate(effect); err != nil {
		return harness.Job{}, err
	}
	job, err := x.collect(ctx, effect.ID, job)
	if err == nil {
		err = job.Validate(effect)
	}
	if err == nil {
		err = x.receiptWitnesses(effect, job)
	}
	if err == nil {
		data, marshalErr := json.Marshal(record{Format: recordFormat, Job: job})
		err = marshalErr
		if err == nil {
			err = publish(x.record(effect.ID), data, true)
		}
	}
	if err != nil {
		return harness.Job{}, err
	}
	x.release(ctx, job.ID)
	return job, nil
}

// load rejects old, malformed and nonterminal receipts. Known outcomes are
// immutable; later artifact loss cannot rewrite the retained receipt.
func (x *Executor) load(id string) (record, bool, error) {
	var rec record
	found, err := readJSON(x.record(id), &rec)
	if err == nil && found {
		if rec.Format != recordFormat || !terminal(rec.Status) {
			err = errors.New("unsupported or incomplete record; use fresh state")
		} else {
			err = rec.Job.Validate(harness.Effect{ID: id, Spec: rec.Spec})
		}
	}
	if err != nil {
		return record{}, false, fmt.Errorf("docker: record of effect %s: %w", id, err)
	}
	return rec, found, nil
}

// recorded validates the accepted descriptor and both witnesses before even
// retrying release. A foreign receipt cannot authorize removal of an owner.
func (x *Executor) recorded(ctx context.Context, effect harness.Effect) (record, bool, error) {
	rec, done, err := x.load(effect.ID)
	if err != nil || !done {
		return rec, done, err
	}
	if err := rec.Job.Validate(effect); err != nil {
		return record{}, false, err
	}
	if err := x.receiptWitnesses(effect, rec.Job); err != nil {
		return record{}, false, err
	}
	if err := x.syncWitnesses(effect.ID); err != nil {
		return record{}, false, err
	}
	x.release(ctx, rec.ID)
	return rec, true, nil
}

// receiptWitnesses checks not only identity but the durable publication chain
// needed to establish the retained status. A missing latch or stop intent is
// corruption, even if the receipt by itself has a valid terminal shape.
func (x *Executor) receiptWitnesses(effect harness.Effect, job harness.Job) error {
	witness, err := x.readWitnesses(effect)
	if err != nil {
		return err
	}
	if job.Fenced != witness.fenced || witness.bound != (job.ID != "") || witness.owner.ID != job.ID {
		return fmt.Errorf("docker: receipt of effect %s contradicts its binding or cancellation witness", effect.ID)
	}
	valid := false
	switch job.Status {
	case harness.JobSucceeded, harness.JobFailed, harness.JobFailedToStart:
		valid = witness.latch == latchStart && !witness.stopping
	case harness.JobCancelled:
		if job.StartedAt.IsZero() {
			valid = witness.latch != latchStart && !witness.stopping
		} else {
			valid = witness.latch == latchStart && witness.stopping
		}
	}
	if !valid {
		return fmt.Errorf("docker: receipt of effect %s contradicts its launch or stop witness", effect.ID)
	}
	return nil
}

// release uses only the immutable physical ID; a later name owner is untouched.
func (x *Executor) release(ctx context.Context, jobID string) {
	if jobID != "" {
		_ = x.api(ctx, "rm", http.MethodDelete, "/containers/"+jobID+"?v=1", nil, nil)
	}
}

func (x *Executor) record(id string) string { return filepath.Join(x.dir(id), "output.json") }

func (x *Executor) collect(ctx context.Context, id string, job harness.Job) (harness.Job, error) {
	if job.ID == "" {
		return job, nil
	}
	rel := id + "/output"
	path := filepath.Join(x.artifacts(), rel)
	t := &tap{w: io.Discard, limit: x.cfg.OutputLimit, previewLimit: x.cfg.PreviewLimit}
	f, err := x.artifactFile(path)
	if err != nil {
		t.werr = err
	} else {
		t.w = f
	}
	var logsErr error
	if job.ID != "" {
		logsErr = x.api(ctx, "logs", http.MethodGet, "/containers/"+job.ID+"/logs?stdout=1&stderr=1", nil, func(r *http.Response) error {
			return demux(t, r.Body)
		})
	}
	if f != nil {
		if err := install(f, path, false, errors.Join(t.werr, logsErr)); logsErr == nil {
			t.werr = errors.Join(err, syncDir(x.artifacts()))
		}
	}
	if err := ctx.Err(); err != nil {
		return harness.Job{}, err
	}
	if err := errors.Join(logsErr, t.werr); err != nil {
		return harness.Job{}, err
	}
	job.Output = describe(rel, t, job)
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
