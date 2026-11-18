package docker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

// record is the final report of a finished job and the hash of the spec it
// ran under. Once published it is the authority for the effect.
type record struct {
	Format int
	harness.Job
}

var errCaptureEvidence = errors.New("docker: command capture evidence unavailable")

// tap keeps the first limit bytes of a stream in w, counts every byte, and
// feeds the shared preview buffer. It never fails a write, so the stream is
// always read to its end.
type tap struct {
	w              io.Writer
	limit          int64
	artifact       string
	stored, total  int64
	storedNewlines int64
	storedLast     byte
	preview        jobrunner.PreviewBuffer
	werr           error
}

func (t *tap) Write(p []byte) (int, error) {
	t.total += int64(len(p))
	t.preview.Observe(p)
	if t.werr == nil && t.stored < t.limit {
		k := min(int64(len(p)), t.limit-t.stored)
		n, err := t.w.Write(p[:k])
		if n > 0 {
			t.storedNewlines += int64(bytes.Count(p[:n], []byte{'\n'}))
			t.storedLast = p[n-1]
		}
		t.stored += int64(n)
		if err == nil && int64(n) < k {
			err = io.ErrShortWrite
		}
		t.werr = err
	}
	return len(p), nil
}

func (t *tap) output() string {
	lines := int64(0)
	if t.stored > 0 {
		lines = t.storedNewlines
		if t.storedLast != '\n' {
			lines++
		}
	}
	return t.preview.String(jobrunner.PreviewOptions{Artifact: t.artifact, Stored: t.stored, ArtifactLimit: t.limit, StoredLines: lines})
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
	collected, err := x.collect(ctx, effect.ID, job)
	job = collected
	if err != nil && errors.Is(err, errCaptureEvidence) {
		job.Status = harness.JobUnknown
		job.Output = harness.ArtifactRef{Detail: err.Error()}
		return job, nil
	}
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
	// The durable receipt and artifact now carry the evidence. Remove the
	// per-effect capture directory; an unknown or failed outcome never reaches
	// this point and keeps it for inspection. A cleanup error is surfaced so a
	// later inspection of the valid receipt retries it.
	if err := x.cleanupCapture(effect.ID); err != nil {
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
	if err := x.cleanupCapture(effect.ID); err != nil {
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

func (x *Executor) cleanupCapture(id string) error {
	if err := os.RemoveAll(x.capture(id)); err != nil {
		return fmt.Errorf("docker: remove capture of effect %s: %w", id, err)
	}
	return nil
}

func (x *Executor) collect(ctx context.Context, id string, job harness.Job) (harness.Job, error) {
	if job.ID == "" || job.Status == harness.JobFailedToStart {
		return job, nil
	}
	if job.Status == harness.JobCancelled && job.StartedAt.IsZero() {
		job.Output.Detail = "job never started; no output captured"
		return job, nil
	}
	root, err := os.OpenRoot(x.capture(id))
	if err != nil {
		if job.Status == harness.JobCancelled {
			return x.cancelledCapture(nil, id, job)
		}
		return harness.Job{}, fmt.Errorf("docker: command capture directory: %w", err)
	}
	defer root.Close()
	rec, err := x.readCapture(root, id, job)
	if err != nil {
		if job.Status == harness.JobCancelled {
			return x.cancelledCapture(root, id, job)
		}
		return harness.Job{}, err
	}
	if err := x.publishCapture(root, id, rec); err != nil {
		return job, err
	}
	job.Output = captureArtifact(id, rec, rec.Stored, job.Status == harness.JobCancelled)
	job.Preview = x.capturePreview(root, id, rec, job)
	return job, nil
}

func (x *Executor) readCapture(root *os.Root, id string, job harness.Job) (jobrunner.Record, error) {
	data, err := jobrunner.ReadEvidence(root, "completion.json", jobrunner.MetadataLimit)
	if err != nil {
		return jobrunner.Record{}, fmt.Errorf("docker: command completion evidence: %w", err)
	}
	var rec jobrunner.Record
	if err = json.Unmarshal(data, &rec); err != nil {
		return jobrunner.Record{}, fmt.Errorf("docker: command completion evidence: %w", err)
	}
	if rec.Protocol != jobrunner.Protocol || rec.EffectID != id || rec.Spec != job.Spec || rec.Generation != 1 || rec.ExitCode == nil ||
		rec.Stored < 0 || rec.Bytes < rec.Stored || rec.Stored > x.cfg.OutputLimit ||
		rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt) || rec.FinishedAt.After(time.Now().UTC().Add(time.Second)) ||
		(!job.StartedAt.IsZero() && rec.StartedAt.Before(job.StartedAt.Add(-time.Second))) ||
		(!job.FinishedAt.IsZero() && rec.FinishedAt.After(job.FinishedAt.Add(time.Second))) {
		return jobrunner.Record{}, errors.New("docker: command completion contradicts bound process evidence or capture limits")
	}
	if job.Status != harness.JobCancelled && job.ExitCode != nil && *rec.ExitCode != *job.ExitCode {
		return jobrunner.Record{}, errors.New("docker: command completion exit status differs from the container")
	}
	return rec, nil
}

func (x *Executor) publishCapture(root *os.Root, id string, rec jobrunner.Record) error {
	path := filepath.Join(x.artifacts(), id, "output")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	if err = f.Chmod(0644); err == nil {
		h := sha256.New()
		var n int64
		n, err = jobrunner.CopyEvidence(root, "output", x.cfg.OutputLimit, f, h)
		if err == nil && (n != rec.Stored || hex.EncodeToString(h.Sum(nil)) != rec.Digest) {
			err = errors.Join(errCaptureEvidence, errors.New("docker: command output differs from its completion record"))
		}
		if err != nil && (errors.Is(err, jobrunner.ErrEvidenceSource) || errors.Is(err, errCaptureEvidence)) {
			err = errors.Join(errCaptureEvidence, err)
		}
	}
	if err := install(f, path, false, err); err != nil {
		return err
	}
	return syncDir(filepath.Dir(filepath.Dir(path)))
}

func (x *Executor) capturePreview(root *os.Root, id string, rec jobrunner.Record, job harness.Job) string {
	rel := id + "/output"
	if job.Status == harness.JobCancelled {
		if preview, err := jobrunner.ReadEvidence(root, jobrunner.PreviewFile, jobrunner.MaxPreviewBytes); err == nil && validPreview(preview, rec, x.cfg.PreviewLimit) {
			return cancelledProjection(string(preview), rec, archive.ArtifactsRoot+"/"+rel, x.cfg.PreviewLimit)
		}
		return partialUnavailablePreview(archive.ArtifactsRoot + "/" + rel)
	}
	preview, err := jobrunner.ReadEvidence(root, jobrunner.PreviewFile, jobrunner.MaxPreviewBytes)
	if err == nil && validPreview(preview, rec, x.cfg.PreviewLimit) {
		return string(preview)
	}
	return unavailablePreview(archive.ArtifactsRoot + "/" + rel)
}

func captureArtifact(id string, rec jobrunner.Record, stored int64, cancelled bool) harness.ArtifactRef {
	rel := id + "/output"
	var details []string
	if rec.CaptureError != "" {
		details = append(details, rec.CaptureError)
	}
	if rec.Incomplete {
		details = append(details, "foreground capture ended before inherited output pipes closed; only observed bytes are counted")
	}
	if rec.Bytes > rec.Stored {
		details = append(details, fmt.Sprintf("kept %d of %d bytes", rec.Stored, rec.Bytes))
	}
	if cancelled {
		details = append(details, "job was cancelled; output is partial")
	}
	return harness.ArtifactRef{Path: rel, Bytes: stored, Available: true,
		Complete: len(details) == 0 && rec.Bytes == rec.Stored, Detail: strings.Join(details, "; ")}
}

// publishOutput streams the captured output into the artifact without loading
// it whole. It is the partial-artifact path used when cancellation leaves no
// completion record.
func (x *Executor) publishOutput(root *os.Root, id string, limit int64) (int64, error) {
	return publishOutputEvidence(root, filepath.Join(x.artifacts(), id, "output"), limit)
}

func publishOutputEvidence(root *os.Root, path string, limit int64) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return 0, err
	}
	if err = f.Chmod(0644); err == nil {
		var n int64
		n, err = jobrunner.CopyEvidence(root, "output", limit, f, nil)
		if err == nil {
			if err = install(f, path, false, nil); err == nil {
				err = syncDir(filepath.Dir(filepath.Dir(path)))
			}
			return n, err
		}
	}
	return 0, install(f, path, false, err)
}

func (x *Executor) cancelledCapture(root *os.Root, id string, job harness.Job) (harness.Job, error) {
	rel := id + "/output"
	if root != nil {
		n, err := x.publishOutput(root, id, x.cfg.OutputLimit)
		if err != nil && !(errors.Is(err, jobrunner.ErrEvidenceSource) && errors.Is(err, os.ErrNotExist)) {
			return harness.Job{}, err
		}
		if err == nil && n > 0 {
			job.Output = harness.ArtifactRef{Path: rel, Bytes: n, Available: true, Detail: "job was cancelled; output is partial; no helper completion"}
		}
	}
	if job.Output.Detail == "" {
		job.Output.Detail = "job was cancelled; output is partial; no helper completion"
	}
	if root != nil {
		if preview, err := jobrunner.PreviewEvidence(root, "output", x.cfg.OutputLimit, x.cfg.PreviewLimit, archive.ArtifactsRoot+"/"+rel, x.cfg.OutputLimit, true); err == nil {
			job.Preview = preview
		}
	}
	if job.Preview == "" {
		job.Preview = partialUnavailablePreview(archive.ArtifactsRoot + "/" + rel)
	}
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
