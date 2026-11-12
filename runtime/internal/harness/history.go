package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

// PublishHistory publishes the projection of run runID that jobs read under
// root/runID: goal, constraints when set, steps/<step number in 8 digits>/
// with meta, command, purpose, and preview for completed steps, and result
// once the run is closed. meta names an available output under
// archive.ArtifactsRoot, where jobs read the artifacts.
//
// The projection is derived from the journal and never an authority. Each
// file and step directory is published once, atomically, and never
// rewritten; temporaries live in the run directory, which each call sweeps.
// A call adds whichever of the goal, the constraints, the latest completed
// step, and the result is missing, which suffices because a loop publishes
// before it freezes each request, and rebuilds a missing steps directory
// from every completed step; a step directory removed inside an existing
// steps directory is not detected. An entry it looks at that is not a
// regular file, or not a directory where one belongs, fails the call; a
// symlink is neither. Callers must not publish one run concurrently.
func PublishHistory(ctx context.Context, j Journal, root, runID string) error {
	run, err := j.Run(ctx, runID)
	var snap Snapshot
	var latest []StepRecord
	if err == nil {
		snap, err = j.Latest(ctx, runID)
	}
	if err == nil {
		latest, err = j.Recent(ctx, runID, 0, 1)
	}
	if err != nil {
		return err
	}
	return publishHistory(ctx, j, root, run, snap.State, latest)
}

// publishHistory is PublishHistory for run at s, whose latest completed
// step, if any, ends latest.
func publishHistory(ctx context.Context, j Journal, root string, run Run, s core.State, latest []StepRecord) error {
	if strings.Trim(run.ID, ".") == "" || strings.ContainsRune(run.ID, filepath.Separator) {
		return fmt.Errorf("harness: run ID %q cannot name a directory", run.ID)
	}
	dir := filepath.Join(root, run.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if _, err := exists(dir, true); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	for _, e := range entries {
		if err == nil && strings.HasPrefix(e.Name(), ".tmp-") {
			err = os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
	if err == nil {
		err = putFile(dir, "goal", run.Goal)
	}
	if err == nil && run.Constraints != "" {
		err = putFile(dir, "constraints", run.Constraints)
	}
	if err == nil {
		err = publishPrevious(ctx, j, dir, run)
	}
	steps, found := filepath.Join(dir, archive.StepsDir), false
	if err == nil {
		found, err = exists(steps, true)
	}
	switch {
	case err != nil:
	case !found:
		err = rebuild(ctx, j, dir, run.ID)
	case len(latest) > 0:
		err = putStep(dir, steps, latest[len(latest)-1])
	}
	if err == nil && s.Closed() {
		result := "phase: returned\n\n" + s.Decision.Payload
		if s.Phase == core.PhaseStopped {
			result = "phase: stopped\nreason: " + string(s.Reason) + "\n"
		}
		err = putFile(dir, "result", result)
	}
	return err
}

// publishPrevious records the previous run of the run's line in the run
// directory, or an empty record when it starts the line. The projection is
// derived from the journal, never an authority.
func publishPrevious(ctx context.Context, journal Journal, dir string, run Run) error {
	previous := ""
	if !run.Fresh {
		prior, _, err := journal.PreviousRuns(ctx, run.ID)
		if err != nil {
			return err
		}
		if len(prior) > 0 {
			previous = prior[0].Run.ID
		}
	}
	return putFile(dir, "previous", previous)
}

// publishDelivered installs exactly the input records a RequestFrozen
// rendered. The list is bounded by the request; publication never scans the
// archive root or journal history.
func publishDelivered(ctx context.Context, root string, inputs []Input) error {
	for _, in := range inputs {
		if err := installInputRecord(ctx, root, in); err != nil {
			return err
		}
	}
	return nil
}

// installInputRecord installs one input directory atomically. A complete
// directory is never rewritten; a partial directory left by an interrupted
// writer is replaced. The fixed per-position temporary is removed before use,
// so a stale temporary for this input is not left behind.
func installInputRecord(ctx context.Context, root string, in Input) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.Pos < 1 {
		return newIntegrityError("input position %d", in.Pos)
	}
	final := filepath.Join(root, archive.InputDir(in.Pos))
	tmp := filepath.Join(root, ".tmp-"+archive.InputDir(in.Pos))
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	files := inputFiles(in)
	if info, err := os.Lstat(final); err == nil {
		if !info.IsDir() {
			return newIntegrityError("published input record %s is not a directory", final)
		}
		complete, err := inputRecordComplete(ctx, root, in.Pos, files)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		if err := os.RemoveAll(final); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return err
	}
	for _, file := range files {
		if err := putFile(tmp, file.name, file.data); err != nil {
			return err
		}
	}
	if err := syncDir(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(root)
}

type inputFile struct{ name, data string }

func inputFiles(in Input) []inputFile {
	meta := archive.InputMeta{
		Authority: string(in.Authority), Delivery: string(in.Delivery), Run: in.AdmittedRunID,
		InputID: in.InputID, Source: in.Source, Subject: in.Subject,
	}
	if !in.Admitted.IsZero() {
		meta.Admitted = in.Admitted.UTC().Format(time.RFC3339Nano)
	}
	return []inputFile{{archive.MetaFile, meta.Encode()}, {"text", in.Text}}
}

// inputRecordComplete reports whether every file is already installed with
// the journal's bytes. A missing file is an incomplete directory; a present
// file with other bytes is an integrity conflict and is never overwritten.
func inputRecordComplete(ctx context.Context, root string, pos int64, files []inputFile) (bool, error) {
	reader := archive.Reader{Inputs: root}
	present := 0
	for _, file := range files {
		body, err := reader.InputFile(ctx, pos, file.name, len(file.data))
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case err != nil:
			return false, err
		}
		present++
		if string(body) != file.data {
			return false, newIntegrityError("published input record %s has conflicting bytes", filepath.Join(root, archive.InputDir(pos), file.name))
		}
	}
	return present == len(files), nil
}

// repairInput installs the archive record of one delivered input on a native
// read. An undelivered input is reported absent.
func (l *Loop) repairInput(ctx context.Context, pos int64) error {
	in, err := l.journal.InputAt(ctx, pos)
	if err != nil {
		return err
	}
	if in.DeliveredPos == 0 {
		return archive.ErrInputAbsent
	}
	return installInputRecord(ctx, l.cfg.Inputs, in)
}

// rebuild publishes the steps directory of the run in dir with every
// completed step and syncs the directory holding dir.
func rebuild(ctx context.Context, j Journal, dir, runID string) error {
	tmp, err := os.MkdirTemp(dir, ".tmp-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for before := int64(0); ; {
		page, err := j.Recent(ctx, runID, before, historyPage)
		if err != nil {
			return err
		}
		for _, r := range page {
			step := filepath.Join(tmp, archive.StepDir(r.Step))
			if err := os.Mkdir(step, 0o755); err != nil {
				return err
			}
			if err := writeStep(step, r); err != nil {
				return err
			}
		}
		if len(page) < historyPage || page[0].Step == 1 {
			break
		}
		before = page[0].Seq
	}
	if err := errors.Join(os.Chmod(tmp, 0o755), syncDir(tmp)); err != nil {
		return err
	}
	if err := install(tmp, filepath.Join(dir, archive.StepsDir)); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

// putStep publishes the records of r in steps unless they exist, writing
// them in a temporary directory of dir.
func putStep(dir, steps string, r StepRecord) error {
	target := filepath.Join(steps, archive.StepDir(r.Step))
	if found, err := exists(target, true); err != nil {
		return err
	} else if found {
		return repairStep(target, r)
	}
	tmp, err := os.MkdirTemp(dir, ".tmp-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := writeStep(tmp, r); err != nil {
		return err
	}
	return install(tmp, target)
}

// writeStep writes the records of r into the directory path and syncs it.
func writeStep(path string, r StepRecord) error {
	if err := os.Chmod(path, 0o755); err != nil {
		return err
	}
	files, err := stepFiles(r)
	if err != nil {
		return err
	}
	if r.retrieval() != nil {
		if err := os.Mkdir(filepath.Join(path, archive.EvidenceDir), 0o755); err != nil {
			return err
		}
	}
	for _, file := range files {
		f, err := os.Create(filepath.Join(path, file.name))
		if err == nil {
			err = fill(f, file.data)
		}
		if err != nil {
			return err
		}
	}
	if r.retrieval() != nil {
		if err := syncDir(filepath.Join(path, archive.EvidenceDir)); err != nil {
			return err
		}
	}
	return syncDir(path)
}

type recordFile struct{ name, data string }

func stepFiles(r StepRecord) ([]recordFile, error) {
	files := []recordFile{{archive.MetaFile, meta(r)}, {archive.CommandFile, r.Command}, {archive.PurposeFile, r.Purpose}, {archive.PreviewFile, r.presented().Preview}}
	retrieval := r.retrieval()
	if retrieval == nil {
		return files, nil
	}
	if len(retrieval.Sources) > archive.MaxSources || len(retrieval.Sources) != len(retrieval.Evidence) {
		return nil, newIntegrityError("retrieval evidence shape at sequence %d", r.Seq)
	}
	manifest := archive.EvidenceManifest{Seq: r.Seq, Query: retrieval.Query, Entries: retrieval.Entries,
		NextOffset: retrieval.NextOffset, NextCursor: retrieval.NextCursor, More: retrieval.More, Incomplete: retrieval.Incomplete,
		SkippedCount: retrieval.SkippedCount, Skipped: retrieval.Skipped, Error: retrieval.Error}
	for i, source := range retrieval.Sources {
		text := retrieval.Evidence[i]
		if len(text) != source.Length || len(text) > archive.MaxBytes || fmt.Sprintf("%x", sha256.Sum256([]byte(text))) != source.SHA256 {
			return nil, newIntegrityError("retrieval source %d at sequence %d", i, r.Seq)
		}
		name := archive.EvidencePath(i)
		manifest.Sources = append(manifest.Sources, archive.EvidenceSource{SourceRange: source, Source: i, File: name})
		files = append(files, recordFile{name, text})
	}
	body, err := json.Marshal(manifest)
	return append(files, recordFile{archive.EvidenceFile, string(body) + "\n"}), err
}

// Existing derived records can be repaired from committed facts. Any differing
// bytes fail integrity rather than silently replacing a historical observation.
func repairStep(dir string, r StepRecord) error {
	files, err := stepFiles(r)
	if err != nil {
		return err
	}
	createdDirectory := false
	if r.retrieval() != nil {
		evidence := filepath.Join(dir, archive.EvidenceDir)
		found, err := exists(evidence, true)
		if err != nil {
			return err
		}
		createdDirectory = !found
		if err := os.MkdirAll(evidence, 0o755); err != nil {
			return err
		}
	}
	for _, file := range files {
		target := filepath.Join(dir, file.name)
		found, err := exists(target, false)
		if err != nil {
			return err
		}
		if !found {
			if err := putFile(filepath.Dir(target), filepath.Base(target), file.data); err != nil {
				return err
			}
		}
		info, err := os.Lstat(target)
		if err != nil {
			return err
		}
		if info.Size() != int64(len(file.data)) {
			return newIntegrityError("published record %s has conflicting size", target)
		}
		body, err := os.ReadFile(target)
		if err != nil {
			return err
		}
		if string(body) != file.data {
			return newIntegrityError("published record %s has conflicting bytes", target)
		}
	}
	if createdDirectory {
		return syncDir(dir)
	}
	return nil
}

// putFile publishes data as dir/name unless it exists.
func putFile(dir, name, data string) error {
	target := filepath.Join(dir, name)
	if found, err := exists(target, false); err != nil || found {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := fill(f, data); err != nil {
		return err
	}
	return install(f.Name(), target)
}

func fill(f *os.File, data string) error {
	_, err := f.WriteString(data)
	if err == nil {
		err = f.Chmod(0o644)
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// install renames tmp, whose content is synced, to target and syncs the
// directory of target.
func install(tmp, target string) error {
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	return syncDir(filepath.Dir(target))
}

func meta(r StepRecord) string {
	o := r.presented()
	m := archive.StepMeta{Step: r.Step, Execution: string(o.Execution), ExitCode: o.ExitCode, Resolved: r.Resolved,
		OutputBytes: o.Output.Bytes, OutputComplete: o.Output.Complete, OutputDetail: o.Output.Detail}
	if o.Output.Available {
		m.Output = path.Join(archive.ArtifactsRoot, o.Output.Path)
	}
	return m.Encode()
}

// exists reports whether path exists, failing unless it is a directory when
// dir is set and a regular file otherwise; a symlink is neither.
func exists(path string, dir bool) (bool, error) {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	case dir && !info.IsDir():
		return false, fmt.Errorf("harness: step record %s is not a directory", path)
	case !dir && !info.Mode().IsRegular():
		return false, fmt.Errorf("harness: step record %s is not a regular file", path)
	}
	return true, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// repairSnapshot restores one requested historical observation from its
// authoritative journal record. Arbitrary shell opens do not invoke repair.
func (l *Loop) repairSnapshot(ctx context.Context, q archive.RetrievalQuery) error {
	target, ok := archive.ParseSnapshotPath(q.Path)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	record, err := l.journal.CompletionAtStep(ctx, target.RunID, target.Step)
	if err != nil {
		return err
	}
	retrieval := record.retrieval()
	if retrieval == nil {
		return errors.New("harness: snapshot target is not a retrieval")
	}
	if target.Source != nil && *target.Source >= len(retrieval.Sources) {
		return errors.New("harness: snapshot source does not exist")
	}
	dir := filepath.Join(l.cfg.History, target.RunID)
	if _, err := exists(dir, true); err != nil {
		return err
	}
	steps := filepath.Join(dir, archive.StepsDir)
	if _, err := exists(steps, true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(steps, 0o755); err != nil {
		return err
	}
	if err := putStep(dir, steps, record); err != nil {
		return err
	}
	return ctx.Err()
}
