package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

// PublishHistory publishes the projection of run runID that jobs read under
// root/runID: goal, constraints when set, steps/<step number in 8 digits>/
// with meta, command, and preview for completed steps, and result once the
// run is closed. meta names an available output under
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
	if err == nil && run.SessionID != "" {
		err = publishSession(ctx, j, dir, run)
	}
	steps, found := filepath.Join(dir, "steps"), false
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
	if err == nil && (s.Phase == core.PhaseReturned || s.Phase == core.PhaseStopped) {
		result := "phase: returned\n\n" + s.Decision.Payload
		if s.Phase == core.PhaseStopped {
			result = "phase: stopped\nreason: " + string(s.Reason) + "\n"
		}
		err = putFile(dir, "result", result)
	}
	return err
}

func publishSession(ctx context.Context, journal Journal, dir string, run Run) error {
	j, ok := journal.(SessionJournal)
	if !ok {
		return errors.New("harness: session journal is required for a session run")
	}
	if run.SessionSeq < 1 {
		return errors.New("harness: session sequence is required")
	}
	prior, err := j.SessionRuns(ctx, run.SessionID, run.SessionSeq, 1)
	if err != nil {
		return err
	}
	if len(prior) > 1 || len(prior) == 1 && (prior[0].SessionID != run.SessionID || prior[0].SessionSeq < 1 || prior[0].SessionSeq >= run.SessionSeq) {
		return errors.New("harness: invalid previous session run")
	}
	var previous string
	if len(prior) == 1 {
		previous = prior[0].ID
	}
	data, err := json.Marshal(struct {
		SessionID     string `json:"session_id"`
		Sequence      int64  `json:"sequence"`
		InputID       string `json:"input_id"`
		PreviousRunID string `json:"previous_run_id,omitempty"`
	}{run.SessionID, run.SessionSeq, run.InputID, previous})
	if err != nil {
		return err
	}
	return putFile(dir, "session", string(data)+"\n")
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
	if err := install(tmp, filepath.Join(dir, "steps")); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

// putStep publishes the records of r in steps unless they exist, writing
// them in a temporary directory of dir.
func putStep(dir, steps string, r StepRecord) error {
	target := filepath.Join(steps, archive.StepDir(r.Step))
	if found, err := exists(target, true); err != nil || found {
		for _, name := range [...]string{archive.MetaFile, archive.CommandFile, archive.PreviewFile} {
			if err == nil {
				_, err = exists(filepath.Join(target, name), false)
			}
		}
		return err
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
	for _, file := range [...]struct{ name, data string }{{archive.MetaFile, meta(r)}, {archive.CommandFile, r.Command}, {archive.PreviewFile, r.Observation.Preview}} {
		f, err := os.Create(filepath.Join(path, file.name))
		if err == nil {
			err = fill(f, file.data)
		}
		if err != nil {
			return err
		}
	}
	return syncDir(path)
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
	o := r.Observation
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
