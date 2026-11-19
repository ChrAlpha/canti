package host

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	projectctx "github.com/ChrAlpha/canti/runtime/internal/project"
)

// Prepare resolves a new run's execution wiring and binds the project
// snapshot it would store. It writes no state and creates no directory; the
// host's admission remains the authoritative binding point.
func Prepare(ctx context.Context, state string, s Spec) (Spec, error) {
	c, err := ResolveExecution(ctx, s.Config, state)
	if err != nil {
		return s, SpecError{Err: err}
	}
	s.Config = c
	if c.NoProjectContext {
		s.Project = projectctx.Snapshot{}
		return s, nil
	}
	s.Project, err = BindProject(ctx, state, c)
	if err != nil {
		return s, SpecError{Err: fmt.Errorf("project context: %w", err)}
	}
	return s, nil
}

// BindProject reads the workspace instructions and skills of the run c would
// create. Ephemeral runs read the host workspace directly. Persistent runs
// execute the pinned helper inside the environment, so the snapshot is read
// from the same mount view as a job; no host-side mount mapping is used.
func BindProject(ctx context.Context, state string, c RunConfig) (projectctx.Snapshot, error) {
	if c.Ephemeral {
		return projectctx.Read(projectctx.Request{Workspace: c.Workspace, Root: "/workspace", Workdir: "/workspace"})
	}
	root, workdir, err := containerWorkspace(c)
	if err != nil {
		return projectctx.Snapshot{}, err
	}
	x, err := docker.NewAttached(AttachedConfig(c, state))
	if err != nil {
		return projectctx.Snapshot{}, err
	}
	return x.ReadProject(ctx, root, workdir)
}

// containerWorkspace returns the workspace mount point and job working
// directory as the container sees them. It uses mount metadata only; it never
// reads a host path through the mapping.
func containerWorkspace(c RunConfig) (root, workdir string, err error) {
	if c.Attachment == nil {
		return "", "", errors.New("persistent execution requires a stored attachment")
	}
	var roots []string
	for _, m := range c.Attachment.Mounts {
		if m.Type != "bind" || !m.Writable || !filepath.IsAbs(m.Source) {
			continue
		}
		rel, relErr := filepath.Rel(m.Source, c.Workspace)
		if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		roots = append(roots, filepath.Join(m.Target, rel))
	}
	if len(roots) != 1 {
		return "", "", errors.New("workspace requires one unambiguous writable environment bind mapping")
	}
	root = roots[0]
	if !path.IsAbs(root) || path.Clean(root) != root {
		return "", "", errors.New("workspace mapping is not an absolute clean path")
	}
	workdir = c.EnvironmentWorkdir
	if workdir == "" {
		workdir = root
	}
	if !path.IsAbs(workdir) || path.Clean(workdir) != workdir {
		return "", "", errors.New("environment workdir is outside the mapped workspace")
	}
	if workdir != root {
		rel, relErr := filepath.Rel(root, workdir)
		if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", "", errors.New("environment workdir is outside the mapped workspace")
		}
	}
	return root, workdir, nil
}
