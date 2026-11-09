package host

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
)

// CheckExecutionMode checks the closed choice for a new run and stored wiring.
func CheckExecutionMode(c RunConfig) error {
	if c.Ephemeral == (c.Environment != "") {
		return errors.New("select exactly one of -environment DIRECTORY or -ephemeral")
	}
	if c.Ephemeral {
		if c.EnvironmentWorkdir != "" || c.Attachment != nil {
			return errors.New("environment workdir and attachment require -environment")
		}
		return nil
	}
	if c.Image != "" || c.Network != "" || c.Memory != "" || c.CPUs != "" || c.Pids != 0 {
		return errors.New("per-command container resources require -ephemeral; provision environment resources externally")
	}
	if !filepath.IsAbs(c.Environment) || filepath.Clean(c.Environment) != c.Environment {
		return errors.New("environment must be an absolute canonical directory")
	}
	if c.Attachment != nil && c.Attachment.Directory != c.Environment {
		return errors.New("stored attachment does not match environment directory")
	}
	return nil
}

// ResolveExecution pins a new run's environment before admission. Existing runs
// reconstruct from stored facts instead; input replay must precede this call.
func ResolveExecution(ctx context.Context, c RunConfig, state string) (RunConfig, error) {
	state, err := CanonicalState(c.Workspace, state)
	if err != nil {
		return c, err
	}
	if err := CheckExecutionMode(c); err != nil {
		return c, err
	}
	if c.Ephemeral {
		return c, docker.CheckConfig(ExecutorConfig(c, state))
	}
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	a, err := docker.ReadAttachment(c.Environment)
	if err != nil {
		return c, err
	}
	workdir, err := docker.EnvironmentWorkdir(call, a, c.Workspace, state, c.EnvironmentWorkdir)
	if err != nil {
		return c, err
	}
	c.Environment, c.Attachment, c.EnvironmentWorkdir = a.Directory, &a, workdir
	_, err = docker.NewAttached(AttachedConfig(c, state))
	return c, err
}

// AttachedConfig contains only committed execution wiring. Construction and
// specification never contact Docker or read the attachment directory.
func AttachedConfig(c RunConfig, state string) docker.AttachedConfig {
	var a docker.Attachment
	if c.Attachment != nil {
		a = *c.Attachment
	}
	return docker.AttachedConfig{Attachment: a, Workspace: c.Workspace, State: state, Workdir: c.EnvironmentWorkdir,
		OutputLimit: c.OutputLimit, PreviewLimit: PromptLimits(c).Preview}
}

func persistentEnvironment(c RunConfig, b harness.Budget) string {
	timeout := "unlimited"
	if b.JobTimeout > 0 {
		timeout = b.JobTimeout.String()
	}
	identity := "selected environment"
	if c.Attachment != nil {
		identity = fmt.Sprintf("container %s, attachment incarnation %d", c.Attachment.ContainerID, c.Attachment.Incarnation)
	}
	return fmt.Sprintf("Commands run in fresh POSIX shells in one persistent Linux development environment (%s), working directory %s. Files, installed tools, HOME, PATH and background services persist across commands, runs, sessions and context resets. If configured, the initialization file is sourced for each command. HOME and PATH start from the stored configuration; shell-local exports and directory changes do not carry between commands. Background services must use separate logs and streams. Command runtime limit: %s. Cancellation or timeout stops the whole environment, including services; explicit environment start retains files and opens it again. An unknown result requires inspection or resolution. Archive addresses under /canti/runs, /canti/artifacts and /canti/imports are logical native retrieval addresses, not shell mounts. Use canti:retrieve and canti:focus for history and frozen evidence.", identity, c.EnvironmentWorkdir, timeout)
}
