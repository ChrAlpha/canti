package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/project"
)

const (
	projectReadTimeout        = 10 * time.Second
	projectReadHelperDeadline = 9 * time.Second
	projectReadOutput         = 4 << 20
)

// ReadProject reads the workspace project context through the pinned canti-job
// helper. The helper runs under the environment's user and command
// environment, and resolves paths in the same container mount view as a job.
// It is read-only: no job record, spool directory, or foreground slot is
// created, and a stopped or quarantined environment is refused by the same
// readiness checks a launch uses.
func (x *Attached) ReadProject(ctx context.Context, root, workdir string) (project.Snapshot, error) {
	call, cancel := context.WithTimeout(ctx, projectReadTimeout)
	defer cancel()
	a := x.cfg.Attachment
	// Revalidate the attachment identity at exec time, as a launch does, and
	// prune only acknowledged lifecycle requests before deciding readiness.
	c, err := x.checkAttachment(call, a)
	if err != nil {
		return project.Snapshot{}, err
	}
	if _, err = observedMapping(c, x.cfg.Workspace, x.cfg.State, x.cfg.Workdir); err != nil {
		return project.Snapshot{}, err
	}
	_, state, err := readEnvironment(a.Directory)
	if err != nil {
		return project.Snapshot{}, err
	}
	owner := &Attached{cfg: AttachedConfig{Attachment: a}}
	if err = owner.pruneRequests(&state); err != nil {
		return project.Snapshot{}, err
	}
	if state.Gate != "open" {
		return project.Snapshot{}, gateError(state.Gate)
	}
	if state.Pending != "" || len(state.Requests) > 0 {
		return project.Snapshot{}, gateError("quarantined")
	}
	if !c.ready() {
		return project.Snapshot{}, errors.New("docker: environment must be running and ready before reading project context")
	}
	mounts := make([]project.ContainerMount, 0, len(a.Mounts))
	for _, m := range a.Mounts {
		if m.Target != "" {
			mounts = append(mounts, project.ContainerMount{Target: m.Target, Readable: m.Type == "bind" && m.Source != ""})
		}
	}
	mountData, err := json.Marshal(mounts)
	if err != nil {
		return project.Snapshot{}, err
	}
	request := execRequest{User: a.User, WorkingDir: x.cfg.Workdir, Env: commandEnv(a),
		Cmd:          []string{a.Helper, "-project-read", "-root", root, "-workdir", workdir, "-mounts", string(mountData), "-deadline", projectReadHelperDeadline.String()},
		AttachStdout: true, AttachStderr: true}
	var created struct{ ID string }
	if err := x.api(call, "exec-create", http.MethodPost, "/containers/"+a.ContainerID+"/exec", request, decodeResponse(&created)); err != nil {
		return project.Snapshot{}, err
	}
	if len(created.ID) != 64 {
		return project.Snapshot{}, errors.New("docker: project reader returned an invalid exec ID")
	}
	startEpoch := c.State.StartedAt.Format(time.RFC3339Nano)
	after, err := x.inspectPrepared(call, a.ContainerID)
	if err != nil {
		return project.Snapshot{}, err
	}
	if !after.ready() || after.State.StartedAt.Format(time.RFC3339Nano) != startEpoch {
		return project.Snapshot{}, errors.New("docker: environment start epoch changed before project read")
	}
	var stdout, stderr bytes.Buffer
	err = x.api(call, "start", http.MethodPost, "/exec/"+created.ID+"/start", struct{ Detach, Tty bool }{false, false}, func(res *http.Response) error {
		return readExecStream(call, res.Body, &stdout, &stderr, projectReadOutput)
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(call.Err(), context.DeadlineExceeded) {
			return project.Snapshot{}, fmt.Errorf("docker: project reader did not finish within %v", projectReadTimeout)
		}
		return project.Snapshot{}, err
	}
	if err := call.Err(); err != nil {
		return project.Snapshot{}, fmt.Errorf("docker: project reader did not finish within %v", projectReadTimeout)
	}
	var metadata execMetadata
	if err := x.api(call, "exec-inspect", http.MethodGet, "/exec/"+created.ID+"/json", nil, decodeResponse(&metadata)); err != nil {
		return project.Snapshot{}, err
	}
	if metadata.ExitCode == nil || *metadata.ExitCode != 0 {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = "no diagnostic"
		}
		return project.Snapshot{}, fmt.Errorf("docker: project reader failed: %s", detail)
	}
	var snapshot project.Snapshot
	if err := json.Unmarshal(stdout.Bytes(), &snapshot); err != nil {
		return project.Snapshot{}, fmt.Errorf("docker: project reader returned invalid JSON: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return project.Snapshot{}, fmt.Errorf("docker: project reader returned an invalid snapshot: %w", err)
	}
	return snapshot, nil
}

// readExecStream reads Docker's multiplexed exec output, separating stdout and
// stderr and refusing output over limit.
func readExecStream(ctx context.Context, body io.Reader, stdout, stderr *bytes.Buffer, limit int) error {
	total := 0
	header := make([]byte, 8)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(body, header); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		n := int(binary.BigEndian.Uint32(header[4:]))
		if n < 0 || total+n > limit {
			return fmt.Errorf("docker: project reader output exceeds %d bytes", limit)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(body, data); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		total += n
		switch header[0] {
		case 1:
			stdout.Write(data)
		case 2:
			stderr.Write(data)
		default:
			return fmt.Errorf("docker: project reader used unknown stream %d", header[0])
		}
	}
}
