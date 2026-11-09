package docker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// EnvironmentStatus is bounded operational metadata, not a container environment dump.
type EnvironmentStatus struct {
	Directory, Endpoint, DaemonID, ContainerID string
	AuthorityID                                string
	Incarnation                                uint64
	Gate, Active                               string
	Unsettled                                  int
	Running                                    bool
	Epoch                                      string
}

func fencePath(authority, ref string) (string, error) {
	if len(ref) != 32 || strings.Trim(ref, "0123456789abcdef") != "" {
		return "", errors.New("docker: invalid durable request reference")
	}
	return filepath.Join(authority, "requests", ref), nil
}
func (x *Attached) loadRequest(ref string) (string, requestFence, error) {
	dir, err := fencePath(x.authority(), ref)
	if err != nil {
		return "", requestFence{}, err
	}
	var r requestFence
	found, err := readJSON(filepath.Join(dir, "intent.json"), &r)
	if err != nil {
		return "", r, err
	}
	a := x.cfg.Attachment
	if !found || r.AuthorityID != a.AuthorityID || r.BackendID != a.DaemonID || r.EnvironmentID != a.ContainerID || r.Incarnation != a.Incarnation {
		return "", r, errors.New("docker: request reference contradicts attachment")
	}
	return dir, r, r.validate()
}
func (x *Attached) pruneRequests(s *environmentState) error {
	unresolved := make([]string, 0, len(s.Requests))
	for _, ref := range s.Requests {
		dir, r, err := x.loadRequest(ref)
		if err != nil {
			return err
		}
		done, err := requestSettled(dir, r)
		if err != nil {
			return err
		}
		if !done {
			unresolved = append(unresolved, ref)
		}
	}
	s.Requests = unresolved
	return nil
}
func (x *Attached) recoverControl(ctx context.Context, s *environmentState) error {
	if err := x.pruneRequests(s); err != nil {
		return err
	}
	if s.Pending != "" {
		dir, r, err := x.loadRequest(s.Pending)
		if err != nil {
			return err
		}
		done, err := requestSettled(dir, r)
		if err != nil {
			return err
		}
		if !done {
			s.Gate = "quarantined"
			return saveEnvironmentState(x.cfg.Attachment.Directory, *s)
		}
		c, err := x.backend.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
		if err != nil {
			return err
		}
		switch {
		case r.Kind == "environment_stop" && !c.State.Running:
			s.Gate = "stopped"
		case r.Kind == "environment_start" && c.State.Running && !c.State.Paused && !c.State.Restarting:
			s.Gate = "open"
			s.StartGeneration++
		default:
			s.Gate = "quarantined"
		}
		s.Pending = ""
	}
	if len(s.Requests) > 0 && s.Gate == "stopped" {
		s.Gate = "quarantined"
	}
	return saveEnvironmentState(x.cfg.Attachment.Directory, *s)
}

func (x *Attached) lifecycleRequest(ctx context.Context, s *environmentState, kind string, generation uint64) error {
	if s.Pending != "" {
		return gateError("quarantined")
	}
	a := x.cfg.Attachment
	generation = max(generation, 1)
	r := requestFence{Format: 1, AuthorityID: a.AuthorityID, Kind: kind, BackendID: a.DaemonID, EnvironmentID: a.ContainerID, TargetID: a.ContainerID, Incarnation: a.Incarnation, Generation: generation}
	dir, err := beginRequest(x.authority(), r)
	if err != nil {
		return err
	}
	ref := filepath.Base(dir)
	s.Pending = ref
	s.Requests = append(s.Requests, ref)
	if kind == "environment_stop" {
		s.LastStop = ref
	}
	if kind == "environment_stop" {
		s.Gate = "stopping"
	} else {
		s.Gate = "starting"
	}
	if err = saveEnvironmentState(a.Directory, *s); err != nil {
		return err
	}
	// The call is synchronous, single-send and bounded. Caller cancellation after
	// durable intent cannot turn it into a detached goroutine owned by nobody.
	call := context.WithoutCancel(ctx)
	op, path := "stop", "/containers/"+a.ContainerID+"/stop?t="+strconv.Itoa(int((x.cfg.StopGrace+time.Second-1)/time.Second))
	if kind == "environment_start" {
		op, path = "start", "/containers/"+a.ContainerID+"/start"
	}
	response := 0
	err = x.backend.api(call, op, http.MethodPost, path, nil, func(res *http.Response) error { response = res.StatusCode; return nil })
	if err == nil {
		if response == 0 {
			response = http.StatusNotModified
		}
		err = acknowledgeRequest(dir, r, response, false)
	}
	if err != nil {
		s.Gate = "quarantined"
		saved := saveEnvironmentState(a.Directory, *s)
		return errors.Join(err, saved)
	}
	return x.recoverControl(call, s)
}

// ControlEnvironment serializes lifecycle operations with command dispatch.
// Retire stays available under quarantine, but cannot forget a present old ID.
func ControlEnvironment(ctx context.Context, directory, operation string) (EnvironmentStatus, error) {
	a, err := ReadAttachment(directory)
	if err != nil {
		return EnvironmentStatus{}, err
	}
	x := &Attached{cfg: AttachedConfig{Attachment: a, StopGrace: 5 * time.Second}, backend: environmentEngine(a.Endpoint)}
	unlock, s, err := x.locked(ctx)
	if err != nil {
		return EnvironmentStatus{}, err
	}
	defer unlock()
	if operation == "retire" {
		return x.retire(ctx, &s)
	}
	c, err := x.backend.checkAttachment(ctx, a)
	if err != nil {
		return EnvironmentStatus{}, err
	}
	if err = x.recoverControl(ctx, &s); err != nil {
		return EnvironmentStatus{}, err
	}
	if operation == "inspect" {
		return x.status(ctx, s)
	}
	if s.Gate == "retired" || s.Gate == "retiring" || s.Gate == "detached" {
		return EnvironmentStatus{}, gateError(s.Gate)
	}
	switch operation {
	case "stop", "detach":
		if err = x.stopEnvironment(ctx, &s, operation, c); err != nil {
			return EnvironmentStatus{}, err
		}

	case "start":
		if s.Active != "" {
			if err = x.reconcileActive(ctx, &s, c); err != nil {
				return EnvironmentStatus{}, err
			}
		}
		if s.Active != "" || len(s.Requests) > 0 || s.Pending != "" || s.Gate == "quarantined" {
			return EnvironmentStatus{}, gateError("quarantined")
		}
		if err = x.lifecycleRequest(ctx, &s, "environment_start", s.Generation); err != nil {
			return EnvironmentStatus{}, err
		}
	default:
		return EnvironmentStatus{}, errors.New("docker: unknown environment operation")
	}
	if err = saveEnvironmentState(a.Directory, s); err != nil {
		return EnvironmentStatus{}, err
	}
	return x.status(ctx, s)
}
func (x *Attached) status(ctx context.Context, s environmentState) (EnvironmentStatus, error) {
	a := x.cfg.Attachment
	out := EnvironmentStatus{AuthorityID: a.AuthorityID, Directory: a.Directory, Endpoint: a.Endpoint, DaemonID: a.DaemonID, ContainerID: a.ContainerID, Incarnation: a.Incarnation, Gate: s.Gate, Active: s.Active, Unsettled: len(s.Requests)}
	c, err := x.backend.inspectPrepared(ctx, a.ContainerID)
	if err != nil {
		return out, err
	}
	out.Running, out.Epoch = c.State.Running, c.State.StartedAt.Format(time.RFC3339Nano)
	return out, nil
}
func (x *Attached) retire(ctx context.Context, s *environmentState) (EnvironmentStatus, error) {
	a := x.cfg.Attachment
	backend, err := x.backend.daemon(ctx)
	if err != nil {
		return EnvironmentStatus{}, err
	}
	if backend != a.DaemonID {
		return EnvironmentStatus{}, errors.New("docker: cannot retire against a different daemon")
	}
	s.Gate = "retiring"
	if err = saveEnvironmentState(a.Directory, *s); err != nil {
		return EnvironmentStatus{}, err
	}
	_, err = x.backend.inspectPrepared(ctx, a.ContainerID)
	if !errors.Is(err, errMissing) {
		if err == nil {
			err = fmt.Errorf("docker: remove exact old container %s with Docker/Dev Containers, then retry environment retire; designated volumes may survive but system-layer changes do not", a.ContainerID)
		}
		return EnvironmentStatus{}, err
	}
	retired := struct {
		Attachment  Attachment
		State       environmentState
		ConfirmedAt time.Time
	}{a, *s, time.Now().UTC()}
	dir := filepath.Join(x.authority(), "retired")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return EnvironmentStatus{}, err
	}
	path := filepath.Join(dir, strconv.FormatUint(a.Incarnation, 10)+".json")
	if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
		if err = saveJSON(path, retired, true); err != nil {
			return EnvironmentStatus{}, err
		}
	}
	if err = syncDir(x.authority()); err != nil {
		return EnvironmentStatus{}, err
	}
	s.Gate, s.Active, s.Pending, s.Requests = "retired", "", "", nil
	s.ActiveSpec, s.ActiveState = "", ""
	if err = saveEnvironmentState(a.Directory, *s); err != nil {
		return EnvironmentStatus{}, err
	}
	return EnvironmentStatus{AuthorityID: a.AuthorityID, Directory: a.Directory, Endpoint: a.Endpoint, DaemonID: a.DaemonID, ContainerID: a.ContainerID, Incarnation: a.Incarnation, Gate: "retired"}, nil
}

func (x *Attached) stopEnvironment(ctx context.Context, s *environmentState, operation string, observed preparedContainer) error {
	if s.Active != "" {
		owner, r, err := x.activeOwner(*s, observed)
		if err != nil {
			return err
		}
		if _, err = owner.cancel(ctx, s, &r); err != nil {
			return err
		}
	}
	if s.Pending != "" {
		return gateError("quarantined")
	}
	c, err := x.backend.inspectPrepared(ctx, x.cfg.Attachment.ContainerID)
	if err != nil {
		return err
	}
	if c.State.Running {
		if err = x.lifecycleRequest(ctx, s, "environment_stop", s.Generation); err != nil {
			return err
		}
	} else if len(s.Requests) > 0 {
		s.Gate = "quarantined"
	} else {
		s.Gate = "stopped"
	}
	if operation == "detach" {
		if len(s.Requests) > 0 || s.Active != "" {
			return gateError("quarantined")
		}
		s.Gate = "detached"
	}
	return nil
}
