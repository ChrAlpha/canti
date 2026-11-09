package docker

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

type attachedReference struct {
	Format                                                     int
	EffectID, Spec, CommandHash, AuthorityID, Directory, State string
	Generation                                                 uint64
}
type attachedBinding struct {
	Reference     attachedReference
	ExecID, Epoch string
}

func (x *Attached) referencePath(id string) string {
	return filepath.Join(x.cfg.State, "jobs", id, "attached-reference.json")
}
func (x *Attached) readReference(e harness.Effect) (bool, error) {
	var ref attachedReference
	found, err := readJSON(x.referencePath(e.ID), &ref)
	if err != nil || !found {
		return found, err
	}
	a := x.cfg.Attachment
	if ref.Format != 1 || ref.EffectID != e.ID || ref.Spec != e.Spec || ref.CommandHash != harness.ConfigDigest([]byte(e.Command)) || ref.AuthorityID != a.AuthorityID || ref.Directory != a.Directory || ref.State != x.cfg.State || ref.Generation == 0 {
		return false, errors.New("docker: local execution reference contradicts its acceptance")
	}
	return true, nil
}
func (x *Attached) mirrorReference(r attachedRecord) error {
	ref := attachedReference{Format: 1, EffectID: r.Effect.ID, Spec: r.Effect.Spec, CommandHash: harness.ConfigDigest([]byte(r.Effect.Command)), AuthorityID: x.cfg.Attachment.AuthorityID, Directory: x.cfg.Attachment.Directory, State: x.cfg.State, Generation: r.Generation}
	path := x.referencePath(r.Effect.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	var saved attachedReference
	found, err := readJSON(path, &saved)
	if err != nil {
		return err
	}
	if found {
		if saved != ref {
			return errors.New("docker: local command reference has another generation")
		}
	} else if err = saveJSON(path, ref, true); err != nil {
		return err
	}
	if r.ExecID != "" {
		bound := attachedBinding{Reference: ref, ExecID: r.ExecID, Epoch: r.Epoch}
		var old attachedBinding
		bindingPath := filepath.Join(filepath.Dir(path), "attached-binding.json")
		found, err = readJSON(bindingPath, &old)
		if err != nil {
			return err
		}
		if found {
			if old != bound {
				return errors.New("docker: local physical binding has another owner")
			}
		} else if err = saveJSON(bindingPath, bound, true); err != nil {
			return err
		}
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	if err = syncDir(filepath.Dir(filepath.Dir(path))); err != nil {
		return err
	}
	return syncDir(x.cfg.State)
}
func (a Attachment) sameIdentity(other Attachment) bool {
	return a.AuthorityID == other.AuthorityID && a.Directory == other.Directory && a.Endpoint == other.Endpoint && a.DaemonID == other.DaemonID && a.ContainerID == other.ContainerID && a.Incarnation == other.Incarnation
}

type attachedLaunch struct {
	Binding      attachedBinding
	Request      string
	DispatchedAt time.Time
}

func (x *Attached) mirrorLaunch(r attachedRecord) error {
	path := filepath.Join(filepath.Dir(x.referencePath(r.Effect.ID)), "attached-launch.json")
	launch := attachedLaunch{Binding: attachedBinding{Reference: attachedReference{Format: 1, EffectID: r.Effect.ID, Spec: r.Effect.Spec, CommandHash: harness.ConfigDigest([]byte(r.Effect.Command)), AuthorityID: x.cfg.Attachment.AuthorityID, Directory: x.cfg.Attachment.Directory, State: x.cfg.State, Generation: r.Generation}, ExecID: r.ExecID, Epoch: r.Epoch}, Request: r.Launch, DispatchedAt: r.DispatchedAt}
	var old attachedLaunch
	found, err := readJSON(path, &old)
	if err != nil {
		return err
	}
	if found {
		if old != launch {
			return errors.New("docker: local launch latch contradicts command authority")
		}
		return syncDir(filepath.Dir(path))
	}
	return saveJSON(path, launch, true)
}
func (x *Attached) validateLocalBinding(r attachedRecord) error {
	dir := filepath.Dir(x.referencePath(r.Effect.ID))
	var bound attachedBinding
	found, err := readJSON(filepath.Join(dir, "attached-binding.json"), &bound)
	if err != nil {
		return err
	}
	if found && (bound.ExecID != r.ExecID || bound.Epoch != r.Epoch || bound.Reference.Generation != r.Generation || bound.Reference.Spec != r.Effect.Spec) {
		return errors.New("docker: local physical owner contradicts command authority")
	}
	var launch attachedLaunch
	found, err = readJSON(filepath.Join(dir, "attached-launch.json"), &launch)
	if err != nil {
		return err
	}
	if found && (launch.Request != r.Launch || !launch.DispatchedAt.Equal(r.DispatchedAt) || launch.Binding.ExecID != r.ExecID || launch.Binding.Reference.Generation != r.Generation) {
		return errors.New("docker: local launch latch is missing from command authority")
	}
	return nil
}
