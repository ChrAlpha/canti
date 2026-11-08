package docker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

const recordFormat = 2

// ownership is either the immutable physical binding, or an identity-only
// cancellation fence. A fence never asserts that an owner is absent.
type ownership struct {
	Format                          int
	EffectID, Spec, CommandHash, ID string
}

func ownerOf(effect harness.Effect, id string) ownership {
	return ownership{recordFormat, effect.ID, effect.Spec, harness.ConfigDigest([]byte(effect.Command)), id}
}

func readJSON(path string, value any) (bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return false, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return false, errors.New("trailing data")
	}
	return true, nil
}

func (x *Executor) ownership(effect harness.Effect, name string) (ownership, bool, error) {
	var saved ownership
	found, err := readJSON(filepath.Join(x.dir(effect.ID), name), &saved)
	if err == nil && found {
		want := ownerOf(effect, saved.ID)
		if saved != want || (name == "binding.json" || name == "stop") && saved.ID == "" || name == "cancel" && saved.ID != "" {
			err = errors.New("unsupported or mismatched identity; use fresh state")
		}
	}
	if err != nil {
		return ownership{}, false, fmt.Errorf("docker: %s of effect %s: %w", name, effect.ID, err)
	}
	return saved, found, nil
}

// saveOwnership publishes the file and syncs its effect directory, then the
// parent jobs directory, before a caller can consume the launch latch.
func (x *Executor) saveOwnership(name string, owner ownership) error {
	data, err := json.Marshal(owner)
	if err == nil {
		err = publish(filepath.Join(x.dir(owner.EffectID), name), data, true)
	}
	if err != nil {
		return err
	}
	return x.syncWitnesses(owner.EffectID)
}

func (x *Executor) binding(effect harness.Effect, c container) error {
	saved, found, err := x.ownership(effect, "binding.json")
	if err != nil {
		return err
	}
	if found {
		if saved.ID != c.ID {
			return fmt.Errorf("%w: effect %s owner %s replaced by %s", harness.ErrJobLost, effect.ID, saved.ID, c.ID)
		}
		return x.syncWitnesses(effect.ID)
	}
	return x.saveOwnership("binding.json", ownerOf(effect, c.ID))
}

func (x *Executor) fence(effect harness.Effect) error {
	_, found, err := x.ownership(effect, "cancel")
	if err != nil {
		return err
	}
	if found {
		return x.syncWitnesses(effect.ID)
	}
	return x.saveOwnership("cancel", ownerOf(effect, ""))
}

// A preceding publication may have failed after making its name visible.
// Retry directory synchronization before relying on that witness for an effect.
func (x *Executor) syncWitnesses(id string) error {
	if err := syncDir(x.dir(id)); err != nil {
		return err
	}
	return syncDir(filepath.Dir(x.dir(id)))
}

func (x *Executor) stopRequested(effect harness.Effect, id string) error {
	saved, found, err := x.ownership(effect, "stop")
	if err != nil {
		return err
	}
	if found {
		if saved.ID != id {
			return fmt.Errorf("docker: stop witness for effect %s has a different owner", effect.ID)
		}
		return x.syncWitnesses(effect.ID)
	}
	return x.saveOwnership("stop", ownerOf(effect, id))
}

type witnesses struct {
	owner                   ownership
	bound, fenced, stopping bool
	latch                   string
}

// readWitnesses validates the complete publication chain without backend I/O.
func (x *Executor) readWitnesses(effect harness.Effect) (w witnesses, err error) {
	if w.owner, w.bound, err = x.ownership(effect, "binding.json"); err != nil {
		return w, err
	}
	if _, w.fenced, err = x.ownership(effect, "cancel"); err != nil {
		return w, err
	}
	stopped, found, err := x.ownership(effect, "stop")
	if err != nil {
		return w, err
	}
	w.stopping = found
	if found && (!w.bound || !w.fenced || stopped.ID != w.owner.ID) {
		return w, fmt.Errorf("docker: stop witness of effect %s contradicts its binding or fence", effect.ID)
	}
	if w.latch, err = readLatch(x.dir(effect.ID)); err != nil {
		return w, err
	}
	if w.latch != "" && !w.bound {
		return w, fmt.Errorf("docker: effect %s has a latch without its binding witness", effect.ID)
	}
	if w.latch == latchCancel && !w.fenced {
		return w, fmt.Errorf("docker: effect %s has a cancellation latch without its fence", effect.ID)
	}
	return w, nil
}
