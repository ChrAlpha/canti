package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// inputChecked is called after the checks of an input that may create the
// state directory and before the state is created; tests block a process
// there.
var inputChecked = func() {}

// submit admits one input through the host and, when it opened a run, drives
// it. A steer or queued follow-up only acknowledges: it prints the
// acknowledgement and exits without driving.
func (a *app) submit(ctx context.Context, input, goal, constraints string, explicitConstraints bool, spec host.Spec, url string, fresh bool, delivery harness.InputDelivery, once bool) (int, error) {
	if input != "" && !sqlite.ValidKey(input) {
		return exitUsage, errors.New("-input must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
	}
	if err := docker.CheckLayout(a.workspace, a.state); err != nil {
		return exitError, err
	}
	if _, err := os.Stat(filepath.Join(a.state, "canti.db")); errors.Is(err, fs.ErrNotExist) {
		if err := a.checkNewInput(ctx, goal, constraints, spec, url); err != nil {
			return submitCode(err), err
		}
	}
	inputChecked()
	drive, release := a.drive(ctx, once)
	defer release()
	b := a.binding(spec.Config.API, url)
	if a.remoteMode() {
		return a.remoteSubmit(ctx, hostrpc.Submission{InputID: input, Goal: goal, Constraints: constraints,
			ExplicitConstraints: explicitConstraints, Delivery: string(delivery), Fresh: fresh, Spec: hostserver.SpecOf(spec)}, b, once)
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return exitError, err
	}
	h, err := a.openHost(ctx, true)
	if err != nil {
		return exitError, err
	}
	defer h.Close()
	ack, err := h.Submit(ctx, host.Submission{InputID: input, Goal: goal, Constraints: constraints,
		ExplicitConstraints: explicitConstraints, Delivery: delivery, Fresh: fresh, Spec: spec, Binding: b, Now: time.Now()})
	switch {
	case errors.Is(err, host.ErrConflict):
		return exitError, err
	case err != nil:
		return submitCode(err), err
	case ack.Replayed:
		a.noteReplay(input, ack)
	}
	if !ack.Opened() {
		h.Close()
		return a.printInputAck(ack)
	}
	if ack.Replayed {
		h.Close()
		return a.report(ack.Snapshot, nil, exitActive, ack.Config)
	}
	a.log.Info("opened run", "run_id", ack.Run.ID, "input_id", ack.Run.InputID, "input_pos", ack.Run.InputPos, "fresh", ack.Run.Fresh)
	snap, err := drive(h, ack.Run.ID, b)
	h.Close()
	return a.report(snap, err, exitActive, ack.Config)
}

// printInputAck prints the acknowledgement of an input that opened no run, as
// one JSON object.
func (a *app) printInputAck(ack host.Ack) (int, error) {
	out := inputAck{InputID: ack.Input.InputID, Disposition: string(ack.Input.Disposition), Pos: ack.Input.Pos,
		Replayed: ack.Replayed, Differs: ack.Differs}
	return exitOK, a.encoder().Encode(out)
}

type inputAck struct {
	InputID     string   `json:"input_id,omitempty"`
	Disposition string   `json:"disposition,omitempty"`
	Pos         int64    `json:"pos"`
	RunID       string   `json:"run_id,omitempty"`
	Replayed    bool     `json:"replayed"`
	Differs     []string `json:"differs,omitempty"`
}

func (a *app) checkNewInput(ctx context.Context, goal, constraints string, spec host.Spec, url string) error {
	if err := host.Validate(ctx, a.state, host.Submission{Goal: goal, Constraints: constraints, Spec: spec}); err != nil {
		return err
	}
	return a.checkCredential(ctx, spec.Config, url)
}

// noteReplay says on stderr, for a replayed input, the settings the
// submission named otherwise, if any, which the input does not take: it keeps
// its own.
func (a *app) noteReplay(input string, ack host.Ack) {
	if len(ack.Differs) == 0 {
		return
	}
	what := fmt.Sprintf("input %s at position %d", input, ack.Input.Pos)
	if ack.Opened() {
		what = fmt.Sprintf("input %s names run %s", input, ack.Run.ID)
	}
	fmt.Fprintf(a.stderr, "canti: %s, which keeps its own settings; this submission differs in %s\n", what, strings.Join(ack.Differs, ", "))
}
