package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// observeCommand admits evidence about a subject. It never starts or wakes a
// run and prints the acknowledgement of the admitted fact.
func observeCommand(fs *flag.FlagSet, a *app) action {
	source := fs.String("source", "", "named `source` of the evidence; required")
	subject := fs.String("subject", "", "`subject` the evidence is about; required")
	input := fs.String("input", "", "observation ID; a retry with the same ID names the same observation")
	text := fs.String("text", "", "evidence `text`; read from stdin when omitted")
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		o, code, err := observationInput(fs, *source, *subject, *input, *text)
		if err != nil {
			return code, err
		}
		return a.observe(ctx, o)
	}
}

// observationInput checks the observation flags and reads its text.
func observationInput(fs *flag.FlagSet, source, subject, input, text string) (host.Observation, int, error) {
	if source == "" || subject == "" {
		return host.Observation{}, exitUsage, errors.New("-source and -subject are required")
	}
	if input != "" && !sqlite.ValidKey(input) {
		return host.Observation{}, exitUsage, errors.New("-input must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
	}
	body, code, err := observationText(fs, text)
	if err != nil {
		return host.Observation{}, code, err
	}
	return host.Observation{InputID: input, Source: source, Subject: subject, Text: body}, exitOK, nil
}

func observationText(fs *flag.FlagSet, text string) (string, int, error) {
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == "text" })
	body := text
	if !set {
		data, err := io.ReadAll(io.LimitReader(os.Stdin, harness.ObservationTextBytes+1))
		if err != nil {
			return "", exitError, err
		}
		body = string(data)
	}
	switch {
	case body == "":
		return "", exitUsage, errors.New("evidence text is required, as -text or on stdin")
	case len(body) > harness.ObservationTextBytes:
		return "", exitUsage, fmt.Errorf("evidence text must be at most %d bytes", harness.ObservationTextBytes)
	}
	return body, exitOK, nil
}

// observe admits the observation through the host of the state directory.
func (a *app) observe(ctx context.Context, o host.Observation) (int, error) {
	if err := a.checkHostMode(); err != nil {
		return exitUsage, err
	}
	if a.remoteMode() {
		c, err := a.connect(ctx)
		if err != nil {
			return exitError, err
		}
		ack, err := c.Observe(ctx, hostrpc.Observation{InputID: o.InputID, Source: o.Source, Subject: o.Subject, Text: o.Text})
		if err != nil {
			return submitCode(err), err
		}
		return exitOK, a.encoder().Encode(inputAck{Pos: ack.Pos, Replayed: ack.Replayed})
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return exitError, err
	}
	h, err := a.openHost(ctx, true)
	if err != nil {
		return exitError, err
	}
	defer h.Close()
	ack, err := h.Observe(ctx, o)
	if err != nil {
		return submitCode(err), err
	}
	return exitOK, a.encoder().Encode(inputAck{InputID: ack.Input.InputID, Pos: ack.Input.Pos, Replayed: ack.Replayed})
}

// withdrawCommand takes back the named held input, or every held input, and
// prints each as one JSON object per line.
func withdrawCommand(fs *flag.FlagSet, a *app) action {
	input := fs.String("input", "", "held input `ID` to withdraw; omitted withdraws every held input")
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		if err := a.checkHostMode(); err != nil {
			return exitUsage, err
		}
		if *input != "" && !sqlite.ValidKey(*input) {
			return exitUsage, errors.New("-input must be 1 to 128 ASCII letters, digits, underscores, or hyphens")
		}
		if _, err := os.Stat(filepath.Join(a.state, "canti.db")); errors.Is(err, os.ErrNotExist) {
			return exitOK, nil
		}
		if a.remoteMode() {
			c, err := a.connect(ctx)
			if err != nil {
				return exitError, err
			}
			withdrawn, err := c.Withdraw(ctx, *input)
			if err != nil {
				return submitCode(err), err
			}
			return a.printWithdrawn(withdrawn)
		}
		h, err := a.openHost(ctx, false)
		if err != nil {
			return exitError, err
		}
		defer h.Close()
		withdrawn, err := h.Withdraw(ctx, *input)
		if err != nil {
			return submitCode(err), err
		}
		out := make([]hostrpc.Input, 0, len(withdrawn))
		for _, in := range withdrawn {
			out = append(out, hostserver.InputOf(in))
		}
		return a.printWithdrawn(out)
	}
}

func (a *app) printWithdrawn(withdrawn []hostrpc.Input) (int, error) {
	enc := a.encoder()
	for _, in := range withdrawn {
		line := map[string]any{"pos": in.Pos, "input_id": in.InputID, "authority": in.Authority,
			"delivery": in.Delivery, "text": in.Text}
		if err := enc.Encode(line); err != nil {
			return exitError, err
		}
	}
	return exitOK, nil
}

// cancelCommand cancels the accepted effect of a run without stopping it, and
// prints the resulting run state.
func cancelCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	hostFlags(fs, a)
	return func(ctx context.Context) (int, error) {
		if err := a.checkHostMode(); err != nil {
			return exitUsage, err
		}
		if a.remoteMode() {
			return a.remoteRun(ctx, func(c *hostrpc.Client, r hostrpc.Run) (int, error) {
				v, err := c.CancelEffect(ctx, r.RunID)
				if err != nil {
					return exitError, err
				}
				return exitOK, a.encoder().Encode(runState(v))
			})
		}
		h, err := a.openHost(ctx, false)
		if err != nil {
			return exitError, err
		}
		defer h.Close()
		run, _, err := h.Find(ctx, a.runID)
		if err != nil {
			return exitError, err
		}
		if _, err := h.CancelEffect(ctx, run.ID); err != nil {
			return exitError, err
		}
		v, err := h.View(ctx, run.ID)
		if err != nil {
			return exitError, err
		}
		return exitOK, a.encoder().Encode(runState(hostserver.RunOf(v, nil)))
	}
}

// runState is the compact state a cancel prints.
type runStateJSON struct {
	RunID      string `json:"run_id"`
	Status     string `json:"status"`
	Phase      string `json:"phase"`
	Step       int    `json:"step"`
	Cursor     string `json:"cursor"`
	StopReason string `json:"stop_reason,omitempty"`
	EffectID   string `json:"effect_id,omitempty"`
	Driver     string `json:"driver"`
	Drivable   bool   `json:"drivable"`
	Why        string `json:"why,omitempty"`
}

func runState(r hostrpc.Run) runStateJSON {
	return runStateJSON{RunID: r.RunID, Status: r.Status, Phase: r.Phase, Step: r.Step, Cursor: r.Cursor,
		StopReason: r.StopReason, EffectID: r.EffectID, Driver: r.Driver, Drivable: r.Drivable, Why: r.Why}
}
