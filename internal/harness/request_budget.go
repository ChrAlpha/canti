package harness

import (
	"fmt"
	"math"

	"github.com/ChrAlpha/canti/core"
)

// RequestInputEstimate deliberately counts every serialized byte plus framing
// as an input token. This is a conservative admission estimate for the text
// Messages adapter, not provider telemetry or a billing count. A deployment
// supplies its model's context allowance; actual usage comes from responses.
func RequestInputEstimate(req Request) int64 { return int64(len(req.Body)) + 256 }

func (l *Loop) admitRequest(req Request) error {
	if len(req.Body) > l.cfg.RequestBytes {
		return fmt.Errorf("harness: serialized request is %d bytes; limit %d", len(req.Body), l.cfg.RequestBytes)
	}
	output := l.outputReserve(req)
	if output < 1 || int64(output) > math.MaxInt64-RequestInputEstimate(req) {
		return fmt.Errorf("harness: invalid output token reserve")
	}
	if l.cfg.ContextTokens > 0 && (output > l.cfg.ContextTokens || RequestInputEstimate(req) > int64(l.cfg.ContextTokens-output)) {
		return fmt.Errorf("harness: estimated input %d plus reserved output %d exceeds context allowance %d tokens", RequestInputEstimate(req), output, l.cfg.ContextTokens)
	}
	return nil
}

// AdmitInitial checks the serialized envelope before a CLI creates state. The
// possible session navigation is reserved even before Submit assigns a session.
// Freeze is pure: it does not send a request or acquire credentials.
func AdmitInitial(cfg LoopConfig, run Run, limits core.Limits, model Model) error {
	cfg, err := normalize(cfg)
	if err != nil {
		return err
	}
	state, err := core.NewState(run.ID, limits)
	if err != nil {
		return err
	}
	loop := Loop{cfg: cfg}
	f := loop.frame(run, state)
	f.Window = &Window{FirstStep: 1}
	f.SessionID = "admission"
	if err := f.ValidateContext(cfg.Limits); err != nil {
		return err
	}
	p, err := f.Prompt(cfg.Limits)
	if err != nil {
		return err
	}
	req, err := model.Freeze(p)
	if err != nil {
		return err
	}
	return loop.admitRequest(req)
}

func (l *Loop) outputReserve(req Request) int {
	if req.MaxOutputTokens != 0 {
		return req.MaxOutputTokens
	}
	return l.cfg.OutputTokens
}
