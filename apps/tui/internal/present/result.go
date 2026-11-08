package present

import (
	"fmt"
	"github.com/ChrAlpha/canti/apps/tui/internal/session"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
	"strings"
)

// Result keeps the answer separate from diagnostic and recovery text.
func Result(out session.Result) (answer, diagnostic string) {
	r := out.Run
	var b strings.Builder
	if out.Detached {
		fmt.Fprintf(&b, "canti: detached from run %s, which the host keeps driving; follow it with \"canti resume -host auto -run %[1]s\".\n", r.RunID)
	}
	if out.Err != nil {
		fmt.Fprintf(&b, "canti-tui: %v\n", out.Err)
	}
	if r.Why != "" && r.Status == hostrpc.StatusActive && !r.Drivable {
		fmt.Fprintf(&b, "canti: the host cannot continue run %s by itself: %s\n", r.RunID, r.Why)
	}
	switch r.Status {
	case hostrpc.StatusReturned:
		answer = r.Result + "\n"
	case hostrpc.StatusStopped:
		fmt.Fprintf(&b, "canti: run %s stopped: %s\n", r.RunID, r.StopReason)
	case hostrpc.StatusNeedsResolution:
		fmt.Fprintf(&b, "canti: run %s needs resolution: the outcome of effect %s is unknown. Inspect it, then record its job with \"canti resolve -run %[1]s -job\" or its outcome with \"canti resolve -run %[1]s -execution STATE\"; -execution records the outcome only and leaves a job that may still run untouched.\n", r.RunID, r.EffectID)
		if r.StopReason != "" {
			fmt.Fprintf(&b, "canti: the stop is recorded (%s), but its job may still run until it is resolved; after \"canti resolve -run %s -job\" records a job that still runs, \"canti stop\" cancels it.\n", r.StopReason, r.RunID)
		}
	case hostrpc.StatusActive:
		if !out.Detached {
			next := "resume"
			if r.Phase == "cancelling" {
				next = "stop"
			}
			fmt.Fprintf(&b, "canti: run %s is %s; continue it with \"canti %s -run %[1]s\".\n", r.RunID, r.Phase, next)
		}
	}
	return Clean(answer), Clean(b.String())
}
