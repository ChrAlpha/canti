package present

import (
	"fmt"
	"strings"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// Result keeps the answer separate from diagnostic and recovery text.
func Result(out chat.Result) (answer, diagnostic string) {
	return result(out, false)
}

// ResultAfterTerminal is Result for a run whose terminal progress item was
// already shown: it keeps the diagnostic and recovery text but not the answer
// or stop line that the item displayed.
func ResultAfterTerminal(out chat.Result) (answer, diagnostic string) {
	return result(out, true)
}

func result(out chat.Result, terminalShown bool) (answer, diagnostic string) {
	r := out.Run
	var b strings.Builder
	if out.Detached {
		fmt.Fprintf(&b, "canti: detached from run %s, which the host keeps driving; follow it with \"canti resume -host auto -run %[1]s\".\n", r.RunID)
	}
	if out.Err != nil {
		if out.Rejected && !out.NoInputFits && strings.HasPrefix(out.Err.Error(), "-request-bytes:") {
			fmt.Fprintf(&b, "canti-tui: the input does not fit -request-bytes (%s)\n", strings.TrimPrefix(out.Err.Error(), "-request-bytes: "))
		} else {
			fmt.Fprintf(&b, "canti-tui: %v\n", out.Err)
		}
	}
	if out.Err == nil && r.Totals.Coverage.Attempts > 0 {
		c := r.Totals.Coverage
		fmt.Fprintf(&b, "canti: reported usage input %d output %d; coverage input %d/%d output %d/%d; lost %d\n", r.Totals.InputTokens, r.Totals.OutputTokens, c.InputReports, c.Attempts, c.OutputReports, c.Attempts, c.Lost)
	}
	if r.Why != "" && r.Status == hostrpc.StatusActive && !r.Drivable {
		fmt.Fprintf(&b, "canti: the host cannot continue run %s by itself: %s\n", r.RunID, r.Why)
	}
	switch r.Status {
	case hostrpc.StatusReturned:
		if !terminalShown {
			answer = r.Result + "\n"
		}
	case hostrpc.StatusStopped:
		if r.Environment != nil {
			fmt.Fprintf(&b, "canti: inspect the persistent environment gate with canti environment inspect -environment %q; start explicitly after an acknowledged stop.\n", r.Environment.Directory)
		}
		if !terminalShown {
			fmt.Fprintf(&b, "canti: run %s stopped: %s\n", r.RunID, r.StopReason)
		}
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

// Reconciliation is mandatory outcome guidance, independent of log level.
func Reconciliation(inputID string) string {
	return fmt.Sprintf("canti: acceptance of input %s is unknown; reconcile with \"canti run -input %[1]s\" and the original goal and launch flags before starting new input.\n", inputID)
}
