package main

import (
	"fmt"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
) // report prints the outcome of the run at snap, of config c, and returns its
// exit status, or active while the run is still active. Callers release the
// workspace lock first: behind a stderr queue each line waits up to
// stderrFinalTime.
func (a *app) report(s hostrpc.Run, err error) (int, error) {
	out := a.stderr
	if a.display != nil {
		out = finalStderr{a.display}
	}
	if err == nil && s.Totals.Coverage.Attempts > 0 {
		c := s.Totals.Coverage
		fmt.Fprintf(out, "canti: reported usage input %d output %d; coverage input %d/%d output %d/%d; lost %d\n", s.Totals.InputTokens, s.Totals.OutputTokens, c.InputReports, c.Attempts, c.OutputReports, c.Attempts, c.Lost)
	}
	switch {
	case err != nil:
		return exitError, err
	case s.Status == hostrpc.StatusReturned:
		fmt.Fprintln(a.stdout, s.Result)
		return exitOK, nil
	case s.Status == hostrpc.StatusStopped:
		fmt.Fprintf(out, "canti: run %s stopped: %s\n", s.RunID, s.StopReason)
		return exitStopped, nil
	case s.Status == hostrpc.StatusNeedsResolution:
		fmt.Fprintf(out, "canti: run %s needs resolution: the outcome of effect %s is unknown. Inspect it, then "+
			"record its job with \"canti resolve -run %[1]s -job\" or its outcome with \"canti resolve -run %[1]s -execution STATE\"; "+
			"-execution records the outcome only and leaves a job that may still run untouched.\n", s.RunID, s.EffectID)
		if s.StopReason != "" {
			fmt.Fprintf(out, "canti: the stop is recorded (%s), but its job may still run until it is resolved; "+
				"after \"canti resolve -run %s -job\" records a job that still runs, \"canti stop\" cancels it.\n", s.StopReason, s.RunID)
		}
		return exitResolution, nil
	}
	next := "resume"
	if s.Phase == "cancelling" {
		next = "stop"
	}
	fmt.Fprintf(out, "canti: run %s is %s; continue it with \"canti %s -run %[1]s\".\n", s.RunID, s.Phase, next)
	return exitActive, nil
}
