package harness

import "github.com/ChrAlpha/canti/core"

// Blocks reports whether a run whose latest state s was reached by last,
// nil before the run's first transition, keeps a new run out of its state
// directory: while it is open, and after it was abandoned with an effect
// whose outcome stays unknown.
func Blocks(s core.State, last core.Event) bool {
	if !s.Closed() {
		return true
	}
	r, ok := last.(core.Resolved)
	return ok && r.Execution == core.ExecutionUnknown
}
