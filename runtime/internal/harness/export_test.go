package harness

// SetCarryMarginStep makes the loops that cfg configures admit a carry only
// when it leaves one maximal step of work capacity beyond the step the
// reserve fits, the arm of the admission comparison not adopted.
func SetCarryMarginStep(cfg *LoopConfig) { cfg.carryMarginStep = true }
