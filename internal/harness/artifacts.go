package harness

import "github.com/ChrAlpha/canti/core"

// ArtifactRef points to output stored outside the bounded observation preview.
type ArtifactRef struct {
	Path      string
	Bytes     int64
	Complete  bool
	Available bool
}

// Observation is the durable result recorded after an effect is inspected.
type Observation struct {
	Execution core.ExecutionState
	ExitCode  *int
	Preview   string
	Output    ArtifactRef
}
