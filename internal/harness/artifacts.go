package harness

import (
	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

// ArtifactRef points to output stored outside the bounded observation
// preview. Path is relative to the executor's artifact root. Detail explains
// why an artifact is incomplete or unavailable.
type ArtifactRef struct {
	Path      string
	Bytes     int64
	Complete  bool
	Available bool
	Detail    string
}

// Observation is the durable result recorded after an effect is inspected
// or resolved. Retrieval and Handover are only read: versions before
// ActionSemantics recorded the outcomes of harness actions as observations.
type Observation struct {
	Execution core.ExecutionState
	ExitCode  *int
	Preview   string
	Output    ArtifactRef
	Retrieval *archive.Retrieval `json:",omitempty"`
	Handover  *Handover          `json:",omitempty"`
}
