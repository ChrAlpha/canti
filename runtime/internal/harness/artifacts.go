package harness

import (
	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
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

// Observation is the durable external result. Retrieval and handover are
// transient presentation fields for local outcomes; persisted observations
// must leave both nil.
type Observation struct {
	Execution core.ExecutionState
	ExitCode  *int
	Preview   string
	Output    ArtifactRef
	Retrieval *archive.Retrieval `json:",omitempty"`
}
