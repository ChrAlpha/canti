package anthropic

import (
	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// ExtractForTest builds the synthetic extraction a test Response would carry.
func ExtractForTest(m *Model, resp harness.Response) harness.Extraction {
	return m.extract(resp.Body, resp.Body, resp.Adapter, resp.Complete)
}
