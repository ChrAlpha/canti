package harness

import "github.com/ChrAlpha/canti/runtime/core"

func testLoaded(step, attempt int, body []byte) core.ResponseLoaded {
	decision, err := core.Decode(body)
	if err == nil {
		return core.ResponseLoaded{Step: step, Attempt: attempt, Body: body, Decision: &decision}
	}
	return core.ResponseLoaded{Step: step, Attempt: attempt, Body: body, Diagnostic: true}
}
