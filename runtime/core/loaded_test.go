package core_test

import (
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func loaded(step, attempt int, body []byte) core.ResponseLoaded {
	decision, err := core.Decode(body)
	if err == nil {
		return core.ResponseLoaded{Step: step, Attempt: attempt, Body: body, Decision: &decision}
	}
	return core.ResponseLoaded{Step: step, Attempt: attempt, Body: body, Diagnostic: true}
}

func mustDecision(t *testing.T, body []byte) *core.Decision {
	t.Helper()
	d, err := core.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	return &d
}
