package harness

import (
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestResponseSavedRequiresOneCommittedExtraction(t *testing.T) {
	response := &Response{Body: []byte(`{"kind":"return","payload":"done"}`), Complete: true}
	decision := core.Decision{Kind: core.Return, Payload: "done"}
	for name, extraction := range map[string]*Extraction{
		"missing":    nil,
		"empty":      {},
		"both":       {Decision: &decision, Diagnostic: "invalid"},
		"diagnostic": {Diagnostic: "invalid", Body: []byte("display")},
		"decision":   {Decision: &decision},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (Facts{Response: response, Extraction: extraction}).responseSaved()
			if (err == nil) != (name == "diagnostic" || name == "decision") {
				t.Fatalf("responseSaved(%s) = %v", name, err)
			}
		})
	}
}
