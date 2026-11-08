//go:build integration

package main

import (
	"encoding/json"
	"os"
	"testing"
)

// TestAcceptanceReleasedResources keeps final receipt and storage assertions
// inside runtime; root acceptance supplies the public history JSON via stdin.
func TestAcceptanceReleasedResources(t *testing.T) {
	state := os.Getenv("CANTI_ACCEPTANCE_VERIFY_STATE")
	if state == "" {
		t.Skip("runtime-owned fixture invoked by root acceptance")
	}
	var history []entry
	if err := json.NewDecoder(os.Stdin).Decode(&history); err != nil {
		t.Fatal(err)
	}
	checkReleased(t, &fixture{t: t, state: state}, history)
}
