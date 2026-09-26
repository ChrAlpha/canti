package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestEffectIDLegacyBytes(t *testing.T) {
	for _, tc := range []struct {
		runID string
		step  int
	}{
		{"", 0},
		{"run-1", 12},
		{"你好", -12},
		{"a-very-long-run-identifier-which-exceeds-the-stack-input-buffer", 123456789},
	} {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%d", len(tc.runID), tc.runID, tc.step)))
		want := "effect_" + hex.EncodeToString(sum[:])
		if got := effectID(tc.runID, tc.step); got != want {
			t.Errorf("effectID(%q, %d) = %q, want %q", tc.runID, tc.step, got, want)
		}
	}
}
