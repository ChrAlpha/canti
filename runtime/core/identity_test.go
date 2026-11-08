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

func TestEffectIDsMatchCommittedLiterals(t *testing.T) {
	const uuid = "3f2c9a1e-7b4d-4c8e-9a6f-0d1b2c3e4f50"
	for _, tc := range []struct {
		runID string
		step  int
		want  string
	}{
		{uuid, 1, "effect_c042f9eece9b5bc161ea58017b865704c2fbf6cf3f510a1d0ca63ef287f6358d"},
		{uuid, 9, "effect_371edff20886199482d53f7e4a6f74f2a2ddb5d8e74274e53052b1255e139650"},
		{uuid, 10, "effect_212b8585921fd69c0f47d60f854572490dd5dc3979078bad8f04e507b86b09b8"},
		{uuid, 12345, "effect_b2643e58c37c598f6d5938d3a216557eaa627638e9798095e8d5f428387f6c74"},
		{"run_0123456789abcdef0123456789abcdef", 1, "effect_150f1d740dec1a6883c2261d90b0981f51817ab5dfbc37fa76e7a0dd77aeca1a"},
		{"", 1, "effect_ef134f2a180ba05de91ab32d2976f51de13b68d823ea784171b1b0dafee67be4"},
		{"运行-é🙂", 3, "effect_03d79136d82002876e94399affd52b4cc7dcabc84e860ec70a5f03ef25091158"},
	} {
		if got := effectID(tc.runID, tc.step); got != tc.want {
			t.Errorf("effectID(%q, %d) = %q, want %q", tc.runID, tc.step, got, tc.want)
		}
		if tc.runID == "" {
			continue
		}
		s := State{RunID: tc.runID, Step: tc.step, Attempt: 1, RequestRev: 1, Phase: PhaseResponseSaved, Limits: Limits{MaxAttempts: 1}}
		next, err := s.Next(ResponseLoaded{Step: tc.step, Attempt: 1, Body: []byte(`{"kind":"exec","payload":"true"}`)})
		if err != nil || next.EffectID != tc.want {
			t.Errorf("decided effect of %q step %d = %q, %v; want %q", tc.runID, tc.step, next.EffectID, err, tc.want)
		}
	}
}
