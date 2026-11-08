package core_test

import (
	"encoding/json"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestEventIDsMatchCommittedLiterals(t *testing.T) {
	const run = "3f2c9a1e-7b4d-4c8e-9a6f-0d1b2c3e4f50"
	const effect = "effect_371edff20886199482d53f7e4a6f74f2a2ddb5d8e74274e53052b1255e139650"
	cases := []struct {
		event         core.Event
		kind, encoded string
		id            string
	}{
		{core.RequestFrozen{}, "request_frozen", `{}`,
			"event_656e3ad6e24c67e21e2cfe50fa940ef4e740bc7c61d87e172f5b17b4fab03725"},
		{core.AttemptStarted{Step: 9, Attempt: 2}, "attempt_started", `{"Step":9,"Attempt":2}`,
			"event_a8ff4b62c8573689b579fd25ce6f60b2f72199430eae2335adef2d3fe64fcd35"},
		{core.ResponseSaved{Step: 9, Attempt: 2}, "response_saved", `{"Step":9,"Attempt":2}`,
			"event_fb1e6a34760a8376f6bccaba8289a9288bb8fcc457c4b9dedd95522c02046c1d"},
		{core.ResponseIncomplete{Step: 9, Attempt: 2}, "response_incomplete", `{"Step":9,"Attempt":2}`,
			"event_ce73726bec4f54580afb71beb4a6b737e40fa5d8affa314397402b78c9aca61a"},
		{core.ResponseLoaded{Step: 9, Attempt: 2, Body: []byte(`{"kind":"exec","payload":"go test ./..."}`)}, "response_loaded",
			`{"Step":9,"Attempt":2,"Body":"eyJraW5kIjoiZXhlYyIsInBheWxvYWQiOiJnbyB0ZXN0IC4vLi4uIn0="}`,
			"event_a4c7088d04e609a41fe7dcc8835db463c6668ddc38661ef65685d350ff7f08fd"},
		{core.EffectRecovered{EffectID: effect}, "effect_recovered", `{"EffectID":"` + effect + `"}`,
			"event_05966fd6a096e379939d12ddc1d363d3e8cbc55845bc83c674e07afefac3b1fe"},
		{core.Observed{EffectID: effect, Execution: core.ExecutionExited}, "observed", `{"EffectID":"` + effect + `","Execution":"exited"}`,
			"event_ec2b0cf9f514eab5abede9a59cce4655f29e5901346d88158c741df56b5f9e38"},
		{core.Performed{EffectID: effect}, "performed", `{"EffectID":"` + effect + `"}`,
			"event_8647d1c75cac75032731b4b73b85a37284524ef93fb103f43f575e7f0735351f"},
		{core.OutcomeUnknown{EffectID: effect}, "outcome_unknown", `{"EffectID":"` + effect + `"}`,
			"event_dc3c7c0ac451ca5762b369b9a017fd10640f4a4e1103e11cb1e3c5d15a3d74b7"},
		{core.StopRequested{Reason: core.StopBudgetExhausted}, "stop_requested", `{"Reason":"budget_exhausted"}`,
			"event_9c6854cbbb370bd1afdbd7a14a88e35f0dfefe3762c5ef7ba050cf54fdb1fac1"},
		{core.Resolved{EffectID: effect, Execution: core.ExecutionFailedToStart}, "resolved", `{"EffectID":"` + effect + `","Execution":"failed_to_start"}`,
			"event_2dbf01369b95849ad6ada09e05251276e533c691250ef2158364c359e0c0abfd"},
	}
	kinds := map[string]bool{}
	for _, tc := range cases {
		kinds[tc.event.Kind()] = true
		if tc.event.Kind() != tc.kind {
			t.Errorf("%T kind = %q, want %q", tc.event, tc.event.Kind(), tc.kind)
		}
		if got, err := json.Marshal(tc.event); err != nil || string(got) != tc.encoded {
			t.Errorf("%s encoding = %s, %v; want %s", tc.kind, got, err, tc.encoded)
		}
		if got, err := core.EventID(run, 42, tc.event); err != nil || got != tc.id {
			t.Errorf("%s event ID = %q, %v; want %q", tc.kind, got, err, tc.id)
		}
	}
	if len(kinds) != len(core.Events()) {
		t.Fatalf("covered %d distinct event kinds, want all %d", len(kinds), len(core.Events()))
	}
}
