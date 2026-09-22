package harness

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

var errInjected = errors.New("injected boundary failure")

type eventRecorder struct {
	events []string
	failAt string
	after  func(string)
}

func (r *eventRecorder) record(event string) error {
	r.events = append(r.events, event)
	if r.after != nil {
		r.after(event)
	}
	if r.failAt == event {
		return errInjected
	}
	return nil
}

type recordingStore struct{ *eventRecorder }

func (s recordingStore) SaveRequest(context.Context, RequestRecord) error {
	return s.record("request")
}

func (s recordingStore) SaveResponse(context.Context, ResponseRecord) error {
	return s.record("response")
}

func (s recordingStore) SaveDecision(context.Context, DecisionRecord) error {
	return s.record("decision")
}

func (s recordingStore) SaveEffect(context.Context, EffectRecord) error {
	return s.record("effect")
}

func (s recordingStore) SaveObservation(context.Context, ObservationRecord) error {
	return s.record("observation")
}

func (s recordingStore) SaveReturn(context.Context, ReturnRecord) error {
	return s.record("return")
}

type recordingExecutor struct {
	*eventRecorder
	last Effect
}

func (e *recordingExecutor) StartOrResume(_ context.Context, effect Effect) (Job, error) {
	e.last = effect
	return Job{ID: "job-1", Status: JobRunning}, e.record("start")
}

func (e *recordingExecutor) Inspect(context.Context, string) (Job, error) {
	return Job{}, e.record("inspect")
}

func (e *recordingExecutor) Cancel(context.Context, string) error {
	return e.record("cancel")
}

func TestApplyDecisionCommitBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		kind    string
		failAt  string
		want    []string
		outcome OutcomeKind
	}{
		{name: "exec", kind: "exec", want: []string{"decision", "effect", "start"}, outcome: OutcomeDispatched},
		{name: "return", kind: "return", want: []string{"decision", "return"}, outcome: OutcomeReturned},
		{name: "decision failure", kind: "exec", failAt: "decision", want: []string{"decision"}},
		{name: "effect failure", kind: "exec", failAt: "effect", want: []string{"decision", "effect"}},
		{name: "return failure", kind: "return", failAt: "return", want: []string{"decision", "return"}},
		{name: "executor failure", kind: "exec", failAt: "start", want: []string{"decision", "effect", "start"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &eventRecorder{failAt: tt.failAt}
			executor := &recordingExecutor{eventRecorder: events}
			runner, err := NewRunner(recordingStore{events}, executor)
			if err != nil {
				t.Fatal(err)
			}
			raw := []byte("{\"kind\":\"" + tt.kind + "\",\"payload\":\"test\"}")
			out, err := runner.ApplyDecision(context.Background(), "run-1", "step-1", Response{Body: raw, Complete: true})
			if tt.failAt != "" {
				if !errors.Is(err, errInjected) || out.Kind != "" {
					t.Fatalf("outcome = %#v, error = %v; want failure", out, err)
				}
			} else if err != nil || out.Kind != tt.outcome {
				t.Fatalf("outcome = %#v, error = %v", out, err)
			}
			if !reflect.DeepEqual(events.events, tt.want) {
				t.Fatalf("events = %v, want %v", events.events, tt.want)
			}
			if out.Kind == OutcomeDispatched && (out.Effect == nil || *out.Effect != executor.last || out.Job == nil) {
				t.Fatalf("outcome lost execution identity: %#v", out)
			}
			if out.Kind == OutcomeReturned && (out.Effect != nil || out.Job != nil) {
				t.Fatalf("return has an effect: %#v", out)
			}
		})
	}
}

func TestApplyDecisionRejectsInvalidInputBeforeWriting(t *testing.T) {
	tests := []struct{ name, runID, stepID, raw string }{
		{"invalid response", "run-1", "step-1", "{\"kind\":\"unknown\",\"payload\":\"x\"}"},
		{"missing run", "", "step-1", "{\"kind\":\"exec\",\"payload\":\"x\"}"},
		{"missing step", "run-1", "", "{\"kind\":\"exec\",\"payload\":\"x\"}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &eventRecorder{}
			runner, err := NewRunner(recordingStore{events}, &recordingExecutor{eventRecorder: events})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runner.ApplyDecision(context.Background(), tt.runID, tt.stepID, Response{Body: []byte(tt.raw), Complete: true}); err == nil {
				t.Fatal("ApplyDecision succeeded, want error")
			}
			if len(events.events) != 0 {
				t.Fatalf("invalid input caused side effects: %v", events.events)
			}
		})
	}
}

func TestApplyDecisionCancellation(t *testing.T) {
	for _, cancelAt := range []string{"before", "effect"} {
		t.Run(cancelAt, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			events := &eventRecorder{after: func(event string) {
				if event == cancelAt {
					cancel()
				}
			}}
			if cancelAt == "before" {
				cancel()
			}
			runner, err := NewRunner(recordingStore{events}, &recordingExecutor{eventRecorder: events})
			if err != nil {
				t.Fatal(err)
			}
			response := Response{Body: []byte(`{"kind":"exec","payload":"x"}`), Complete: true}
			_, err = runner.ApplyDecision(ctx, "run-1", "step-1", response)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want cancellation", err)
			}
			var want []string
			if cancelAt == "effect" {
				want = []string{"decision", "effect"}
			}
			if !reflect.DeepEqual(events.events, want) {
				t.Fatalf("events = %v, want %v", events.events, want)
			}
		})
	}
}

func TestStableEffectID(t *testing.T) {
	pairs := [][2]string{{"run-1", "step-1"}, {"run-1", "step-2"}, {"run-2", "step-1"}, {"a\x00b", "c"}, {"a", "b\x00c"}}
	seen := make(map[string][2]string)
	for _, pair := range pairs {
		id := stableEffectID(pair[0], pair[1])
		if previous, exists := seen[id]; exists {
			t.Fatalf("%q and %q share ID %q", pair, previous, id)
		}
		if again := stableEffectID(pair[0], pair[1]); id != again {
			t.Fatalf("effect ID changed: %q != %q", id, again)
		}
		seen[id] = pair
	}
}

func TestApplyDecisionRejectsIncompleteResponse(t *testing.T) {
	events := &eventRecorder{}
	runner, err := NewRunner(recordingStore{events}, &recordingExecutor{eventRecorder: events})
	if err != nil {
		t.Fatal(err)
	}
	response := Response{Body: []byte(`{"kind":"exec","payload":"x"}`)}
	if _, err := runner.ApplyDecision(context.Background(), "run-1", "step-1", response); err == nil {
		t.Fatal("valid JSON in an incomplete response must not be accepted")
	}
	if len(events.events) != 0 {
		t.Fatalf("incomplete response caused side effects: %v", events.events)
	}
}
