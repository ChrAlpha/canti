package harness

import (
	"context"
	"reflect"
	"testing"

	"github.com/ChrAlpha/canti/core"
)

type recordingStore struct {
	events []string
}

func (s *recordingStore) SaveRequest(context.Context, RequestRecord) error {
	s.events = append(s.events, "request")
	return nil
}

func (s *recordingStore) SaveResponse(context.Context, ResponseRecord) error {
	s.events = append(s.events, "response")
	return nil
}

func (s *recordingStore) SaveDecision(context.Context, DecisionRecord) error {
	s.events = append(s.events, "decision")
	return nil
}

func (s *recordingStore) SaveEffect(context.Context, EffectRecord) error {
	s.events = append(s.events, "effect")
	return nil
}

func (s *recordingStore) SaveObservation(context.Context, ObservationRecord) error {
	s.events = append(s.events, "observation")
	return nil
}

func (s *recordingStore) SaveReturn(context.Context, ReturnRecord) error {
	s.events = append(s.events, "return")
	return nil
}

type recordingExecutor struct {
	events []string
	last   Effect
}

func (e *recordingExecutor) StartOrResume(_ context.Context, effect Effect) (Job, error) {
	e.events = append(e.events, "start")
	e.last = effect
	return Job{ID: "job-1", Status: JobRunning}, nil
}

func (e *recordingExecutor) Inspect(context.Context, string) (Job, error) {
	e.events = append(e.events, "inspect")
	return Job{}, nil
}

func (e *recordingExecutor) Cancel(context.Context, string) error {
	e.events = append(e.events, "cancel")
	return nil
}

func TestApplyDecisionPersistsBeforeDispatch(t *testing.T) {
	store := &recordingStore{}
	executor := &recordingExecutor{}
	runner, err := NewRunner(store, executor)
	if err != nil {
		t.Fatal(err)
	}

	outcome, err := runner.ApplyDecision(context.Background(), "run-1", "step-1", []byte(`{"kind":"exec","payload":"go test ./..."}`))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != OutcomeDispatched || outcome.Job == nil || outcome.Effect == nil {
		t.Fatalf("unexpected outcome: %#v", outcome)
	}
	if got, want := store.events, []string{"decision", "effect"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("store events = %v, want %v", got, want)
	}
	if got, want := executor.events, []string{"start"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("executor events = %v, want %v", got, want)
	}
	if outcome.Effect.ID != executor.last.ID {
		t.Fatalf("dispatched effect ID = %q, executor saw %q", outcome.Effect.ID, executor.last.ID)
	}
}

func TestApplyDecisionReturnDoesNotDispatch(t *testing.T) {
	store := &recordingStore{}
	executor := &recordingExecutor{}
	runner, err := NewRunner(store, executor)
	if err != nil {
		t.Fatal(err)
	}

	outcome, err := runner.ApplyDecision(context.Background(), "run-1", "step-1", []byte(`{"kind":"return","payload":"blocked"}`))
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Kind != OutcomeReturned || outcome.Decision.Kind != core.Return {
		t.Fatalf("unexpected outcome: %#v", outcome)
	}
	if got, want := store.events, []string{"decision", "return"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("store events = %v, want %v", got, want)
	}
	if len(executor.events) != 0 {
		t.Fatalf("executor events = %v, want none", executor.events)
	}
}

func TestApplyDecisionRejectsInvalidResponseBeforeWriting(t *testing.T) {
	store := &recordingStore{}
	executor := &recordingExecutor{}
	runner, err := NewRunner(store, executor)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := runner.ApplyDecision(context.Background(), "run-1", "step-1", []byte(`{"kind":"unknown","payload":"x"}`)); err == nil {
		t.Fatal("ApplyDecision() succeeded, want error")
	}
	if len(store.events) != 0 || len(executor.events) != 0 {
		t.Fatalf("invalid response caused side effects: store=%v executor=%v", store.events, executor.events)
	}
}

func TestStableEffectIDDependsOnRunAndStep(t *testing.T) {
	first := stableEffectID("run-1", "step-1")
	second := stableEffectID("run-1", "step-1")
	otherStep := stableEffectID("run-1", "step-2")
	if first != second {
		t.Fatalf("stableEffectID changed between calls: %q != %q", first, second)
	}
	if first == otherStep {
		t.Fatalf("different steps share effect ID %q", first)
	}
}
