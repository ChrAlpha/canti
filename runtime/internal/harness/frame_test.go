package harness

import (
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func testFrame() Frame {
	return Frame{Goal: "repair tests", Environment: "/workspace", RunID: "run_x", Step: 1, MaxSteps: 10, Attempt: 1, MaxAttempts: 3, ContextState: &ContextState{FirstStep: 1}}
}
func TestFrameValidate(t *testing.T) {
	for _, edit := range []func(*Frame){func(f *Frame) { f.ContextState = nil }, func(f *Frame) { f.Goal = "\xff" }, func(f *Frame) { f.Goal = "" }, func(f *Frame) { f.Environment = "" }, func(f *Frame) { f.Step = 0 }, func(f *Frame) { f.Attempt = 4 }, func(f *Frame) { f.Step = 2 }, func(f *Frame) { f.ContextState = &ContextState{FirstStep: 2} }, func(f *Frame) { f.Latest = &StepRecord{Step: 9} }} {
		f := testFrame()
		edit(&f)
		if err := f.Validate(); err == nil {
			t.Fatalf("invalid frame accepted: %+v", f)
		}
	}
	f := testFrame()
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
	f.MaxSteps = 0
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
	f.Step = 3
	f.ContextState = &ContextState{AnchorSeq: 10, FirstStep: 3}
	last := StepRecord{Seq: 9, Step: 2, Observation: Observation{Execution: core.ExecutionExited}}
	f.Latest = &last
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
}
