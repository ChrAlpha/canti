package harness

import (
	"math"
	"strings"
	"testing"
)

func testFrame() Frame {
	return Frame{
		Goal: "repair tests", Environment: "/workspace", Step: 1, MaxSteps: 10, Attempt: 1, MaxAttempts: 3,
		RunID: "run_x", Window: &Window{FirstStep: 1},
	}
}

func TestFrameValidate(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*Frame)
		wantErr bool
	}{
		{name: "valid", edit: func(*Frame) {}},
		{name: "unlimited steps", edit: func(f *Frame) { f.MaxSteps = 0 }},
		{name: "unlimited late step", edit: func(f *Frame) {
			f.Step, f.MaxSteps, f.Earlier, f.Window = math.MaxInt, 0, math.MaxInt-1, &Window{FirstStep: math.MaxInt, Through: math.MaxInt - 1, Summary: "continue"}
		}},
		{name: "negative step limit", edit: func(f *Frame) { f.MaxSteps = -1 }, wantErr: true},
		{name: "last step and attempt", edit: func(f *Frame) {
			f.Step, f.Attempt, f.Earlier, f.Window = 10, 3, 9, &Window{FirstStep: 10, Through: 9, Summary: "continue"}
		}},
		{name: "current page", edit: func(f *Frame) { f.Step, f.Earlier, f.Steps = 4, 0, []StepRecord{{Step: 1}, {Step: 2}, {Step: 3}} }},
		{name: "rejection after an attempt", edit: func(f *Frame) { f.Attempt, f.Rejected = 2, &Rejection{} }},
		{name: "missing goal", edit: func(f *Frame) { f.Goal = "" }, wantErr: true},
		{name: "blank goal", edit: func(f *Frame) { f.Goal = " \n\t" }, wantErr: true},
		{name: "missing environment", edit: func(f *Frame) { f.Environment = "" }, wantErr: true},
		{name: "blank environment", edit: func(f *Frame) { f.Environment = "  " }, wantErr: true},
		{name: "missing run ID", edit: func(f *Frame) { f.RunID = "" }, wantErr: true},
		{name: "missing window", edit: func(f *Frame) { f.Window = nil }, wantErr: true},
		{name: "missing page steps", edit: func(f *Frame) { f.Step, f.Earlier, f.Steps = 4, 1, []StepRecord{{Step: 2}, {Step: 3}} }, wantErr: true},
		{name: "zero step", edit: func(f *Frame) { f.Step = 0 }, wantErr: true},
		{name: "step beyond the limit", edit: func(f *Frame) { f.Step, f.Earlier = 11, 10 }, wantErr: true},
		{name: "zero attempt", edit: func(f *Frame) { f.Attempt = 0 }, wantErr: true},
		{name: "attempt beyond the limit", edit: func(f *Frame) { f.Attempt = 4 }, wantErr: true},
		{name: "rejection on the first attempt", edit: func(f *Frame) { f.Rejected = &Rejection{} }, wantErr: true},
		{name: "negative earlier steps", edit: func(f *Frame) { f.Step, f.Earlier, f.Steps = 2, -1, []StepRecord{{}, {Step: 1}} }, wantErr: true},
		{name: "overflowing earlier steps", edit: func(f *Frame) { f.Step, f.Earlier, f.Steps = 1, math.MaxInt, []StepRecord{{}} }, wantErr: true},
		{name: "missing completed steps", edit: func(f *Frame) { f.Step = 5 }, wantErr: true},
		{name: "steps beyond the current one", edit: func(f *Frame) { f.Steps = []StepRecord{{Step: 1}} }, wantErr: true},
		{name: "steps newest first", edit: func(f *Frame) { f.Step, f.Steps = 3, []StepRecord{{Step: 2}, {Step: 1}} }, wantErr: true},
		{name: "steps after a gap", edit: func(f *Frame) { f.Step, f.Earlier, f.Steps = 4, 1, []StepRecord{{Step: 3}, {Step: 4}} }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := testFrame()
			tt.edit(&frame)
			err := frame.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Frame.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnlimitedProgress(t *testing.T) {
	f := testFrame()
	f.MaxSteps = 0
	p, err := f.Prompt(DefaultPromptLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.User(), "Step 1 (unlimited steps), attempt 1 of 3.") || strings.Contains(p.User(), "of at most 0") {
		t.Fatalf("unlimited progress missing: %s", p.User())
	}
}

// TestHistoryPageIsOutgrownByTheLongWindowOfTheContextVersion requires that
// the 65 reads after which the context version of cmd/canti records a
// request, through TestContextVersionPinsItsRules, outgrow one journal page,
// so that the record covers the selection of a window across pages.
func TestHistoryPageIsOutgrownByTheLongWindowOfTheContextVersion(t *testing.T) {
	if historyPage >= 65 {
		t.Fatalf("a journal page holds %d records; raise the 65 reads of storedRules in cmd/canti above it", historyPage)
	}
}
