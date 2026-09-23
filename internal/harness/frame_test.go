package harness

import (
	"math"
	"testing"
)

func testFrame() Frame {
	return Frame{
		Goal: "repair tests", Environment: "/workspace", Step: 1, MaxSteps: 10, Attempt: 1, MaxAttempts: 3,
		ArtifactDir: "/canti/artifacts", Notes: Notes{Path: "/workspace/.canti/notes.md"},
	}
}

func TestFrameValidate(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*Frame)
		wantErr bool
	}{
		{name: "valid", edit: func(*Frame) {}},
		{name: "last step and attempt", edit: func(f *Frame) { f.Step, f.Attempt, f.Earlier = 10, 3, 9 }},
		{name: "recent steps", edit: func(f *Frame) { f.Step, f.Earlier, f.Steps = 4, 1, []StepRecord{{Step: 2}, {Step: 3}} }},
		{name: "rejection after an attempt", edit: func(f *Frame) { f.Attempt, f.Rejected = 2, &Rejection{} }},
		{name: "missing goal", edit: func(f *Frame) { f.Goal = "" }, wantErr: true},
		{name: "blank goal", edit: func(f *Frame) { f.Goal = " \n\t" }, wantErr: true},
		{name: "missing environment", edit: func(f *Frame) { f.Environment = "" }, wantErr: true},
		{name: "blank environment", edit: func(f *Frame) { f.Environment = "  " }, wantErr: true},
		{name: "missing artifact directory", edit: func(f *Frame) { f.ArtifactDir = " " }, wantErr: true},
		{name: "missing notes path", edit: func(f *Frame) { f.Notes.Path = "" }, wantErr: true},
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
