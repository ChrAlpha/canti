package harness

import "testing"

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
		{name: "missing goal", edit: func(f *Frame) { f.Goal = "" }, wantErr: true},
		{name: "blank goal", edit: func(f *Frame) { f.Goal = " \n\t" }, wantErr: true},
		{name: "missing environment", edit: func(f *Frame) { f.Environment = "" }, wantErr: true},
		{name: "blank environment", edit: func(f *Frame) { f.Environment = "  " }, wantErr: true},
		{name: "zero step", edit: func(f *Frame) { f.Step = 0 }, wantErr: true},
		{name: "step beyond the limit", edit: func(f *Frame) { f.Step = 11 }, wantErr: true},
		{name: "zero attempt", edit: func(f *Frame) { f.Attempt = 0 }, wantErr: true},
		{name: "attempt beyond the limit", edit: func(f *Frame) { f.Attempt = 4 }, wantErr: true},
		{name: "negative earlier steps", edit: func(f *Frame) { f.Earlier = -1 }, wantErr: true},
		{name: "missing artifact directory", edit: func(f *Frame) { f.ArtifactDir = " " }, wantErr: true},
		{name: "missing notes path", edit: func(f *Frame) { f.Notes.Path = "" }, wantErr: true},
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
