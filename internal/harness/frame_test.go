package harness

import "testing"

func TestFrameValidate(t *testing.T) {
	tests := []struct {
		name    string
		frame   Frame
		wantErr bool
	}{
		{name: "valid", frame: Frame{Goal: "repair tests", Environment: "/workspace"}},
		{name: "missing goal", frame: Frame{Environment: "/workspace"}, wantErr: true},
		{name: "missing environment", frame: Frame{Goal: "repair tests"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.frame.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Frame.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
