package hostrpc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	wire "github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestServingHostValidateRejectsWrongLocationBeforeInheritedFacts(t *testing.T) {
	h, _, client, _ := servingWith(t, slog.New(slog.DiscardHandler))
	state := filepath.Dir(client.Path)
	base := wire.Validation{
		State: state,
		Submission: wire.Submission{
			SessionID: "invalid/session", Goal: "input",
			Spec: wire.Spec{Workspace: h.Workspace(), WallTime: "30m", JobTimeout: "10m"},
		},
	}
	_, err := client.Validate(context.Background(), base)
	if err == nil || !strings.Contains(err.Error(), "-session") {
		t.Fatalf("control validation = %v; want session error at correct location", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	control := base
	control.Submission.SessionID = "session-qa"
	_, err = Validate(cancelled, &control, h)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("inherited lookup at correct location = %v, want canceled context", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*wire.Validation) string
	}{
		{"wrong state", func(v *wire.Validation) string {
			v.State = filepath.Join(t.TempDir(), "other-state")
			return v.State
		}},
		{"wrong workspace", func(v *wire.Validation) string {
			v.Submission.Spec.Workspace = filepath.Join(t.TempDir(), "other-workspace")
			return v.Submission.Spec.Workspace
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validation := base
			wrong := tc.change(&validation)
			_, err := client.Validate(context.Background(), validation)
			var spec wire.SpecError
			if !errors.As(err, &spec) || !strings.Contains(err.Error(), "this host's directories") || strings.Contains(err.Error(), "-session") {
				t.Fatalf("validation at wrong location = %v; want location error before session facts", err)
			}
			control := validation
			control.Submission.SessionID = "session-qa"
			_, err = Validate(cancelled, &control, h)
			if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "this host's directories") {
				t.Fatalf("validation with canceled inherited lookup = %v; want location refusal first", err)
			}
			if _, statErr := os.Stat(wrong); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("wrong location was created: %v", statErr)
			}
		})
	}
}
