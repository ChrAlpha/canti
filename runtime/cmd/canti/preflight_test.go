package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func testPreflightValidation(t *testing.T) hostrpc.Validation {
	t.Helper()
	dir := t.TempDir()
	workspace := filepath.Join(dir, "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return hostrpc.Validation{
		State: filepath.Join(dir, "state"),
		Submission: hostrpc.Submission{
			SessionID: "session-qa", Goal: "qa preflight", ExplicitConstraints: true,
			Spec: hostrpc.Spec{
				Workspace: workspace, Image: "debian:bookworm-slim", Network: "none", Memory: "2g",
				Pids: 512, OutputLimit: 8 << 20, API: "openai-responses", Model: "qa-model",
				MaxTokens: 16000, Structured: true, ContextBytes: 256 << 10,
				HandoverBytes: 2048, MaxSteps: 20, MaxAttempts: 3, WallTime: "30m",
				OutputBudget: 256 << 20, JobTimeout: "10m",
			},
		},
		Binding: hostrpc.Binding{OpenAIKey: "x"},
	}
}

func callPreflight(t *testing.T, body []byte) hostrpc.Response {
	t.Helper()
	var out bytes.Buffer
	if code := preflight(context.Background(), bytes.NewReader(body), &out); code != exitOK {
		t.Fatalf("preflight exit = %d, want %d", code, exitOK)
	}
	var response hostrpc.Response
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatalf("preflight response: %v", err)
	}
	if response.Protocol != hostrpc.Protocol {
		t.Fatalf("response protocol = %d, want %d", response.Protocol, hostrpc.Protocol)
	}
	return response
}

func preflightRequest(t *testing.T, validation hostrpc.Validation) []byte {
	t.Helper()
	body, err := json.Marshal(hostrpc.Request{Protocol: hostrpc.Protocol, Op: hostrpc.OpValidate, Validate: &validation})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertNoState(t *testing.T, state string) {
	t.Helper()
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preflight created state %s: %v", state, err)
	}
}

func TestPreflightRejectsInvalidSpecsWithoutCreatingState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*hostrpc.Validation)
		valid  bool
	}{
		{"valid", func(*hostrpc.Validation) {}, true},
		{"layout", func(v *hostrpc.Validation) { v.State = filepath.Join(v.Submission.Spec.Workspace, "state") }, false},
		{"CPU", func(v *hostrpc.Validation) { v.Submission.Spec.CPUs = "0.0000000001" }, false},
		{"memory", func(v *hostrpc.Validation) { v.Submission.Spec.Memory = "1x" }, false},
		{"provider", func(v *hostrpc.Validation) { v.Binding.Provider = "relative-provider" }, false},
		{"context", func(v *hostrpc.Validation) { v.Submission.Spec.ContextBytes = 1 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validation := testPreflightValidation(t)
			tc.change(&validation)
			response := callPreflight(t, preflightRequest(t, validation))
			if tc.valid && (response.Validated == nil || response.Error != nil) {
				t.Fatalf("valid preflight = %+v", response)
			}
			if !tc.valid && (response.Error == nil || response.Validated != nil) {
				t.Fatalf("invalid preflight = %+v", response)
			}
			assertNoState(t, validation.State)
		})
	}
}

func TestPreflightStandaloneKeepsSuppliedConstraintsWithoutCreatingState(t *testing.T) {
	validation := testPreflightValidation(t)
	validation.Submission.SessionID = ""
	validation.Submission.ExplicitConstraints = false
	validation.Submission.Constraints = "supplied standalone rules"
	response := callPreflight(t, preflightRequest(t, validation))
	if response.Error != nil || response.Validated == nil || response.Validated.Constraints != validation.Submission.Constraints {
		t.Fatalf("standalone preflight = %+v", response)
	}
	assertNoState(t, validation.State)
}

func TestPreflightRejectsTrailingDocumentBeforeValidation(t *testing.T) {
	validation := testPreflightValidation(t)
	body := append(preflightRequest(t, validation), []byte(" {}")...)
	response := callPreflight(t, body)
	if response.Error == nil || response.Validated != nil {
		t.Fatalf("trailing request accepted: %+v", response)
	}
	assertNoState(t, validation.State)
}
