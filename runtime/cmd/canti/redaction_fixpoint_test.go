package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/core"
)

func TestReplacementCannotReconstituteCredentialInDurableBody(t *testing.T) {
	const secret = "sk-sol-safe-credential"
	const suffix = "abcdefghijklm"
	for _, api := range []string{"anthropic-messages", "openai-responses"} {
		t.Run(api, func(t *testing.T) {
			f := newFixture(t, map[int]core.Decision{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprintf(w, `{"error":{"type":"authentication_error","message":"%s%s"}}`, secret, suffix)
			}))
			defer srv.Close()
			run := runCLI(t, []string{"ANTHROPIC_API_KEY=" + secret, "OPENAI_API_KEY=" + secret}, "run",
				"-api", api, "-model", "fixture-model", "-context-tokens", "6100", "-ephemeral", "-goal", "Verify redaction.",
				"-workspace", f.workspace, "-state", f.state, "-model-url", srv.URL, "-max-attempts", "1")
			if run.code == exitOK {
				t.Fatalf("CLI accepted an authentication failure: %+v", run)
			}
			var found bool
			for _, e := range f.history() {
				if e.Facts.Response == nil {
					continue
				}
				found = true
				if bytes.Contains(e.Facts.Response.Body, []byte(secret)) || strings.Contains(e.Facts.Failure, secret) {
					t.Fatalf("durable body/failure reconstituted the credential: body=%q failure=%q", e.Facts.Response.Body, e.Facts.Failure)
				}
			}
			if !found {
				t.Fatal("no response persisted")
			}
			data, err := os.ReadFile(filepath.Join(f.state, "canti.db"))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("SQLite contains the credential")
			}
		})
	}
}
