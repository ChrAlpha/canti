package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

var credentialVariables = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY"}

func credentials() hostrpc.Binding {
	var v []string
	for _, n := range credentialVariables {
		v = append(v, os.Getenv(n))
		os.Unsetenv(n)
	}
	b := hostrpc.Binding{AnthropicKey: v[0], AnthropicToken: v[1], OpenAIKey: v[2]}
	if b.AnthropicKey != "" {
		b.AnthropicToken = ""
	}
	return b
}

func (a *app) binding(api, url string) hostrpc.Binding {
	b := a.cred
	b.Provider = url
	switch api {
	case "anthropic-messages":
		b.Fallback = os.Getenv("ANTHROPIC_BASE_URL")
		b.OpenAIKey = ""
	case "openai-responses":
		b.Fallback = os.Getenv("OPENAI_BASE_URL")
		b.AnthropicKey = ""
		b.AnthropicToken = ""
	}
	return b
}

func (a *app) runtimeExecutable() (string, error) {
	path, err := exec.LookPath(cmp.Or(a.runtimePath, "canti"))
	if err != nil {
		return "", fmt.Errorf("find canti runtime: %w; install canti on PATH or set -runtime PATH", err)
	}
	return filepath.Abs(path)
}

// validate calls a short-lived runtime without inheriting credentials. The
// versioned request carries them over stdin, and the response contains none.
func (a *app) validate(ctx context.Context, sub hostrpc.Submission, b hostrpc.Binding) (hostrpc.Validated, error) {
	ctx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()
	if !utf8.ValidString(sub.Goal) || !utf8.ValidString(sub.Constraints) {
		return hostrpc.Validated{}, hostrpc.SpecError{Err: errors.New("input and -constraints must be valid UTF-8")}
	}
	req := hostrpc.Request{Protocol: hostrpc.Protocol, Op: hostrpc.OpValidate, Validate: &hostrpc.Validation{State: a.state, Submission: sub, Binding: b}}
	if err := req.CheckText(); err != nil {
		return hostrpc.Validated{}, err
	}
	if path, err := hostrpc.SocketPath(a.state); err == nil {
		v, err := validateServing(ctx, &hostrpc.Client{Path: path}, *req.Validate)
		if err == nil || !hostrpc.Unavailable(err) {
			return v, err
		}
	}
	exe, err := a.runtimeExecutable()
	if err != nil {
		return hostrpc.Validated{}, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return hostrpc.Validated{}, err
	}
	if len(body) > preflightRequestLimit {
		return hostrpc.Validated{}, hostrpc.SpecError{Err: errors.New("runtime preflight request exceeds 1 MiB")}
	}
	output, err := runPreflight(ctx, exe, body, b)
	if err != nil {
		return hostrpc.Validated{}, err
	}
	return decodeValidation(output, b)
}

func runtimeEnvironment() []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !isCredential(name) {
			env = append(env, kv)
		}
	}
	return env
}

func submitCode(err error) int {
	if errors.As(err, new(hostrpc.SpecError)) {
		return exitUsage
	}
	return exitError
}

func (a *app) checkTask(label, goal, constraints string) error {
	hint := "; put a long specification in a workspace file and reference it from the goal"
	switch g := strings.TrimSpace(goal); {
	case g == "":
		return fmt.Errorf("%s is required", label)
	case !utf8.ValidString(goal) || !utf8.ValidString(constraints):
		return fmt.Errorf("%s and -constraints must be valid UTF-8", label)
	case len(g) > a.goalLimit:
		return fmt.Errorf("%s must be at most %d bytes%s", label, a.goalLimit, hint)
	case len(strings.TrimSpace(constraints)) > a.constraintsLimit:
		return fmt.Errorf("-constraints must be at most %d bytes%s", a.constraintsLimit, hint)
	}
	return nil
}

func (a *app) noteDiffers(input, run string, differs []string) {
	if len(differs) > 0 {
		fmt.Fprintf(a.stderr, "canti: input %s names run %s, which keeps its own settings; this submission differs in %s\n", input, run, strings.Join(differs, ", "))
	}
}

// Owned mode runs a separate foreground host for one input. Each host is
// reaped before the editor reopens, releasing admission between inputs.
// Auto hosts remain independent and survive a detached client.
func (a *app) closeHost() {
	if a.child == nil {
		return
	}
	defer func() { a.child = nil; a.childDone = nil }()
	_ = a.child.Process.Signal(syscall.SIGTERM)
	select {
	case <-a.childDone:
	case <-time.After(stopCleanup):
		_ = a.child.Process.Kill()
		<-a.childDone
	}
}

// validDelimiter is syntax of a line-input block, not a runtime identity rule.
func validDelimiter(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Validation is read-only, so a Hello/validation lost during host shutdown can
// be retried without creating state or admitting an input. Unavailable lets the
// caller use executable preflight; the caller's deadline bounds all retries.
func validateServing(ctx context.Context, c *hostrpc.Client, v hostrpc.Validation) (hostrpc.Validated, error) {
	for {
		result, err := c.Validate(ctx, v)
		if err == nil || !leaving(err) {
			return result, err
		}
		select {
		case <-ctx.Done():
			return hostrpc.Validated{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
