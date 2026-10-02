package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/anthropic"
	"github.com/ChrAlpha/canti/internal/harness/docker"
)

// config is the host wiring stored with a run; every later command rebuilds
// the executor and model from it.
type config struct {
	Workspace      string `json:"workspace"`
	Image          string `json:"image"`
	Network        string `json:"network"`
	Memory         string `json:"memory"`
	CPUs           string `json:"cpus"`
	Pids           int    `json:"pids"`
	OutputLimit    int64  `json:"output_limit"`
	Model          string `json:"model"`
	MaxTokens      int    `json:"max_tokens"`
	Structured     bool   `json:"structured"`
	Fallbacks      bool   `json:"fallbacks"`
	ContextBytes   int    `json:"context_bytes"`
	ContextVersion int    `json:"context_version"`
	HistoryImports string `json:"history_imports,omitempty"`
	WindowSteps    int    `json:"window_steps,omitempty"`
	WindowBytes    int    `json:"window_bytes,omitempty"`
	HandoverBytes  int    `json:"handover_bytes,omitempty"`
}

const minContextBytes = 64 << 10
const contextVersion = 1

type spec struct {
	config config
	budget harness.Budget
	limits core.Limits
}

func (s *spec) register(fs *flag.FlagSet) {
	c, b, l := &s.config, &s.budget, &s.limits
	c.ContextVersion = contextVersion
	fs.StringVar(&c.Image, "image", "debian:bookworm-slim", "container `image` of every job")
	fs.StringVar(&c.Network, "network", "none", "container `network` of every job")
	fs.StringVar(&c.Memory, "memory", "2g", "memory `limit` of every job; unlimited if empty")
	fs.StringVar(&c.CPUs, "cpus", "", "CPU `limit` of every job; unlimited if empty")
	fs.IntVar(&c.Pids, "pids", 512, "process `limit` of every job; 0 is unlimited")
	fs.Int64Var(&c.OutputLimit, "output-limit", 8<<20, "output `bytes` retained per job")
	fs.StringVar(&c.Model, "model", "claude-opus-5", "Anthropic `model`")
	fs.IntVar(&c.MaxTokens, "max-tokens", 16000, "output `tokens` per model response")
	fs.BoolVar(&c.Structured, "structured", true, "constrain responses to the decision schema")
	fs.BoolVar(&c.Fallbacks, "fallbacks", true, "enable server-side refusal fallbacks")
	fs.IntVar(&c.ContextBytes, "context-bytes", 256<<10, "prompt `bytes` per model request, including the active handover window; at least 65536")
	fs.StringVar(&c.HistoryImports, "history-imports", "", "read-only imported history `directory` exposed under /canti/imports")
	fs.IntVar(&c.WindowSteps, "window-steps", 0, "request handover after this many completed steps per window; 0 disables step trigger")
	fs.IntVar(&c.WindowBytes, "window-bytes", 0, "request handover at this many rendered local-history bytes; 0 disables byte trigger")
	fs.IntVar(&c.HandoverBytes, "handover-bytes", 2048, "maximum handover UTF-8 `bytes` (1..8192)")
	fs.IntVar(&l.MaxSteps, "max-steps", 20, "decision `steps` per run; 0 is unlimited")
	fs.IntVar(&l.MaxAttempts, "max-attempts", 3, "model `attempts` per step")
	fs.DurationVar(&b.WallTime, "wall-time", 30*time.Minute, "run `time` budget; 0 is unlimited")
	fs.Int64Var(&b.Tokens, "token-budget", 0, "model `tokens` per run; 0 is unlimited")
	fs.Int64Var(&b.OutputBytes, "output-budget", 256<<20, "job output `bytes` per run; 0 is unlimited")
	fs.DurationVar(&b.JobTimeout, "job-timeout", 10*time.Minute, "run `time` of one job; 0 is unlimited")
}

func newRun(goal, constraints string, s spec, now time.Time) (harness.Run, core.State, error) {
	c, b := s.config, s.budget
	switch {
	case c.ContextBytes < minContextBytes:
		return harness.Run{}, core.State{}, fmt.Errorf("-context-bytes must be at least %d", minContextBytes)
	case c.WindowSteps < 0 || c.WindowBytes < 0 || c.HandoverBytes < 1 || c.HandoverBytes > 8192:
		return harness.Run{}, core.State{}, errors.New("window limits must be nonnegative and handover-bytes must be 1..8192")
	case c.MaxTokens < 1 || c.OutputLimit < 1:
		return harness.Run{}, core.State{}, errors.New("-max-tokens and -output-limit must be positive")
	case c.Image == "" || c.Network == "" || c.Model == "":
		return harness.Run{}, core.State{}, errors.New("-image, -network, and -model must not be empty")
	case min(int64(c.Pids), b.Tokens, b.OutputBytes, int64(b.WallTime), int64(b.JobTimeout)) < 0:
		return harness.Run{}, core.State{}, errors.New("limits and budgets must not be negative")
	}
	if c.HistoryImports != "" {
		var err error
		c.HistoryImports, err = filepath.Abs(c.HistoryImports)
		if err == nil {
			c.HistoryImports, err = filepath.EvalSymlinks(c.HistoryImports)
		}
		if err != nil {
			return harness.Run{}, core.State{}, fmt.Errorf("history imports: %w", err)
		}
		info, err := os.Stat(c.HistoryImports)
		if err != nil {
			return harness.Run{}, core.State{}, err
		}
		if !info.IsDir() {
			return harness.Run{}, core.State{}, errors.New("history imports must be a directory")
		}
	}
	var id [16]byte
	rand.Read(id[:])
	run := harness.Run{ID: "run_" + hex.EncodeToString(id[:]), Goal: goal, Constraints: constraints, Budget: b, Created: now}
	initial, err := core.NewState(run.ID, s.limits)
	if err != nil {
		return harness.Run{}, core.State{}, fmt.Errorf("-max-steps must be nonnegative and -max-attempts must be positive")
	}
	f := harness.Frame{
		Goal: goal, Constraints: constraints, Environment: environment(c, b),
		Step: 1, MaxSteps: s.limits.MaxSteps, Attempt: 1, MaxAttempts: s.limits.MaxAttempts,
		RunID: run.ID, ArtifactDir: docker.ArtifactDir, RunsDir: runsDir,
		Window: &harness.Window{FirstStep: 1}, HandoverBytes: c.HandoverBytes,
		RetrievalEnabled: true, ImportsEnabled: c.HistoryImports != "",
	}
	if err := f.ValidateContext(promptLimits(c)); err != nil {
		return harness.Run{}, core.State{}, fmt.Errorf("-context-bytes: %w", err)
	}
	run.Config, err = json.Marshal(c)
	return run, initial, err
}

// decodeConfig returns the stored config of run, which must belong to
// workspace.
func decodeConfig(run harness.Run, workspace string) (config, error) {
	var c config
	d := json.NewDecoder(bytes.NewReader(run.Config))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("run %s has an unreadable config: %w; runs of an earlier revision are not supported, so use a fresh -state", run.ID, err)
	}
	if c.ContextVersion != contextVersion {
		return c, fmt.Errorf("run %s uses an unsupported context version; use a fresh -state", run.ID)
	}
	if c.Workspace != workspace {
		return c, fmt.Errorf("run %s belongs to workspace %s, not %s", run.ID, c.Workspace, workspace)
	}
	return c, nil
}

func promptLimits(c config) harness.PromptLimits {
	l := harness.DefaultPromptLimits()
	l.Total = c.ContextBytes
	return l
}

// runsDir is where every job reads, read-only, the step records of every run
// in the state directory.
const runsDir = "/canti/runs"

func historyRoot(state string) string { return filepath.Join(state, "history") }

func executorConfig(c config, state string) docker.Config {
	return docker.Config{
		Image: c.Image, Workspace: c.Workspace, State: state, Network: c.Network,
		Memory: c.Memory, CPUs: c.CPUs, PidsLimit: c.Pids, OutputLimit: c.OutputLimit,
		Mounts: []docker.Mount{{Source: historyRoot(state), Target: runsDir}},
	}
}

// newExecutor creates the step record root that jobs mount and builds the
// executor.
func newExecutor(c config, state string) (*docker.Executor, error) {
	if err := os.MkdirAll(historyRoot(state), 0o755); err != nil {
		return nil, err
	}
	return docker.New(executorConfig(c, state))
}

type credential struct{ apiKey, authToken string }

// credentials reads the model credential and removes it from the
// environment, so that no child process inherits it.
func credentials() (credential, error) {
	c := credential{apiKey: os.Getenv("ANTHROPIC_API_KEY"), authToken: os.Getenv("ANTHROPIC_AUTH_TOKEN")}
	os.Unsetenv("ANTHROPIC_API_KEY")
	os.Unsetenv("ANTHROPIC_AUTH_TOKEN")
	switch {
	case c.apiKey != "":
		return credential{apiKey: c.apiKey}, nil
	case c.authToken != "":
		return c, nil
	}
	return c, errors.New("set ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN to call the model")
}

func newModel(c config, url string, cred credential) (harness.Model, error) {
	return anthropic.New(modelConfig(c, cmp.Or(url, os.Getenv("ANTHROPIC_BASE_URL")), cred))
}

func modelConfig(c config, url string, cred credential) anthropic.Config {
	return anthropic.Config{
		BaseURL: url, APIKey: cred.apiKey, AuthToken: cred.authToken, Model: c.Model,
		MaxTokens: c.MaxTokens, StructuredOutput: c.Structured, Fallbacks: c.Fallbacks,
	}
}

func environment(c config, b harness.Budget) string {
	pids, timeout := "unlimited", "unlimited"
	if c.Pids > 0 {
		pids = strconv.Itoa(c.Pids)
	}
	if b.JobTimeout > 0 {
		timeout = b.JobTimeout.String()
	}
	return fmt.Sprintf("Each command runs in a new Linux container from image %s with working directory %s, "+
		"which is read-write and persists between steps. "+
		"Network: %s. Per-command limits: memory %s, CPUs %s, processes %s, run time %s.",
		c.Image, docker.WorkDir, c.Network,
		cmp.Or(c.Memory, "unlimited"), cmp.Or(c.CPUs, "unlimited"), pids, timeout)
}

func defaultState(workspace string) (string, error) {
	base := os.Getenv("XDG_STATE_HOME")
	if !filepath.IsAbs(base) {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("no state directory: %w", err)
		}
		base = filepath.Join(home, ".local", "state")
	}
	sum := sha256.Sum256([]byte(workspace))
	return filepath.Join(base, "canti", hex.EncodeToString(sum[:8])), nil
}

var errNoModel = errors.New("this command does not call the model")

func withoutModel(config) (harness.Model, error) { return noModel{}, nil }

type noModel struct{}

func (noModel) Freeze(harness.Prompt) (harness.Request, error) { return harness.Request{}, errNoModel }
func (noModel) Decision(harness.Response) []byte               { return nil }
func (noModel) Complete(context.Context, harness.Request) (harness.Response, error) {
	return harness.Response{}, errNoModel
}
