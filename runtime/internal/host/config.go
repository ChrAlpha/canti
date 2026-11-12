package host

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
)

// RunConfig is the host wiring stored with a run; every later command
// rebuilds the executor and model from it.
type RunConfig struct {
	Ephemeral          bool               `json:"ephemeral"`
	Environment        string             `json:"environment,omitempty"`
	EnvironmentWorkdir string             `json:"environment_workdir,omitempty"`
	Attachment         *docker.Attachment `json:"attachment,omitempty"`
	Workspace          string             `json:"workspace"`
	Image              string             `json:"image"`
	Network            string             `json:"network"`
	Memory             string             `json:"memory"`
	CPUs               string             `json:"cpus"`
	Pids               int                `json:"pids"`
	OutputLimit        int64              `json:"output_limit"`
	API                string             `json:"api"`
	Model              string             `json:"model"`
	Provider           string             `json:"provider,omitempty"`
	MaxTokens          int                `json:"max_tokens"`
	Effort             string             `json:"effort,omitempty"`
	Structured         bool               `json:"structured"`
	DecisionFormat     string             `json:"decision_format,omitempty"`
	Fallbacks          bool               `json:"fallbacks"`
	RequestBytes       int                `json:"request_bytes"`
	ContextVersion     int                `json:"context_version"`
	HistoryImports     string             `json:"history_imports,omitempty"`
}

// ContextVersion names the rules that decide the context of a run, which
// testdata/context-v<version>.txt records; see TestContextVersionPinsItsRules.
const ContextVersion = 20

// Spec is what a new run is created with: its stored configuration, the
// budgets the harness measures, and the core limits.
type Spec struct {
	Config RunConfig
	Budget harness.Budget
	Limits core.Limits
}

// NewRun checks s and returns the run of goal and constraints created at now,
// with its initial state.
func NewRun(goal, constraints string, s Spec, now time.Time) (harness.Run, core.State, error) {
	c, b := s.Config, s.Budget
	if err := CheckExecutionMode(c); err != nil {
		return harness.Run{}, core.State{}, err
	}
	if c.ContextVersion != ContextVersion {
		return harness.Run{}, core.State{}, fmt.Errorf("context version %d is unsupported; want %d", c.ContextVersion, ContextVersion)
	}
	switch {
	case c.RequestBytes < 1 || c.RequestBytes > harness.MaxRequestBytes:
		return harness.Run{}, core.State{}, errors.New("-request-bytes must be 1..8388608")
	case c.MaxTokens < 1 || c.OutputLimit < 1:
		return harness.Run{}, core.State{}, errors.New("-max-tokens and -output-limit must be positive")
	case c.DecisionFormat != "" && c.DecisionFormat != "text" && c.DecisionFormat != "tool":
		return harness.Run{}, core.State{}, errors.New("-decision-format must be text or tool")
	case c.Ephemeral && (c.Image == "" || c.Network == ""):
		return harness.Run{}, core.State{}, errors.New("-image and -network must not be empty")
	case min(int64(c.Pids), b.Tokens, b.OutputBytes, int64(b.WallTime), int64(b.JobTimeout)) < 0:
		return harness.Run{}, core.State{}, errors.New("limits and budgets must not be negative")
	}
	if err := CheckModel(c); err != nil {
		return harness.Run{}, core.State{}, err
	}
	if c.HistoryImports != "" {
		var err error
		if c.HistoryImports, err = CanonicalImports(c.HistoryImports); err != nil {
			return harness.Run{}, core.State{}, err
		}
	}
	var id [16]byte
	rand.Read(id[:])
	run := harness.Run{ID: "run_" + hex.EncodeToString(id[:]), Goal: goal, Constraints: constraints, Budget: b, Created: now}
	initial, err := core.NewState(run.ID, s.Limits)
	if err != nil {
		return harness.Run{}, core.State{}, fmt.Errorf("-max-steps must be nonnegative and -max-attempts must be positive")
	}
	err = harness.CheckOrientation(ContextConfig(c, b), run, s.Limits)
	if err != nil {
		return harness.Run{}, core.State{}, fmt.Errorf("-request-bytes: %w", err)
	}
	run.Config, err = json.Marshal(c)
	return run, initial, err
}

// CanonicalImports is the canonical form of dir, an absolute history imports
// directory: with every symbolic link resolved, so that every process that
// reads it, and every comparison of it, means the same directory. A relative
// dir is refused: only the process whose working directory it names can make
// it absolute, as a client does before it submits, and a host never resolves
// it against its own.
func CanonicalImports(dir string) (string, error) {
	if !filepath.IsAbs(dir) {
		return "", fmt.Errorf("history imports %q must be an absolute directory", dir)
	}
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", fmt.Errorf("history imports: %w", err)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("history imports must be a directory")
	}
	return canonical, nil
}

// ContextConfig is the loop configuration that decides the context of a run
// of c with budget b; a loop adds the paths of its state directory.
func ContextConfig(c RunConfig, b harness.Budget) harness.LoopConfig {
	return harness.LoopConfig{
		Environment: Environment(c, b), Limits: PromptLimits(c),
		Reader:       &archive.Reader{Imports: c.HistoryImports},
		DecisionTool: c.DecisionFormat == "tool", NativeArchive: c.Environment != "",
	}
}

// DecodeConfig returns the stored config of run, which must belong to
// workspace.
func DecodeConfig(run harness.Run, workspace string) (RunConfig, error) {
	var c RunConfig
	d := json.NewDecoder(bytes.NewReader(run.Config))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("run %s has an unreadable config: %w; runs of an earlier revision are not supported, so use a fresh -state", run.ID, err)
	}
	if _, err := d.Token(); err != io.EOF {
		return c, fmt.Errorf("run %s has trailing configuration data; use fresh state", run.ID)
	}
	if c.DecisionFormat != "" && c.DecisionFormat != "text" && c.DecisionFormat != "tool" {
		return c, fmt.Errorf("run %s has an unsupported decision format %q", run.ID, c.DecisionFormat)
	}
	if c.ContextVersion != ContextVersion {
		return c, fmt.Errorf("run %s uses unsupported context version %d; use fresh state", run.ID, c.ContextVersion)
	}
	if err := CheckProvider(c.Provider); err != nil {
		return c, fmt.Errorf("run %s has invalid stored provider: %w", run.ID, err)
	}
	if c.Workspace != workspace {
		return c, fmt.Errorf("run %s belongs to workspace %s, not %s", run.ID, c.Workspace, workspace)
	}
	return c, nil
}

// Unresumable explains an unsupported context version and its remedy.
func Unresumable(c RunConfig) string {
	if c.ContextVersion == ContextVersion {
		return ""
	}
	stored := "none"
	if c.ContextVersion != 0 {
		stored = strconv.Itoa(c.ContextVersion)
	}
	return fmt.Sprintf("uses context version %s, which this canti, of context version %d, does not support; use fresh state", stored, ContextVersion)
}

// PromptLimits are the prompt limits of a run of c.
func PromptLimits(c RunConfig) harness.PromptLimits {
	l := harness.DefaultPromptLimits()
	l.Total = c.RequestBytes
	return l
}

// HistoryRoot is the directory under state that the step records of every
// run are published in.
func HistoryRoot(state string) string { return filepath.Join(state, "history") }

// InputsRoot is the directory under state that the admitted inputs of the
// agent log are published in.
func InputsRoot(state string) string { return filepath.Join(state, "inputs") }

// ExecutorConfig is the executor configuration of a run of c in state.
func ExecutorConfig(c RunConfig, state string) docker.Config {
	return docker.Config{
		Image: c.Image, Workspace: c.Workspace, State: state, Network: c.Network,
		Memory: c.Memory, CPUs: c.CPUs, PidsLimit: c.Pids, OutputLimit: c.OutputLimit, PreviewLimit: PromptLimits(c).Preview,
		Mounts: []docker.Mount{{Source: HistoryRoot(state), Target: archive.RunsRoot}, {Source: InputsRoot(state), Target: archive.InputsRoot}},
	}
}

// NewExecutor creates the step record root that jobs mount and builds the
// executor.
func NewExecutor(c RunConfig, state string) (harness.Executor, error) {
	if err := CheckExecutionMode(c); err != nil {
		return nil, err
	}
	if c.Environment != "" {
		if c.Attachment == nil {
			return nil, errors.New("persistent execution requires a stored attachment")
		}
		return docker.NewAttached(AttachedConfig(c, state))
	}
	if err := os.MkdirAll(HistoryRoot(state), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(InputsRoot(state), 0o755); err != nil {
		return nil, err
	}
	return docker.New(ExecutorConfig(c, state))
}

// Environment describes to the agent where the commands of a run of c with
// budget b run.
func Environment(c RunConfig, b harness.Budget) string {
	if c.Environment != "" {
		return persistentEnvironment(c, b)
	}
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

// ErrNoModel reports a model call by a loop that may make none.
var ErrNoModel = errors.New("this command does not call the model")

// NoModel is the model of a loop that only stops or resolves, which renders
// and sends no request.
var NoModel harness.Model = noModel{}

type noModel struct{}

func (noModel) Freeze(harness.Prompt) (harness.Request, error) { return harness.Request{}, ErrNoModel }
func (noModel) Decision(harness.Response) []byte               { return nil }
func (noModel) Complete(context.Context, harness.Request) (harness.Response, error) {
	return harness.Response{}, ErrNoModel
}
