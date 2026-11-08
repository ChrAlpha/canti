package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

// registerSpec registers the flags of a new run on fs, which fill s.
func registerSpec(fs *flag.FlagSet, s *host.Spec) {
	c, b, l := &s.Config, &s.Budget, &s.Limits
	c.ContextVersion = host.ContextVersion
	fs.StringVar(&c.Image, "image", "debian:bookworm-slim", "container `image` of every job")
	fs.StringVar(&c.Network, "network", "none", "container `network` of every job")
	fs.StringVar(&c.Memory, "memory", "2g", "memory `limit` of every job; unlimited if empty")
	fs.StringVar(&c.CPUs, "cpus", "", "CPU `limit` of every job; unlimited if empty")
	fs.IntVar(&c.Pids, "pids", 512, "process `limit` of every job; 0 is unlimited")
	fs.Int64Var(&c.OutputLimit, "output-limit", 8<<20, "output `bytes` retained per job")
	fs.StringVar(&c.API, "api", "", "model `API` of the run: "+strings.Join(host.APINames, " or ")+"; required")
	fs.StringVar(&c.Model, "model", "", "`model` of the run; required")
	fs.IntVar(&c.MaxTokens, "max-tokens", 16000, "output `tokens` per model response")
	fs.StringVar(&c.Effort, "effort", "", "model `effort`, a level of the model API; empty sends none, leaving the model's default")
	fs.BoolVar(&c.Structured, "structured", true, "constrain text responses to the decision schema")
	fs.StringVar(&c.DecisionFormat, "decision-format", "", "decision transport: text (default) or tool")
	fs.BoolVar(&c.Fallbacks, "fallbacks", false, "enable server-side refusal fallbacks of "+host.APIMessages)
	fs.IntVar(&c.RequestBytes, "request-bytes", 8<<20, "resource `bytes` for rendered and serialized requests (1..8388608); does not predict provider context fit")
	fs.StringVar(&c.HistoryImports, "history-imports", "", "read-only imported history `directory` exposed under "+archive.ImportsRoot)
	fs.IntVar(&l.MaxSteps, "max-steps", 20, "decision `steps` per run; 0 is unlimited")
	fs.IntVar(&l.MaxAttempts, "max-attempts", 3, "model `attempts` per step")
	fs.DurationVar(&b.WallTime, "wall-time", 30*time.Minute, "run `time` budget; 0 is unlimited")
	fs.Int64Var(&b.Tokens, "token-budget", 0, "retrospective stop threshold on reported model `tokens`; last call may overshoot and missing usage remains unknown; 0 disables")
	fs.Int64Var(&b.OutputBytes, "output-budget", 256<<20, "job output `bytes` per run; 0 is unlimited")
	fs.DurationVar(&b.JobTimeout, "job-timeout", 10*time.Minute, "run `time` of one job; 0 is unlimited")
}

// defaultState is the state directory of workspace unless one is named.
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

// canonicalImports makes the history imports directory of c, which a flag
// names relative to this process's working directory, absolute, before
// anything sends it to a host, which never resolves a relative directory
// against its own, and canonical, as the workspace is, when it exists. A
// submission that is surely new, as one without -input is, needs the
// directory to exist; one with -input may retry an admitted run, whose
// directory may since be gone, so the host decides it: it answers a retry
// first, and refuses only a new run whose directory is missing.
func canonicalImports(c *host.RunConfig, mayRetry bool) error {
	if c.HistoryImports == "" {
		return nil
	}
	dir, err := filepath.Abs(c.HistoryImports)
	if err != nil {
		return fmt.Errorf("-history-imports: %w", err)
	}
	canonical, err := host.CanonicalImports(dir)
	switch {
	case err == nil:
		c.HistoryImports = canonical
	case mayRetry:
		c.HistoryImports = dir
	default:
		return fmt.Errorf("-history-imports: %w", err)
	}
	return nil
}
