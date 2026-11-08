package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

const preflightRequestLimit = 1 << 20
const preflightOutputLimit = 1 << 20
const preflightDiagnosticLimit = 16 << 10
const preflightTimeout = 30 * time.Second
const preflightWaitDelay = time.Second

var errPreflightOutput = errors.New("runtime preflight output exceeds its limit")

// preflightOutput caps retained bytes and ends the process on overflow. Each
// stream has its own cap; a flood on stderr cannot consume stdout's allowance.
type preflightOutput struct {
	mu       sync.Mutex
	data     []byte
	limit    int
	overflow bool
	cancel   context.CancelFunc
}

func (w *preflightOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	n := min(len(p), w.limit-len(w.data))
	w.data = append(w.data, p[:n]...)
	exceeded := n < len(p)
	w.overflow = w.overflow || exceeded
	w.mu.Unlock()
	if exceeded {
		w.cancel()
		return n, errPreflightOutput
	}
	return n, nil
}

func (w *preflightOutput) result() ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.data), w.overflow
}

// runPreflight kills the entire subprocess group on cancellation, timeout or
// output overflow. WaitDelay bounds inherited pipes even if a descendant has
// escaped that group. It never retains unbounded output or returns raw keys.
func runPreflight(ctx context.Context, exe string, body []byte, b hostrpc.Binding) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()
	output := &preflightOutput{limit: preflightOutputLimit, cancel: cancel}
	diagnostic := &preflightOutput{limit: preflightDiagnosticLimit, cancel: cancel}
	cmd := exec.CommandContext(ctx, exe, "preflight")
	cmd.Stdin = bytes.NewReader(body)
	cmd.Env = runtimeEnvironment()
	cmd.Stdout, cmd.Stderr = output, diagnostic
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = preflightWaitDelay
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	err := cmd.Run()
	if err != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	out, overflow := output.result()
	note, diagnosticOverflow := diagnostic.result()
	if overflow {
		return nil, fmt.Errorf("%w: stdout exceeds %d bytes", errPreflightOutput, preflightOutputLimit)
	}
	if diagnosticOverflow {
		return nil, fmt.Errorf("%w: stderr exceeds %d bytes", errPreflightOutput, preflightDiagnosticLimit)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("runtime preflight: %w", ctx.Err())
	}
	if err != nil {
		return nil, fmt.Errorf("runtime preflight: %w: %s; install matching canti and canti-tui executables", err, redactBinding(strings.TrimSpace(string(note)), b))
	}
	return out, nil
}

func redactBinding(s string, b hostrpc.Binding) string {
	for _, key := range []string{b.AnthropicKey, b.AnthropicToken, b.OpenAIKey} {
		if key != "" {
			s = strings.ReplaceAll(s, key, "[redacted]")
		}
	}
	return s
}

// decodeValidation rejects trailing values, unknown fields and ambiguous
// success/error responses. Other protocol versions are refused before decoding
// their fields, with the same installation remedy as socket mismatches.
func decodeValidation(body []byte, b hostrpc.Binding) (hostrpc.Validated, error) {
	var version struct {
		Protocol int `json:"protocol"`
	}
	if err := json.Unmarshal(body, &version); err != nil {
		return hostrpc.Validated{}, fmt.Errorf("runtime preflight response: %w", err)
	}
	if version.Protocol != hostrpc.Protocol {
		return hostrpc.Validated{}, fmt.Errorf("%w; install matching canti and canti-tui executables", hostrpc.ErrProtocol)
	}
	var resp struct {
		Protocol  int                `json:"protocol"`
		Error     *hostrpc.Error     `json:"error,omitempty"`
		Validated *hostrpc.Validated `json:"validated,omitempty"`
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&resp); err != nil {
		return hostrpc.Validated{}, fmt.Errorf("runtime preflight response: %w", err)
	}
	if resp.Error != nil && resp.Validated != nil {
		return hostrpc.Validated{}, errors.New("runtime preflight returned both validation and error")
	}
	if e := resp.Error; e != nil {
		if e.Code == hostrpc.CodeProtocol {
			return hostrpc.Validated{}, fmt.Errorf("%w; install matching canti and canti-tui executables", hostrpc.ErrProtocol)
		}
		message := redactBinding(e.Message, b)
		if e.Code == hostrpc.CodeSpec {
			return hostrpc.Validated{}, hostrpc.SpecError{Err: errors.New(message)}
		}
		return hostrpc.Validated{}, &hostrpc.RemoteError{Code: e.Code, Message: message}
	}
	if resp.Validated == nil {
		return hostrpc.Validated{}, errors.New("runtime preflight returned no validation")
	}
	return *resp.Validated, nil
}
