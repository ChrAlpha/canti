package chat

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

const (
	Owned       = "owned"
	Auto        = "auto"
	autoIdle    = 10 * time.Minute
	maxStarts   = 4
	stopCleanup = 30 * time.Second
)

var ErrLocked = errors.New("workspace locked")
var credentialVariables = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "OPENAI_API_KEY"}

type runtimeClient struct {
	workspace, state, runtimePath, hostMode string
	hostWait                                time.Duration
	env                                     []string
	logLevel                                string
	child                                   *exec.Cmd
	childDone                               chan error
	starting                                <-chan struct{}
}

func (a *runtimeClient) checkHostMode() error {
	if a.hostMode != Owned && a.hostMode != Auto {
		return fmt.Errorf("-host must be %s or %s", Owned, Auto)
	}
	if a.hostWait <= 0 {
		return errors.New("-host-wait must be positive")
	}
	return nil
}

func leaving(err error) bool { return errors.Is(err, hostrpc.ErrClosing) || hostrpc.Lost(err) }

func (a *runtimeClient) connect(ctx context.Context) (*hostrpc.Client, error) {
	if a.hostMode == Owned && a.child == nil && locked(a.state) {
		return nil, fmt.Errorf("%w: owned mode requires exclusive runtime ownership; use -host auto to attach", ErrLocked)
	}
	path, err := hostrpc.SocketPath(a.state)
	if err != nil {
		return nil, err
	}
	c := &hostrpc.Client{Path: path}
	err = a.hello(ctx, c)
	if err == nil || !hostrpc.Unavailable(err) && !leaving(err) {
		return c, err
	}

	starts := 0
	deadline := time.Now().Add(a.hostWait)
	for {
		err := a.hello(ctx, c)
		switch {
		case err == nil || !hostrpc.Unavailable(err) && !leaving(err):
			return c, err
		case hostrpc.Unavailable(err) && starts < maxStarts && !locked(a.state) && a.canStartHost():
			// The host that held the state directory, or the one this client
			// started, has exited without serving: one that was shutting down
			// as this client arrived, or one that lost the lock to it. Start
			// the next one.
			if err := a.startHost(); err != nil {
				return nil, err
			}
			starts++
		case time.Now().After(deadline):
			if locked(a.state) {
				return nil, fmt.Errorf("workspace %s is %w and no host serves it", a.workspace, ErrLocked)
			}
			return nil, fmt.Errorf("no host started serving workspace %s within %v; see %s", a.workspace, a.hostWait, filepath.Join(a.state, "host.log"))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func locked(state string) bool {
	f, err := os.Open(filepath.Join(state, "lock"))
	if err != nil {
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

func (a *runtimeClient) startHost() error {
	exe, err := a.runtimeExecutable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(a.state, 0o700); err != nil {
		return err
	}
	logPath := filepath.Join(a.state, "host.log")
	if info, err := os.Stat(logPath); err == nil && info.Size() > 4<<20 {
		os.Rename(logPath, logPath+".1")
	}
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(exe, "host", "-workspace", a.workspace, "-state", a.state, "-idle", autoIdle.String(), "-log-level", strings.ToLower(a.logLevel))
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if a.hostMode == Owned {
		cmd.SysProcAttr.Pdeathsig = syscall.SIGTERM
	}
	for _, kv := range a.env {
		if name, _, _ := strings.Cut(kv, "="); !isCredential(name) {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	a.starting = done
	if a.hostMode == Owned {
		exited := make(chan error, 1)
		a.child, a.childDone = cmd, exited
		go func() { exited <- cmd.Wait(); close(done) }()
	} else {
		go func() { cmd.Wait(); close(done) }()
	}
	return nil
}

func isCredential(name string) bool {
	for _, v := range credentialVariables {
		if name == v {
			return true
		}
	}
	return false
}

func (a *runtimeClient) retry(ctx context.Context, op func(*hostrpc.Client) error) error {
	var err error
	for range 5 {
		var c *hostrpc.Client
		if c, err = a.connect(ctx); err != nil {
			return err
		}
		// A request that reached no host, whose answer was lost, or that a
		// host shutting down refused without acting on it, is sent again,
		// to a host started anew if the old one went away.
		if err = op(c); err == nil || !hostrpc.Lost(err) && !hostrpc.Unavailable(err) && !errors.Is(err, hostrpc.ErrClosing) {
			return err
		}
	}
	return err
}

func (a *runtimeClient) canStartHost() bool {
	if a.starting != nil {
		select {
		case <-a.starting:
			a.starting = nil
		default:
			return false
		}
	}
	if a.child == nil {
		return true
	}
	select {
	case <-a.childDone:
		a.child = nil
		a.childDone = nil
		return true
	default:
		return false
	}
}

func (a *runtimeClient) hello(ctx context.Context, c *hostrpc.Client) error {
	h, err := c.Hello(ctx)
	if err == nil && a.hostMode == Owned && (a.child == nil || h.PID != a.child.Process.Pid) {
		return fmt.Errorf("%w: owned mode requires its own runtime; use -host auto to attach to the serving host", ErrLocked)
	}
	return err
}

func (a *runtimeClient) runtimeExecutable() (string, error) {
	path, err := exec.LookPath(cmp.Or(a.runtimePath, "canti"))
	if err != nil {
		return "", fmt.Errorf("find canti runtime: %w; install canti on PATH or set -runtime PATH", err)
	}
	return filepath.Abs(path)
}

func (a *runtimeClient) validate(ctx context.Context, sub hostrpc.Submission, b hostrpc.Binding) (hostrpc.Validated, error) {
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
	output, err := runPreflight(ctx, exe, body, b, a.env)
	if err != nil {
		return hostrpc.Validated{}, err
	}
	return decodeValidation(output, b)
}

func (a *runtimeClient) closeHost(ctx context.Context) {
	if a.child == nil {
		return
	}
	defer func() { a.child = nil; a.childDone = nil }()
	_ = a.child.Process.Signal(syscall.SIGTERM)
	select {
	case <-a.childDone:
		return
	case <-ctx.Done():
	case <-time.After(stopCleanup):
	}
	_ = a.child.Process.Kill()
	<-a.childDone
}

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
