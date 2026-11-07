package docker

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/internal/harness"
)

// createRequest is the container that the docker CLI creates for the run
// arguments of a job, under wrapper, with its identity labels.
type createRequest struct {
	User         string
	AttachStdout bool
	AttachStderr bool
	Env          []string
	Cmd          []string
	Image        string
	WorkingDir   string
	Entrypoint   []string
	Labels       map[string]string
	HostConfig   struct {
		Binds     []string
		LogConfig struct {
			Type   string
			Config map[string]string
		}
		NetworkMode string
		CapDrop     []string
		SecurityOpt []string
		Memory      int64  `json:",omitempty"`
		NanoCpus    int64  `json:",omitempty"`
		PidsLimit   *int64 `json:",omitempty"`
		Init        bool
	}
}

// request is the canonical container configuration and identity labels.
func (x *Executor) request(effect harness.Effect, spec string) (createRequest, error) {
	c := x.cfg
	r := createRequest{User: c.User, AttachStdout: true, AttachStderr: true, Image: c.Image, WorkingDir: WorkDir, Entrypoint: []string{"/bin/sh"},
		Env: []string{"TINI_KILL_PROCESS_GROUP=1"}, Cmd: []string{"-c", wrapper, "canti", script(effect.Command)},
		Labels: map[string]string{"canti.effect": effect.ID, "canti.state": c.State, "canti.spec": spec, exitLabel: exitFile}}
	for _, k := range proxies {
		r.Env = append(r.Env, k+"=", strings.ToLower(k)+"=")
	}
	h := &r.HostConfig
	h.Binds, h.NetworkMode, h.CapDrop, h.SecurityOpt, h.Init = x.binds(), c.Network, []string{"ALL"}, []string{"no-new-privileges"}, true
	h.LogConfig.Type, h.LogConfig.Config = "json-file", map[string]string{"max-size": "1p", "mode": "blocking"}
	if c.PidsLimit > 0 {
		n := int64(c.PidsLimit)
		h.PidsLimit = &n
	}
	var err error
	if c.Memory != "" {
		h.Memory, err = ramBytes(c.Memory)
	}
	if err == nil && c.CPUs != "" {
		h.NanoCpus, err = nanoCPUs(c.CPUs)
	}
	return r, err
}

// create creates the deterministic owner, pulling the image if necessary.
// An ambiguous reply is recovered by the caller's identity-checked lookup.
func (x *Executor) create(ctx context.Context, effect harness.Effect, spec string) error {
	req, err := x.request(effect, spec)
	if err != nil {
		return fmt.Errorf("docker create: %w", err)
	}
	path := "/containers/create?name=canti-" + effect.ID
	err = x.api(ctx, "create", http.MethodPost, path, req, nil)
	if status(err) == http.StatusNotFound {
		if err = x.pull(ctx); err == nil {
			err = x.api(ctx, "create", http.MethodPost, path, req, nil)
		}
	}
	if status(err) == http.StatusConflict {
		return nil
	}
	return err
}

// pull pulls the image through the docker CLI, which holds the registry
// credentials, under its own deadline.
func (x *Executor) pull(ctx context.Context) error {
	call, cancel := context.WithTimeout(ctx, x.timeouts.transfer)
	defer cancel()
	cmd := exec.CommandContext(call, x.cfg.Docker, "pull", "--quiet", x.cfg.Image)
	// Platform selection follows the daemon/image defaults, independent of the
	// ambient CLI override, just as the Engine create request does.
	cmd.Env = make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "DOCKER_DEFAULT_PLATFORM=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.WaitDelay = time.Second
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	errOut := &tail{}
	cmd.Stderr = errOut
	err := cmd.Run()
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case call.Err() != nil:
		return fmt.Errorf("docker pull: no response within %v", x.timeouts.transfer)
	}
	if s := errOut.String(); s != "" {
		err = fmt.Errorf("%w: %s", err, s)
	}
	return fmt.Errorf("docker pull: %w", err)
}

// ramBytes parses a memory limit as the docker CLI's --memory does: a
// non-negative number, optionally followed by a space and by b or a binary
// unit k, m, g, t, or p with an optional b or ib, case-insensitively.
func ramBytes(s string) (int64, error) {
	sep := strings.LastIndexAny(s, "0123456789. ")
	if sep == -1 {
		return 0, fmt.Errorf("invalid size: '%s'", s)
	}
	num, unit := s[:sep+1], strings.ToLower(s[sep+1:])
	if s[sep] == ' ' {
		num = s[:sep]
	}
	size, err := strconv.ParseFloat(num, 64)
	switch {
	case err != nil:
		return 0, err
	case size < 0:
		return 0, fmt.Errorf("invalid size: '%s'", s)
	case unit == "" || unit == "b":
		return int64(size), nil
	}
	shift := strings.IndexByte("kmgtp", unit[0])
	if shift < 0 || len(unit) > 3 || len(unit) == 2 && unit[1] != 'b' || len(unit) == 3 && unit[1:] != "ib" {
		return 0, fmt.Errorf("invalid suffix: '%s'", unit)
	}
	return int64(size * float64(int64(1)<<(10*(shift+1)))), nil
}

// nanoCPUs parses a CPU limit as the docker CLI's --cpus does: a rational
// number whose billionths are whole.
func nanoCPUs(s string) (int64, error) {
	cpu, ok := new(big.Rat).SetString(s)
	if !ok {
		return 0, fmt.Errorf("failed to parse %v as a rational number", s)
	}
	nano := cpu.Mul(cpu, big.NewRat(1e9, 1))
	if !nano.IsInt() {
		return 0, fmt.Errorf("value is too precise")
	}
	return nano.Num().Int64(), nil
}
