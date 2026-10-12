package docker

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
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

// request is the create request of the job of effect, whose run arguments
// hash to spec. The spec hash covers the run arguments alone, so a container
// of an earlier revision, which ran its script directly and has no
// exitLabel, still binds and is derived from Docker's exit code.
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

// create creates the container of effect for the platform that
// DOCKER_DEFAULT_PLATFORM names, if any, and pulls its image through the
// docker CLI when the daemon answers that it is not found, as docker create
// does. A name the daemon reports in use leaves the container that holds it
// to the caller's lookup: a create sent again after a lost response finds its
// own.
func (x *Executor) create(ctx context.Context, effect harness.Effect, spec string) error {
	req, err := x.request(effect, spec)
	if err != nil {
		return fmt.Errorf("docker create: %w", err)
	}
	path := "/containers/create?name=canti-" + effect.ID
	if p := os.Getenv("DOCKER_DEFAULT_PLATFORM"); p != "" {
		path += "&platform=" + url.QueryEscape(p)
	}
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
	cmd.WaitDelay = time.Second
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
