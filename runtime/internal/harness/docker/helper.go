package docker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

// checkHelper verifies that the host helper speaks the current protocol and
// that the image can execute a host-built binary. It runs before the launch
// latch, so a mismatch is a configuration error and never a silent job.
func (x *Executor) checkHelper(ctx context.Context) error {
	versionCall, cancelVersion := context.WithTimeout(ctx, 10*time.Second)
	defer cancelVersion()
	cmd := exec.CommandContext(versionCall, x.cfg.Helper, "-version")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=/nonexistent"}
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("docker: run canti-job %s: %w", x.cfg.Helper, err)
	}
	if strings.TrimSpace(string(out)) != fmt.Sprint(jobrunner.Protocol) {
		return fmt.Errorf("docker: canti-job %s does not speak protocol %d", x.cfg.Helper, jobrunner.Protocol)
	}
	imageCall, cancelImage := context.WithTimeout(ctx, x.timeouts.transfer)
	defer cancelImage()
	var image struct{ Os, Architecture string }
	err = x.api(imageCall, "image", http.MethodGet, "/images/"+url.PathEscape(x.cfg.Image)+"/json", nil, decodeResponse(&image))
	if status(err) == http.StatusNotFound {
		err = x.pull(imageCall)
		if err == nil {
			err = x.api(imageCall, "image", http.MethodGet, "/images/"+url.PathEscape(x.cfg.Image)+"/json", nil, decodeResponse(&image))
		}
	}
	if err != nil {
		return fmt.Errorf("docker: inspect helper image %s: %w", x.cfg.Image, err)
	}
	if image.Os != runtime.GOOS || image.Architecture != runtime.GOARCH {
		return fmt.Errorf("docker: image %s is %s/%s and cannot run the host canti-job helper (%s/%s); use a matching image",
			x.cfg.Image, image.Os, image.Architecture, runtime.GOOS, runtime.GOARCH)
	}
	return nil
}

// DefaultHelperPath resolves the helper used by a new ephemeral run.
func DefaultHelperPath() string {
	if path := os.Getenv("CANTI_JOB_HELPER"); path != "" {
		return path
	}
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Join(filepath.Dir(exe), "canti-job")
}

// DefaultHelperIdentity returns the content digest and protocol of the
// default helper when it is available.
func DefaultHelperIdentity() (string, int, error) {
	path := DefaultHelperPath()
	if path == "" {
		return "", 0, errors.New("docker: helper path unavailable")
	}
	digest, err := jobrunner.HelperDigest(path)
	if err != nil {
		return "", 0, err
	}
	return digest, jobrunner.Protocol, nil
}

// pinHelper installs the verified helper as content-addressed state and makes
// every launch bind that copy. Replacing the original path after New cannot
// change the bytes a pinned effect starts.
func pinHelper(cfg Config) (Config, error) {
	if cfg.HelperDigest == "" {
		return Config{}, errors.New("docker: helper identity is not pinned")
	}
	dir := filepath.Join(cfg.State, "helpers", cfg.HelperDigest)
	path := filepath.Join(dir, "canti-job")
	if digest, err := jobrunner.HelperDigest(path); err == nil {
		if digest != cfg.HelperDigest {
			return Config{}, errors.New("docker: stored helper content differs from its pinned digest")
		}
		cfg.Helper = path
		return cfg, nil
	}
	if _, err := os.Lstat(path); err == nil {
		return Config{}, errors.New("docker: stored helper is unreadable or not a regular file")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return Config{}, err
	}
	src, err := os.Open(cfg.Helper)
	if err != nil {
		return Config{}, fmt.Errorf("docker: open helper for pinning: %w", err)
	}
	defer src.Close()
	tmp, err := os.CreateTemp(dir, ".canti-job-*")
	if err != nil {
		return Config{}, err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), src)
	if err != nil {
		tmp.Close()
		return Config{}, err
	}
	if n == 0 || hex.EncodeToString(h.Sum(nil)) != cfg.HelperDigest {
		tmp.Close()
		return Config{}, errors.New("docker: helper changed before it could be pinned")
	}
	if err = tmp.Chmod(0555); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return Config{}, err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return Config{}, err
	}
	if err = syncDir(dir); err != nil {
		return Config{}, err
	}
	cfg.Helper = path
	return cfg, nil
}
