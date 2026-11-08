// Package jobrunner captures one foreground shell independently of its controller.
// It knows no journal, Docker API, model, or run transitions.
package jobrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const Protocol = 1
const MetadataLimit = 16 << 10

// Config is supplied in the bound exec's arguments, never read from job files.
type Config struct {
	Directory, EffectID, Spec, Command, Workdir, Init string
	Generation                                        uint64
	OutputLimit                                       int64
	PreviewLimit                                      int
}

// Record is mutable-environment evidence. Only the external controller can turn
// it, together with independent process evidence, into an authoritative receipt.
type Record struct {
	Protocol                      int
	EffectID, Spec                string
	Generation                    uint64
	StartedAt, FinishedAt         time.Time
	ExitCode                      *int
	Bytes, Stored                 int64
	Digest, Preview, CaptureError string
	Incomplete                    bool
}

func (c Config) validate() error {
	if !filepath.IsAbs(c.Directory) || !filepath.IsAbs(c.Workdir) || c.EffectID == "" || c.Generation == 0 || c.OutputLimit < 1 || c.PreviewLimit < 64 || c.PreviewLimit > 4096 {
		return errors.New("job runner: invalid identity, directory or capture limits")
	}
	b, err := hex.DecodeString(c.Spec)
	if err != nil || len(b) != sha256.Size || hex.EncodeToString(b) != c.Spec {
		return errors.New("job runner: invalid specification")
	}
	if c.Init != "" && !filepath.IsAbs(c.Init) {
		return errors.New("job runner: init must be absolute")
	}
	return nil
}

// Run launches a fresh shell, bounds retained output, and publishes a synced
// completion. An error never represents a command's exit code.
func Run(c Config) (Record, error) {
	if err := c.validate(); err != nil {
		return Record{}, err
	}
	root, err := os.OpenRoot(c.Directory)
	if err != nil {
		return Record{}, err
	}
	defer root.Close()
	out, err := root.OpenFile("output", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return Record{}, err
	}
	defer out.Close()
	reader, writer, err := os.Pipe()
	if err != nil {
		return Record{}, err
	}
	defer reader.Close()
	defer writer.Close()
	script := c.Command
	if c.Init != "" {
		script = ". \"$1\" || exit $?\ncd -- \"$2\" || exit $?\n" + script
	}
	cmd := exec.Command("/bin/sh", "-c", script, "canti-job", c.Init, c.Workdir)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = c.Workdir, nil, writer, writer
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	rec := Record{Protocol: Protocol, EffectID: c.EffectID, Spec: c.Spec, Generation: c.Generation}
	if err := cmd.Start(); err != nil {
		return rec, err
	}
	rec.StartedAt = time.Now().UTC() // The helper's observation, not a Docker PID1 timestamp.
	stopSignals := forwardSignals(cmd.Process.Pid)
	defer stopSignals()
	writer.Close()
	captured := make(chan capture, 1)
	go func() { captured <- collect(reader, out, c.OutputLimit, c.PreviewLimit) }()
	waitErr := cmd.Wait()
	rec.FinishedAt = time.Now().UTC()
	code := exitCode(waitErr)
	rec.ExitCode = &code
	var got capture
	select {
	case got = <-captured:
	case <-time.After(100 * time.Millisecond):
		reader.Close()
		got = <-captured
		rec.Incomplete = true
	}
	rec.Bytes, rec.Stored, rec.Preview = got.total, got.stored, got.preview()
	if got.err != nil {
		rec.CaptureError = "capture failed"
		rec.Incomplete = true
	}
	if err := out.Sync(); err != nil {
		return rec, fmt.Errorf("job runner: sync output: %w", err)
	}
	if err := out.Close(); err != nil {
		return rec, err
	}
	// Hash only the bounded retained file, after capture is quiescent.
	f, err := root.OpenFile("output", os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return rec, err
	}
	hash := sha256.New()
	_, err = io.Copy(hash, io.LimitReader(f, c.OutputLimit+1))
	err = errors.Join(err, f.Close())
	if err != nil {
		return rec, err
	}
	rec.Digest = hex.EncodeToString(hash.Sum(nil))
	if err := publish(root, rec); err != nil {
		return rec, err
	}
	return rec, nil
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exited *exec.ExitError
	if errors.As(err, &exited) {
		if status, ok := exited.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal())
		}
		return exited.ExitCode()
	}
	return 125
}

func forwardSignals(pid int) func() {
	signals := make(chan os.Signal, 4)
	done := make(chan struct{})
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-done:
				return
			case s := <-signals:
				if sig, ok := s.(syscall.Signal); ok {
					_ = syscall.Kill(-pid, sig)
				}
			}
		}
	}()
	return func() { signal.Stop(signals); close(done) }
}

func publish(root *os.Root, rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(data) > MetadataLimit {
		return errors.New("job runner: completion exceeds metadata limit")
	}
	f, err := root.OpenFile("completion.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = root.Rename("completion.tmp", "completion.json"); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

type capture struct {
	total, stored int64
	head, tail    []byte
	limit         int
	err           error
}

func collect(r io.Reader, w io.Writer, limit int64, preview int) capture {
	c := capture{limit: preview}
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			c.write(w, buf[:n], limit)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.err = errors.Join(c.err, err)
			}
			return c
		}
	}
}
func (c *capture) write(w io.Writer, p []byte, limit int64) {
	c.total += int64(len(p))
	n := min(len(p), c.limit-len(c.head))
	c.head = append(c.head, p[:n]...)
	c.tail = append(c.tail, p[max(0, len(p)-c.limit):]...)
	if len(c.tail) > c.limit {
		c.tail = append(c.tail[:0], c.tail[len(c.tail)-c.limit:]...)
	}
	if c.err == nil && c.stored < limit {
		wanted := min(int64(len(p)), limit-c.stored)
		n, err := w.Write(p[:wanted])
		c.stored += int64(n)
		if err == nil && int64(n) != wanted {
			err = io.ErrShortWrite
		}
		c.err = err
	}
}
func (c capture) preview() string {
	if c.total <= int64(c.limit) {
		return strings.ToValidUTF8(string(c.head), "�")
	}
	marker := fmt.Sprintf("\n... %d bytes total ...\n", c.total)
	n := (c.limit - len(marker)) / 2
	return strings.ToValidUTF8(string(c.head[:n]), "�") + marker + strings.ToValidUTF8(string(c.tail[len(c.tail)-n:]), "�")
}
