package jobrunner

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func config(t *testing.T, command string) Config {
	t.Helper()
	return Config{Directory: t.TempDir(), EffectID: "effect_test", Spec: strings.Repeat("a", 64), Generation: 1, Workdir: t.TempDir(), Command: command, OutputLimit: 1024, PreviewLimit: 128}
}
func TestCompletionKeepsCommandExitAndImmutableCapture(t *testing.T) {
	c := config(t, "printf hello; printf error >&2; exit 7")
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ExitCode == nil || *rec.ExitCode != 7 || rec.Bytes != 10 || rec.Stored != 10 || rec.Incomplete || rec.StartedAt.IsZero() || rec.FinishedAt.Before(rec.StartedAt) {
		t.Fatalf("record = %+v", rec)
	}
	out, err := os.ReadFile(filepath.Join(c.Directory, "output"))
	if err != nil || string(out) != "helloerror" {
		t.Fatalf("output=%q: %v", out, err)
	}
	data, err := os.ReadFile(filepath.Join(c.Directory, "completion.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Record
	if err = json.Unmarshal(data, &saved); err != nil || saved.Spec != c.Spec || saved.Digest != rec.Digest {
		t.Fatalf("saved = %+v: %v", saved, err)
	}
	if _, err = Run(c); err == nil {
		t.Fatal("a second launcher reused existing command evidence")
	}
}
func TestCaptureBoundsOutputWithoutStoppingCommand(t *testing.T) {
	c := config(t, "i=0; while [ $i -lt 10000 ]; do printf 0123456789; i=$((i+1)); done; printf done > finished")
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Bytes != 100000 || rec.Stored != c.OutputLimit || *rec.ExitCode != 0 {
		t.Fatalf("record=%+v", rec)
	}
	info, err := os.Stat(filepath.Join(c.Directory, "output"))
	if err != nil || info.Size() != c.OutputLimit {
		t.Fatalf("output=%v: %v", info, err)
	}
	if _, err = os.Stat(filepath.Join(c.Workdir, "finished")); err != nil {
		t.Fatal(err)
	}
}
func TestInheritedBackgroundPipeCannotHoldCompletion(t *testing.T) {
	c := config(t, "sleep 1 & printf foreground")
	start := time.Now()
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 800*time.Millisecond || !rec.Incomplete || *rec.ExitCode != 0 || rec.Preview != "foreground" {
		t.Fatalf("elapsed=%v record=%+v", time.Since(start), rec)
	}
}
func TestInitializationAndWorkingDirectoryAreExplicit(t *testing.T) {
	c := config(t, "printf '%s:%s' \"$TOOL_PATH\" \"$PWD\"")
	c.Init = filepath.Join(t.TempDir(), "init.sh")
	if err := os.WriteFile(c.Init, []byte("export TOOL_PATH=persistent\ncd /\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Preview != "persistent:"+c.Workdir {
		t.Fatalf("preview=%q", rec.Preview)
	}
}
func TestEvidenceDoesNotFollowAnOutputSymlink(t *testing.T) {
	c := config(t, "touch should-not-run")
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(c.Directory, "output")); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(c); err == nil {
		t.Fatal("followed output link")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "untouched" {
		t.Fatal("modified symlink target")
	}
	if _, err := os.Stat(filepath.Join(c.Workdir, "should-not-run")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("command ran")
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }
func TestCaptureFailureStillDrainsTheChild(t *testing.T) {
	c := collect(strings.NewReader(strings.Repeat("x", 10000)), brokenWriter{}, 1024, 128)
	if c.total != 10000 || c.err == nil || c.stored != 0 {
		t.Fatalf("capture=%+v", c)
	}
}
func TestSignalExitRetainsTheShellStatus(t *testing.T) {
	c := config(t, "kill -TERM $$")
	rec, err := Run(c)
	if err != nil {
		t.Fatal(err)
	}
	if *rec.ExitCode != 143 {
		t.Fatalf("exit=%d", *rec.ExitCode)
	}
}
