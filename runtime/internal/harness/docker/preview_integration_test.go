//go:build integration

package docker_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

// TestEphemeralAndAttachedOutputPreviewsMatch runs one output through both
// execution paths. The previews are identical apart from the job-readable
// artifact path, and a later command on each path can page the omitted lines
// from the path the marker names.
func TestEphemeralAndAttachedOutputPreviewsMatch(t *testing.T) {
	ctx, control, workspace, state, endpoint, id := prepareAttachedDocker(t, "0:0")
	attachment, err := docker.AttachEnvironment(ctx, control, endpoint, id, "")
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := docker.EnvironmentWorkdir(ctx, attachment, workspace, state, "")
	if err != nil {
		t.Fatal(err)
	}
	attached, err := docker.NewAttached(docker.AttachedConfig{
		Attachment: attachment, Workspace: workspace, State: state, Workdir: cwd,
		OutputLimit: 1 << 20, PreviewLimit: 50 << 10, StopGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := setup(t, docker.Config{OutputLimit: 1 << 20, PreviewLimit: 50 << 10})
	ephemeral := open(t, cfg)

	command := "seq 1 3000"
	shared := harness.Effect{ID: "preview-shared", Command: command}
	shared.Spec = attached.Specification(command)
	persistentJob := waitAttachedDockerJob(t, ctx, attached, shared)
	if persistentJob.Status != harness.JobSucceeded {
		t.Fatalf("persistent job = %+v", persistentJob)
	}
	ephemeralEffect := accepted(ephemeral, harness.Effect{ID: "preview-shared", Command: command})
	ephemeralJob := run(ctx, t, ephemeral, ephemeralEffect)

	persistentPath := "/var/lib/canti/jobs/preview-shared/output"
	ephemeralPath := "/canti/artifacts/preview-shared/output"
	if !strings.Contains(persistentJob.Preview, persistentPath) || !strings.Contains(ephemeralJob.Preview, ephemeralPath) {
		t.Fatalf("previews do not name the job-readable artifact:\npersistent %q\nephemeral %q", persistentJob.Preview, ephemeralJob.Preview)
	}
	if got := strings.ReplaceAll(ephemeralJob.Preview, ephemeralPath, persistentPath); got != persistentJob.Preview {
		t.Fatalf("the paths rendered different previews for the same output:\npersistent %q\nephemeral  %q", persistentJob.Preview, got)
	}
	for _, want := range []string{"lines 1-999 and 2001-3000 of 3000", "(lines 1000-2000)", "artifact complete"} {
		if !strings.Contains(persistentJob.Preview, want) {
			t.Errorf("persistent preview lacks %q: %q", want, persistentJob.Preview)
		}
	}

	page := func(preview, path string) string {
		t.Helper()
		_, after, ok := strings.Cut(preview, "page omitted bytes ")
		if !ok {
			t.Fatalf("preview has no omitted byte page: %q", preview)
		}
		_, command, ok := strings.Cut(after, ": ")
		if !ok {
			t.Fatalf("preview has no page command separator: %q", after)
		}
		command, _, ok = strings.Cut(command, " ...")
		if !ok || !strings.Contains(command, path) {
			t.Fatalf("preview has no bounded page command for %s: %q", path, command)
		}
		return command
	}
	persistentPage := harness.Effect{ID: "preview-page", Command: page(persistentJob.Preview, persistentPath)}
	persistentPage.Spec = attached.Specification(persistentPage.Command)
	paged := waitAttachedDockerJob(t, ctx, attached, persistentPage)
	if paged.Status != harness.JobSucceeded || !strings.HasPrefix(paged.Preview, "1000\n") || !strings.Contains(paged.Preview, "\n2000\n") {
		t.Fatalf("persistent paging = %+v", paged)
	}

	ephemeralPage := accepted(ephemeral, harness.Effect{ID: "preview-page", Command: page(ephemeralJob.Preview, ephemeralPath)})
	paged = run(ctx, t, ephemeral, ephemeralPage)
	if paged.Status != harness.JobSucceeded || !strings.HasPrefix(paged.Preview, "1000\n") || !strings.Contains(paged.Preview, "\n2000\n") {
		t.Fatalf("ephemeral paging = %+v", paged)
	}

	rawCommand := "printf '\\377\\376a\\n'; i=0; while [ $i -lt 30000 ]; do printf '\\377x'; i=$((i+1)); done"
	raw := harness.Effect{ID: "preview-raw", Command: rawCommand}
	raw.Spec = attached.Specification(rawCommand)
	persistentRaw := waitAttachedDockerJob(t, ctx, attached, raw)
	ephemeralRaw := run(ctx, t, ephemeral, accepted(ephemeral, harness.Effect{ID: "preview-raw", Command: rawCommand}))
	rawPath := "/var/lib/canti/jobs/preview-raw/output"
	ephemeralRawPath := "/canti/artifacts/preview-raw/output"
	if got := strings.ReplaceAll(ephemeralRaw.Preview, ephemeralRawPath, rawPath); got != persistentRaw.Preview {
		t.Fatalf("invalid bytes rendered different previews:\npersistent %q\nephemeral  %q", persistentRaw.Preview, got)
	}
	if !strings.Contains(persistentRaw.Preview, "60004 bytes observed") || !strings.Contains(persistentRaw.Preview, "artifact complete") {
		t.Fatalf("invalid-byte preview lost its source byte count: %q", persistentRaw.Preview)
	}
	persistentBytes, err := os.ReadFile(filepath.Join(state, "artifacts", "preview-raw", "output"))
	if err != nil {
		t.Fatal(err)
	}
	ephemeralBytes, err := os.ReadFile(filepath.Join(cfg.State, "artifacts", "preview-raw", "output"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(persistentBytes, ephemeralBytes) || !bytes.Equal(persistentBytes, []byte("\xff\xfea\n"+strings.Repeat("\xffx", 30000))) {
		t.Fatalf("artifacts did not preserve the raw bytes: persistent=%d ephemeral=%d", len(persistentBytes), len(ephemeralBytes))
	}

	cutAttached, err := docker.NewAttached(docker.AttachedConfig{
		Attachment: attachment, Workspace: workspace, State: state, Workdir: cwd,
		OutputLimit: 512, PreviewLimit: 50 << 10, StopGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cutCfg, _ := setup(t, docker.Config{OutputLimit: 512, PreviewLimit: 50 << 10})
	cutEphemeral := open(t, cutCfg)
	cutCommand := "i=0; while [ $i -lt 5120 ]; do printf '0123456789'; i=$((i+1)); done"
	cut := harness.Effect{ID: "preview-cut", Command: cutCommand}
	cut.Spec = cutAttached.Specification(cutCommand)
	persistentCut := waitAttachedDockerJob(t, ctx, cutAttached, cut)
	ephemeralCut := run(ctx, t, cutEphemeral, accepted(cutEphemeral, harness.Effect{ID: "preview-cut", Command: cutCommand}))
	cutPath := "/var/lib/canti/jobs/preview-cut/output"
	ephemeralCutPath := "/canti/artifacts/preview-cut/output"
	if got := strings.ReplaceAll(ephemeralCut.Preview, ephemeralCutPath, cutPath); got != persistentCut.Preview {
		t.Fatalf("artifact cuts selected different content:\npersistent %q\nephemeral  %q", persistentCut.Preview, got)
	}
	if !strings.Contains(persistentCut.Preview, "artifact keeps 512 of 51200 bytes (-output-limit)") || !strings.Contains(ephemeralCut.Preview, "artifact keeps 512 of 51200 bytes (-output-limit)") {
		t.Fatalf("artifact cut not reported: persistent %q ephemeral %q", persistentCut.Preview, ephemeralCut.Preview)
	}

	exactCommand := "printf 'a\\377\\376b\\000z\\n'"
	exact := harness.Effect{ID: "preview-invalid", Command: exactCommand}
	exact.Spec = attached.Specification(exactCommand)
	persistentExact := waitAttachedDockerJob(t, ctx, attached, exact)
	ephemeralExact := run(ctx, t, ephemeral, accepted(ephemeral, harness.Effect{ID: "preview-invalid", Command: exactCommand}))
	exactPath := "/var/lib/canti/jobs/preview-invalid/output"
	ephemeralExactPath := "/canti/artifacts/preview-invalid/output"
	if got := strings.ReplaceAll(ephemeralExact.Preview, ephemeralExactPath, exactPath); got != persistentExact.Preview {
		t.Fatalf("invalid byte runs rendered different previews:\npersistent %q\nephemeral  %q", persistentExact.Preview, got)
	}
	if !strings.HasPrefix(persistentExact.Preview, "a\uFFFDb\x00z\n") {
		t.Fatalf("invalid byte run lost its replacements: %q", persistentExact.Preview)
	}
	for _, artifact := range []string{
		filepath.Join(state, "artifacts", "preview-invalid", "output"),
		filepath.Join(cfg.State, "artifacts", "preview-invalid", "output"),
	} {
		data, err := os.ReadFile(artifact)
		if err != nil || !bytes.Equal(data, []byte{'a', 0xff, 0xfe, 'b', 0, 'z', '\n'}) {
			t.Fatalf("invalid-byte artifact %s = % x: %v", artifact, data, err)
		}
	}
}

// TestPreviewPagingPreservesBinaryBytesOnCoreutilsAndBusyBox executes the
// emitted byte page outside the preview renderer. NUL, CR, invalid bytes and an
// unterminated last line must round-trip exactly through both readers.
func TestPreviewPagingPreservesBinaryBytesOnCoreutilsAndBusyBox(t *testing.T) {
	data := bytes.Repeat([]byte{'A', 0, 0xff, '\r', '\n'}, 10000)
	data = append(data, 0, 0xff)
	dir := t.TempDir()
	path := filepath.Join(dir, "output")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	buffer := jobrunner.NewPreviewBuffer(jobrunner.MaxPreviewBytes)
	buffer.Observe(data)
	preview := buffer.String(jobrunner.PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: 10000})
	_, after, ok := strings.Cut(preview, "page omitted bytes ")
	if !ok {
		t.Fatalf("preview has no omitted byte page: %q", preview)
	}
	span, command, ok := strings.Cut(after, ": ")
	if !ok {
		t.Fatalf("preview has no page command: %q", after)
	}
	command, _, ok = strings.Cut(command, " ...")
	if !ok {
		t.Fatalf("preview has an unterminated page command: %q", after)
	}
	var start, end int
	if _, err := fmt.Sscanf(span, "%d-%d", &start, &end); err != nil || start < 1 || end < start || end > len(data) {
		t.Fatalf("page span %q: %v", span, err)
	}
	want := data[start-1 : end]
	run := func(name string, args []string) []byte {
		t.Helper()
		out, err := exec.Command(args[0], args[1:]...).Output()
		if err != nil {
			t.Fatalf("%s page: %v", name, err)
		}
		return out
	}
	if got := run("coreutils", []string{"sh", "-c", command}); !bytes.Equal(got, want) {
		t.Fatalf("coreutils page = %d bytes, want %d", len(got), len(want))
	}
	if got := run("busybox", []string{"docker", "run", "--rm", "-v", path + ":/artifact:ro", "alpine:3.23", "sh", "-c", strings.ReplaceAll(command, path, "/artifact")}); !bytes.Equal(got, want) {
		t.Fatalf("busybox page = %d bytes, want %d", len(got), len(want))
	}
}

func TestPreviewPagingTerminatorsOnBothReaders(t *testing.T) {
	for _, suffix := range []string{"", "\n", "\r\n"} {
		for _, shape := range []string{"single long line", "many lines"} {
			name := shape + "/" + map[string]string{"": "unterminated", "\n": "lf", "\r\n": "crlf"}[suffix]
			t.Run(name, func(t *testing.T) {
				var data []byte
				if shape == "single long line" {
					data = append(bytes.Repeat([]byte("x"), 60000), suffix...)
				} else {
					line := "x\n"
					if suffix != "" {
						line = "x" + suffix
					}
					data = bytes.Repeat([]byte(line), 5000)
				}
				path := filepath.Join(t.TempDir(), "output")
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				buffer := jobrunner.NewPreviewBuffer(jobrunner.MaxPreviewBytes)
				buffer.Observe(data)
				preview := buffer.String(jobrunner.PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: int64(jobrunner.PreviewLines(string(data))),
					PageEnd: func(start, max int64) int64 {
						if end, err := pageEndInData(data, start, max, jobrunner.MaxPreviewBytes); err == nil {
							return end
						}
						return max
					}})
				_, after, ok := strings.Cut(preview, "page omitted bytes ")
				if !ok {
					t.Fatalf("no page: %q", preview)
				}
				span, command, ok := strings.Cut(after, ": ")
				if !ok {
					t.Fatalf("no page separator: %q", after)
				}
				command, _, ok = strings.Cut(command, " ...")
				if !ok {
					t.Fatalf("unterminated page: %q", after)
				}
				var start, end int
				if _, err := fmt.Sscanf(span, "%d-%d", &start, &end); err != nil || start < 1 || end < start || end > len(data) {
					t.Fatalf("span %q: %v", span, err)
				}
				want := data[start-1 : end]
				run := func(prefix string, args []string) {
					t.Helper()
					out, err := exec.Command(args[0], args[1:]...).Output()
					if err != nil {
						t.Fatalf("%s: %v", prefix, err)
					}
					if !bytes.Equal(out, want) {
						t.Fatalf("%s = %d bytes, want %d", prefix, len(out), len(want))
					}
				}
				run("coreutils", []string{"sh", "-c", command})
				run("busybox", []string{"docker", "run", "--rm", "-v", path + ":/artifact:ro", "alpine:3.23", "sh", "-c", strings.ReplaceAll(command, path, "/artifact")})
			})
		}
	}
}

func TestPreviewPagingInvalidUTF8FitsOnBothReaders(t *testing.T) {
	data := bytes.Repeat([]byte{0xff, 'x'}, 60000)
	path := filepath.Join(t.TempDir(), "output")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	buffer := jobrunner.NewPreviewBuffer(jobrunner.MaxPreviewBytes)
	buffer.Observe(data)
	preview := buffer.String(jobrunner.PreviewOptions{Artifact: path, Stored: int64(len(data)), ArtifactLimit: int64(len(data)), StoredLines: int64(jobrunner.PreviewLines(string(data))), PreviewLimit: jobrunner.MaxPreviewBytes,
		PageEnd: func(start, max int64) int64 {
			if end, err := pageEndInData(data, start, max, jobrunner.MaxPreviewBytes); err == nil {
				return end
			}
			return max
		}})
	_, after, ok := strings.Cut(preview, "page omitted bytes ")
	if !ok {
		t.Fatalf("no invalid-byte page: %q", preview)
	}
	_, command, ok := strings.Cut(after, ": ")
	if !ok {
		t.Fatalf("no page separator: %q", after)
	}
	command, _, ok = strings.Cut(command, " ...")
	if !ok {
		t.Fatalf("unterminated page: %q", after)
	}
	run := func(name string, args []string) []byte {
		t.Helper()
		out, err := exec.Command(args[0], args[1:]...).Output()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		next := jobrunner.NewPreviewBuffer(jobrunner.MaxPreviewBytes)
		next.Observe(out)
		rendered := next.String(jobrunner.PreviewOptions{Artifact: path, Stored: int64(len(out)), ArtifactLimit: int64(len(out)), StoredLines: int64(jobrunner.PreviewLines(string(out))), PreviewLimit: jobrunner.MaxPreviewBytes})
		if strings.Contains(rendered, "output truncated") || rendered != strings.ToValidUTF8(string(out), "\uFFFD") {
			t.Fatalf("%s page rendered with a marker: %d raw bytes -> %d rendered", name, len(out), len(rendered))
		}
		return out
	}
	run("coreutils", []string{"sh", "-c", command})
	run("busybox", []string{"docker", "run", "--rm", "-v", path + ":/artifact:ro", "alpine:3.23", "sh", "-c", strings.ReplaceAll(command, path, "/artifact")})
}

func pageEndInData(data []byte, start, max int64, previewLimit int) (int64, error) {
	if start < 0 {
		start = 0
	}
	if max > int64(len(data)) {
		max = int64(len(data))
	}
	rawEnd := min(max, start+int64(previewLimit))
	if rawEnd <= start {
		return start, nil
	}
	raw := data[start:rawEnd]
	rendered, lines, last := 0, 0, 0
	invalidRun := false
	for i := 0; i < len(raw); {
		r, size := utf8.DecodeRune(raw[i:])
		if r == utf8.RuneError && size == 1 {
			if !invalidRun {
				rendered += len("\uFFFD")
				invalidRun = true
			}
		} else {
			if r == '\n' {
				if lines >= jobrunner.MaxPreviewLines {
					return start + int64(last), nil
				}
				lines++
			}
			rendered += size
			invalidRun = false
		}
		if rendered > previewLimit {
			return start + int64(last), nil
		}
		i += size
		last = i
		if r == '\n' && lines == jobrunner.MaxPreviewLines {
			return start + int64(last), nil
		}
	}
	return start + int64(last), nil
}

func TestCancelledPreviewKeepsHelperCaptureFacts(t *testing.T) {
	cfg, id := setup(t, docker.Config{OutputLimit: 100, PreviewLimit: 50 << 10, StopGrace: time.Second})
	ctx := testContext(t, time.Minute)
	e := harness.Effect{ID: id("cancel-facts"), Command: "seq 3000; touch /workspace/began; sleep 300"}
	x := open(t, cfg)
	for range 2 {
		if _, err := x.Drive(ctx, accepted(x, e)); err != nil {
			t.Fatal(err)
		}
	}
	for {
		if _, err := os.Stat(filepath.Join(cfg.Workspace, "began")); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("job never wrote its readiness marker")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := open(t, cfg).Cancel(ctx, accepted(open(t, cfg), e)); err != nil {
		t.Fatal(err)
	}
	job := inspect(ctx, t, open(t, cfg), e)
	if job.Status != harness.JobCancelled || strings.Contains(job.Preview, "preview unavailable") {
		t.Fatalf("cancelled preview = %+v", job)
	}
	for _, want := range []string{"13893 bytes", "-output-limit", "2999\n3000", "command was cancelled", "output is partial"} {
		if !strings.Contains(job.Preview, want) {
			t.Fatalf("cancelled preview lacks %q: %q", want, job.Preview)
		}
	}
	if len(job.Preview) > 50<<10 || strings.Count(job.Preview, "\n") > 2000 {
		t.Fatalf("cancelled preview exceeds its limits: %d bytes, %d newlines", len(job.Preview), strings.Count(job.Preview, "\n"))
	}
}

func TestCancelledPreviewFullArtifactHasNoCompletionClaim(t *testing.T) {
	cfg, id := setup(t, docker.Config{OutputLimit: 1 << 20, PreviewLimit: 50 << 10, StopGrace: time.Second})
	ctx := testContext(t, time.Minute)
	e := harness.Effect{ID: id("cancel-full"), Command: "seq 3000; touch /workspace/began; sleep 300"}
	x := open(t, cfg)
	for range 2 {
		if _, err := x.Drive(ctx, accepted(x, e)); err != nil {
			t.Fatal(err)
		}
	}
	for {
		if _, err := os.Stat(filepath.Join(cfg.Workspace, "began")); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("job never wrote its readiness marker")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := open(t, cfg).Cancel(ctx, accepted(open(t, cfg), e)); err != nil {
		t.Fatal(err)
	}
	job := inspect(ctx, t, open(t, cfg), e)
	if job.Status != harness.JobCancelled || job.Output.Complete || strings.Contains(job.Preview, "artifact complete") ||
		!strings.Contains(job.Preview, "13893 bytes") || !strings.Contains(job.Preview, "2999\n3000") || !strings.Contains(job.Preview, "command was cancelled") {
		t.Fatalf("full contaminated preview = %+v", job)
	}
}
