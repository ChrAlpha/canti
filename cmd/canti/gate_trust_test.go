//go:build integration

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness/docker"
)

// evSections maps the name of every "== NAME" line of out to the lines
// after it.
func evSections(out string) map[string]string {
	m, name := map[string]string{}, ""
	for line := range strings.Lines(out) {
		if n, ok := strings.CutPrefix(line, "== "); ok {
			name = strings.TrimSuffix(n, "\n")
			m[name] = ""
			continue
		}
		m[name] += line
	}
	return m
}

func evNotes(prompt string) string {
	_, notes, _ := strings.Cut(prompt, "\n## Notes\n")
	notes, _, _ = strings.Cut(notes, "\n## ")
	return notes
}

// evLeaks returns the files under dir holding any of secrets.
func evLeaks(t *testing.T, dir string, secrets ...string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			data, err := os.ReadFile(p)
			for _, s := range secrets {
				if err != nil || bytes.Contains(data, []byte(s)) {
					out = append(out, p)
					break
				}
			}
		}
		return nil
	})
	return out
}

func TestGateTrustBoundaryKeepsCredentialsAndStateOut(t *testing.T) {
	f, image := evFixture(t, map[int]core.Decision{2: {Kind: core.Return, Payload: "done"}})
	probe := "probe-" + rand.Text()
	command := strings.Join([]string{
		released + "echo '== env'", "env",
		"echo '== uid'", "id -u",
		"echo '== caps'", "grep CapEff /proc/self/status",
		"echo '== mountinfo'", "cat /proc/self/mountinfo",
		"echo '== touch'", `touch /canti/artifacts/x; echo "touch=$?"`,
		"echo '== root'", "ls -la /",
		"echo '== state'", "ls " + f.state + `; echo "ls=$?"`,
		"echo '== journal'", `find / \( -path /proc -o -path /sys \) -prune -o \( -name 'canti.db*' -o -name docker.sock -o -path /etc/hostname \) -print 2>/dev/null`,
		"echo '== net'", "ls /sys/class/net",
		"echo '== wget'", "wget -q -T 2 -O- " + f.model.URL + `; echo "wget=$?"`,
		"echo end",
	}, "\n")
	d, _ := json.Marshal(core.Decision{Kind: core.Exec, Payload: command})
	f.model.answer(1, 1, reply{text: string(d)})
	_, wait := f.start([]string{"CANTI_TRUST_PROBE=" + probe}, "run", "-goal", "g", "-image", image)
	c := evInspect(t, evJob(t, evAwait(t, f, "job_started")))
	writeFile(t, filepath.Join(f.workspace, "release"))
	r := wait()
	if r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	h := f.history()
	if !slices.Equal(kinds(h), slices.Concat(evStep, evReturn)) {
		t.Fatalf("history = %q", kinds(h))
	}
	obs := h[6].Facts.Observation
	if obs == nil || obs.Execution != core.ExecutionExited || obs.ExitCode == nil || *obs.ExitCode != 0 || !obs.Output.Complete {
		t.Fatalf("observation = %+v", obs)
	}
	data, err := os.ReadFile(filepath.Join(f.state, "artifacts", obs.Output.Path))
	if err != nil || int64(len(data)) != obs.Output.Bytes {
		t.Fatalf("artifact of %d bytes: %v", len(data), err)
	}
	out := string(data)
	sent := f.model.requests()
	for _, secret := range []string{f.key, probe} {
		if strings.Contains(out, secret) || strings.Contains(obs.Preview, secret) {
			t.Errorf("the job saw %q", secret)
		}
		for _, body := range sent {
			if bytes.Contains(body, []byte(secret)) {
				t.Errorf("a request body holds %q", secret)
			}
		}
	}
	if leaks := evLeaks(t, f.state, probe); len(leaks) > 0 || strings.Contains(f.output.String(), probe) {
		t.Errorf("the probe leaked into %q or the CLI output", leaks)
	}
	s := evSections(out)
	for _, marker := range []string{"ANTHROPIC_", "CANTI_", "GORACE"} {
		if strings.Contains(s["env"], marker) {
			t.Errorf("job environment holds %s:\n%s", marker, s["env"])
		}
	}
	if want := fmt.Sprintf("%d\n", os.Getuid()); s["uid"] != want {
		t.Errorf("job uid = %q, want %q", s["uid"], want)
	}
	if s["caps"] != "CapEff:\t0000000000000000\n" {
		t.Errorf("job capabilities = %q", s["caps"])
	}
	mounts := map[string]string{}
	for line := range strings.Lines(s["mountinfo"]) {
		if fields := strings.Fields(line); len(fields) > 5 {
			mounts[fields[4]] = fields[5]
		}
	}
	for dir, mode := range map[string]string{docker.WorkDir: "rw", docker.ArtifactDir: "ro", runDir: "ro", runsDir: "ro"} {
		if got := strings.Split(mounts[dir], ","); got[0] != mode {
			t.Errorf("mount %s = %q, want %s", dir, mounts[dir], mode)
		}
	}
	if !strings.Contains(s["touch"], "Read-only file system") || !strings.Contains(s["touch"], "touch=1") {
		t.Errorf("touch in the artifacts = %q", s["touch"])
	}
	if _, err := os.Stat(filepath.Join(f.state, "artifacts", "x")); !os.IsNotExist(err) {
		t.Errorf("the job wrote the artifacts: %v", err)
	}
	if !strings.Contains(s["state"], "No such file or directory") || strings.Contains(s["state"], "ls=0") {
		t.Errorf("the state directory in the job = %q", s["state"])
	}
	if !strings.Contains(s["root"], " canti\n") || !strings.Contains(s["root"], " workspace\n") {
		t.Errorf("job root = %q", s["root"])
	}
	if s["journal"] != "/etc/hostname\n" || s["net"] != "lo\n" {
		t.Errorf("journal files %q, network interfaces %q", s["journal"], s["net"])
	}
	if !strings.Contains(s["wget"], "wget=") || strings.Contains(s["wget"], "wget=0") || strings.Contains(s["wget"], "unexpected credentials") {
		t.Errorf("model URL from the job = %q", s["wget"])
	}
	if !strings.HasSuffix(out, "end\n") {
		t.Errorf("output ends %q", out[max(0, len(out)-64):])
	}
	if len(sent) != 2 {
		t.Errorf("the model received %d requests, want 2", len(sent))
	}
	for i, body := range sent {
		if step, _, err := promptOf(body); err != nil || step != i+1 {
			t.Errorf("request %d is not the step %d prompt: %v", i, i+1, err)
		}
	}
	checkReleased(t, f, h)
	if !c.State.Running || c.HostConfig.NetworkMode != "none" || !slices.Equal(c.HostConfig.CapDrop, []string{"ALL"}) || c.Config.User != fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()) {
		t.Errorf("container config = %+v", c)
	}
	evLog(t, image, "trust boundary", "fixture key and CANTI_TRUST_PROBE in the CLI environment; job probes env, uid, caps, mounts, state, journal, network",
		fmt.Sprintf("key and probe absent from output, requests, and state; uid %s; artifacts %s, workspace %s, run %s, runs %s; state and journal absent; interfaces %s only; model URL unreachable",
			strings.TrimSpace(s["uid"]), mounts[docker.ArtifactDir], mounts[docker.WorkDir], mounts[runDir], mounts[runsDir], strings.TrimSpace(s["net"])),
		fmt.Sprintf("output %d bytes, network %s, cap-drop %v, user %s", len(data), c.HostConfig.NetworkMode, c.HostConfig.CapDrop, c.Config.User))
}

func TestGateTrustBoundaryNotes(t *testing.T) {
	t.Run("writable", func(t *testing.T) {
		f, image := evFixture(t, map[int]core.Decision{
			1: {Kind: core.Exec, Payload: "mkdir -p .canti && echo plan-alpha > .canti/notes.md"},
			2: {Kind: core.Return, Payload: "done"},
		})
		if r := f.cli(nil, "run", "-goal", "g", "-image", image); r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		p1, p2 := f.model.prompts(1), f.model.prompts(2)
		if len(p1) != 1 || !strings.Contains(evNotes(p1[0]), "The file is empty or missing.") {
			t.Errorf("step 1 notes = %q", p1)
		}
		if len(p2) != 1 {
			t.Fatalf("step 2 prompts = %q", p2)
		}
		notes := evNotes(p2[0])
		if !strings.Contains(notes, "plan-alpha") || !strings.Contains(notes, docker.WorkDir+"/.canti/notes.md") || !strings.Contains(notes, "not verified by the harness") {
			t.Errorf("step 2 notes = %q", notes)
		}
		if data, err := os.ReadFile(filepath.Join(f.workspace, ".canti", "notes.md")); err != nil || string(data) != "plan-alpha\n" {
			t.Errorf("notes file = %q, %v", data, err)
		}
		evLog(t, image, "trust boundary (notes)", "step 1 writes plan-alpha to .canti/notes.md in the workspace",
			"step 2 prompt shows plan-alpha in the Notes section, marked unverified", fmt.Sprintf("step 2 prompt %d bytes", len(p2[0])))
	})
	t.Run("escaping_symlink", func(t *testing.T) {
		f, image := evFixture(t, map[int]core.Decision{
			1: {Kind: core.Exec, Payload: `ln -sfn ../../secret.txt .canti/notes.md; cat .canti/notes.md; echo "cat=$?"`},
			2: {Kind: core.Return, Payload: "done"},
		})
		outside := filepath.Join(t.TempDir(), "secret.txt")
		for file, text := range map[string]string{outside: "HOSTSECRET-absolute\n", filepath.Join(filepath.Dir(f.workspace), "secret.txt"): "HOSTSECRET-relative\n"} {
			if err := os.WriteFile(file, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		notes := filepath.Join(f.workspace, ".canti", "notes.md")
		if err := os.Mkdir(filepath.Dir(notes), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, notes); err != nil {
			t.Fatal(err)
		}
		if r := f.cli(nil, "run", "-goal", "g", "-image", image); r.code != exitOK || r.stdout != "done\n" {
			t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
		}
		sent := f.model.requests()
		if len(sent) != 2 {
			t.Fatalf("%d requests", len(sent))
		}
		for i, body := range sent {
			_, prompt, err := promptOf(body)
			if err != nil || !strings.Contains(evNotes(prompt), "notes unreadable") || bytes.Contains(body, []byte("HOSTSECRET")) {
				t.Errorf("request %d notes = %q, %v", i+1, evNotes(prompt), err)
			}
		}
		if data, err := os.ReadFile(notes); err != nil || string(data) != "HOSTSECRET-relative\n" {
			t.Errorf("the agent's symlink does not escape on the host: %q, %v", data, err)
		}
		h := f.history()
		if o := h[6].Facts.Observation; o == nil || !strings.Contains(o.Preview, "cat=1") || strings.Contains(o.Preview, "HOSTSECRET") {
			t.Errorf("step 1 observation = %+v", o)
		}
		if leaks := evLeaks(t, f.state, "HOSTSECRET"); len(leaks) > 0 {
			t.Errorf("the host secret leaked into %q", leaks)
		}
		evLog(t, image, "trust boundary (notes)", "notes.md is a host symlink to an outside file, then the job repoints it with ../../secret.txt",
			"both prompts report the notes unreadable; HOSTSECRET absent from requests and state", fmt.Sprintf("%d requests checked", len(sent)))
	})
}

func TestGateTrustBoundaryProtectsFacts(t *testing.T) {
	tamper := strings.Join([]string{
		`rm -rf /canti/artifacts/*; echo "rm=$?"`,
		`echo forged > /canti/artifacts/forged; echo "write=$?"`,
		`echo forged > canti.db; mkdir -p artifacts && echo forged > artifacts/output`,
		`ls -R /canti/artifacts`,
	}, "\n")
	f, image := evFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: "echo fact-one"},
		2: {Kind: core.Exec, Payload: tamper},
		3: {Kind: core.Return, Payload: "done"},
	})
	r := f.cli(nil, "run", "-once", "-goal", "g", "-image", image)
	for n := 1; r.code == exitActive && f.status().Step < 2; n++ {
		if n > 20 {
			t.Fatalf("step 1 unfinished after %d processes", n)
		}
		r = f.cli(nil, "resume", "-once")
	}
	if r.code != exitActive {
		t.Fatalf("stepping = %d\nstderr: %s", r.code, r.stderr)
	}
	before := f.cli(nil, "history")
	h := f.history()
	if !slices.Equal(kinds(h), evStep) {
		t.Fatalf("history before the tampering step = %q", kinds(h))
	}
	fact := h[6].Facts.Observation
	artifact := filepath.Join(f.state, "artifacts", fact.Output.Path)
	if data, err := os.ReadFile(artifact); err != nil || string(data) != "fact-one\n" || fact.Output.Bytes != 9 || !fact.Output.Available {
		t.Fatalf("step 1 artifact = %q, %v; observation %+v", data, err, fact)
	}
	if r := f.cli(nil, "resume"); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("resume = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	after := f.cli(nil, "history")
	if before.code != exitOK || after.code != exitOK || !strings.HasPrefix(after.stdout, before.stdout) {
		t.Errorf("the committed history changed:\nbefore: %s\nafter: %s", before.stdout, after.stdout)
	}
	h = f.history()
	if !slices.Equal(kinds(h), slices.Concat(evStep, evStep, evReturn)) {
		t.Fatalf("history = %q", kinds(h))
	}
	tampered := h[13].Facts.Observation
	for _, want := range []string{"Read-only file system", "rm=1", "write=1", path.Base(path.Dir(fact.Output.Path))} {
		if tampered == nil || !strings.Contains(tampered.Preview, want) {
			t.Errorf("tampering output lacks %q: %+v", want, tampered)
		}
	}
	if data, err := os.ReadFile(artifact); err != nil || string(data) != "fact-one\n" {
		t.Errorf("step 1 artifact after tampering = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(f.state, "artifacts", "forged")); !os.IsNotExist(err) {
		t.Errorf("the job wrote the artifacts: %v", err)
	}
	for _, name := range []string{"canti.db", "artifacts/output"} {
		if data, err := os.ReadFile(filepath.Join(f.workspace, name)); err != nil || string(data) != "forged\n" {
			t.Errorf("workspace %s = %q, %v", name, data, err)
		}
	}
	ref := fmt.Sprintf("Output: 9 bytes, complete, at %s", path.Join(docker.ArtifactDir, fact.Output.Path))
	if p := f.model.prompts(3); len(p) != 1 || !strings.Contains(p[0], ref) || !strings.Contains(p[0], "fact-one") {
		t.Errorf("step 3 prompt lacks %q: %q", ref, p)
	}
	if s := f.status(); s.Phase != core.PhaseReturned || s.Totals.Steps != 2 {
		t.Errorf("status = %+v", s)
	}
	checkReleased(t, f, h)
	evLog(t, image, "trust boundary (facts)", "step 2 runs rm -rf /canti/artifacts/*, writes into /canti/artifacts, and forges canti.db and artifacts/output in /workspace",
		"removal and write fail read-only; committed history is an unchanged prefix; step 1 artifact intact; forgeries stay in the workspace",
		fmt.Sprintf("history %d bytes before, %d after; artifact %d bytes", len(before.stdout), len(after.stdout), fact.Output.Bytes))
}

func TestGateTrustBoundaryProtectsStepRecords(t *testing.T) {
	long := "echo fact-one; echo 日本; : " + strings.Repeat("x", 3000)
	probe := strings.Join([]string{
		"cd " + runDir,
		"echo '== meta'", "cat steps/00000001/meta",
		"echo '== command'", "cat steps/00000001/command; echo",
		"echo '== preview'", "cat steps/00000001/preview",
		"echo '== goal'", "cat goal; echo",
		"echo '== tamper'",
		`touch x; echo "touch=$?"`,
		`echo forged > goal; echo "goal=$?"`,
		`echo forged > steps/00000001/preview; echo "preview=$?"`,
		`rm -rf steps/00000001; echo "rm=$?"`,
		`mkdir steps/00000002; echo "mkdir=$?"`,
		`mkdir ` + runsDir + `/forged; echo "runs=$?"`,
		"echo '== runs'", "ls " + runsDir,
	}, "\n")
	f, image := evFixture(t, map[int]core.Decision{
		1: {Kind: core.Exec, Payload: long},
		2: {Kind: core.Exec, Payload: probe},
		3: {Kind: core.Return, Payload: "done"},
	})
	if r := f.cli(nil, "run", "-goal", "records", "-image", image); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	h := f.history()
	if !slices.Equal(kinds(h), slices.Concat(evStep, evStep, evReturn)) {
		t.Fatalf("history = %q", kinds(h))
	}
	id, one := h[0].State.RunID, h[6].Facts.Observation
	data, err := os.ReadFile(filepath.Join(f.state, "artifacts", h[13].Facts.Observation.Output.Path))
	if err != nil {
		t.Fatal(err)
	}
	s := evSections(string(data))
	meta := fmt.Sprintf("step: 1\nexecution: exited\nexit_code: 0\nresolved: false\noutput: %s\noutput_bytes: %d\noutput_complete: true\n",
		path.Join(docker.ArtifactDir, one.Output.Path), one.Output.Bytes)
	if s["meta"] != meta || s["command"] != long+"\n" || s["preview"] != "fact-one\n日本\n" || s["preview"] != one.Preview || s["goal"] != "records\n" || s["runs"] != id+"\n" {
		t.Errorf("records read by step 2 = %q\nwant meta %q", s, meta)
	}
	for _, want := range []string{"touch=1", "goal=1", "preview=1", "rm=1", "mkdir=1", "runs=1", "Read-only file system"} {
		if !strings.Contains(s["tamper"], want) {
			t.Errorf("tampering output lacks %q:\n%s", want, s["tamper"])
		}
	}
	if p := f.model.prompts(2); len(p) != 1 || strings.Contains(p[0], long) || !strings.Contains(p[0], "…[truncated ") || !strings.Contains(p[0], runDir+"/steps/") {
		t.Errorf("step 2 prompt does not cut the command and name the records: %q", p)
	}
	for _, name := range []string{filepath.Join(id, "x"), "forged"} {
		if _, err := os.Stat(filepath.Join(f.state, "history", name)); !os.IsNotExist(err) {
			t.Errorf("the job created %s: %v", name, err)
		}
	}
	checkRecords(t, f, "records", h)

	earlier := strings.Join([]string{
		"cd " + runsDir + "/" + id,
		"echo '== goal'", "cat goal; echo",
		"echo '== result'", "cat result; echo",
		"echo '== command'", "cat steps/00000002/command; echo",
		"echo '== runs'", "ls " + runsDir,
	}, "\n")
	for step, d := range map[int]core.Decision{1: {Kind: core.Exec, Payload: earlier}, 2: {Kind: core.Return, Payload: "second"}} {
		text, _ := json.Marshal(d)
		f.model.answer(step, 2, reply{text: string(text)})
	}
	if r := f.cli(nil, "run", "-goal", "again", "-image", image); r.code != exitOK || r.stdout != "second\n" {
		t.Fatalf("second run = %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	h2 := f.history()
	if !slices.Equal(kinds(h2), slices.Concat(evStep, evReturn)) || h2[0].State.RunID == id {
		t.Fatalf("second run history = %q", kinds(h2))
	}
	if data, err = os.ReadFile(filepath.Join(f.state, "artifacts", h2[6].Facts.Observation.Output.Path)); err != nil {
		t.Fatal(err)
	}
	s2 := evSections(string(data))
	runs := strings.Join(slices.Sorted(slices.Values([]string{id, h2[0].State.RunID})), "\n") + "\n"
	if s2["goal"] != "records\n" || s2["result"] != "phase: returned\n\ndone\n" || s2["command"] != probe+"\n" || s2["runs"] != runs {
		t.Errorf("first run read by the second = %q", s2)
	}
	checkRecords(t, f, "again", h2)
	checkReleased(t, f, slices.Concat(h, h2))
	evLog(t, image, "trust boundary (step records)", "step 2 reads /canti/run and tries to write, overwrite, and remove it and /canti/runs; a second run reads the first under /canti/runs",
		"step 1 records match its observation and full command; every write fails read-only; the second run reads the first run's goal, result, and commands",
		fmt.Sprintf("command %d bytes, meta %d bytes, %d runs", len(long), len(meta), strings.Count(runs, "\n")))
}
