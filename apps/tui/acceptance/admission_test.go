//go:build acceptance

package acceptance

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestTUIStateDockerCannotMountIsRefusedBeforeItExists(t *testing.T) {
	for _, args := range [][]string{{tuiCommand}} {
		f := newFixture(t, nil)
		state := filepath.Join(t.TempDir(), "a:b", "state")
		r := f.cli(nil, args[0], slices.Concat(args[1:], []string{"-state", state})...)
		if r.code != exitError || !strings.Contains(r.stderr, "can be mounted") {
			t.Errorf("canti %q = %d\nstderr: %s", args, r.code, r.stderr)
		}
		if _, err := os.Stat(filepath.Dir(state)); !os.IsNotExist(err) {
			t.Errorf("canti %q created %s: %v", args, filepath.Dir(state), err)
		}
	}
}
func TestTUIStateInsideTheWorkspaceIsRefused(t *testing.T) {
	for _, layout := range []struct {
		name     string
		paths    func(workspace, link string) (string, string)
		absent   bool
		existing bool
	}{
		{"inside", func(ws, _ string) (string, string) { return ws, filepath.Join(ws, "state") }, true, false},
		{"containing", func(ws, _ string) (string, string) { return ws, filepath.Dir(ws) }, false, false},
		{"inside a linked workspace", func(_, link string) (string, string) { return link, filepath.Join(link, "state") }, true, false},
		{"inside, holding a journal", func(ws, _ string) (string, string) { return ws, filepath.Join(ws, "state") }, false, true},
	} {
		for _, command := range []struct {
			name string
			args []string
		}{
			{tuiCommand, []string{tuiCommand}},
		} {
			t.Run(layout.name+"/"+command.name, func(t *testing.T) {
				f := newFixture(t, nil)
				link := filepath.Join(t.TempDir(), "workspace")
				if err := os.Symlink(f.workspace, link); err != nil {
					t.Fatal(err)
				}
				workspace, state := layout.paths(f.workspace, link)
				if layout.existing {
					if err := os.Mkdir(state, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(state, "canti.db"), []byte("not a journal\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				tree := func() (files map[string]string) {
					files = map[string]string{}
					for _, dir := range []string{filepath.Dir(f.workspace), filepath.Dir(link)} {
						if err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							files[p] = info.ModTime().String()
							if info.Mode().IsRegular() {
								data, err := os.ReadFile(p)
								files[p] += string(data)
								return err
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					}
					return files
				}
				before := tree()
				r := f.cli(nil, command.args[0], slices.Concat(command.args[1:], []string{"-workspace", workspace, "-state", state})...)
				if r.code != exitError || !strings.Contains(r.stderr, "overlap") {
					t.Fatalf("%s = %d\nstderr: %s", command.name, r.code, r.stderr)
				}
				if _, err := os.Stat(state); layout.absent && !os.IsNotExist(err) {
					t.Errorf("state directory inside the workspace: %v", err)
				}
				if _, err := os.Stat(filepath.Join(state, "history")); !os.IsNotExist(err) {
					t.Errorf("history directory of the refused state: %v", err)
				}
				after := tree()
				for _, p := range slices.Sorted(maps.Keys(after)) {
					if b, ok := before[p]; !ok || b != after[p] {
						t.Errorf("the refusal created or changed %s", p)
					}
				}
				for p := range before {
					if _, ok := after[p]; !ok {
						t.Errorf("the refusal removed %s", p)
					}
				}
				if len(f.model.requests()) != 0 {
					t.Error("the model was called")
				}
			})
		}
	}
}
