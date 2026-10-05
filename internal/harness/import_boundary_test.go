package harness_test

import (
	"errors"
	"go/build"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/ChrAlpha/canti"

// moduleImports reads the non-test imports of every package of the module in
// this process, honouring build constraints, so the test cache tracks each
// file it reads and no module lookup is needed.
func moduleImports(t *testing.T) map[string][]string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if mod, err := os.ReadFile(filepath.Join(root, "go.mod")); err != nil || !strings.HasPrefix(string(mod), "module "+module+"\n") {
		t.Fatalf("%s is not the root of module %s: %v", root, module, err)
	}
	imports := map[string][]string{}
	err = filepath.WalkDir(root, func(dir string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		if dir != root {
			if name := d.Name(); strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				return filepath.SkipDir
			}
		}
		p, err := build.ImportDir(dir, 0)
		var none *build.NoGoError
		if errors.As(err, &none) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, dir)
		path := module
		if rel != "." {
			path += "/" + filepath.ToSlash(rel)
		}
		imports[path] = p.Imports
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return imports
}

// dependencies returns the module packages path reaches through non-test
// imports and the packages outside the module and the standard library,
// whose first path element has no dot, that those import.
func dependencies(t *testing.T, imports map[string][]string, path string) (internal, external []string) {
	t.Helper()
	if _, ok := imports[path]; !ok {
		t.Fatalf("module has no package %s", path)
	}
	seen := map[string]bool{path: true}
	for queue := []string{path}; len(queue) > 0; queue = queue[1:] {
		for _, imp := range imports[queue[0]] {
			switch {
			case seen[imp]:
			case imp == module || strings.HasPrefix(imp, module+"/"):
				if _, ok := imports[imp]; !ok {
					t.Fatalf("%s imports %s, which the module does not hold", queue[0], imp)
				}
				internal, queue = append(internal, imp), append(queue, imp)
			case strings.Contains(strings.Split(imp, "/")[0], "."):
				external = append(external, imp)
			}
			seen[imp] = true
		}
	}
	slices.Sort(internal)
	slices.Sort(external)
	return internal, external
}

func TestPackagesKeepTheirImportBoundaries(t *testing.T) {
	imports := moduleImports(t)
	for _, tc := range []struct {
		path string
		want []string
	}{
		{module + "/core", nil},
		{module + "/internal/harness", []string{module + "/core"}},
	} {
		if internal, external := dependencies(t, imports, tc.path); !slices.Equal(internal, tc.want) || external != nil {
			t.Errorf("%s depends on module packages %v and third-party packages %v, want only the standard library and %v", tc.path, internal, external, tc.want)
		}
	}
	deps, _ := dependencies(t, imports, module+"/cmd/canti")
	if !slices.Contains(deps, module+"/internal/harness") {
		t.Fatalf("cmd/canti module dependencies %v do not hold internal/harness", deps)
	}
	if slices.Contains(deps, module+"/internal/harness/executortest") {
		t.Errorf("cmd/canti depends on internal/harness/executortest")
	}
}
