package harness_test

import (
	"go/build"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/ChrAlpha/canti"

// buildTags lists every build tag the module's files are built with.
var buildTags = []string{"integration", "live", "race"}

// dependencies reads, in this process and honouring the build constraints of
// every combination of buildTags, the non-test imports of path and of every
// module package they reach, so the test cache tracks each file it reads and
// no module lookup is needed. It returns the module packages reached and the
// imports outside the module and the standard library, whose first path
// element has no dot.
func dependencies(t *testing.T, path string) (internal, external []string) {
	t.Helper()
	for set := 0; set < 1<<len(buildTags); set++ {
		ctx := build.Default
		for i, tag := range buildTags {
			if set&(1<<i) != 0 {
				ctx.BuildTags = append(ctx.BuildTags, tag)
			}
		}
		in, ex := tagDependencies(t, ctx, path)
		internal, external = append(internal, in...), append(external, ex...)
	}
	slices.Sort(internal)
	slices.Sort(external)
	return slices.Compact(internal), slices.Compact(external)
}

func tagDependencies(t *testing.T, ctx build.Context, path string) (internal, external []string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if mod, err := os.ReadFile(filepath.Join(root, "go.mod")); err != nil || !strings.HasPrefix(string(mod), "module "+module+"\n") {
		t.Fatalf("%s is not the root of module %s: %v", root, module, err)
	}
	seen := map[string]bool{path: true}
	for queue := []string{path}; len(queue) > 0; queue = queue[1:] {
		p, err := ctx.ImportDir(filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(queue[0], module))), 0)
		if err != nil {
			t.Fatalf("read package %s with tags %v: %v", queue[0], ctx.BuildTags, err)
		}
		for _, imp := range p.Imports {
			switch {
			case seen[imp]:
			case imp == module || strings.HasPrefix(imp, module+"/"):
				internal, queue = append(internal, imp), append(queue, imp)
			case strings.Contains(strings.Split(imp, "/")[0], "."):
				external = append(external, imp)
			}
			seen[imp] = true
		}
	}
	return internal, external
}

func TestPackagesKeepTheirImportBoundaries(t *testing.T) {
	for _, tc := range []struct {
		path string
		want []string
	}{
		{module + "/core", nil},
		{module + "/internal/harness", []string{module + "/core"}},
	} {
		if internal, external := dependencies(t, tc.path); !slices.Equal(internal, tc.want) || external != nil {
			t.Errorf("%s depends on module packages %v and third-party packages %v, want only the standard library and %v", tc.path, internal, external, tc.want)
		}
	}
	deps, _ := dependencies(t, module+"/cmd/canti")
	if !slices.Contains(deps, module+"/internal/harness") {
		t.Fatalf("cmd/canti module dependencies %v do not hold internal/harness", deps)
	}
	if slices.Contains(deps, module+"/internal/harness/executortest") {
		t.Errorf("cmd/canti depends on internal/harness/executortest")
	}
}
