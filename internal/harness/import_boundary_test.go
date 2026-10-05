package harness_test

import (
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/ChrAlpha/canti"

func goList(t *testing.T, args ...string) []string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Env = append(os.Environ(), "GOPROXY=off", "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("go list %s: %v\n%s", strings.Join(args, " "), err, exit.Stderr)
		}
		t.Fatalf("go list %s: %v", strings.Join(args, " "), err)
	}
	return strings.Fields(string(out))
}

func TestPackagesKeepTheirImportBoundaries(t *testing.T) {
	nonStandard := "{{if not .Standard}}{{.ImportPath}}{{end}}"
	if got, want := goList(t, "-deps", "-f", nonStandard, module+"/core"), []string{module + "/core"}; !slices.Equal(got, want) {
		t.Errorf("core depends on %v, want only the standard library", got)
	}
	if got, want := goList(t, "-deps", "-f", nonStandard, module+"/internal/harness"), []string{module + "/core", module + "/internal/harness"}; !slices.Equal(got, want) {
		t.Errorf("internal/harness depends on %v, want only the standard library and core", got)
	}
	deps := goList(t, "-deps", module+"/cmd/canti")
	if !slices.Contains(deps, module+"/internal/harness") {
		t.Fatalf("cmd/canti dependency list %v does not hold internal/harness", deps)
	}
	if slices.Contains(deps, module+"/internal/harness/executortest") {
		t.Errorf("cmd/canti depends on internal/harness/executortest")
	}
}
