package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/host"
)

// TestDefaultEnvironmentMatchesTheRequestCorpus reads the environment the
// request corpus of internal/harness freezes from its source, so the corpus
// cannot drift from the environment a default run states.
func TestDefaultEnvironmentMatchesTheRequestCorpus(t *testing.T) {
	path := filepath.Join("..", "..", "internal", "harness", "frozen_request_fixture_test.go")
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	var corpus []string
	ast.Inspect(f, func(n ast.Node) bool {
		if v, ok := n.(*ast.ValueSpec); ok && len(v.Names) == 1 && v.Names[0].Name == "corpusEnvironment" && len(v.Values) == 1 {
			corpus = append(corpus, concatenation(t, v.Values[0]))
		}
		return true
	})
	if len(corpus) != 1 {
		t.Fatalf("%s declares corpusEnvironment %d times", path, len(corpus))
	}
	s := runSpec(t)
	if got := host.Environment(s.Config, s.Budget); got != corpus[0] {
		t.Errorf("default environment\n%s\ndiffers from the corpus environment\n%s", got, corpus[0])
	}
}

func concatenation(t *testing.T, e ast.Expr) string {
	t.Helper()
	switch e := e.(type) {
	case *ast.BasicLit:
		if s, err := strconv.Unquote(e.Value); err == nil && e.Kind == token.STRING {
			return s
		}
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			return concatenation(t, e.X) + concatenation(t, e.Y)
		}
	}
	t.Fatalf("corpusEnvironment is not a concatenation of string literals")
	return ""
}
