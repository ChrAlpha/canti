package anthropic_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness/anthropic"
)

func TestEveryDecoderSwitchNameIsInProtocolTable(t *testing.T) {
	table := map[string]bool{}
	for _, name := range anthropic.ProtocolNames() {
		table[name] = true
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		check := func(lit *ast.BasicLit) {
			if lit.Kind != token.STRING {
				return
			}
			name, err := strconv.Unquote(lit.Value)
			if err != nil || name == "" {
				return
			}
			if !table[name] || anthropic.ProtocolCategory(name) == "" {
				t.Errorf("%s: decoder literal %q is not in the protocol table with a category", fset.Position(lit.Pos()), name)
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CaseClause:
				for _, expr := range node.List {
					if lit, ok := expr.(*ast.BasicLit); ok {
						check(lit)
					}
				}
			case *ast.CallExpr:
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "HasPrefix", "HasSuffix", "Contains", "EqualFold":
						for _, arg := range node.Args {
							if lit, ok := arg.(*ast.BasicLit); ok {
								check(lit)
							}
						}
					}
				}
			case *ast.BinaryExpr:
				if node.Op == token.EQL || node.Op == token.NEQ {
					if lit, ok := node.X.(*ast.BasicLit); ok {
						check(lit)
					}
					if lit, ok := node.Y.(*ast.BasicLit); ok {
						check(lit)
					}
				}
			}
			return true
		})
	}
}

func TestProtocolCategoriesAreAssigned(t *testing.T) {
	events := map[string]bool{}
	for _, name := range anthropic.ProtocolEvents() {
		events[name] = true
	}
	for _, name := range anthropic.ProtocolNames() {
		if anthropic.ProtocolCategory(name) == "" {
			t.Errorf("%q has no protocol category", name)
		}
		if anthropic.ProtocolIsEvent(name) != events[name] {
			t.Errorf("%q event category = %v, ProtocolEvents = %v", name, anthropic.ProtocolIsEvent(name), events[name])
		}
	}
}
