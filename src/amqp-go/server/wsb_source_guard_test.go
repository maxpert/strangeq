package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWSB_NoDirectSocketWrites (A4 source guard): the ONLY server files that
// may write to a client socket are conn_write.go (the deadline-armed helpers)
// and the two whitelisted WriteFrameToConnectionWithDeadline users
// (alarm_monitor.go, frame_helpers.go — reader-flow/alarm paths that set and
// clear their own deadline under WriteMutex).
func TestWSB_NoDirectSocketWrites(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}

	isConnConnWrite := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Write" {
			return false
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		base, ok := inner.X.(*ast.Ident)
		return ok && base.Name == "conn" && inner.Sel.Name == "Conn"
	}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "conn_write.go" {
			continue // the helpers themselves
		}
		path := filepath.Join(".", name)
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch {
			case isConnConnWrite(call.Fun):
				t.Errorf("%s:%d: direct conn.Conn.Write outside conn_write.go", name, fset.Position(call.Pos()).Line)
			case sel.Sel.Name == "WriteFrameToConnection":
				// Exact match: WriteFrameToConnectionWithDeadline is a different
				// (whitelisted) function.
				t.Errorf("%s:%d: protocol.WriteFrameToConnection outside conn_write.go", name, fset.Position(call.Pos()).Line)
			case sel.Sel.Name == "WriteTo" && len(call.Args) > 0 && isConnConn(call.Args[0]):
				t.Errorf("%s:%d: WriteTo(conn.Conn) outside conn_write.go", name, fset.Position(call.Pos()).Line)
			case sel.Sel.Name == "WriteFrameToConnectionWithDeadline":
				if name != "alarm_monitor.go" && name != "frame_helpers.go" {
					t.Errorf("%s:%d: WriteFrameToConnectionWithDeadline outside the whitelist", name, fset.Position(call.Pos()).Line)
				}
			}
			return true
		})
	}
}

func isConnConn(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	base, ok := sel.X.(*ast.Ident)
	return ok && base.Name == "conn" && sel.Sel.Name == "Conn"
}
