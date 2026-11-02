// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/ast"
	"regexp"
	"strings"
)

// directiveRE matches the text after "//" of a directive comment (DET-200).
var directiveRE = regexp.MustCompile(`^tenure:([a-z]+)(?:[ \t]+(.*\S))?[ \t]*$`)

// directiveRule returns the rule the directive name suppresses, and whether name is a known
// directive (DET-200). No other rule can be suppressed.
func directiveRule(name string) (Rule, bool) {
	switch name {
	case "maporder":
		return RuleMapRange, true
	case "totalorder":
		return RuleSort, true
	}
	return "", false
}

// directive is one //tenure:<name> <reason> comment.
type directive struct {
	file   string // slash path relative to r.base, as findings name it
	line   int    // line of the comment
	col    int    // column of the comment
	target int    // the line the directive applies to
	name   string
	reason string
}

// directives returns the directives of the host file f in source order. A directive on a line
// where a syntax node starts or ends applies to that line; any other directive applies to the
// next line (DET-200).
func (r *pkgRun) directives(f *ast.File) []*directive {
	code := map[int]bool{} // lines where a syntax node starts or ends
	ast.Inspect(f, func(n ast.Node) bool {
		switch n.(type) {
		case nil, *ast.CommentGroup:
			return false
		}
		code[r.c.fset.PositionFor(n.Pos(), false).Line] = true
		code[r.c.fset.PositionFor(n.End()-1, false).Line] = true
		return true
	})
	var out []*directive
	for _, g := range f.Comments {
		for _, c := range g.List {
			text, ok := strings.CutPrefix(c.Text, "//")
			if !ok {
				continue
			}
			m := directiveRE.FindStringSubmatch(text)
			if m == nil {
				continue
			}
			pos := r.c.fset.PositionFor(c.Pos(), false)
			target := pos.Line + 1
			if code[pos.Line] {
				target = pos.Line
			}
			out = append(out, &directive{file: r.rel(pos.Filename), line: pos.Line, col: pos.Column, target: target, name: m[1], reason: m[2]})
		}
	}
	return out
}

// applyDirectives removes from r.out the findings that valid directives suppress, and adds a
// DT000 finding for each directive that is unknown, has no reason, or suppresses nothing
// (DET-200).
func (r *pkgRun) applyDirectives(dirs []*directive) {
	var problems []Finding
	meta := func(d *directive, msg string) {
		problems = append(problems, Finding{File: d.file, Line: d.line, Col: d.col, Rule: RuleMeta, Msg: msg})
	}
	used := make([]bool, len(dirs))
	kept := make([]Finding, 0, len(r.out))
	for _, f := range r.out {
		suppressed := false
		for i, d := range dirs {
			rule, known := directiveRule(d.name)
			if known && d.reason != "" && f.Rule == rule && f.File == d.file && f.Line == d.target {
				used[i], suppressed = true, true
			}
		}
		if !suppressed {
			kept = append(kept, f)
		}
	}
	for i, d := range dirs {
		switch _, known := directiveRule(d.name); {
		case !known:
			meta(d, "unknown directive //tenure:"+d.name)
		case d.reason == "":
			meta(d, fmt.Sprintf("//tenure:%s needs a reason", d.name))
		case !used[i]:
			meta(d, fmt.Sprintf("//tenure:%s suppresses no finding", d.name))
		}
	}
	kept = append(kept, problems...)
	r.out = kept
}
