// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// panicValue runs fn and returns what it panicked with, formatted with %v, or "" if it returned.
func panicValue(fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return ""
}

const goldenHeader = "# tenure golden v1\n# name seed hash count\n"

// TestATDET08GoldenFormat checks DET-400 (AT-DET-08).
func TestATDET08GoldenFormat(t *testing.T) {
	entries := []Entry{
		{Name: "fanin", Seed: 2, Hash: 0xa, Count: 5},
		{Name: "ask", Seed: 9, Hash: 0xb, Count: 7},
		{Name: "ask", Seed: 3, Hash: 0xc, Count: 1},
	}
	want := goldenHeader +
		"ask 0x0000000000000003 0x000000000000000c 1\n" +
		"ask 0x0000000000000009 0x000000000000000b 7\n" +
		"fanin 0x0000000000000002 0x000000000000000a 5\n"
	input := slices.Clone(entries)
	got := FormatGolden(input)
	if string(got) != want {
		t.Fatalf("FormatGolden:\n%s\nwant:\n%s", got, want)
	}
	if !slices.Equal(input, entries) {
		t.Errorf("FormatGolden reordered its argument: %v", input)
	}

	parsed, err := ParseGolden("x.txt", got)
	if err != nil {
		t.Fatalf("ParseGolden: %v", err)
	}
	wantParsed := []Entry{entries[2], entries[1], entries[0]}
	if !slices.Equal(parsed, wantParsed) {
		t.Errorf("ParseGolden = %v, want %v", parsed, wantParsed)
	}

	if got := string(FormatGolden(nil)); got != goldenHeader {
		t.Errorf("FormatGolden(nil) = %q, want %q", got, goldenHeader)
	}
	if got, err := ParseGolden("x.txt", []byte(goldenHeader)); err != nil || len(got) != 0 {
		t.Errorf("ParseGolden(header only) = %v, %v; want no entries and no error", got, err)
	}

	for _, name := range []string{"Bad Name", "", "-a", "a b", "a\n"} {
		msg := panicValue(func() { FormatGolden([]Entry{{Name: name}}) })
		if want := fmt.Sprintf("dettest: invalid golden name %q", name); msg != want {
			t.Errorf("FormatGolden(name %q) panic = %q, want %q", name, msg, want)
		}
	}
	for _, name := range []string{"a", "0", "keyshard", "a.b_c/d-e", "z9"} {
		if msg := panicValue(func() { FormatGolden([]Entry{{Name: name}}) }); msg != "" {
			t.Errorf("FormatGolden(name %q) panicked: %s", name, msg)
		}
	}

	// Two entries with one (Name, Seed) would give a file ParseGolden rejects.
	dup := []Entry{{"b", 1, 0x1, 1}, {"ask", 3, 0x1, 1}, {"b", 2, 0x2, 2}, {"ask", 3, 0x2, 2}}
	msg := panicValue(func() { FormatGolden(dup) })
	if want := "dettest: duplicate golden entry ask 0x0000000000000003"; msg != want {
		t.Errorf("FormatGolden with a duplicate (Name, Seed) panic = %q, want %q", msg, want)
	}
	sameName := []Entry{{"ask", 3, 0x1, 1}, {"ask", 4, 0x1, 1}, {"fanin", 3, 0x1, 1}}
	if msg := panicValue(func() { FormatGolden(sameName) }); msg != "" {
		t.Errorf("FormatGolden of distinct (Name, Seed) pairs panicked: %s", msg)
	}

	// The largest values round-trip in every numeric field.
	const maxU64 = ^uint64(0)
	big := []Entry{{Name: "max", Seed: maxU64, Hash: maxU64, Count: maxU64}}
	data := FormatGolden(big)
	if want := goldenHeader + "max 0xffffffffffffffff 0xffffffffffffffff 18446744073709551615\n"; string(data) != want {
		t.Errorf("FormatGolden(MaxUint64 fields) = %q, want %q", data, want)
	}
	if got, err := ParseGolden("x.txt", data); err != nil || !slices.Equal(got, big) {
		t.Errorf("ParseGolden(MaxUint64 fields) = %v, %v; want %v", got, err, big)
	}
}

// TestGoldenParseAccepts pins the inputs ParseGolden accepts although FormatGolden never writes
// them (DET-410 checks line 1 only and skips later # lines).
func TestGoldenParseAccepts(t *testing.T) {
	ask := Entry{Name: "ask", Seed: 3, Hash: 0xc, Count: 1}
	tests := []struct {
		name, data string
		want       []Entry
	}{
		{"no final newline", goldenHeader + "ask 0x0000000000000003 0x000000000000000c 1", []Entry{ask}},
		{"header without final newline", "# tenure golden v1", nil},
		{"no line 2", "# tenure golden v1\nask 0x0000000000000003 0x000000000000000c 1\n", []Entry{ask}},
		{"other line 2", "# tenure golden v1\n# columns\nask 0x0000000000000003 0x000000000000000c 1\n", []Entry{ask}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGolden("x.txt", []byte(tt.data))
			if err != nil {
				t.Fatalf("ParseGolden: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("ParseGolden = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestATDET09GoldenParseErrors checks DET-410 (AT-DET-09).
func TestATDET09GoldenParseErrors(t *testing.T) {
	const (
		ask3  = "ask 0x0000000000000003 0x000000000000000c 1\n"
		ask9  = "ask 0x0000000000000009 0x000000000000000b 7\n"
		fanin = "fanin 0x0000000000000002 0x000000000000000a 5\n"
	)
	tests := []struct {
		name, data, want string
	}{
		{"empty", "", `x.txt: missing header "# tenure golden v1"`},
		{"wrong header", "# tenure golden v2\n", `x.txt: missing header "# tenure golden v1"`},
		{"header with trailing space", "# tenure golden v1 \n", `x.txt: missing header "# tenure golden v1"`},
		{"three fields", goldenHeader + "ask 0x1 0x2\n", "x.txt:3: malformed entry"},
		{"five fields", goldenHeader + "ask 0x0000000000000003 0x000000000000000c 1 2\n", "x.txt:3: malformed entry"},
		{"two spaces", goldenHeader + "ask  0x0000000000000003 0x000000000000000c 1\n", "x.txt:3: malformed entry"},
		{"empty line", goldenHeader + "\n", "x.txt:3: malformed entry"},
		{"invalid name", goldenHeader + "Ask 0x0000000000000003 0x000000000000000c 1\n", "x.txt:3: malformed entry"},
		{"short seed", goldenHeader + "ask 0x3 0x000000000000000c 1\n", "x.txt:3: malformed entry"},
		{"seed without 0x", goldenHeader + "ask 000000000000000003 0x000000000000000c 1\n", "x.txt:3: malformed entry"},
		{"uppercase hash", goldenHeader + "ask 0x0000000000000003 0x000000000000000C 1\n", "x.txt:3: malformed entry"},
		{"hash not hex", goldenHeader + "ask 0x0000000000000003 0x00000000000000zz 1\n", "x.txt:3: malformed entry"},
		{"negative count", goldenHeader + "ask 0x0000000000000003 0x000000000000000c -1\n", "x.txt:3: malformed entry"},
		{"count with sign", goldenHeader + "ask 0x0000000000000003 0x000000000000000c +1\n", "x.txt:3: malformed entry"},
		{"count with leading zero", goldenHeader + "ask 0x0000000000000003 0x000000000000000c 01\n", "x.txt:3: malformed entry"},
		{"count overflows", goldenHeader + "ask 0x0000000000000003 0x000000000000000c 18446744073709551616\n", "x.txt:3: malformed entry"},
		{"duplicate", goldenHeader + ask3 + ask3, "x.txt:4: duplicate entry ask 0x0000000000000003"},
		{"duplicate with another hash", goldenHeader + ask3 + "ask 0x0000000000000003 0x0000000000000001 2\n", "x.txt:4: duplicate entry ask 0x0000000000000003"},
		{"out of order names", goldenHeader + fanin + ask3, "x.txt:4: entries out of order"},
		{"out of order seeds", goldenHeader + ask9 + ask3, "x.txt:4: entries out of order"},
		{"duplicate before order", goldenHeader + ask3 + ask9 + ask3, "x.txt:5: duplicate entry ask 0x0000000000000003"},
		{"first problem wins", goldenHeader + fanin + ask3 + "bad\n", "x.txt:4: entries out of order"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGolden("x.txt", []byte(tt.data))
			if err == nil {
				t.Fatalf("ParseGolden = %v, nil; want error %q", got, tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("ParseGolden error = %q, want %q", err, tt.want)
			}
			if got != nil {
				t.Errorf("ParseGolden returned entries %v with an error", got)
			}
		})
	}

	t.Run("comment lines are skipped", func(t *testing.T) {
		data := goldenHeader + ask3 + "# note\n" + ask9 + "#\n" + fanin
		got, err := ParseGolden("x.txt", []byte(data))
		if err != nil {
			t.Fatalf("ParseGolden: %v", err)
		}
		want := []Entry{
			{Name: "ask", Seed: 3, Hash: 0xc, Count: 1},
			{Name: "ask", Seed: 9, Hash: 0xb, Count: 7},
			{Name: "fanin", Seed: 2, Hash: 0xa, Count: 5},
		}
		if !slices.Equal(got, want) {
			t.Errorf("ParseGolden = %v, want %v", got, want)
		}
	})

	t.Run("line numbers count comment lines", func(t *testing.T) {
		data := goldenHeader + "# note\n" + ask3 + "ask 0x1 0x2\n"
		_, err := ParseGolden("x.txt", []byte(data))
		if want := "x.txt:5: malformed entry"; err == nil || err.Error() != want {
			t.Errorf("ParseGolden error = %v, want %q", err, want)
		}
	})
}

// TestATDET10Compare checks DET-420 (AT-DET-10).
func TestATDET10Compare(t *testing.T) {
	got := []Entry{{"a", 1, 0x1, 5}, {"b", 2, 0x2, 6}}
	want := []Entry{{"a", 1, 0x9, 5}, {"c", 3, 0x3, 7}}
	wantLines := []string{
		"golden: a seed 0x0000000000000001: got hash 0x0000000000000001 count 5, want hash 0x0000000000000009 count 5",
		"golden: b seed 0x0000000000000002: missing from the golden file",
		"golden: c seed 0x0000000000000003: in the golden file but not produced",
	}
	if lines := CompareGolden(got, want); !slices.Equal(lines, wantLines) {
		t.Errorf("CompareGolden =\n%q\nwant\n%q", lines, wantLines)
	}
	// Unsorted inputs give the same sorted report.
	if lines := CompareGolden([]Entry{got[1], got[0]}, []Entry{want[1], want[0]}); !slices.Equal(lines, wantLines) {
		t.Errorf("CompareGolden of reversed inputs =\n%q\nwant\n%q", lines, wantLines)
	}

	countOnly := CompareGolden([]Entry{{"a", 1, 0x1, 5}}, []Entry{{"a", 1, 0x1, 6}})
	wantCount := []string{"golden: a seed 0x0000000000000001: got hash 0x0000000000000001 count 5, want hash 0x0000000000000001 count 6"}
	if !slices.Equal(countOnly, wantCount) {
		t.Errorf("CompareGolden of a count change = %q, want %q", countOnly, wantCount)
	}

	x := []Entry{{"a", 1, 0x1, 5}, {"a", 2, 0x2, 6}, {"b", 1, 0x3, 7}}
	y := []Entry{x[2], x[0], x[1]}
	if lines := CompareGolden(x, y); lines != nil {
		t.Errorf("CompareGolden of equal entries in different orders = %q, want nil", lines)
	}
	if lines := CompareGolden(nil, nil); lines != nil {
		t.Errorf("CompareGolden(nil, nil) = %q, want nil", lines)
	}
	if !slices.Equal(x[:2], []Entry{{"a", 1, 0x1, 5}, {"a", 2, 0x2, 6}}) || y[0] != (Entry{"b", 1, 0x3, 7}) {
		t.Errorf("CompareGolden reordered its arguments: %v, %v", x, y)
	}
}

// TestATDET11Digest checks DET-430 (AT-DET-11).
func TestATDET11Digest(t *testing.T) {
	var d Digest
	if got, want := d.Entry("e"), (Entry{"e", 0, 0xcbf29ce484222325, 0}); got != want {
		t.Errorf("zero Digest Entry = %+v, want %+v", got, want)
	}
	d.Add("a")
	if got, want := d.Entry("e"), (Entry{"e", 0, 0x089bdc07b544e7b2, 1}); got != want {
		t.Errorf("after Add(a): Entry = %+v, want %+v", got, want)
	}

	var d2 Digest
	d2.Add("a")
	d2.Add("b")
	want := Entry{"e", 0, 0x78ed6781f136a14e, 2}
	if got := d2.Entry("e"); got != want {
		t.Errorf("after Add(a), Add(b): Entry = %+v, want %+v", got, want)
	}
	if got := d2.Entry("e"); got != want {
		t.Errorf("second Entry = %+v, want %+v", got, want)
	}

	var d3 Digest
	d3.Add("")
	if got, want := d3.Entry("e"), (Entry{"e", 0, 0xaf63c74c8601c8dd, 1}); got != want {
		t.Errorf("after Add(\"\"): Entry = %+v, want %+v (FNV-1a 64 of a newline)", got, want)
	}

	var d4 Digest
	d4.Add("a")
	msg := panicValue(func() { d4.Add("a\nb") })
	if want := "dettest: Digest.Add: line contains a newline"; msg != want {
		t.Errorf("Add(a\\nb) panic = %q, want %q", msg, want)
	}
	if got, want := d4.Entry("e"), (Entry{"e", 0, 0x089bdc07b544e7b2, 1}); got != want {
		t.Errorf("after a rejected Add: Entry = %+v, want %+v", got, want)
	}
}

// Results lines in faultline v0.2.0's encoding: the fields and their order are those of its
// resultLine, as json.Marshal writes them.
const (
	resultRun   = `{"faultline_result":1,"package":"github.com/hmdsefi/tenure/simtest","test":"TestScenario","seed":"0x00000000000000ff","index":0,"status":"pass","events":12,"trace_hash":"0x00000000000000aa","wall_ns":1500}`
	resultSetup = `{"faultline_result":1,"package":"github.com/hmdsefi/tenure/simtest","test":"TestScenario","seed":"","index":-1,"status":"fail","kind":"setup","signature":"setup:","message":"faultline: no seeds","wall_ns":0}`
)

// TestATDET15Results checks DET-530 (AT-DET-15).
func TestATDET15Results(t *testing.T) {
	got, err := ParseResults([]byte(resultRun + "\n" + resultSetup + "\n"))
	if err != nil {
		t.Fatalf("ParseResults: %v", err)
	}
	want := []Result{
		{Test: "TestScenario", Seed: 0xff, Status: "pass", Events: 12, Hash: 0xaa},
		{Test: "TestScenario", Seed: 0, Status: "fail", Events: 0, Hash: 0},
	}
	if !slices.Equal(got, want) {
		t.Errorf("ParseResults = %+v, want %+v", got, want)
	}

	for _, tc := range []struct {
		name, line, want string
	}{
		{"no trace_hash", strings.Replace(resultRun, `,"trace_hash":"0x00000000000000aa"`, "", 1),
			"dettest: results line 1: missing trace_hash"},
		{"version 2", strings.Replace(resultRun, `"faultline_result":1`, `"faultline_result":2`, 1),
			"dettest: results line 1: faultline_result 2, want 1"},
		{"bad trace_hash", strings.Replace(resultRun, `"trace_hash":"0x00000000000000aa"`, `"trace_hash":"zz"`, 1),
			`dettest: results line 1: invalid trace_hash "zz"`},
	} {
		if _, err := ParseResults([]byte(tc.line + "\n")); err == nil || err.Error() != tc.want {
			t.Errorf("%s: ParseResults error = %v, want %q", tc.name, err, tc.want)
		}
	}
	const prefix = "dettest: results line 1: "
	if _, err := ParseResults([]byte("{\n")); err == nil || !strings.HasPrefix(err.Error(), prefix) || err.Error() == prefix {
		t.Errorf("ParseResults({) error = %v, want %q followed by the JSON error", err, prefix)
	}
}

// TestParseResultsLines checks DET-530's other cases: line numbers count every line, blank lines
// are skipped, an empty seed is missing on a pass line, and fields missing from a skip line are 0.
func TestParseResultsLines(t *testing.T) {
	skip := `{"faultline_result":1,"package":"p","test":"TestSkip","seed":"0x0000000000000003","index":0,"status":"skip","wall_ns":7}`
	got, err := ParseResults([]byte("\n" + resultRun + "\n\n" + skip))
	if err != nil {
		t.Fatalf("ParseResults: %v", err)
	}
	want := []Result{
		{Test: "TestScenario", Seed: 0xff, Status: "pass", Events: 12, Hash: 0xaa},
		{Test: "TestSkip", Seed: 3, Status: "skip"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("ParseResults = %+v, want %+v", got, want)
	}
	if got, err := ParseResults(nil); err != nil || len(got) != 0 {
		t.Errorf("ParseResults(nil) = %v, %v; want no results and no error", got, err)
	}

	for _, tc := range []struct {
		name, data, want string
	}{
		{"empty seed", "\n\n" + strings.Replace(resultRun, `"seed":"0x00000000000000ff"`, `"seed":""`, 1),
			"dettest: results line 3: missing seed"},
		{"no seed", strings.Replace(resultRun, `"seed":"0x00000000000000ff",`, "", 1),
			"dettest: results line 1: missing seed"},
		{"no events", resultSetup + "\n" + strings.Replace(resultRun, `"events":12,`, "", 1),
			"dettest: results line 2: missing events"},
		{"bad seed", strings.Replace(resultSetup, `"seed":""`, `"seed":"0xff"`, 1),
			`dettest: results line 1: invalid seed "0xff"`},
	} {
		if _, err := ParseResults([]byte(tc.data)); err == nil || err.Error() != tc.want {
			t.Errorf("%s: ParseResults error = %v, want %q", tc.name, err, tc.want)
		}
	}
}
