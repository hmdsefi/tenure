// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// goldenMagic is line 1 of every golden file.
const goldenMagic = "# tenure golden v1"

// goldenColumns is line 2 of the files FormatGolden writes.
const goldenColumns = "# name seed hash count"

// Entry is one golden entry.
type Entry struct {
	Name  string
	Seed  uint64
	Hash  uint64
	Count uint64
}

// ParseGolden parses a golden file. file names it in error messages.
func ParseGolden(file string, data []byte) ([]Entry, error) {
	lines := strings.Split(string(data), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1] // the final newline ends the last line
	}
	if len(lines) == 0 || lines[0] != goldenMagic {
		return nil, fmt.Errorf("%s: missing header %q", file, goldenMagic)
	}
	type key struct {
		name string
		seed uint64
	}
	var entries []Entry
	seen := make(map[key]bool)
	for i, line := range lines[1:] {
		n := i + 2 // 1-based line number
		if strings.HasPrefix(line, "#") {
			continue
		}
		e, ok := parseEntry(line)
		if !ok {
			return nil, fmt.Errorf("%s:%d: malformed entry", file, n)
		}
		k := key{e.Name, e.Seed}
		if seen[k] {
			return nil, fmt.Errorf("%s:%d: duplicate entry %s 0x%016x", file, n, e.Name, e.Seed)
		}
		seen[k] = true
		if len(entries) > 0 && compareEntries(entries[len(entries)-1], e) > 0 {
			return nil, fmt.Errorf("%s:%d: entries out of order", file, n)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// parseEntry parses one entry line in exactly the form FormatGolden writes.
func parseEntry(line string) (Entry, bool) {
	fields := strings.Split(line, " ")
	if len(fields) != 4 || !validGoldenName(fields[0]) {
		return Entry{}, false
	}
	seed, ok1 := parseHex64(fields[1])
	hash, ok2 := parseHex64(fields[2])
	count, ok3 := parseDecimal(fields[3])
	if !ok1 || !ok2 || !ok3 {
		return Entry{}, false
	}
	return Entry{Name: fields[0], Seed: seed, Hash: hash, Count: count}, true
}

// parseHex64 parses "0x" followed by exactly 16 lowercase hexadecimal digits.
func parseHex64(s string) (uint64, bool) {
	digits, ok := strings.CutPrefix(s, "0x")
	if !ok || len(digits) != 16 {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if c := digits[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(digits, 16, 64)
	return v, err == nil
}

// parseDecimal parses a uint64 written as strconv.FormatUint(v, 10) writes it.
func parseDecimal(s string) (uint64, bool) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(v, 10) != s {
		return 0, false
	}
	return v, true
}

// validGoldenName reports whether name matches ^[a-z0-9][a-z0-9._/-]*$.
func validGoldenName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case i > 0 && (c == '.' || c == '_' || c == '/' || c == '-'):
		default:
			return false
		}
	}
	return true
}

// compareEntries orders entries by (Name, Seed).
func compareEntries(a, b Entry) int {
	if c := strings.Compare(a.Name, b.Name); c != 0 {
		return c
	}
	return cmp.Compare(a.Seed, b.Seed)
}

// sortedEntries returns a copy of entries sorted by (Name, Seed), keeping the input order of
// entries that share a name and seed.
func sortedEntries(entries []Entry) []Entry {
	s := slices.Clone(entries)
	slices.SortStableFunc(s, compareEntries)
	return s
}

// FormatGolden returns the golden file holding entries, sorted by (Name, Seed). It panics
// if a name is invalid.
func FormatGolden(entries []Entry) []byte {
	b := make([]byte, 0, len(goldenMagic)+len(goldenColumns)+2+len(entries)*64)
	b = append(b, goldenMagic+"\n"+goldenColumns+"\n"...)
	sorted := sortedEntries(entries)
	for i, e := range sorted {
		if !validGoldenName(e.Name) {
			panic(fmt.Sprintf("dettest: invalid golden name %q", e.Name))
		}
		if i > 0 && compareEntries(sorted[i-1], e) == 0 {
			panic(fmt.Sprintf("dettest: duplicate golden entry %s 0x%016x", e.Name, e.Seed))
		}
		b = fmt.Appendf(b, "%s 0x%016x 0x%016x %d\n", e.Name, e.Seed, e.Hash, e.Count)
	}
	return b
}

// CompareGolden returns one report line per difference between got and want, sorted by
// (Name, Seed), or nil when they hold the same entries.
func CompareGolden(got, want []Entry) []string {
	g, w := sortedEntries(got), sortedEntries(want)
	var lines []string
	for len(g) > 0 || len(w) > 0 {
		var c int
		switch {
		case len(g) == 0:
			c = 1
		case len(w) == 0:
			c = -1
		default:
			c = compareEntries(g[0], w[0])
		}
		switch {
		case c < 0:
			lines = append(lines, fmt.Sprintf("golden: %s seed 0x%016x: missing from the golden file", g[0].Name, g[0].Seed))
			g = g[1:]
		case c > 0:
			lines = append(lines, fmt.Sprintf("golden: %s seed 0x%016x: in the golden file but not produced", w[0].Name, w[0].Seed))
			w = w[1:]
		default:
			if g[0] != w[0] {
				lines = append(lines, fmt.Sprintf("golden: %s seed 0x%016x: got hash 0x%016x count %d, want hash 0x%016x count %d",
					g[0].Name, g[0].Seed, g[0].Hash, g[0].Count, w[0].Hash, w[0].Count))
			}
			g, w = g[1:], w[1:]
		}
	}
	return lines
}

// FNV-1a 64 parameters, as hash/fnv's New64a uses them.
const (
	fnvOffset64 = 0xcbf29ce484222325
	fnvPrime64  = 0x100000001b3
)

// Digest accumulates the text lines of one golden case. The zero Digest is empty and ready
// to use.
type Digest struct {
	hash  uint64 // FNV-1a 64 state after count lines; unused while count is 0
	count uint64
}

// sum returns the current FNV-1a 64 state: the offset basis before the first line.
func (d *Digest) sum() uint64 {
	if d.count == 0 {
		return fnvOffset64
	}
	return d.hash
}

// Add appends line, followed by a newline, to the digest. It panics if line contains a
// newline.
func (d *Digest) Add(line string) {
	if strings.Contains(line, "\n") {
		panic("dettest: Digest.Add: line contains a newline")
	}
	h := d.sum()
	for i := 0; i < len(line); i++ {
		h ^= uint64(line[i])
		h *= fnvPrime64
	}
	h ^= '\n'
	h *= fnvPrime64
	d.hash = h
	d.count++
}

// Entry returns the golden entry of the digest under name, with Seed 0.
func (d *Digest) Entry(name string) Entry {
	return Entry{Name: name, Seed: 0, Hash: d.sum(), Count: d.count}
}
