// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"testing"
)

// treeTQ is the acceptance tests' tq tree.
var treeTQ = Tree{Name: "tq", Read: 8, Write: 8, Degree: 3}

// treePanicText runs f and returns the text of the error it panics with. It fails the
// test if f does not panic or panics with a value that is not an error.
func treePanicText(t *testing.T, f func()) (text string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("did not panic")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value = %#v, want an error", r)
		}
		text = err.Error()
	}()
	f()
	return ""
}

func TestATPTR01Validate(t *testing.T) {
	tests := []struct {
		name string
		tree Tree
		want string // empty: valid
	}{
		{"tq", treeTQ, ""},
		{"unpartitioned", Tree{Name: "tq", Read: 1, Write: 1, Degree: 1}, ""},
		{"zero value", Tree{}, "core: invalid tree: empty Name"},
		{"suffix 3", Tree{Name: "orders/3", Read: 8, Write: 8, Degree: 3}, `core: invalid tree: Name "orders/3" ends in partition suffix "/3"`},
		{"suffix 1 alone", Tree{Name: "/1", Read: 8, Write: 8, Degree: 3}, `core: invalid tree: Name "/1" ends in partition suffix "/1"`},
		{"suffix 10", Tree{Name: "a/10", Read: 8, Write: 8, Degree: 3}, `core: invalid tree: Name "a/10" ends in partition suffix "/10"`},
		{"suffix after slashes", Tree{Name: "a/b/42", Read: 8, Write: 8, Degree: 3}, `core: invalid tree: Name "a/b/42" ends in partition suffix "/42"`},
		{"suffix with long digits", Tree{Name: "a/9223372036854775808123", Read: 8, Write: 8, Degree: 3}, `core: invalid tree: Name "a/9223372036854775808123" ends in partition suffix "/9223372036854775808123"`},
		{"suffix 0", Tree{Name: "orders/0", Read: 1, Write: 1, Degree: 1}, ""},
		{"suffix 007", Tree{Name: "orders/007", Read: 1, Write: 1, Degree: 1}, ""},
		{"suffix text", Tree{Name: "orders/eu", Read: 1, Write: 1, Degree: 1}, ""},
		{"trailing slash", Tree{Name: "orders/", Read: 1, Write: 1, Degree: 1}, ""},
		{"only slash", Tree{Name: "/", Read: 1, Write: 1, Degree: 1}, ""},
		{"nested", Tree{Name: "a/b/c", Read: 1, Write: 1, Degree: 1}, ""},
		{"zero byte", Tree{Name: "a\x00b", Read: 1, Write: 1, Degree: 1}, ""},
		{"digits without slash", Tree{Name: "3", Read: 1, Write: 1, Degree: 1}, ""},
		{"digit then text", Tree{Name: "a/1x", Read: 1, Write: 1, Degree: 1}, ""},
		{"slash in the middle", Tree{Name: "a/1/b", Read: 1, Write: 1, Degree: 1}, ""},
		{"write zero", Tree{Name: "tq", Read: 8, Write: 0, Degree: 3}, `core: invalid tree "tq": Write 0 is below 1`},
		{"write negative", Tree{Name: "tq", Read: 8, Write: -2, Degree: 3}, `core: invalid tree "tq": Write -2 is below 1`},
		{"read below write", Tree{Name: "tq", Read: 2, Write: 4}, `core: invalid tree "tq": Read 2 is below Write 4`},
		{"degree zero", Tree{Name: "tq", Read: 8, Write: 8, Degree: 0}, `core: invalid tree "tq": Degree 0 is below 1`},
		{"degree negative", Tree{Name: "tq", Read: 8, Write: 8, Degree: -1}, `core: invalid tree "tq": Degree -1 is below 1`},
		{"huge counts", Tree{Name: "tq", Read: math.MaxInt, Write: math.MaxInt, Degree: math.MaxInt}, ""},
		// Check order.
		{"name before counts", Tree{Name: "x/1", Read: 0, Write: 0, Degree: 0}, `core: invalid tree: Name "x/1" ends in partition suffix "/1"`},
		{"empty name before counts", Tree{Read: 0, Write: 0, Degree: 0}, "core: invalid tree: empty Name"},
		{"write before read", Tree{Name: "tq", Read: -1, Write: 0, Degree: 0}, `core: invalid tree "tq": Write 0 is below 1`},
		{"read before degree", Tree{Name: "tq", Read: 1, Write: 2, Degree: 0}, `core: invalid tree "tq": Read 1 is below Write 2`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.tree.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Fatalf("Validate() = %q, want %q", err.Error(), tc.want)
			}
			if !errors.Is(err, ErrInvalidTree) {
				t.Fatalf("Validate() = %v, does not wrap ErrInvalidTree", err)
			}
		})
	}
}

func TestATPTR02Keys(t *testing.T) {
	keys := []struct {
		tree Tree
		i    int
		want string
	}{
		{treeTQ, 0, "tq"},
		{treeTQ, 1, "tq/1"},
		{treeTQ, 12, "tq/12"},
		{Tree{Name: "tq", Read: 4, Write: 4, Degree: 3}, 9, "tq/9"},
		{Tree{Name: "a"}, 2, "a/2"}, // invalid tree, no panic
		{treeTQ, 4095, "tq/4095"},
		{treeTQ, 8, "tq/8"}, // not checked against Read
		{treeTQ, math.MaxInt, "tq/" + strconv.Itoa(math.MaxInt)},
		{Tree{}, 0, ""},
		{Tree{Name: "a/b"}, 100, "a/b/100"},
	}
	for _, k := range keys {
		if got := k.tree.Key(k.i); got != k.want {
			t.Errorf("%+v.Key(%d) = %q, want %q", k.tree, k.i, got, k.want)
		}
	}
	if got := treePanicText(t, func() { treeTQ.Key(-1) }); got != "core: Tree.Key: partition -1 is negative" {
		t.Errorf("Key(-1) panic = %q", got)
	}
	if got := treePanicText(t, func() { treeTQ.Key(math.MinInt) }); got != "core: Tree.Key: partition "+strconv.Itoa(math.MinInt)+" is negative" {
		t.Errorf("Key(MinInt) panic = %q", got)
	}

	if !treeTQ.IsRoot(0) {
		t.Error("IsRoot(0) = false")
	}
	if treeTQ.IsRoot(1) {
		t.Error("IsRoot(1) = true")
	}
	if got := treePanicText(t, func() { treeTQ.IsRoot(-1) }); got != "core: Tree.IsRoot: partition -1 is negative" {
		t.Errorf("IsRoot(-1) panic = %q", got)
	}
}

func TestATPTR03Injectivity(t *testing.T) {
	names := []string{"a", "a/0", "a/01", "a/x", "a/", "b", "/", "a/b/c"}
	type origin struct {
		name string
		i    int
	}
	seen := make(map[string]origin)
	for _, name := range names {
		tree := Tree{Name: name, Read: 1, Write: 1, Degree: 1}
		if err := tree.Validate(); err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		for i := 0; i <= 200; i++ {
			key := tree.Key(i)
			if prev, dup := seen[key]; dup {
				t.Fatalf("key %q of (%q, %d) is also the key of (%q, %d)", key, name, i, prev.name, prev.i)
			}
			seen[key] = origin{name, i}
		}
	}
	if len(seen) != 1608 {
		t.Fatalf("distinct keys = %d, want 1608", len(seen))
	}
}

func TestATPTR04Topology(t *testing.T) {
	t.Run("tq parents", func(t *testing.T) {
		want := []int{0, 0, 0, 1, 1, 1, 2}
		for i, w := range want {
			if got := treeTQ.Parent(i + 1); got != w {
				t.Errorf("Parent(%d) = %d, want %d", i+1, got, w)
			}
		}
	})

	tests := []struct {
		name string
		tree Tree
		i    int
		want []int
	}{
		{"tq 0", treeTQ, 0, []int{1, 2, 3}},
		{"tq 1", treeTQ, 1, []int{4, 5, 6}},
		{"tq 2", treeTQ, 2, []int{7}},
		{"tq 3", treeTQ, 3, nil},
		{"tq 7", treeTQ, 7, nil},
		{"tq 100", treeTQ, 100, nil},
		{"degree 1 root", Tree{Name: "tq", Read: 4, Degree: 1}, 0, []int{1}},
		{"degree 1 two", Tree{Name: "tq", Read: 4, Degree: 1}, 2, []int{3}},
		{"degree 1 leaf", Tree{Name: "tq", Read: 4, Degree: 1}, 3, nil},
		{"degree 2 root", Tree{Name: "tq", Read: 6, Degree: 2}, 0, []int{1, 2}},
		{"degree 2 one", Tree{Name: "tq", Read: 6, Degree: 2}, 1, []int{3, 4}},
		{"degree 2 two", Tree{Name: "tq", Read: 6, Degree: 2}, 2, []int{5}},
		{"read 1 degree 1", Tree{Name: "tq", Read: 1, Degree: 1}, 0, nil},
		{"read 0", Tree{Name: "tq", Read: 0, Degree: 2}, 0, nil},
		{"read negative", Tree{Name: "tq", Read: -5, Degree: 2}, 0, nil},
		{"read 2 degree 20", Tree{Name: "tq", Read: 2, Degree: 20}, 0, []int{1}},
		{"last child at read", Tree{Name: "tq", Read: 7, Degree: 3}, 1, []int{4, 5, 6}},
		{"short last range", Tree{Name: "tq", Read: 6, Degree: 3}, 1, []int{4, 5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.tree.Children(tc.i)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("Children(%d) = %v, want %v", tc.i, got, tc.want)
			}
			if tc.want == nil && got != nil {
				t.Fatalf("Children(%d) = %#v, want nil", tc.i, got)
			}
		})
	}

	t.Run("degree 1 parent", func(t *testing.T) {
		tree := Tree{Name: "tq", Read: 4, Degree: 1}
		for i := 1; i < 100; i++ {
			if got := tree.Parent(i); got != i-1 {
				t.Errorf("Parent(%d) = %d, want %d", i, got, i-1)
			}
		}
	})

	t.Run("panics", func(t *testing.T) {
		panics := []struct {
			name string
			f    func()
			want string
		}{
			{"parent root", func() { treeTQ.Parent(0) }, "core: Tree.Parent: partition 0 has no parent"},
			{"parent negative", func() { treeTQ.Parent(-1) }, "core: Tree.Parent: partition -1 has no parent"},
			{"parent degree", func() { Tree{Name: "tq", Degree: 0}.Parent(1) }, "core: Tree.Parent: Degree 0 is below 1"},
			{"parent degree negative", func() { Tree{Name: "tq", Degree: -3}.Parent(1) }, "core: Tree.Parent: Degree -3 is below 1"},
			{"parent partition first", func() { Tree{Degree: 0}.Parent(0) }, "core: Tree.Parent: partition 0 has no parent"},
			{"children negative", func() { treeTQ.Children(-1) }, "core: Tree.Children: partition -1 is negative"},
			{"children degree", func() { Tree{Name: "tq", Read: 8, Degree: 0}.Children(0) }, "core: Tree.Children: Degree 0 is below 1"},
			// AT-PTR-04: {Read: 1, Degree: 1}.Children(0) is nil (a case above), but with
			// Degree 0 PTR-070 makes Children panic, before the Read <= 1 shortcut.
			{"children read 1 degree 0", func() { Tree{Name: "tq", Read: 1}.Children(0) }, "core: Tree.Children: Degree 0 is below 1"},
			{"children partition first", func() { Tree{Read: 8, Degree: 0}.Children(-1) }, "core: Tree.Children: partition -1 is negative"},
		}
		for _, tc := range panics {
			t.Run(tc.name, func(t *testing.T) {
				if got := treePanicText(t, tc.f); got != tc.want {
					t.Fatalf("panic = %q, want %q", got, tc.want)
				}
			})
		}
	})

	t.Run("overflow", func(t *testing.T) {
		big := Tree{Name: "t", Read: math.MaxInt, Write: 1, Degree: 2}
		if got := big.Children(math.MaxInt / 2); got != nil {
			t.Errorf("Children(MaxInt/2) = %v, want nil", got)
		}
		want := []int{math.MaxInt - 2, math.MaxInt - 1}
		if got := big.Children(math.MaxInt/2 - 1); !slices.Equal(got, want) {
			t.Errorf("Children(MaxInt/2-1) = %v, want %v", got, want)
		}
		if got := big.Children(math.MaxInt); got != nil {
			t.Errorf("Children(MaxInt) = %v, want nil", got)
		}
		wide := Tree{Name: "t", Read: math.MaxInt, Write: 1, Degree: math.MaxInt}
		if got := wide.Children(1); got != nil {
			t.Errorf("Children(1) with Degree MaxInt = %v, want nil", got)
		}
		// The first child is 1 and the last is capped at Read-1: no sum overflows.
		wide.Read = 5
		if got, want := wide.Children(0), []int{1, 2, 3, 4}; !slices.Equal(got, want) {
			t.Errorf("Children(0) with Degree MaxInt = %v, want %v", got, want)
		}
		if got := wide.Parent(math.MaxInt); got != 0 {
			t.Errorf("Parent(MaxInt) with Degree MaxInt = %d, want 0", got)
		}
		if got := big.Parent(math.MaxInt); got != (math.MaxInt-1)/2 {
			t.Errorf("Parent(MaxInt) = %d, want %d", got, (math.MaxInt-1)/2)
		}
	})
}

// TestATPTR05TemporalParent checks Parent against the expression Temporal's matching
// service uses: (*NormalPartition).ParentPartition in common/tqid/task_queue_id.go of
// temporalio/temporal at commit c8b6d18f0231ff18e431ec75f5efe1dc64e2b413 computes
// (i + degree - 1) / degree - 1.
func TestATPTR05TemporalParent(t *testing.T) {
	for d := 1; d <= 64; d++ {
		tree := Tree{Name: "t", Read: 1, Write: 1, Degree: d}
		for i := 1; i <= 100000; i++ {
			if got, want := tree.Parent(i), (i+d-1)/d-1; got != want {
				t.Fatalf("Degree %d: Parent(%d) = %d, want %d", d, i, got, want)
			}
		}
	}
}

func TestATPTR06Convergence(t *testing.T) {
	for _, d := range []int{1, 2, 3, 20} {
		for p := 1; p <= 200; p++ {
			tree := Tree{Name: "tq", Read: p, Write: p, Degree: d}
			count := make(map[int]int)
			for j := range p {
				for _, c := range tree.Children(j) {
					count[c]++
				}
			}
			for i := 1; i < p; i++ {
				parent := tree.Parent(i)
				if parent < 0 || parent >= i {
					t.Fatalf("Degree %d: Parent(%d) = %d, want 0 <= parent < %d", d, i, parent, i)
				}
				if parent > p-2 {
					t.Fatalf("Degree %d, P %d: Parent(%d) = %d is outside 0..%d", d, p, i, parent, p-2)
				}

				// The step bound, with integer arithmetic only.
				bound := i
				if d >= 2 {
					h, pow := 0, d
					for pow <= i {
						h++
						if pow > i/d {
							break
						}
						pow *= d
					}
					bound = h + 1
				}
				steps := 0
				for at := i; at != 0; at = tree.Parent(at) {
					steps++
					if steps > bound {
						t.Fatalf("Degree %d: partition %d needs more than %d steps to reach 0", d, i, bound)
					}
				}

				if !slices.Contains(tree.Children(parent), i) || count[i] != 1 {
					t.Fatalf("Degree %d, P %d: partition %d is listed %d times by its parent's Children", d, p, i, count[i])
				}
			}
			if len(count) != p-1 {
				t.Fatalf("Degree %d, P %d: Children lists %d distinct partitions, want %d", d, p, len(count), p-1)
			}
		}
	}
}

func TestTreeChildrenMatchParent(t *testing.T) {
	// PTR-070: Children(i) is exactly the j in 1..Read-1 with Parent(j) == i, ascending.
	for _, d := range []int{1, 2, 3, 5, 20} {
		for _, read := range []int{1, 2, 3, 7, 8, 21, 22, 100} {
			tree := Tree{Name: "tq", Read: read, Write: 1, Degree: d}
			for i := 0; i < read+3; i++ {
				var want []int
				for j := 1; j < read; j++ {
					if tree.Parent(j) == i {
						want = append(want, j)
					}
				}
				if got := tree.Children(i); !slices.Equal(got, want) {
					t.Fatalf("Degree %d, Read %d: Children(%d) = %v, want %v", d, read, i, got, want)
				}
			}
		}
	}
}

func TestATPTR07Pick(t *testing.T) {
	tree := Tree{Name: "tq", Read: 8, Write: 4, Degree: 3}
	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"Pick(0)", tree.Pick(0), 0},
		{"Pick(5)", tree.Pick(5), 1},
		{"Pick(MaxUint64)", tree.Pick(math.MaxUint64), 3},
		{"PickRead(13)", tree.PickRead(13), 5},
		{"PickRead(MaxUint64)", tree.PickRead(math.MaxUint64), 7},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	var writes [4]int
	var reads [8]int
	for r := range uint64(8000) {
		writes[tree.Pick(r)]++
		reads[tree.PickRead(r)]++
	}
	for p, n := range writes {
		if n != 2000 {
			t.Errorf("write partition %d picked %d times, want 2000", p, n)
		}
	}
	for p, n := range reads {
		if n != 1000 {
			t.Errorf("read partition %d picked %d times, want 1000", p, n)
		}
	}

	if got, want := treePanicText(t, func() { Tree{Write: 0}.Pick(1) }), "core: Tree.Pick: Write 0 is below 1"; got != want {
		t.Errorf("Pick panic = %q, want %q", got, want)
	}
	if got, want := treePanicText(t, func() { Tree{Write: -3}.Pick(1) }), "core: Tree.Pick: Write -3 is below 1"; got != want {
		t.Errorf("Pick panic = %q, want %q", got, want)
	}
	if got, want := treePanicText(t, func() { Tree{Read: 0}.PickRead(1) }), "core: Tree.PickRead: Read 0 is below 1"; got != want {
		t.Errorf("PickRead panic = %q, want %q", got, want)
	}
	// Pick reads only Write, and PickRead only Read.
	if got := (Tree{Write: 2}).Pick(3); got != 1 {
		t.Errorf("Tree{Write: 2}.Pick(3) = %d, want 1", got)
	}
	if got := (Tree{Read: 2}).PickRead(3); got != 1 {
		t.Errorf("Tree{Read: 2}.PickRead(3) = %d, want 1", got)
	}
}

func TestATPTR08Unpartitioned(t *testing.T) {
	for _, degree := range []int{20, 1} {
		u := Tree{Name: "orders/eu", Read: 1, Write: 1, Degree: degree}
		if got := u.Key(0); got != "orders/eu" {
			t.Errorf("Degree %d: Key(0) = %q, want %q", degree, got, "orders/eu")
		}
		want := KeyShard("orders/eu", 64)
		for _, r := range []uint64{0, 1, 12345, math.MaxUint64} {
			if got := u.Pick(r); got != 0 {
				t.Errorf("Degree %d: Pick(%d) = %d, want 0", degree, r, got)
			}
			if got := u.PickRead(r); got != 0 {
				t.Errorf("Degree %d: PickRead(%d) = %d, want 0", degree, r, got)
			}
			if got := KeyShard(u.Key(u.Pick(r)), 64); got != want {
				t.Errorf("Degree %d: KeyShard of the picked key = %d, want %d", degree, got, want)
			}
		}
		if !u.IsRoot(0) {
			t.Errorf("Degree %d: IsRoot(0) = false, want true", degree)
		}
		if got := u.Children(0); got != nil {
			t.Errorf("Degree %d: Children(0) = %v, want nil", degree, got)
		}
	}
}

func TestATPTR09ValidateChange(t *testing.T) {
	tq := treeTQ
	tests := []struct {
		name string
		from Tree
		to   Tree
		want string // empty: valid
	}{
		{"lower Write", tq, Tree{Name: "tq", Read: 8, Write: 4, Degree: 3}, ""},
		{"then lower Read", Tree{Name: "tq", Read: 8, Write: 4, Degree: 3}, Tree{Name: "tq", Read: 4, Write: 4, Degree: 3}, ""},
		{"lower both at once", tq, Tree{Name: "tq", Read: 4, Write: 4, Degree: 3}, `core: invalid tree change "tq": Read 4 is below the previous Write 8`},
		{"Degree change", tq, Tree{Name: "tq", Read: 8, Write: 8, Degree: 5}, ""},
		{"name change", tq, Tree{Name: "other", Read: 8, Write: 8, Degree: 3}, `core: invalid tree change: Name "tq" to "other"`},
		{"next invalid", tq, Tree{Name: "tq", Read: 8, Write: 9, Degree: 3}, `core: invalid tree "tq": Read 8 is below Write 9`},
		{"receiver invalid", Tree{Name: "tq", Read: 0, Write: 0, Degree: 3}, tq, `core: invalid tree "tq": Write 0 is below 1`},
		{"both invalid, receiver first", Tree{Name: "tq", Read: 0, Write: 0, Degree: 3}, Tree{}, `core: invalid tree "tq": Write 0 is below 1`},
		{"invalid next beats a name change", tq, Tree{Name: "x/1", Read: 8, Write: 8, Degree: 3}, `core: invalid tree: Name "x/1" ends in partition suffix "/1"`},
		{"scale up, step one", Tree{Name: "tq", Read: 4, Write: 4, Degree: 3}, Tree{Name: "tq", Read: 8, Write: 4, Degree: 3}, ""},
		{"scale up, step two", Tree{Name: "tq", Read: 8, Write: 4, Degree: 3}, Tree{Name: "tq", Read: 8, Write: 8, Degree: 3}, ""},
		{"scale up in one step", Tree{Name: "tq", Read: 4, Write: 4, Degree: 3}, Tree{Name: "tq", Read: 8, Write: 8, Degree: 3}, ""},
		{"no change", tq, tq, ""},
		{"Read equals previous Write", Tree{Name: "tq", Read: 8, Write: 4, Degree: 3}, Tree{Name: "tq", Read: 4, Write: 2, Degree: 3}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.from.ValidateChange(tc.to)
			if tc.want == "" {
				if err != nil {
					t.Errorf("ValidateChange = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateChange = nil, want %q", tc.want)
			}
			if err.Error() != tc.want {
				t.Errorf("ValidateChange = %q, want %q", err.Error(), tc.want)
			}
			if !errors.Is(err, ErrInvalidTree) {
				t.Errorf("ValidateChange error %q does not wrap ErrInvalidTree", err)
			}
		})
	}
}

func TestATPTR10ScaleDown(t *testing.T) {
	s0 := Tree{Name: "tq", Read: 8, Write: 8, Degree: 2}
	s1 := Tree{Name: "tq", Read: 8, Write: 3, Degree: 2}
	s2 := Tree{Name: "tq", Read: 3, Write: 3, Degree: 2}
	if err := s0.ValidateChange(s1); err != nil {
		t.Errorf("s0 to s1: %v, want nil", err)
	}
	if err := s1.ValidateChange(s2); err != nil {
		t.Errorf("s1 to s2: %v, want nil", err)
	}
	if err := s0.ValidateChange(s2); !errors.Is(err, ErrInvalidTree) {
		t.Errorf("s0 to s2: %v, want an error wrapping ErrInvalidTree", err)
	}

	// Under s1 writers stay in 0..2, readers poll 0..7, and every draining partition
	// forwards into 0..2.
	polled := make(map[int]bool)
	for r := range uint64(1000) {
		if p := s1.Pick(r); p < 0 || p > 2 {
			t.Fatalf("s1.Pick(%d) = %d, want 0..2", r, p)
		}
		polled[s1.PickRead(r)] = true
	}
	for p := range 8 {
		if !polled[p] {
			t.Errorf("s1.PickRead never returned %d", p)
		}
	}
	for p := 3; p <= 7; p++ {
		at := p
		for at > 2 {
			at = s1.Parent(at)
		}
		if at < 0 || at > 2 {
			t.Errorf("partition %d does not reach 0..2", p)
		}
	}
	if got := s1.Parent(7); got != 3 || s1.Parent(3) != 1 {
		t.Errorf("chain from 7 = 7, %d, %d, want 7, 3, 1", got, s1.Parent(3))
	}

	if got := s2.Children(1); got != nil {
		t.Errorf("s2.Children(1) = %v, want nil", got)
	}
	if got, want := s2.Children(0), []int{1, 2}; !slices.Equal(got, want) {
		t.Errorf("s2.Children(0) = %v, want %v", got, want)
	}
}
