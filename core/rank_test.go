// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
)

// permutations returns every ordering of ms, each in a new slice.
func permutations(ms []Member) [][]Member {
	if len(ms) <= 1 {
		return [][]Member{slices.Clone(ms)}
	}
	var out [][]Member
	for i := range ms {
		rest := append(slices.Clone(ms[:i]), ms[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]Member{ms[i]}, p...))
		}
	}
	return out
}

// testKeys returns k0 … k(n-1).
func testKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	return keys
}

func TestATCOR11Ordering(t *testing.T) {
	perms := permutations(members("n1", "n2", "n3", "n4"))
	if len(perms) != 24 {
		t.Fatalf("%d permutations, want 24", len(perms))
	}
	counter := []struct {
		key  string
		want []string
	}{
		{"k0", []string{"n4", "n1", "n2", "n3"}},
		{"k3", []string{"n1", "n3", "n4", "n2"}},
		{"k8", []string{"n4", "n3", "n1", "n2"}},
	}
	for _, tt := range counter {
		for _, ms := range perms {
			if got := ids(Rank("counter", tt.key, ms, 4)); !slices.Equal(got, tt.want) {
				t.Errorf("Rank(%q, %q, %v, 4) = %v, want %v", "counter", tt.key, ids(ms), got, tt.want)
			}
		}
	}

	for _, key := range []string{"alice", "bob"} {
		want := []string{"n3", "n2"}
		if got := ids(Rank("account", key, members("n1", "n2", "n3"), 2)); !slices.Equal(got, want) {
			t.Errorf("Rank(%q, %q, [n1 n2 n3], 2) = %v, want %v", "account", key, got, want)
		}
	}
}

func TestATCOR12Prefixes(t *testing.T) {
	ms := members("n1", "n2", "n3", "n4")
	if got := Rank("account", "k1", ms, 0); got != nil {
		t.Errorf("Rank(n = 0) = %v, want nil", ids(got))
	}
	if got := Rank("account", "k1", ms, -1); got != nil {
		t.Errorf("Rank(n = -1) = %v, want nil", ids(got))
	}
	if got := Rank("account", "k1", ms, 10); len(got) != 4 {
		t.Errorf("Rank(n = 10) has %d members, want 4", len(got))
	}
	if got := Rank("x", "y", nil, 3); got != nil {
		t.Errorf("Rank(nil members) = %v, want nil", ids(got))
	}
	if got := Rank("x", "y", []Member{}, 3); got != nil {
		t.Errorf("Rank(empty members) = %v, want nil", ids(got))
	}

	for _, key := range testKeys(100) {
		for b := 0; b <= 5; b++ {
			full := Rank("account", key, ms, b)
			for a := 0; a <= b; a++ {
				got := Rank("account", key, ms, a)
				want := full[:min(a, len(full))]
				if !slices.Equal(ids(got), ids(want)) {
					t.Errorf("key %s: Rank(n = %d) = %v, want the prefix %v of Rank(n = %d)", key, a, ids(got), ids(want), b)
				}
			}
		}
	}
}

// filterNothingMembers returns AT-COR-13's members.
func filterNothingMembers() []Member {
	return []Member{
		{ID: "n1", Draining: true},
		{ID: "n2", Slot: 0, LeaveAt: t0},
		{ID: "n3", Addr: "x", Meta: map[string]string{"role": "history"}},
		{ID: "bad/id"},
	}
}

func TestATCOR13FiltersNothing(t *testing.T) {
	ms := filterNothingMembers()
	before := deepCopyMembers(ms)

	got := Rank("account", "alice", ms, 4)
	if len(got) != 4 {
		t.Fatalf("Rank() has %d members, want 4", len(got))
	}
	want := []string{"n1", "n2", "n3", "bad/id"}
	slices.SortFunc(want, func(a, b string) int {
		sa, sb := Score("account", "alice", a), Score("account", "alice", b)
		switch {
		case sa > sb:
			return -1
		case sa < sb:
			return 1
		}
		return 0
	})
	if !slices.Equal(ids(got), want) {
		t.Errorf("Rank() IDs = %v, want %v (by Score descending)", ids(got), want)
	}
	byID := map[string]Member{}
	for _, m := range ms {
		byID[m.ID] = m
	}
	for i, m := range got {
		in := byID[m.ID]
		if m.Addr != in.Addr || m.Draining != in.Draining || m.Slot != in.Slot || !m.JoinAt.Equal(in.JoinAt) || !m.LeaveAt.Equal(in.LeaveAt) {
			t.Errorf("result[%d] = %+v, want the input's fields %+v", i, m, in)
		}
		if m.ID == "n3" {
			if m.Addr != "x" {
				t.Errorf("n3 Addr = %q, want %q", m.Addr, "x")
			}
			if reflect.ValueOf(m.Meta).Pointer() != reflect.ValueOf(in.Meta).Pointer() {
				t.Error("n3 Meta is not the input's map")
			}
		}
	}
	if !reflect.DeepEqual(before, ms) {
		t.Errorf("Rank changed its input:\nbefore %+v\nafter  %+v", before, ms)
	}

	changed := filterNothingMembers()
	for i := range changed {
		changed[i].Draining = !changed[i].Draining
		changed[i].Slot = 5
		changed[i].Addr = "z"
		changed[i].JoinAt = t0
	}
	again := Rank("account", "alice", changed, 4)
	if !slices.Equal(ids(again), want) {
		t.Errorf("Rank() with other fields changed = %v, want %v", ids(again), want)
	}
	for i, m := range again {
		if m.Addr != "z" || m.Slot != 5 || !m.JoinAt.Equal(t0) {
			t.Errorf("result[%d] = %+v, want the changed input's fields", i, m)
		}
	}

	got[0].ID = "zz"
	if !reflect.DeepEqual(before, ms) {
		t.Error("changing the result changed the input")
	}
}

func TestATCOR14Ties(t *testing.T) {
	ms := []Member{{ID: "a", Addr: "1"}, {ID: "b"}, {ID: "a", Addr: "2"}}
	got := Rank("ns", "k", ms, 3)
	i := slices.IndexFunc(got, func(m Member) bool { return m.ID == "a" })
	if len(got) != 3 || i < 0 || i+1 >= len(got) || got[i+1].ID != "a" || got[i].Addr != "1" || got[i+1].Addr != "2" {
		t.Errorf("Rank() = %+v, want the two a entries adjacent, Addr 1 before 2", got)
	}

	constant := func(string) uint64 { return 7 }
	if got := ids(rankScored(members("c", "a", "b"), 3, constant)); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("rankScored([c a b], 3, constant) = %v, want [a b c]", got)
	}
	if got := ids(rankScored(members("c", "a", "b"), 1, constant)); !slices.Equal(got, []string{"a"}) {
		t.Errorf("rankScored([c a b], 1, constant) = %v, want [a]", got)
	}
	xs := []Member{{ID: "x", Addr: "1"}, {ID: "x", Addr: "2"}}
	if got := rankScored(xs, 2, constant); len(got) != 2 || got[0].Addr != "1" || got[1].Addr != "2" {
		t.Errorf("rankScored([x x], 2, constant) = %+v, want input order", got)
	}
	if got := rankScored(xs, 1, constant); len(got) != 1 || got[0].Addr != "1" {
		t.Errorf("rankScored([x x], 1, constant) = %+v, want the first x", got)
	}
}

// vacOwner is lockstep VAC-110 written directly: the node with the largest Score(kind, key,
// node), ties to the smaller name; "" when nodes is empty.
func vacOwner(kind, key string, nodes []string) string {
	best := ""
	var bestScore uint64
	for i, node := range nodes {
		s := Score(kind, key, node)
		if i == 0 || s > bestScore || s == bestScore && node < best {
			best, bestScore = node, s
		}
	}
	return best
}

// vacRunnerUp is lockstep VAC-120 written directly: the owner among the nodes other than
// vacOwner's.
func vacRunnerUp(kind, key string, nodes []string) string {
	owner := vacOwner(kind, key, nodes)
	return vacOwner(kind, key, slices.DeleteFunc(slices.Clone(nodes), func(n string) bool { return n == owner }))
}

func TestATCOR15LockstepCompat(t *testing.T) {
	sets := [][]string{{"n1", "n2", "n3"}, {"n1", "n2", "n3", "n4"}, {"n2", "n3"}}
	for _, kind := range []string{"account", "counter"} {
		for _, nodes := range sets {
			ms := members(nodes...)
			for _, key := range testKeys(10000) {
				want := []string{vacOwner(kind, key, nodes), vacRunnerUp(kind, key, nodes)}
				if got := ids(Rank(kind, key, ms, 2)); !slices.Equal(got, want) {
					t.Fatalf("Rank(%q, %q, %v, 2) = %v, want %v", kind, key, nodes, got, want)
				}
			}
		}
	}

	// lockstep AT-VAC-02: n1, n2, n3 up; then n3 filtered out by lockstep.
	for _, key := range []string{"alice", "bob"} {
		if got := Rank("account", key, members("n1", "n2", "n3"), 1); len(got) != 1 || got[0].ID != "n3" {
			t.Errorf("owner of %s over n1, n2, n3 = %v, want n3", key, ids(got))
		}
		if got := Rank("account", key, members("n1", "n2"), 1); len(got) != 1 || got[0].ID != "n2" {
			t.Errorf("owner of %s over n1, n2 = %v, want n2", key, ids(got))
		}
	}
}

// firstChoices returns Rank(ns, key, ms, 1)[0].ID for each key.
func firstChoices(ns string, keys []string, ms []Member) []string {
	out := make([]string, len(keys))
	for i, key := range keys {
		out[i] = Rank(ns, key, ms, 1)[0].ID
	}
	return out
}

func TestATCOR16Movement(t *testing.T) {
	keys := testKeys(10000)
	three := firstChoices("account", keys, members("n1", "n2", "n3"))
	counts := map[string]int{}
	for _, id := range three {
		counts[id]++
	}
	if want := map[string]int{"n1": 3329, "n2": 3332, "n3": 3339}; !reflect.DeepEqual(counts, want) {
		t.Errorf("first choices over n1, n2, n3 = %v, want %v", counts, want)
	}

	four := firstChoices("account", keys, members("n1", "n2", "n3", "n4"))
	moved := 0
	for i := range keys {
		if four[i] != three[i] {
			moved++
			if four[i] != "n4" {
				t.Errorf("adding n4 moved %s from %s to %s, want n4", keys[i], three[i], four[i])
			}
		}
	}
	if moved != 2476 {
		t.Errorf("adding n4 changed %d first choices, want 2476", moved)
	}

	two := firstChoices("account", keys, members("n1", "n2"))
	moved = 0
	for i := range keys {
		if two[i] != three[i] {
			moved++
			if three[i] != "n3" {
				t.Errorf("removing n3 moved %s from %s to %s, want only keys of n3", keys[i], three[i], two[i])
			}
		}
	}
	if moved != 3339 {
		t.Errorf("removing n3 changed %d first choices, want 3339", moved)
	}
}

func TestATCOR17Rendezvous(t *testing.T) {
	keys := testKeys(1000)
	perms := permutations(members("n1", "n2", "n3", "n4"))
	base := NewRendezvous(perms[0])
	for _, ms := range perms {
		p := NewRendezvous(ms)
		for _, key := range keys {
			for n := 0; n <= 5; n++ {
				got := p.Rank("account", key, n)
				if want := Rank("account", key, ms, n); !reflect.DeepEqual(got, want) {
					t.Fatalf("NewRendezvous(%v).Rank(%q, %d) = %v, want Rank's %v", ids(ms), key, n, ids(got), ids(want))
				}
				if want := base.Rank("account", key, n); !reflect.DeepEqual(got, want) {
					t.Fatalf("NewRendezvous(%v).Rank(%q, %d) = %v, want %v as for %v", ids(ms), key, n, ids(got), ids(want), ids(perms[0]))
				}
				for a := 0; a <= n; a++ {
					if prefix := p.Rank("account", key, a); !slices.Equal(ids(prefix), ids(got[:min(a, len(got))])) {
						t.Fatalf("Rank(%q, %d) = %v, want the prefix of Rank(n = %d) %v", key, a, ids(prefix), n, ids(got))
					}
				}
			}
		}
	}

	t.Run("later changes", func(t *testing.T) {
		ms := members("n1", "n2", "n3", "n4")
		p := NewRendezvous(ms)
		want := make([][]Member, len(keys))
		for i, key := range keys {
			want[i] = p.Rank("account", key, 4)
		}
		ms[0].ID = "zz"
		res := p.Rank("account", "k0", 4)
		res[0].ID = "yy"
		for i, key := range keys {
			if got := p.Rank("account", key, 4); !reflect.DeepEqual(got, want[i]) {
				t.Fatalf("Rank(%q) after changes = %v, want %v", key, ids(got), ids(want[i]))
			}
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		p := NewRendezvous(members("n1", "n2", "n3", "n4"))
		sequential := make([][]Member, len(keys))
		for i, key := range keys {
			sequential[i] = p.Rank("account", key, 1+i%4)
		}
		results := make([][][]Member, 8)
		var wg sync.WaitGroup
		for g := range results {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out := make([][]Member, len(keys))
				for i, key := range keys {
					out[i] = p.Rank("account", key, 1+i%4)
				}
				results[g] = out
			}()
		}
		wg.Wait()
		for g, out := range results {
			if !reflect.DeepEqual(out, sequential) {
				t.Errorf("goroutine %d got results that differ from a sequential run", g)
			}
		}
	})

	if got := NewRendezvous(nil).Rank("x", "y", 1); got != nil {
		t.Errorf("NewRendezvous(nil).Rank() = %v, want nil", ids(got))
	}
}

func TestATCOR18RendezvousDuplicates(t *testing.T) {
	func() {
		defer func() {
			r := recover()
			err, ok := r.(error)
			if !ok {
				t.Fatalf("NewRendezvous with a duplicate ID panicked with %#v, want an error", r)
			}
			if want := `core: NewRendezvous: duplicate member ID "b"`; err.Error() != want {
				t.Errorf("panic = %q, want %q", err, want)
			}
		}()
		NewRendezvous([]Member{{ID: "b"}, {ID: "a"}, {ID: "b"}})
	}()

	func() {
		defer func() {
			err, _ := recover().(error)
			if want := `core: NewRendezvous: duplicate member ID "a"`; err == nil || err.Error() != want {
				t.Errorf("panic = %v, want %q (the first duplicate in ID order)", err, want)
			}
		}()
		NewRendezvous(members("c", "c", "a", "b", "a"))
	}()

	p := NewRendezvous([]Member{{ID: "bad/id"}, {ID: ""}})
	got := ids(p.Rank("x", "y", 2))
	slices.Sort(got)
	if !slices.Equal(got, []string{"", "bad/id"}) {
		t.Errorf("NewRendezvous([bad/id, \"\"]).Rank(2) = %q, want both members", got)
	}
}

func TestATCOR19PurityRank(t *testing.T) {
	slotted := eligibleView()
	slotted.Members[0].Meta = map[string]string{"role": "x"}
	for i := range slotted.Members {
		slotted.Members[i].Slot = -1
	}
	slotted.Members[0].Slot = 0
	inputs := [][]Member{
		filterNothingMembers(),
		eligibleView().Members,
		slotted.Members,
		members("a", "b", "c"),
		members("b", "a"),
		members("x/y", "a"),
		{{ID: "a", Slot: 0}, {ID: "b", Slot: -1}},
		nil,
	}
	for i, ms := range inputs {
		before := deepCopyMembers(ms)
		_ = Rank("account", "alice", ms, 3)
		_ = Rank("account", "alice", ms, 1)
		p := NewRendezvous(ms)
		_ = p.Rank("account", "alice", 3)
		if !reflect.DeepEqual(before, ms) {
			t.Errorf("input %d changed:\nbefore %+v\nafter  %+v", i, before, ms)
		}
	}
}
