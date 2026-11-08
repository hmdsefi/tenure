// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of the resolver. Steps carrying an AT-RUN number were written from the issue's text.

// newResolver starts a standalone resolver over m, failing t on error.
func newResolver(t *testing.T, m tenure.Membership, opts ...tenure.ResolverOption) *tenure.Resolver {
	t.Helper()
	r, err := tenure.NewResolver(m, opts...)
	if err != nil {
		t.Fatalf("NewResolver() = %v, want nil", err)
	}
	return r
}

// closeResolver closes r, failing t unless Close returns nil.
func closeResolver(t *testing.T, r *tenure.Resolver) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatalf("Resolver.Close() = %v, want nil", err)
	}
}

// listIDs returns the IDs of members, in order.
func listIDs(members []tenure.Member) []string {
	out := make([]string, len(members))
	for i, m := range members {
		out[i] = m.ID
	}
	return out
}

// receiveList returns the list waiting in ch, failing t when none is.
func receiveList(t *testing.T, ch <-chan []tenure.Member) []tenure.Member {
	t.Helper()
	select {
	case l, ok := <-ch:
		if !ok {
			t.Fatal("Subscribe channel closed, want a list")
		}
		return l
	default:
		t.Fatal("Subscribe channel empty, want a list")
		return nil
	}
}

// noList fails t when a list is waiting in ch or ch is closed.
func noList(t *testing.T, ch <-chan []tenure.Member) {
	t.Helper()
	select {
	case l, ok := <-ch:
		t.Fatalf("Subscribe channel holds %v (open %t), want nothing", listIDs(l), ok)
	default:
	}
}

// closedList fails t unless ch is closed and empty.
func closedList(t *testing.T, ch <-chan []tenure.Member) {
	t.Helper()
	select {
	case l, ok := <-ch:
		if ok {
			t.Fatalf("Subscribe channel holds %v, want it closed", listIDs(l))
		}
	default:
		t.Fatal("Subscribe channel open, want it closed")
	}
}

// closedAfterLists fails t unless ch is closed once the lists waiting in it are received.
func closedAfterLists(t *testing.T, ch <-chan []tenure.Member) {
	t.Helper()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			t.Fatal("Subscribe channel open, want it closed")
		}
	}
}

// fiveMembers is a group of n1 to n5 in which n3 is draining: n1, n2, n4 and n5 are eligible.
func fiveMembers() []tenure.Member {
	return []tenure.Member{
		{ID: "n1", Addr: "a1", Meta: map[string]string{"zone": "z1"}},
		{ID: "n2", Addr: "a2"},
		{ID: "n3", Addr: "a3", Draining: true},
		{ID: "n4", Addr: "a4"},
		{ID: "n5", Addr: "a5"},
	}
}

// eligible returns the members of members with the given IDs, in that order.
func eligible(members []tenure.Member, want ...string) []tenure.Member {
	var out []tenure.Member
	for _, id := range want {
		i := slices.IndexFunc(members, func(m tenure.Member) bool { return m.ID == id })
		out = append(out, members[i])
	}
	return out
}

// TestATRUN80ResolverLookups checks a standalone resolver: it watches without joining, and its
// lookups rank the eligible members as core.Rank does.
func TestATRUN80ResolverLookups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t, fiveMembers()...)
		m := g.Absent("r1")
		r := newResolver(t, m, tenure.WithNamespace("q"))
		synctest.Wait()

		if got, want := callMethods(m.Calls()), []string{"Watch"}; !slices.Equal(got, want) {
			t.Errorf("membership calls = %v, want %v", got, want)
		}
		want := eligible(fiveMembers(), "n1", "n2", "n4", "n5")
		if got := r.Members(); !reflect.DeepEqual(got, want) {
			t.Errorf("Members() = %+v, want %+v", got, want)
		}
		for i := range 1000 {
			k := fmt.Sprintf("k%d", i)
			got, err := r.Lookup(k)
			if err != nil || !reflect.DeepEqual(got, core.Rank("q", k, want, 1)[0]) {
				t.Fatalf("Lookup(%q) = %+v, %v, want %+v, nil", k, got, err, core.Rank("q", k, want, 1)[0])
			}
			got3, err := r.LookupN(k, 3)
			if err != nil || !reflect.DeepEqual(got3, core.Rank("q", k, want, 3)) {
				t.Fatalf("LookupN(%q, 3) = %v, %v, want %v, nil", k, listIDs(got3), err, listIDs(core.Rank("q", k, want, 3)))
			}
			got10, err := r.LookupN(k, 10)
			if err != nil || len(got10) != 4 || !reflect.DeepEqual(got10, core.Rank("q", k, want, 10)) {
				t.Fatalf("LookupN(%q, 10) = %v, %v, want the 4 of %v, nil", k, listIDs(got10), err, listIDs(core.Rank("q", k, want, 10)))
			}
		}
		if got, want := listIDs(r.Members()), []string{"n1", "n2", "n4", "n5"}; !slices.Equal(got, want) {
			t.Errorf("Members() = %v, want %v", got, want)
		}

		closeResolver(t, r)
		if got, want := callMethods(m.Calls()), []string{"Watch"}; !slices.Equal(got, want) {
			t.Errorf("membership calls after Close = %v, want %v", got, want)
		}
		check()
	})
}

// TestATRUN81ResolverErrors checks the resolver's errors, its Watch retries, and that no Watch
// follows the first that returns ErrClosed.
func TestATRUN81ResolverErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		if _, err := tenure.NewResolver(nil); err == nil || err.Error() != "tenure: NewResolver: nil membership" {
			t.Errorf("NewResolver(nil) = %v, want tenure: NewResolver: nil membership", err)
		}
		g := newGroup(t, tenure.Member{ID: "n1", Addr: "a1", Draining: true})
		boom := errors.New("boom")
		failing := g.Absent("r0")
		failing.Fail("Watch", boom)
		if _, err := tenure.NewResolver(failing); err == nil || err.Error() != "tenure: NewResolver: watch: boom" || !errors.Is(err, boom) {
			t.Errorf("NewResolver(failing Watch) = %v, want tenure: NewResolver: watch: boom", err)
		}
		if got, want := callMethods(failing.Calls()), []string{"Watch"}; !slices.Equal(got, want) {
			t.Errorf("failing membership calls = %v, want %v", got, want)
		}

		m := g.Absent("r1")
		t0 := time.Now()
		r := newResolver(t, m)
		synctest.Wait()
		if _, err := r.Lookup("k"); !errors.Is(err, tenure.ErrNoMembers) {
			t.Errorf("Lookup with no eligible member = %v, want ErrNoMembers", err)
		}
		if _, err := r.LookupN("k", 1); !errors.Is(err, tenure.ErrNoMembers) {
			t.Errorf("LookupN(k, 1) with no eligible member = %v, want ErrNoMembers", err)
		}
		if got := r.Members(); got != nil {
			t.Errorf("Members() with no eligible member = %v, want nil", got)
		}
		for _, n := range []int{0, -1} {
			if _, err := r.LookupN("k", n); err == nil || err.Error() != "tenure: LookupN: n must be at least 1" {
				t.Errorf("LookupN(k, %d) = %v, want tenure: LookupN: n must be at least 1", n, err)
			}
		}

		at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }
		g.Join(tenure.Member{ID: "n2", Addr: "a2"})
		synctest.Wait()
		if got, err := r.Lookup("k"); err != nil || got.ID != "n2" {
			t.Errorf("Lookup(k) = %+v, %v, want n2", got, err)
		}
		at(10 * time.Second)
		m.Fail("Watch", boom)
		m.CloseWatch(0)
		at(15 * time.Second) // failed at T0+12s and T0+14s
		m.Fail("Watch", tenure.ErrClosed)
		at(30 * time.Second) // ErrClosed at T0+16s, then nothing
		watches := m.Calls("Watch")
		if got, want := startsAt(watches, t0), []time.Duration{0, 12 * time.Second, 14 * time.Second, 16 * time.Second}; !slices.Equal(got, want) {
			t.Fatalf("Watch calls at %v, want %v", got, want)
		}
		if err := watches[3].Err; !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("last Watch = %v, want ErrClosed", watches[3])
		}
		if got, err := r.Lookup("k"); err != nil || got.ID != "n2" {
			t.Errorf("Lookup(k) after ErrClosed = %+v, %v, want n2 from the last list", got, err)
		}

		closeResolver(t, r)
		if _, err := r.Lookup("k"); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Lookup after Close = %v, want ErrClosed", err)
		}
		if _, err := r.LookupN("k", 2); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("LookupN after Close = %v, want ErrClosed", err)
		}
		if got := r.Members(); got != nil {
			t.Errorf("Members() after Close = %v, want nil", got)
		}
		if err := r.Close(); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("second Close() = %v, want ErrClosed", err)
		}
		check()
	})
}

// TestATRUN82Subscribe checks Subscribe: the current list at once, a list per change of the
// eligible members only, the latest list for a slow receiver, and the close.
func TestATRUN82Subscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t, tenure.Member{ID: "n1", Addr: "a1"}, tenure.Member{ID: "n2", Addr: "a2"})
		r := newResolver(t, g.Absent("r1"))
		synctest.Wait()

		ch := r.Subscribe()
		if got, want := listIDs(receiveList(t, ch)), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("first list = %v, want %v", got, want)
		}
		noList(t, ch)

		g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		synctest.Wait()
		if got, want := listIDs(receiveList(t, ch)), []string{"n1", "n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("list after n3 joined = %v, want %v", got, want)
		}

		g.Join(tenure.Member{ID: "n4", Addr: "a4", Draining: true}) // a new view, the same list
		synctest.Wait()
		noList(t, ch)

		g.Leave("n3")
		g.SetDraining("n2", true)
		synctest.Wait()
		if got, want := listIDs(receiveList(t, ch)), []string{"n1"}; !slices.Equal(got, want) {
			t.Errorf("list after two changes = %v, want only the latest %v", got, want)
		}
		noList(t, ch)

		late := r.Subscribe()
		l := receiveList(t, late)
		if got, want := listIDs(l), []string{"n1"}; !slices.Equal(got, want) {
			t.Errorf("late subscriber's first list = %v, want %v", got, want)
		}
		l[0].ID = "changed" // each receiver has its own copy
		if got := r.Members()[0].ID; got != "n1" {
			t.Errorf("Members()[0] after a receiver changed its list = %q, want n1", got)
		}

		closeResolver(t, r)
		closedList(t, ch)
		closedList(t, late)
		closedList(t, r.Subscribe())
		check()
	})
}

// TestATRUN83NodeResolver checks Node.Resolver: it ranks over the node's views with no watch of
// its own, follows the node's drain, and the node's Close closes it.
func TestATRUN83NodeResolver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t, tenure.Member{ID: "n2", Addr: "a2"})
		m := g.Absent("n1")
		n := startN1(t, m)

		r := n.Resolver("q")
		ch := r.Subscribe()
		if got, want := listIDs(r.Members()), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("Members() at once = %v, want %v", got, want)
		}
		if got, want := listIDs(receiveList(t, ch)), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("first list = %v, want %v", got, want)
		}

		g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		synctest.Wait()
		v := published(t, n)
		want := core.Eligible(v, time.Now())
		if got := r.Members(); !reflect.DeepEqual(got, want) || !slices.Equal(listIDs(got), []string{"n1", "n2", "n3"}) {
			t.Errorf("Members() after n3 joined = %+v, want %+v", got, want)
		}
		for i := range 100 {
			k := fmt.Sprintf("k%d", i)
			if got, err := r.Lookup(k); err != nil || !reflect.DeepEqual(got, core.Rank("q", k, want, 1)[0]) {
				t.Fatalf("Lookup(%q) = %+v, %v, want %+v", k, got, err, core.Rank("q", k, want, 1)[0])
			}
		}
		if got, want := listIDs(receiveList(t, ch)), []string{"n1", "n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("list after n3 joined = %v, want %v", got, want)
		}

		if err := n.Drain(t.Context()); err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
		synctest.Wait()
		if got, want := listIDs(r.Members()), []string{"n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("Members() after Drain = %v, want %v", got, want)
		}
		if got, want := callMethods(m.Calls("Watch")), []string{"Watch"}; !slices.Equal(got, want) {
			t.Errorf("Watch calls = %v, want only Start's", got)
		}

		userClosed := n.Resolver("q")
		closeResolver(t, userClosed) // a resolver the user closes leaves the node
		closeNode(t, n)
		closedAfterLists(t, ch)
		if _, err := r.Lookup("k"); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("node resolver's Lookup after Close = %v, want ErrClosed", err)
		}
		if err := r.Close(); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("node resolver's Close after the node's = %v, want ErrClosed", err)
		}
		after := n.Resolver("q")
		if _, err := after.Lookup("k"); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Lookup of a resolver made after Close = %v, want ErrClosed", err)
		}
		closedList(t, after.Subscribe())
		check()
	})
}

// fakePlacement places members in reverse ID order and records the namespaces it is asked
// about.
type fakePlacement struct {
	members []tenure.Member
	mu      *sync.Mutex
	asked   *[]string
}

func (p fakePlacement) Rank(namespace, _ string, n int) []tenure.Member {
	p.mu.Lock()
	*p.asked = append(*p.asked, namespace)
	p.mu.Unlock()
	out := slices.Clone(p.members)
	slices.Reverse(out)
	return out[:min(n, len(out))]
}

// TestATRUN84WithPlacement checks WithPlacement: the resolver builds one placement per change of
// the eligible members, none per lookup or per view that leaves the list as it is, and answers
// from it with the namespace of WithNamespace.
func TestATRUN84WithPlacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		var mu sync.Mutex
		var built [][]string
		var asked []string
		place := func(members []tenure.Member) core.Placement {
			mu.Lock()
			defer mu.Unlock()
			built = append(built, listIDs(members))
			return fakePlacement{members: members, mu: &mu, asked: &asked}
		}
		builds := func() [][]string {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(built)
		}

		g := newGroup(t, tenure.Member{ID: "n1", Addr: "a1"}, tenure.Member{ID: "n2", Addr: "a2"})
		r := newResolver(t, g.Absent("r1"), tenure.WithNamespace("ns"), tenure.WithPlacement(place))
		synctest.Wait()
		if got, want := builds(), [][]string{{"n1", "n2"}}; !reflect.DeepEqual(got, want) {
			t.Errorf("placements built = %v, want %v", got, want)
		}
		for i := range 1000 {
			k := fmt.Sprintf("k%d", i)
			if got, err := r.Lookup(k); err != nil || got.ID != "n2" {
				t.Fatalf("Lookup(%q) = %+v, %v, want n2 from the placement", k, got, err)
			}
		}
		if got, err := r.LookupN("k", 5); err != nil || !slices.Equal(listIDs(got), []string{"n2", "n1"}) {
			t.Errorf("LookupN(k, 5) = %v, %v, want [n2 n1]", listIDs(got), err)
		}

		g.Join(tenure.Member{ID: "n0", Addr: "a0", Draining: true}) // a new view, the same list
		synctest.Wait()
		if got := len(builds()); got != 1 {
			t.Errorf("%d placements after a view with the same list, want 1", got)
		}
		g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		synctest.Wait()
		if got, want := builds(), [][]string{{"n1", "n2"}, {"n1", "n2", "n3"}}; !reflect.DeepEqual(got, want) {
			t.Errorf("placements built = %v, want %v", got, want)
		}
		if got, err := r.Lookup("k"); err != nil || got.ID != "n3" {
			t.Errorf("Lookup(k) after n3 joined = %+v, %v, want n3", got, err)
		}
		mu.Lock()
		for i, ns := range asked {
			if ns != "ns" {
				t.Errorf("Rank call %d had namespace %q, want ns", i, ns)
				break
			}
		}
		mu.Unlock()

		closeResolver(t, r)
		check()
	})
}

// TestSubscribeAtLeaveAt checks the timer at the members' JoinAt and LeaveAt: a member enters the
// list at its JoinAt and leaves it at its LeaveAt, at exactly those instants. A JoinAt equal to
// the view's arrival counts at once, two bounds at one instant give one list, and Close stops a
// timer still armed.
func TestSubscribeAtLeaveAt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		t0 := time.Now()
		g := newGroup(t,
			tenure.Member{ID: "n0", Addr: "a0", JoinAt: t0},
			tenure.Member{ID: "n1", Addr: "a1"},
			tenure.Member{ID: "n2", Addr: "a2", LeaveAt: t0.Add(10 * time.Second)},
			tenure.Member{ID: "n3", Addr: "a3", JoinAt: t0.Add(5 * time.Second)},
			tenure.Member{ID: "n4", Addr: "a4", JoinAt: t0.Add(10 * time.Second)},
			tenure.Member{ID: "n5", Addr: "a5", LeaveAt: t0.Add(time.Hour)},
		)
		r := newResolver(t, g.Absent("r1"))
		synctest.Wait()
		ch := r.Subscribe()
		if got, want := listIDs(receiveList(t, ch)), []string{"n0", "n1", "n2", "n5"}; !slices.Equal(got, want) {
			t.Errorf("list at T0 = %v, want %v", got, want)
		}

		steps := []struct {
			at   time.Duration
			want []string
		}{
			{5 * time.Second, []string{"n0", "n1", "n2", "n3", "n5"}},
			{10 * time.Second, []string{"n0", "n1", "n3", "n4", "n5"}}, // n2 leaves as n4 joins
		}
		for _, s := range steps {
			l := <-ch
			if d := time.Since(t0); d != s.at {
				t.Errorf("list %v received at T0+%v, want T0+%v", listIDs(l), d, s.at)
			}
			if got := listIDs(l); !slices.Equal(got, s.want) {
				t.Errorf("list at T0+%v = %v, want %v", s.at, got, s.want)
			}
			if got := listIDs(r.Members()); !slices.Equal(got, s.want) {
				t.Errorf("Members() at T0+%v = %v, want %v", s.at, got, s.want)
			}
		}
		time.Sleep(time.Minute)
		noList(t, ch)

		start := time.Now() // the timer at n5's LeaveAt is still armed
		closeResolver(t, r)
		if d := time.Since(start); d != 0 {
			t.Errorf("Close returned %v after it was called, want at once", d)
		}
		closedList(t, ch)
		check()
	})
}
