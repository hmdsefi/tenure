// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package membershiptest_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/membershiptest"
)

// mutants are the defective memberships the suite must catch, each with the cases it must fail.
var mutants = []struct {
	name  string
	cases []string
}{
	{"meta-alias", []string{"Join/MetaCopy"}},
	{"twice-join", []string{"Join/Twice"}},
	{"no-validate", []string{"Join/Invalid"}},
	{"noop-views", []string{"Draining/Noop"}},
	{"drain-before-join", []string{"Draining/BeforeJoin"}},
	{"leave-open", []string{"Leave/Closed"}},
	{"leave-keeps-channels", []string{"Leave/Closed"}},
	{"unsorted", []string{"Watch/Sorted"}},
	{"fifo", []string{"Watch/Coalesce"}},
	{"no-close", []string{"Watch/Cancel"}},
	{"hide-unjoined", []string{"Watch/WithoutJoin"}},
}

// baselines are the memberships the suite must pass: "none" has no defect, and
// "hide-unjoined-not-observer" is hide-unjoined declared with Listed and Observer false.
var baselines = []string{"none", "hide-unjoined-not-observer"}

// failureLine is the form of every log line of a mutant child.
var failureLine = regexp.MustCompile(`^\s+\S+\.go:\d+: (\w+\(.*\) = .*, want .*|view \d+ on \w+: .*, want .*)$`)

// TestMutant runs the suite on each mutant and baseline. It is child-only: the mutants fail.
func TestMutant(t *testing.T) {
	skipUnlessChild(t)
	names := slices.Clone(baselines)
	for _, m := range mutants {
		names = append(names, m.name)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) { membershiptest.Run(t, newMutantFixture(name)) })
	}
}

// TestMutants: AT-MEM-02 to AT-MEM-06, AT-MEM-08, AT-MEM-09, AT-MEM-11 and AT-MEM-20 (mutant
// parts; steps written from the text of issue #44). Each mutant child starts every case, fails
// at least its named cases and exits non-zero, and every line it logs is a suite failure. Each
// baseline child starts every case and fails none.
func TestMutants(t *testing.T) {
	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			parent := "TestMutant/" + m.name
			r := runChild(t, "^TestMutant$/^"+m.name+"$")
			if !r.failed {
				t.Errorf("child %s exited 0, want it to fail", parent)
			}
			if got := r.under(parent); !slices.Equal(got, caseNames) {
				t.Errorf("%s started %q, want %q", parent, got, caseNames)
			}
			for _, c := range m.cases {
				if got := r.results[parent+"/"+c]; got != "FAIL" {
					t.Errorf("%s/%s: %s, want FAIL", parent, c, got)
				}
			}
			for name, logs := range r.logs {
				for _, l := range logs {
					if !failureLine.MatchString(l.line) {
						t.Errorf("%s logs %q, want a line matching %s", name, l.line, failureLine)
					}
				}
			}
			if t.Failed() {
				t.Logf("child output:\n%s", r.out)
			}
		})
	}
	for _, name := range baselines {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parent := "TestMutant/" + name
			r := runChild(t, "^TestMutant$/^"+name+"$")
			if r.failed {
				t.Errorf("child %s failed, want it to pass:\n%s", parent, r.out)
			}
			if got := r.under(parent); !slices.Equal(got, caseNames) {
				t.Errorf("%s started %q, want %q", parent, got, caseNames)
			}
			for _, c := range caseNames {
				if got := r.results[parent+"/"+c]; got == "FAIL" {
					t.Errorf("%s/%s: FAIL, want PASS or SKIP", parent, c)
				}
			}
		})
	}
}

// newMutantFixture returns the newMembership function of the mutant or baseline called name:
// every instance is a new handle on one mutant group that lists ids.
func newMutantFixture(name string) func(t *testing.T, ids []string) membershiptest.Fixture {
	return func(t *testing.T, ids []string) membershiptest.Fixture {
		defect := name
		if name == "hide-unjoined-not-observer" {
			defect = "hide-unjoined"
		}
		members := make([]core.Member, len(ids))
		for i, id := range ids {
			members[i] = core.Member{ID: id, Slot: -1}
		}
		g := &mutantGroup{
			defect:  defect,
			members: members,
			version: 1,
			joined:  make(map[string]*mutant),
			handles: make(map[*mutant]struct{}),
		}
		return membershiptest.Fixture{
			New:      func() tenure.Membership { return &mutant{g: g} },
			Listed:   defect != "hide-unjoined",
			Observer: name != "hide-unjoined-not-observer",
		}
	}
}

// mutantGroup is a group with static's rules, except for its one defect, which it names. An
// empty or unknown defect is no defect.
type mutantGroup struct {
	defect string

	mu      sync.Mutex
	members []core.Member // sorted by ID unless the defect is "unsorted"
	version uint64
	joined  map[string]*mutant
	handles map[*mutant]struct{} // handles with open watches
}

// mutant is one handle on a mutant group. It implements tenure.Membership and
// tenure.PropagationEstimator.
type mutant struct {
	g *mutantGroup

	// The fields below are guarded by g.mu.
	self    string
	closed  bool
	watches map[*mutantWatch]struct{}
}

// mutantWatch is one channel returned by Watch.
type mutantWatch struct {
	ch   chan core.View
	stop func() bool
}

// has reports whether the group's defect is d.
func (g *mutantGroup) has(d string) bool { return g.defect == d }

func (m *mutant) Join(ctx context.Context, self core.Member) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return tenure.ErrClosed
	}
	if err := self.Validate(); err != nil && !g.has("no-validate") {
		return fmt.Errorf("mutant: %w", err)
	}
	switch {
	case m.self != "" && !g.has("twice-join"):
		return errors.New("mutant: already joined")
	case g.joined[self.ID] != nil && g.joined[self.ID] != m:
		return fmt.Errorf("mutant: member %q already joined", self.ID)
	}
	if m.self != "" {
		delete(g.joined, m.self)
	}
	m.self = self.ID
	g.joined[self.ID] = m
	stored := self
	if !g.has("meta-alias") {
		stored = copyMember(self)
	}
	next := slices.Clone(g.members)
	i, found := g.index(self.ID)
	switch {
	case g.has("unsorted"):
		if found {
			next = slices.Delete(next, i, i+1)
		}
		next = append(next, stored)
	case found:
		next[i] = stored
	default:
		next = slices.Insert(next, i, stored)
	}
	g.update(next)
	return nil
}

func (m *mutant) Watch(ctx context.Context) (<-chan core.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return nil, tenure.ErrClosed
	}
	size := 1
	if g.has("fifo") {
		size = 64
	}
	w := &mutantWatch{ch: make(chan core.View, size)}
	w.ch <- g.viewFor(m)
	if m.watches == nil {
		m.watches = make(map[*mutantWatch]struct{})
	}
	m.watches[w] = struct{}{}
	g.handles[m] = struct{}{}
	w.stop = context.AfterFunc(ctx, func() {
		if g.has("no-close") {
			return
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if _, ok := m.watches[w]; ok {
			m.unwatch(w)
		}
	})
	return w.ch, nil
}

func (m *mutant) SetDraining(ctx context.Context, draining bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case m.closed:
		return tenure.ErrClosed
	case m.self == "" && g.has("drain-before-join"):
		return nil
	case m.self == "":
		return errors.New("mutant: not joined")
	}
	i, _ := g.index(m.self)
	if g.members[i].Draining == draining && !g.has("noop-views") {
		return nil
	}
	next := slices.Clone(g.members)
	next[i].Draining = draining
	g.update(next)
	return nil
}

func (m *mutant) Leave(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return nil
	}
	if m.self != "" {
		i, _ := g.index(m.self)
		delete(g.joined, m.self)
		g.update(slices.Delete(slices.Clone(g.members), i, i+1))
		m.self = ""
	}
	if !g.has("leave-keeps-channels") {
		for w := range m.watches {
			w.stop()
			m.unwatch(w)
		}
	}
	m.closed = !g.has("leave-open")
	return nil
}

func (m *mutant) Propagation() time.Duration { return 0 }

// unwatch closes w and forgets it. The caller holds m.g.mu.
func (m *mutant) unwatch(w *mutantWatch) {
	close(w.ch)
	delete(m.watches, w)
	if len(m.watches) == 0 {
		delete(m.g.handles, m)
	}
}

// index returns the index of the member with id, searching every member so that it works on an
// unsorted list too, and whether it is there; when it is not, the index where it would go in a
// sorted list. The caller holds g.mu.
func (g *mutantGroup) index(id string) (int, bool) {
	for i, e := range g.members {
		if e.ID == id {
			return i, true
		}
	}
	i := 0
	for i < len(g.members) && g.members[i].ID < id {
		i++
	}
	return i, false
}

// viewFor returns the current view as h sees it, with its own copy of the members. With the
// defect "hide-unjoined", a handle that has not joined sees no members. The caller holds g.mu.
func (g *mutantGroup) viewFor(h *mutant) core.View {
	v := core.View{Version: g.version}
	if h.self != "" || !g.has("hide-unjoined") {
		v.Members = copyMembers(g.members)
	}
	return v
}

// update makes next the group's member list and, when member data changed (always with the
// defect "noop-views"), delivers the new view to every open channel. A view not yet received is
// replaced, except with the defect "fifo"; with "hide-unjoined", a handle that has not joined
// gets no new view. The caller holds g.mu.
func (g *mutantGroup) update(next []core.Member) {
	if slices.EqualFunc(g.members, next, sameMemberData) && !g.has("noop-views") {
		return
	}
	g.members = next
	g.version++
	for h := range g.handles {
		if h.self == "" && g.has("hide-unjoined") {
			continue
		}
		for w := range h.watches {
			if !g.has("fifo") {
				select {
				case <-w.ch:
				default:
				}
			}
			w.ch <- g.viewFor(h)
		}
	}
}

// sameMemberData reports whether a and b hold the same member data: Slot is ignored and Meta is
// compared by content.
func sameMemberData(a, b core.Member) bool {
	return a.ID == b.ID && a.Addr == b.Addr && a.Draining == b.Draining &&
		maps.Equal(a.Meta, b.Meta) && a.JoinAt.Equal(b.JoinAt) && a.LeaveAt.Equal(b.LeaveAt)
}

// copyMember returns m with its own copy of m.Meta.
func copyMember(m core.Member) core.Member {
	m.Meta = maps.Clone(m.Meta)
	return m
}

// copyMembers returns a copy of members, Meta maps included, or nil when members is empty.
func copyMembers(members []core.Member) []core.Member {
	if len(members) == 0 {
		return nil
	}
	out := make([]core.Member, len(members))
	for i, m := range members {
		out[i] = copyMember(m)
	}
	return out
}
