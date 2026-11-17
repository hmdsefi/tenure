// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package membershiptest_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/membershiptest"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/static"
)

// watchOnlyCaseNames is every case RunWatchOnly starts, in order.
var watchOnlyCaseNames = []string{
	"WatchOnly/Initial",
	"WatchOnly/Follow",
	"WatchOnly/Slots",
	"WatchOnly/Refuse",
	"WatchOnly/Cancel",
	"WatchOnly/Fresh",
}

// watchOnlyMutants are the defective watch-only wrappers the suite must catch, each with the
// cases it must fail.
var watchOnlyMutants = []struct {
	name  string
	cases []string
}{
	{"leak-write", []string{"WatchOnly/Refuse"}},
	{"join-unwrapped", []string{"WatchOnly/Refuse"}},
	{"drain-nil", []string{"WatchOnly/Refuse"}},
	{"leave-nil", []string{"WatchOnly/Refuse"}},
	{"skip-drain", []string{"WatchOnly/Follow"}},
	{"repeat-view", []string{"WatchOnly/Follow"}},
	{"empty-first", []string{"WatchOnly/Initial", "WatchOnly/Slots"}},
	{"no-slots", []string{"WatchOnly/Slots"}},
	{"holder-name", []string{"WatchOnly/Slots"}},
	{"stuck-holder", []string{"WatchOnly/Slots"}},
	{"no-close", []string{"WatchOnly/Cancel"}},
	{"stale", []string{"WatchOnly/Fresh"}},
}

// watchOnlyBaselines are the wrappers the suite must pass: "none" has no defect, and
// "no-stale-method" has no Stale method, so the case about it skips.
var watchOnlyBaselines = []string{"none", "no-stale-method"}

// TestWatchOnlyConformance runs RunWatchOnly on the wrapper without defects: static writers, and
// slot data from a memstore Store merged into the watch-only views as a node merges its own.
func TestWatchOnlyConformance(t *testing.T) {
	membershiptest.RunWatchOnly(t, newWatchOnlyFixture("none"))
}

// TestWatchOnlyCaseNames checks that RunWatchOnly starts exactly the expected cases, in order,
// and skips none on the wrapper without defects.
func TestWatchOnlyCaseNames(t *testing.T) {
	r := runChild(t, "^TestWatchOnlyConformance$")
	if r.failed {
		t.Fatalf("child TestWatchOnlyConformance failed:\n%s", r.out)
	}
	if got := r.under("TestWatchOnlyConformance"); !slices.Equal(got, watchOnlyCaseNames) {
		t.Errorf("cases started:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "),
			strings.Join(watchOnlyCaseNames, "\n  "))
	}
	for _, c := range watchOnlyCaseNames {
		if got := r.results["TestWatchOnlyConformance/"+c]; got != "PASS" {
			t.Errorf("TestWatchOnlyConformance/%s: %s, want PASS", c, got)
		}
	}
}

// TestWatchOnlyMutant runs RunWatchOnly on each mutant and baseline. It is child-only: the
// mutants fail.
func TestWatchOnlyMutant(t *testing.T) {
	skipUnlessChild(t)
	names := slices.Clone(watchOnlyBaselines)
	for _, m := range watchOnlyMutants {
		names = append(names, m.name)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) { membershiptest.RunWatchOnly(t, newWatchOnlyFixture(name)) })
	}
}

// TestWatchOnlyMutants checks that the suite catches a watch-only wrapper that leaks a write,
// refuses one without tenure.ErrWatchOnly, skips a view, repeats one, starts with an empty view,
// drops or garbles the slot data, leaves a channel open past its context or reports itself stale
// while views arrive (RTR-780): each mutant child starts every case, fails at least its
// named cases and exits non-zero, and every line it logs is a suite failure. Each baseline child
// fails no case; without a Stale method, WatchOnly/Fresh skips.
func TestWatchOnlyMutants(t *testing.T) {
	for _, m := range watchOnlyMutants {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			parent := "TestWatchOnlyMutant/" + m.name
			r := runChild(t, "^TestWatchOnlyMutant$/^"+m.name+"$")
			if !r.failed {
				t.Errorf("child %s exited 0, want it to fail", parent)
			}
			if got := r.under(parent); !slices.Equal(got, watchOnlyCaseNames) {
				t.Errorf("%s started %q, want %q", parent, got, watchOnlyCaseNames)
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
	for _, name := range watchOnlyBaselines {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parent := "TestWatchOnlyMutant/" + name
			r := runChild(t, "^TestWatchOnlyMutant$/^"+name+"$")
			if r.failed {
				t.Errorf("child %s failed, want it to pass:\n%s", parent, r.out)
			}
			for _, c := range watchOnlyCaseNames {
				want := "PASS"
				if name == "no-stale-method" && c == "WatchOnly/Fresh" {
					want = "SKIP"
				}
				if got := r.results[parent+"/"+c]; got != want {
					t.Errorf("%s/%s: %s, want %s", parent, c, got, want)
				}
			}
		})
	}
}

// TestWatchOnlyFixtureChecks checks that RunWatchOnly fails a case whose fixture is unusable,
// with the first failing check's message, and skips the slot case of a fixture without Slots.
func TestWatchOnlyFixtureChecks(t *testing.T) {
	r := runChild(t, "^TestWatchOnlyBadFixture$")
	want := map[string]string{
		"nil-watch-only": "membershiptest: WatchOnlyFixture.WatchOnly is nil",
		"few-writers":    "membershiptest: WatchOnlyFixture has 2 Writers for 8 ids",
		"nil-writer":     "membershiptest: WatchOnlyFixture.Writers[3] is nil",
		"negative":       "membershiptest: WatchOnlyFixture.Settle -1s is negative",
		"nil-slot-func":  "membershiptest: WatchOnlyFixture.Slots has a nil function",
		"no-slots":       "membershiptest: fixture has no Slots",
	}
	for name, msg := range want {
		test := "TestWatchOnlyBadFixture/" + name + "/WatchOnly/Initial"
		if name == "no-slots" {
			test = "TestWatchOnlyBadFixture/" + name + "/WatchOnly/Slots"
		}
		logs := r.logs[test]
		if len(logs) == 0 || logs[0].msg != msg {
			t.Errorf("%s logs %v, want first %q", test, logs, msg)
		}
	}
	if got := r.results["TestWatchOnlyBadFixture/no-slots/WatchOnly/Slots"]; got != "SKIP" {
		t.Errorf("no-slots: WatchOnly/Slots %s, want SKIP", got)
	}
	if t.Failed() {
		t.Logf("child output:\n%s", r.out)
	}
}

// TestWatchOnlyBadFixture runs RunWatchOnly on unusable fixtures. It is child-only: they fail.
func TestWatchOnlyBadFixture(t *testing.T) {
	skipUnlessChild(t)
	good := newWatchOnlyFixture("none")
	bad := map[string]func(f *membershiptest.WatchOnlyFixture){
		"nil-watch-only": func(f *membershiptest.WatchOnlyFixture) { f.WatchOnly = nil },
		"few-writers":    func(f *membershiptest.WatchOnlyFixture) { f.Writers = f.Writers[:2] },
		"nil-writer":     func(f *membershiptest.WatchOnlyFixture) { f.Writers[3] = nil },
		"negative":       func(f *membershiptest.WatchOnlyFixture) { f.Settle = -time.Second },
		"nil-slot-func":  func(f *membershiptest.WatchOnlyFixture) { f.Slots.Release = nil },
		"no-slots":       func(f *membershiptest.WatchOnlyFixture) { f.Slots = nil },
	}
	for name, change := range bad {
		t.Run(name, func(t *testing.T) {
			membershiptest.RunWatchOnly(t, func(t *testing.T, ids []string) membershiptest.WatchOnlyFixture {
				f := good(t, ids)
				change(&f)
				return f
			})
		})
	}
}

// newWatchOnlyFixture returns the fixture function of the wrapper with the defect called name:
// writers are handles on one static group that starts empty, and the watch-only membership is a
// wrapper around another handle that merges in the slot data of a memstore Store, whose slots the
// fixture's SlotControl changes.
func newWatchOnlyFixture(name string) func(t *testing.T, ids []string) membershiptest.WatchOnlyFixture {
	return func(t *testing.T, ids []string) membershiptest.WatchOnlyFixture {
		g, err := static.New(nil)
		if err != nil {
			t.Fatalf("static.New(nil) = %v", err)
		}
		store := memstore.New(nil)
		writers := make([]tenure.Membership, len(ids))
		for i := range ids {
			writers[i] = g.Share()
		}
		wo := &watchOnly{m: g.Share(), slots: store, defect: name, period: 5 * time.Millisecond,
			held: make(map[int]string)}
		var m tenure.Membership = wo
		if name == "no-stale-method" {
			m = plainWatchOnly{wo}
		}
		holder := func(id string) string { return id + "/1" }
		return membershiptest.WatchOnlyFixture{
			Writers:   writers,
			WatchOnly: m,
			Settle:    100 * time.Millisecond,
			Slots: &membershiptest.SlotControl{
				SetCount: store.SetSlots,
				Claim: func(ctx context.Context, id string) (int, error) {
					if err := store.KeepAlive(ctx, holder(id), core.DefaultTiming); err != nil {
						return -1, err
					}
					return store.ClaimSlot(ctx, holder(id))
				},
				Release: func(ctx context.Context, id string, slot int) error {
					return store.ReleaseSlot(ctx, holder(id), slot)
				},
			},
		}
	}
}

// slotReader is what watchOnly reads slot data from: memstore's Slots.
type slotReader interface {
	Slots(ctx context.Context) (count int, holders []string, err error)
}

// watchOnly is a watch-only membership for the suite's own tests: it follows a static handle
// that never joins, merges into each view the slot data it reads from a store every period, as a
// node merges its own, and refuses every write with tenure.ErrWatchOnly. A merged view gets the
// next Version of the wrapper's own counter, and only when its member or slot data changes. A
// newer view replaces one not yet received. defect names the one defect it has, if any.
type watchOnly struct {
	m       tenure.Membership
	slots   slotReader
	defect  string
	period  time.Duration
	version atomic.Uint64

	mu   sync.Mutex
	held map[int]string // with the defect "stuck-holder", the last holder seen on each slot
}

func (w *watchOnly) Join(ctx context.Context, self core.Member) error {
	switch w.defect {
	case "leak-write":
		_ = w.m.Join(ctx, self)
	case "join-unwrapped":
		return fmt.Errorf("watchonly: Join: %v", tenure.ErrWatchOnly)
	}
	return fmt.Errorf("watchonly: Join: %w", tenure.ErrWatchOnly)
}

func (w *watchOnly) SetDraining(context.Context, bool) error {
	if w.defect == "drain-nil" {
		return nil
	}
	return fmt.Errorf("watchonly: SetDraining: %w", tenure.ErrWatchOnly)
}

// Leave refuses, or returns nil with the defect "leave-nil", as Leave before Join may on a
// membership that can join.
func (w *watchOnly) Leave(context.Context) error {
	if w.defect == "leave-nil" {
		return nil
	}
	return fmt.Errorf("watchonly: Leave: %w", tenure.ErrWatchOnly)
}

// Stale reports false, or true with the defect "stale".
func (w *watchOnly) Stale() bool { return w.defect == "stale" }

// Watch returns a channel holding the merged current view, which receives each later merged
// view until ctx ends or the static channel closes. With the defect "empty-first", the first
// view lists no members and no slots, and the merged view follows it.
func (w *watchOnly) Watch(ctx context.Context) (<-chan core.View, error) {
	in, err := w.m.Watch(ctx)
	if err != nil {
		return nil, err
	}
	mem := <-in // static holds the current view at once
	out := make(chan core.View, 1)
	last := w.merge(ctx, mem)
	if w.defect == "empty-first" {
		last = core.View{Version: last.Version}
	}
	out <- last
	go w.follow(ctx, in, out, mem, last)
	return out, nil
}

// follow merges each view of in, and each change of the slot data read every period, into out
// until in closes, then closes out (not with the defect "no-close"). With the defect
// "repeat-view", each merged view is sent twice.
func (w *watchOnly) follow(ctx context.Context, in <-chan core.View, out chan core.View, mem, last core.View) {
	tick := time.NewTicker(w.period)
	defer tick.Stop()
	for {
		select {
		case v, ok := <-in:
			if !ok {
				if w.defect != "no-close" {
					close(out)
				}
				return
			}
			mem = v
		case <-tick.C:
		}
		next := w.merge(ctx, mem)
		if sameView(next, last) {
			continue
		}
		if w.defect == "skip-drain" && slices.Equal(ids(next), ids(last)) && next.Slots == last.Slots &&
			slices.Equal(next.SlotHolders, last.SlotHolders) {
			continue
		}
		next.Version = w.version.Add(1)
		last = next
		if w.defect == "repeat-view" {
			w.sendTwice(ctx, out, next)
			continue
		}
		select {
		case <-out:
		default:
		}
		out <- next
	}
}

// sendTwice sends v and then a copy of it with the next Version, without replacing an unreceived
// view, so that the copy is received after v: the defect "repeat-view".
func (w *watchOnly) sendTwice(ctx context.Context, out chan core.View, v core.View) {
	dup := v
	dup.Version = w.version.Add(1)
	for _, x := range []core.View{v, dup} {
		select {
		case out <- x:
		case <-ctx.Done():
			return
		}
	}
}

// merge returns v with the store's slot data merged in, as a node merges it, and the wrapper's
// next Version; with the defect "no-slots", without slot data. A failed read merges no slots.
// With the defect "holder-name", each held slot's holder is the slot's name instead; with
// "stuck-holder", a slot keeps the last holder seen on it after a release.
func (w *watchOnly) merge(ctx context.Context, v core.View) core.View {
	out := core.View{Version: w.version.Add(1), Members: slices.Clone(v.Members)}
	if w.defect != "no-slots" {
		if count, holders, err := w.slots.Slots(ctx); err == nil && count > 0 {
			out.Slots = count
			out.SlotHolders = make([]string, count)
			copy(out.SlotHolders, holders)
		}
	}
	for s, h := range out.SlotHolders {
		switch {
		case w.defect == "holder-name" && h != "":
			out.SlotHolders[s] = fmt.Sprintf("slot-%d", s)
		case w.defect == "stuck-holder":
			w.mu.Lock()
			if h != "" {
				w.held[s] = h
			} else {
				out.SlotHolders[s] = w.held[s]
			}
			w.mu.Unlock()
		}
	}
	for i := range out.Members {
		out.Members[i].Slot = slices.IndexFunc(out.SlotHolders, func(h string) bool {
			id, _, _ := strings.Cut(h, "/")
			return h != "" && id == out.Members[i].ID
		})
	}
	return out
}

// plainWatchOnly is a watchOnly without the Stale method.
type plainWatchOnly struct{ w *watchOnly }

func (p plainWatchOnly) Join(ctx context.Context, self core.Member) error { return p.w.Join(ctx, self) }
func (p plainWatchOnly) SetDraining(ctx context.Context, d bool) error {
	return p.w.SetDraining(ctx, d)
}
func (p plainWatchOnly) Leave(ctx context.Context) error { return p.w.Leave(ctx) }
func (p plainWatchOnly) Watch(ctx context.Context) (<-chan core.View, error) {
	return p.w.Watch(ctx)
}

// sameView reports whether a and b hold the same member data, Slots included, and slot data.
func sameView(a, b core.View) bool {
	return slices.EqualFunc(a.Members, b.Members, func(x, y core.Member) bool {
		return sameMemberData(x, y) && x.Slot == y.Slot
	}) && a.Slots == b.Slots && slices.Equal(a.SlotHolders, b.SlotHolders)
}

// ids returns the IDs of v's members.
func ids(v core.View) []string {
	out := make([]string, len(v.Members))
	for i, m := range v.Members {
		out[i] = m.ID
	}
	return out
}
