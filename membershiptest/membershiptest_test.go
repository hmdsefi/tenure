// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package membershiptest_test

import (
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/membershiptest"
	"github.com/hmdsefi/tenure/static"
)

// caseNames is every case Run starts, in order.
var caseNames = []string{
	"Watch/Initial",
	"Watch/Sorted",
	"Watch/Versions",
	"Watch/Coalesce",
	"Watch/Cancel",
	"Watch/Multiple",
	"Watch/WithoutJoin",
	"Join/Visible",
	"Join/MetaCopy",
	"Join/Twice",
	"Join/Invalid",
	"Join/Canceled",
	"Draining/Visible",
	"Draining/Noop",
	"Draining/BeforeJoin",
	"Leave/Visible",
	"Leave/Closed",
	"Leave/BeforeJoin",
	"Propagation/Estimate",
}

// wantIDs are the IDs Run passes to newMembership.
var wantIDs = []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"}

// newGroup returns a static group that lists ids, each with only its ID and Slot -1.
func newGroup(t *testing.T, ids []string) *static.Membership {
	t.Helper()
	members := make([]core.Member, len(ids))
	for i, id := range ids {
		members[i] = core.Member{ID: id, Slot: -1}
	}
	g, err := static.New(members)
	if err != nil {
		t.Fatalf("static.New(%v) = %v", ids, err)
	}
	return g
}

// newStatic is the static fixture: every instance is a new handle on one group that lists ids.
func newStatic(t *testing.T, ids []string) membershiptest.Fixture {
	g := newGroup(t, ids)
	return membershiptest.Fixture{
		New:      func() tenure.Membership { return g.Share() },
		Listed:   true,
		Observer: true,
	}
}

func TestConformance(t *testing.T) {
	membershiptest.Run(t, newStatic)
}

// TestCaseNames checks that Run starts exactly the expected cases, in order, on the static
// fixture.
func TestCaseNames(t *testing.T) {
	r := runChild(t, "^TestConformance$")
	if r.failed {
		t.Fatalf("child TestConformance failed:\n%s", r.out)
	}
	if got := r.under("TestConformance"); !slices.Equal(got, caseNames) {
		t.Errorf("cases started on the static fixture:\n  %s\nwant:\n  %s",
			strings.Join(got, "\n  "), strings.Join(caseNames, "\n  "))
	}
}

// hidden is a Membership that shows only the tenure.Membership methods of the one it wraps, so
// it does not implement tenure.PropagationEstimator.
type hidden struct{ tenure.Membership }

// badFixtures are the fixtures every case must fail on, each with the one message every case
// logs. Each breaks one rule of the static fixture.
var badFixtures = []struct {
	name string
	msg  string
	new  func(t *testing.T, ids []string) membershiptest.Fixture
}{
	{"nil-new", "membershiptest: Fixture.New is nil",
		func(t *testing.T, ids []string) membershiptest.Fixture {
			f := newStatic(t, ids)
			f.New = nil
			return f
		}},
	{"negative-settle", "membershiptest: Fixture.Settle -1ns is negative",
		func(t *testing.T, ids []string) membershiptest.Fixture {
			f := newStatic(t, ids)
			f.Settle = -1
			return f
		}},
	{"extra-id", `membershiptest: group lists "m8", which is not in ids`,
		func(t *testing.T, ids []string) membershiptest.Fixture {
			return newStatic(t, append(slices.Clone(ids), "m8"))
		}},
}

// TestChildFixtureChecks runs the suite on each bad fixture. It is child-only: every case fails.
func TestChildFixtureChecks(t *testing.T) {
	skipUnlessChild(t)
	for _, b := range badFixtures {
		t.Run(b.name, func(t *testing.T) { membershiptest.Run(t, b.new) })
	}
}

// TestChildHiddenEstimator runs the suite on the static fixture with PropagationEstimator
// hidden. It is child-only.
func TestChildHiddenEstimator(t *testing.T) {
	skipUnlessChild(t)
	membershiptest.Run(t, func(t *testing.T, ids []string) membershiptest.Fixture {
		f := newStatic(t, ids)
		next := f.New
		f.New = func() tenure.Membership { return hidden{next()} }
		return f
	})
}

// TestChildNotObserver runs the suite on the static fixture with Observer false. It is
// child-only.
func TestChildNotObserver(t *testing.T) {
	skipUnlessChild(t)
	membershiptest.Run(t, func(t *testing.T, ids []string) membershiptest.Fixture {
		f := newStatic(t, ids)
		f.Observer = false
		return f
	})
}

// TestATMEM19Fixture: AT-MEM-19 (steps written from the text of issue #43). Run calls
// newMembership once per case with the IDs m0 to m7. With a nil New, a Settle of -1 and a group
// that lists an extra ID, every case fails, logging exactly the fixture's message and nothing
// else. With PropagationEstimator hidden, Propagation/Estimate skips with its message and every
// other case passes.
func TestATMEM19Fixture(t *testing.T) {
	var (
		mu    sync.Mutex
		calls = make(map[string][][]string)
	)
	t.Run("suite", func(t *testing.T) {
		membershiptest.Run(t, func(t *testing.T, ids []string) membershiptest.Fixture {
			name, _ := strings.CutPrefix(t.Name(), "TestATMEM19Fixture/suite/")
			mu.Lock()
			calls[name] = append(calls[name], slices.Clone(ids))
			mu.Unlock()
			return newStatic(t, ids)
		})
	})
	for _, name := range caseNames {
		if got := calls[name]; len(got) != 1 || !slices.Equal(got[0], wantIDs) {
			t.Errorf("%s called newMembership with %q, want once with %q", name, got, wantIDs)
		}
	}
	if len(calls) != len(caseNames) {
		t.Errorf("newMembership was called by %d cases, want %d", len(calls), len(caseNames))
	}

	r := runChild(t, "^TestChildFixtureChecks$")
	if !r.failed {
		t.Errorf("child TestChildFixtureChecks passed, want it to fail:\n%s", r.out)
	}
	for _, b := range badFixtures {
		parent := "TestChildFixtureChecks/" + b.name
		if got := r.under(parent); !slices.Equal(got, caseNames) {
			t.Errorf("%s started %q, want %q", b.name, got, caseNames)
		}
		for _, name := range caseNames {
			full := parent + "/" + name
			if got := r.results[full]; got != "FAIL" {
				t.Errorf("%s %s: %s, want FAIL", b.name, name, got)
			}
			if got, want := msgs(r.logs[full]), []string{b.msg}; !slices.Equal(got, want) {
				t.Errorf("%s %s logs %q, want %q", b.name, name, got, want)
			}
		}
	}

	checkOneSkip(t, "TestChildHiddenEstimator", "Propagation/Estimate",
		"membershiptest: membership does not implement tenure.PropagationEstimator")
}

// TestATMEM11NotObserver: AT-MEM-11 (static-fixture part; steps written from the text of issue
// #43). With Observer false, Watch/WithoutJoin skips with its message and every other case
// passes.
func TestATMEM11NotObserver(t *testing.T) {
	checkOneSkip(t, "TestChildNotObserver", "Watch/WithoutJoin", "membershiptest: fixture is not an Observer")
}

// checkOneSkip runs the child-only test parent and checks that its case skipped skips, logging
// only msg, and that every other case passes without logging.
func checkOneSkip(t *testing.T, parent, skipped, msg string) {
	t.Helper()
	r := runChild(t, "^"+parent+"$")
	if r.failed {
		t.Errorf("child %s failed:\n%s", parent, r.out)
	}
	if got := r.under(parent); !slices.Equal(got, caseNames) {
		t.Errorf("%s started %q, want %q", parent, got, caseNames)
	}
	for _, name := range caseNames {
		full := parent + "/" + name
		want, wantLogs := "PASS", []string(nil)
		if name == skipped {
			want, wantLogs = "SKIP", []string{msg}
		}
		if got := r.results[full]; got != want {
			t.Errorf("%s: %s, want %s", full, got, want)
		}
		if got := msgs(r.logs[full]); !slices.Equal(got, wantLogs) {
			t.Errorf("%s logs %q, want %q", full, got, wantLogs)
		}
	}
}

// msgs returns the messages of log lines.
func msgs(logs []childLog) []string {
	var out []string
	for _, l := range logs {
		out = append(out, l.msg)
	}
	return out
}
