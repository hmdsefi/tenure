// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/storetest"
)

// caseNames is every case Run starts, in order.
var caseNames = []string{
	"Session/RenewalKeepsRecords",
	"Session/ExpiryBoundary",
	"Session/Expired",
	"Session/LateRenewal",
	"Session/Replace",
	"Session/ReplacedForGood",
	"Session/Close",
	"Session/CloseUnknown",
	"Session/HolderLease",
	"Session/RetryAfter",
	"Acquire/Fresh",
	"Acquire/Held",
	"Acquire/Reacquire",
	"Acquire/AfterRelease",
	"Acquire/Independent",
	"Term/WallAhead",
	"Term/WallBehind",
	"Term/LostRecord",
	"Term/LostRecordWallBehind",
	"Term/Sequence",
	"Release/Match",
	"Release/WrongTerm",
	"Release/WrongHolder",
	"Release/Late",
	"Release/Absent",
	"Release/Twice",
	"Release/EndedSession",
	"CloseSession/Stale",
	"Holdings/Order",
	"Holdings/Version",
	"Confirm/Held",
	"Confirm/Other",
	"Confirm/WrongTerm",
	"Confirm/AfterTakeover",
	"Confirm/NoSession",
	"Race/TwoAcquirers",
	"Race/ManyAcquirers",
	"Race/ReleaseAcquire",
	"Race/CloseAcquire",
	"Race/Terms",
	"Slots/Claim",
	"Slots/Own",
	"Slots/Full",
	"Slots/NoSession",
	"Slots/Expired",
	"Slots/Replaced",
	"Slots/Release",
	"Slots/ReleaseOther",
	"Slots/Count",
	"Slots/Zero",
	"Slots/Retired",
	"Slots/Grow",
	"Slots/CloseSession",
	"Watch/Grant",
	"Watch/Release",
	"Watch/CloseSession",
	"Watch/Order",
	"Watch/Versions",
	"Watch/Delete",
	"Watch/Cancel",
	"Watch/Canceled",
	"Watch/Multiple",
	"Context/Canceled",
	"Args/Invalid",
}

// testClock is a memstore.Clock that moves only when the test moves it.
type testClock struct {
	mu   sync.Mutex
	mono time.Duration
	wall time.Time
}

// epoch is the virtual clock's starting wall time.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestClock() *testClock { return &testClock{wall: epoch} }

func (c *testClock) Monotonic() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

func (c *testClock) Wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

// advance moves both readings forward by d.
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mono += d
	c.wall = c.wall.Add(d)
}

// setWall sets the wall reading only.
func (c *testClock) setWall(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall = t
}

// realTiming is the real-time fixture's Timing, short enough to keep the suite quick.
var realTiming = core.Timing{
	LeaseDuration: 400 * time.Millisecond,
	RenewDeadline: 300 * time.Millisecond,
	RetryPeriod:   100 * time.Millisecond,
	SlotGrace:     400 * time.Millisecond,
}

// virtualFixture is a memstore on a test clock: the suite runs in virtual time. The peer is the
// same Store, as a second client of one in-memory backend.
func virtualFixture(clock *testClock, st tenure.Store, mem *memstore.Store) storetest.Fixture {
	return storetest.Fixture{
		Store:        st,
		Peer:         func() tenure.Store { return st },
		Timing:       core.DefaultTiming,
		Advance:      clock.advance,
		SetWall:      clock.setWall,
		DeleteRecord: mem.DeleteRecord,
	}
}

// newVirtual returns the virtual-time memstore fixture.
func newVirtual(*testing.T) storetest.Fixture {
	clock := newTestClock()
	st := memstore.New(clock)
	return virtualFixture(clock, st, st)
}

// newRealTime returns the real-time memstore fixture: the system clock, no Advance, no SetWall.
func newRealTime(*testing.T) storetest.Fixture {
	st := memstore.New(nil)
	return storetest.Fixture{
		Store:        st,
		Peer:         func() tenure.Store { return st },
		Timing:       realTiming,
		DeleteRecord: st.DeleteRecord,
	}
}

func TestConformance(t *testing.T) {
	t.Run("virtual", func(t *testing.T) { storetest.Run(t, newVirtual) })
	t.Run("realtime", func(t *testing.T) { storetest.Run(t, newRealTime) })
}

// TestCaseNames checks that Run starts exactly the expected cases, in order, under the virtual
// fixture.
func TestCaseNames(t *testing.T) {
	r := runChild(t, "^TestConformance$/^virtual$")
	if r.failed {
		t.Fatalf("child TestConformance/virtual failed:\n%s", r.out)
	}
	if got := r.under("TestConformance/virtual"); !slices.Equal(got, caseNames) {
		t.Errorf("cases started under the virtual fixture:\n  %s\nwant:\n  %s",
			strings.Join(got, "\n  "), strings.Join(caseNames, "\n  "))
	}
}

// badFixtures are AT-STO-35's five bad fixtures, each with the message every case must fail
// with. Each breaks one rule of an otherwise valid fixture.
var badFixtures = []struct {
	name string
	msg  string
	fix  func(f *storetest.Fixture)
}{
	{"nil-store", "storetest: Fixture.Store is nil",
		func(f *storetest.Fixture) { f.Store = nil }},
	{"bad-timing", "storetest: Fixture.Timing: " + core.Timing{}.Validate().Error(),
		func(f *storetest.Fixture) { f.Timing = core.Timing{} }},
	{"negative-slack", "storetest: Fixture.Slack -1ms is negative",
		func(f *storetest.Fixture) { f.Slack = -time.Millisecond }},
	{"nil-peer", "storetest: Fixture.Peer returned nil",
		func(f *storetest.Fixture) { f.Peer = func() tenure.Store { return nil } }},
	{"long-lease", "storetest: real-time Fixture.Timing.LeaseDuration 11s is over 10s",
		func(f *storetest.Fixture) {
			f.Advance, f.SetWall = nil, nil
			f.Timing = core.Timing{LeaseDuration: 11 * time.Second, RenewDeadline: 9 * time.Second,
				RetryPeriod: 2 * time.Second, SlotGrace: 11 * time.Second}
		}},
}

// TestChildFixtureChecks runs the suite on each bad fixture. It is child-only: every case fails.
func TestChildFixtureChecks(t *testing.T) {
	skipUnlessChild(t)
	for _, b := range badFixtures {
		t.Run(b.name, func(t *testing.T) {
			storetest.Run(t, func(t *testing.T) storetest.Fixture {
				f := newVirtual(t)
				b.fix(&f)
				return f
			})
		})
	}
}

// TestATSTO35FixtureChecks: AT-STO-35 (steps written from the text of issue #38). With each of
// the five bad fixtures (nil Store, invalid Timing, negative Slack, a Peer that returns nil, and
// a real-time LeaseDuration over 10s) every case fails, logging exactly the fixture's message and
// nothing else.
func TestATSTO35FixtureChecks(t *testing.T) {
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
}

// keepAlive is one KeepAlive call as the recorder saw it.
type keepAlive struct {
	holder string
	at     time.Duration // the virtual clock's monotonic reading at the call
}

// recorder is a tenure.Store that forwards to a memstore and records its KeepAlive calls.
type recorder struct {
	tenure.Store
	clock *testClock

	mu    sync.Mutex
	calls []keepAlive
}

func (r *recorder) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	r.mu.Lock()
	r.calls = append(r.calls, keepAlive{holder, r.clock.Monotonic()})
	r.mu.Unlock()
	return r.Store.KeepAlive(ctx, holder, timing)
}

// TestATSTO36VirtualTime: AT-STO-36 (steps written from the text of issue #38). On the virtual
// fixture, Session/RenewalKeepsRecords takes under 5s of real time although it lets twice the
// 15s LeaseDuration pass, and after its opens its renewals arrive after steps of exactly
// RetryPeriod, h1 before h2 at each step.
func TestATSTO36VirtualTime(t *testing.T) {
	const target = "Session/RenewalKeepsRecords"
	var (
		rec     *recorder
		elapsed time.Duration
	)
	t.Run("suite", func(t *testing.T) {
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			if !strings.HasSuffix(t.Name(), "/"+target) {
				return newVirtual(t)
			}
			clock := newTestClock()
			mem := memstore.New(clock)
			rec = &recorder{Store: mem, clock: clock}
			start := time.Now()
			t.Cleanup(func() { elapsed = time.Since(start) })
			return virtualFixture(clock, rec, mem)
		})
	})
	if rec == nil {
		t.Fatalf("%s did not run", target)
	}
	if elapsed >= 5*time.Second {
		t.Errorf("%s took %v of real time, want under 5s", target, elapsed)
	}

	h1, h2 := holder(1, 1), holder(2, 1)
	timing := core.DefaultTiming
	var want []keepAlive
	for at := timing.RetryPeriod; at <= 2*timing.LeaseDuration; at += timing.RetryPeriod {
		want = append(want, keepAlive{h1, at}, keepAlive{h2, at})
	}
	var renewals []keepAlive
	opened := make(map[string]bool)
	for _, c := range rec.calls {
		if opened[c.holder] {
			renewals = append(renewals, c)
		}
		opened[c.holder] = true
	}
	if !slices.Equal(renewals, want) {
		t.Errorf("renewals of %s:\n  %v\nwant:\n  %v", target, renewals, want)
	}
}

// holder returns incarnation k of member i, as the suite names holders.
func holder(i, k int) string { return fmt.Sprintf("m%d/%032x", i, k) }

// msgs returns the messages of log lines.
func msgs(logs []childLog) []string {
	var out []string
	for _, l := range logs {
		out = append(out, l.msg)
	}
	return out
}
