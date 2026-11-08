// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of a node without a store. Steps carrying an AT-RUN number were written from the
// issue's text.

// holder01 is the holder of member n1 whose incarnation is the bytes 0 to 15.
const holder01 = "n1/000102030405060708090a0b0c0d0e0f"

// countingRand returns a reader of the bytes 0, 1, 2, … 15.
func countingRand() io.Reader {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(i)
	}
	return bytes.NewReader(b)
}

// memberIDs returns the IDs of v's members, in order.
func memberIDs(v core.View) []string {
	ids := make([]string, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.ID
	}
	return ids
}

// published returns n's published view, failing t when there is none.
func published(t *testing.T, n *tenure.Node) core.View {
	t.Helper()
	v, ok := n.PublishedView()
	if !ok {
		t.Fatal("PublishedView() = none, want a view")
	}
	return v
}

// startN1 starts member n1 over m with the counting Rand, failing t on error.
func startN1(t *testing.T, m tenure.Membership) *tenure.Node {
	t.Helper()
	n, err := tenure.Start(context.Background(), tenure.Config{
		Self:       tenure.Member{ID: "n1", Addr: "a1"},
		Membership: m,
		Rand:       countingRand(),
	})
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	return n
}

// closeNode closes n, failing t unless Close returns nil.
func closeNode(t *testing.T, n *tenure.Node) {
	t.Helper()
	if err := n.Close(context.Background()); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// startsAt returns the start time of each call, relative to t0.
func startsAt(calls []call, t0 time.Time) []time.Duration {
	out := make([]time.Duration, len(calls))
	for i, c := range calls {
		out[i] = c.Start.Sub(t0)
	}
	return out
}

// TestATRUN77MembershipOnlyNode runs a node with neither a store nor spaces through its life: it
// joins, follows the group's views, drains through the membership and leaves.
func TestATRUN77MembershipOnlyNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t, tenure.Member{ID: "n2", Addr: "a2", Slot: -1})
		m := g.Absent("n1")
		t0 := time.Now()
		n := startN1(t, m)
		if d := time.Since(t0); d != 0 {
			t.Errorf("Start took %v, want 0", d)
		}

		v := published(t, n)
		if got, want := memberIDs(v), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("first published view has %v, want %v", got, want)
		}
		if want := g.View().Version; v.Version != want {
			t.Errorf("first published view has Version %d, want %d", v.Version, want)
		}
		for _, mb := range v.Members {
			if mb.Slot != -1 {
				t.Errorf("published member %s has Slot %d, want -1", mb.ID, mb.Slot)
			}
		}

		g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		synctest.Wait()
		if got, want := memberIDs(published(t, n)), []string{"n1", "n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("published view after n3 joined has %v, want %v", got, want)
		}

		if err := n.Drain(context.Background()); err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
		synctest.Wait()
		v = published(t, n)
		if i := slices.Index(memberIDs(v), "n1"); i < 0 || !v.Members[i].Draining {
			t.Errorf("published view after Drain = %+v, want n1 draining", v.Members)
		}

		closeNode(t, n)
		if got, want := callMethods(m.Calls()), []string{"Join", "Watch", "SetDraining", "Leave"}; !slices.Equal(got, want) {
			t.Errorf("membership calls = %v, want %v", got, want)
		}
		if sd := m.Calls("SetDraining"); len(sd) != 1 || sd[0].Args[0] != true {
			t.Errorf("SetDraining calls = %v, want one with true", sd)
		}
		if got, want := memberIDs(g.View()), []string{"n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("group after Close has %v, want %v", got, want)
		}
		check()
	})
}

// TestATRUN01MembershipOnlyStarts checks a membership-only Start: the holder, the epoch, the
// member Join receives, and a Watch that outlives Start's context.
func TestATRUN01MembershipOnlyStarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t, tenure.Member{ID: "n2", Addr: "a2"})
		rm := g.Absent("n1")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t0 := time.Now()
		self := tenure.Member{ID: "n1", Addr: "a1", Meta: map[string]string{"zone": "z1"}, Slot: 5, Draining: true}
		n, err := tenure.Start(ctx, tenure.Config{Self: self, Membership: rm, Rand: countingRand()})
		if err != nil {
			t.Fatalf("Start() = %v, want nil", err)
		}
		if got := n.Holder(); got != holder01 {
			t.Errorf("holder = %q, want %q", got, holder01)
		}
		if got := n.Epoch(); !got.Equal(t0) {
			t.Errorf("epoch = %v, want %v", got, t0)
		}

		joins := rm.Calls("Join")
		if len(joins) != 1 {
			t.Fatalf("Join calls = %v, want one", joins)
		}
		joined := argAt[core.Member](joins[0].Args, 0)
		if joined.ID != "n1" || joined.Addr != "a1" || joined.Meta["zone"] != "z1" || joined.Slot != -1 || joined.Draining {
			t.Errorf("Join(%+v), want n1 at a1 with zone z1, Slot -1 and Draining false", joined)
		}
		watches := rm.Calls("Watch")
		if len(watches) != 1 || watches[0].HasDeadline {
			t.Errorf("Watch calls = %v, want one without a deadline", watches)
		}

		time.Sleep(10 * time.Second) // Start's ctx has ended; the node still follows the group
		g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		synctest.Wait()
		if got, want := memberIDs(published(t, n)), []string{"n1", "n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("published view after Start's ctx ended has %v, want %v", got, want)
		}
		if got := len(rm.Calls("Watch")); got != 1 {
			t.Errorf("%d Watch calls, want 1", got)
		}

		closeNode(t, n)
		check()
	})
}

// TestATRUN03IncarnationDrawFails checks that a failing Rand fails Start before any call.
func TestATRUN03IncarnationDrawFails(t *testing.T) {
	tests := []struct {
		name string
		rand io.Reader
		want string
		is   error
	}{
		{"short", bytes.NewReader(make([]byte, 5)), "tenure: draw incarnation: unexpected EOF", io.ErrUnexpectedEOF},
		{"error", iotest.ErrReader(errors.New("boom")), "tenure: draw incarnation: boom", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				m := newGroup(t).Absent("n1")
				n, err := tenure.Start(context.Background(), tenure.Config{
					Self: tenure.Member{ID: "n1"}, Membership: m, Rand: tt.rand,
				})
				if n != nil || err == nil || err.Error() != tt.want {
					t.Fatalf("Start() = %v, %v, want nil, %q", n, err, tt.want)
				}
				if tt.is != nil && !errors.Is(err, tt.is) {
					t.Errorf("errors.Is(%v, %v) = false, want true", err, tt.is)
				}
				if calls := m.Calls(); len(calls) != 0 {
					t.Errorf("membership calls = %v, want none", calls)
				}
				check()
			})
		})
	}
}

// TestATRUN05JoinFails checks that a failing Join fails Start with no Leave and no goroutine left.
func TestATRUN05JoinFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		boom := errors.New("boom")
		m.Fail("Join", boom)
		n, err := tenure.Start(context.Background(), tenure.Config{
			Self: tenure.Member{ID: "n1"}, Membership: m, Rand: countingRand(),
		})
		const want = "tenure: join: boom"
		if n != nil || err == nil || err.Error() != want {
			t.Fatalf("Start() = %v, %v, want nil, %q", n, err, want)
		}
		if !errors.Is(err, boom) {
			t.Errorf("errors.Is(%v, boom) = false, want true", err)
		}
		if got, want := callMethods(m.Calls()), []string{"Join"}; !slices.Equal(got, want) {
			t.Errorf("membership calls = %v, want %v", got, want)
		}
		check()
	})
}

// TestStartCleanup checks RUN-070's cleanup when Start fails after Join: Leave with a context
// bounded by RetryPeriod, and no goroutine left.
func TestStartCleanup(t *testing.T) {
	tests := []struct {
		name  string
		setup func(m *recMembership)
		want  string
		after time.Duration // when Start returns
	}{
		{"watch fails", func(m *recMembership) { m.Fail("Watch", errors.New("boom")) }, "tenure: watch: boom", 0},
		{"watch closed", func(m *recMembership) { m.Fail("Watch", tenure.ErrClosed) }, "tenure: watch: tenure: closed", 0},
		{"no first view", func(m *recMembership) { m.Pause() }, "tenure: first view: context deadline exceeded", 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				g := newGroup(t, tenure.Member{ID: "n2"})
				m := g.Absent("n1")
				tt.setup(m)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				t0 := time.Now()
				n, err := tenure.Start(ctx, tenure.Config{
					Self: tenure.Member{ID: "n1"}, Membership: m, Rand: countingRand(),
				})
				if n != nil || err == nil || err.Error() != tt.want {
					t.Fatalf("Start() = %v, %v, want nil, %q", n, err, tt.want)
				}
				if d := time.Since(t0); d != tt.after {
					t.Errorf("Start returned after %v, want %v", d, tt.after)
				}
				if got, want := callMethods(m.Calls()), []string{"Join", "Watch", "Leave"}; !slices.Equal(got, want) {
					t.Errorf("membership calls = %v, want %v", got, want)
				}
				leave := m.Calls("Leave")[0]
				if !leave.HasDeadline || leave.Deadline.Sub(leave.Start) != core.DefaultTiming.RetryPeriod || leave.Err != nil {
					t.Errorf("Leave = %v with deadline %v after its start, want nil within %v",
						leave, leave.Deadline.Sub(leave.Start), core.DefaultTiming.RetryPeriod)
				}
				if got, want := memberIDs(g.View()), []string{"n2"}; !slices.Equal(got, want) {
					t.Errorf("group after the failed Start has %v, want %v", got, want)
				}
				check()
			})
		})
	}
}

// TestStartWaitsForValidView checks that Start returns at the first merged view that validates,
// and that the watcher drops invalid views and views whose Version is not above the published one.
func TestStartWaitsForValidView(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		views := make(chan core.View, 1)
		m.Answer("Watch", func(context.Context, []any) ([]any, error) {
			return []any{(<-chan core.View)(views)}, nil
		})
		n1, n2, n3 := core.Member{ID: "n1"}, core.Member{ID: "n2"}, core.Member{ID: "n3"}
		started := make(chan *tenure.Node, 1)
		go func() { started <- startN1(t, m) }()

		views <- core.View{Version: 1, Members: []core.Member{n2, n1}} // not sorted
		synctest.Wait()
		if len(started) != 0 {
			t.Fatal("Start returned at an invalid view")
		}
		views <- core.View{Version: 2, Members: []core.Member{n1, n2}}
		n := <-started
		if v := published(t, n); v.Version != 2 || !slices.Equal(memberIDs(v), []string{"n1", "n2"}) {
			t.Errorf("first published view = %+v, want Version 2 with n1 n2", v)
		}

		views <- core.View{Version: 2, Members: []core.Member{n1}} // not newer
		views <- core.View{Version: 3, Members: []core.Member{n3, n1}}
		synctest.Wait()
		if v := published(t, n); v.Version != 2 {
			t.Errorf("published view = %+v, want Version 2 still", v)
		}
		views <- core.View{Version: 4, Members: []core.Member{n1, n3}}
		synctest.Wait()
		if v := published(t, n); v.Version != 4 || !slices.Equal(memberIDs(v), []string{"n1", "n3"}) {
			t.Errorf("published view = %+v, want Version 4 with n1 n3", v)
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN20WatchRetry checks the watcher: a closed channel and a failing Watch are retried
// RetryPeriod later, the view after a gap is the latest one, and no Watch follows Close's Leave.
func TestATRUN20WatchRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t, tenure.Member{ID: "n2"})
		m := g.Absent("n1")
		t0 := time.Now()
		n := startN1(t, m)
		at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }

		at(10 * time.Second)
		m.CloseWatch(0)
		at(13 * time.Second)
		if got, want := startsAt(m.Calls("Watch"), t0), []time.Duration{0, 12 * time.Second}; !slices.Equal(got, want) {
			t.Fatalf("Watch calls at %v, want %v", got, want)
		}

		boom := errors.New("boom")
		m.Fail("Watch", boom)
		at(20 * time.Second)
		m.CloseWatch(1)
		at(21 * time.Second)
		g.Join(tenure.Member{ID: "n3"}) // while the node watches nothing
		at(27 * time.Second)
		if got, want := memberIDs(published(t, n)), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("published view without a watch has %v, want %v", got, want)
		}
		m.Fail("Watch", nil)
		at(29 * time.Second)
		watches := m.Calls("Watch")
		want := []time.Duration{0, 12, 22, 24, 26, 28}
		for i := range want {
			want[i] *= time.Second
		}
		if got := startsAt(watches, t0); !slices.Equal(got, want) {
			t.Fatalf("Watch calls at %v, want %v", got, want)
		}
		for i, c := range watches {
			if failed := i >= 2 && i <= 4; failed != (c.Err != nil) {
				t.Errorf("Watch call %d = %v, want failed %t", i, c, failed)
			}
		}
		if got, want := memberIDs(published(t, n)), []string{"n1", "n2", "n3"}; !slices.Equal(got, want) {
			t.Errorf("published view after the new Watch has %v, want %v", got, want)
		}

		at(30 * time.Second)
		closeNode(t, n)
		time.Sleep(10 * time.Second)
		leaves := m.Calls("Leave")
		if len(leaves) != 1 {
			t.Fatalf("Leave calls = %v, want one", leaves)
		}
		watches = m.Calls("Watch")
		if len(watches) != len(want) {
			t.Errorf("Watch calls = %v, want none after %v", watches, want[len(want)-1])
		}
		for _, c := range watches {
			if c.Seq > leaves[0].Seq {
				t.Errorf("%v started after Leave", c)
			}
		}
		check()
	})
}

// TestWatcherStopsAtLeave checks that no Watch follows Close's Leave while the watcher waits to
// retry a failed Watch.
func TestWatcherStopsAtLeave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		t0 := time.Now()
		n := startN1(t, m)
		m.Fail("Watch", errors.New("boom"))
		m.CloseWatch(0)
		time.Sleep(3 * time.Second) // Watch failed at T0+2s; the next would be at T0+4s
		closeNode(t, n)
		time.Sleep(10 * time.Second)
		if got, want := startsAt(m.Calls("Watch"), t0), []time.Duration{0, 2 * time.Second}; !slices.Equal(got, want) {
			t.Errorf("Watch calls at %v, want %v", got, want)
		}
		check()
	})
}

// TestATRUN75ConcurrentDrain checks that concurrent Drain calls share one SetDraining.
func TestATRUN75ConcurrentDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		n := startN1(t, m)
		release := m.Block("SetDraining")
		errs := make(chan error, 2)
		for range 2 {
			go func() { errs <- n.Drain(context.Background()) }()
		}
		synctest.Wait()
		if len(errs) != 0 {
			t.Fatal("Drain returned while SetDraining was blocked")
		}
		release()
		for range 2 {
			if err := <-errs; err != nil {
				t.Errorf("Drain() = %v, want nil", err)
			}
		}
		if calls := m.Calls("SetDraining"); len(calls) != 1 {
			t.Errorf("SetDraining calls = %v, want one", calls)
		}
		closeNode(t, n)
		if calls := m.Calls("SetDraining"); len(calls) != 1 {
			t.Errorf("SetDraining calls after Close = %v, want one", calls)
		}
		check()
	})
}

// TestDrainRetriesSetDraining checks that a failed SetDraining is tried again every RetryPeriod,
// each attempt bounded by RetryPeriod, and that Drain returns when one succeeds, or at its ctx.
func TestDrainRetriesSetDraining(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		t0 := time.Now()
		n := startN1(t, m)
		m.Fail("SetDraining", errors.New("boom"))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := n.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Drain(1s ctx) = %v, want context.DeadlineExceeded", err)
		}
		if d := time.Since(t0); d != time.Second {
			t.Errorf("Drain(1s ctx) returned at T0+%v, want T0+1s", d)
		}

		done := make(chan time.Duration, 1)
		go func() {
			if err := n.Drain(context.Background()); err != nil {
				t.Errorf("Drain() = %v, want nil", err)
			}
			done <- time.Since(t0)
		}()
		time.Sleep(2 * time.Second) // T0+3s
		m.Fail("SetDraining", nil)
		if d := <-done; d != 4*time.Second {
			t.Errorf("Drain returned at T0+%v, want T0+4s", d)
		}
		calls := m.Calls("SetDraining")
		if got, want := startsAt(calls, t0), []time.Duration{0, 2 * time.Second, 4 * time.Second}; !slices.Equal(got, want) {
			t.Errorf("SetDraining calls at %v, want %v", got, want)
		}
		for _, c := range calls {
			if !c.HasDeadline || c.Deadline.Sub(c.Start) != core.DefaultTiming.RetryPeriod {
				t.Errorf("%v has deadline %v after its start, want %v", c, c.Deadline.Sub(c.Start), core.DefaultTiming.RetryPeriod)
			}
		}
		closeNode(t, n)
		check()
	})
}

// TestCloseTwice checks that Close leaves once and that later calls return ErrClosed.
func TestCloseTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		n := startN1(t, m)
		closeNode(t, n)
		if err := n.Close(context.Background()); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("second Close() = %v, want ErrClosed", err)
		}
		if err := n.Drain(context.Background()); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Drain() after Close = %v, want ErrClosed", err)
		}
		if got, want := callMethods(m.Calls()), []string{"Join", "Watch", "SetDraining", "Leave"}; !slices.Equal(got, want) {
			t.Errorf("membership calls = %v, want %v", got, want)
		}
		check()
	})
}

// TestCloseContextEnds checks that a Close whose ctx ends before the drain completes stops the
// node without leaving, and that later calls return ErrClosed.
func TestCloseContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t)
		m := g.Absent("n1")
		t0 := time.Now()
		n := startN1(t, m)
		m.Fail("SetDraining", errors.New("boom"))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := n.Close(ctx)
		const want = "tenure: close: context deadline exceeded"
		if err == nil || err.Error() != want || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close(3s ctx) = %v, want %q", err, want)
		}
		if d := time.Since(t0); d != 3*time.Second {
			t.Errorf("Close returned at T0+%v, want T0+3s", d)
		}
		if got, want := callMethods(m.Calls()), []string{"Join", "Watch", "SetDraining", "SetDraining"}; !slices.Equal(got, want) {
			t.Errorf("membership calls = %v, want %v", got, want)
		}
		if got, want := memberIDs(g.View()), []string{"n1"}; !slices.Equal(got, want) {
			t.Errorf("group after Close has %v, want %v", got, want)
		}
		if err := n.Close(context.Background()); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("second Close() = %v, want ErrClosed", err)
		}
		if err := n.Drain(context.Background()); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Drain() after Close = %v, want ErrClosed", err)
		}
		check()
	})
}
