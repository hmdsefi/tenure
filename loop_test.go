// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// The tests of a node with a store and no spaces: Start, the loop, the session, Drain and Close.
// Steps carrying an AT-RUN number were written from the issue's text.

// rig is one node's doubles: fs over a memstore and sm for the absent member n1 of a group that
// lists n2.
type rig struct {
	ms *memstore.Store
	fs *recStore
	g  *group
	sm *recMembership
}

// newRig returns a rig.
func newRig(t *testing.T) rig {
	t.Helper()
	ms := memstore.New(nil)
	g := newGroup(t, tenure.Member{ID: "n2", Addr: "a2"})
	return rig{ms: ms, fs: newRecStore(ms), g: g, sm: g.Absent("n1")}
}

// config returns the Config of member n1 over r's doubles with the counting Rand.
func (r rig) config() tenure.Config {
	return tenure.Config{
		Self:       tenure.Member{ID: "n1", Addr: "a1"},
		Membership: r.sm,
		Store:      r.fs,
		Rand:       countingRand(),
	}
}

// start starts cfg with ctx, failing t on error.
func start(t *testing.T, ctx context.Context, cfg tenure.Config) *tenure.Node {
	t.Helper()
	n, err := tenure.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	return n
}

// wantSessionClosed fails t unless the memstore's session of holder01 has ended.
func (r rig) wantSessionClosed(t *testing.T) {
	t.Helper()
	if err := r.ms.KeepAlive(context.Background(), holder01, core.DefaultTiming); !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("memstore KeepAlive(%s) = %v, want ErrSessionLost: the session is still open", holder01, err)
	}
}

// stepLog records the steps of a node through WithStepHook.
type stepLog struct {
	mu   sync.Mutex
	recs []tenure.StepRecord
	seqs []uint64 // a seq stamp per record, taken in the hook
}

// ctx returns ctx with the log's hook.
func (l *stepLog) ctx(ctx context.Context) context.Context {
	return tenure.WithStepHook(ctx, func(r tenure.StepRecord) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.recs = append(l.recs, r)
		l.seqs = append(l.seqs, seq())
	})
}

// records returns a copy of the records and their stamps.
func (l *stepLog) records() ([]tenure.StepRecord, []uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.recs), slices.Clone(l.seqs)
}

// wakeOf returns the reading of the Wake among acts, and false when there is none.
func wakeOf(acts []core.Action) (time.Duration, bool) {
	for _, a := range acts {
		if w, ok := a.(core.Wake); ok {
			return w.At, true
		}
	}
	return 0, false
}

// wantDeadlines fails t unless every call in calls has a deadline RetryPeriod after its start.
func wantDeadlines(t *testing.T, calls []call) {
	t.Helper()
	for _, c := range calls {
		if !c.HasDeadline || c.Deadline.Sub(c.Start) != core.DefaultTiming.RetryPeriod {
			t.Errorf("%v has deadline %v after its start (set %t), want %v", c, c.Deadline.Sub(c.Start), c.HasDeadline,
				core.DefaultTiming.RetryPeriod)
		}
	}
}

// secs returns the durations of ss seconds each.
func secs(ss ...float64) []time.Duration {
	out := make([]time.Duration, len(ss))
	for i, s := range ss {
		out[i] = time.Duration(s * float64(time.Second))
	}
	return out
}

// TestATRUN01ValidConfigStarts starts valid configs with a store: each opens its session,
// publishes its first view, and drains, closes the session and leaves at Close.
func TestATRUN01ValidConfigStarts(t *testing.T) {
	custom := core.Timing{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 4 * time.Second}
	applied := custom
	applied.SlotGrace = tenure.DefaultSlotGrace
	tests := []struct {
		name   string
		mutate func(c *tenure.Config, r rig)
		timing core.Timing // KeepAlive's
		slots  bool        // Start reads the slots
		seeds  bool        // Start seeds the slot count
	}{
		{"store and membership", func(*tenure.Config, rig) {}, core.DefaultTiming, true, false},
		{"store without slots", func(c *tenure.Config, r rig) { c.Store = r.fs.StoreOnly() }, core.DefaultTiming, false, false},
		{"Slot and Draining ignored", func(c *tenure.Config, _ rig) {
			c.Self.Slot, c.Self.Draining = 5, true
		}, core.DefaultTiming, true, false},
		{"Meta keys that are not reserved", func(c *tenure.Config, _ rig) {
			c.Self.Meta = map[string]string{"tenure": "", "Tenure.x": "", "zone": "z1"}
		}, core.DefaultTiming, true, false},
		{"explicit Timing", func(c *tenure.Config, _ rig) { c.Timing = custom }, applied, true, false},
		{"slot count seeded", func(c *tenure.Config, _ rig) { c.Slots = 2 }, core.DefaultTiming, true, true},
		{"checkers accept", func(c *tenure.Config, r rig) {
			c.Store, c.Membership = r.fs.Checking(nil, nil), r.sm.Checking(nil, nil)
		}, core.DefaultTiming, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newRig(t)
				cfg := r.config()
				tt.mutate(&cfg, r)
				n := start(t, context.Background(), cfg)

				var wantStore []string
				switch {
				case tt.seeds:
					wantStore = []string{"Slots", "SetSlots", "Slots", "KeepAlive"}
				case tt.slots:
					wantStore = []string{"Slots", "KeepAlive"}
				default:
					wantStore = []string{"KeepAlive"}
				}
				fsCalls := withoutRefreshes(r.fs.Calls("Slots", "SetSlots", "KeepAlive", "CloseSession"))
				if got := callMethods(fsCalls); !slices.Equal(got, wantStore) {
					t.Errorf("store calls after Start = %v, want %v", got, wantStore)
				}
				ka := r.fs.Calls("KeepAlive")
				if len(ka) != 1 || ka[0].Args[0] != holder01 || ka[0].Args[1] != tt.timing || ka[0].Err != nil {
					t.Errorf("KeepAlive calls = %v, want KeepAlive(%s, %v) = nil", ka, holder01, tt.timing)
				}
				joins := r.sm.Calls("Join")
				if len(joins) != 1 {
					t.Fatalf("Join calls = %v, want one", joins)
				}
				if m := argAt[core.Member](joins[0].Args, 0); m.ID != "n1" || m.Slot != -1 || m.Draining {
					t.Errorf("Join(%+v), want n1 with Slot -1 and Draining false", m)
				}
				if got, want := memberIDs(published(t, n)), []string{"n1", "n2"}; !slices.Equal(got, want) {
					t.Errorf("published view has %v, want %v", got, want)
				}

				closeNode(t, n)
				wantStore = append(wantStore, "CloseSession")
				if got := callMethods(withoutRefreshes(r.fs.Calls("Slots", "SetSlots", "KeepAlive", "CloseSession"))); !slices.Equal(got, wantStore) {
					t.Errorf("store calls after Close = %v, want %v", got, wantStore)
				}
				if got, want := callMethods(r.sm.Calls("Join", "Watch", "SetDraining", "Leave")),
					[]string{"Join", "Watch", "SetDraining", "Leave"}; !slices.Equal(got, want) {
					t.Errorf("membership calls = %v, want %v", got, want)
				}
				r.wantSessionClosed(t)
				if got, want := memberIDs(r.g.View()), []string{"n2"}; !slices.Equal(got, want) {
					t.Errorf("group after Close has %v, want %v", got, want)
				}
				check()
			})
		})
	}

	t.Run("with spaces", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			var runs runLog
			cfg := r.config()
			cfg.Spaces = []tenure.Space{orders(runs.Run)}
			n := start(t, context.Background(), cfg)
			synctest.Wait()
			var got []int
			for _, c := range runs.Calls() {
				got = append(got, c.Shard.ID)
			}
			slices.Sort(got)
			if want := bal("orders", 8, "n1", "n2")["n1"]; !slices.Equal(got, want) {
				t.Errorf("runs of shards %v, want %v", got, want)
			}
			closeNode(t, n)
			if got, want := len(r.fs.Calls("Release")), len(runs.Calls()); got != want {
				t.Errorf("%d Release calls, want %d", got, want)
			}
			r.wantSessionClosed(t)
			check()
		})
	})
}

// TestATRUN03Holder checks the holder string: Self.ID, "/" and the 16 drawn bytes in lowercase
// hex, used for the session's calls; and a new holder for each Start.
func TestATRUN03Holder(t *testing.T) {
	t.Run("a: from the drawn bytes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			n := start(t, context.Background(), r.config())
			if got := n.Holder(); got != holder01 {
				t.Errorf("Holder() = %q, want %q", got, holder01)
			}
			time.Sleep(3 * time.Second)
			closeNode(t, n)
			for _, c := range r.fs.Calls("KeepAlive", "CloseSession") {
				if c.Args[0] != holder01 {
					t.Errorf("%v, want holder %s", c, holder01)
				}
			}
			if got := len(r.fs.Calls("KeepAlive")); got != 2 {
				t.Errorf("%d KeepAlive calls, want 2 (T0 and T0+2s)", got)
			}
			check()
		})
	})

	t.Run("b: a new holder per Start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			pattern := regexp.MustCompile(`^n1/[0-9a-f]{32}$`)
			var holders []string
			for range 2 {
				cfg := r.config()
				cfg.Rand = nil // crypto/rand.Reader
				cfg.Membership = r.g.Absent("n1")
				n := start(t, context.Background(), cfg)
				holders = append(holders, n.Holder())
				closeNode(t, n)
			}
			for _, h := range holders {
				if !pattern.MatchString(h) {
					t.Errorf("holder %q does not match %v", h, pattern)
				}
			}
			if holders[0] == holders[1] {
				t.Errorf("both Starts drew holder %q", holders[0])
			}
			ka := r.fs.Calls("KeepAlive")
			cs := r.fs.Calls("CloseSession")
			if len(ka) != 2 || len(cs) != 2 {
				t.Fatalf("KeepAlive calls %v and CloseSession calls %v, want two each", ka, cs)
			}
			for i, h := range holders {
				if ka[i].Args[0] != h || cs[i].Args[0] != h {
					t.Errorf("Start %d: %v and %v, want holder %s", i, ka[i], cs[i], h)
				}
			}
			check()
		})
	})
}

// TestATRUN04StartupOrder checks Start's calls and the loop's first steps: Join, Watch, Slots and
// KeepAlive, then SessionRenewed with KeepAlive's SentAt and ViewChanged with the first view, all
// before Start returns. Right after Start returns, Route already names the member alone in its
// views for every key.
func TestATRUN04StartupOrder(t *testing.T) {
	t.Run("order", testStartupOrder)
	t.Run("route", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, context.Background(), r.config())
			for i := range 100 {
				wantRoute(t, n, "orders", "key-"+strconv.Itoa(i), "n1")
			}
			closeNode(t, n)
			check()
		})
	})
}

// testStartupOrder is AT-RUN-04's order of Start's calls and of the loop's first steps.
func testStartupOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config())
		returned := seq()

		calls := callsInOrder(r.fs.Calls(), r.sm.Calls())
		if got, want := callMethods(calls), []string{"Join", "Watch", "Slots", "KeepAlive"}; !slices.Equal(got, want) {
			t.Fatalf("calls before Start returned = %v, want %v", got, want)
		}
		for _, c := range calls {
			if !c.Done || c.EndSeq > returned {
				t.Errorf("%v did not return before Start", c)
			}
		}
		recs, seqs := log.records()
		if len(recs) != 2 {
			t.Fatalf("steps before Start returned = %+v, want 2", recs)
		}
		for i, s := range seqs {
			if s > returned {
				t.Errorf("step %d was reported after Start returned", i)
			}
		}
		if want := (core.SessionRenewed{}); recs[0].Input != want {
			t.Errorf("first step's input = %#v, want %#v", recs[0].Input, want)
		}
		if want := n.Epoch(); !recs[0].SentWall.Equal(want) || !t0.Equal(want) {
			t.Errorf("first step's SentWall = %v, want the epoch %v", recs[0].SentWall, want)
		}
		v := published(t, n)
		if want := (core.ViewChanged{View: v, Wall: n.Epoch()}); !reflect.DeepEqual(recs[1].Input, want) {
			t.Errorf("second step's input = %#v, want %#v", recs[1].Input, want)
		}
		if got, want := memberIDs(v), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("first view has %v, want %v", got, want)
		}
		if st := recs[1].After; st.Session != core.SessionStateLive || st.Deadline != 10*time.Second || st.ViewVersion != v.Version {
			t.Errorf("Status after the first steps = %+v, want live until 10s with view %d", st, v.Version)
		}
		if got := n.SessionDeadline(); got != 10*time.Second {
			t.Errorf("SessionDeadline() = %v, want 10s", got)
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN05StartFailureCleanup checks Start's cleanup once Join succeeded: Leave after a
// failed Slots read or KeepAlive, and CloseSession, then Leave, once the session is open.
func TestATRUN05StartFailureCleanup(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name  string
		fail  string // the store method that fails
		want  string
		store []string
	}{
		{"a: read slots fails", "Slots", "tenure: read slots: boom", []string{"Slots"}},
		{"b: open session fails", "KeepAlive", "tenure: open session: boom", []string{"Slots", "KeepAlive"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newRig(t)
				r.fs.Fail(tt.fail, boom)
				n, err := tenure.Start(context.Background(), r.config())
				if n != nil || err == nil || err.Error() != tt.want || !errors.Is(err, boom) {
					t.Fatalf("Start() = %v, %v, want nil, %q", n, err, tt.want)
				}
				if got := callMethods(r.fs.Calls()); !slices.Equal(got, tt.store) {
					t.Errorf("store calls = %v, want %v", got, tt.store)
				}
				if got, want := callMethods(r.sm.Calls()), []string{"Join", "Watch", "Leave"}; !slices.Equal(got, want) {
					t.Errorf("membership calls = %v, want %v", got, want)
				}
				wantDeadlines(t, r.sm.Calls("Leave"))
				if got, want := memberIDs(r.g.View()), []string{"n2"}; !slices.Equal(got, want) {
					t.Errorf("group after the failed Start has %v, want %v", got, want)
				}
				check()
			})
		})
	}

	t.Run("c: context ends after the session opened", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			release := make(chan struct{})
			var once sync.Once
			hook := tenure.WithStepHook(context.Background(), func(tenure.StepRecord) {
				once.Do(func() { <-release }) // hold the loop's first step
			})
			ctx, cancel := context.WithTimeout(hook, 5*time.Second)
			defer cancel()
			go func() {
				time.Sleep(6 * time.Second)
				close(release)
			}()
			t0 := time.Now()
			n, err := tenure.Start(ctx, r.config())
			const want = "tenure: start: context deadline exceeded"
			if n != nil || err == nil || err.Error() != want || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Start() = %v, %v, want nil, %q", n, err, want)
			}
			if d := time.Since(t0); d != 6*time.Second {
				t.Errorf("Start returned at T0+%v, want T0+6s, once the loop has stopped", d)
			}
			all := withoutRefreshes(callsInOrder(r.fs.Calls(), r.sm.Calls()))
			if got, want := callMethods(all), []string{"Join", "Watch", "Slots", "KeepAlive", "CloseSession", "Leave"}; !slices.Equal(got, want) {
				t.Errorf("calls = %v, want %v", got, want)
			}
			cs := r.fs.Calls("CloseSession")
			wantDeadlines(t, cs)
			wantDeadlines(t, r.sm.Calls("Leave"))
			if len(cs) == 1 && (cs[0].Args[0] != holder01 || cs[0].Start.Sub(t0) != 6*time.Second || cs[0].Err != nil) {
				t.Errorf("%v at T0+%v, want CloseSession(%s) = nil at T0+6s", cs[0], cs[0].Start.Sub(t0), holder01)
			}
			r.wantSessionClosed(t)
			check()
		})
	})
}

// TestATRUN06StartContextScope checks that Start's ctx bounds only Start's own calls: the
// session is renewed every RetryPeriod after it ended, each renewal bounded by RetryPeriod; each
// Acquire is bounded by RetryPeriod; and the runs neither end with it nor have its deadline.
func TestATRUN06StartContextScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		var runs runLog
		cfg := r.config()
		cfg.Spaces = []tenure.Space{orders(runs.Run)}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t0 := time.Now()
		n := start(t, ctx, cfg)
		time.Sleep(9 * time.Second)

		mine := bal("orders", 8, "n1", "n2")["n1"]
		rc := runs.Calls()
		if len(rc) != len(mine) {
			t.Errorf("runs = %v, want one for each of %v", rc, mine)
		}
		for _, c := range rc {
			if !slices.Contains(mine, c.Shard.ID) || c.HasDeadline || !c.CtxDone.IsZero() || !c.Shard.Valid() {
				t.Errorf("%v: deadline set %t, ctx done at %v, Valid %t; want a run of n1 with no deadline that goes on",
					c, c.HasDeadline, c.CtxDone, c.Shard.Valid())
			}
		}
		wantDeadlines(t, r.fs.Calls("Acquire"))

		ka := r.fs.Calls("KeepAlive")
		if got, want := startsAt(ka, t0), secs(0, 2, 4, 6, 8); !slices.Equal(got, want) {
			t.Fatalf("KeepAlive calls at %v, want %v", got, want)
		}
		if !ka[0].HasDeadline || !ka[0].Deadline.Equal(t0.Add(5*time.Second)) {
			t.Errorf("Start's KeepAlive has deadline %v, want Start's ctx's %v", ka[0].Deadline, t0.Add(5*time.Second))
		}
		wantDeadlines(t, ka[1:])
		for _, c := range ka {
			if c.Err != nil {
				t.Errorf("%v, want nil", c)
			}
		}
		sl := r.fs.Calls("Slots")
		if len(sl) == 0 || !sl[0].Deadline.Equal(t0.Add(5*time.Second)) {
			t.Errorf("Slots calls = %v, want Start's first, with Start's ctx", sl)
		}
		if got, want := startsAt(sl, t0), secs(0, 2, 4, 6, 8); !slices.Equal(got, want) {
			t.Errorf("Slots calls at %v, want Start's and then one every RetryPeriod", got)
		} else {
			wantDeadlines(t, sl[1:])
		}
		if got := n.SessionDeadline(); got != 18*time.Second {
			t.Errorf("SessionDeadline() = %v, want 18s from the renewal at T0+8s", got)
		}
		closeNode(t, n)
		wantDeadlines(t, r.fs.Calls("CloseSession"))
		check()
	})
}

// TestATRUN13OneRenewalInFlight checks that at most one KeepAlive is in flight and one more
// queued: renewals the machine asks for while one blocks are coalesced, and the queued one goes
// out as soon as the blocked one returns.
func TestATRUN13OneRenewalInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config())
		at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }

		release := r.fs.Block("KeepAlive")
		at(7 * time.Second)
		release()
		at(9500 * time.Millisecond)

		ka := r.fs.Calls("KeepAlive")
		if got, want := startsAt(ka, t0), secs(0, 2, 7, 8); !slices.Equal(got, want) {
			t.Fatalf("KeepAlive calls at %v, want %v", got, want)
		}
		for i := 1; i < len(ka); i++ {
			if ka[i].Seq < ka[i-1].EndSeq {
				t.Errorf("%v started while %v was in flight", ka[i], ka[i-1])
			}
		}
		if c := ka[1]; c.End.Sub(t0) != 7*time.Second || !errors.Is(c.Err, context.DeadlineExceeded) {
			t.Errorf("blocked call %v ended at T0+%v, want context.DeadlineExceeded at T0+7s", c, c.End.Sub(t0))
		}
		recs, _ := log.records()
		var keepAlives int
		for _, rec := range recs {
			for _, a := range rec.Actions {
				if _, ok := a.(core.KeepAlive); ok {
					keepAlives++
				}
			}
		}
		if keepAlives != 4 {
			t.Errorf("machine asked for %d KeepAlives by T0+9.5s, want 4 (at 2, 4, 6 and 8 s); the one at 6 s was dropped", keepAlives)
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN14CallDeadlines checks the contexts of the loop's store calls: each is bounded by
// RetryPeriod from its start, and a call that outlives it answers SessionFailed with its error.
func TestATRUN14CallDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config())
		at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }

		at(5 * time.Second)
		r.fs.Delay("KeepAlive", 3*time.Second)
		at(6500 * time.Millisecond)
		r.fs.Delay("KeepAlive", 0)
		at(10500 * time.Millisecond)
		closeNode(t, n)

		ka := r.fs.Calls("KeepAlive")
		if got, want := startsAt(ka, t0), secs(0, 2, 4, 6, 9, 10); !slices.Equal(got, want) {
			t.Fatalf("KeepAlive calls at %v, want %v", got, want)
		}
		if ka[0].HasDeadline {
			t.Errorf("Start's KeepAlive has a deadline, want Start's ctx, which has none")
		}
		wantDeadlines(t, ka[1:])
		for i, c := range ka {
			if late := i == 3; late != errors.Is(c.Err, context.DeadlineExceeded) || !late && c.Err != nil {
				t.Errorf("%v, want context.DeadlineExceeded only for the call at T0+6s", c)
			}
		}
		cs := r.fs.Calls("CloseSession")
		if len(cs) != 1 {
			t.Fatalf("CloseSession calls = %v, want one", cs)
		}
		wantDeadlines(t, cs)
		wantDeadlines(t, r.sm.Calls("SetDraining", "Leave"))

		recs, _ := log.records()
		var failed []tenure.StepRecord
		for _, rec := range recs {
			if _, ok := rec.Input.(core.SessionFailed); ok {
				failed = append(failed, rec)
			}
		}
		want := core.SessionFailed{Now: 9 * time.Second, Cause: core.CauseUnreachable}
		if len(failed) != 1 || failed[0].Input != want || !errors.Is(failed[0].Err, context.DeadlineExceeded) ||
			!failed[0].SentWall.Equal(t0.Add(6*time.Second)) {
			t.Errorf("SessionFailed steps = %+v, want one %+v with Err DeadlineExceeded and SentWall T0+6s", failed, want)
		}
		check()
	})
}

// TestATRUN16CloseSessionRetry checks the drain's CloseSession: a failed call is repeated every
// RetryPeriod until one succeeds, and a lost or unknown session counts as closed.
func TestATRUN16CloseSessionRetry(t *testing.T) {
	t.Run("c: retried until it succeeds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			t0 := time.Now()
			n := start(t, context.Background(), r.config())
			at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }

			at(time.Second)
			r.fs.Fail("CloseSession", errors.New("boom"))
			done := make(chan time.Duration, 1)
			go func() {
				if err := n.Drain(context.Background()); err != nil {
					t.Errorf("Drain() = %v, want nil", err)
				}
				done <- time.Since(t0)
			}()
			at(6 * time.Second)
			if len(done) != 0 {
				t.Fatal("Drain returned while CloseSession failed")
			}
			r.fs.Fail("CloseSession", nil)
			if d := <-done; d != 7*time.Second {
				t.Errorf("Drain returned at T0+%v, want T0+7s", d)
			}
			cs := r.fs.Calls("CloseSession")
			if got, want := startsAt(cs, t0), secs(1, 3, 5, 7); !slices.Equal(got, want) {
				t.Errorf("CloseSession calls at %v, want %v", got, want)
			}
			wantDeadlines(t, cs)
			for _, c := range r.fs.Calls("KeepAlive") {
				if c.Seq > cs[0].Seq {
					t.Errorf("%v after the first CloseSession", c)
				}
			}
			r.wantSessionClosed(t)
			closeNode(t, n)
			if got := len(r.fs.Calls("CloseSession")); got != 4 {
				t.Errorf("%d CloseSession calls after Close, want 4", got)
			}
			check()
		})
	})

	t.Run("c: a lost session is closed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			t0 := time.Now()
			n := start(t, context.Background(), r.config())
			r.fs.Answer("CloseSession", func(context.Context, []any) ([]any, error) {
				return nil, tenure.ErrSessionLost
			})
			time.Sleep(time.Second)
			closeNode(t, n)
			if d := time.Since(t0); d != time.Second {
				t.Errorf("Close returned at T0+%v, want T0+1s", d)
			}
			if cs := r.fs.Calls("CloseSession"); len(cs) != 1 {
				t.Errorf("CloseSession calls = %v, want one", cs)
			}
			check()
		})
	})
}

// TestATRUN76SetDrainingRetries checks a drain whose SetDraining fails: it is retried every
// RetryPeriod, and the drain completes once it succeeds, after the session has been closed.
func TestATRUN76SetDrainingRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		t0 := time.Now()
		n := start(t, context.Background(), r.config())
		at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }

		at(time.Second)
		r.sm.Fail("SetDraining", errors.New("boom"))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := n.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Drain(1s ctx) = %v, want context.DeadlineExceeded", err)
		}
		if cs := r.fs.Calls("CloseSession"); len(cs) != 1 || cs[0].Start.Sub(t0) != time.Second || cs[0].Err != nil {
			t.Errorf("CloseSession calls = %v, want one at T0+1s", cs)
		}
		at(4 * time.Second)
		r.sm.Fail("SetDraining", nil)
		if err := n.Drain(context.Background()); err != nil {
			t.Errorf("Drain() = %v, want nil", err)
		}
		if d := time.Since(t0); d != 5*time.Second {
			t.Errorf("Drain returned at T0+%v, want T0+5s", d)
		}
		sd := r.sm.Calls("SetDraining")
		if got, want := startsAt(sd, t0), secs(1, 3, 5); !slices.Equal(got, want) {
			t.Errorf("SetDraining calls at %v, want %v", got, want)
		}
		wantDeadlines(t, sd)
		if ka := r.fs.Calls("KeepAlive"); len(ka) != 1 {
			t.Errorf("KeepAlive calls = %v, want only Start's: the session closed at T0+1s", ka)
		}
		closeNode(t, n)
		// Drain posts Drain{} to the loop and makes its first SetDraining attempt at the same
		// instant, so that attempt and CloseSession, both at T0+1s, may come in either order.
		// Each double's own order is fixed, and so is the order across them at other instants.
		// The slot refresher's Slots calls after KeepAlive are left out.
		if got, want := callMethods(withoutRefreshes(r.fs.Calls())), []string{"Slots", "KeepAlive", "CloseSession"}; !slices.Equal(got, want) {
			t.Errorf("store calls = %v, want %v", got, want)
		}
		if got, want := callMethods(r.sm.Calls()), []string{"Join", "Watch", "SetDraining", "SetDraining", "SetDraining", "Leave"}; !slices.Equal(got, want) {
			t.Errorf("membership calls = %v, want %v", got, want)
		}
		all := callMethods(withoutRefreshes(callsInOrder(r.fs.Calls(), r.sm.Calls())))
		if i := slices.Index(all, "KeepAlive"); i < 0 || slices.Index(all, "SetDraining") < i || slices.Index(all, "CloseSession") < i {
			t.Errorf("calls = %v, want SetDraining and CloseSession after KeepAlive", all)
		}
		if all[len(all)-1] != "Leave" {
			t.Errorf("calls = %v, want Leave last", all)
		}
		check()
	})
}

// TestATRUN93IdleNode checks that an idle node keeps its session for an hour on the machine's
// cadence, and that Start and Close leave no goroutine behind, ten times over.
func TestATRUN93IdleNode(t *testing.T) {
	t.Run("an hour", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newRig(t)
			n := start(t, context.Background(), r.config())
			time.Sleep(time.Hour)
			synctest.Wait() // the renewal due at T0+1h
			ka := r.fs.Calls("KeepAlive")
			if len(ka) < 1800 {
				t.Errorf("%d KeepAlive calls in an hour, want at least 1800", len(ka))
			}
			for _, c := range ka {
				if c.Err != nil {
					t.Fatalf("%v, want nil", c)
				}
			}
			if got, want := n.SessionDeadline(), time.Hour+10*time.Second; got != want {
				t.Errorf("SessionDeadline() = %v, want %v", got, want)
			}
			closeNode(t, n)
			r.wantSessionClosed(t)
			check()
		})
	})

	t.Run("ten cycles", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := newRig(t)
			for i := range 10 {
				check := baseline(t)
				cfg := r.config()
				cfg.Rand = nil
				cfg.Membership = r.g.Absent("n1")
				n := start(t, context.Background(), cfg)
				time.Sleep(3 * time.Second)
				closeNode(t, n)
				check()
				if t.Failed() {
					t.Fatalf("cycle %d left goroutines behind", i)
				}
			}
			if got := len(r.fs.Calls("CloseSession")); got != 10 {
				t.Errorf("%d CloseSession calls, want 10", got)
			}
		})
	})
}

// TestStepRecords checks the records of a node's steps through renewals, a failed renewal, view
// changes, a drain and a close.
func TestStepRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config())
		at := func(d time.Duration) { time.Sleep(time.Until(t0.Add(d))) }

		at(time.Second)
		r.g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		at(3 * time.Second)
		boom := errors.New("boom")
		r.fs.Fail("KeepAlive", boom)
		at(5 * time.Second)
		r.fs.Fail("KeepAlive", nil)
		at(7500 * time.Millisecond)
		r.g.Leave("n2")
		at(19500 * time.Millisecond)
		closeNode(t, n)
		last := published(t, n)

		recs, _ := log.records()
		if len(recs) < 10 {
			t.Fatalf("%d steps, want more", len(recs))
		}
		if want := (core.Status{Session: core.SessionStateOpening, Slot: -1, Spaces: []core.SpaceStatus{}}); !reflect.DeepEqual(recs[0].Before, want) {
			t.Errorf("first step's Before = %+v, want %+v", recs[0].Before, want)
		}
		var (
			wake     time.Duration
			waking   bool
			views    []core.View // stepped before the session closed
			stepped  core.View   // the last view stepped
			closed   = -1        // the SessionClosed step
			renewed  int
			failed   int
			deadline time.Duration
		)
		for i, rec := range recs {
			if rec.Gen != 1 {
				t.Errorf("step %d: Gen %d, want 1", i, rec.Gen)
			}
			if i > 0 {
				if rec.Now < recs[i-1].Now {
					t.Errorf("step %d: Now %v after %v", i, rec.Now, recs[i-1].Now)
				}
				if !reflect.DeepEqual(rec.Before, recs[i-1].After) {
					t.Errorf("step %d: Before %+v, want the previous After %+v", i, rec.Before, recs[i-1].After)
				}
			}
			if waking && rec.Now >= wake {
				if want := (core.Tick{Now: wake}); rec.Input != want {
					t.Errorf("step %d: %#v at %v, want %#v first", i, rec.Input, rec.Now, want)
				}
			}
			wake, waking = wakeOf(rec.Actions)
			if rec.After.Now != rec.Now {
				t.Errorf("step %d: Status().Now %v, want the input's %v", i, rec.After.Now, rec.Now)
			}
			for _, a := range rec.Actions {
				if e, ok := a.(core.ExtendDeadline); ok {
					deadline = e.Deadline
				}
			}
			switch in := rec.Input.(type) {
			case core.ViewChanged:
				if in.Now != rec.Now || !in.Wall.Equal(n.Epoch().Add(rec.Now)) {
					t.Errorf("step %d: ViewChanged at %v with Wall %v, want Wall %v", i, in.Now, in.Wall, n.Epoch().Add(rec.Now))
				}
				stepped = in.View
				if closed < 0 {
					views = append(views, in.View)
				}
			case core.SessionClosed:
				closed = i
				if !rec.After.Done || rec.After.Session != core.SessionStateClosed {
					t.Errorf("step %d: SessionClosed leaves %+v, want the machine closed and done", i, rec.After)
				}
			case core.SessionRenewed:
				renewed++
				if !rec.SentWall.Equal(n.Epoch().Add(in.SentAt)) || rec.Err != nil {
					t.Errorf("step %d: SessionRenewed with SentWall %v and Err %v, want %v and nil",
						i, rec.SentWall, rec.Err, n.Epoch().Add(in.SentAt))
				}
			case core.SessionFailed:
				failed++
				if in.Cause != core.CauseUnreachable || !errors.Is(rec.Err, boom) || !rec.SentWall.Equal(t0.Add(4*time.Second)) {
					t.Errorf("step %d: %+v with Err %v and SentWall %v, want unreachable, boom and T0+4s", i, in, rec.Err, rec.SentWall)
				}
			}
		}
		if failed != 1 {
			t.Errorf("%d SessionFailed steps, want 1 (the renewal at T0+4s)", failed)
		}
		if renewed < 9 {
			t.Errorf("%d SessionRenewed steps, want at least 9", renewed)
		}
		if got := [][]string{memberIDs(views[0]), memberIDs(views[1]), memberIDs(views[len(views)-1])}; !reflect.DeepEqual(got,
			[][]string{{"n1", "n2"}, {"n1", "n2", "n3"}, {"n1", "n3"}}) {
			t.Errorf("stepped views have %v, want [n1 n2], [n1 n2 n3] and last [n1 n3]", got)
		}
		if !reflect.DeepEqual(last, stepped) {
			t.Errorf("published view %+v, want the last stepped %+v", last, stepped)
		}
		if got := n.SessionDeadline(); got != deadline || got != 28*time.Second {
			t.Errorf("SessionDeadline() = %v, want the last ExtendDeadline's %v, 28s", got, deadline)
		}
		if closed < 0 {
			t.Fatal("no SessionClosed step")
		}
		// Close's Leave may still send a view; a closed machine changes nothing for it.
		for i, rec := range recs[closed+1:] {
			if rec.Actions != nil || !reflect.DeepEqual(rec.Before, rec.After) {
				t.Errorf("step %d after the close: %+v, want no action and no change", closed+1+i, rec)
			}
		}
		var drained bool
		for _, rec := range recs {
			if _, ok := rec.Input.(core.Drain); ok {
				drained = slices.Contains(rec.Actions, core.Action(core.CloseSession{}))
			}
		}
		if !drained {
			t.Error("no Drain step returned CloseSession")
		}
		check()
	})
}

// TestLoopFeedsResolvers checks that with a store the views the loop steps and publishes reach
// the node's resolvers, and that Close closes them.
func TestLoopFeedsResolvers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newRig(t)
		n := start(t, context.Background(), r.config())
		res := n.Resolver("q")
		if got, want := listIDs(res.Members()), []string{"n1", "n2"}; !slices.Equal(got, want) {
			t.Errorf("Members() at once = %v, want %v", got, want)
		}
		r.g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		synctest.Wait()
		want := core.Eligible(published(t, n), time.Now())
		if got := res.Members(); !reflect.DeepEqual(got, want) || !slices.Equal(listIDs(got), []string{"n1", "n2", "n3"}) {
			t.Errorf("Members() after n3 joined = %+v, want %+v", got, want)
		}
		closeNode(t, n)
		if _, err := res.Lookup("k"); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Lookup after Close = %v, want ErrClosed", err)
		}
		check()
	})
}
