// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// checker is the shape of the CheckNames and CheckTiming methods of the Checking variants.
type checker interface {
	CheckNames(spaces []string, member string) error
	CheckTiming(t core.Timing) error
}

// failRecorder is a testing.TB that records Errorf and Fatalf instead of failing; Fatalf ends the
// calling goroutine, so run it under runToEnd.
type failRecorder struct {
	testing.TB
	mu   sync.Mutex
	errs []string
}

func (f *failRecorder) Helper() {}

func (f *failRecorder) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, fmt.Sprintf(format, args...))
}

func (f *failRecorder) Fatalf(format string, args ...any) {
	f.Errorf(format, args...)
	runtime.Goexit()
}

// failures returns the recorded messages.
func (f *failRecorder) failures() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.errs)
}

// runToEnd runs f on a goroutine of its own and waits until it returns or calls runtime.Goexit.
func runToEnd(f func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	<-done
}

// wantErr fails t unless errors.Is(err, want).
func wantErr(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s = %v, want %v", what, err, want)
	}
}

// wantView returns the view waiting in ch, failing t when there is none or ch is closed.
func wantView(t *testing.T, ch <-chan core.View) core.View {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("receive = closed channel, want a view")
		}
		return v
	default:
		t.Fatal("receive = nothing, want a view")
	}
	return core.View{}
}

// wantNoView fails t when ch holds a view or is closed.
func wantNoView(t *testing.T, ch <-chan core.View) {
	t.Helper()
	select {
	case v, ok := <-ch:
		t.Fatalf("receive = %v, %t, want nothing", v, ok)
	default:
	}
}

// wantClosed fails t unless ch is closed with nothing left in it.
func wantClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("receive = %v, want a closed channel", v)
		}
	default:
		t.Fatal("receive = nothing, want a closed channel")
	}
}

// ids returns the member IDs of v.
func ids(v core.View) []string {
	out := make([]string, len(v.Members))
	for i, m := range v.Members {
		out[i] = m.ID
	}
	return out
}

// listed returns members with the given IDs and no slot.
func listed(ids ...string) []core.Member {
	out := make([]core.Member, len(ids))
	for i, id := range ids {
		out[i] = core.Member{ID: id, Slot: -1}
	}
	return out
}

func TestFixtureStore(t *testing.T) {
	const h = "n1/1"
	tm := core.DefaultTiming
	boom := errors.New("boom")

	t.Run("RecordsAndForwards", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			ms := memstore.New(nil)
			fs := newRecStore(ms)
			t0 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			if err := fs.KeepAlive(ctx, h, tm); err != nil {
				t.Fatalf("KeepAlive = %v, want nil", err)
			}
			term, err := fs.Acquire(ctx, h, "orders", 3)
			if err != nil || term == 0 {
				t.Fatalf("Acquire = %v, %v, want a term", term, err)
			}
			got, err := fs.Confirm(ctx, h, "orders", 3, term)
			if err != nil || got.Holder != h || got.Term != term {
				t.Fatalf("Confirm = %+v, %v, want holder %s at %v", got, err, h, term)
			}
			if hs, err := fs.Holdings(ctx, "orders"); err != nil || len(hs) != 1 || hs[0].Holder != h {
				t.Fatalf("Holdings = %+v, %v, want orders/3 held by %s", hs, err, h)
			}
			if err := fs.Release(ctx, h, "orders", 3, term); err != nil {
				t.Fatalf("Release = %v, want nil", err)
			}
			if err := fs.SetSlots(ctx, 2); err != nil {
				t.Fatalf("SetSlots = %v, want nil", err)
			}
			if slot, err := fs.ClaimSlot(ctx, h); err != nil || slot != 0 {
				t.Fatalf("ClaimSlot = %d, %v, want 0, nil", slot, err)
			}
			n, holders, err := fs.Slots(ctx)
			if err != nil || n != 2 || !slices.Equal(holders, []string{h, ""}) {
				t.Fatalf("Slots = %d, %q, %v, want 2, [%s \"\"], nil", n, holders, err, h)
			}
			holders[0] = "changed" // the caller's slice: the log keeps its own copy
			if got := fs.Calls("Slots")[0].Result; !reflect.DeepEqual(got, []any{2, []string{h, ""}}) {
				t.Fatalf("recorded Slots result = %v, want [2 [%s \"\"]]", got, h)
			}
			if err := fs.ReleaseSlot(ctx, h, 0); err != nil {
				t.Fatalf("ReleaseSlot = %v, want nil", err)
			}
			if err := fs.CloseSession(ctx, h); err != nil {
				t.Fatalf("CloseSession = %v, want nil", err)
			}

			// Every call reached the memstore.
			if hs, _ := ms.Holdings(ctx, "orders"); len(hs) != 1 || hs[0].Holder != "" || hs[0].Term != term {
				t.Fatalf("memstore Holdings = %+v, want orders/3 released at %v", hs, term)
			}
			if err := ms.KeepAlive(ctx, h, tm); !errors.Is(err, tenure.ErrSessionLost) {
				t.Fatalf("memstore KeepAlive after CloseSession = %v, want ErrSessionLost", err)
			}

			calls := fs.Calls()
			want := []string{"KeepAlive", "Acquire", "Confirm", "Holdings", "Release", "SetSlots", "ClaimSlot",
				"Slots", "ReleaseSlot", "CloseSession"}
			if got := callMethods(calls); !slices.Equal(got, want) {
				t.Fatalf("methods = %v, want %v", got, want)
			}
			var last uint64
			for _, c := range calls {
				if !c.HasDeadline || !c.Deadline.Equal(t0.Add(2*time.Second)) {
					t.Errorf("%v: deadline %v (%t), want %v", c, c.Deadline, c.HasDeadline, t0.Add(2*time.Second))
				}
				if !c.Done || !c.Start.Equal(t0) || !c.End.Equal(t0) || c.Err != nil {
					t.Errorf("%v: done %t, start %v, end %v, want done at %v with nil", c, c.Done, c.Start, c.End, t0)
				}
				if c.Seq <= last || c.EndSeq <= c.Seq {
					t.Errorf("%v: stamps %d, %d after %d, want increasing", c, c.Seq, c.EndSeq, last)
				}
				last = c.EndSeq
			}
			acq := fs.Calls("Acquire", "Release")
			if len(acq) != 2 || !reflect.DeepEqual(acq[0].Args, []any{h, "orders", 3}) ||
				!reflect.DeepEqual(acq[0].Result, []any{term}) ||
				!reflect.DeepEqual(acq[1].Args, []any{h, "orders", 3, term}) {
				t.Fatalf("Calls(Acquire, Release) = %v", acq)
			}
			if got, want := acq[0].String(), fmt.Sprintf("Acquire(%s, orders, 3) = [%v], <nil>", h, term); got != want {
				t.Errorf("String() = %q, want %q", got, want)
			}

			var st tenure.Store = fs
			if _, ok := st.(tenure.SlotStore); !ok {
				t.Error("recStore is no SlotStore")
			}
			if _, ok := st.(tenure.Watcher); ok {
				t.Error("recStore is a Watcher")
			}
			if _, ok := st.(checker); ok {
				t.Error("recStore has CheckNames and CheckTiming")
			}
			cancel()
			check()
		})
	})

	t.Run("Fail", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			ms := memstore.New(nil)
			fs := newRecStore(ms)
			ctx := context.Background()
			fs.Fail("KeepAlive", boom)
			wantErr(t, "KeepAlive while failing", fs.KeepAlive(ctx, h, tm), boom)
			if err := ms.KeepAlive(ctx, h, tm); err != nil {
				t.Fatalf("memstore KeepAlive = %v, want nil: the failed call must not be forwarded", err)
			}
			fs.Fail("KeepAlive", nil)
			wantErr(t, "KeepAlive after Fail(nil)", fs.KeepAlive(ctx, h, tm), nil)
			if c := fs.Calls("KeepAlive"); len(c) != 2 || c[0].Err != boom || c[1].Err != nil {
				t.Fatalf("Calls(KeepAlive) = %v, want boom then nil", c)
			}
			check()
		})
	})

	t.Run("BlockPastDeadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			fs := newRecStore(memstore.New(nil))
			release := fs.Block("KeepAlive")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			t0 := time.Now()
			errc := make(chan error, 1)
			go func() { errc <- fs.KeepAlive(ctx, h, tm) }()

			time.Sleep(5 * time.Second)
			synctest.Wait()
			if c := fs.Calls("KeepAlive"); len(c) != 1 || c[0].Done || !c[0].Deadline.Equal(t0.Add(time.Second)) {
				t.Fatalf("Calls(KeepAlive) = %v, want one in flight with deadline %v", c, t0.Add(time.Second))
			}
			release()
			release()
			wantErr(t, "blocked KeepAlive", <-errc, context.DeadlineExceeded)
			if c := fs.Calls("KeepAlive")[0]; c.End.Sub(c.Start) != 5*time.Second {
				t.Fatalf("blocked KeepAlive took %v, want 5s", c.End.Sub(c.Start))
			}

			// Released: later calls pass at once.
			wantErr(t, "KeepAlive after release", fs.KeepAlive(context.Background(), h, tm), nil)
			if c := fs.Calls("KeepAlive")[1]; !c.End.Equal(c.Start) {
				t.Fatalf("KeepAlive after release took %v, want 0", c.End.Sub(c.Start))
			}
			check()
		})
	})

	t.Run("DelayPastDeadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			ms := memstore.New(nil)
			fs := newRecStore(ms)
			wantErr(t, "KeepAlive", fs.KeepAlive(context.Background(), h, tm), nil)
			fs.Delay("Acquire", 3*time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := fs.Acquire(ctx, h, "orders", 1)
			wantErr(t, "delayed Acquire", err, context.DeadlineExceeded)
			if c := fs.Calls("Acquire")[0]; c.End.Sub(c.Start) != 3*time.Second {
				t.Fatalf("delayed Acquire took %v, want 3s", c.End.Sub(c.Start))
			}
			fs.Delay("Acquire", 0)
			if _, err := fs.Acquire(context.Background(), h, "orders", 1); err != nil {
				t.Fatalf("Acquire after Delay(0) = %v, want nil", err)
			}
			if c := fs.Calls("Acquire")[1]; !c.End.Equal(c.Start) {
				t.Fatalf("Acquire after Delay(0) took %v, want 0", c.End.Sub(c.Start))
			}
			check()
		})
	})

	t.Run("Answer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			ms := memstore.New(nil)
			fs := newRecStore(ms)
			ctx := context.Background()
			wantErr(t, "KeepAlive", fs.KeepAlive(ctx, h, tm), nil)

			var seen []any
			fs.Answer("Acquire", func(_ context.Context, args []any) ([]any, error) {
				seen = args
				return []any{core.Term(7)}, nil
			})
			if term, err := fs.Acquire(ctx, h, "orders", 2); term != 7 || err != nil {
				t.Fatalf("answered Acquire = %v, %v, want 7, nil", term, err)
			}
			if !reflect.DeepEqual(seen, []any{h, "orders", 2}) {
				t.Fatalf("answer got %v, want [%s orders 2]", seen, h)
			}
			if hs, _ := ms.Holdings(ctx, "orders"); len(hs) != 0 {
				t.Fatalf("memstore Holdings = %v, want none: the answered call must not be forwarded", hs)
			}

			// An answer can forward; the forwarded call is not recorded again.
			fs.Answer("Acquire", func(ctx context.Context, args []any) ([]any, error) {
				return fs.Forward(ctx, "Acquire", args)
			})
			term, err := fs.Acquire(ctx, h, "orders", 2)
			if err != nil || term == 0 {
				t.Fatalf("forwarding Acquire = %v, %v, want a term", term, err)
			}
			fs.Fail("Acquire", boom)
			_, err = fs.Acquire(ctx, h, "orders", 2)
			wantErr(t, "Acquire with Fail and Answer", err, boom)
			fs.Fail("Acquire", nil)
			fs.Answer("Acquire", nil)
			if got, err := fs.Acquire(ctx, h, "orders", 2); err != nil || got <= term {
				t.Fatalf("Acquire after Answer(nil) = %v, %v, want a term above %v", got, err, term)
			}
			if n := len(fs.Calls("Acquire")); n != 4 {
				t.Fatalf("%d Acquire calls recorded, want 4", n)
			}
			check()
		})
	})

	t.Run("Watching", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			fs := newRecStore(memstore.New(nil))
			ws := fs.Watching()
			var st tenure.Store = ws
			if _, ok := st.(tenure.Watcher); !ok {
				t.Fatal("Watching() is no Watcher")
			}
			if _, ok := st.(tenure.SlotStore); !ok {
				t.Fatal("Watching() is no SlotStore")
			}
			ctx, cancel := context.WithCancel(context.Background())
			ch, err := ws.WatchHolders(ctx)
			if err != nil {
				t.Fatalf("WatchHolders = %v, want nil", err)
			}
			wantErr(t, "KeepAlive", ws.KeepAlive(ctx, h, tm), nil)
			term, _ := ws.Acquire(ctx, h, "orders", 5)
			if got := <-ch; got.Holder != h || got.Shard != 5 || got.Term != term {
				t.Fatalf("watched %+v, want orders/5 held by %s at %v", got, h, term)
			}
			if got := callMethods(fs.Calls()); !slices.Equal(got, []string{"WatchHolders", "KeepAlive", "Acquire"}) {
				t.Fatalf("shared log = %v", got)
			}
			cancel()
			synctest.Wait()
			wantClosed(t, ch)
			check()
		})
	})

	t.Run("Injected", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			fs := newRecStore(memstore.New(nil))
			is := fs.Injected()
			if _, ok := tenure.Store(is).(tenure.Watcher); !ok {
				t.Fatal("Injected() is no Watcher")
			}
			if fs.Injected() != is {
				t.Fatal("a second Injected() is another value: its Inject would miss the first one's channels")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch1, err1 := is.WatchHolders(ctx)
			ch2, err2 := is.WatchHolders(ctx)
			if err1 != nil || err2 != nil {
				t.Fatalf("WatchHolders = %v, %v, want nil", err1, err2)
			}

			// A real write is not delivered; an injected one is, to every channel.
			wantErr(t, "KeepAlive", is.KeepAlive(ctx, h, tm), nil)
			if _, err := is.Acquire(ctx, h, "orders", 1); err != nil {
				t.Fatalf("Acquire = %v, want nil", err)
			}
			if len(ch1) != 0 || len(ch2) != 0 {
				t.Fatalf("real write delivered: %d, %d values", len(ch1), len(ch2))
			}
			stale := tenure.Holding{Space: "orders", Shard: 1, Holder: "x/1", Term: 9, Version: 1}
			is.Inject(stale)
			if got1, got2 := <-ch1, <-ch2; got1 != stale || got2 != stale {
				t.Fatalf("injected %+v, received %+v and %+v", stale, got1, got2)
			}

			is.CloseWatch()
			wantClosed(t, ch1)
			wantClosed(t, ch2)
			ch3, err := is.WatchHolders(ctx)
			if err != nil {
				t.Fatalf("WatchHolders after CloseWatch = %v, want nil", err)
			}
			is.Inject(stale)
			if got := <-ch3; got != stale {
				t.Fatalf("after CloseWatch received %+v, want %+v", got, stale)
			}
			is.Fail("WatchHolders", boom)
			if _, err := is.WatchHolders(ctx); !errors.Is(err, boom) {
				t.Fatalf("failing WatchHolders = %v, want boom", err)
			}
			if n := len(fs.Calls("WatchHolders")); n != 4 {
				t.Fatalf("%d WatchHolders calls recorded, want 4", n)
			}
			cancel()
			synctest.Wait()
			wantClosed(t, ch3)
			if _, err := is.Watching().WatchHolders(ctx); !errors.Is(err, boom) {
				t.Fatalf("WatchHolders of the shared script = %v, want boom", err)
			}
			check()
		})
	})

	t.Run("Checking", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			fs := newRecStore(memstore.New(nil))
			cs := fs.Checking(boom, nil)
			var st tenure.Store = cs
			if _, ok := st.(checker); !ok {
				t.Fatal("Checking() has no CheckNames and CheckTiming")
			}
			if _, ok := st.(tenure.SlotStore); !ok {
				t.Fatal("Checking() is no SlotStore")
			}
			spaces := []string{"orders", "markets"}
			wantErr(t, "CheckNames", cs.CheckNames(spaces, "n1"), boom)
			spaces[0] = "changed"
			wantErr(t, "CheckTiming", cs.CheckTiming(tm), nil)
			if err := fs.Checking(nil, boom).CheckTiming(tm); !errors.Is(err, boom) {
				t.Fatalf("CheckTiming = %v, want boom", err)
			}
			c := fs.Calls()
			if len(c) != 3 || c[0].Method != "CheckNames" || c[1].Method != "CheckTiming" ||
				!reflect.DeepEqual(c[0].Args, []any{[]string{"orders", "markets"}, "n1"}) ||
				!reflect.DeepEqual(c[1].Args, []any{tm}) || c[0].HasDeadline {
				t.Fatalf("Calls() = %v", c)
			}
			check()
		})
	})

	t.Run("StoreOnly", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			fs := newRecStore(memstore.New(nil))
			so := fs.StoreOnly()
			if _, ok := so.(tenure.SlotStore); ok {
				t.Fatal("StoreOnly() is a SlotStore")
			}
			if _, ok := so.(tenure.Watcher); ok {
				t.Fatal("StoreOnly() is a Watcher")
			}
			ctx := context.Background()
			wantErr(t, "KeepAlive", so.KeepAlive(ctx, h, tm), nil)
			term, err := so.Acquire(ctx, h, "orders", 0)
			if err != nil {
				t.Fatalf("Acquire = %v, want nil", err)
			}
			if _, err := so.Confirm(ctx, h, "orders", 0, term); err != nil {
				t.Fatalf("Confirm = %v, want nil", err)
			}
			if _, err := so.Holdings(ctx, "orders"); err != nil {
				t.Fatalf("Holdings = %v, want nil", err)
			}
			wantErr(t, "Release", so.Release(ctx, h, "orders", 0, term), nil)
			wantErr(t, "CloseSession", so.CloseSession(ctx, h), nil)
			want := []string{"KeepAlive", "Acquire", "Confirm", "Holdings", "Release", "CloseSession"}
			if got := callMethods(fs.Calls()); !slices.Equal(got, want) {
				t.Fatalf("recorded %v, want %v", got, want)
			}
			check()
		})
	})
}

func TestFixtureMembership(t *testing.T) {
	boom := errors.New("boom")

	t.Run("RecordsAndForwards", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			g := newGroup(t, listed("n1", "n2")...)
			sm := g.Node("n1")
			fs := newRecStore(memstore.New(nil))
			ctx := context.Background()
			t0 := time.Now()

			ch, err := sm.Watch(ctx)
			if err != nil {
				t.Fatalf("Watch = %v, want nil", err)
			}
			if v := wantView(t, ch); v.Version != 1 || !slices.Equal(ids(v), []string{"n1", "n2"}) {
				t.Fatalf("first view = %+v, want version 1 with n1, n2", v)
			}
			// As with static, a view reaches the channel inside the call that makes it.
			self := core.Member{ID: "n1", Addr: "a1", Slot: -1}
			wantErr(t, "Join", sm.Join(ctx, self), nil)
			wantErr(t, "KeepAlive", fs.KeepAlive(ctx, "n1/1", core.DefaultTiming), nil)
			if v := wantView(t, ch); v.Version != 2 || v.Members[0].Addr != "a1" {
				t.Fatalf("view after Join = %+v, want version 2 with n1 at a1", v)
			}
			wantErr(t, "SetDraining", sm.SetDraining(ctx, true), nil)
			if v := wantView(t, ch); !v.Members[0].Draining {
				t.Fatalf("view after SetDraining = %+v, want n1 draining", v)
			}

			// Leave delivers the last view and closes the channel before it returns. No goroutine
			// was there for the open channel.
			check()
			wantErr(t, "Leave", sm.Leave(ctx), nil)
			if v := wantView(t, ch); !slices.Equal(ids(v), []string{"n2"}) {
				t.Fatalf("view after Leave = %+v, want n2 only", v)
			}
			wantClosed(t, ch)
			_, err = sm.Watch(ctx)
			wantErr(t, "Watch after Leave", err, tenure.ErrClosed)

			calls := sm.Calls()
			if got := callMethods(calls); !slices.Equal(got, []string{"Watch", "Join", "SetDraining", "Leave", "Watch"}) {
				t.Fatalf("methods = %v", got)
			}
			if !reflect.DeepEqual(calls[1].Args, []any{self}) || !reflect.DeepEqual(calls[2].Args, []any{true}) ||
				!calls[0].Start.Equal(t0) || calls[0].HasDeadline {
				t.Fatalf("calls = %v", calls)
			}
			if got := callMethods(callsInOrder(fs.Calls(), sm.Calls())); !slices.Equal(got,
				[]string{"Watch", "Join", "KeepAlive", "SetDraining", "Leave", "Watch"}) {
				t.Fatalf("callsInOrder = %v", got)
			}
			check()
		})
	})

	t.Run("Fail", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			g := newGroup(t, listed("n1")...)
			sm := g.Node("n1")
			ctx, cancel := context.WithCancel(context.Background())
			sm.Fail("Watch", boom)
			_, err := sm.Watch(ctx)
			wantErr(t, "failing Watch", err, boom)
			sm.Fail("Watch", nil)
			ch, err := sm.Watch(ctx)
			wantErr(t, "Watch after Fail(nil)", err, nil)
			wantView(t, ch)
			sm.Delay("Join", time.Second)
			t0 := time.Now()
			wantErr(t, "delayed Join", sm.Join(ctx, core.Member{ID: "n1", Slot: -1}), nil)
			if d := time.Since(t0); d != time.Second {
				t.Fatalf("delayed Join took %v, want 1s", d)
			}
			if c := sm.Calls("Watch"); len(c) != 2 || c[0].Err != boom || c[1].Err != nil {
				t.Fatalf("Calls(Watch) = %v", c)
			}
			cancel()
			synctest.Wait()
			wantClosed(t, ch)
			check()
		})
	})

	t.Run("CloseWatch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			g := newGroup(t, listed("n1", "n2")...)
			sm := g.Node("n1")
			ctx, cancel := context.WithCancel(context.Background())
			ch0, _ := sm.Watch(ctx)
			ch1, _ := sm.Watch(ctx)
			wantView(t, ch0)
			wantView(t, ch1)
			sm.CloseWatch(1)
			wantClosed(t, ch1)
			g.Join(core.Member{ID: "n3", Slot: -1})
			if v := wantView(t, ch0); !slices.Equal(ids(v), []string{"n1", "n2", "n3"}) {
				t.Fatalf("view on the open channel = %+v, want n1, n2, n3", v)
			}
			sm.CloseWatch(1) // again: nothing happens

			// A view not yet received stays in a closed channel, as in a static one.
			g.Join(core.Member{ID: "n4", Slot: -1})
			sm.CloseWatch(0)
			if v := wantView(t, ch0); !slices.Equal(ids(v), []string{"n1", "n2", "n3", "n4"}) {
				t.Fatalf("view left in the closed channel = %+v, want n1, n2, n3, n4", v)
			}
			wantClosed(t, ch0)

			rec := &failRecorder{TB: t}
			newGroup(rec, listed("n1")...).Node("n1").CloseWatch(0)
			if f := rec.failures(); len(f) != 1 || !strings.Contains(f[0], "Watch returned 0 channels") {
				t.Fatalf("CloseWatch(0) without a Watch failed with %q", f)
			}

			// A channel is closed when its ctx is done, with the view not yet received left in it.
			ch2, _ := sm.Watch(ctx)
			cancel()
			synctest.Wait()
			if v := wantView(t, ch2); v.Version != 3 {
				t.Fatalf("view in the channel closed by its ctx = %+v, want version 3", v)
			}
			wantClosed(t, ch2)
			check()
		})
	})

	t.Run("PauseResume", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			g := newGroup(t, listed("n1", "n2")...)
			sm := g.Node("n1")
			ctx, cancel := context.WithCancel(context.Background())
			ch, _ := sm.Watch(ctx)
			wantView(t, ch)

			sm.Pause()
			g.Join(core.Member{ID: "n3", Slot: -1})
			g.SetDraining("n3", true)
			g.Join(core.Member{ID: "n4", Slot: -1})
			synctest.Wait()
			wantNoView(t, ch)
			sm.Resume()
			if v := wantView(t, ch); v.Version != 4 || !slices.Equal(ids(v), []string{"n1", "n2", "n3", "n4"}) ||
				!v.Members[2].Draining {
				t.Fatalf("view after Resume = %+v, want version 4 with n3 draining and n4", v)
			}
			wantNoView(t, ch)

			// A view made just before Pause is delivered, not held back.
			g.Leave("n4")
			sm.Pause()
			if v := wantView(t, ch); v.Version != 5 || !slices.Equal(ids(v), []string{"n1", "n2", "n3"}) {
				t.Fatalf("view made before Pause = %+v, want version 5 with n1, n2, n3", v)
			}
			sm.Resume()
			wantNoView(t, ch)

			// A view delivered before Pause and not yet received is replaced by the latest.
			g.Join(core.Member{ID: "n4", Slot: -1})
			sm.Pause()
			g.Leave("n4")
			g.Leave("n3")
			sm.Resume()
			if v := wantView(t, ch); v.Version != 8 || !slices.Equal(ids(v), []string{"n1", "n2"}) {
				t.Fatalf("view after the third Resume = %+v, want version 8 with n1, n2", v)
			}
			wantNoView(t, ch)

			// A channel opened while paused holds nothing until Resume.
			sm.Pause()
			late, _ := sm.Watch(ctx)
			synctest.Wait()
			wantNoView(t, late)
			sm.Resume()
			if v := wantView(t, late); v.Version != 8 {
				t.Fatalf("late view = %+v, want version 8", v)
			}
			sm.Resume() // nothing held: nothing delivered
			wantNoView(t, ch)
			cancel()
			check()
		})
	})

	t.Run("SetDrainingQuiet", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			g := newGroup(t, listed("n1", "n2")...)
			sm := g.Node("n1")
			ctx := context.Background()
			wantErr(t, "Join", sm.Join(ctx, core.Member{ID: "n1", Slot: -1}), nil)
			sm.Fail("SetDraining", boom)
			before := len(sm.Calls())
			sm.SetDrainingQuiet(true)
			if after := len(sm.Calls()); after != before {
				t.Fatalf("SetDrainingQuiet recorded %d calls, want none", after-before)
			}
			if v := g.View(); !v.Members[0].Draining {
				t.Fatalf("view = %+v, want n1 draining", v)
			}

			rec := &failRecorder{TB: t}
			newGroup(rec, listed("n1")...).Node("n1").SetDrainingQuiet(true)
			if f := rec.failures(); len(f) != 1 || !strings.Contains(f[0], "not joined") {
				t.Fatalf("SetDrainingQuiet before Join failed with %q", f)
			}
			wantErr(t, "Leave", sm.Leave(ctx), nil)
			check()
		})
	})

	t.Run("Group", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			g := newGroup(t, listed("n1", "n2")...)
			ctx := context.Background()

			// Absent: the member appears only once it joins.
			sm := g.Absent("n9")
			if v := g.View(); slices.Contains(ids(v), "n9") {
				t.Fatalf("view = %v before n9 joined", ids(v))
			}
			wantErr(t, "Join", sm.Join(ctx, core.Member{ID: "n9", Slot: -1}), nil)
			if v := g.View(); !slices.Equal(ids(v), []string{"n1", "n2", "n9"}) {
				t.Fatalf("view = %v, want n1, n2, n9", ids(v))
			}

			// Helpers change the list without recording.
			g.Join(core.Member{ID: "n3", Addr: "a3", Slot: -1})
			g.SetDraining("n2", true) // listed, joined by no one
			v := g.View()
			if !slices.Equal(ids(v), []string{"n1", "n2", "n3", "n9"}) || !v.Members[1].Draining || v.Members[2].Addr != "a3" {
				t.Fatalf("view = %+v, want n2 draining and n3 at a3", v)
			}
			g.Leave("n2")
			g.Leave("n3")
			g.Leave("n1")
			if v := g.View(); !slices.Equal(ids(v), []string{"n9"}) {
				t.Fatalf("view = %v, want n9 only", ids(v))
			}
			g.Join(core.Member{ID: "n3", Slot: -1})
			if got := callMethods(sm.Calls()); !slices.Equal(got, []string{"Join"}) {
				t.Fatalf("recorded %v, want [Join]", got)
			}

			// Misuse fails the test.
			rec := &failRecorder{TB: t}
			bad := newGroup(rec, listed("n1")...)
			runToEnd(func() { bad.Node("n5") })
			runToEnd(func() { bad.Absent("n1") })
			runToEnd(func() { bad.Leave("n5") })
			runToEnd(func() { newGroup(rec, listed("n1", "n1")...) })
			if f := rec.failures(); len(f) != 4 || !strings.Contains(f[0], "not listed") ||
				!strings.Contains(f[1], "listed; use Node") || !strings.Contains(f[2], "not in the view") ||
				!strings.Contains(f[3], "duplicate") {
				t.Fatalf("misuse failed with %q", f)
			}
			wantErr(t, "Leave", sm.Leave(ctx), nil)
			check()
		})
	})

	t.Run("Checking", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			sm := newGroup(t, listed("n1")...).Node("n1")
			cm := sm.Checking(nil, boom)
			var m tenure.Membership = cm
			if _, ok := m.(checker); !ok {
				t.Fatal("Checking() has no CheckNames and CheckTiming")
			}
			if _, ok := tenure.Membership(sm).(checker); ok {
				t.Fatal("recMembership has CheckNames and CheckTiming")
			}
			wantErr(t, "CheckNames", cm.CheckNames(nil, "n1"), nil)
			wantErr(t, "CheckTiming", cm.CheckTiming(core.DefaultTiming), boom)
			if err := sm.Checking(boom, nil).CheckNames([]string{"a"}, "n1"); !errors.Is(err, boom) {
				t.Fatalf("CheckNames = %v, want boom", err)
			}
			c := sm.Calls()
			if len(c) != 3 || !reflect.DeepEqual(c[0].Args, []any{[]string(nil), "n1"}) ||
				!reflect.DeepEqual(c[1].Args, []any{core.DefaultTiming}) || c[1].Err != boom {
				t.Fatalf("Calls() = %v", c)
			}
			check()
		})
	})
}

func TestFixtureOutsideHolder(t *testing.T) {
	tm := core.DefaultTiming

	t.Run("RenewsAcquiresReleases", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			ms := memstore.New(nil)
			fs := newRecStore(ms)
			ctx := context.Background()
			t0 := time.Now()
			o := newOutside(t, fs, tm)
			term, err := o.Acquire("orders", 3)
			if err != nil || term == 0 {
				t.Fatalf("outside Acquire = %v, %v, want a term", term, err)
			}

			// Well past LeaseDuration the session is still live: x/1 keeps the shard.
			time.Sleep(41 * time.Second)
			wantErr(t, "KeepAlive n1", ms.KeepAlive(ctx, "n1/1", tm), nil)
			_, err = ms.Acquire(ctx, "n1/1", "orders", 3)
			var held *tenure.HeldError
			if !errors.As(err, &held) || held.Holder != outsideHolder {
				t.Fatalf("Acquire by n1 = %v, want held by %s", err, outsideHolder)
			}
			wantErr(t, "outside Release", o.Release("orders", 3), nil)
			if _, err := ms.Acquire(ctx, "n1/1", "orders", 3); err != nil {
				t.Fatalf("Acquire by n1 after the outside release = %v, want nil", err)
			}
			if err := o.Release("orders", 3); err == nil || !strings.Contains(err.Error(), "orders/3 not acquired") {
				t.Fatalf("second outside Release = %v, want not acquired", err)
			}
			if _, err := o.Acquire("orders", 4); err != nil {
				t.Fatalf("outside Acquire orders/4 = %v, want nil", err)
			}

			// Renewals every RetryPeriod from the open, until Stop.
			o.Stop()
			o.Stop()
			ka := fs.Calls("KeepAlive")
			if len(ka) != 21 {
				t.Fatalf("%d KeepAlive calls, want 21", len(ka))
			}
			for i, c := range ka {
				if want := t0.Add(time.Duration(i) * tm.RetryPeriod); !c.Start.Equal(want) || c.Args[0] != outsideHolder || c.Err != nil {
					t.Fatalf("KeepAlive %d = %v at %v, want %s at %v", i, c, c.Start.Sub(t0), outsideHolder, want.Sub(t0))
				}
			}

			// Stopped: the session expires and another member takes the shard.
			time.Sleep(tm.LeaseDuration)
			if n := len(fs.Calls("KeepAlive")); n != 21 {
				t.Fatalf("%d KeepAlive calls after Stop, want 21", n)
			}
			wantErr(t, "KeepAlive n2", ms.KeepAlive(ctx, "n2/1", tm), nil)
			if _, err := ms.Acquire(ctx, "n2/1", "orders", 4); err != nil {
				t.Fatalf("Acquire by n2 after the outside session expired = %v, want nil", err)
			}
			check()
		})
	})

	t.Run("CleanupStops", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			newOutside(t, memstore.New(nil), tm)
			time.Sleep(time.Minute)
			// No Stop: t's cleanup ends the goroutine, else the bubble would not end.
		})
	})
}

func TestFixtureBal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		members := []string{"n1", "n2", "n3"}
		got := bal("orders", 8, members...)
		idx := core.Balanced("orders", 8, members)
		var shards []int
		for id, own := range got {
			if len(own) < 2 || len(own) > 3 || !slices.IsSorted(own) {
				t.Errorf("%s owns %v, want 2 or 3 sorted shards", id, own)
			}
			for _, s := range own {
				if members[idx[s]] != id {
					t.Errorf("shard %d: bal says %s, core.Balanced says %s", s, id, members[idx[s]])
				}
			}
			shards = append(shards, own...)
		}
		slices.Sort(shards)
		if !slices.Equal(shards, []int{0, 1, 2, 3, 4, 5, 6, 7}) {
			t.Fatalf("bal covers shards %v, want 0…7 once each", shards)
		}
		if again := bal("orders", 8, "n3", "n1", "n2"); !reflect.DeepEqual(again, got) {
			t.Fatalf("reordered IDs give %v, want %v", again, got)
		}
		if one := bal("markets", 2, "slot-0", "slot-1", "slot-2"); len(one) != 2 {
			t.Fatalf("bal over 2 shards and 3 IDs = %v, want 2 owners", one)
		}
		if none := bal("orders", 4); len(none) != 0 {
			t.Fatalf("bal without IDs = %v, want empty", none)
		}
	})
}

// leakyGoroutine blocks until ch is closed.
func leakyGoroutine(ch <-chan struct{}) { <-ch }

func TestFixtureBaseline(t *testing.T) {
	t.Run("Clean", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			rec := &failRecorder{TB: t}
			check := baseline(rec)
			ch := make(chan struct{})
			go leakyGoroutine(ch)
			close(ch)
			check() // waits for the goroutine to end
			if f := rec.failures(); len(f) != 0 {
				t.Fatalf("check failed with %q, want nothing", f)
			}
		})
	})

	t.Run("Leak", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			before := make(chan struct{})
			go leakyGoroutine(before) // there at the start: not reported
			rec := &failRecorder{TB: t}
			check := baseline(rec)
			ch := make(chan struct{})
			go leakyGoroutine(ch)
			check()
			f := rec.failures()
			if len(f) != 1 || !strings.Contains(f[0], "baseline: 1 goroutine(s) not there at the start") ||
				!strings.Contains(f[0], "leakyGoroutine") || !strings.Contains(f[0], "chan receive") {
				t.Fatalf("check failed with %q, want one leaked goroutine and its stack", f)
			}
			close(ch)
			close(before)
		})
	})
}
