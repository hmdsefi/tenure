// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// recvHolding returns the next value of ch, waiting at most d; ok is false when ch is closed
// and the test fails when nothing comes.
func recvHolding(t testing.TB, ch <-chan tenure.Holding, d time.Duration) (h tenure.Holding, ok bool) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case h, ok = <-ch:
		return h, ok
	case <-timer.C:
		t.Fatalf("WatchHolders channel sent nothing for %v", d)
		return tenure.Holding{}, false
	}
}

// nextHolding checks that the next value of ch, within 10 s, is want but for its Version, and
// returns it.
func nextHolding(t testing.TB, ch <-chan tenure.Holding, want tenure.Holding) tenure.Holding {
	t.Helper()
	got, ok := recvHolding(t, ch, 10*time.Second)
	want.Version = got.Version
	if !ok || got != want {
		t.Fatalf("WatchHolders channel sent %+v (open %v), want %+v", got, ok, want)
	}
	return got
}

// wantClosed checks that ch is closed within d with no value left in it.
func wantClosed(t testing.TB, ch <-chan tenure.Holding, d time.Duration) {
	t.Helper()
	if h, ok := recvHolding(t, ch, d); ok {
		t.Fatalf("WatchHolders channel sent %+v, want it closed", h)
	}
}

// wantQuiet checks that ch sends nothing and stays open for d.
func wantQuiet(t testing.TB, ch <-chan tenure.Holding, d time.Duration) {
	t.Helper()
	select {
	case h, ok := <-ch:
		t.Fatalf("WatchHolders channel sent %+v (open %v), want nothing", h, ok)
	case <-time.After(d):
	}
}

// mustWatch calls WatchHolders and fails the test on an error.
func mustWatch(t testing.TB, a *Adapter, ctx context.Context) <-chan tenure.Holding {
	t.Helper()
	ch, err := a.WatchHolders(ctx)
	if err != nil || ch == nil {
		t.Fatalf("WatchHolders = %v, %v; want a channel, nil", ch, err)
	}
	return ch
}

// waitWatches waits up to 2 s for the running cache of a to have n open WatchHolders channels.
func waitWatches(t testing.TB, a *Adapter, n int) {
	t.Helper()
	c := a.runningCache()
	if c == nil {
		t.Fatal("no running cache")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.holders.mu.Lock()
		got := len(c.holders.set)
		c.holders.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d open WatchHolders channels after 2s, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// malformedLogs returns the resourceVersion of each Warn record about a malformed term.
func malformedLogs(h *logRecorder) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		if r.Level != slog.LevelWarn || r.Message != "k8s: malformed term" {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "resourceVersion" {
				out = append(out, a.Value.String())
			}
			return true
		})
	}
	return out
}

// TestK8S510HolderEvents hands informer events to the channels of a run directly: only
// creations, holder and term changes and deletions of the group's shard Leases are sent, and
// only above R; changes delivered before R is known wait; a malformed term reads as 0 and is
// logged once per resourceVersion; a deletion has no holder and term 0; an unparsable
// resourceVersion, a deletion whose final state is unknown and a full backlog close every
// channel concerned.
func TestK8S510HolderEvents(t *testing.T) {
	logs := &logRecorder{}
	a := New(newFake(), "default", "g", WithTiming(T), WithLogger(slog.New(logs)))
	c := &leaseCache{}
	record := func(holder, term, rv string) *coordinationv1.Lease {
		l := shardLease("g", "markets", 0, holder, map[string]string{annotationTerm: term})
		l.ResourceVersion = rv
		return l
	}
	const h1, h2 = "pod-0/1", "pod-1/1"

	// No channel: nothing is logged.
	a.holderEvent(c, nil, record(h1, "x", "100"), false, false)

	w1 := c.holders.add()
	a.holderEvent(c, nil, record(h1, "5", "105"), false, false)
	confirmed := record(h1, "5", "107")
	confirmed.Annotations[annotationConfirm] = "1"
	a.holderEvent(c, record(h1, "5", "105"), confirmed, false, false)
	a.holderEvent(c, nil, record(h1, "6", "108"), false, false)
	c.holders.start(w1, 105) // 105 is at R, 108 above it
	nextHolding(t, w1.ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: 6})

	// Writes of other Leases send nothing.
	member := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: memberName("g", "pod-0"), Labels: leaseLabels("g", kindMember, ""), ResourceVersion: "110"}}
	slot := slotLease("g", 0, h1, "")
	slot.ResourceVersion = "111"
	foreign := record(h1, "9", "112")
	foreign.Labels[labelKind] = kindMember
	other := shardLease("h", "markets", 0, h1, map[string]string{annotationTerm: "9"})
	other.Labels[labelGroup], other.ResourceVersion = "g", "113"
	for _, l := range []*coordinationv1.Lease{member, slot, slotsLease("g", "2"), foreign, other} {
		a.holderEvent(c, nil, l, false, false)
	}

	w2 := c.holders.add()
	c.holders.start(w2, 0)
	// A malformed term reads as 0 and is logged once per resourceVersion, here delivered twice.
	for range 2 {
		a.holderEvent(c, record(h1, "6", "108"), record(h1, "x", "120"), false, false)
	}
	a.holderEvent(c, record(h1, "x", "120"), record(h1, "y", "121"), false, false) // still 0
	a.holderEvent(c, record(h1, "y", "121"), record(h2, "y", "122"), false, false)
	a.holderEvent(c, record(h2, "y", "122"), record(h2, "y", "130"), true, false)
	for _, ch := range []chan tenure.Holding{w1.ch, w2.ch} {
		for _, want := range []tenure.Holding{
			{Space: "markets", Shard: 0, Holder: h1, Version: 120},
			{Space: "markets", Shard: 0, Holder: h1, Version: 120},
			{Space: "markets", Shard: 0, Holder: h2, Version: 122},
			{Space: "markets", Shard: 0, Version: 130},
		} {
			if got := nextHolding(t, ch, want); got.Version != want.Version {
				t.Fatalf("Version %d, want %d", got.Version, want.Version)
			}
		}
	}
	if got := malformedLogs(logs); len(got) != 2 || got[0] != "120" || got[1] != "122" {
		t.Errorf("malformed term logged at %q, want [120 122]", got)
	}

	// An unparsable resourceVersion closes every channel.
	a.holderEvent(c, nil, record(h1, "7", "abc"), false, false)
	wantClosed(t, w1.ch, time.Second)
	wantClosed(t, w2.ch, time.Second)

	// A full channel, and a full backlog, close their channel.
	w3 := c.holders.add()
	c.holders.start(w3, 0)
	w4 := c.holders.add()
	for i := range holderBuffer + 1 {
		a.holderEvent(c, nil, record(h1, "7", strconv.Itoa(200+i)), false, false)
	}
	wantClosed(t, w4.ch, time.Second)
	for i := range holderBuffer {
		if h := <-w3.ch; h.Version != uint64(200+i) {
			t.Fatalf("value %d at Version %d, want %d", i, h.Version, 200+i)
		}
	}
	wantClosed(t, w3.ch, time.Second)

	// A deletion whose final state is unknown closes every channel.
	w5 := c.holders.add()
	c.holders.start(w5, 0)
	a.holderEvent(c, nil, record(h1, "7", "1"), true, true)
	wantClosed(t, w5.ch, time.Second)
	if n := len(c.holders.set); n != 0 {
		t.Errorf("%d channels still registered, want 0", n)
	}
}

// TestATK8S27WatchHolders checks WatchHolders on a cluster and on a fake client. The acceptance
// test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S27WatchHolders(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		c := newCluster(t)
		g := group()
		ctx := t.Context()
		a, _ := c.adapter(t, g)
		b, _ := c.adapter(t, g)
		const h1, h2 = "pod-0/1", "pod-1/1"
		closeAtEnd(t, a, h1)
		closeAtEnd(t, b, h2)
		startRenewer(t, a, h1)
		startRenewer(t, b, h2)
		mustKeepAlive(t, a, h1)
		mustKeepAlive(t, b, h2)

		// A write committed before WatchHolders returns is not sent.
		t1 := mustAcquire(t, a, h1, "markets", 0)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch := mustWatch(t, a, wctx)
		// b acts on what its own channel sends: its cache has seen that change by then.
		chB := mustWatch(t, b, ctx)

		// Confirm's write, slot Leases, the count and member Leases send nothing.
		if _, err := a.Confirm(ctx, h1, "markets", 0, t1); err != nil {
			t.Fatalf("Confirm: %v", err)
		}
		if err := b.SetSlots(ctx, 1); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		mustClaim(t, b, h2, 0)
		if err := a.Release(ctx, h1, "markets", 0, t1); err != nil {
			t.Fatalf("Release: %v", err)
		}
		nextHolding(t, chB, tenure.Holding{Space: "markets", Shard: 0, Term: t1})
		t2 := mustAcquire(t, b, h2, "markets", 0)
		t3 := mustAcquire(t, a, h1, "orders", 3)
		if err := c.leases().Delete(ctx, shardName(g, "markets", 0), metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		var top uint64
		for _, want := range []tenure.Holding{
			{Space: "markets", Shard: 0, Term: t1},
			{Space: "markets", Shard: 0, Holder: h2, Term: t2},
			{Space: "orders", Shard: 3, Holder: h1, Term: t3},
			{Space: "markets", Shard: 0},
		} {
			got := nextHolding(t, ch, want)
			if got.Version <= top {
				t.Fatalf("%+v after Version %d, want a larger Version", got, top)
			}
			top = got.Version
		}
		for _, want := range []tenure.Holding{
			{Space: "markets", Shard: 0, Holder: h2, Term: t2},
			{Space: "orders", Shard: 3, Holder: h1, Term: t3},
			{Space: "markets", Shard: 0},
		} {
			nextHolding(t, chB, want)
		}
		// Renewals go on meanwhile: still nothing.
		wantQuiet(t, ch, 2*T.RetryPeriod+time.Second)

		cancel()
		start := time.Now()
		wantClosed(t, ch, 2*time.Second)
		if d := time.Since(start); d > time.Second {
			t.Errorf("channel closed %v after its context ended, want within 1s", d)
		}
	})

	t.Run("unit", func(t *testing.T) {
		t.Run("buffer", func(t *testing.T) {
			ctx := t.Context()
			client := newFake()
			a := fakeAdapter(t, client)
			closeAtEnd(t, a, "pod-0/1")
			mustKeepAlive(t, a, "pod-0/1")
			ch := mustWatch(t, a, ctx)

			// 1025 changes, never received: the first 1024 come out, then the channel is closed.
			// The fake's watch holds at most 100 events, so they go in batches the cache observes.
			leases := client.CoordinationV1().Leases("default")
			for i := range holderBuffer + 1 {
				l, err := leases.Create(ctx, shardLease("g", "markets", i, "", map[string]string{annotationTerm: strconv.Itoa(i + 1)}), metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				if i%50 == 49 || i == holderBuffer {
					waitObserved(t, a, l.Name, l.ResourceVersion)
				}
			}
			// The cache observes a change before the channels get it: wait for the channel to go.
			waitWatches(t, a, 0)
			var top uint64
			for i := range holderBuffer {
				got := nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: i, Term: core.Term(i + 1)})
				if got.Version <= top {
					t.Fatalf("%+v after Version %d, want a larger Version", got, top)
				}
				top = got.Version
			}
			wantClosed(t, ch, time.Second)
			ch = mustWatch(t, a, ctx)
			if _, err := leases.Create(ctx, shardLease("g", "orders", 0, "", nil), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create: %v", err)
			}
			nextHolding(t, ch, tenure.Holding{Space: "orders", Shard: 0})
		})

		t.Run("watch error", func(t *testing.T) {
			ctx := t.Context()
			client := newFake()
			var mu sync.Mutex
			var current watch.Interface
			refuse := false
			client.PrependWatchReactor(resourceLeases, func(action k8stesting.Action) (bool, watch.Interface, error) {
				mu.Lock()
				defer mu.Unlock()
				if refuse {
					return true, nil, apierrors.NewForbidden(leasesResource.GroupResource(), "", errors.New("watch refused"))
				}
				w, _ := action.(k8stesting.WatchActionImpl)
				current, _ = client.Tracker().Watch(leasesResource, action.GetNamespace(), w.ListOptions)
				return true, current, nil
			})
			a := fakeAdapter(t, client)
			ch1, ch2 := mustWatch(t, a, ctx), mustWatch(t, a, ctx)
			if _, err := client.CoordinationV1().Leases("default").Create(ctx, shardLease("g", "markets", 0, "", nil), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create: %v", err)
			}
			nextHolding(t, ch1, tenure.Holding{Space: "markets", Shard: 0})
			nextHolding(t, ch2, tenure.Holding{Space: "markets", Shard: 0})

			// The watch ends after an event and cannot be opened again: the reflector calls its
			// watch error handler before it lists again.
			lists := len(actions(client, "list"))
			mu.Lock()
			refuse = true
			current.Stop()
			mu.Unlock()
			wantClosed(t, ch1, 2*time.Second)
			wantClosed(t, ch2, time.Second)
			if n := len(actions(client, "list")); n != lists {
				t.Errorf("%d LISTs before the channels closed, want %d: the list again, not the handler, closed them", n, lists)
			}
			mu.Lock()
			refuse = false
			mu.Unlock()
		})

		t.Run("list again", func(t *testing.T) {
			ctx := t.Context()
			client := newFake()
			var mu sync.Mutex
			var current *watch.RaceFreeFakeWatcher
			client.PrependWatchReactor(resourceLeases, func(action k8stesting.Action) (bool, watch.Interface, error) {
				mu.Lock()
				defer mu.Unlock()
				w, _ := action.(k8stesting.WatchActionImpl)
				wi, err := client.Tracker().Watch(leasesResource, action.GetNamespace(), w.ListOptions)
				current, _ = wi.(*watch.RaceFreeFakeWatcher)
				return true, wi, err
			})
			a := fakeAdapter(t, client)
			ch := mustWatch(t, a, ctx)
			if _, err := client.CoordinationV1().Leases("default").Create(ctx, shardLease("g", "markets", 0, "", nil), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create: %v", err)
			}
			nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0})

			// The watch ends with an error: the informer lists the Leases again, which shows only
			// their latest states.
			lists := len(actions(client, "list"))
			mu.Lock()
			current.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired, Message: "too old"})
			mu.Unlock()
			wantClosed(t, ch, 5*time.Second)
			for deadline := time.Now().Add(time.Second); len(actions(client, "list")) <= lists; time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("no LIST in 1s after the channel closed, want the informer's")
				}
			}
		})
	})
}

// TestATK8S38Versions checks Holding.Version: the Lease's resourceVersion, larger for a Lease
// created anew, with K8S-070's GET fallback. The acceptance test's steps were written from the
// issue's text: the original text is unavailable.
func TestATK8S38Versions(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		c := newCluster(t)
		g := group()
		ctx := t.Context()
		a, _ := c.adapter(t, g)
		const h1 = "pod-0/1"
		closeAtEnd(t, a, h1)
		startRenewer(t, a, h1)
		mustKeepAlive(t, a, h1)
		ch := mustWatch(t, a, ctx)
		name := shardName(g, "markets", 0)

		t1 := mustAcquire(t, a, h1, "markets", 0)
		granted := version(t, rawGet(t, c, name))
		if got := nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: t1}); got.Version != granted {
			t.Errorf("grant sent Version %d, want the resourceVersion %d", got.Version, granted)
		}
		h, err := a.Confirm(ctx, h1, "markets", 0, t1)
		if confirmed := version(t, rawGet(t, c, name)); err != nil || h.Version != confirmed || confirmed <= granted {
			t.Errorf("Confirm = %+v, %v; want Version %d, the resourceVersion, above %d", h, err, confirmed, granted)
		}
		hs, err := a.Holdings(ctx, "markets")
		if raw := version(t, rawGet(t, c, name)); err != nil || len(hs) != 1 || hs[0].Version != raw {
			t.Errorf("Holdings = %+v, %v; want one record at Version %d", hs, err, raw)
		}
		if err := a.Release(ctx, h1, "markets", 0, t1); err != nil {
			t.Fatalf("Release: %v", err)
		}
		released := version(t, rawGet(t, c, name))
		if got := nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Term: t1}); got.Version != released {
			t.Errorf("release sent Version %d, want the resourceVersion %d", got.Version, released)
		}
		if err := c.leases().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		deleted := nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0})
		if deleted.Version <= released {
			t.Errorf("deletion sent Version %d, want above %d", deleted.Version, released)
		}
		t2 := mustAcquire(t, a, h1, "markets", 0)
		recreated := version(t, rawGet(t, c, name))
		got := nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: t2})
		if got.Version != recreated || recreated <= deleted.Version || t2 <= t1 {
			t.Errorf("grant of the Lease created anew sent Version %d, term %d; want the resourceVersion %d above %d, a term above %d",
				got.Version, t2, recreated, deleted.Version, t1)
		}
	})

	t.Run("unit", func(t *testing.T) {
		ctx := t.Context()
		client := newFake()
		a := fakeAdapter(t, client)
		const h1 = "pod-0/1"
		closeAtEnd(t, a, h1)
		mustKeepAlive(t, a, h1)
		ch := mustWatch(t, a, ctx)
		name := shardName("g", "markets", 1)
		t1 := mustAcquire(t, a, h1, "markets", 1)
		nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 1, Holder: h1, Term: t1})

		// From here, a write of the record stores resourceVersion abc, and a GET answers with
		// reread.
		tracker := client.Tracker()
		var mu sync.Mutex
		reread := "17"
		client.PrependReactor("update", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
			update, _ := action.(k8stesting.UpdateAction)
			l, _ := update.GetObject().(*coordinationv1.Lease)
			if l.Name != name {
				return false, nil, nil
			}
			l = l.DeepCopy()
			l.ResourceVersion = "abc"
			return true, l.DeepCopy(), tracker.Update(leasesResource, l, "default")
		})
		client.PrependReactor("get", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
			get, _ := action.(k8stesting.GetAction)
			if get.GetName() != name {
				return false, nil, nil
			}
			obj, err := tracker.Get(leasesResource, "default", name)
			if err != nil {
				return true, nil, err
			}
			l, _ := obj.(*coordinationv1.Lease)
			l = l.DeepCopy()
			mu.Lock()
			l.ResourceVersion = reread
			mu.Unlock()
			return true, l, nil
		})

		want := tenure.Holding{Space: "markets", Shard: 1, Holder: h1, Term: t1, Version: 17}
		if got, err := a.Confirm(ctx, h1, "markets", 1, t1); err != nil || got != want {
			t.Errorf("Confirm with a written resourceVersion abc = %+v, %v; want %+v", got, err, want)
		}
		mu.Lock()
		reread = "abc"
		mu.Unlock()
		const wantErr = `k8s: lease g-markets-1: resourceVersion "abc" is not an integer`
		if got, err := a.Confirm(ctx, h1, "markets", 1, t1); err == nil || err.Error() != wantErr {
			t.Errorf("Confirm with a reread resourceVersion abc = %+v, %v; want %s", got, err, wantErr)
		}
		// The watch can not order a change at resourceVersion abc: it closes.
		if err := a.Release(ctx, h1, "markets", 1, t1); err != nil {
			t.Fatalf("Release: %v", err)
		}
		wantClosed(t, ch, 2*time.Second)
	})
}
