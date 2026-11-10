// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestATK8S23TrafficBudget checks the API traffic budget (K8S-720) on a cluster: per member one
// write per RetryPeriod, a session's renewal or, for a member without a session, the heartbeat,
// and two writes per shard move. Members a and b have joined and renew their sessions every
// RetryPeriod, as the runtime does, a holding a shard; member m has joined without a session.
// Once settled, each Adapter's recording over 10 s holds only 20 ± 1 updates of its own member
// Lease: no GET or LIST, no new WATCH, no request on a shard Lease. Moving the shard from a to b
// then adds exactly one update of the shard Lease to a's recording (the release) and one to b's
// (the grant), with no read, and nothing to m's. The informer of each Adapter starts with one
// LIST and one WATCH, or, with client-go's watch list, with one WATCH alone that sends the
// initial events. The acceptance test's steps were written from the issue's text: the original
// text is unavailable.
func TestATK8S23TrafficBudget(t *testing.T) {
	c := newCluster(t)
	ctx := t.Context()
	g := group()
	a, recA := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	m, recM := c.adapter(t, g)
	const ha, hb = "pod-a/1", "pod-b/1"
	for _, x := range []struct {
		a  *Adapter
		id string
	}{{a, "pod-a"}, {b, "pod-b"}, {m, "pod-m"}} {
		mustJoin(t, x.a, core.Member{ID: x.id, Addr: "10.0.0.1:7000", Slot: -1})
	}
	for _, x := range []struct {
		a *Adapter
		h string
	}{{a, ha}, {b, hb}} {
		if err := x.a.KeepAlive(ctx, x.h, T); err != nil {
			t.Fatalf("KeepAlive(%s): %v", x.h, err)
		}
		closeAtEnd(t, x.a, x.h)
	}
	term := mustAcquire(t, a, ha, "markets", 0)
	shard := shardName(g, "markets", 0)
	startRenewer(t, a, ha)
	startRenewer(t, b, hb)

	// The informers' start, before the window.
	for _, x := range []struct {
		name string
		rec  *recorder
	}{{"a", recA}, {"b", recB}, {"m", recM}} {
		lists, watches := x.rec.filter(on(verbList, "")), x.rec.filter(on(verbWatch, ""))
		t.Logf("%s: informer start %v, %v", x.name, lists, watches)
		switch {
		case len(watches) != 1:
			t.Errorf("%s: informer start %v, %v; want one WATCH", x.name, lists, watches)
		case len(lists) == 0 && watches[0].query.Get("sendInitialEvents") != "true":
			t.Errorf("%s: informer start with WATCH %v and no LIST; want the WATCH to send the initial events",
				x.name, watches[0].query)
		case len(lists) > 1:
			t.Errorf("%s: informer start %v, %v; want at most one LIST", x.name, lists, watches)
		}
	}

	// Settle: b's and m's informers deliver the grant, every renewer and heartbeat runs.
	time.Sleep(2 * T.RetryPeriod)
	const window = 10 * time.Second
	for _, rec := range []*recorder{recA, recB, recM} {
		rec.reset()
	}
	time.Sleep(window)
	for _, x := range []struct {
		name string
		rec  *recorder
		id   string
	}{{"a", recA, "pod-a"}, {"b", recB, "pod-b"}, {"m", recM, "pod-m"}} {
		own := on(verbUpdate, memberName(g, x.id))
		var updates int
		var other []request
		for _, r := range x.rec.requests() {
			if own(r) {
				updates++
			} else {
				other = append(other, r)
			}
		}
		if updates < 19 || updates > 21 {
			t.Errorf("%s: %d updates of its member Lease in %v, want 20 ± 1", x.name, updates, window)
		}
		if len(other) != 0 {
			t.Errorf("%s: requests %v in %v, want only updates of its member Lease", x.name, other, window)
		}
	}

	// A move: two writes, one in each recording.
	for _, rec := range []*recorder{recA, recB, recM} {
		rec.reset()
	}
	if err := a.Release(ctx, ha, "markets", 0, term); err != nil {
		t.Fatalf("Release: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if o, ok := b.cached(shard); ok && holderOf(o.lease) == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("b has not observed the release in 2s")
		}
		time.Sleep(time.Millisecond)
	}
	if got, err := b.Acquire(ctx, hb, "markets", 0); err != nil || got <= term {
		t.Fatalf("Acquire(%s) = %d, %v; want a term above %d", hb, got, err, term)
	}
	for _, x := range []struct {
		name string
		rec  *recorder
		want []string
	}{
		{"a", recA, []string{"UPDATE " + shard + " 200"}},
		{"b", recB, []string{"UPDATE " + shard + " 200"}},
		{"m", recM, nil},
	} {
		var got []string
		for _, r := range x.rec.filter(shardRequests(g)) {
			got = append(got, r.String())
		}
		if len(got) != len(x.want) || (len(got) == 1 && got[0] != x.want[0]) {
			t.Errorf("%s: shard requests %q during the move, want %q", x.name, got, x.want)
		}
		if reads := append(x.rec.filter(on(verbGet, "")), x.rec.filter(on(verbList, ""))...); len(reads) != 0 {
			t.Errorf("%s: reads %v during the move, want none", x.name, reads)
		}
	}
}
