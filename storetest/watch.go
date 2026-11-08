// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import (
	"context"
	"math"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The Watch cases check tenure.Watcher: a channel receives every shard-record write committed
// after WatchHolders returns, in commit order, as the record after the write with a version
// above that of every earlier write, and closes once its context ends. Each channel is opened
// through Store; every receive waits at most watchWait of real time.

// watchWait is the longest a case waits, in real time, for a watch channel to send or close.
const watchWait = 10 * time.Second

// watchGrant: a grant, a re-grant included, arrives as the record after the write, with the
// holder, the term and a larger version, the version Holdings then shows; a refused Acquire
// sends nothing.
func watchGrant(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	w := e.watch(e.ctx)

	t1 := e.acq(h1, "a", 0)
	v1 := w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t1})
	t2 := e.acq(h2, "b", 3)
	v2 := w.next(tenure.Holding{Space: "b", Shard: 3, Holder: h2, Term: t2})
	e.same(v1, e.rec(h2, "a", 0, h1, t1))
	e.same(v2, e.rec(h1, "b", 3, h2, t2))

	e.held(h2, "a", 0, h1)
	t3 := e.acq(h1, "a", 0)
	v3 := w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t3})
	e.same(v3, e.rec(h2, "a", 0, h1, t3))
}

// watchRelease: a Release that clears a record arrives as the record with no holder, the term
// kept and a larger version; a Release that matches nothing sends nothing.
func watchRelease(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	w := e.watch(e.ctx)

	t1 := e.acq(h1, "a", 0)
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t1})
	e.release(h1, "a", 0, t1)
	v := w.next(tenure.Holding{Space: "a", Shard: 0, Term: t1})
	e.release(h1, "a", 0, t1)
	e.same(v, e.rec(h2, "a", 0, "", t1))

	t2 := e.acq(h2, "a", 0)
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h2, Term: t2})
	e.rec(h1, "a", 0, h2, t2)
	e.release(h1, "a", 0, t2)
	e.release(h2, "a", 0, t1)
	e.release(h2, "a", 0, t2)
	v = w.next(tenure.Holding{Space: "a", Shard: 0, Term: t2})
	e.same(v, e.rec(h1, "a", 0, "", t2))
}

// watchCloseSession: CloseSession sends one value per record it clears, each with no holder,
// the term kept and a larger version, and nothing for the records of other holders; a second
// CloseSession sends nothing.
func watchCloseSession(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	w := e.watch(e.ctx)

	ta0 := e.acq(h1, "a", 0)
	ta2 := e.acq(h1, "a", 2)
	tb1 := e.acq(h1, "b", 1)
	tb2 := e.acq(h2, "b", 2)
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: ta0})
	w.next(tenure.Holding{Space: "a", Shard: 2, Holder: h1, Term: ta2})
	w.next(tenure.Holding{Space: "b", Shard: 1, Holder: h1, Term: tb1})
	w.next(tenure.Holding{Space: "b", Shard: 2, Holder: h2, Term: tb2})

	e.closeSession(h1)
	w.nextSet(
		tenure.Holding{Space: "a", Shard: 0, Term: ta0},
		tenure.Holding{Space: "a", Shard: 2, Term: ta2},
		tenure.Holding{Space: "b", Shard: 1, Term: tb1},
	)
	tc0 := e.acq(h2, "c", 0)
	w.next(tenure.Holding{Space: "c", Shard: 0, Holder: h2, Term: tc0})

	e.closeSession(h1)
	tc1 := e.acq(h2, "c", 1)
	w.next(tenure.Holding{Space: "c", Shard: 1, Holder: h2, Term: tc1})
}

// watchOrder: writes to records of several spaces and shards, through both clients, arrive in
// the order they were committed, each with a version above that of the value before it.
func watchOrder(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	w := e.watch(e.ctx)

	var want []tenure.Holding
	grant := func(h, space string, shard int) {
		e.t.Helper()
		want = append(want, tenure.Holding{Space: space, Shard: shard, Holder: h, Term: e.acq(h, space, shard)})
	}
	drop := func(h, space string, shard int, term core.Term) {
		e.t.Helper()
		e.release(h, space, shard, term)
		want = append(want, tenure.Holding{Space: space, Shard: shard, Term: term})
	}
	grant(h1, "a", 0)
	grant(h2, "b", 1)
	grant(h3, "c", 2)
	drop(h1, "a", 0, want[0].Term)
	grant(h2, "a", 1)
	e.rec(h2, "a", 0, "", want[0].Term)
	grant(h2, "a", 0)
	grant(h1, "b", 0)
	drop(h3, "c", 2, want[2].Term)
	grant(h1, "c", 2)
	drop(h2, "b", 1, want[1].Term)
	e.rec(h3, "b", 1, "", want[1].Term)
	grant(h3, "b", 1)

	var top uint64
	for _, wv := range want {
		got := w.next(wv)
		if got.Version <= top {
			e.t.Fatalf("WatchHolders() = a channel that sent %s after Version %d, want a Version above %d",
				fmtHolding(got), top, top)
		}
		top = got.Version
	}
}

// watchVersions: the versions of each record's writes grow, through grants, re-grants, clears
// and grants to another holder, and each record's latest value carries the version Holdings
// shows.
func watchVersions(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	w := e.watch(e.ctx)

	latest := make(map[shardKey]tenure.Holding)
	grant := func(h, space string, shard int) core.Term {
		e.t.Helper()
		t := e.acq(h, space, shard)
		latest[shardKey{space, shard}] = w.next(tenure.Holding{Space: space, Shard: shard, Holder: h, Term: t})
		return t
	}
	drop := func(h, space string, shard int, term core.Term) {
		e.t.Helper()
		e.release(h, space, shard, term)
		latest[shardKey{space, shard}] = w.next(tenure.Holding{Space: space, Shard: shard, Term: term})
	}

	grant(h1, "a", 0)
	grant(h2, "b", 4)
	grant(h1, "a", 0)
	ta0 := grant(h1, "a", 0)
	tb4 := grant(h2, "b", 4)
	drop(h1, "a", 0, ta0)
	e.rec(h2, "a", 0, "", ta0)
	grant(h2, "a", 0)
	drop(h2, "b", 4, tb4)
	e.rec(h1, "b", 4, "", tb4)
	tb4 = grant(h1, "b", 4)
	ta0 = grant(h2, "a", 0)
	drop(h2, "a", 0, ta0)

	e.same(latest[shardKey{"a", 0}], e.rec(h1, "a", 0, "", ta0))
	e.same(latest[shardKey{"b", 4}], e.rec(h2, "b", 4, h1, tb4))
}

// watchDelete: a lost record arrives as a value with only Space, Shard and a larger Version set,
// and the next grant of the shard arrives with a larger version again.
func watchDelete(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	w := e.watch(e.ctx)

	t1 := e.acq(h1, "a", 0)
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t1})
	e.f.DeleteRecord("a", 0)
	w.next(tenure.Holding{Space: "a", Shard: 0})
	e.rec(h1, "a", 0, "", 0)
	e.rec(h2, "a", 0, "", 0)

	t2 := e.acqIn(h1, "a", 0, 1, exactly(math.MaxUint64))
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t2})
	t3 := e.acq(h2, "a", 1)
	w.next(tenure.Holding{Space: "a", Shard: 1, Holder: h2, Term: t3})
}

// watchCancel: once the context of a watch ends, its channel is closed, and a channel opened
// later still receives every write.
func watchCancel(e *env) {
	h1 := holder(1, 1)
	e.open(h1)
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	w := e.watch(ctx)

	t1 := e.acq(h1, "a", 0)
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t1})
	cancel()
	e.acq(h1, "a", 1)
	w.closed()

	w2 := e.watch(e.ctx)
	t2 := e.acq(h1, "a", 2)
	w2.next(tenure.Holding{Space: "a", Shard: 2, Holder: h1, Term: t2})
}

// watchCanceled: WatchHolders with a context already canceled or past its deadline returns the
// context's error itself, and a watch opened afterwards works.
func watchCanceled(e *env) {
	h1 := holder(1, 1)
	e.open(h1)
	wt, _ := e.f.Store.(tenure.Watcher)

	canceled, cancel := context.WithCancel(e.ctx)
	cancel()
	if _, err := wt.WatchHolders(canceled); err != canceled.Err() {
		e.t.Fatalf("WatchHolders(a canceled context) = %s, want %s", errText(err), errText(canceled.Err()))
	}
	expired, cancel := context.WithDeadline(e.ctx, time.Unix(0, 0))
	defer cancel()
	if _, err := wt.WatchHolders(expired); err != expired.Err() {
		e.t.Fatalf("WatchHolders(a context past its deadline) = %s, want %s", errText(err),
			errText(expired.Err()))
	}

	w := e.watch(e.ctx)
	t1 := e.acq(h1, "a", 0)
	w.next(tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: t1})
}

// watchMultiple: two channels open at once both receive every write, and one keeps receiving
// after the other's context ends.
func watchMultiple(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	ctx1, cancel1 := context.WithCancel(e.ctx)
	defer cancel1()
	w1 := e.watch(ctx1)
	w2 := e.watch(e.ctx)

	ta := e.acq(h1, "a", 0)
	tb := e.acq(h2, "b", 1)
	e.release(h1, "a", 0, ta)
	want := []tenure.Holding{
		{Space: "a", Shard: 0, Holder: h1, Term: ta},
		{Space: "b", Shard: 1, Holder: h2, Term: tb},
		{Space: "a", Shard: 0, Term: ta},
	}
	for _, w := range []*watch{w1, w2} {
		for _, wv := range want {
			w.next(wv)
		}
	}

	cancel1()
	w1.closed()
	tc := e.acq(h2, "c", 2)
	w2.next(tenure.Holding{Space: "c", Shard: 2, Holder: h2, Term: tc})
}

// watchStores reports whether the store, and the peer when there is one, implement
// tenure.Watcher.
func (e *env) watchStores() bool {
	_, ok := e.f.Store.(tenure.Watcher)
	if e.peer != nil {
		_, peerOK := e.peer.(tenure.Watcher)
		ok = ok && peerOK
	}
	return ok
}

// watch is one WatchHolders channel a case reads.
type watch struct {
	e  *env
	ch <-chan tenure.Holding

	// last holds the version of each record's latest value received.
	last map[shardKey]uint64
}

// watch opens a watch through Store with ctx and checks that WatchHolders returns a channel and
// nil. Call it only when watchStores reports true.
func (e *env) watch(ctx context.Context) *watch {
	e.t.Helper()
	wt, _ := e.f.Store.(tenure.Watcher)
	ch, err := wt.WatchHolders(ctx)
	if err != nil || ch == nil {
		got := "a channel"
		if ch == nil {
			got = "nil"
		}
		e.t.Fatalf("WatchHolders() = %s, %s, want a channel, nil", got, errText(err))
	}
	return &watch{e: e, ch: ch, last: make(map[shardKey]uint64)}
}

// recv returns the next value the channel sends, waiting at most watchWait. want describes the
// value expected, for the failure message. In virtual time it calls Advance(0) first.
func (w *watch) recv(want string) tenure.Holding {
	w.e.t.Helper()
	if w.e.f.Advance != nil {
		w.e.f.Advance(0)
	}
	timer := time.NewTimer(watchWait)
	defer timer.Stop()
	select {
	case got, ok := <-w.ch:
		if !ok {
			w.e.t.Fatalf("WatchHolders() = a closed channel, want a channel that sends %s", want)
		}
		return got
	case <-timer.C:
		w.e.t.Fatalf("WatchHolders() = a channel that sent nothing for %v, want a channel that sends %s",
			watchWait, want)
	}
	return tenure.Holding{}
}

// next checks that the next value the channel sends is the record want, with a version above
// that of every earlier value of the record, and returns it. want's Version is not compared; a
// want with no holder and term 0 is a deletion.
func (w *watch) next(want tenure.Holding) tenure.Holding {
	w.e.t.Helper()
	key := shardKey{want.Space, want.Shard}
	got := w.recv(fmtWant(want))
	if got.Space != want.Space || got.Shard != want.Shard || got.Holder != want.Holder ||
		got.Term != want.Term || got.Version <= w.last[key] {
		w.e.t.Fatalf("WatchHolders() = a channel that sent %s, want %s with Version above %d",
			fmtHolding(got), fmtWant(want), w.last[key])
	}
	w.last[key] = got.Version
	return got
}

// nextSet checks that the next len(wants) values the channel sends are the records wants, in
// any order, each with a version above that of every earlier value of the record.
func (w *watch) nextSet(wants ...tenure.Holding) {
	w.e.t.Helper()
	left := append([]tenure.Holding(nil), wants...)
	for range wants {
		got := w.recv("one of " + fmtHoldings(left, fmtWant))
		i := -1
		for j, want := range left {
			if got.Space == want.Space && got.Shard == want.Shard && got.Holder == want.Holder &&
				got.Term == want.Term {
				i = j
				break
			}
		}
		key := shardKey{got.Space, got.Shard}
		if i < 0 || got.Version <= w.last[key] {
			w.e.t.Fatalf("WatchHolders() = a channel that sent %s, want one of %s with Version above that of the record's earlier values",
				fmtHolding(got), fmtHoldings(left, fmtWant))
		}
		w.last[key] = got.Version
		left = append(left[:i], left[i+1:]...)
	}
}

// closed checks that the channel is closed within watchWait, taking any values it still sends
// first.
func (w *watch) closed() {
	w.e.t.Helper()
	if w.e.f.Advance != nil {
		w.e.f.Advance(0)
	}
	timer := time.NewTimer(watchWait)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-w.ch:
			if !ok {
				return
			}
		case <-timer.C:
			w.e.t.Fatalf("WatchHolders() = a channel still open %v after its context ended, want a closed channel",
				watchWait)
		}
	}
}
