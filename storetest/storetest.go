// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package storetest is the conformance suite for tenure.Store implementations. Every store
// adapter runs it unchanged; a store that passes it keeps the contract the tenure runtime relies
// on.
//
// An adapter calls Run from one of its tests, with a function that sets up a fresh, empty store
// for each case:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) storetest.Fixture {
//			st := mystore.New(t) // a fresh, empty backend; clean it up with t.Cleanup
//			return storetest.Fixture{
//				Store:  st,
//				Peer:   func() tenure.Store { return mystore.Connect(st) },
//				Timing: core.Timing{LeaseDuration: 4 * time.Second, RenewDeadline: 3 * time.Second,
//					RetryPeriod: time.Second, SlotGrace: 4 * time.Second},
//				Slack: 500 * time.Millisecond,
//			}
//		})
//	}
//
// Run starts every case as a parallel subtest named <Group>/<Case>, such as
// Session/RenewalKeepsRecords. Each case calls newStore once with its own *testing.T, calls
// Fixture.Peer once when it is set, checks the fixture and then runs against that store alone,
// so newStore must return a store that shares no records or sessions with any other.
//
// A fixture with Advance runs in virtual time: the cases move the store's clock only through
// Advance and never sleep, so a store whose clock the test controls runs the suite in a moment.
// A fixture without Advance runs in real time: the cases sleep, so keep Timing short; its
// LeaseDuration may be at most 10s. Cases that need something the fixture or the store lacks,
// such as Advance, SetWall, DeleteRecord, Peer, tenure.SlotStore or tenure.Watcher, skip and say
// what is missing.
//
// When Peer is set, the cases act as several clients of one backend: holders with an even member
// number call through the store Peer returned, all others through Store. In real time a peer may
// lag behind Store: a read whose result a case checks is retried every 10ms for at most Slack
// before the case fails. A write that the store refuses with a *tenure.HeldError,
// tenure.ErrSessionLost or tenure.ErrNoSlot is never retried.
//
// The cases use only spaces "a", "b" and "c", shards 0 to 19, slots 0 to 7, and holders
// "m<i>/<incarnation>" for members m1 to m20, the incarnation being 32 lowercase hex digits.
// A failure reads "<Method>(<args>) = <got>, want <want>", with strings quoted, errors as their
// text and nil as nil.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Fixture is one store under test, with what the suite needs to drive it.
type Fixture struct {
	// Store is the store under test. It is required.
	Store tenure.Store

	// Peer, when set, returns a second client of the same backend as Store. Holders with an
	// even member number call through it.
	Peer func() tenure.Store

	// Timing is the Timing of every session a case opens, unless the case says otherwise. It
	// must pass Validate.
	Timing core.Timing

	// Advance, when set, moves the store's clock forward by d, monotonic and wall time alike;
	// the suite then runs in virtual time and never sleeps. When nil, the suite runs in real
	// time.
	Advance func(d time.Duration)

	// Slack is how long, in real time, a read through Peer may lag behind a write through
	// Store. It must not be negative.
	Slack time.Duration

	// SetWall, when set, sets the store's wall clock to t, without moving its monotonic clock.
	SetWall func(t time.Time)

	// DeleteRecord, when set, removes the record of the shard from the backend, as a lost record
	// would be.
	DeleteRecord func(space string, shard int)
}

// Run runs every conformance case against stores from newStore, each as a parallel subtest of t
// named <Group>/<Case>. newStore is called once per case, with the case's *testing.T, and must
// return a fresh, empty store each time.
func Run(t *testing.T, newStore func(t *testing.T) Fixture) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := newStore(t)
			var peer tenure.Store
			if f.Peer != nil {
				peer = f.Peer()
			}
			validate(t, f, peer)
			checkNeeds(t, f, peer, c.needs)
			c.run(newEnv(t, f, peer))
		})
	}
}

// needs is the set of optional features a case needs.
type needs uint8

const (
	needAdvance needs = 1 << iota
	needSetWall
	needDeleteRecord
	needPeer
	needSlots
	needWatch
)

// testCase is one entry of the case table.
type testCase struct {
	name  string
	needs needs
	run   func(e *env)
}

// cases is the case table, in the order Run starts them.
var cases = []testCase{
	{"Session/RenewalKeepsRecords", 0, sessionRenewalKeepsRecords},
	{"Session/ExpiryBoundary", needAdvance, sessionExpiryBoundary},
	{"Session/Expired", 0, sessionExpired},
	{"Session/LateRenewal", 0, sessionLateRenewal},
	{"Session/Replace", 0, sessionReplace},
	{"Session/ReplacedForGood", 0, sessionReplacedForGood},
	{"Session/Close", 0, sessionClose},
	{"Session/CloseUnknown", 0, sessionCloseUnknown},
	{"Session/HolderLease", 0, sessionHolderLease},
	{"Session/RetryAfter", 0, sessionRetryAfter},
	{"Acquire/Fresh", 0, acquireFresh},
	{"Acquire/Held", 0, acquireHeld},
	{"Acquire/Reacquire", 0, acquireReacquire},
	{"Acquire/AfterRelease", 0, acquireAfterRelease},
	{"Acquire/Independent", 0, acquireIndependent},
	{"Term/WallAhead", needSetWall, termWallAhead},
	{"Term/WallBehind", needSetWall, termWallBehind},
	{"Term/LostRecord", needDeleteRecord, termLostRecord},
	{"Term/LostRecordWallBehind", needSetWall | needDeleteRecord, termLostRecordWallBehind},
	{"Term/Sequence", 0, termSequence},
	{"Release/Match", 0, releaseMatch},
	{"Release/WrongTerm", 0, releaseWrongTerm},
	{"Release/WrongHolder", 0, releaseWrongHolder},
	{"Release/Late", 0, releaseLate},
	{"Release/Absent", 0, releaseAbsent},
	{"Release/Twice", 0, releaseTwice},
	{"Release/EndedSession", 0, releaseEndedSession},
	{"CloseSession/Stale", 0, closeSessionStale},
	{"Holdings/Order", 0, holdingsOrder},
	{"Holdings/Version", 0, holdingsVersion},
	{"Confirm/Held", 0, confirmHeld},
	{"Confirm/Other", 0, confirmOther},
	{"Confirm/WrongTerm", 0, confirmWrongTerm},
	{"Confirm/AfterTakeover", 0, confirmAfterTakeover},
	{"Confirm/NoSession", 0, confirmNoSession},
}

// maxRealTimeLease is the longest LeaseDuration a real-time fixture may have, so that the
// suite's sleeps stay short.
const maxRealTimeLease = 10 * time.Second

// syncInterval is how often a real-time read is retried while a peer may lag.
const syncInterval = 10 * time.Millisecond

// validate fails the case when the fixture is unusable. The first failing check wins (STO-810).
func validate(t *testing.T, f Fixture, peer tenure.Store) {
	t.Helper()
	switch {
	case f.Store == nil:
		t.Fatal("storetest: Fixture.Store is nil")
	case f.Timing.Validate() != nil:
		t.Fatal("storetest: Fixture.Timing: " + f.Timing.Validate().Error())
	case f.Slack < 0:
		t.Fatalf("storetest: Fixture.Slack %v is negative", f.Slack)
	case f.Peer != nil && peer == nil:
		t.Fatal("storetest: Fixture.Peer returned nil")
	case f.Advance == nil && f.Timing.LeaseDuration > maxRealTimeLease:
		t.Fatalf("storetest: real-time Fixture.Timing.LeaseDuration %v is over %v",
			f.Timing.LeaseDuration, maxRealTimeLease)
	}
}

// checkNeeds skips the case when the fixture or the store lacks a feature it needs, checking the
// features in a fixed order (STO-830).
func checkNeeds(t *testing.T, f Fixture, peer tenure.Store, n needs) {
	t.Helper()
	_, slots := f.Store.(tenure.SlotStore)
	_, watch := f.Store.(tenure.Watcher)
	if peer != nil {
		_, peerSlots := peer.(tenure.SlotStore)
		_, peerWatch := peer.(tenure.Watcher)
		slots = slots && peerSlots
		watch = watch && peerWatch
	}
	switch {
	case n&needAdvance != 0 && f.Advance == nil:
		t.Skip("storetest: fixture has no Advance")
	case n&needSetWall != 0 && f.SetWall == nil:
		t.Skip("storetest: fixture has no SetWall")
	case n&needDeleteRecord != 0 && f.DeleteRecord == nil:
		t.Skip("storetest: fixture has no DeleteRecord")
	case n&needPeer != 0 && peer == nil:
		t.Skip("storetest: fixture has no Peer")
	case n&needSlots != 0 && !slots:
		t.Skip("storetest: store does not implement tenure.SlotStore")
	case n&needWatch != 0 && !watch:
		t.Skip("storetest: store does not implement tenure.Watcher")
	}
}

// holder returns incarnation k of member i, the only holder strings the suite uses.
func holder(i, k int) string { return fmt.Sprintf("m%d/%032x", i, k) }

// shardKey names one shard.
type shardKey struct {
	space string
	shard int
}

// env is one running case: its fixture, its clients and what it has learned about the store.
type env struct {
	t    *testing.T
	ctx  context.Context
	f    Fixture
	peer tenure.Store

	// timings holds each holder's Timing as of its last successful KeepAlive.
	timings map[string]core.Timing

	// terms holds the latest term each shard was granted at in this case.
	terms map[shardKey]core.Term
}

func newEnv(t *testing.T, f Fixture, peer tenure.Store) *env {
	return &env{
		t:       t,
		ctx:     t.Context(),
		f:       f,
		peer:    peer,
		timings: make(map[string]core.Timing),
		terms:   make(map[shardKey]core.Term),
	}
}

// client returns the store h calls through: the peer for an even member number when there is a
// peer, else Store.
func (e *env) client(h string) tenure.Store {
	if e.peer == nil {
		return e.f.Store
	}
	digits, _, _ := strings.Cut(strings.TrimPrefix(h, "m"), "/")
	if i, err := strconv.Atoi(digits); err == nil && i%2 == 0 {
		return e.peer
	}
	return e.f.Store
}

// timingOf returns the Timing h's session last stored, or the fixture's.
func (e *env) timingOf(h string) core.Timing {
	if tm, ok := e.timings[h]; ok {
		return tm
	}
	return e.f.Timing
}

// open opens or renews h's session with the fixture's Timing.
func (e *env) open(h string) {
	e.t.Helper()
	e.openWith(h, e.f.Timing)
}

// openWith opens or renews h's session with tm.
func (e *env) openWith(h string, tm core.Timing) {
	e.t.Helper()
	if err := e.client(h).KeepAlive(e.ctx, h, tm); err != nil {
		e.t.Fatalf("KeepAlive(%q, %v) = %s, want nil", h, tm, errText(err))
	}
	e.timings[h] = tm
}

// lost checks that h's KeepAlive returns tenure.ErrSessionLost.
func (e *env) lost(h string) {
	e.t.Helper()
	tm := e.timingOf(h)
	if err := e.client(h).KeepAlive(e.ctx, h, tm); !errors.Is(err, tenure.ErrSessionLost) {
		e.t.Fatalf("KeepAlive(%q, %v) = %s, want %s", h, tm, errText(err), tenure.ErrSessionLost)
	}
}

// lostAcq checks that h's Acquire of the shard returns 0 and tenure.ErrSessionLost.
func (e *env) lostAcq(h, space string, shard int) {
	e.t.Helper()
	term, err := e.client(h).Acquire(e.ctx, h, space, shard)
	if term != 0 || !errors.Is(err, tenure.ErrSessionLost) {
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, %s, want 0, %s", h, space, shard, term, errText(err),
			tenure.ErrSessionLost)
	}
}

// acq checks that h is granted the shard at a term above every term the case has seen it
// granted at, and returns the term.
func (e *env) acq(h, space string, shard int) core.Term {
	e.t.Helper()
	key := shardKey{space, shard}
	prev := e.terms[key]
	term, err := e.client(h).Acquire(e.ctx, h, space, shard)
	if err != nil || term == 0 || term <= prev {
		want := "a term"
		if prev != 0 {
			want = fmt.Sprintf("a term above %d", prev)
		}
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, %s, want %s, nil", h, space, shard, term, errText(err), want)
	}
	e.terms[key] = term
	return term
}

// held checks that h's Acquire of the shard returns 0 and a *tenure.HeldError naming by, with
// the RetryPeriod of h's Timing as RetryAfter.
func (e *env) held(h, space string, shard int, by string) {
	e.t.Helper()
	want := &tenure.HeldError{Holder: by, RetryAfter: e.timingOf(h).RetryPeriod}
	term, err := e.client(h).Acquire(e.ctx, h, space, shard)
	var he *tenure.HeldError
	if term != 0 || !errors.As(err, &he) || *he != *want {
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, %s, want 0, %s", h, space, shard, term, errText(err), want)
	}
}

// acqIn checks that h is granted the shard at a term in [lo, hi], and returns the term. Unlike
// acq it allows a term below those the case has seen, as after a lost record.
func (e *env) acqIn(h, space string, shard int, lo, hi core.Term) core.Term {
	e.t.Helper()
	term, err := e.client(h).Acquire(e.ctx, h, space, shard)
	if err != nil || term < lo || term > hi {
		want := fmt.Sprintf("a term in [%d, %d]", lo, hi)
		if lo == hi {
			want = strconv.FormatUint(uint64(lo), 10)
		}
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, %s, want %s, nil", h, space, shard, term, errText(err), want)
	}
	e.terms[shardKey{space, shard}] = term
	return term
}

// release checks that h's Release of the shard at term returns nil.
func (e *env) release(h, space string, shard int, term core.Term) {
	e.t.Helper()
	if err := e.client(h).Release(e.ctx, h, space, shard, term); err != nil {
		e.t.Fatalf("Release(%q, %q, %d, %d) = %s, want nil", h, space, shard, term, errText(err))
	}
}

// closeSession checks that h's CloseSession returns nil.
func (e *env) closeSession(h string) {
	e.t.Helper()
	if err := e.client(h).CloseSession(e.ctx, h); err != nil {
		e.t.Fatalf("CloseSession(%q) = %s, want nil", h, errText(err))
	}
}

// rec checks, through via's client, that the shard's record names holder at term, and returns
// the record. holder "" means no holder; term 0 means no record. The read is synced.
func (e *env) rec(via, space string, shard int, holder string, term core.Term) tenure.Holding {
	e.t.Helper()
	want := tenure.Holding{Space: space, Shard: shard, Holder: holder, Term: term}
	var got tenure.Holding
	e.sync(func() string {
		hs, err := e.client(via).Holdings(e.ctx, space)
		if err != nil {
			return fmt.Sprintf("Holdings(%q) = %v, %s, want a record %s, nil", space, hs, errText(err),
				fmtWant(want))
		}
		got = tenure.Holding{Space: space, Shard: shard}
		for _, h := range hs {
			if h.Shard == shard {
				got = h
				break
			}
		}
		if got.Space != space || got.Holder != holder || got.Term != term {
			return fmt.Sprintf("Holdings(%q) = a record %s, nil, want a record %s, nil", space,
				fmtHolding(got), fmtWant(want))
		}
		return ""
	})
	return got
}

// ver checks that the record got, read after a write to it, has a version above that of prev,
// the same record read before the write.
func (e *env) ver(prev, got tenure.Holding) {
	e.t.Helper()
	if got.Version <= prev.Version {
		e.t.Fatalf("Holdings(%q) = a record %s, nil, want a record of shard %d with Version above %d, nil",
			got.Space, fmtHolding(got), got.Shard, prev.Version)
	}
}

// same checks that got, the record read after a call that must leave it as it is, equals prev,
// the same record read before the call, Version included.
func (e *env) same(prev, got tenure.Holding) {
	e.t.Helper()
	if got != prev {
		e.t.Fatalf("Holdings(%q) = a record %s, nil, want a record %s, nil", got.Space,
			fmtHolding(got), fmtHolding(prev))
	}
}

// conf checks that h's Confirm of the shard at term returns the record naming holder at
// recTerm. The read is synced.
func (e *env) conf(h, space string, shard int, term core.Term, holder string, recTerm core.Term) {
	e.t.Helper()
	want := tenure.Holding{Space: space, Shard: shard, Holder: holder, Term: recTerm}
	e.sync(func() string {
		got, err := e.client(h).Confirm(e.ctx, h, space, shard, term)
		if err != nil || got.Space != space || got.Shard != shard || got.Holder != holder || got.Term != recTerm {
			return fmt.Sprintf("Confirm(%q, %q, %d, %d) = %s, %s, want %s, nil", h, space, shard, term,
				fmtHolding(got), errText(err), fmtWant(want))
		}
		return ""
	})
}

// lostConf checks that h's Confirm of the shard at term returns tenure.ErrSessionLost.
func (e *env) lostConf(h, space string, shard int, term core.Term) {
	e.t.Helper()
	got, err := e.client(h).Confirm(e.ctx, h, space, shard, term)
	if !errors.Is(err, tenure.ErrSessionLost) {
		e.t.Fatalf("Confirm(%q, %q, %d, %d) = %s, %s, want %s", h, space, shard, term, fmtHolding(got),
			errText(err), tenure.ErrSessionLost)
	}
}

// elapse lets d pass in steps of at most the fixture's RetryPeriod: through Advance in virtual
// time, by sleeping in real time. After each full step it renews the keep holders, in order.
func (e *env) elapse(d time.Duration, keep ...string) {
	e.t.Helper()
	step := e.f.Timing.RetryPeriod
	for d > 0 {
		s := min(step, d)
		if e.f.Advance != nil {
			e.f.Advance(s)
		} else {
			time.Sleep(s)
		}
		d -= s
		if s == step {
			for _, h := range keep {
				e.openWith(h, e.timingOf(h))
			}
		}
	}
}

// expire lets the fixture's LeaseDuration pass, renewing only the keep holders, so that every
// other session opened or renewed before the call expires.
func (e *env) expire(keep ...string) {
	e.t.Helper()
	e.elapse(e.f.Timing.LeaseDuration, keep...)
}

// sync runs read, which returns "" when the store shows what the case expects and a failure
// message otherwise. In virtual time it calls Advance(0) and reads once. In real time it retries
// the read every 10ms for at most the fixture's Slack, since a peer may lag. The case fails with
// the last message.
func (e *env) sync(read func() string) {
	e.t.Helper()
	if e.f.Advance != nil {
		e.f.Advance(0)
		if msg := read(); msg != "" {
			e.t.Fatal(msg)
		}
		return
	}
	deadline := time.Now().Add(e.f.Slack)
	for {
		msg := read()
		if msg == "" {
			return
		}
		if !time.Now().Before(deadline) {
			e.t.Fatal(msg)
		}
		time.Sleep(syncInterval)
	}
}

// errText returns err's text, or "nil".
func errText(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

// fmtHolding formats a record in full.
func fmtHolding(h tenure.Holding) string {
	return fmt.Sprintf("{Space: %q, Shard: %d, Holder: %q, Term: %d, Version: %d}",
		h.Space, h.Shard, h.Holder, h.Term, h.Version)
}

// fmtWant formats an expected record, whose version the case does not check.
func fmtWant(h tenure.Holding) string {
	return fmt.Sprintf("{Space: %q, Shard: %d, Holder: %q, Term: %d}", h.Space, h.Shard, h.Holder, h.Term)
}
