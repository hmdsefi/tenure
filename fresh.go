// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// errStoreWatchEnded is the error the node reports when a channel of the store's WatchHolders
// closes while the node runs.
var errStoreWatchEnded = errors.New("tenure: store watch ended")

// reading is one observation of a shard record: a value of the store watch, an entry of a
// Holdings answer (or a shard missing from it), or the answer of a Confirm call. Readings reach
// the machine as HolderChanged only through the freshness filter (RUN-175).
type reading struct {
	// h is the record as read. Version 0 means the read found no record: a shard missing from a
	// Holdings answer, or a Confirm answer for an absent record. A watch deletion has Version set
	// and neither Holder nor Term.
	h Holding

	// confirm reports that h is the answer of a Confirm call.
	confirm bool

	// start is the executor stamp taken when the read started: right before a Holdings call, when
	// the watcher received a watch value, and at the start of a Confirm call. end is the stamp
	// taken once the reading was in hand: after a Holdings or Confirm call returned, and for a
	// watch value the same as start.
	start, end uint64
}

// absent reports that rd shows no record of the shard. Every record has a term once granted, so
// a reading with neither a version nor a term is absent.
func (rd reading) absent() bool { return rd.h.Version == 0 || rd.h.Term == 0 }

// shardFresh is the freshness state of one shard. floor and last are kept for the node's life;
// grant and confirm belong to one machine.
type shardFresh struct {
	// floor is the highest record Version stepped as HolderChanged (MCH O4).
	floor uint64

	// last is the end stamp of the latest reading stepped, or of the latest Acquire that
	// granted the shard, whichever is later: a reading of no record without a version is stepped
	// only when its read started after it.
	last uint64

	// grant is the term of the shard's latest Acquired with a grant in this machine. Terms grow,
	// so a record with a smaller, non-zero term was read before that grant.
	grant core.Term

	// confirm is the tag of the shard's latest Confirm action in this machine, 0 for none.
	confirm uint64
}

// freshness filters the readings of shard records before they reach the machine as
// HolderChanged (RUN-175, MCH O4 and O5). Only the loop goroutine uses it.
type freshness struct {
	holder string
	names  []string                // the space names, sorted, as Status lists the spaces
	shards map[string][]shardFresh // by space name, indexed by shard

	// started returns the tag and the start stamp of the latest Confirm call that went out for a
	// shard, as executor.confirmStarted does.
	started func(space string, shard int) (tag, start uint64)

	// issued and answered count the Confirm calls queued and answered in this machine. A resync
	// due after a renewal (RUN-170) waits, with wait set, until answered reaches resyncAt: the
	// count issued once the renewal's step was carried out, so its Confirm calls go out first.
	issued, answered uint64
	wait             bool
	resyncAt         uint64
}

// newFreshness returns the freshness state of spaces for holder, with every floor at 0.
func newFreshness(holder string, spaces []Space, started func(space string, shard int) (tag, start uint64)) *freshness {
	f := &freshness{holder: holder, shards: make(map[string][]shardFresh, len(spaces)), started: started}
	for _, sp := range spaces {
		f.names = append(f.names, sp.Name)
		f.shards[sp.Name] = make([]shardFresh, sp.Shards)
	}
	slices.Sort(f.names)
	return f
}

// shard returns the state of a shard, nil for a space or shard the node does not have.
func (f *freshness) shard(space string, shard int) *shardFresh {
	s := f.shards[space]
	if shard < 0 || shard >= len(s) {
		return nil
	}
	return &s[shard]
}

// confirmSent notes a Confirm action, which doConfirm gave tag and queued unless queued is
// false. A Confirm that is dropped never goes out, so the shard's readings stay held back until
// the machine sends another.
func (f *freshness) confirmSent(a core.Confirm, tag uint64, queued bool) {
	if s := f.shard(a.Space, a.Shard); s != nil {
		s.confirm = tag
	}
	if queued {
		f.issued++
	}
}

// newMachine starts the state of a new incarnation's machine, whose holder is holder (RUN-180):
// every grant and Confirm tag is cleared, the Confirm counts and a pending resync are dropped,
// and each shard keeps its floor and last, which hold for the store's records whatever machine
// reads them.
func (f *freshness) newMachine(holder string) {
	f.holder = holder
	for _, s := range f.shards {
		for i := range s {
			s[i].grant, s[i].confirm = 0, 0
		}
	}
	f.issued, f.answered, f.wait, f.resyncAt = 0, 0, false, 0
}

// granted notes an Acquired with term > 0, from an Acquire call that ended at stamp end.
func (f *freshness) granted(space string, shard int, term core.Term, end uint64) {
	if s := f.shard(space, shard); s != nil {
		s.grant = term
		s.last = max(s.last, end)
	}
}

// admit applies RUN-175's filter to rd, with st the machine's Status, and returns the
// HolderChanged to step and true, or false when rd is dropped or held back. In order:
//
//  1. a reading of a space or shard the node does not have is dropped;
//  2. a reading with a Version at or below the shard's floor is dropped (O4), except that a
//     Confirm answer may equal it: the store does not move the version on Confirm, and the
//     answer must reach a degraded shard;
//  3. a record whose non-zero term is below the shard's grant is older than that grant, and is
//     dropped;
//  4. a reading of no record without a Version (absent-record rule) is stepped only when its read
//     started after the end of the latest reading stepped and of the latest grant, since it
//     cannot be ordered by version;
//  5. while st shows the shard degraded, a record naming this holder at the run's term is
//     stepped only when its read started at or after the start of the call of the shard's latest
//     Confirm action (O5); before that call has gone out, every such record is held back.
//
// A reading stepped raises the floor to its Version and last to its end stamp. A reading of no
// record steps HolderChanged with no holder and term 0.
func (f *freshness) admit(rd reading, st core.Status) (core.HolderChanged, bool) {
	h := rd.h
	s := f.shard(h.Space, h.Shard)
	if s == nil {
		return core.HolderChanged{}, false
	}
	if h.Version != 0 && (h.Version < s.floor || h.Version == s.floor && !rd.confirm) {
		return core.HolderChanged{}, false
	}
	absent := rd.absent()
	if !absent && h.Term < s.grant {
		return core.HolderChanged{}, false
	}
	if absent && h.Version == 0 && rd.start <= s.last {
		return core.HolderChanged{}, false
	}
	if sh, ok := f.status(st, h.Space, h.Shard); ok && sh.State == core.ShardStateDegraded &&
		!absent && h.Holder == f.holder && h.Term == sh.Term {
		tag, start := f.started(h.Space, h.Shard)
		if s.confirm == 0 || tag != s.confirm || rd.start < start {
			return core.HolderChanged{}, false
		}
	}

	s.floor = max(s.floor, h.Version)
	s.last = max(s.last, rd.end)
	hc := core.HolderChanged{Space: h.Space, Shard: h.Shard}
	if !absent {
		hc.Holder, hc.Term = h.Holder, h.Term
	}
	return hc, true
}

// status returns the shard's entry in st.
func (f *freshness) status(st core.Status, space string, shard int) (core.ShardStatus, bool) {
	i, ok := slices.BinarySearchFunc(st.Spaces, space, func(s core.SpaceStatus, name string) int {
		return strings.Compare(s.Name, name)
	})
	if !ok || shard < 0 || shard >= len(st.Spaces[i].Shards) {
		return core.ShardStatus{}, false
	}
	return st.Spaces[i].Shards[shard], true
}

// readings holds the readings that the store watch and the resyncs hand to the loop, in post
// order. It is safe for concurrent use.
type readings struct {
	mu    sync.Mutex
	rds   []reading
	ready chan struct{} // buffer 1: a value means readings may be waiting
}

// newReadings returns an empty readings.
func newReadings() *readings {
	return &readings{ready: make(chan struct{}, 1)}
}

// post adds rds after the readings waiting.
func (b *readings) post(rds ...reading) {
	b.mu.Lock()
	b.rds = append(b.rds, rds...)
	b.mu.Unlock()
	select {
	case b.ready <- struct{}{}:
	default:
	}
}

// take returns the readings waiting, in post order, and empties b.
func (b *readings) take() []reading {
	b.mu.Lock()
	defer b.mu.Unlock()
	rds := b.rds
	b.rds = nil
	return rds
}

// deliver steps the inputs of r, a result of the executor (RUN-150). A successful Confirm's
// answer is a reading and goes through the freshness filter instead (RUN-175). An Acquired with a
// grant sets the shard's grant. A renewal that ends a run of failed renewals makes a resync of
// every space due (RUN-170) once the Confirm calls queued so far have been answered, and so does
// the first accepted renewal of a new incarnation. The answer to a new incarnation's first
// KeepAlive opens its machine (open). A result of an earlier generation is not stepped, and
// neither is one that reaches a lost machine: lose has discarded its machine's results in the
// executor, but those taken in the batch the loss came in are still here, and the machine has
// had its answers from lose (MCH O3). A slot result that is stepped then raises the slot data
// floor and asks for a slot read (slotResult); one that is not stepped does neither, since no
// machine has seen it, so it holds back no view of the next incarnation.
func (l *loop) deliver(r callResult) {
	if r.gen != l.gen || l.status.Session == core.SessionStateLost {
		return
	}
	if l.opening {
		l.open(r)
		l.resyncIfDue()
		return
	}
	f := l.n.fresh
	if a, ok := r.act.(core.Confirm); ok {
		f.answered++
		if r.err == nil {
			h := r.holding
			h.Space, h.Shard = a.Space, a.Shard
			l.observe(reading{h: h, confirm: true, start: r.start, end: r.end})
			l.resyncIfDue()
			return
		}
	}
	for _, in := range r.inputs {
		if a, ok := in.(core.Acquired); ok && a.Term > 0 {
			f.granted(a.Space, a.Shard, a.Term, r.end)
		}
		failing := l.status.LastFailure != core.CauseNone
		l.step(loopInput{in: in, sentWall: r.sentWall, err: r.err})
		if _, ok := in.(core.SessionRenewed); ok {
			if failing && l.status.LastFailure == core.CauseNone {
				f.wait, f.resyncAt = true, f.issued
			}
			l.renewed()
		}
	}
	l.resyncIfDue()
	l.slotResult(r)
}

// observe steps rd as HolderChanged if the freshness filter admits it. The filter sees the
// Status after every wake due by now, and the HolderChanged is stepped at that same reading, so
// no Tick, and no Confirm action, comes between the filter's decision and the step.
func (l *loop) observe(rd reading) {
	now := time.Since(l.n.epoch)
	l.tickUntil(now)
	if hc, ok := l.n.fresh.admit(rd, l.status); ok {
		l.apply(loopInput{in: l.at(hc, now), read: rd.start}, now)
	}
}

// confirm carries out a Confirm (RUN-130): the executor runs it, never dropping it, and the
// freshness filter notes its tag.
func (l *loop) confirm(a core.Confirm) {
	tag, queued := l.n.exec.doConfirm(l.gen, l.n.holder, a)
	l.n.fresh.confirmSent(a, tag, queued)
}

// resyncIfDue asks for a resync once the one a renewal made due may go.
func (l *loop) resyncIfDue() {
	f := l.n.fresh
	if f.wait && f.answered >= f.resyncAt {
		f.wait = false
		l.n.requestResync()
	}
}

// requestResync asks the resync goroutine for a resync of every space. A request made while one
// runs gives one more after it.
func (n *Node) requestResync() {
	select {
	case n.resync <- struct{}{}:
	default:
	}
}

// watchHolders is the store-watch goroutine (RUN-170). It calls WatchHolders once the session is
// open and hands every value of the channel to the loop as a reading, stamped when it arrives.
// After a failed call or a closed channel it reports the failure, waits RetryPeriod, and calls
// WatchHolders again; once a later call succeeds it asks for a resync of every space, which reads
// what the watch missed. It ends when the internal context is done.
func (n *Node) watchHolders(w Watcher) {
	restarted := false
	for {
		ch, err := w.WatchHolders(n.ctx)
		if n.ctx.Err() != nil {
			return
		}
		if err == nil && ch == nil {
			err = errors.New("tenure: WatchHolders returned a nil channel")
		}
		if err == nil {
			if restarted {
				n.requestResync()
			}
			if !n.followHolders(ch) {
				return
			}
			err = errStoreWatchEnded
		}
		n.obs.callFailed(callResult{op: "WatchHolders", err: err})
		restarted = true
		if !n.pause() {
			return
		}
	}
}

// followHolders hands each value of ch to the loop until ch is closed, and reports whether the
// watch should start again: false when the internal context is done.
func (n *Node) followHolders(ch <-chan Holding) bool {
	for {
		select {
		case h, ok := <-ch:
			if !ok {
				return n.ctx.Err() == nil
			}
			s := n.exec.stamp()
			n.readings.post(reading{h: h, start: s, end: s})
		case <-n.ctx.Done():
			return false
		}
	}
}

// resyncHolders is the resync goroutine (RUN-170). For each request it reads the holdings of
// every space, one space at a time in name order, and hands them to the loop. It ends when the
// internal context is done.
func (n *Node) resyncHolders() {
	for {
		select {
		case <-n.resync:
		case <-n.ctx.Done():
			return
		}
		for _, name := range n.fresh.names {
			if !n.readHoldings(name) {
				return
			}
		}
	}
}

// readHoldings calls Holdings for space under one of the executor's permits, with a context
// bounded by RetryPeriod, and again RetryPeriod after each failed attempt started, until one
// succeeds; each failure is reported. It hands the loop one reading per shard of the space: the
// shard's record, or a reading of no record for a shard the answer does not list. It reports
// whether a call succeeded before the internal context was done.
func (n *Node) readHoldings(space string) bool {
	period := n.cfg.Timing.RetryPeriod
	shards := len(n.shards[space].local)
	for {
		var (
			at         time.Time
			hs         []Holding
			err        error
			start, end uint64
		)
		if !n.exec.withPermit(func() {
			ctx, cancel := context.WithTimeout(n.ctx, period)
			defer cancel()
			at, start = time.Now(), n.exec.stamp()
			hs, err = n.cfg.Store.Holdings(withCallStamp(ctx, start), space)
			end = n.exec.stamp()
		}) {
			return false
		}
		if err == nil {
			n.readings.post(holdingReadings(space, shards, hs, start, end)...)
			return true
		}
		if n.ctx.Err() != nil {
			return false
		}
		n.obs.callFailed(callResult{op: "Holdings", space: space, err: err, start: start, end: end})
		t := time.NewTimer(time.Until(at.Add(period)))
		select {
		case <-t.C:
		case <-n.ctx.Done():
			t.Stop()
			return false
		}
	}
}

// holdingReadings returns one reading per shard of space, which has shards shards, from hs, a
// Holdings answer read between the stamps start and end: the record of the shard with the
// highest Version, or a reading of no record when hs lists none. Records of other spaces and of
// shards out of range are left out.
func holdingReadings(space string, shards int, hs []Holding, start, end uint64) []reading {
	rds := make([]reading, shards)
	for i := range rds {
		rds[i] = reading{h: Holding{Space: space, Shard: i}, start: start, end: end}
	}
	for _, h := range hs {
		if h.Space != space || h.Shard < 0 || h.Shard >= shards {
			continue
		}
		if r := &rds[h.Shard]; r.h.Version == 0 || h.Version > r.h.Version {
			r.h = h
		}
	}
	return rds
}
