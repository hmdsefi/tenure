// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// errWatchClosed is the error the watcher reports when a Watch channel closes before Leave.
var errWatchClosed = errors.New("tenure: watch channel closed")

// mergeView returns v with the slot data merged in (RUN-220): Slots is count, SlotHolders is
// holders cut or padded with "" to count entries, and each member's Slot is the lowest slot whose
// holder's member ID equals the member's ID, else -1. Version is v's. The members slice is a copy,
// never shared with v; the members' Meta maps are shared.
func mergeView(v core.View, count int, holders []string) core.View {
	out := core.View{Version: v.Version, Slots: count}
	slotOf := make(map[string]int)
	if count > 0 {
		out.SlotHolders = make([]string, count)
		copy(out.SlotHolders, holders)
		for s, h := range out.SlotHolders {
			if h == "" {
				continue
			}
			if id := holderID(h); slotOf[id] == 0 {
				slotOf[id] = s + 1 // 1 + the lowest slot, so that 0 means none
			}
		}
	}
	if v.Members != nil {
		out.Members = make([]core.Member, len(v.Members))
		for i, m := range v.Members {
			m.Slot = slotOf[m.ID] - 1
			out.Members[i] = m
		}
	}
	return out
}

// holderID returns the member ID part of a holder string: the text before the first "/", or the
// whole string when it has none.
func holderID(holder string) string {
	id, _, _ := strings.Cut(holder, "/")
	return id
}

// watchViews is the watcher goroutine (RUN-200). It follows ch, the channel of Start's Watch, and
// after a closed channel or a failed Watch calls Watch again RetryPeriod later. It ends when Watch
// returns ErrClosed, when the internal context is done, and once leave has been called: the
// channel Leave closes is not reported and no Watch follows it. Each view it reads is merged and
// offered; ended is closed when it returns.
func (n *Node) watchViews(ch <-chan core.View) {
	defer close(n.ended)
	for {
		if ch != nil && !n.follow(ch) {
			return
		}
		if !n.pause() {
			return
		}
		var err error
		ch, err = n.rewatch()
		switch {
		case ch != nil:
		case err == nil, errors.Is(err, ErrClosed), n.ctx.Err() != nil:
			return // leave was called, or the membership or the node is closed
		default:
			n.obs.membershipFailed("Watch", err)
		}
	}
}

// follow offers each view of ch until ch is closed, and reports whether the watcher should call
// Watch again: false when the internal context is done or leave has been called.
func (n *Node) follow(ch <-chan core.View) bool {
	for {
		select {
		case v, ok := <-ch:
			if ok {
				n.offer(v)
				continue
			}
			if n.leaving() || n.ctx.Err() != nil {
				return false
			}
			n.obs.membershipFailed("Watch", errWatchClosed)
			return true
		case <-n.ctx.Done():
			return false
		}
	}
}

// pause waits RetryPeriod and reports whether the internal context is still not done.
func (n *Node) pause() bool {
	t := time.NewTimer(n.cfg.Timing.RetryPeriod)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-n.ctx.Done():
		return false
	}
}

// rewatch calls Watch on the internal context, unless leave has been called: then it returns
// nil, nil. While the call is in flight, n.watchCall is open, so that leave can wait for it and no
// Watch starts once Leave has been called.
func (n *Node) rewatch() (<-chan core.View, error) {
	n.mu.Lock()
	if n.left {
		n.mu.Unlock()
		return nil, nil
	}
	call := make(chan struct{})
	n.watchCall = call
	n.mu.Unlock()

	ch, err := n.cfg.Membership.Watch(n.ctx)

	n.mu.Lock()
	n.watchCall = nil
	n.mu.Unlock()
	close(call)
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, errors.New("tenure: Watch returned a nil channel")
	}
	return ch, nil
}

// offer takes one view from the membership. It drops the view when it is not valid or its
// Version is not above that of the last view offer took; otherwise the merger merges the node's
// slot data into it and posts the merged view. The first view offer takes lets Start go on.
func (n *Node) offer(v core.View) {
	if mergeView(v, 0, nil).Validate() != nil {
		return
	}
	if taken, first := n.merge.membership(v); taken && first {
		close(n.first)
	}
}

// postView is the merger's post function for the node (RUN-200): without a store it publishes
// v and stores the routing snapshot of it; with a store it hands v to the loop, which steps it
// and then publishes it.
func (n *Node) postView(v core.View) {
	if n.cfg.Store == nil {
		n.publish(v)
		n.snap.Store(newRoutes(n.view.Load(), nil, nil)) // no spaces: nothing to assign
		return
	}
	n.inbox.postView(v)
}

// publish makes v the node's published view, feeds it to every node resolver, and reports it.
func (n *Node) publish(v core.View) {
	n.view.Store(&v)
	n.feedResolvers(v)
	n.obs.view(v)
}

// leaving reports whether leave has been called.
func (n *Node) leaving() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.left
}

// leave calls Leave with ctx bounded by RetryPeriod (RUN-070). It first marks the node as left and
// waits, at most RetryPeriod or until ctx is done, for a Watch call in flight, so that no Watch
// starts after Leave; Leave's own bound starts after that wait. A failed Leave is reported and
// otherwise ignored.
func (n *Node) leave(ctx context.Context) {
	period := n.cfg.Timing.RetryPeriod
	n.mu.Lock()
	n.left = true
	call := n.watchCall
	n.mu.Unlock()
	if call != nil {
		t := time.NewTimer(period)
		select {
		case <-call:
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
	ctx, cancel := context.WithTimeout(ctx, period)
	defer cancel()
	if err := n.cfg.Membership.Leave(ctx); err != nil {
		n.obs.membershipFailed("Leave", err)
	}
}

// merger keeps the latest membership view and the latest slot data of the node, and posts their
// merge (RUN-220) whenever either changes. The merged view's Version is the membership view's
// plus the number of slot data changes posted so far, so it grows with every post: a membership
// view raises the first term, a slot change the second. Without slot data it is the membership
// view's own Version.
//
// O8 (RUN-210): the loop raises the floor to the end stamp of each SlotClaimed or SlotReleased
// result it steps. Slot data read by a call that started before the floor is older than that
// result: the merger drops such a read, and while its own data is that old it posts nothing, so
// that no view stepped after a slot result comes from an older read. A view posted before the
// floor rose may still wait for the loop: when it came from an older read, the loop drops it
// (stale), and the next read that is not older posts again.
//
// Its methods are safe for concurrent use. The post function is called with mu held, so posts
// reach it in Version order.
type merger struct {
	post func(v core.View)

	mu      sync.Mutex
	hasMem  bool
	mem     core.View // the latest membership view taken
	count   int
	holders []string // count entries
	read    uint64   // the start stamp of the read the slot data comes from
	floor   uint64   // the end stamp of the latest SlotClaimed or SlotReleased result stepped
	bumps   uint64   // slot data changes and repeats posted so far
	posted  bool     // a merged view has been posted
	postRd  uint64   // the read of the latest posted view
	postVer uint64   // the Version of the latest posted view
	staleTo uint64   // posted views up to this Version came from reads below the floor
}

// membership takes v, a valid membership view, unless its Version is not above that of the last
// view taken, and posts the merged view unless the slot data is older than the floor. It reports
// whether it took v and whether v was the first view taken.
func (m *merger) membership(v core.View) (taken, first bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hasMem && v.Version <= m.mem.Version {
		return false, false
	}
	first = !m.hasMem
	m.mem, m.hasMem = v, true
	if m.read >= m.floor {
		m.postLocked()
	}
	return true, first
}

// slots takes slot data that a Slots call starting at stamp read returned: count and holders,
// cut or padded with "" to count. It drops the data, and reports stale, when read is below the
// floor. Otherwise it keeps the data and posts the merged view when the count or a holder changed,
// or when the latest posted view came from a read below the floor. version is the Version of the
// merged view that holds the data, posted now or before.
func (m *merger) slots(read uint64, count int, holders []string) (version uint64, stale bool) {
	norm := make([]string, count)
	copy(norm, holders)
	m.mu.Lock()
	defer m.mu.Unlock()
	if read < m.floor {
		return 0, true
	}
	m.read = read
	if count != m.count || !slices.Equal(norm, m.holders) || (m.posted && m.postRd < m.floor) {
		m.count, m.holders = count, norm
		m.bumps++
		if m.hasMem {
			m.postLocked()
		}
	}
	return m.mem.Version + m.bumps, false
}

// postLocked posts the merge of the latest membership view and slot data. The caller holds mu.
func (m *merger) postLocked() {
	v := mergeView(m.mem, m.count, m.holders)
	v.Version = m.mem.Version + m.bumps
	m.posted, m.postRd, m.postVer = true, m.read, v.Version
	m.post(v)
}

// raise raises the floor to end, the end stamp of a SlotClaimed or SlotReleased result the loop
// has stepped. When the latest posted view came from a read below the new floor, every view
// posted so far is stale.
func (m *merger) raise(end uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.floor = max(m.floor, end)
	if m.posted && m.postRd < m.floor {
		m.staleTo = m.postVer
	}
}

// stale reports whether the posted view of Version version came from a read older than the
// latest slot result the loop stepped. Only the latest posted view can wait for the loop, and it
// is stale exactly when its read is below the floor.
func (m *merger) stale(version uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return version <= m.staleTo
}

// slotsRead is the outcome of one refresh: the Version of the merged view that holds its data,
// or the error of the Slots call.
type slotsRead struct {
	version uint64
	err     error
}

// slotsWaiter is a refreshWait call waiting for the first read that starts after its request.
type slotsWaiter struct {
	seq uint64
	ch  chan slotsRead // buffer 1
}

// refresher reads the slot data of a SlotStore and hands it to the merger (RUN-210). It reads
// every RetryPeriod after the start of the previous read, at once for each request, and again
// at once when a read started before the merger's floor. One read is in flight at a time, under
// one of the executor's permits, with a context bounded by RetryPeriod. A failed read is
// reported and changes nothing: the merger keeps the data it has.
type refresher struct {
	n     *Node
	store SlotStore
	kick  chan struct{} // buffer 1: a request may be waiting

	mu      sync.Mutex
	asked   uint64 // requests so far
	served  uint64 // requests served by a read that finished
	waiters []slotsWaiter
}

// newRefresher returns the refresher of n over store.
func newRefresher(n *Node, store SlotStore) *refresher {
	return &refresher{n: n, store: store, kick: make(chan struct{}, 1)}
}

// refresh requests a read that starts after the call, without waiting for it. The loop calls it
// after each ClaimSlot and ReleaseSlot result.
func (r *refresher) refresh() {
	r.ask(nil)
}

// refreshWait requests a read that starts after the call and waits for it. It returns the
// Version of the merged view that holds the data read, which the merger has posted by then, or
// the read's error; ctx's error when ctx ends first, and ErrClosed when the node stops first.
func (r *refresher) refreshWait(ctx context.Context) (uint64, error) {
	ch := make(chan slotsRead, 1)
	r.ask(ch)
	select {
	case res := <-ch:
		return res.version, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.n.ctx.Done():
		return 0, ErrClosed
	}
}

// ask records a request, with its waiter's channel when ch is not nil, and wakes the refresher.
func (r *refresher) ask(ch chan slotsRead) {
	r.mu.Lock()
	r.asked++
	if ch != nil {
		r.waiters = append(r.waiters, slotsWaiter{seq: r.asked, ch: ch})
	}
	r.mu.Unlock()
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// run is the refresher's goroutine. last is the start of the latest read, Start's own. It ends
// when the internal context is done.
func (r *refresher) run(last time.Time) {
	n := r.n
	t := time.NewTimer(time.Until(last.Add(n.cfg.Timing.RetryPeriod)))
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		case <-r.kick:
			r.mu.Lock()
			pending := r.asked > r.served
			r.mu.Unlock()
			if !pending {
				continue
			}
		}
		for {
			r.mu.Lock()
			upTo := r.asked
			r.mu.Unlock()
			start := time.Now()
			res, again, ok := r.read()
			if !ok {
				return
			}
			if again {
				continue // started before a slot result: read again at once
			}
			last = start
			if !r.serve(upTo, res) {
				break
			}
		}
		t.Reset(time.Until(last.Add(n.cfg.Timing.RetryPeriod)))
	}
}

// read makes one Slots call under a permit and hands its data to the merger. It reports again
// when the merger dropped the data as stale, and ok false when the internal context is done.
func (r *refresher) read() (res slotsRead, again, ok bool) {
	n := r.n
	var (
		stamp   uint64
		count   int
		holders []string
		err     error
	)
	if !n.exec.withPermit(func() {
		ctx, cancel := context.WithTimeout(n.ctx, n.cfg.Timing.RetryPeriod)
		defer cancel()
		stamp = n.exec.stamp()
		count, holders, err = readSlotData(ctx, r.store)
	}) || n.ctx.Err() != nil {
		return slotsRead{}, false, false
	}
	if err != nil {
		n.obs.slotsFailed(err)
		return slotsRead{err: err}, false, true
	}
	version, stale := n.merge.slots(stamp, count, holders)
	return slotsRead{version: version}, stale, true
}

// readSlotData calls ss.Slots and fails for a negative count, which breaks the SlotStore
// contract and which the merger cannot hold.
func readSlotData(ctx context.Context, ss SlotStore) (int, []string, error) {
	count, holders, err := ss.Slots(ctx)
	if err == nil && count < 0 {
		return 0, nil, fmt.Errorf("negative slot count %d", count)
	}
	return count, holders, err
}

// serve hands res to every waiter whose request a read starting with upTo requests made serves,
// and reports whether requests made since then are still waiting for a read.
func (r *refresher) serve(upTo uint64, res slotsRead) (more bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.served = max(r.served, upTo)
	kept := r.waiters[:0]
	for _, w := range r.waiters {
		if w.seq <= upTo {
			w.ch <- res
			continue
		}
		kept = append(kept, w)
	}
	clear(r.waiters[len(kept):])
	r.waiters = kept
	return r.asked > r.served
}
