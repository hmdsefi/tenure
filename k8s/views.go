// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Views (K8S-610 to K8S-630, K8S-650).
//
// A view lists the live members of the group as this Adapter observes them (K8S-610): every
// member Lease of the group that carries a valid member record and whose resourceVersion this
// Adapter first saw less than the Lease's wait ago (now − rvSeenAt < leaseDurationSeconds), or
// every such Lease while the cache is not fresh (K8S-150): the views are frozen then, and only
// observed changes, a record written or removed or a Lease deleted, change them. A member's
// fields come from its record; its Slot is the lowest slot whose live holder is one of its
// incarnations, and the view's Slots and SlotHolders are what Slots would return from the same
// observations. An invalid member record leaves its Lease out of the views and is logged at Warn
// once per resourceVersion (K8S-060).
//
// One goroutine, the builder, runs while a Watch channel is open. It builds the view again
// (K8S-620) on every informer event, at the earliest instant at which time alone changes the
// view (a member's rvSeenAt + wait, a slot holder's tokenSeenAt + wait or a tombstone's end,
// and the instant freshness turns by itself) with one timer, and when freshness changes. A view
// that differs from the latest one in anything but Version takes the next Version of the
// Adapter's one sequence and replaces, in every open channel, a view not yet received
// (K8S-630). Watch builds the current view before it returns.
//
// Leave (K8S-650) delivers a last view without the member that left and closes every channel;
// Watch returns tenure.ErrClosed from then on. Lock order: views.mu, then the Adapter's mu, then
// the cache's mu.

var _ tenure.Membership = (*Adapter)(nil)

// views is the state of an Adapter's views.
type views struct {
	mu sync.Mutex
	// closed is set by Leave, for the Adapter's life.
	closed bool
	// version is the Version of view, 0 before the first view.
	version uint64
	// view is the latest view delivered.
	view core.View
	// watches are the open Watch channels.
	watches map[*viewWatch]struct{}
	// kick wakes the builder after a change of the observations; made on first use, it buffers
	// one wake-up.
	kick chan struct{}
	// stop ends the running builder; nil while none runs.
	stop chan struct{}
	// warned holds, by member Lease name, the resourceVersion whose invalid member record was
	// logged.
	warned map[string]string
}

// viewWatch is one channel Watch returned.
type viewWatch struct {
	ch chan core.View
	// cache is the watch's informer hold.
	cache *leaseCache
	// unhook removes the watch's hook on its context.
	unhook func() bool
}

// Watch returns a channel that holds the current view of the group, as this Adapter observes
// it, and receives each later one. A view lists the members whose member Lease carries a member
// record and changed within the Lease's duration, as this Adapter observes it, sorted by ID; a
// member's Slot is the lowest slot one of its sessions holds, and Slots and SlotHolders are what
// Slots reports. While the Adapter has joined or holds a session but has not seen its own member
// Lease writes come back from its watch within RenewDeadline, the passing of time alone removes
// no member. A new view, with the next Version, comes when anything but Version changes. The
// channel holds one view; a newer view replaces one not yet received. Watch may be called before
// Join, also on an Adapter that never joins, and any number of times. The channel is closed when
// ctx is done and when Leave ends the membership; Watch returns tenure.ErrClosed after Leave.
// While a channel is open, the Adapter watches the group's Leases.
func (a *Adapter) Watch(ctx context.Context) (<-chan core.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.err != nil {
		return nil, a.err
	}
	v := &a.views
	v.mu.Lock()
	closed := v.closed
	v.mu.Unlock()
	if closed {
		return nil, tenure.ErrClosed
	}
	c, err := a.holdCache(ctx)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		a.releaseCache(c)
		return nil, tenure.ErrClosed
	}
	view, _ := a.buildViewLocked("")
	w := &viewWatch{ch: make(chan core.View, 1), cache: c}
	w.ch <- a.publishLocked(view)
	if v.watches == nil {
		v.watches = map[*viewWatch]struct{}{}
	}
	v.watches[w] = struct{}{}
	if v.stop == nil {
		if v.kick == nil {
			v.kick = make(chan struct{}, 1)
		}
		stop := make(chan struct{})
		v.stop = stop
		a.runs.Go(func() { a.buildViews(stop, v.kick) })
	}
	w.unhook = context.AfterFunc(ctx, func() { a.unwatch(w) })
	return w.ch, nil
}

// viewsChanged wakes the builder after a change of the observations, without waiting.
func (a *Adapter) viewsChanged() {
	v := &a.views
	v.mu.Lock()
	kick := v.kick
	v.mu.Unlock()
	if kick != nil {
		select {
		case kick <- struct{}{}:
		default:
		}
	}
}

// buildViews is the builder (K8S-620): it builds the view, delivers it when it changed, and
// waits for the next informer event, the next instant at which time alone changes the view, or
// a change of freshness, until stop is closed.
func (a *Adapter) buildViews(stop <-chan struct{}, kick <-chan struct{}) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		// Taken before the build reads freshness, so no change falls between.
		freshCh := a.freshChanged()
		if next := a.rebuild(); !next.IsZero() {
			timer.Reset(max(next.Sub(a.now()), time.Millisecond))
		} else {
			timer.Stop()
		}
		select {
		case <-stop:
			return
		case <-kick:
		case <-freshCh:
		case <-timer.C:
		}
	}
}

// rebuild builds the view and delivers it when it changed. It returns the next instant at which
// time alone may change the view, zero for none.
func (a *Adapter) rebuild() time.Time {
	v := &a.views
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.watches) == 0 {
		return time.Time{}
	}
	view, next := a.buildViewLocked("")
	a.publishLocked(view)
	return next
}

// publishLocked makes view the latest view and delivers it to every open channel when it
// differs from the latest one in anything but Version, with the next Version. It returns the
// latest view. The caller holds views.mu.
func (a *Adapter) publishLocked(view core.View) core.View {
	v := &a.views
	if v.version > 0 && sameView(v.view, view) {
		return v.view
	}
	v.version++
	view.Version = v.version
	v.view = view
	for w := range v.watches {
		// The builder is the only sender, under views.mu: once a view not yet received is
		// taken out, the send finds room.
		select {
		case <-w.ch:
		default:
		}
		w.ch <- view
	}
	return view
}

// unwatch closes the channel of w, when it is still open, and gives back its informer hold.
func (a *Adapter) unwatch(w *viewWatch) {
	v := &a.views
	v.mu.Lock()
	defer v.mu.Unlock()
	a.unwatchLocked(w)
}

// unwatchLocked is unwatch for a caller that holds views.mu. The last channel closed stops the
// builder.
func (a *Adapter) unwatchLocked(w *viewWatch) {
	v := &a.views
	if _, ok := v.watches[w]; !ok {
		return
	}
	delete(v.watches, w)
	w.unhook()
	close(w.ch)
	a.releaseCache(w.cache)
	if len(v.watches) == 0 && v.stop != nil {
		close(v.stop)
		v.stop = nil
	}
}

// closeViews ends the views for Leave (K8S-650): it delivers a view without the member left,
// the ID of the member that left ("" for none), and closes every channel; Watch returns
// tenure.ErrClosed from then on. The caller does not hold the Adapter's mu.
func (a *Adapter) closeViews(left string) {
	v := &a.views
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closed = true
	if len(v.watches) == 0 {
		return
	}
	view, _ := a.buildViewLocked(left)
	a.publishLocked(view)
	for w := range v.watches {
		a.unwatchLocked(w)
	}
}

// buildViewLocked builds the view of the group from the cache's observations at this moment
// (K8S-610), leaving out the member without, and returns it, with no Version, and the next
// instant at which time alone may change it, zero for none: the earliest of the instants at
// which a listed member's Lease, or a slot holder's session, turns gone by its wait, and at
// which freshness may change. The caller holds views.mu.
func (a *Adapter) buildViewLocked(without string) (core.View, time.Time) {
	fresh, next := a.freshness()
	now := a.now()
	obs, tombs, ok := a.snapshot()
	if !ok {
		return core.View{}, time.Time{}
	}

	count, holders, err := a.slotsOf(obs, tombs, now, fresh)
	if err != nil {
		// A count that is not a number in [0, 10000]: the view has no slots until it is.
		a.warnOnceLocked(slotsName(a.group), obs[slotsName(a.group)].lease.ResourceVersion, "k8s: invalid slot count", err)
		count, holders = 0, nil
	}
	for n := range count {
		o, ok := obs[slotName(a.group, n)]
		if !ok || holders[n] == "" {
			continue
		}
		// A live holder that time alone turns gone: its verdict's end.
		next = earliest(next, a.slotVerdict(o, obs, tombs, now, fresh).until)
	}
	slotOf := map[string]int{}
	for n := count - 1; n >= 0; n-- {
		if h := holders[n]; h != "" {
			slotOf[memberOf(h)] = n
		}
	}

	var members []core.Member
	seen := map[string]bool{}
	for name, o := range obs {
		m, ok := a.viewMemberLocked(name, o)
		if !ok || m.ID == without {
			continue
		}
		seen[name] = true
		if fresh {
			until := o.rvSeenAt.Add(leaseWait(o.lease, a.timing.LeaseDuration))
			if !now.Before(until) {
				continue
			}
			next = earliest(next, until)
		}
		m.Slot = -1
		if n, ok := slotOf[m.ID]; ok {
			m.Slot = n
		}
		members = append(members, m)
	}
	slices.SortFunc(members, func(x, y core.Member) int { return strings.Compare(x.ID, y.ID) })
	for name := range a.views.warned {
		if _, ok := obs[name]; !ok {
			delete(a.views.warned, name)
		}
	}
	return core.View{Members: members, Slots: count, SlotHolders: holders}, next
}

// viewMemberLocked returns the member that o, the observation of Lease name, holds: ok when the
// Lease is a member Lease of the group with a member record. An invalid record is logged at
// Warn once per resourceVersion (K8S-060). The caller holds views.mu.
func (a *Adapter) viewMemberLocked(name string, o observation) (core.Member, bool) {
	l := o.lease
	if !isMemberLease(l, a.group) {
		return core.Member{}, false
	}
	kind, _, id, _, ok := parseName(a.group, name)
	if !ok || kind != kindMember {
		return core.Member{}, false
	}
	record, ok := l.Annotations[annotationMember]
	if !ok {
		// A session's Lease of a member that has not joined.
		return core.Member{}, false
	}
	m, err := decodeMember(id, record)
	if err == nil {
		if verr := m.Validate(); verr != nil {
			err = fmt.Errorf("k8s: member record: %w", verr)
		}
	}
	if err != nil {
		a.warnOnceLocked(name, l.ResourceVersion, "k8s: invalid member record", err)
		return core.Member{}, false
	}
	return m, true
}

// warnOnceLocked logs msg at Warn for Lease name at resourceVersion rv, unless it was logged for
// that resourceVersion already. The caller holds views.mu.
func (a *Adapter) warnOnceLocked(name, rv, msg string, err error) {
	v := &a.views
	if w, ok := v.warned[name]; ok && w == rv {
		return
	}
	if v.warned == nil {
		v.warned = map[string]string{}
	}
	v.warned[name] = rv
	a.logger.Warn(msg, "lease", name, "resourceVersion", rv, "err", err)
}

// sameView reports whether x and y are the same view but for Version: the same members with the
// same data, Slot included and Meta compared by content, and the same slot data.
func sameView(x, y core.View) bool {
	return x.Slots == y.Slots && slices.Equal(x.SlotHolders, y.SlotHolders) &&
		slices.EqualFunc(x.Members, y.Members, func(m, n core.Member) bool {
			return m.ID == n.ID && m.Addr == n.Addr && m.Draining == n.Draining && m.Slot == n.Slot &&
				maps.Equal(m.Meta, n.Meta) && m.JoinAt.Equal(n.JoinAt) && m.LeaveAt.Equal(n.LeaveAt)
		})
}
