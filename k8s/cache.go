// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordinationinformers "k8s.io/client-go/informers/coordination/v1"
	"k8s.io/client-go/tools/cache"
)

// The Adapter's cache (K8S-100, K8S-110).
//
// One informer watches the Leases of the group (label tenure.group=<group>) in the namespace,
// with no resync. It runs while something holds it: an active session holds it from its opening
// to its end (session.go); Join, Watch and WatchHolders hold it the same way. holdCache starts
// it, or joins the run that is already there, and returns once its initial list has been
// observed, or with ctx.Err(); releaseCache gives the hold back, and the last release stops the
// informer at once. A stopped run's observations are discarded: the next run
// observes every Lease anew. What must outlive a run, such as the terms of deleted shard Leases,
// belongs on the Adapter instead.
//
// While a run is up, every Lease event is observed on the Adapter's clock (observation).
// Deleting a member Lease leaves a tombstone keyed by its UID until the session it held has
// been gone for that Lease's duration.
//
// Freshness (K8S-150). The cache is fresh while the Adapter has neither joined nor an active
// session, or when the informer delivered one of the Adapter's own member Lease writes that
// changed its resourceVersion and was sent within the last RenewDeadline (of the active
// sessions' Timing, the largest when there are several, else of the Adapter's). The age counts
// from the write's sending, not from its delivery: the watch delivers in resourceVersion order,
// so the delivery shows the cache holds every change committed before the write, and no later
// one. A watch that lags behind by more than RenewDeadline thus leaves the cache stale, even
// while it still delivers. Own writes come every RetryPeriod (a renewal or a heartbeat), so a
// cache whose watch has stopped delivering turns stale too. Only the informer's event handlers
// count deliveries: reads and own writes are also fed through observe (noteRead), and must not
// make a stopped watch look alive. While the cache is not
// fresh, a holder that only the passing of time would turn gone stays live (frozen), for Slots,
// ClaimSlot and the views. freshness reports the rule's answer and the next instant at which it
// may change by itself; freshChanged signals every event that may change it otherwise.
//
// Every event also wakes the builder of the views (views.go), and each run collects member Leases
// dead for long (gc.go).

// resyncPeriod is the informer's resync period: none, as observations rest on changes only.
const resyncPeriod = 0

// maxPending is how many own writes of one member Lease the cache waits for at most: older ones
// are dropped, as a delivery of a later one covers them.
const maxPending = 8

// pendingWrite is an own write of a member Lease that the informer has not delivered yet: its
// resourceVersion, and when it was sent, on the Adapter's clock.
type pendingWrite struct {
	rv   uint64
	sent time.Time
}

// observation is what this Adapter has observed of one Lease (K8S-110), on its own clock.
type observation struct {
	// lease is the latest state seen. It is shared: never change it.
	lease *coordinationv1.Lease
	// rvSeenAt is when this Adapter first saw the Lease's current resourceVersion.
	rvSeenAt time.Time
	// tokenSeenAt is when this Adapter first saw the Lease's current session token: its UID,
	// holderIdentity and renewTime.
	tokenSeenAt time.Time
	// named is the last holder this Adapter saw the Lease name under its current UID: it stays
	// when the holder is cleared, so a Lease without a holder tells whose session it saw end.
	named string
}

// tombstone is what remains of a deleted member Lease (K8S-110): the session it held stays
// live, for an observer, until tokenSeenAt + wait.
type tombstone struct {
	tokenSeenAt time.Time
	// wait is the deleted Lease's leaseDurationSeconds, or the Adapter's LeaseDuration when it
	// had none.
	wait time.Duration
}

// expired reports whether the tombstone's session is gone at now.
func (t tombstone) expired(now time.Time) bool { return !now.Before(t.tokenSeenAt.Add(t.wait)) }

// leaseCache is one run of the informer and its observations.
type leaseCache struct {
	// refs counts the holds on the run; the Adapter's mu guards it.
	refs   int
	cancel context.CancelFunc
	// synced is closed once the initial list has been observed.
	synced <-chan struct{}

	now         func() time.Time
	defaultWait time.Duration
	// terms is the Adapter's, which outlives the run: the terms of deleted shard Leases.
	terms *shardTerms

	mu    sync.Mutex
	obs   map[string]*observation // by Lease name
	tombs map[types.UID]tombstone // of member Leases, by UID

	// Freshness (K8S-150), from the informer's deliveries only.
	// pending holds, by member Lease name, the own writes the informer has not delivered yet.
	pending map[string][]pendingWrite
	// delivered is the resourceVersion of the latest delivery of each member Lease, by name.
	delivered map[string]uint64
	// ownSentAt is when the latest own member Lease write that the informer delivered was sent;
	// zero before the first.
	ownSentAt time.Time
	// gone holds, by Lease name, the UID of the last Lease of that name seen deleted.
	gone map[string]types.UID
}

// holdCache holds the informer running, starting it unless it runs already, and waits for its
// initial list to be observed. It returns ctx.Err() when ctx ends first, and holds nothing then.
// The caller gives the hold back with releaseCache(c), once; the last release stops the
// informer.
func (a *Adapter) holdCache(ctx context.Context) (c *leaseCache, err error) {
	a.mu.Lock()
	c = a.cache
	if c == nil {
		c = a.startCache()
		a.cache = c
	}
	c.refs++
	a.mu.Unlock()
	select {
	case <-c.synced:
		return c, nil
	case <-ctx.Done():
		a.releaseCache(c)
		return nil, ctx.Err()
	}
}

// releaseCache gives back one hold on run c, stopping it with the last.
func (a *Adapter) releaseCache(c *leaseCache) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releaseCacheLocked(c)
}

// releaseCacheLocked is releaseCache for a caller that holds a.mu.
func (a *Adapter) releaseCacheLocked(c *leaseCache) {
	c.refs--
	if c.refs > 0 {
		return
	}
	c.cancel()
	if a.cache == c {
		a.cache = nil
	}
}

// startCache starts a run of the informer. The caller holds a.mu.
func (a *Adapter) startCache() *leaseCache {
	c := &leaseCache{
		now:         a.now,
		defaultWait: a.timing.LeaseDuration,
		terms:       &a.terms,
		obs:         map[string]*observation{},
		tombs:       map[types.UID]tombstone{},
		pending:     map[string][]pendingWrite{},
		delivered:   map[string]uint64{},
		gone:        map[string]types.UID{},
	}
	selector := labelGroup + "=" + a.group
	informer := coordinationinformers.NewFilteredLeaseInformer(a.client, a.namespace, resyncPeriod, cache.Indexers{},
		func(o *metav1.ListOptions) { o.LabelSelector = selector })
	deliver := func(obj any) {
		l := asLease(obj)
		c.observe(l)
		a.delivered(c, l)
		a.viewsChanged()
	}
	reg, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    deliver,
		UpdateFunc: func(_, obj any) { deliver(obj) },
		DeleteFunc: func(obj any) {
			_, gap := obj.(cache.DeletedFinalStateUnknown)
			c.forget(asLease(obj), gap)
			a.viewsChanged()
		},
	})
	if err != nil {
		// AddEventHandler fails only on an informer that has stopped; this one has not started.
		panic("k8s: add event handler: " + err.Error())
	}
	c.synced = reg.HasSyncedChecker().Done()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	a.runs.Go(func() { informer.RunWithContext(ctx) })
	a.runs.Go(func() { a.collectMembers(ctx) })
	return c
}

// asLease returns the Lease of an informer event, also of a deletion whose final state is
// unknown, or nil.
func asLease(obj any) *coordinationv1.Lease {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	l, _ := obj.(*coordinationv1.Lease)
	return l
}

// sameToken reports whether a and b carry the same session token: UID, holderIdentity and
// renewTime.
func sameToken(a, b *coordinationv1.Lease) bool {
	if a.UID != b.UID || holderOf(a) != holderOf(b) {
		return false
	}
	ra, rb := a.Spec.RenewTime, b.Spec.RenewTime
	if ra == nil || rb == nil {
		return ra == rb
	}
	return ra.Equal(rb)
}

// holderOf returns the holderIdentity of l, "" when it has none.
func holderOf(l *coordinationv1.Lease) string {
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// isOlder reports whether resourceVersion rv is older than seen. Versions that do not parse
// are never older: a change is assumed.
func isOlder(rv, seen string) bool {
	v, ok1 := parseUint(rv)
	w, ok2 := parseUint(seen)
	return ok1 && ok2 && v < w
}

// observe records state l of a Lease (K8S-110). A state older than the one recorded is ignored,
// also when the recorded one is of a Lease created anew at the name.
func (c *leaseCache) observe(l *coordinationv1.Lease) {
	if l == nil {
		return
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)
	o, ok := c.obs[l.Name]
	switch {
	case ok && isOlder(l.ResourceVersion, o.lease.ResourceVersion):
		// Older than the state recorded, also of a Lease since deleted: a late delivery after a
		// read from the API server was noted (noteRead).
	case c.isGone(l.Name, l.UID):
		// A state of a Lease seen deleted, noted after the deletion (noteRead): that Lease is
		// gone for good.
	case !ok || o.lease.UID != l.UID:
		if ok {
			// The Lease was deleted and created again without a deletion event, as a relist
			// after a gap in the watch shows it: the deleted one leaves its tombstone all the
			// same, stamped now, as renewals during the gap may have been missed.
			c.bury(o.lease, now, now)
			c.noteGone(l.Name, o.lease.UID)
		}
		c.obs[l.Name] = &observation{lease: l, rvSeenAt: now, tokenSeenAt: now, named: holderOf(l)}
	case l.ResourceVersion == o.lease.ResourceVersion || isOlder(l.ResourceVersion, o.lease.ResourceVersion):
	default:
		next := &observation{lease: l, rvSeenAt: now, tokenSeenAt: o.tokenSeenAt, named: o.named}
		if h := holderOf(l); h != "" {
			next.named = h
		}
		if !sameToken(o.lease, l) {
			next.tokenSeenAt = now
		}
		c.obs[l.Name] = next
	}
}

// forget records the deletion of Lease l: its observation goes, its UID is noted as gone, and a
// member Lease leaves a tombstone. gap reports a deletion the informer found only by listing
// again after a gap in its watch (a final state unknown): renewals of the session may have been
// missed during the gap, so the tombstone counts the session as seen now, not when the cache last
// saw its token change.
func (c *leaseCache) forget(l *coordinationv1.Lease, gap bool) {
	if l == nil {
		return
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)
	tokenSeenAt := now
	if o, ok := c.obs[l.Name]; ok && o.lease.UID == l.UID {
		if !gap {
			tokenSeenAt = o.tokenSeenAt
		}
		delete(c.obs, l.Name)
	}
	c.noteGone(l.Name, l.UID)
	c.bury(l, tokenSeenAt, now)
}

// noteGone notes uid as the last Lease named name seen deleted. The caller holds c.mu.
func (c *leaseCache) noteGone(name string, uid types.UID) {
	if c.gone == nil {
		c.gone = map[string]types.UID{}
	}
	c.gone[name] = uid
}

// isGone reports whether uid is the last Lease named name seen deleted. The caller holds c.mu.
func (c *leaseCache) isGone(name string, uid types.UID) bool {
	u, ok := c.gone[name]
	return ok && u == uid
}

// bury leaves the tombstone of deleted Lease l, whose session token was first seen at
// tokenSeenAt, when l is a member Lease and the tombstone has not expired at now; a deleted
// shard Lease leaves its term with the Adapter. The caller holds c.mu.
func (c *leaseCache) bury(l *coordinationv1.Lease, tokenSeenAt, now time.Time) {
	if l.Labels[labelKind] == kindShard {
		if term, ok := decodeTerm(l.Annotations); ok && term > 0 {
			c.terms.note(l.Name, term)
		}
	}
	if l.Labels[labelKind] != kindMember {
		return
	}
	wait := c.defaultWait
	if d := l.Spec.LeaseDurationSeconds; d != nil {
		wait = time.Duration(*d) * time.Second
	}
	if t := (tombstone{tokenSeenAt: tokenSeenAt, wait: wait}); !t.expired(now) {
		c.tombs[l.UID] = t
	}
}

// prune drops the tombstones that have expired at now. The caller holds c.mu.
func (c *leaseCache) prune(now time.Time) {
	for uid, t := range c.tombs {
		if t.expired(now) {
			delete(c.tombs, uid)
		}
	}
}

// cached returns the observation of Lease name. ok is false when the informer is not running or
// has not seen the Lease.
func (a *Adapter) cached(name string) (o observation, ok bool) {
	c := a.runningCache()
	if c == nil {
		return observation{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.obs[name]
	if !ok {
		return observation{}, false
	}
	return *p, true
}

// deleted reports whether the running informer has seen the deletion of Lease name with uid,
// the last Lease of that name to go.
func (a *Adapter) deleted(name string, uid types.UID) bool {
	c := a.runningCache()
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isGone(name, uid)
}

// cachedLeases returns the observations of the Leases of kind, by name. It returns nil when the
// informer is not running.
func (a *Adapter) cachedLeases(kind string) []observation {
	c := a.runningCache()
	if c == nil {
		return nil
	}
	c.mu.Lock()
	var out []observation
	for _, o := range c.obs {
		if o.lease.Labels[labelKind] == kind {
			out = append(out, *o)
		}
	}
	c.mu.Unlock()
	slices.SortFunc(out, func(x, y observation) int { return strings.Compare(x.lease.Name, y.lease.Name) })
	return out
}

// tombstoneOf returns the tombstone of the member Lease with uid while it has not expired. ok is
// false when there is none or the informer is not running.
func (a *Adapter) tombstoneOf(uid types.UID) (t tombstone, ok bool) {
	c := a.runningCache()
	if c == nil {
		return tombstone{}, false
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)
	t, ok = c.tombs[uid]
	return t, ok
}

// snapshot returns the observations of the running cache, by Lease name, and its tombstones
// that have not expired, by UID, as of one moment. ok is false when the informer is not running.
func (a *Adapter) snapshot() (obs map[string]observation, tombs map[types.UID]tombstone, ok bool) {
	c := a.runningCache()
	if c == nil {
		return nil, nil, false
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)
	obs = make(map[string]observation, len(c.obs))
	for name, o := range c.obs {
		obs[name] = *o
	}
	return obs, maps.Clone(c.tombs), true
}

// noteOwnWrite notes written, the state of a Lease that this Adapter wrote on cur, nil for a
// creation, in a request sent at sent, for freshness (K8S-150): a member Lease write that changed
// the resourceVersion is waited for in the informer's deliveries.
func (a *Adapter) noteOwnWrite(cur, written *coordinationv1.Lease, sent time.Time) {
	if written == nil || !isMemberLease(written, a.group) || cur != nil && cur.ResourceVersion == written.ResourceVersion {
		return
	}
	if c := a.runningCache(); c != nil {
		if prev, ok := c.ownWrite(written, sent); ok {
			a.ownWriteSeen(c, prev)
		}
	}
}

// ownWrite waits for own write l of a member Lease, sent at sent, in the informer's deliveries.
// When a state at or past it was delivered already, as the watch can deliver a write before the
// write's response comes back, the write counts at once: ok reports that, with prev, the send
// time of the latest own write delivered before (zero for none).
func (c *leaseCache) ownWrite(l *coordinationv1.Lease, sent time.Time) (prev time.Time, ok bool) {
	v, ok := parseUint(l.ResourceVersion)
	if !ok {
		return time.Time{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.delivered[l.Name]; ok && d >= v {
		return c.countOwnLocked(sent)
	}
	p := c.pending[l.Name]
	p = append(p, pendingWrite{rv: v, sent: sent})
	if len(p) > maxPending {
		p = p[len(p)-maxPending:]
	}
	c.pending[l.Name] = p
	return time.Time{}, false
}

// deliver records that the informer delivered state l of a Lease. It reports whether that
// delivered an own member Lease write the cache was waiting for, and prev, the send time of the
// latest own write delivered before (zero for none).
func (c *leaseCache) deliver(l *coordinationv1.Lease) (prev time.Time, own bool) {
	if l == nil || l.Labels[labelKind] != kindMember {
		return time.Time{}, false
	}
	v, ok := parseUint(l.ResourceVersion)
	if !ok {
		return time.Time{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.delivered[l.Name]; !ok || v > d {
		c.delivered[l.Name] = v
	}
	var sent time.Time
	p := slices.DeleteFunc(c.pending[l.Name], func(w pendingWrite) bool {
		if w.rv > v {
			return false
		}
		if w.sent.After(sent) {
			sent = w.sent
		}
		return true
	})
	if sent.IsZero() {
		return time.Time{}, false
	}
	if len(p) == 0 {
		delete(c.pending, l.Name)
	} else {
		c.pending[l.Name] = p
	}
	return c.countOwnLocked(sent)
}

// countOwnLocked counts a delivered own write sent at sent, and returns prev, the send time of
// the latest own write delivered before (zero for none), and true. The caller holds c.mu.
func (c *leaseCache) countOwnLocked(sent time.Time) (prev time.Time, ok bool) {
	prev = c.ownSentAt
	if sent.After(c.ownSentAt) {
		c.ownSentAt = sent
	}
	return prev, true
}

// delivered is the informer's handler of state l of a Lease, delivered to run c, for freshness.
func (a *Adapter) delivered(c *leaseCache, l *coordinationv1.Lease) {
	if prev, own := c.deliver(l); own {
		a.ownWriteSeen(c, prev)
	}
}

// ownWriteSeen signals (freshChanged) an own write that run c counted as delivered, when the
// latest one before, sent at prev, zero for none, left the cache stale: the new one may make it
// fresh.
func (a *Adapter) ownWriteSeen(c *leaseCache, prev time.Time) {
	now := c.now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, deadline, _ := a.freshRuleLocked(now); prev.IsZero() || !now.Before(prev.Add(deadline)) {
		a.signalFreshLocked()
	}
}

// freshness reports whether the cache is fresh now (K8S-150), and next, the earliest instant at
// which the answer may change without an event that freshChanged signals: when RenewDeadline has
// passed since the latest own write delivered was sent, or when an active session expires. next is
// zero when only an event can change the answer.
func (a *Adapter) freshness() (fresh bool, next time.Time) {
	now := a.now()
	a.mu.Lock()
	active, deadline, next := a.freshRuleLocked(now)
	c := a.cache
	a.mu.Unlock()
	if !active {
		return true, time.Time{}
	}
	if c == nil {
		return false, next
	}
	c.mu.Lock()
	seen := c.ownSentAt
	c.mu.Unlock()
	if seen.IsZero() || !now.Before(seen.Add(deadline)) {
		return false, next
	}
	return true, earliest(next, seen.Add(deadline))
}

// freshRuleLocked returns the inputs of the freshness rule at now: whether the Adapter has
// joined or has an active session, which then needs own writes delivered to be fresh; the
// RenewDeadline that bounds their age, the largest of the active sessions' Timings, else the
// Adapter's; and the earliest expiry of an active session, zero for none. The caller holds a.mu.
func (a *Adapter) freshRuleLocked(now time.Time) (active bool, deadline time.Duration, expiry time.Time) {
	active = a.member.joined
	for _, s := range a.sessions {
		if s.ended || s.expired(now) {
			continue
		}
		active = true
		deadline = max(deadline, s.timing.RenewDeadline)
		expiry = earliest(expiry, s.sent.Add(s.timing.LeaseDuration))
	}
	if deadline == 0 {
		deadline = a.timing.RenewDeadline
	}
	return active, deadline, expiry
}

// earliest returns the earlier of a and b, a zero time standing for none.
func earliest(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

// freshChanged returns a channel that is closed at the next event that may change freshness: a
// session opening or ending, a session's Timing changing, Join, Leave, or a delivered own write
// that makes a stale cache fresh. Changes by the passing of time alone come at freshness's next
// instant instead. Take the channel before reading freshness, so no change falls between.
func (a *Adapter) freshChanged() <-chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.freshCh == nil {
		a.freshCh = make(chan struct{})
	}
	return a.freshCh
}

// signalFreshLocked closes the channel of freshChanged. The caller holds a.mu.
func (a *Adapter) signalFreshLocked() {
	if a.freshCh != nil {
		close(a.freshCh)
		a.freshCh = nil
	}
}

// runningCache returns the running informer's cache, or nil.
func (a *Adapter) runningCache() *leaseCache {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cache
}
