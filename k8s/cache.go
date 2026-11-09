// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
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

// resyncPeriod is the informer's resync period: none, as observations rest on changes only.
const resyncPeriod = 0

// observation is what this Adapter has observed of one Lease (K8S-110), on its own clock.
type observation struct {
	// lease is the latest state seen. It is shared: never change it.
	lease *coordinationv1.Lease
	// rvSeenAt is when this Adapter first saw the Lease's current resourceVersion.
	rvSeenAt time.Time
	// tokenSeenAt is when this Adapter first saw the Lease's current session token: its UID,
	// holderIdentity and renewTime.
	tokenSeenAt time.Time
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

	mu    sync.Mutex
	obs   map[string]*observation // by Lease name
	tombs map[types.UID]tombstone // of member Leases, by UID
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
		obs:         map[string]*observation{},
		tombs:       map[types.UID]tombstone{},
	}
	selector := labelGroup + "=" + a.group
	informer := coordinationinformers.NewFilteredLeaseInformer(a.client, a.namespace, resyncPeriod, cache.Indexers{},
		func(o *metav1.ListOptions) { o.LabelSelector = selector })
	reg, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.observe(asLease(obj)) },
		UpdateFunc: func(_, obj any) { c.observe(asLease(obj)) },
		DeleteFunc: func(obj any) { c.forget(asLease(obj)) },
	})
	if err != nil {
		// AddEventHandler fails only on an informer that has stopped; this one has not started.
		panic("k8s: add event handler: " + err.Error())
	}
	c.synced = reg.HasSyncedChecker().Done()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	a.runs.Go(func() { informer.RunWithContext(ctx) })
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

// observe records state l of a Lease (K8S-110). A state older than the one recorded, of the same
// Lease, is ignored.
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
	case !ok || o.lease.UID != l.UID:
		if ok {
			// The Lease was deleted and created again without a deletion event, as a relist
			// after a gap in the watch shows it: the deleted one leaves its tombstone all the same.
			c.bury(o.lease, o.tokenSeenAt, now)
		}
		c.obs[l.Name] = &observation{lease: l, rvSeenAt: now, tokenSeenAt: now}
	case l.ResourceVersion == o.lease.ResourceVersion || isOlder(l.ResourceVersion, o.lease.ResourceVersion):
	default:
		next := &observation{lease: l, rvSeenAt: now, tokenSeenAt: o.tokenSeenAt}
		if !sameToken(o.lease, l) {
			next.tokenSeenAt = now
		}
		c.obs[l.Name] = next
	}
}

// forget records the deletion of Lease l: its observation goes, and a member Lease leaves a
// tombstone.
func (c *leaseCache) forget(l *coordinationv1.Lease) {
	if l == nil {
		return
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(now)
	tokenSeenAt := now
	if o, ok := c.obs[l.Name]; ok && o.lease.UID == l.UID {
		tokenSeenAt = o.tokenSeenAt
		delete(c.obs, l.Name)
	}
	c.bury(l, tokenSeenAt, now)
}

// bury leaves the tombstone of deleted Lease l, whose session token was first seen at
// tokenSeenAt, when l is a member Lease and the tombstone has not expired at now. The caller
// holds c.mu.
func (c *leaseCache) bury(l *coordinationv1.Lease, tokenSeenAt, now time.Time) {
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

// runningCache returns the running informer's cache, or nil.
func (a *Adapter) runningCache() *leaseCache {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cache
}
