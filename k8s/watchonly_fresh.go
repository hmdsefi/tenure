// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/tools/cache"
)

// Freshness of a watch-only Adapter (RTR-710, RTR-720).
//
// A writer judges its cache fresh by seeing its own member Lease writes come back (K8S-150); a
// watch-only Adapter writes nothing. Instead, while its view lists a member, it expects a Lease
// event at least every RenewDeadline, as every member renews its Lease every RetryPeriod. When
// none has come for RenewDeadline (quiet), it restarts its informer: it stops the running one
// and starts another on the same observations, which lists the Leases again. From the quiet
// instant until that list has been observed, the Adapter is stale: freshness reports the cache
// not fresh, so time alone removes no member and turns no slot holder gone (frozen, K8S-620), and
// Stale reports true.
//
// The new list is observed like any delivery, but with resourceVersions compared for equality
// only, never by order (equalOnly in observe): a Lease whose resourceVersion differs from the one
// last observed counts as changed at that instant; an equal one keeps its rvSeenAt, and so keeps
// aging. A Lease the list no longer shows was deleted during the gap, and is forgotten as a
// deletion found by a relist. The list's own deliveries count as Lease events, so a relist comes
// at most once per quiet RenewDeadline. When the new informer's list fails, it is stopped and
// started again after a backoff that starts at RetryPeriod and doubles up to LeaseDuration; the
// Adapter stays stale meanwhile.
//
// Each informer of a run has a generation, and the event handlers of an informer that is no
// longer the run's drop its events, which a stopped informer may still hand over. Lock order:
// views.mu, then the Adapter's mu, then the cache's mu.

// informerRun is one informer of a run of the cache.
type informerRun struct {
	informer cache.SharedIndexInformer
	// cancel stops the informer; done is closed once it has stopped and its handlers returned.
	cancel context.CancelFunc
	done   <-chan struct{}
	// synced is closed once its initial list has been observed.
	synced <-chan struct{}
	// failed is closed at its first list or watch error.
	failed <-chan struct{}
}

// stop stops the informer and waits until its handlers have returned.
func (r *informerRun) stop() {
	r.cancel()
	<-r.done
}

// Stale reports whether the views of a watch-only Adapter may be out of date: its watch has
// delivered no Lease event for the RenewDeadline of its Timing while its latest view lists a
// member, and the list that restarts the watch has not been observed yet. While it is stale, the
// passing of time alone removes no member from its views and turns no slot holder gone. Stale is
// always false for an Adapter that is not watch-only, whose own writes tell it whether its view
// is fresh.
func (a *Adapter) Stale() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stale
}

// setStale records whether the Adapter is stale, and signals a change of freshness.
func (a *Adapter) setStale(stale bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stale != stale {
		a.stale = stale
		a.signalFreshLocked()
	}
}

// viewListsMember reports whether a Watch channel is open and the latest view lists a member.
func (a *Adapter) viewListsMember() bool {
	v := &a.views
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.watches) > 0 && len(v.view.Members) > 0
}

// followFreshness relists for run c of a watch-only Adapter whenever its watch has been quiet
// for RenewDeadline while its view lists a member, starting from informer run, until ctx, the
// run's, ends.
func (a *Adapter) followFreshness(ctx context.Context, c *leaseCache, run *informerRun) {
	rd := a.timing.RenewDeadline
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	// from is when the latest check found no member listed: the quiet period counts again from
	// there.
	var from time.Time
	for {
		quiet := c.lastEventAt()
		if from.After(quiet) {
			quiet = from
		}
		timer.Reset(max(quiet.Add(rd).Sub(a.now()), time.Millisecond))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		now := a.now()
		if now.Before(c.lastEventAt().Add(rd)) {
			continue
		}
		if !a.viewListsMember() {
			from = now
			continue
		}
		if run = a.relist(ctx, c, run); run == nil {
			return
		}
		from = a.now()
	}
}

// relist stops informer old of run c and starts new ones until one has observed its initial
// list, with backoff between failed ones, while the Adapter is stale. It returns the informer
// that listed, or nil when ctx ends first.
func (a *Adapter) relist(ctx context.Context, c *leaseCache, old *informerRun) *informerRun {
	a.setStale(true)
	defer a.setStale(false)
	old.stop()
	backoff := a.timing.RetryPeriod
	for {
		run := a.startInformer(ctx, c)
		select {
		case <-run.synced:
		case <-run.failed:
		case <-ctx.Done():
			return nil
		}
		select {
		case <-run.synced:
			// Listed, even if an error followed: the informer goes on from there.
			a.forgetUnlisted(c, run.informer.GetStore())
			a.viewsChanged()
			return run
		default:
			run.stop()
		}
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
		backoff = min(2*backoff, a.timing.LeaseDuration)
	}
}

// noteEvent records that the current informer of run c delivered an event now.
func (c *leaseCache) noteEvent() {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastEvent = now
}

// lastEventAt returns when the current informer of run c last delivered an event, or when the
// run started.
func (c *leaseCache) lastEventAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastEvent
}

// forgetUnlisted forgets, as deletions found by a relist, the Leases that run c has observed but
// that store, a new informer's after its initial list, does not hold.
func (a *Adapter) forgetUnlisted(c *leaseCache, store cache.Store) {
	c.mu.Lock()
	var gone []*coordinationv1.Lease
	for _, o := range c.obs {
		if _, ok, _ := store.GetByKey(a.namespace + "/" + o.lease.Name); !ok {
			gone = append(gone, o.lease)
		}
	}
	c.mu.Unlock()
	for _, l := range gone {
		c.forget(l, true)
	}
}
