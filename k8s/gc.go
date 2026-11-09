// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Member GC (K8S-260).
//
// While the informer runs, every RetryPeriod of the Adapter's Timing, and only while the cache is
// fresh (K8S-150), the Adapter deletes each member Lease of the group whose resourceVersion it
// first saw at least max(d, 2·wait) ago, d being WithMemberGC's duration and wait the Lease's
// leaseDurationSeconds: a member that crashed without leaving, or a session never closed, leaves
// no Lease behind for good. The deletion carries the observed UID and resourceVersion as
// preconditions, so a Lease written meanwhile stays; each deletion is logged at Info. It never
// deletes the Adapter's own member Leases: the joined member's and those of its active
// sessions. d = 0 turns the deletion off.

// collectMembers runs member GC for one run of the informer, until ctx, the run's, ends.
func (a *Adapter) collectMembers(ctx context.Context) {
	if a.memberGC == 0 {
		return
	}
	tick := time.NewTicker(a.timing.RetryPeriod)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		a.collect(ctx)
	}
}

// collect deletes, once, every member Lease that member GC finds dead for long: unchanged, as
// this Adapter observes it, for max(d, 2·wait), while the cache is fresh, and not its own.
func (a *Adapter) collect(ctx context.Context) {
	if fresh, _ := a.freshness(); !fresh {
		return
	}
	obs, _, ok := a.snapshot()
	if !ok {
		return
	}
	now := a.now()
	own := a.ownMemberLeases()
	var dead []observation
	for name, o := range obs {
		if !isMemberLease(o.lease, a.group) || own[name] {
			continue
		}
		if age := max(a.memberGC, 2*leaseWait(o.lease, a.timing.LeaseDuration)); !now.Before(o.rvSeenAt.Add(age)) {
			dead = append(dead, o)
		}
	}
	slices.SortFunc(dead, func(x, y observation) int { return strings.Compare(x.lease.Name, y.lease.Name) })
	for _, o := range dead {
		l := o.lease
		pre := metav1.Preconditions{UID: &l.UID, ResourceVersion: &l.ResourceVersion}
		err := a.leases().Delete(ctx, l.Name, metav1.DeleteOptions{Preconditions: &pre})
		switch {
		case err == nil:
			a.logger.Info("k8s: deleted member lease", "lease", l.Name, "resourceVersion", l.ResourceVersion,
				"unchanged", now.Sub(o.rvSeenAt))
		case ctx.Err() != nil:
			return
		case apierrors.IsNotFound(err) || apierrors.IsConflict(err):
			// Deleted or written meanwhile: no longer dead for long.
		default:
			a.logger.Warn("k8s: member GC failed", "lease", l.Name, "err", err)
		}
	}
}

// ownMemberLeases returns the names of the Adapter's own member Leases: the joined member's and
// those of its active sessions.
func (a *Adapter) ownMemberLeases() map[string]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	own := map[string]bool{}
	if a.member.joined {
		own[a.member.name] = true
	}
	for _, s := range a.sessions {
		if !s.ended {
			own[s.name] = true
		}
	}
	return own
}
