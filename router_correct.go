// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import "time"

// correction records that a member answered for a shard with the address of the member that
// serves it (RTR-400). It belongs to the snapshot it was made in (RTR-420): a lookup ignores it
// once the snapshot has another version, or once it is older than the correction TTL.
type correction struct {
	version uint64    // the snapshot version it was made in
	at      time.Time // the clock's time when it was made
	member  Member    // the member with the corrected address, as the snapshot's view lists it
}

// Correct records that addr serves shard of space, as a member's redirect says when the Router's
// view lags the members'. Until the Router takes a new snapshot, and for at most the correction
// TTL (see WithCorrectionTTL), lookups of that shard return the member of the current snapshot's
// view whose Addr is addr. Correct returns false, and records nothing, when no member of the
// snapshot's view has addr, for a space the Router does not have, for a shard outside the space,
// for a space whose shard count a member disagrees with, before the first view and after Close;
// the caller then follows the redirect for that request
// only. A correction leaves lookups without allocation.
func (r *Router) Correct(space string, shard int, addr string) bool {
	h, ok := r.byName[space]
	s := r.snap.Load()
	if !ok || s.closed || s.view == nil || shard < 0 || shard >= h.shards || s.tables[h.index].mismatch != nil {
		return false
	}
	for _, m := range s.view.Members {
		if m.Addr == addr && addr != "" {
			c := &correction{version: s.version, at: r.opts.now(), member: m}
			if h.corrections[shard].Swap(c) == nil {
				h.live.Add(1)
			}
			r.events.emit(RouterEvent{Kind: RouterEventCorrected, Version: s.version, Space: space, Shard: shard, Addr: addr})
			return true
		}
	}
	return false
}

// corrected returns the member that serves shard in snapshot s while some correction of the
// space is set: a correction of the shard made in s and younger than the TTL, else the
// snapshot's owner. Lookups call it only then, so that without corrections they read one
// counter more than the snapshot lookup (RTR-410); with one, they read the clock as well.
//
// A lookup reads the counter before the snapshot, and publish stores a new snapshot before it
// drops the old one's corrections, so a correction of s that a lookup misses was dropped after a
// newer snapshot was stored (RTR-420). corrected then answers from the newest snapshot instead of
// from s, whose owner the correction overrode.
func (h *RouteSpace) corrected(s *routerSnapshot, shard int) (Member, error) {
	for {
		if c := h.corrections[shard].Load(); c != nil && c.version == s.version && s.tables[h.index].mismatch == nil &&
			h.r.opts.now().Sub(c.at) < h.r.opts.correctionTTL {
			return c.member, nil
		}
		now := h.r.snap.Load()
		if now == s {
			return s.route(h.index, shard)
		}
		if now.closed {
			return Member{}, ErrClosed
		}
		s = now
	}
}

// dropCorrections clears every correction not made in the snapshot of version, which lookups
// ignore anyway (RTR-420), so that lookups skip them again. publish calls it after it stores that
// snapshot: a lookup that still holds the old one then finds the newer one in corrected. It keeps
// a correction a Correct made in the new snapshot meanwhile; a Correct racing with it may leave
// one of an old snapshot, which lookups ignore. The caller holds r.mu.
func (r *Router) dropCorrections(version uint64) {
	for _, h := range r.spaces {
		if h.live.Load() == 0 {
			continue
		}
		for i := range h.corrections {
			for {
				c := h.corrections[i].Load()
				if c == nil || c.version == version {
					break
				}
				if h.corrections[i].CompareAndSwap(c, nil) {
					h.live.Add(-1)
					break
				}
			}
		}
	}
}
