// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
)

// Liveness of a record's holder (K8S-120, K8S-130, K8S-140).
//
// A record (a shard or slot Lease) names holder h = <id>/<incarnation> and, in tenure.session,
// the UID s of h's member Lease when h's session opened. M is the member Lease
// <group>-member-<id> as this Adapter observed it. liveness decides from these observations
// alone, on the Adapter's monotonic clock, whether h's session is still live:
//
//	case       condition                                    result                         GET M
//	vacant     the record names no holder                   gone                           no
//	current    M.uid == s, M names h                        live while now-tokenSeenAt(M)  when
//	                                                        < wait(M), else expired        expired
//	ended      M.uid == s, M names no holder                gone: only h's CloseSession    no
//	                                                        clears it
//	replaced   M.uid == s, M names another holder           gone: a new incarnation opens  yes
//	                                                        only once the old one stopped
//	tombstone  M absent or M.uid != s, a tombstone of s     live until the tombstone's     when
//	                                                        tokenSeenAt + wait             passed
//	missing    no M of s and no tombstone of s, or s        live until rvSeenAt(record) +  when
//	           missing or malformed                         wait(record)                   passed
//
// wait(M) is M's leaseDurationSeconds, the holder's own LeaseDuration; wait(record) is the
// record's; either is the Adapter's LeaseDuration when the Lease has none.
//
// A gone verdict that asks for a GET is only a candidate (K8S-130): the acquirer reads the
// record state its write will carry, then reads M from the API server, notes it as an
// observation (a changed session token resets tokenSeenAt) and decides again before it writes.
//
// No time written in a Lease, renewTime or acquireTime, is ever compared with a clock
// (K8S-140): only the moments this Adapter first saw a state count.
//
// While the cache is not fresh (K8S-150, cache.go), slots and views take the verdict through
// frozen: time alone never turns a holder from live to gone then.

// The cases of liveness.
const (
	caseVacant    = "vacant"
	caseCurrent   = "current"
	caseExpired   = "expired"
	caseEnded     = "ended"
	caseReplaced  = "replaced"
	caseTombstone = "tombstone"
	caseMissing   = "missing"
)

// verdict is liveness's decision on a record's holder.
type verdict struct {
	// live reports whether the holder's session is live.
	live bool
	// until is when a live verdict ends unless the observations change; zero when gone.
	until time.Time
	// get asks the caller to confirm a gone verdict against the API server, by reading the
	// member Lease and deciding again, before acting on it (K8S-130).
	get bool
	// why is the case of the table that applied.
	why string
}

// liveness decides whether the holder of record rec is live at now (K8S-120). member is the
// observation of the member Lease of rec's holder, nil when there is none; tomb is the
// tombstone of the member Lease whose UID rec's tenure.session holds, nil when there is none.
// defaultWait is the Adapter's LeaseDuration, the wait of a Lease without
// leaseDurationSeconds. It is a pure function of its arguments, and reads no written time.
func liveness(rec observation, member *observation, tomb *tombstone, now time.Time, defaultWait time.Duration) verdict {
	h := holderOf(rec.lease)
	if h == "" {
		return verdict{why: caseVacant}
	}
	s, ok := decodeSession(rec.lease.Annotations)
	if ok && member != nil && member.lease.UID == s {
		switch holderOf(member.lease) {
		case h:
			return liveUntil(member.tokenSeenAt.Add(leaseWait(member.lease, defaultWait)), now, caseCurrent, caseExpired)
		case "":
			return verdict{why: caseEnded}
		default:
			return verdict{get: true, why: caseReplaced}
		}
	}
	if ok && tomb != nil {
		return liveUntil(tomb.tokenSeenAt.Add(tomb.wait), now, caseTombstone, caseTombstone)
	}
	return liveUntil(rec.rvSeenAt.Add(leaseWait(rec.lease, defaultWait)), now, caseMissing, caseMissing)
}

// frozen returns verdict v as an Adapter whose cache is not fresh takes it (K8S-150): a holder
// that only the passing of time turned gone (expired, tombstone, missing) stays live, with no
// end and nothing to confirm, as the cache may just not have seen it renew. A gone verdict that
// rests on an observed change (vacant, ended, replaced) stands. When fresh, v stands as it is.
func frozen(v verdict, fresh bool) verdict {
	if fresh || v.live {
		return v
	}
	switch v.why {
	case caseExpired, caseTombstone, caseMissing:
		return verdict{live: true, why: v.why}
	}
	return v
}

// liveUntil is live, case live, before until, and gone, case gone, to be confirmed by a GET, from
// until on.
func liveUntil(until, now time.Time, live, gone string) verdict {
	if now.Before(until) {
		return verdict{live: true, until: until, why: live}
	}
	return verdict{get: true, why: gone}
}

// leaseWait returns the leaseDurationSeconds of l, or def when it has none.
func leaseWait(l *coordinationv1.Lease, def time.Duration) time.Duration {
	if d := l.Spec.LeaseDurationSeconds; d != nil {
		return time.Duration(*d) * time.Second
	}
	return def
}
