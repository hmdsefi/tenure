// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestK8S120Liveness checks the liveness decision over every case: vacant; current, live and
// expired; ended; replaced, by a live and an ended incarnation; tombstone, live and passed;
// missing, with no member Lease, another member Lease, a missing or malformed tenure.session,
// live and passed. Each wait comes from the right Lease, or from the Adapter's LeaseDuration
// when the Lease has none. Only the gone verdicts that K8S-130 confirms ask for a GET; ended
// does not when this Adapter saw the member Lease name the holder before it was cleared. Every row is decided again with written times far in the past and far in the
// future (K8S-140), and the decision changes none of its inputs.
func TestK8S120Liveness(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	const (
		h     = "pod-0/1"
		s     = types.UID("6f1c1a43-6c55-4f4e-9d0e-2b8f2b0c1d11")
		other = types.UID("0b7d2b4e-1f3a-4c5d-8e9f-a0b1c2d3e4f5")
	)
	secs := func(n int32) *int32 { return &n }
	record := func(holder, session string, seconds *int32) *coordinationv1.Lease {
		l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "g-markets-0", Labels: leaseLabels("g", kindShard, "markets")}}
		if session != "" {
			l.Annotations = map[string]string{annotationSession: session}
		}
		if holder != "" {
			l.Spec.HolderIdentity = &holder
		}
		l.Spec.LeaseDurationSeconds = seconds
		return l
	}
	member := func(uid types.UID, holder string, seconds *int32, tokenSeenAt time.Time) *observation {
		l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "g-member-pod-0", UID: uid, Labels: leaseLabels("g", kindMember, "")}}
		if holder != "" {
			l.Spec.HolderIdentity = &holder
		}
		l.Spec.LeaseDurationSeconds = seconds
		// rvSeenAt is later than tokenSeenAt, as after a change that kept the session token.
		return &observation{lease: l, rvSeenAt: tokenSeenAt.Add(2 * time.Second), tokenSeenAt: tokenSeenAt, named: holder}
	}
	// seenNaming is observation o of a member Lease that this Adapter saw name holder before.
	seenNaming := func(o *observation, holder string) *observation {
		o.named = holder
		return o
	}
	tomb := func(tokenSeenAt time.Time, wait time.Duration) *tombstone {
		return &tombstone{tokenSeenAt: tokenSeenAt, wait: wait}
	}
	live := func(until time.Time, why string) verdict { return verdict{live: true, until: until, why: why} }
	gone := func(why string) verdict { return verdict{get: why != caseVacant && why != caseEnded, why: why} }

	tests := []struct {
		name   string
		rec    *coordinationv1.Lease
		seenAt time.Time // the record's rvSeenAt
		member *observation
		tomb   *tombstone
		now    time.Time
		wait   time.Duration // the Adapter's LeaseDuration; 3s when zero
		want   verdict
	}{
		{name: "vacant", rec: record("", string(s), secs(3)), seenAt: t0, member: member(s, h, secs(3), t0), now: t0, want: gone(caseVacant)},
		{name: "vacant without session", rec: record("", "", nil), seenAt: t0, now: t0, want: gone(caseVacant)},

		{
			name: "current, live", rec: record(h, string(s), secs(3)), seenAt: t0, member: member(s, h, secs(3), t0),
			now: t0.Add(3*time.Second - time.Nanosecond), want: live(t0.Add(3*time.Second), caseCurrent),
		},
		{
			name: "current, live while the record's own rvSeenAt has passed", rec: record(h, string(s), secs(3)), seenAt: t0.Add(-time.Hour),
			member: member(s, h, secs(3), t0), now: t0.Add(time.Second), want: live(t0.Add(3*time.Second), caseCurrent),
		},
		{
			name: "current, expired at tokenSeenAt + wait", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(s, h, secs(3), t0), now: t0.Add(3 * time.Second), want: gone(caseExpired),
		},
		{
			name: "current, expired later", rec: record(h, string(s), secs(3)), seenAt: t0.Add(time.Hour),
			member: member(s, h, secs(3), t0), now: t0.Add(time.Hour), want: gone(caseExpired),
		},
		{
			name: "current, waits the member Lease's own duration", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(s, h, secs(10), t0), now: t0.Add(9 * time.Second), wait: time.Second, want: live(t0.Add(10*time.Second), caseCurrent),
		},
		{
			name: "current, member Lease without duration waits the Adapter's", rec: record(h, string(s), secs(10)), seenAt: t0,
			member: member(s, h, nil, t0), now: t0.Add(4 * time.Second), wait: 5 * time.Second, want: live(t0.Add(5*time.Second), caseCurrent),
		},
		{
			name: "current, member Lease without duration, expired", rec: record(h, string(s), secs(10)), seenAt: t0,
			member: member(s, h, nil, t0), now: t0.Add(5 * time.Second), wait: 5 * time.Second, want: gone(caseExpired),
		},
		{
			name: "current, a tombstone of the session does not count", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(s, h, secs(3), t0), tomb: tomb(t0, time.Hour), now: t0.Add(4 * time.Second), want: gone(caseExpired),
		},

		{
			name: "ended", rec: record(h, string(s), secs(3)), seenAt: t0, member: seenNaming(member(s, "", secs(3), t0), h),
			now: t0, want: gone(caseEnded),
		},
		{
			name: "ended, however recent the record", rec: record(h, string(s), secs(3)), seenAt: t0.Add(time.Second),
			member: seenNaming(member(s, "", secs(3), t0.Add(time.Second)), h), tomb: tomb(t0, time.Hour), now: t0.Add(time.Second),
			want: gone(caseEnded),
		},
		{
			name: "ended, the holder never seen on the member Lease", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(s, "", secs(3), t0), now: t0, want: verdict{get: true, why: caseEnded},
		},
		{
			name: "ended, another incarnation seen on the member Lease", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: seenNaming(member(s, "", secs(3), t0), "pod-0/2"), now: t0, want: verdict{get: true, why: caseEnded},
		},

		{
			name: "replaced by a live incarnation", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(s, "pod-0/2", secs(3), t0), now: t0, want: gone(caseReplaced),
		},
		{
			name: "replaced by an ended incarnation", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(s, "pod-0/2", secs(3), t0.Add(-time.Hour)), now: t0, want: gone(caseReplaced),
		},

		{
			name: "tombstone, live", rec: record(h, string(s), secs(3)), seenAt: t0.Add(-time.Hour),
			tomb: tomb(t0, 3*time.Second), now: t0.Add(2 * time.Second), want: live(t0.Add(3*time.Second), caseTombstone),
		},
		{
			name: "tombstone, member Lease created anew", rec: record(h, string(s), secs(3)), seenAt: t0.Add(-time.Hour),
			member: member(other, h, secs(3), t0.Add(time.Second)), tomb: tomb(t0, 3*time.Second), now: t0.Add(2 * time.Second),
			want: live(t0.Add(3*time.Second), caseTombstone),
		},
		{
			name: "tombstone, waits the deleted Lease's duration", rec: record(h, string(s), secs(3)), seenAt: t0,
			tomb: tomb(t0, 10*time.Second), now: t0.Add(9 * time.Second), want: live(t0.Add(10*time.Second), caseTombstone),
		},
		{
			name: "tombstone, passed", rec: record(h, string(s), secs(3)), seenAt: t0.Add(time.Hour),
			tomb: tomb(t0, 3*time.Second), now: t0.Add(3 * time.Second), want: gone(caseTombstone),
		},

		{
			name: "missing, no member Lease", rec: record(h, string(s), secs(3)), seenAt: t0,
			now: t0.Add(2 * time.Second), want: live(t0.Add(3*time.Second), caseMissing),
		},
		{
			name: "missing, another member Lease", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(other, h, secs(3), t0.Add(-time.Hour)), now: t0.Add(2 * time.Second), want: live(t0.Add(3*time.Second), caseMissing),
		},
		{
			name: "missing, waits the record's duration", rec: record(h, string(s), secs(7)), seenAt: t0,
			now: t0.Add(6 * time.Second), wait: time.Second, want: live(t0.Add(7*time.Second), caseMissing),
		},
		{
			name: "missing, record without duration waits the Adapter's", rec: record(h, string(s), nil), seenAt: t0,
			now: t0.Add(4 * time.Second), wait: 5 * time.Second, want: live(t0.Add(5*time.Second), caseMissing),
		},
		{
			name: "missing, passed", rec: record(h, string(s), secs(3)), seenAt: t0,
			member: member(other, h, secs(3), t0.Add(2*time.Second)), now: t0.Add(3 * time.Second), want: gone(caseMissing),
		},
		{
			name: "missing session, though the member Lease is current", rec: record(h, "", secs(3)), seenAt: t0,
			member: member(s, h, secs(3), t0.Add(time.Hour)), tomb: tomb(t0, time.Hour), now: t0.Add(3 * time.Second), want: gone(caseMissing),
		},
		{
			name: "missing session, live", rec: record(h, "", secs(3)), seenAt: t0,
			member: member(s, "", secs(3), t0), now: t0.Add(time.Second), want: live(t0.Add(3*time.Second), caseMissing),
		},
		{
			name: "malformed session matches no member Lease", rec: record(h, "not-a-uuid", secs(3)), seenAt: t0,
			member: member("not-a-uuid", "", secs(3), t0), now: t0.Add(time.Second), want: live(t0.Add(3*time.Second), caseMissing),
		},
		{
			name: "session in upper-case hex", rec: record(h, "6F1C1A43-6C55-4F4E-9D0E-2B8F2B0C1D11", secs(3)), seenAt: t0,
			member: member("6F1C1A43-6C55-4F4E-9D0E-2B8F2B0C1D11", h, secs(3), t0), now: t0.Add(time.Second), want: live(t0.Add(3*time.Second), caseCurrent),
		},
		{
			name: "malformed session ignores tombstones", rec: record(h, "6f1c1a43", secs(3)), seenAt: t0,
			tomb: tomb(t0, time.Hour), now: t0.Add(3 * time.Second), want: gone(caseMissing),
		},
	}

	// Written times: none, far in the past and far in the future, on the record and the member
	// Lease alike.
	past := metav1.NewMicroTime(time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC))
	future := metav1.NewMicroTime(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC))
	written := []struct {
		name             string
		acquire, renewal *metav1.MicroTime
	}{
		{"no written times", nil, nil},
		{"written times in the past", &past, &past},
		{"written times in the future", &future, &future},
		{"acquired in the future, renewed in the past", &future, &past},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wait := tc.wait
			if wait == 0 {
				wait = 3 * time.Second
			}
			for _, w := range written {
				rec := tc.rec.DeepCopy()
				rec.Spec.AcquireTime, rec.Spec.RenewTime = w.acquire, w.renewal
				var m *observation
				if tc.member != nil {
					m = &observation{lease: tc.member.lease.DeepCopy(), rvSeenAt: tc.member.rvSeenAt, tokenSeenAt: tc.member.tokenSeenAt, named: tc.member.named}
					m.lease.Spec.AcquireTime, m.lease.Spec.RenewTime = w.acquire, w.renewal
				}
				var tb *tombstone
				if tc.tomb != nil {
					tb = new(*tc.tomb)
				}
				recBefore := rec.DeepCopy()
				var mBefore *coordinationv1.Lease
				if m != nil {
					mBefore = m.lease.DeepCopy()
				}
				// The record's tokenSeenAt is far from its rvSeenAt: only rvSeenAt counts.
				got := liveness(observation{lease: rec, rvSeenAt: tc.seenAt, tokenSeenAt: tc.seenAt.Add(-24 * time.Hour)}, m, tb, tc.now, wait)
				if got != tc.want {
					t.Errorf("%s: liveness = %+v, want %+v", w.name, got, tc.want)
				}
				if !equality.Semantic.DeepEqual(rec, recBefore) || (m != nil && !equality.Semantic.DeepEqual(m.lease, mBefore)) ||
					(tb != nil && *tb != *tc.tomb) {
					t.Errorf("%s: liveness changed its inputs", w.name)
				}
			}
		})
	}
}
