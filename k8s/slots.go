// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/hmdsefi/tenure"
)

// Slots (K8S-400 to K8S-430, K8S-810).
//
// The slot count is the annotation tenure.slots of the Lease <group>-slots; no such Lease, or one
// without the annotation, means a count of 0. The record of slot n is the Lease
// <group>-slot-<n>: holderIdentity names its holder and tenure.session the UID of the holder's
// member Lease when the holder's session opened, as a shard record's do, and liveness judges its
// holder the same way (K8S-120). A slot is free when it has no record, no holder, or a holder
// that is gone; while the cache is not fresh (K8S-150), time alone turns no holder gone
// (frozen).
//
// ClaimSlot returns the holder's own slot below the count without writing; otherwise it scans
// the slots from 0 up and writes the first free one by compare-and-swap, deciding again on the
// same slot after a conflict, and taking another holder's slot only when holderLive, confirmed
// against the API server, says that holder is gone. ReleaseSlot clears a slot's holder when the
// record names the caller, whatever the session token. SetSlots creates <group>-slots or changes
// only its tenure.slots. Slots reads the cache, or lists the group's Leases once when the
// informer does not run. The only writers of slot Leases are ClaimSlot, ReleaseSlot and
// CloseSession, and of <group>-slots SetSlots (K8S-360). ClaimSlot, ReleaseSlot and SetSlots run
// under the record semaphore, as Acquire does (K8S-710).

var _ tenure.SlotStore = (*Adapter)(nil)

// errSlotTaken is the decision on a slot whose holder is live: ClaimSlot tries the next one.
var errSlotTaken = errors.New("k8s: slot taken")

// ClaimSlot returns the lowest slot below the count whose record names holder, without writing.
// When holder holds no slot below the count, it claims the lowest free one: a slot with no
// record, no holder, or a holder whose session this Adapter has observed to be gone, which it
// confirms against the API server just before it writes; and returns it. It returns -1 and an
// error matching tenure.ErrNoSlot when holder holds a slot only at or above the count, or no slot
// below the count is free. While the Adapter has not seen its own member Lease writes come back
// from its watch within RenewDeadline, the passing of time alone frees no slot. ClaimSlot needs
// an active session of holder in this Adapter, opened by KeepAlive; otherwise it returns an
// error matching tenure.ErrSessionLost without a request. It returns -1 with every error, and
// never writes a Lease that lacks the group's slot labels. It runs at most as many record
// operations at once as WithRecordConcurrency allows.
func (a *Adapter) ClaimSlot(ctx context.Context, holder string) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if a.err != nil {
		return -1, a.err
	}
	if err := checkHolder(a.group, holder); err != nil {
		return -1, fmt.Errorf("k8s: ClaimSlot: %w", err)
	}
	s, err := a.activeSession(holder)
	if err != nil {
		return -1, err
	}
	if err := a.records.acquire(ctx); err != nil {
		return -1, err
	}
	defer a.records.release()
	// The session may have ended or expired while ClaimSlot waited for its place.
	if s, err = a.activeSession(holder); err != nil {
		return -1, err
	}
	// The session holds the informer already; this hold keeps it for the scan, should the
	// session end meanwhile.
	c, err := a.holdCache(ctx)
	if err != nil {
		return -1, err
	}
	defer a.releaseCache(c)

	count, err := a.readCount(ctx, "claim")
	if err != nil {
		return -1, err
	}
	obs, _, _ := a.snapshot()
	own, retired := -1, -1
	for _, o := range obs {
		kind, _, _, n, ok := parseName(a.group, o.lease.Name)
		if !ok || kind != kindSlot || holderOf(o.lease) != holder || checkOwned(o.lease, a.group, kindSlot) != nil {
			continue
		}
		if n < count && (own < 0 || n < own) {
			own = n
		}
		if n >= count && (retired < 0 || n < retired) {
			retired = n
		}
	}
	switch {
	case own >= 0:
		return own, nil
	case retired >= 0:
		return -1, fmt.Errorf("k8s: %s holds slot %d, not below the count %d: %w", holder, retired, count, tenure.ErrNoSlot)
	}

	for n := range count {
		name := slotName(a.group, n)
		var cur *coordinationv1.Lease
		if o, ok := obs[name]; ok {
			cur = o.lease
		}
		written, err := a.writeLease(ctx, "claim", name, cur, func(cur *coordinationv1.Lease) (change, error) {
			return a.decideClaim(ctx, s, cur)
		})
		if errors.Is(err, errSlotTaken) {
			continue
		}
		if err != nil {
			return -1, err
		}
		a.noteRead(written)
		return n, nil
	}
	return -1, fmt.Errorf("k8s: no free slot among %d: %w", count, tenure.ErrNoSlot)
}

// decideClaim decides ClaimSlot's write on cur, the state of a slot record, nil when there is
// none, for session s: a grant when cur is absent, has no holder, or names a holder that is gone
// (frozen while the cache is not fresh); nothing when it names s's holder already; else
// errSlotTaken.
func (a *Adapter) decideClaim(ctx context.Context, s session, cur *coordinationv1.Lease) (change, error) {
	if cur == nil {
		return a.grantSlot(s, nil), nil
	}
	if err := checkOwned(cur, a.group, kindSlot); err != nil {
		return change{}, err
	}
	switch h := holderOf(cur); h {
	case s.holder:
		return change{}, nil
	case "":
	default:
		fresh, _ := a.freshness()
		live, err := a.holderLive(ctx, "claim", cur, fresh)
		if err != nil {
			return change{}, err
		}
		if live {
			return change{}, errSlotTaken
		}
	}
	return a.grantSlot(s, cur), nil
}

// grantSlot returns the write that grants slot record cur, nil when there is none, to session s:
// the slot labels; holderIdentity; tenure.session the UID of s's member Lease; acquireTime and
// renewTime now; the leaseDurationSeconds of s's Timing; leaseTransitions one more when the
// holder changes. Other labels, annotations and fields stay as they are.
func (a *Adapter) grantSlot(s session, cur *coordinationv1.Lease) change {
	l := &coordinationv1.Lease{}
	var transitions int32
	if cur != nil {
		l = cur.DeepCopy()
		if t := cur.Spec.LeaseTransitions; t != nil {
			transitions = *t
		}
		if holderOf(cur) != s.holder && transitions < math.MaxInt32 {
			transitions++
		}
	}
	at := metav1.NewMicroTime(a.now().UTC().Truncate(time.Microsecond))
	holder, seconds := s.holder, leaseSeconds(s.timing)
	if l.Labels == nil {
		l.Labels = map[string]string{}
	}
	maps.Copy(l.Labels, leaseLabels(a.group, kindSlot, ""))
	if l.Annotations == nil {
		l.Annotations = map[string]string{}
	}
	l.Annotations[annotationSession] = string(s.uid)
	l.Spec.HolderIdentity = &holder
	l.Spec.AcquireTime, l.Spec.RenewTime = &at, new(at)
	l.Spec.LeaseDurationSeconds = &seconds
	l.Spec.LeaseTransitions = &transitions
	return change{lease: l}
}

// ReleaseSlot clears the holder of the slot's record when the record names holder: its
// holderIdentity and tenure.session go, the rest stays. ReleaseSlot reads the record from the
// cache, and from the API server when the cache has not seen it or shows another holder. When
// there is no record, or it names another holder, ReleaseSlot writes nothing and returns nil. It
// does not look at holder's session. slot must be in [0, 9999]. It never writes a Lease that
// lacks the group's slot labels, and returns an error for one.
func (a *Adapter) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	if err := checkHolder(a.group, holder); err != nil {
		return fmt.Errorf("k8s: ReleaseSlot: %w", err)
	}
	if slot < 0 || slot > maxSlot {
		return fmt.Errorf("k8s: ReleaseSlot: slot %d: must be in [0, %d]", slot, maxSlot)
	}
	if err := a.records.acquire(ctx); err != nil {
		return err
	}
	defer a.records.release()
	return a.releaseSlot(ctx, slotName(a.group, slot), nil, holder)
}

// releaseSlot clears the holder of slot record name when the record names holder. base is the
// state of the record the caller knows, nil to take the cache's; when there is none, or it does
// not name holder, the record is read from the API server first. The caller holds a place of the
// record semaphore.
func (a *Adapter) releaseSlot(ctx context.Context, name string, base *coordinationv1.Lease, holder string) error {
	decide := func(cur *coordinationv1.Lease) (change, error) { return decideReleaseSlot(cur, a.group, holder) }
	base, err := a.recheck(ctx, "release", name, base, decide)
	if err != nil {
		return err
	}
	written, err := a.writeLease(ctx, "release", name, base, decide)
	a.noteRead(written)
	return err
}

// releaseListedSlot releases slot record l, a listed state that names holder (K8S-240, step 1).
func (a *Adapter) releaseListedSlot(ctx context.Context, l *coordinationv1.Lease, holder string) error {
	if err := a.records.acquire(ctx); err != nil {
		return err
	}
	defer a.records.release()
	return a.releaseSlot(ctx, l.Name, l, holder)
}

// decideReleaseSlot decides ReleaseSlot's write on cur, the state of a slot record of group, nil
// when there is none: when it names holder, clear holderIdentity and tenure.session, keeping the
// rest; else nothing.
func decideReleaseSlot(cur *coordinationv1.Lease, group, holder string) (change, error) {
	if cur == nil {
		return change{}, nil
	}
	if err := checkOwned(cur, group, kindSlot); err != nil {
		return change{}, err
	}
	if holderOf(cur) != holder {
		return change{}, nil
	}
	l := cur.DeepCopy()
	l.Spec.HolderIdentity = nil
	delete(l.Annotations, annotationSession)
	return change{lease: l}, nil
}

// SetSlots sets the slot count of the group: it creates the Lease that holds the count, or
// changes only the count on it. It writes no slot record, so the holder of a slot at or above the
// new count keeps it until it releases the slot or its session is closed. count must be in
// [0, 10000]. SetSlots needs no session. It never writes a Lease that lacks the group's slot
// count labels, and returns an error for one.
func (a *Adapter) SetSlots(ctx context.Context, count int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	if count < 0 || count > maxSlots {
		return fmt.Errorf("k8s: SetSlots: count %d: must be in [0, %d]", count, maxSlots)
	}
	if err := a.records.acquire(ctx); err != nil {
		return err
	}
	defer a.records.release()

	name, value := slotsName(a.group), encodeSlots(count)
	decide := func(cur *coordinationv1.Lease) (change, error) {
		if cur == nil {
			return change{lease: &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
				Labels:      leaseLabels(a.group, kindSlots, ""),
				Annotations: map[string]string{annotationSlots: value},
			}}}, nil
		}
		if err := checkOwned(cur, a.group, kindSlots); err != nil {
			return change{}, err
		}
		if v, ok := cur.Annotations[annotationSlots]; ok && v == value {
			return change{}, nil
		}
		l := cur.DeepCopy()
		if l.Annotations == nil {
			l.Annotations = map[string]string{}
		}
		l.Annotations[annotationSlots] = value
		return change{lease: l}, nil
	}
	base, err := a.recheck(ctx, "set", name, nil, decide)
	if err != nil {
		return err
	}
	written, err := a.writeLease(ctx, "set", name, base, decide)
	a.noteRead(written)
	return err
}

// Slots returns the slot count and one entry per slot below it: the holder the slot's record
// names while that holder's session is live as this Adapter observes it, else "". When the count
// is 0 it returns 0, nil, nil. Slots reads what the Adapter's watch has observed while one runs:
// while the Adapter has joined or holds a session, but has not seen its own member Lease writes
// come back from the watch within RenewDeadline, the passing of time alone turns no holder gone.
// Otherwise Slots lists the group's Leases with one request, and a holder whose member Lease
// names it counts as live. It returns an error when the count on the group's Lease is not a
// number in [0, 10000]; a Lease at the count's name that lacks the group's labels counts as no
// count.
func (a *Adapter) Slots(ctx context.Context) (count int, holders []string, err error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if a.err != nil {
		return 0, nil, a.err
	}
	fresh, _ := a.freshness()
	now := a.now()
	obs, tombs, ok := a.snapshot()
	if !ok {
		list, err := a.leases().List(ctx, metav1.ListOptions{LabelSelector: labelGroup + "=" + a.group})
		if err != nil {
			return 0, nil, fmt.Errorf("k8s: slots: list leases: %w", err)
		}
		// Every Lease is first seen now.
		obs = make(map[string]observation, len(list.Items))
		for i := range list.Items {
			l := &list.Items[i]
			obs[l.Name] = observation{lease: l, rvSeenAt: now, tokenSeenAt: now, named: holderOf(l)}
		}
		fresh = true
	}
	return a.slotsOf(obs, tombs, now, fresh)
}

// slotsOf returns the slot count and the live holder of each slot below it, as obs, the group's
// Leases as observed, by name, and tombs, the member Lease tombstones, show them at now; fresh
// is the cache's freshness, frozen's input (K8S-400, K8S-150). Records and a count Lease that
// lack the group's labels are left out; a count of 0 gives 0, nil, nil. The views build their
// slot fields with it.
func (a *Adapter) slotsOf(obs map[string]observation, tombs map[types.UID]tombstone, now time.Time, fresh bool) (int, []string, error) {
	count := 0
	if o, ok := obs[slotsName(a.group)]; ok && checkOwned(o.lease, a.group, kindSlots) == nil {
		n, err := countOf(o.lease)
		if err != nil {
			return 0, nil, err
		}
		count = n
	}
	if count == 0 {
		return 0, nil, nil
	}
	holders := make([]string, count)
	for n := range holders {
		o, ok := obs[slotName(a.group, n)]
		if ok && checkOwned(o.lease, a.group, kindSlot) == nil && a.slotVerdict(o, obs, tombs, now, fresh).live {
			holders[n] = holderOf(o.lease)
		}
	}
	return count, holders, nil
}

// slotVerdict judges the holder of slot record rec as obs and tombs show it at now, with no
// request (K8S-120), frozen while the cache is not fresh (K8S-150).
func (a *Adapter) slotVerdict(rec observation, obs map[string]observation, tombs map[types.UID]tombstone, now time.Time, fresh bool) verdict {
	var member *observation
	if h := holderOf(rec.lease); h != "" && checkHolder(a.group, h) == nil {
		if m, ok := obs[memberName(a.group, memberOf(h))]; ok && isMemberLease(m.lease, a.group) {
			member = &m
		}
	}
	var tomb *tombstone
	if s, ok := decodeSession(rec.lease.Annotations); ok {
		if t, ok := tombs[s]; ok {
			tomb = &t
		}
	}
	return frozen(liveness(rec, member, tomb, now, a.timing.LeaseDuration), fresh)
}

// readCount returns the slot count for op, from the cache's state of the count Lease, or else
// the API server's: 0 when there is none. It returns K8S-050's error for a Lease at the count's
// name that lacks the group's labels.
func (a *Adapter) readCount(ctx context.Context, op string) (int, error) {
	l, err := a.readRecord(ctx, op, slotsName(a.group))
	if err != nil || l == nil {
		return 0, err
	}
	if err := checkOwned(l, a.group, kindSlots); err != nil {
		return 0, err
	}
	return countOf(l)
}

// countOf returns the slot count on l, the group's count Lease: 0 without tenure.slots, and an
// error when tenure.slots is not a count in [0, 10000].
func countOf(l *coordinationv1.Lease) (int, error) {
	v, found := l.Annotations[annotationSlots]
	if !found {
		return 0, nil
	}
	n, ok := decodeSlots(l.Annotations)
	if !ok {
		return 0, fmt.Errorf("k8s: lease %s: %s %q is not a slot count in [0, %d]", l.Name, annotationSlots, v, maxSlots)
	}
	return n, nil
}
