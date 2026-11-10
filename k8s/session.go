// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Sessions (K8S-200 to K8S-240).
//
// A holder <id>/<incarnation> keeps its session on the member Lease <group>-member-<id>: the
// Lease names the holder in holderIdentity, and each renewal moves its renewTime on. The
// session table keeps, per holder, what this Adapter knows of its session; a session is active
// from its opening until it ends here, and an ended holder stays ended for the Adapter's life.
// Writes to one member Lease are serialized by lockLease.

// session is one row of the session table. The Adapter's mu guards its fields; a write also
// holds the member Lease's lock.
type session struct {
	holder string
	// name is the member Lease of the holder.
	name string
	// ended is set when the session ends here: lost, expired or closed.
	ended bool
	// uid is the member Lease's UID when the session opened: the session token that records
	// written for this session carry.
	uid types.UID
	// lease is the member Lease as this Adapter last wrote it.
	lease *coordinationv1.Lease
	// timing is the session's Timing, from its latest successful KeepAlive, or from a later
	// failed one with a shorter LeaseDuration (renew).
	timing core.Timing
	// sent is when the latest successful write of the session was sent, on the Adapter's clock.
	sent time.Time
	// cleared is set once the member Lease is known not to name the holder any more.
	cleared bool
	// cache is the informer hold of an active session, nil once it ended.
	cache *leaseCache
}

// expired reports whether session s has expired at now: LeaseDuration of its Timing after its
// latest successful write was sent. A write sent from then on could renew a session that other
// Adapters may already have seen end.
func (s *session) expired(now time.Time) bool {
	return !now.Before(s.sent.Add(s.timing.LeaseDuration))
}

// expiredError is the error of KeepAlive for holder, whose session has expired.
func expiredError(holder string) error {
	return fmt.Errorf("k8s: session %s expired: %w", holder, tenure.ErrSessionLost)
}

// memberOf returns the member ID of holder: the text before its first "/".
func memberOf(holder string) string {
	id, _, _ := strings.Cut(holder, "/")
	return id
}

// liveSession returns a copy of the active session of holder, ok false when the Adapter knows
// no session of holder. It returns ErrSessionLost, and no session, for a holder whose session
// ended here, and ends a session that has expired: LeaseDuration of its Timing after its
// latest successful write was sent. It makes no request.
func (a *Adapter) liveSession(holder string) (s session, ok bool, err error) {
	now := a.now()
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.sessions[holder]
	switch {
	case p == nil:
		return session{}, false, nil
	case p.ended:
		return session{}, false, fmt.Errorf("k8s: session %s ended: %w", holder, tenure.ErrSessionLost)
	case p.expired(now):
		a.endSessionLocked(p)
		return session{}, false, expiredError(holder)
	}
	return *p, true, nil
}

// endSessionLocked ends session s and gives back its informer hold. The caller holds a.mu.
func (a *Adapter) endSessionLocked(s *session) {
	if !s.ended {
		a.signalFreshLocked()
	}
	s.ended = true
	if s.cache != nil {
		a.releaseCacheLocked(s.cache)
		s.cache = nil
	}
}

// lockLease takes the write lock of Lease name, waiting for it until ctx ends.
func (a *Adapter) lockLease(ctx context.Context, name string) (unlock func(), err error) {
	a.mu.Lock()
	l := a.leaseLocks[name]
	if l == nil {
		l = make(chan struct{}, 1)
		a.leaseLocks[name] = l
	}
	a.mu.Unlock()
	select {
	case l <- struct{}{}:
		return func() { <-l }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// KeepAlive opens a session for holder on its member Lease, or renews the session, with the
// Lease duration of timing. The first KeepAlive of a holder creates the member Lease naming the
// holder, with the member record when the Adapter has joined as the holder's member, or takes
// over the Lease when it names no holder or another incarnation of the same member, which ends
// that incarnation's session. A renewal moves the Lease's renewTime on with
// one write. KeepAlive returns an error matching tenure.ErrSessionLost, without a request, for a
// holder whose session ended in this Adapter or expired, LeaseDuration after its latest
// successful renewal was sent; when a renewal's response comes back at or after that expiry,
// although the renewal was written; and when a renewal finds the Lease deleted, naming another
// holder or no holder. Any other failed renewal leaves the session as it was. holder must be
// <member ID>/<incarnation>, and the LeaseDuration of timing a whole number of seconds.
func (a *Adapter) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	if err := checkHolder(a.group, holder); err != nil {
		return fmt.Errorf("k8s: KeepAlive: %w", err)
	}
	if err := timing.Validate(); err != nil {
		return fmt.Errorf("k8s: KeepAlive: %w", err)
	}
	if err := checkWholeSeconds(timing); err != nil {
		return fmt.Errorf("k8s: KeepAlive: %w", err)
	}
	if _, _, err := a.liveSession(holder); err != nil {
		return err
	}
	name := memberName(a.group, memberOf(holder))
	unlock, err := a.lockLease(ctx, name)
	if err != nil {
		return err
	}
	defer unlock()
	s, ok, err := a.liveSession(holder)
	switch {
	case err != nil:
		return err
	case !ok:
		return a.openSession(ctx, holder, name, timing)
	}
	return a.renew(ctx, s, timing)
}

// openSession opens the session of holder on member Lease name (K8S-210). The caller holds the
// Lease's lock.
func (a *Adapter) openSession(ctx context.Context, holder, name string, timing core.Timing) error {
	c, err := a.holdCache(ctx)
	if err != nil {
		return err
	}
	sent := a.now()
	written, err := a.writeLease(ctx, "open", name, a.knownMember(name), func(cur *coordinationv1.Lease) (change, error) {
		return a.decideOpen(cur, holder, timing)
	})
	if err != nil {
		a.releaseCache(c)
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// The Lease names holder now: a session of another incarnation of the member in this
	// Adapter has been replaced, and ends at once.
	for _, p := range a.sessions {
		if p.name == name && p.holder != holder && !p.ended {
			p.cleared = true
			a.endSessionLocked(p)
		}
	}
	a.sessions[holder] = &session{holder: holder, name: name, uid: written.UID, lease: written, timing: timing, sent: sent, cache: c}
	a.signalFreshLocked()
	return nil
}

// decideOpen decides the write that opens the session of holder on its member Lease cur
// (K8S-210): create the Lease naming holder, with the member record when the Adapter has joined
// as holder's member; or name holder, as a new acquisition, on a Lease that names no holder or
// another; or renew a Lease that names holder already.
func (a *Adapter) decideOpen(cur *coordinationv1.Lease, holder string, timing core.Timing) (change, error) {
	now := a.microNow()
	seconds := leaseSeconds(timing)
	if cur == nil {
		var transitions int32
		var annotations map[string]string
		if record, ok := a.joinedRecord(memberOf(holder)); ok {
			annotations = map[string]string{annotationMember: record}
		}
		return change{lease: &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Labels: leaseLabels(a.group, kindMember, ""), Annotations: annotations},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &seconds,
				AcquireTime:          &now,
				RenewTime:            new(now),
				LeaseTransitions:     &transitions,
			},
		}}, nil
	}
	if err := checkOwned(cur, a.group, kindMember); err != nil {
		return change{}, err
	}
	l := cur.DeepCopy()
	rt := nextRenewTime(now, cur)
	l.Spec.RenewTime, l.Spec.LeaseDurationSeconds = &rt, &seconds
	if holderOf(cur) != holder {
		var transitions int32
		if cur.Spec.LeaseTransitions != nil {
			transitions = *cur.Spec.LeaseTransitions + 1
		}
		l.Spec.HolderIdentity, l.Spec.AcquireTime, l.Spec.LeaseTransitions = &holder, new(rt), &transitions
	}
	return change{lease: l}, nil
}

// renew renews active session s with timing (K8S-220): one update of the member Lease from the
// latest state the Adapter knows. The caller holds the Lease's lock.
//
// No write is sent once s has expired, however long the lock, a conflict or its GET took: such
// a write could renew a session that other Adapters may already have seen end. For the same
// reason a renewal whose response comes back at or after the expiry ends the session: it may
// have committed after another Adapter's confirming read found the session gone. A renewal that
// fails may still have been written, with timing's duration: when that is shorter than the
// session's, the session's expiry comes forward to match.
func (a *Adapter) renew(ctx context.Context, s session, timing core.Timing) error {
	sent := a.now()
	expired := false
	written, err := a.writeLease(ctx, "renew", s.name, a.latest(s.name, s.lease), func(cur *coordinationv1.Lease) (change, error) {
		if s.expired(a.now()) {
			expired = true
			return change{}, expiredError(s.holder)
		}
		return a.decideRenew(cur, s, timing)
	})
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.sessions[s.holder]
	switch {
	case errors.Is(err, tenure.ErrSessionLost):
		// An expired session's Lease may still name the holder, for CloseSession to clear.
		p.cleared = !expired
		a.endSessionLocked(p)
		return err
	case err != nil:
		if timing.LeaseDuration < p.timing.LeaseDuration {
			// The session's expiry comes forward, and with it the end of its part in freshness.
			p.timing = timing
			a.signalFreshLocked()
		}
		return err
	}
	if s.expired(a.now()) {
		// The write succeeded, but its response came back at or after the session's expiry: it
		// may have committed after another Adapter's confirming GET saw the session gone and
		// took a shard over, so the renewal cannot extend the session (K8S-220). Its Lease still
		// names the holder, for CloseSession to clear.
		p.lease = written
		a.endSessionLocked(p)
		return fmt.Errorf("k8s: session %s lost: renewal returned at or after its expiry: %w", s.holder, tenure.ErrSessionLost)
	}
	if timing.RenewDeadline != p.timing.RenewDeadline {
		// Freshness takes the largest RenewDeadline of the active sessions (cache.go).
		a.signalFreshLocked()
	}
	p.lease, p.timing, p.sent = written, timing, sent
	return nil
}

// decideRenew decides the renewal of session s on its member Lease cur (K8S-220): move renewTime
// on, or report the session lost when the Lease is gone or names another holder or none.
func (a *Adapter) decideRenew(cur *coordinationv1.Lease, s session, timing core.Timing) (change, error) {
	lost := func(reason string) (change, error) {
		return change{}, fmt.Errorf("k8s: session %s lost: %s: %w", s.holder, reason, tenure.ErrSessionLost)
	}
	if cur == nil || cur.UID != s.uid {
		return lost("lease deleted")
	}
	switch h := holderOf(cur); h {
	case "":
		return lost("holder cleared")
	case s.holder:
	default:
		return lost("replaced by " + h)
	}
	if err := checkOwned(cur, a.group, kindMember); err != nil {
		return change{}, err
	}
	l := cur.DeepCopy()
	rt := nextRenewTime(a.microNow(), cur)
	seconds := leaseSeconds(timing)
	l.Spec.RenewTime, l.Spec.LeaseDurationSeconds = &rt, &seconds
	return change{lease: l}, nil
}

// CloseSession ends the session of holder for good, also for a holder that this Adapter does
// not know or whose session ended already: later calls of KeepAlive for holder return an error
// matching tenure.ErrSessionLost. When the member Lease still names holder, CloseSession deletes
// it, or, when the Adapter has joined as the holder's member or the Lease carries a member
// record, only clears its holder, so the member stays in the group. It never returns
// tenure.ErrSessionLost.
//
// First of all, CloseSession clears the holder of every shard record that names holder, keeping
// its term, as Release at the record's term does, in name order, and then the holder of every
// slot record that names holder, as ReleaseSlot does, in slot order. It finds the records with
// one request that lists the group's Leases, goes on past records it fails to clear, and returns
// their errors joined with the error of ending the session, if any.
func (a *Adapter) CloseSession(ctx context.Context, holder string) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	if err := checkHolder(a.group, holder); err != nil {
		return fmt.Errorf("k8s: CloseSession: %w", err)
	}
	// Step 1: the holder's shard records, then its slot records (records.go, slots.go). Steps 2
	// and 3 follow whatever its outcome.
	released := a.releaseRecordsOf(ctx, holder)
	defer func() { err = errors.Join(released, err) }()
	name := memberName(a.group, memberOf(holder))
	unlock, err := a.lockLease(ctx, name)
	if err != nil {
		// Step 3 all the same: a write of the Lease in flight cannot undo it.
		a.mu.Lock()
		a.endSessionLocked(a.sessionLocked(holder, name))
		a.mu.Unlock()
		return err
	}
	defer unlock()

	a.mu.Lock()
	s := a.sessionLocked(holder, name)
	base, cleared := s.lease, s.cleared
	a.mu.Unlock()
	// The latest state known, read while the session still holds the informer.
	if base != nil {
		base = a.latest(name, base)
	} else {
		base = a.knownMember(name)
	}

	// Step 3 before step 2, under the Lease's lock: the holder has ended whatever step 2 meets.
	a.mu.Lock()
	a.endSessionLocked(s)
	a.mu.Unlock()
	if cleared {
		return nil
	}

	// Step 2: the member Lease stops naming holder.
	if base == nil {
		if base, err = a.getLease(ctx, name); err != nil {
			return fmt.Errorf("k8s: close %s: %w", name, err)
		}
	}
	written, err := a.writeLease(ctx, "close", name, base, func(cur *coordinationv1.Lease) (change, error) {
		return a.decideClose(cur, holder)
	})
	if err != nil {
		return err
	}
	a.mu.Lock()
	s.cleared = true
	a.noteMemberWriteLocked(name, written)
	a.mu.Unlock()
	return nil
}

// sessionLocked returns the row of holder, whose member Lease is name, in the session table,
// adding a row with no session when there is none. The caller holds a.mu.
func (a *Adapter) sessionLocked(holder, name string) *session {
	s := a.sessions[holder]
	if s == nil {
		s = &session{holder: holder, name: name}
		a.sessions[holder] = s
	}
	return s
}

// decideClose decides step 2 of closing the session of holder on its member Lease cur (K8S-240):
// nothing when the Lease does not name holder; else clear its holder when the Adapter has joined
// as holder's member or the Lease carries a member record, which stays; else delete it.
func (a *Adapter) decideClose(cur *coordinationv1.Lease, holder string) (change, error) {
	if cur == nil || holderOf(cur) != holder {
		return change{}, nil
	}
	if err := checkOwned(cur, a.group, kindMember); err != nil {
		return change{}, err
	}
	_, joined := a.joinedRecord(memberOf(holder))
	if _, ok := cur.Annotations[annotationMember]; !ok && !joined {
		return change{del: true}, nil
	}
	l := cur.DeepCopy()
	l.Spec.HolderIdentity = nil
	return change{lease: l}, nil
}

// latest returns the newer of own, the member Lease name as a session of this Adapter last wrote
// it, and the latest state the Adapter knows of that Lease (knownMember), when that is the same
// Lease (same UID) at a later resourceVersion.
func (a *Adapter) latest(name string, own *coordinationv1.Lease) *coordinationv1.Lease {
	if l := a.knownMember(name); l != nil && l.UID == own.UID && isOlder(own.ResourceVersion, l.ResourceVersion) {
		return l
	}
	return own
}

// microNow returns the Adapter's wall time to the microsecond, as a Lease stores it.
func (a *Adapter) microNow() metav1.MicroTime {
	return metav1.NewMicroTime(a.now().UTC().Truncate(time.Microsecond))
}

// nextRenewTime returns the renewTime of a write on base at now: now, or 1µs past base's
// renewTime when now is not past it, so every renewal changes the Lease's session token.
func nextRenewTime(now metav1.MicroTime, base *coordinationv1.Lease) metav1.MicroTime {
	if rt := base.Spec.RenewTime; rt != nil && !now.After(rt.Time) {
		return metav1.NewMicroTime(rt.UTC().Add(time.Microsecond))
	}
	return now
}

// leaseSeconds returns the leaseDurationSeconds of timing, at most the largest an int32 holds.
func leaseSeconds(timing core.Timing) int32 {
	return int32(min(timing.LeaseDuration/time.Second, math.MaxInt32)) //nolint:gosec // G115: at most MaxInt32
}
