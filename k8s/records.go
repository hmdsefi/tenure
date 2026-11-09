// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Shard records (K8S-300 to K8S-330, K8S-710).
//
// The record of shard n of space s is the Lease <group>-<s>-<n>: holderIdentity names its
// holder, tenure.term its term and tenure.session the UID of the holder's member Lease when the
// holder's session opened. Acquire reads the record from the cache, or from the API server when
// the cache has not seen it, decides on that state, and writes it by compare-and-swap with
// writeLease, deciding again on each conflict. A record that names another holder is taken
// only when liveness, confirmed against the API server, says that holder is gone (holderLive).
//
// The terms of deleted shard Leases that the informer saw are kept for the Adapter's life
// (shardTerms, K8S-110), so a record created anew never goes back to an earlier term.
//
// Record requests run under the record semaphore (K8S-710): at most recordConcurrency
// operations on shard and slot records are in flight, and further callers wait in arrival
// order. Session renewals never wait for it.

// Acquire grants the shard to holder and returns the new term when the shard's Lease is absent,
// names no holder, names holder itself, or names a holder whose session this Adapter has
// observed to be gone; it confirms that against the API server just before it writes. The term
// is core.NextTerm of the largest of the record's term and the last term of any earlier Lease of
// the shard that this Adapter saw deleted, and the current wall time. Otherwise Acquire returns
// 0 and a *tenure.HeldError naming the record's holder, with the RetryPeriod of holder's session
// Timing as RetryAfter. Acquire needs an active session of holder in this Adapter, opened by
// KeepAlive; otherwise it returns an error matching tenure.ErrSessionLost without a request. A
// malformed term reads as 0 once its holder is gone. Acquire never writes a Lease that lacks the
// group's shard labels. It runs at most as many record operations at once as
// WithRecordConcurrency allows.
func (a *Adapter) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if a.err != nil {
		return 0, a.err
	}
	name, err := a.checkRecordArgs("Acquire", holder, space, shard)
	if err != nil {
		return 0, err
	}
	s, err := a.activeSession(holder)
	if err != nil {
		return 0, err
	}
	if err := a.records.acquire(ctx); err != nil {
		return 0, err
	}
	defer a.records.release()

	cur, err := a.readRecord(ctx, "acquire", name)
	if err != nil {
		return 0, err
	}
	var term core.Term
	last := cur
	written, err := a.writeLease(ctx, "acquire", name, cur, func(cur *coordinationv1.Lease) (change, error) {
		if cur == nil && last != nil {
			// The record decided on before is gone, deleted before the informer told: its term
			// is the term of a deleted shard Lease all the same.
			if t, ok := decodeTerm(last.Annotations); ok && t > 0 {
				a.terms.note(name, t)
			}
		}
		last = cur
		var ch change
		var err error
		ch, term, err = a.decideAcquire(ctx, s, space, name, cur)
		return ch, err
	})
	if err != nil {
		return 0, err
	}
	a.noteRead(written)
	return term, nil
}

// checkRecordArgs checks the arguments holder, space and shard of method (K8S-030) and returns
// the shard's Lease name. Its errors read "k8s: <method>: <detail>". A shard Lease name always
// fits 253 characters: the group, the space and a decimal int are short enough.
func (a *Adapter) checkRecordArgs(method, holder, space string, shard int) (name string, err error) {
	if err := checkHolder(a.group, holder); err != nil {
		return "", fmt.Errorf("k8s: %s: %w", method, err)
	}
	if d := spaceDetail(space); d != "" {
		return "", fmt.Errorf("k8s: %s: space %q: %s", method, space, d)
	}
	if shard < 0 {
		return "", fmt.Errorf("k8s: %s: shard %d: must not be negative", method, shard)
	}
	return shardName(a.group, space, shard), nil
}

// activeSession returns a copy of the active session of holder from the session table, without
// a request and without looking at the cache. A holder with no session here, or whose session
// ended or expired, gets an error matching tenure.ErrSessionLost.
func (a *Adapter) activeSession(holder string) (session, error) {
	s, ok, err := a.liveSession(holder)
	if err == nil && !ok {
		err = fmt.Errorf("k8s: session %s is not open: %w", holder, tenure.ErrSessionLost)
	}
	return s, err
}

// readRecord returns record Lease name as the cache has it, or else as the API server has it,
// nil when there is none. A record read from the API server is noted in the cache.
func (a *Adapter) readRecord(ctx context.Context, op, name string) (*coordinationv1.Lease, error) {
	if o, ok := a.cached(name); ok {
		return o.lease, nil
	}
	l, err := a.getLease(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("k8s: %s %s: %w", op, name, err)
	}
	a.noteRead(l)
	return l, nil
}

// decideAcquire decides Acquire's write on cur, the state of shard record name of space, nil
// when there is none, for session s (K8S-310): a grant when cur is absent, has no holder, names
// s's holder, or names a holder that is gone; else a *tenure.HeldError. It returns the term of
// the grant.
func (a *Adapter) decideAcquire(ctx context.Context, s session, space, name string, cur *coordinationv1.Lease) (change, core.Term, error) {
	if cur != nil {
		if err := checkOwned(cur, a.group, kindShard); err != nil {
			return change{}, 0, err
		}
		if h := holderOf(cur); h != "" && h != s.holder {
			live, err := a.holderLive(ctx, "acquire", cur)
			if err != nil {
				return change{}, 0, err
			}
			if live {
				return change{}, 0, &tenure.HeldError{Holder: h, RetryAfter: s.timing.RetryPeriod}
			}
		}
	}
	return a.grant(s, space, name, cur)
}

// grant returns the write that grants cur, the state of shard record name of space, nil when
// there is none, to session s, and its term (K8S-320): the shard labels; holderIdentity; a new
// term; tenure.session the UID of s's member Lease; no tenure.confirm; acquireTime and renewTime
// now; the leaseDurationSeconds of s's Timing; leaseTransitions one more when the holder
// changes. Other labels, annotations and fields stay as they are. A malformed term counts as 0.
func (a *Adapter) grant(s session, space, name string, cur *coordinationv1.Lease) (change, core.Term, error) {
	l := &coordinationv1.Lease{}
	var prev core.Term
	var transitions int32
	if cur != nil {
		l = cur.DeepCopy()
		prev, _ = decodeTerm(cur.Annotations)
		if t := cur.Spec.LeaseTransitions; t != nil {
			transitions = *t
		}
		if holderOf(cur) != s.holder && transitions < math.MaxInt32 {
			transitions++
		}
	}
	prev = max(prev, a.terms.of(name))
	if prev == math.MaxUint64 {
		return change{}, 0, fmt.Errorf("k8s: lease %s: term %d has no successor", name, prev)
	}
	now := a.now()
	term := core.NextTerm(prev, now)
	at := metav1.NewMicroTime(now.UTC().Truncate(time.Microsecond))
	holder, seconds := s.holder, leaseSeconds(s.timing)

	if l.Labels == nil {
		l.Labels = map[string]string{}
	}
	maps.Copy(l.Labels, leaseLabels(a.group, kindShard, space))
	if l.Annotations == nil {
		l.Annotations = map[string]string{}
	}
	l.Annotations[annotationTerm] = encodeTerm(term)
	l.Annotations[annotationSession] = string(s.uid)
	delete(l.Annotations, annotationConfirm)
	l.Spec.HolderIdentity = &holder
	l.Spec.AcquireTime, l.Spec.RenewTime = &at, new(at)
	l.Spec.LeaseDurationSeconds = &seconds
	l.Spec.LeaseTransitions = &transitions
	return change{lease: l}, term, nil
}

// holderLive reports whether the holder of record cur, the state a write is about to replace,
// is live (K8S-120). When liveness says it is gone but asks for confirmation, holderLive reads
// the holder's member Lease from the API server, notes it in the cache, and decides again on
// that state (K8S-130): a changed session token makes the holder live again. A gone verdict
// that needs no confirmation (no holder, or a session this Adapter saw end) costs no request,
// nor does a holder that is not <member ID>/<incarnation>, which has no member Lease to read.
// The decision on the GET's state is final: that state is newer than cur. op names the
// operation in errors: "k8s: <op> <member Lease>: <error>".
func (a *Adapter) holderLive(ctx context.Context, op string, cur *coordinationv1.Lease) (bool, error) {
	h := holderOf(cur)
	rec := a.observed(cur)
	var mname string
	var member *observation
	if h != "" && checkHolder(a.group, h) == nil {
		mname = memberName(a.group, memberOf(h))
		if o, ok := a.cached(mname); ok && isMemberLease(o.lease, a.group) {
			member = &o
		}
	}
	s, hasSession := decodeSession(cur.Annotations)
	var tomb *tombstone
	if hasSession {
		if t, ok := a.tombstoneOf(s); ok {
			tomb = &t
		}
	}
	v := liveness(rec, member, tomb, a.now(), a.timing.LeaseDuration)
	if v.live || !v.get || mname == "" {
		return v.live, nil
	}

	fresh, err := a.getLease(ctx, mname)
	if err != nil {
		return false, fmt.Errorf("k8s: %s %s: %w", op, mname, err)
	}
	var next *observation
	if fresh != nil && isMemberLease(fresh, a.group) {
		o := a.observed(fresh)
		next = &o
	}
	// The member Lease of the record's session is gone, and the informer has not told yet. Its
	// renewals since the cached state may have been missed too, so the session counts as seen
	// now, until the informer delivers the deletion and leaves the tombstone of what it saw.
	now := a.now()
	if hasSession && tomb == nil && member != nil && member.lease.UID == s && (next == nil || next.lease.UID != s) {
		tomb = &tombstone{tokenSeenAt: now, wait: leaseWait(member.lease, a.timing.LeaseDuration)}
	}
	return liveness(rec, next, tomb, now, a.timing.LeaseDuration).live, nil
}

// isMemberLease reports whether l carries the labels of a member Lease of group.
func isMemberLease(l *coordinationv1.Lease, group string) bool {
	return l.Labels[labelGroup] == group && l.Labels[labelKind] == kindMember
}

// observed returns state l of a Lease of the group as this Adapter has observed it: noted in the
// cache, with the moments the cache first saw its resourceVersion and its session token; both
// now when the cache does not run.
func (a *Adapter) observed(l *coordinationv1.Lease) observation {
	a.noteRead(l)
	if o, ok := a.cached(l.Name); ok && o.lease.UID == l.UID && o.lease.ResourceVersion == l.ResourceVersion {
		return observation{lease: l, rvSeenAt: o.rvSeenAt, tokenSeenAt: o.tokenSeenAt, named: o.named}
	}
	now := a.now()
	return observation{lease: l, rvSeenAt: now, tokenSeenAt: now, named: holderOf(l)}
}

// noteRead notes l, a state of a Lease of the group that this Adapter read from the API server
// or wrote, in the running cache, as if the informer had delivered it; a state older than the
// cache's is ignored. nil and Leases without the group's label are not noted. Only the
// informer's own handlers see what the informer delivers.
func (a *Adapter) noteRead(l *coordinationv1.Lease) {
	if l == nil || l.Labels[labelGroup] != a.group {
		return
	}
	if c := a.runningCache(); c != nil {
		c.observe(l)
	}
}

// shardTerms keeps the last term of every deleted shard Lease the informer saw, by Lease name,
// for the Adapter's life (K8S-110): runs of the informer come and go, the terms stay. The zero
// value is ready to use.
type shardTerms struct {
	mu    sync.Mutex
	terms map[string]core.Term
}

// note records term as the last term of deleted shard Lease name, unless a larger one is
// recorded.
func (t *shardTerms) note(name string, term core.Term) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terms == nil {
		t.terms = map[string]core.Term{}
	}
	t.terms[name] = max(t.terms[name], term)
}

// of returns the largest last term of the deleted shard Leases name, 0 when none was seen.
func (t *shardTerms) of(name string) core.Term {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.terms[name]
}

// semaphore admits at most n callers at once (K8S-710); the others wait in arrival order.
type semaphore struct {
	mu      sync.Mutex
	free    int
	held    int
	waiters []chan struct{}
}

// newSemaphore returns a semaphore of n places.
func newSemaphore(n int) *semaphore { return &semaphore{free: n} }

// acquire takes a place, waiting behind every earlier caller until one is free. When ctx ends
// first, it returns ctx.Err() and holds nothing.
func (s *semaphore) acquire(ctx context.Context) error {
	s.mu.Lock()
	if s.free > 0 && len(s.waiters) == 0 {
		s.free--
		s.held++
		s.mu.Unlock()
		return nil
	}
	ready := make(chan struct{})
	s.waiters = append(s.waiters, ready)
	s.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		if i := slices.Index(s.waiters, ready); i >= 0 {
			s.waiters = slices.Delete(s.waiters, i, i+1)
			s.mu.Unlock()
			return ctx.Err()
		}
		s.mu.Unlock()
		// The place came as ctx ended: pass it on.
		s.release()
		return ctx.Err()
	}
}

// release gives a place back, to the earliest waiter when there is one.
func (s *semaphore) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiters) > 0 {
		close(s.waiters[0])
		s.waiters = s.waiters[1:]
		return
	}
	s.held--
	s.free++
}

// load returns how many callers hold a place and how many wait for one.
func (s *semaphore) load() (holding, waiting int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held, len(s.waiters)
}
