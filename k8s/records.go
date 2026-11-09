// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Shard records (K8S-300 to K8S-370, K8S-710).
//
// The record of shard n of space s is the Lease <group>-<s>-<n>: holderIdentity names its
// holder, tenure.term its term and tenure.session the UID of the holder's member Lease when the
// holder's session opened. Acquire reads the record from the cache, or from the API server when
// the cache has not seen it, decides on that state, and writes it by compare-and-swap with
// writeLease, deciding again on each conflict. A record that names another holder is taken
// only when liveness, confirmed against the API server, says that holder is gone (holderLive).
//
// Release and Confirm write a record only when it names the caller at the caller's term. They
// decide on the cache's state when it shows that, and otherwise read the record from the API
// server first (recheck), since the cache may be behind. Confirm's write changes only the
// tenure.confirm counter: a grant decided on an earlier state then fails its compare-and-swap
// and decides again. Holdings lists the records of a space as stored, with no liveness. The
// only writers of shard Leases are Acquire, Release, Confirm and CloseSession (K8S-360).
//
// The terms of deleted shard Leases that the informer saw are kept for the Adapter's life
// (shardTerms, K8S-110), so a record created anew never goes back to an earlier term.
//
// Record requests run under the record semaphore (K8S-710): at most recordConcurrency
// operations on shard and slot records are in flight, and further callers wait in arrival
// order. Session renewals never wait for it.

var _ tenure.Store = (*Adapter)(nil)

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
	// The session may have ended or expired while Acquire waited for its place.
	if s, err = a.activeSession(holder); err != nil {
		return 0, err
	}

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

// Release clears the holder of the shard's record when the record names holder at term: its
// holderIdentity, tenure.session and tenure.confirm go, and its term, acquireTime and
// leaseTransitions stay. Release reads the record from the cache, and from the API server when
// the cache has not seen it or shows another holder or term. When there is no record, or it
// names another holder or term, Release writes nothing and returns nil. It does not look at
// holder's session. It never writes a Lease that lacks the group's shard labels, and returns an
// error for one.
func (a *Adapter) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	name, err := a.checkTermArgs("Release", holder, space, shard, term)
	if err != nil {
		return err
	}
	if err := a.records.acquire(ctx); err != nil {
		return err
	}
	defer a.records.release()
	return a.releaseRecord(ctx, name, nil, holder, encodeTerm(term))
}

// checkTermArgs is checkRecordArgs, followed by the check of term (K8S-030).
func (a *Adapter) checkTermArgs(method, holder, space string, shard int, term core.Term) (name string, err error) {
	name, err = a.checkRecordArgs(method, holder, space, shard)
	if err == nil && term == 0 {
		err = fmt.Errorf("k8s: %s: term 0: must not be 0", method)
	}
	return name, err
}

// releaseRecord clears the holder of shard record name when the record names holder at term, its
// tenure.term annotation as written (K8S-340). base is the state of the record the caller knows,
// nil to take the cache's; when there is none, or it does not name holder at term, the record is
// read from the API server first. The caller holds a place of the record semaphore.
func (a *Adapter) releaseRecord(ctx context.Context, name string, base *coordinationv1.Lease, holder, term string) error {
	decide := func(cur *coordinationv1.Lease) (change, error) { return decideRelease(cur, a.group, holder, term) }
	base, err := a.recheck(ctx, "release", name, base, decide)
	if err != nil {
		return err
	}
	written, err := a.writeLease(ctx, "release", name, base, decide)
	a.noteRead(written)
	return err
}

// decideRelease decides Release's write on cur, the state of a shard record of group, nil when
// there is none (K8S-340): when it names holder at term, clear holderIdentity, tenure.session and
// tenure.confirm, keeping the rest; else nothing.
func decideRelease(cur *coordinationv1.Lease, group, holder, term string) (change, error) {
	if cur == nil {
		return change{}, nil
	}
	if err := checkOwned(cur, group, kindShard); err != nil {
		return change{}, err
	}
	if !namesAt(cur, holder, term) {
		return change{}, nil
	}
	l := cur.DeepCopy()
	l.Spec.HolderIdentity = nil
	delete(l.Annotations, annotationSession)
	delete(l.Annotations, annotationConfirm)
	return change{lease: l}, nil
}

// namesAt reports whether record l names holder at term, its tenure.term annotation as written,
// "" for none.
func namesAt(l *coordinationv1.Lease, holder, term string) bool {
	return holderOf(l) == holder && l.Annotations[annotationTerm] == term
}

// recheck returns the state of record name that a write decided by decide starts from: base, or
// the cache's state when base is nil, when decide writes on it; else the record as the API
// server has it, noted in the cache, nil when there is none. A cache that shows another holder or
// term may be behind, so only a write is decided on its state.
func (a *Adapter) recheck(ctx context.Context, op, name string, base *coordinationv1.Lease, decide decider) (*coordinationv1.Lease, error) {
	if base == nil {
		if o, ok := a.cached(name); ok {
			base = o.lease
		}
	}
	if base != nil {
		if ch, err := decide(base); err == nil && (ch.lease != nil || ch.del) {
			return base, nil
		}
	}
	l, err := a.getLease(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("k8s: %s %s: %w", op, name, err)
	}
	a.noteRead(l)
	return l, nil
}

// releaseRecordsOf releases every shard record that names holder, each at its own term, in name
// order (K8S-240, step 1). It finds them with one LIST of the group's Leases, goes on past
// failures, and returns their errors joined.
func (a *Adapter) releaseRecordsOf(ctx context.Context, holder string) error {
	list, err := a.leases().List(ctx, metav1.ListOptions{LabelSelector: labelGroup + "=" + a.group})
	if err != nil {
		return fmt.Errorf("k8s: close %s: list leases: %w", holder, err)
	}
	var shards []*coordinationv1.Lease
	for i := range list.Items {
		l := &list.Items[i]
		kind, _, _, _, ok := parseName(a.group, l.Name)
		if ok && kind == kindShard && checkOwned(l, a.group, kindShard) == nil && holderOf(l) == holder {
			shards = append(shards, l)
		}
	}
	slices.SortFunc(shards, func(x, y *coordinationv1.Lease) int { return strings.Compare(x.Name, y.Name) })
	var errs []error
	for _, l := range shards {
		errs = append(errs, a.releaseListed(ctx, l, holder))
	}
	return errors.Join(errs...)
}

// releaseListed releases shard record l, a listed state that names holder, at the term l shows.
func (a *Adapter) releaseListed(ctx context.Context, l *coordinationv1.Lease, holder string) error {
	if err := a.records.acquire(ctx); err != nil {
		return err
	}
	defer a.records.release()
	return a.releaseRecord(ctx, l.Name, l, holder, l.Annotations[annotationTerm])
}

// Confirm returns the shard's record, written so that no grant to another holder decided on an
// earlier state of the record can commit after it. When the record names holder at term, Confirm
// increments the record's counter in the annotation tenure.confirm, changing nothing else, and
// returns the record as written. Otherwise it returns the record as the API server has it, or
// tenure.Holding{Space: space, Shard: shard} when there is none. Confirm reads the record from
// the cache first, and from the API server when the cache shows another holder or term or has
// not seen the record. It needs an active session of holder in this Adapter, opened by
// KeepAlive; otherwise it returns an error matching tenure.ErrSessionLost without a request. It
// never writes a Lease that lacks the group's shard labels, and returns an error for one.
func (a *Adapter) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	if err := ctx.Err(); err != nil {
		return tenure.Holding{}, err
	}
	if a.err != nil {
		return tenure.Holding{}, a.err
	}
	name, err := a.checkTermArgs("Confirm", holder, space, shard, term)
	if err != nil {
		return tenure.Holding{}, err
	}
	if _, err := a.activeSession(holder); err != nil {
		return tenure.Holding{}, err
	}
	if err := a.records.acquire(ctx); err != nil {
		return tenure.Holding{}, err
	}
	defer a.records.release()
	// The session may have ended or expired while Confirm waited for its place.
	if _, err := a.activeSession(holder); err != nil {
		return tenure.Holding{}, err
	}

	// seen is the state decided on last: the record as stored when the decision is no write.
	var seen *coordinationv1.Lease
	decide := func(cur *coordinationv1.Lease) (change, error) {
		seen = cur
		return decideConfirm(cur, a.group, holder, encodeTerm(term))
	}
	base, err := a.recheck(ctx, "confirm", name, nil, decide)
	if err != nil {
		return tenure.Holding{}, err
	}
	written, err := a.writeLease(ctx, "confirm", name, base, decide)
	if err != nil {
		return tenure.Holding{}, err
	}
	a.noteRead(written)
	if written == nil {
		written = seen
	}
	l, v, err := a.version(ctx, "confirm", name, written)
	if err != nil {
		return tenure.Holding{}, err
	}
	if l == nil {
		return tenure.Holding{Space: space, Shard: shard}, nil
	}
	return holdingOf(space, shard, l, v), nil
}

// decideConfirm decides Confirm's write on cur, the state of a shard record of group, nil when
// there is none (K8S-370): when it names holder at term, add one to its tenure.confirm counter,
// a malformed one counting as 0; else nothing.
func decideConfirm(cur *coordinationv1.Lease, group, holder, term string) (change, error) {
	if cur == nil {
		return change{}, nil
	}
	if err := checkOwned(cur, group, kindShard); err != nil {
		return change{}, err
	}
	if !namesAt(cur, holder, term) {
		return change{}, nil
	}
	n, ok := decodeConfirm(cur.Annotations)
	if !ok || n == math.MaxUint64 {
		n = 0
	}
	l := cur.DeepCopy()
	l.Annotations[annotationConfirm] = encodeConfirm(n + 1)
	return change{lease: l}, nil
}

// Holdings returns the records of space, sorted by shard, from one LIST request to the API
// server; nil when there are none. A record's Holder is the holder it names, whether or not that
// holder's session is live, and a malformed term reads as 0. Leases without the group's shard
// labels for space, and Leases whose names are not names of records of space, are left out.
func (a *Adapter) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.err != nil {
		return nil, a.err
	}
	if d := spaceDetail(space); d != "" {
		return nil, fmt.Errorf("k8s: Holdings: space %q: %s", space, d)
	}
	selector := labelGroup + "=" + a.group + "," + labelKind + "=" + kindShard + "," + labelSpace + "=" + space
	list, err := a.leases().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("k8s: holdings %s: %w", space, err)
	}
	var out []tenure.Holding
	for i := range list.Items {
		l := &list.Items[i]
		kind, s, _, shard, ok := parseName(a.group, l.Name)
		if !ok || kind != kindShard || s != space || checkOwned(l, a.group, kindShard) != nil || l.Labels[labelSpace] != space {
			continue
		}
		l, v, err := a.version(ctx, "holdings", l.Name, l)
		if err != nil {
			return nil, err
		}
		if l != nil {
			out = append(out, holdingOf(space, shard, l, v))
		}
	}
	slices.SortFunc(out, func(x, y tenure.Holding) int { return x.Shard - y.Shard })
	return out, nil
}

// holdingOf returns l, the record of shard in space, at version v, as a tenure.Holding. A
// malformed term reads as 0.
func holdingOf(space string, shard int, l *coordinationv1.Lease, v uint64) tenure.Holding {
	term, _ := decodeTerm(l.Annotations)
	return tenure.Holding{Space: space, Shard: shard, Holder: holderOf(l), Term: term, Version: v}
}

// version returns l, a state of Lease name from a response of the API server, and its parsed
// resourceVersion (K8S-070). When that does not parse, it reads the Lease again and returns the
// state read, noted in the cache, and its version; nil when the Lease is gone; or the error when
// that version does not parse either. It returns nil for a nil l.
func (a *Adapter) version(ctx context.Context, op, name string, l *coordinationv1.Lease) (*coordinationv1.Lease, uint64, error) {
	if l == nil {
		return nil, 0, nil
	}
	if v, err := parseVersion(name, l.ResourceVersion); err == nil {
		return l, v, nil
	}
	l, err := a.getLease(ctx, name)
	if err != nil {
		return nil, 0, fmt.Errorf("k8s: %s %s: %w", op, name, err)
	}
	if l == nil {
		return nil, 0, nil
	}
	v, err := parseVersion(name, l.ResourceVersion)
	if err != nil {
		return nil, 0, err
	}
	a.noteRead(l)
	return l, v, nil
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
