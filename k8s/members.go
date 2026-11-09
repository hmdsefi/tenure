// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"
	"unicode/utf8"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Membership (K8S-210, K8S-230, K8S-250, K8S-600 to K8S-650).
//
// A member that joined has its member record in the annotation tenure.member of its member
// Lease <group>-member-<id>. While it has joined, the Adapter holds the informer and runs the
// heartbeat: every RetryPeriod of the Adapter's Timing it moves the Lease's renewTime on when the
// Lease names no holder; a session that holds the Lease keeps it alive with its renewals
// instead. Join, SetDraining and Leave run one at a time (lockMembership), and every write of
// the member Lease, theirs, the heartbeat's and the sessions', holds the Lease's lock.

// memberState is the membership state of an Adapter. The Adapter's mu guards its fields.
type memberState struct {
	// ops is the lock of Join, SetDraining and Leave, made on first use.
	ops chan struct{}
	// joined is set from a successful Join until Leave.
	joined bool
	// left is set by Leave, for the Adapter's life.
	left bool
	// self is the member that joined, with its own copy of Meta and its current Draining.
	self core.Member
	// name is the member Lease of self.
	name string
	// lease is the member Lease as the Adapter last wrote or read it while joined; nil when
	// the Lease is known to be gone.
	lease *coordinationv1.Lease
	// cache is the informer hold of the joined member.
	cache *leaseCache
	// stop ends the heartbeat; done is closed once it has ended.
	stop context.CancelFunc
	done chan struct{}
}

// lockMembership takes the lock of Join, SetDraining and Leave, waiting for it until ctx ends.
func (a *Adapter) lockMembership(ctx context.Context) (unlock func(), err error) {
	a.mu.Lock()
	if a.member.ops == nil {
		a.member.ops = make(chan struct{}, 1)
	}
	ops := a.member.ops
	a.mu.Unlock()
	select {
	case ops <- struct{}{}:
		return func() { <-ops }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// checkMember returns an error wrapping core.ErrInvalidMember when the Adapter cannot keep m:
// m is not valid, its ID breaks the member rule, a time cannot be written in RFC 3339, or a
// string is not UTF-8, which JSON cannot carry unchanged.
func checkMember(group string, m core.Member) error {
	if err := m.Validate(); err != nil {
		return err
	}
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w %q: %s", core.ErrInvalidMember, m.ID, fmt.Sprintf(format, args...))
	}
	if d := memberDetail(group, m.ID); d != "" {
		return invalid("ID %s", d)
	}
	for _, f := range []struct {
		name string
		t    time.Time
	}{{"JoinAt", m.JoinAt}, {"LeaveAt", m.LeaveAt}} {
		if y := f.t.UTC().Year(); !f.t.IsZero() && (y < 0 || y > 9999) {
			return invalid("%s year %d is outside [0, 9999]", f.name, y)
		}
	}
	if !utf8.ValidString(m.Addr) {
		return invalid("Addr is not valid UTF-8")
	}
	for k, v := range m.Meta {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return invalid("Meta[%q] is not valid UTF-8", k)
		}
	}
	return nil
}

// Join adds self to the group: it writes self's member record into self's member Lease,
// creating the Lease, with no holder, when there is none. Until Leave, the Adapter then watches
// the group's Leases and keeps the member Lease alive every RetryPeriod of its Timing while no
// session holds it. Join returns, without a request, tenure.ErrClosed after Leave; an error
// wrapping core.ErrInvalidMember when self is not valid, its ID is not a member ID the Adapter
// can hold (see CheckNames), a time of self is outside the years 0 to 9999, or a string of self
// is not UTF-8; and an error when the Adapter has joined already. The Adapter keeps its own copy
// of self.Meta.
func (a *Adapter) Join(ctx context.Context, self core.Member) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	unlockOps, err := a.lockMembership(ctx)
	if err != nil {
		return err
	}
	defer unlockOps()
	a.mu.Lock()
	left, joined := a.member.left, a.member.joined
	a.mu.Unlock()
	if left {
		return tenure.ErrClosed
	}
	if err := checkMember(a.group, self); err != nil {
		return fmt.Errorf("k8s: Join: %w", err)
	}
	if joined {
		return errors.New("k8s: already joined")
	}
	self.Meta = maps.Clone(self.Meta)
	name := memberName(a.group, self.ID)
	record := encodeMember(self)

	c, err := a.holdCache(ctx)
	if err != nil {
		return err
	}
	written, err := a.writeMember(ctx, "join", name, record)
	if err != nil {
		a.releaseCache(c)
		return err
	}
	hctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	a.mu.Lock()
	a.member.joined, a.member.self, a.member.name, a.member.lease = true, self, name, written
	a.member.cache, a.member.stop, a.member.done = c, stop, done
	a.signalFreshLocked()
	a.runs.Go(func() {
		defer close(done)
		a.heartbeat(hctx, name)
	})
	a.mu.Unlock()
	// The Adapter's own views show the record at once.
	a.noteRead(written)
	a.viewsChanged()
	return nil
}

// SetDraining sets Draining of the member that joined, rewriting its member record; setting the
// value it has already makes no request. It returns, without a request, tenure.ErrClosed after
// Leave and an error before Join.
func (a *Adapter) SetDraining(ctx context.Context, draining bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	unlockOps, err := a.lockMembership(ctx)
	if err != nil {
		return err
	}
	defer unlockOps()
	a.mu.Lock()
	m := a.member
	a.mu.Unlock()
	switch {
	case m.left:
		return tenure.ErrClosed
	case !m.joined:
		return errors.New("k8s: not joined")
	case m.self.Draining == draining:
		return nil
	}
	self := m.self
	self.Draining = draining
	written, err := a.writeMember(ctx, "drain", m.name, encodeMember(self))
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.member.self, a.member.lease = self, written
	a.mu.Unlock()
	a.noteRead(written)
	a.viewsChanged()
	return nil
}

// Leave removes the member that joined from the group: it stops keeping the member Lease alive,
// and deletes the Lease, with its UID and resourceVersion as preconditions, or, while a session
// holds it, removes only the member record. Every channel Watch returned then receives a view
// without the member and is closed, and Join, SetDraining and Watch return tenure.ErrClosed.
// Leave ends the membership also when its write fails, and returns that write's error; the
// member Lease then expires. Leave before Join or again returns nil, with no request; it closes
// the channels all the same.
func (a *Adapter) Leave(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.err != nil {
		return a.err
	}
	unlockOps, err := a.lockMembership(ctx)
	if err != nil {
		return err
	}
	defer unlockOps()
	a.mu.Lock()
	m := a.member
	if !m.joined {
		a.member.left = true
		a.mu.Unlock()
		a.closeViews("")
		return nil
	}
	a.mu.Unlock()

	// No heartbeat may write after the Lease is gone.
	m.stop()
	<-m.done
	unlock, err := a.lockLease(ctx, m.name)
	if err == nil {
		_, err = a.writeLease(ctx, "leave", m.name, a.knownMember(m.name), a.decideLeave)
		unlock()
	}

	// The member's part in the group ends here, whatever the write did: its Watch channels get
	// a last view without it and close.
	a.mu.Lock()
	a.member.joined, a.member.left, a.member.lease = false, true, nil
	a.member.cache, a.member.stop, a.member.done = nil, nil, nil
	a.signalFreshLocked()
	a.releaseCacheLocked(m.cache)
	a.mu.Unlock()
	a.closeViews(m.self.ID)
	return err
}

// decideLeave decides the write of Leave on the member Lease cur (K8S-650): delete it when it
// names no holder; else remove the member record and leave the session its Lease.
func (a *Adapter) decideLeave(cur *coordinationv1.Lease) (change, error) {
	if cur == nil {
		return change{}, nil
	}
	if err := checkOwned(cur, a.group, kindMember); err != nil {
		return change{}, err
	}
	if holderOf(cur) == "" {
		return change{del: true}, nil
	}
	if _, ok := cur.Annotations[annotationMember]; !ok {
		return change{}, nil
	}
	l := cur.DeepCopy()
	delete(l.Annotations, annotationMember)
	return change{lease: l}, nil
}

// writeMember writes the member record record into member Lease name for op, Join or
// SetDraining, under the Lease's lock, and returns the Lease written.
func (a *Adapter) writeMember(ctx context.Context, op, name, record string) (*coordinationv1.Lease, error) {
	unlock, err := a.lockLease(ctx, name)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return a.writeLease(ctx, op, name, a.knownMember(name), func(cur *coordinationv1.Lease) (change, error) {
		return a.decideRecord(cur, record)
	})
}

// decideRecord decides the write of member record record on the member Lease cur (K8S-600,
// K8S-640): create the Lease with no holder when there is none, else change only its
// tenure.member annotation.
func (a *Adapter) decideRecord(cur *coordinationv1.Lease, record string) (change, error) {
	if cur == nil {
		return change{lease: a.newMemberLease(record)}, nil
	}
	if err := checkOwned(cur, a.group, kindMember); err != nil {
		return change{}, err
	}
	l := cur.DeepCopy()
	if l.Annotations == nil {
		l.Annotations = map[string]string{}
	}
	l.Annotations[annotationMember] = record
	return change{lease: l}, nil
}

// newMemberLease returns the member Lease of a joined member that holds no session: its labels,
// member record and renewTime, the Adapter's LeaseDuration, and no holder.
func (a *Adapter) newMemberLease(record string) *coordinationv1.Lease {
	seconds := leaseSeconds(a.timing)
	var transitions int32
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      leaseLabels(a.group, kindMember, ""),
			Annotations: map[string]string{annotationMember: record},
		},
		Spec: coordinationv1.LeaseSpec{
			LeaseDurationSeconds: &seconds,
			RenewTime:            new(a.microNow()),
			LeaseTransitions:     &transitions,
		},
	}
}

// heartbeat keeps the member Lease name alive until ctx ends (K8S-250): every RetryPeriod of
// the Adapter's Timing, one beat.
func (a *Adapter) heartbeat(ctx context.Context, name string) {
	tick := time.NewTicker(a.timing.RetryPeriod)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		a.beat(ctx, name)
	}
}

// beat is one heartbeat of member Lease name: when the Lease names no holder, an update of its
// renewTime alone; when it is gone, its re-creation. A conflict leaves the state read back for
// the next beat; other errors are logged at Warn.
func (a *Adapter) beat(ctx context.Context, name string) {
	unlock, err := a.lockLease(ctx, name)
	if err != nil {
		return
	}
	defer unlock()
	a.mu.Lock()
	record := encodeMember(a.member.self)
	a.mu.Unlock()
	// The first decision is on the latest state known. After a conflict writeLease decides
	// again on the state it read: the next beat writes from that, but a Lease found gone is
	// re-created at once.
	var read *coordinationv1.Lease
	first := true
	written, err := a.writeLease(ctx, "heartbeat", name, a.knownMember(name), func(cur *coordinationv1.Lease) (change, error) {
		again := !first
		first = false
		switch {
		case cur == nil:
			return change{lease: a.newMemberLease(record)}, nil
		case again:
			read = cur
			return change{}, nil
		}
		if err := checkOwned(cur, a.group, kindMember); err != nil {
			return change{}, err
		}
		if holderOf(cur) != "" {
			return change{}, nil
		}
		l := cur.DeepCopy()
		l.Spec.RenewTime = new(nextRenewTime(a.microNow(), cur))
		return change{lease: l}, nil
	})
	if err != nil {
		if ctx.Err() == nil {
			a.logger.Warn("k8s: heartbeat failed", "lease", name, "err", err)
		}
		return
	}
	if written == nil {
		written = read
	}
	if written != nil {
		a.mu.Lock()
		a.member.lease = written
		a.mu.Unlock()
	}
}

// joinedRecord returns the member record of the member id when the Adapter has joined as id.
func (a *Adapter) joinedRecord(id string) (record string, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.member.joined || a.member.self.ID != id {
		return "", false
	}
	return encodeMember(a.member.self), true
}

// noteMemberWriteLocked records l, written to member Lease name by a session, as the joined member's
// latest state of its Lease when it is newer. The caller holds a.mu.
func (a *Adapter) noteMemberWriteLocked(name string, l *coordinationv1.Lease) {
	m := &a.member
	if l != nil && m.joined && m.name == name && (m.lease == nil || isOlder(m.lease.ResourceVersion, l.ResourceVersion)) {
		m.lease = l
	}
}

// knownMember returns the latest state the Adapter knows of member Lease name, nil when it knows
// none: of its own last write while joined, the last writes of its active sessions on that
// Lease, and the informer's state, the one with the highest resourceVersion.
func (a *Adapter) knownMember(name string) *coordinationv1.Lease {
	var best *coordinationv1.Lease
	consider := func(l *coordinationv1.Lease) {
		if l != nil && (best == nil || isOlder(best.ResourceVersion, l.ResourceVersion)) {
			best = l
		}
	}
	a.mu.Lock()
	if a.member.joined && a.member.name == name {
		consider(a.member.lease)
	}
	for _, s := range a.sessions {
		if s.name == name && !s.ended {
			consider(s.lease)
		}
	}
	a.mu.Unlock()
	if o, ok := a.cached(name); ok {
		consider(o.lease)
	}
	return best
}
