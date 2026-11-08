// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// errLookupN is LookupN's error for n below 1.
var errLookupN = errors.New("tenure: LookupN: n must be at least 1")

// Resolver places keys on the members of a group, by rendezvous rank unless WithPlacement says
// otherwise, with no leases: any number of keys, each served by the members ranked first for it.
// It ranks the eligible members of the latest view, as core.Eligible gives them at the current
// wall-clock time: members that are not draining, inside their JoinAt and LeaveAt window, and not
// standbys. It builds one core.Placement for each new list of eligible members and keeps it until
// that list changes; when a member's JoinAt or LeaveAt comes, it updates the list at that instant.
//
// NewResolver returns a Resolver that follows a Membership itself; Node.Resolver returns one that
// follows the node's views. A Resolver's methods are safe for concurrent use. Lookup, LookupN and
// Members never wait for a view or for another call.
type Resolver struct {
	namespace string
	place     func([]Member) core.Placement

	state atomic.Pointer[resolverState] // what lookups read; replaced, never changed

	mu      sync.Mutex
	closed  bool
	view    core.View          // the latest view
	listed  bool               // a view has come: list is the list of a view
	list    []Member           // the current eligible members
	subs    []chan []Member    // every channel Subscribe returned, until Close
	timer   *time.Timer        // fires at timerAt, the earliest future JoinAt or LeaveAt
	timerAt time.Time          // zero when no timer is armed
	timers  sync.WaitGroup     // timer callbacks armed or running
	stop    context.CancelFunc // a standalone Resolver's watch context; nil for a node's
	done    chan struct{}      // closed when a standalone Resolver's watcher returns
	onClose func(*Resolver)    // a node's Resolver: removes it from the node
}

// resolverState is one immutable snapshot of a Resolver: its eligible members and their
// placement (nil when there are none), or closed.
type resolverState struct {
	members   []Member
	placement core.Placement
	closed    bool
}

// A ResolverOption changes how NewResolver builds a Resolver.
type ResolverOption func(*resolverOptions)

// resolverOptions holds what the ResolverOptions set.
type resolverOptions struct {
	namespace string
	place     func([]Member) core.Placement
}

// WithNamespace makes the Resolver rank keys in namespace, so that the same key in different
// namespaces lands on different members. The default is "".
func WithNamespace(namespace string) ResolverOption {
	return func(o *resolverOptions) { o.namespace = namespace }
}

// WithPlacement makes the Resolver build its placements with place instead of
// core.NewRendezvous. The Resolver calls place once for each new non-empty list of eligible
// members, in ID order, and asks the returned Placement, which must not be nil, for every lookup
// until the list changes. A nil place keeps the default.
func WithPlacement(place func(members []Member) core.Placement) ResolverOption {
	return func(o *resolverOptions) {
		if place != nil {
			o.place = place
		}
	}
}

// newResolver returns an open Resolver with no view.
func newResolver(namespace string, place func([]Member) core.Placement) *Resolver {
	r := &Resolver{namespace: namespace, place: place}
	r.state.Store(&resolverState{})
	return r
}

// NewResolver returns a Resolver that follows the views of m. It calls m.Watch once and returns
// its error, as "tenure: NewResolver: watch: " and the error, but never calls Join: m serves only
// to watch the group, and the Resolver ranks whatever members its views hold. NewResolver does not
// wait for the first view; until it comes, Lookup returns ErrNoMembers.
//
// When the channel of Watch closes, or a later Watch fails, the Resolver calls Watch again
// core.DefaultTiming.RetryPeriod later, until Close or until Watch returns ErrClosed; after
// ErrClosed it keeps its last list. Views that are not valid are ignored.
func NewResolver(m Membership, opts ...ResolverOption) (*Resolver, error) {
	if m == nil {
		return nil, errors.New("tenure: NewResolver: nil membership")
	}
	o := resolverOptions{place: core.NewRendezvous}
	for _, opt := range opts {
		opt(&o)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := m.Watch(ctx)
	switch {
	case err != nil:
		cancel()
		return nil, fmt.Errorf("tenure: NewResolver: watch: %w", err)
	case ch == nil:
		cancel()
		return nil, errors.New("tenure: NewResolver: watch: nil channel")
	}
	r := newResolver(o.namespace, o.place)
	r.stop, r.done = cancel, make(chan struct{})
	go r.watch(ctx, m, ch)
	return r, nil
}

// watch is a standalone Resolver's goroutine (RUN-550: it returns by Close). It follows ch, and
// after a closed channel or a failed Watch calls Watch again RetryPeriod later. It returns when
// ctx is done or when Watch returns ErrClosed.
func (r *Resolver) watch(ctx context.Context, m Membership, ch <-chan core.View) {
	defer close(r.done)
	for {
		if ch != nil && !r.follow(ctx, ch) {
			return
		}
		t := time.NewTimer(core.DefaultTiming.RetryPeriod)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return
		}
		var err error
		ch, err = m.Watch(ctx)
		if errors.Is(err, ErrClosed) || ctx.Err() != nil {
			return
		}
		if err != nil {
			ch = nil
		}
	}
}

// follow feeds each valid view of ch to r until ch is closed, and reports whether ctx is still
// not done.
func (r *Resolver) follow(ctx context.Context, ch <-chan core.View) bool {
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return ctx.Err() == nil
			}
			if v.Validate() == nil {
				r.update(v)
			}
		case <-ctx.Done():
			return false
		}
	}
}

// update makes v the Resolver's latest view.
func (r *Resolver) update(v core.View) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.view = v
	r.refreshLocked()
}

// refreshLocked recomputes the eligible members of the latest view at the current time. When the
// list differs from the current one, it builds the placement, publishes both and sends the list
// to every subscriber. It then arms the timer at the earliest JoinAt or LeaveAt after now. The
// caller holds r.mu.
func (r *Resolver) refreshLocked() {
	now := time.Now()
	list := core.Eligible(r.view, now)
	if !r.listed || !sameMembers(list, r.list) {
		r.listed, r.list = true, list
		s := &resolverState{members: list}
		if len(list) > 0 {
			s.placement = r.place(slices.Clone(list))
		}
		r.state.Store(s)
		for _, ch := range r.subs {
			sendList(ch, list)
		}
	}
	r.armLocked(nextBound(r.view, now))
}

// armLocked makes the timer fire at at, or stops it when at is zero. The caller holds r.mu.
func (r *Resolver) armLocked(at time.Time) {
	if at.Equal(r.timerAt) && !at.IsZero() {
		return
	}
	if r.timer != nil && r.timer.Stop() {
		r.timers.Done() // the callback will not run
	}
	r.timer, r.timerAt = nil, at
	if at.IsZero() {
		return
	}
	r.timers.Add(1)
	r.timer = time.AfterFunc(time.Until(at), func() {
		defer r.timers.Done()
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed {
			return
		}
		r.timer, r.timerAt = nil, time.Time{}
		r.refreshLocked()
	})
}

// nextBound returns the earliest JoinAt or LeaveAt of v's members that is after now, or the zero
// time when there is none.
func nextBound(v core.View, now time.Time) time.Time {
	var next time.Time
	for _, m := range v.Members {
		for _, t := range [2]time.Time{m.JoinAt, m.LeaveAt} {
			if t.After(now) && (next.IsZero() || t.Before(next)) {
				next = t
			}
		}
	}
	return next
}

// sameMembers reports whether a and b list the same members with the same data.
func sameMembers(a, b []Member) bool {
	return slices.EqualFunc(a, b, func(x, y Member) bool {
		return x.ID == y.ID && x.Addr == y.Addr && x.Draining == y.Draining && x.Slot == y.Slot &&
			x.JoinAt.Equal(y.JoinAt) && x.LeaveAt.Equal(y.LeaveAt) && maps.Equal(x.Meta, y.Meta)
	})
}

// sendList puts a copy of list in ch, which has buffer 1 and only senders that hold the
// Resolver's mu, replacing a list not yet received.
func sendList(ch chan []Member, list []Member) {
	select {
	case <-ch:
	default:
	}
	ch <- slices.Clone(list)
}

// load returns the current snapshot; the zero Resolver has none and serves no member.
func (r *Resolver) load() *resolverState {
	if s := r.state.Load(); s != nil {
		return s
	}
	return &resolverState{}
}

// Lookup returns the member ranked first for key. It returns ErrNoMembers when no member is
// eligible, and ErrClosed after Close.
func (r *Resolver) Lookup(key string) (Member, error) {
	s := r.load()
	if s.closed {
		return Member{}, ErrClosed
	}
	if s.placement == nil {
		return Member{}, ErrNoMembers
	}
	got := s.placement.Rank(r.namespace, key, 1)
	if len(got) == 0 {
		return Member{}, ErrNoMembers
	}
	return got[0], nil
}

// LookupN returns the first n members for key, best first, as a new slice: all the eligible
// members when fewer than n are. The first member is the one Lookup returns, and for a <= b,
// LookupN(key, a) is a prefix of LookupN(key, b). It returns ErrClosed after Close, an error
// when n is below 1, and ErrNoMembers when no member is eligible.
func (r *Resolver) LookupN(key string, n int) ([]Member, error) {
	s := r.load()
	switch {
	case s.closed:
		return nil, ErrClosed
	case n < 1:
		return nil, errLookupN
	case s.placement == nil:
		return nil, ErrNoMembers
	}
	got := s.placement.Rank(r.namespace, key, n)
	if len(got) == 0 {
		return nil, ErrNoMembers
	}
	return got, nil
}

// Members returns the eligible members in ID order, as a new slice; nil when none is eligible,
// before the first view and after Close.
func (r *Resolver) Members() []Member {
	return slices.Clone(r.load().members)
}

// Subscribe returns a channel that receives the list of eligible members, as Members returns
// it, each time the list changes. It holds the current list at once when a view has come. The
// channel holds one list, and a newer list replaces one not yet received, so a slow receiver
// gets the latest. Close closes the channel; after Close, Subscribe returns a closed channel.
func (r *Resolver) Subscribe() <-chan []Member {
	ch := make(chan []Member, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		close(ch)
		return ch
	}
	if r.listed {
		sendList(ch, r.list)
	}
	r.subs = append(r.subs, ch)
	return ch
}

// Close stops the Resolver: lookups return ErrClosed, every channel of Subscribe is closed, and
// every goroutine of the Resolver has returned when Close returns. Later calls return ErrClosed.
func (r *Resolver) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	r.closed = true
	r.state.Store(&resolverState{closed: true})
	for _, ch := range r.subs {
		close(ch)
	}
	r.subs, r.list = nil, nil
	r.armLocked(time.Time{})
	r.mu.Unlock()

	if r.stop != nil {
		r.stop()
		<-r.done
	}
	r.timers.Wait()
	if r.onClose != nil {
		r.onClose(r)
	}
	return nil
}
