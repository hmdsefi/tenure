// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// errUnknownSpace is a lookup's error for a space the Router does not have. It names no space,
// so that returning it allocates nothing.
var errUnknownSpace = errors.New("tenure: unknown space")

// Router finds the member that serves a key from outside the group: it follows a Membership
// without joining it, computes each space's assignment from the views as the members do, and
// answers lookups from an immutable snapshot of that assignment.
//
// A lookup, Route, RouteShard or a RouteSpace's methods, loads the snapshot, hashes the key and
// reads two arrays: it takes no lock, makes no call and allocates nothing, also when it returns
// an error, and writes no memory that other lookups read, so lookups from any number of
// goroutines do not slow each other down. A Router's methods are safe for concurrent use.
//
// The table equals each member's own Node.Route once the Router and the member have applied
// the same view. In a group with slots, shards are placed on slots, so the Router needs views
// that carry the slot data, as a member's merged view does; a Membership whose views have no
// slots gives a table by member ID.
type Router struct {
	spaces []*RouteSpace          // in the order NewRouter was given them
	byName map[string]*RouteSpace // never changed after NewRouter
	opts   routerOptions
	epoch  time.Time // the clock's time at NewRouter; readings count from it

	snap atomic.Pointer[routerSnapshot] // what lookups read; replaced, never changed

	ready     chan struct{} // closed when the first view is applied
	readyOnce sync.Once
	closed    chan struct{} // closed by Close
	closeOnce sync.Once

	source Membership         // the Membership the builder watches
	cancel context.CancelFunc // ends the builder's context
	done   chan struct{}      // closed when the builder returns
	stale  atomic.Bool        // the watch has ended or failed and no view has come since

	// mu guards the build state: only one snapshot is built at a time.
	mu       sync.Mutex
	assigner *core.Assigner
	view     *core.View       // the applied view, nil before the first
	reading  time.Duration    // the latest reading fed to assigner
	subs     []chan uint64    // every channel Subscribe returned, until Close
	builds   routerBuildStats // what the builds have done, for tests and events
	events   *routerEvents

	// testApplyHook, when set by a test, is called with each view the builder is about to apply,
	// with mu held.
	testApplyHook func(core.View)
}

// RouteSpace is a handle on one space of a Router. Its lookups skip the space lookup of
// Router.Route and read the Router's latest snapshot. A RouteSpace stays valid for the Router's
// lifetime and is safe for concurrent use.
type RouteSpace struct {
	r       *Router
	index   int // in Router.spaces and in every snapshot's tables
	name    string
	shards  int
	shardOf func(key string) int

	// errOut and errShard are the errors for a ShardOf result and a shard outside the space,
	// built once so that returning them allocates nothing.
	errOut   error
	errShard error

	// corrections holds the space's corrections, one per shard, and live counts those set
	// (RTR-410). Lookups only read them; Correct and the builder write them.
	corrections []atomic.Pointer[correction]
	live        atomic.Int64
}

// A RouterOption changes how NewRouter builds a Router.
type RouterOption func(*routerOptions)

// routerOptions holds what the RouterOptions set.
type routerOptions struct {
	timing        core.Timing
	logger        *slog.Logger
	events        func(RouterEvent)
	correctionTTL time.Duration
	now           func() time.Time
}

// WithRouterTiming sets the timing of the group the Router follows: SlotGrace decides when a
// vacant slot drops out of the table, as on the members, and RetryPeriod how long the Router
// waits before it watches again after its watch ended. It must be the members' timing. The zero
// value means core.DefaultTiming, and a zero SlotGrace means DefaultSlotGrace, as in Config.
func WithRouterTiming(t core.Timing) RouterOption {
	return func(o *routerOptions) { o.timing = t }
}

// WithRouterLogger makes the Router log each of its events to logger. nil logs nothing, the
// default.
func WithRouterLogger(logger *slog.Logger) RouterOption {
	return func(o *routerOptions) { o.logger = logger }
}

// WithRouterEvents makes the Router pass each of its events to f, on a goroutine of its own and
// in order, as Config.OnEvent receives a node's events. nil, the default, passes none.
func WithRouterEvents(f func(RouterEvent)) RouterOption {
	return func(o *routerOptions) { o.events = f }
}

// WithCorrectionTTL sets how long a correction made with Router.Correct lasts at most. The
// default, and the value used for a duration that is not positive, is the timing's
// LeaseDuration.
func WithCorrectionTTL(d time.Duration) RouterOption {
	return func(o *routerOptions) { o.correctionTTL = d }
}

// WithClock makes the Router read the time from now instead of time.Now, for tests. The Router
// measures the time between two readings of now, so now should carry a monotonic reading or
// never go backwards. nil keeps time.Now.
func WithClock(now func() time.Time) RouterOption {
	return func(o *routerOptions) {
		if now != nil {
			o.now = now
		}
	}
}

// NewRouter returns a Router for the spaces of a group whose members announce themselves
// through m. It reads only the Name, Shards and ShardOf of each space, so the members' own
// []Space serves, and checks them as Start checks a Config: each Name is non-empty, contains
// neither "/" nor a zero byte, is distinct and is not reserved, and each Shards is at least 1.
// A failure is returned as "tenure: NewRouter: " followed by the reason, such as
// `tenure: NewRouter: Spaces[1].Name: duplicate name "orders"`.
//
// NewRouter calls m.Watch once and returns its error as "tenure: NewRouter: watch: " and the
// error; it never calls Join, SetDraining or Leave. The Router's one goroutine then applies the
// latest view of the channel, so a burst of views costs at most two builds, and lookups read the
// previous snapshot while it builds. When the placement keys stay the same, as with slots while
// the slot count does, it maps them to the new members without recomputing the assignment. When
// the channel closes or a later Watch fails, the Router keeps its last table, reports Stale, and
// calls Watch again the timing's RetryPeriod later, until Close or until Watch returns ErrClosed.
//
// Until the Router has applied its first view, lookups fail with an error that matches
// ErrNoMembers and says "no view yet".
func NewRouter(m Membership, spaces []Space, opts ...RouterOption) (*Router, error) {
	if m == nil {
		return nil, errors.New("tenure: NewRouter: nil membership")
	}
	o := routerOptions{now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.timing == (core.Timing{}):
		o.timing = core.DefaultTiming
	case o.timing.SlotGrace == 0:
		o.timing.SlotGrace = DefaultSlotGrace
	}
	if err := o.timing.Validate(); err != nil {
		return nil, fmt.Errorf("tenure: NewRouter: timing: %w", err)
	}
	if o.correctionTTL <= 0 {
		o.correctionTTL = o.timing.LeaseDuration
	}
	if err := checkRouterSpaces(spaces); err != nil {
		return nil, err
	}

	r := &Router{
		byName: make(map[string]*RouteSpace, len(spaces)),
		opts:   o,
		epoch:  o.now(),
		ready:  make(chan struct{}),
		closed: make(chan struct{}),
	}
	specs := make([]core.AssignerSpace, len(spaces))
	for i, sp := range spaces {
		h := &RouteSpace{r: r, index: i, name: sp.Name, shards: sp.Shards, shardOf: sp.ShardOf}
		h.errOut = fmt.Errorf("tenure: %s: ShardOf result is outside [0, %d)", sp.Name, sp.Shards)
		h.errShard = fmt.Errorf("tenure: %s: shard is outside [0, %d)", sp.Name, sp.Shards)
		h.corrections = make([]atomic.Pointer[correction], sp.Shards)
		r.spaces = append(r.spaces, h)
		r.byName[sp.Name] = h
		specs[i] = core.AssignerSpace{Name: sp.Name, Shards: sp.Shards}
	}
	a, err := core.NewAssigner(specs, o.timing)
	if err != nil {
		return nil, fmt.Errorf("tenure: NewRouter: %w", err) // checkRouterSpaces has made this unreachable
	}
	r.assigner = a
	r.snap.Store(r.firstSnapshot())
	r.events = newRouterEvents(o.logger, o.events, o.now)
	if err := r.start(m); err != nil {
		r.events.close(RouterEvent{Kind: RouterEventClosed, Shard: -1})
		return nil, err
	}
	return r, nil
}

// checkRouterSpaces checks spaces as checkFields checks a Config's, but for Run.
func checkRouterSpaces(spaces []Space) error {
	for i, sp := range spaces {
		var reason string
		switch {
		case sp.Name == "" || strings.ContainsAny(sp.Name, "/\x00"):
			reason = fmt.Sprintf("Spaces[%d].Name: invalid name %q", i, sp.Name)
		case slices.Contains(reservedSpaceNames, sp.Name):
			reason = fmt.Sprintf("Spaces[%d].Name: reserved name %q", i, sp.Name)
		case slices.ContainsFunc(spaces[:i], func(e Space) bool { return e.Name == sp.Name }):
			reason = fmt.Sprintf("Spaces[%d].Name: duplicate name %q", i, sp.Name)
		case sp.Shards < 1:
			reason = fmt.Sprintf("Spaces[%d].Shards: %d is below 1", i, sp.Shards)
		default:
			continue
		}
		return errors.New("tenure: NewRouter: " + reason)
	}
	return nil
}

// Route returns the member that serves key in space: the member to which the Router's latest
// snapshot assigns key's shard. The shard is the space's ShardOf(key), or
// core.KeyShard(key, Shards) when ShardOf is nil. The Meta map of the result is shared with the
// snapshot and must not be modified. Route allocates nothing.
//
// After Close, Route returns ErrClosed. For a space the Router does not have it returns an error
// whose text is "tenure: unknown space", and for a ShardOf result outside the space one such as
// "tenure: orders: ShardOf result is outside [0, 8)"; neither names the caller's input, so that
// they allocate nothing either. When no member can serve the shard, it returns an error that
// matches ErrNoMembers and names the shard, with the same text as Node.Route, such as
// "tenure: no members: orders/3: slot-1 is vacant", or "tenure: no members: orders/3: no view
// yet" before the Router has applied a view.
func (r *Router) Route(space, key string) (Member, error) {
	h, ok := r.byName[space]
	if !ok {
		if r.snap.Load().closed {
			return Member{}, ErrClosed
		}
		return Member{}, errUnknownSpace
	}
	return h.Route(key)
}

// RouteShard returns the member that serves shard of space, with Route's errors; a shard outside
// [0, Shards) gives an error such as "tenure: orders: shard is outside [0, 8)". RouteShard
// allocates nothing.
func (r *Router) RouteShard(space string, shard int) (Member, error) {
	h, ok := r.byName[space]
	if !ok {
		if r.snap.Load().closed {
			return Member{}, ErrClosed
		}
		return Member{}, errUnknownSpace
	}
	return h.RouteShard(shard)
}

// Space returns a handle on the named space, or an error such as `tenure: unknown space
// "carts"` for a space the Router does not have, or ErrClosed after Close.
func (r *Router) Space(name string) (*RouteSpace, error) {
	if r.snap.Load().closed {
		return nil, ErrClosed
	}
	h, ok := r.byName[name]
	if !ok {
		return nil, fmt.Errorf("tenure: unknown space %q", name)
	}
	return h, nil
}

// Version returns the version of the Router's latest snapshot: 0 before the first view, then one
// more for each new snapshot.
func (r *Router) Version() uint64 {
	return r.snap.Load().version
}

// Ready waits until the Router has applied its first view. It returns nil then, ErrClosed once
// the Router is closed, also after a view, or ctx.Err() if ctx is done first.
func (r *Router) Ready(ctx context.Context) error {
	select {
	case <-r.closed:
		return ErrClosed
	default:
	}
	select {
	case <-r.ready:
		return nil
	case <-r.closed:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Name returns the space's name.
func (h *RouteSpace) Name() string {
	return h.name
}

// Shards returns the space's shard count.
func (h *RouteSpace) Shards() int {
	return h.shards
}

// Shard returns key's shard: ShardOf(key), or core.KeyShard(key, Shards) when ShardOf is nil. A
// ShardOf result outside [0, Shards) gives the error Router.Route gives. Shard allocates
// nothing.
func (h *RouteSpace) Shard(key string) (int, error) {
	if h.shardOf == nil {
		return core.KeyShard(key, h.shards), nil
	}
	shard := h.shardOf(key)
	if shard < 0 || shard >= h.shards {
		return 0, h.errOut
	}
	return shard, nil
}

// Route returns the member that serves key in the space, as Router.Route does.
func (h *RouteSpace) Route(key string) (Member, error) {
	s := h.r.snap.Load()
	if s.closed {
		return Member{}, ErrClosed
	}
	shard, err := h.Shard(key)
	if err != nil {
		return Member{}, err
	}
	if h.live.Load() > 0 {
		return h.corrected(s, shard)
	}
	return s.route(h.index, shard)
}

// RouteShard returns the member that serves shard, as Router.RouteShard does.
func (h *RouteSpace) RouteShard(shard int) (Member, error) {
	s := h.r.snap.Load()
	if s.closed {
		return Member{}, ErrClosed
	}
	if shard < 0 || shard >= h.shards {
		return Member{}, h.errShard
	}
	if h.live.Load() > 0 {
		return h.corrected(s, shard)
	}
	return s.route(h.index, shard)
}

// apply applies v, read at reading and wall-clock time wall, and publishes the new snapshot. A
// view the Assigner ignores, being invalid or older than the applied one, only moves the clock,
// which may expire a slot's vacancy. It reports whether it applied v. A reading before the last
// one counts as the last. The caller holds r.mu.
func (r *Router) apply(v core.View, reading time.Duration, wall time.Time) bool {
	if v.Validate() != nil || r.view != nil && v.Version < r.view.Version {
		r.tick(reading)
		return false
	}
	start := r.opts.now()
	keys := r.assigner.Keys()
	r.reading = max(reading, r.reading)
	r.assigner.ApplyView(v, r.reading, wall)
	r.view = &v
	r.publish(!sameSlice(keys, r.assigner.Keys()), start, true)
	r.readyOnce.Do(func() { close(r.ready) })
	return true
}

// tick moves the Assigner's clock to reading and publishes a new snapshot when that changed an
// assignment. A reading before the last one counts as the last. The caller holds r.mu.
func (r *Router) tick(reading time.Duration) {
	start := r.opts.now()
	r.reading = max(reading, r.reading)
	keys := r.assigner.Keys()
	if r.assigner.Tick(r.reading) && r.view != nil {
		r.publish(!sameSlice(keys, r.assigner.Keys()), start, false)
	}
}

// routerBuildStats counts what a Router's builds have done.
type routerBuildStats struct {
	snapshots uint64 // snapshots published
	balanced  uint64 // of them, those whose placement keys changed, so Balanced ran
}

// publish builds a snapshot of the applied view and the Assigner's assignments, makes it the one
// lookups read, sends its version to every subscriber and reports it (RTR-600). balanced reports
// that the placement keys changed, so the Assigner ran Balanced; start is when the build began;
// viewed that a view was applied. After Close it does nothing. The caller holds r.mu.
func (r *Router) publish(balanced bool, start time.Time, viewed bool) {
	prev := r.snap.Load()
	if prev.closed {
		return
	}
	s := r.buildSnapshot(prev.version + 1)
	r.dropCorrections()
	r.snap.Store(s)
	took := r.opts.now().Sub(start)
	r.builds.snapshots++
	if balanced {
		r.builds.balanced++
	}
	for _, ch := range r.subs {
		sendVersion(ch, s.version)
	}

	if viewed {
		r.events.emit(RouterEvent{Kind: RouterEventView, Version: s.version, ViewVersion: s.view.Version,
			Count: len(s.view.Members), Shard: -1})
	}
	for i, h := range r.spaces {
		r.events.emit(RouterEvent{Kind: RouterEventRebuilt, Version: s.version, Space: h.name, Shard: -1,
			Count: movedShards(prev, s, i), Balanced: balanced, Duration: took})
		was, is := prev.tables[i].mismatch, s.tables[i].mismatch
		switch {
		case was == nil && is != nil:
			r.events.emit(RouterEvent{Kind: RouterEventMismatch, Version: s.version, Space: h.name, Shard: -1, Err: is})
		case was != nil && is == nil:
			r.events.emit(RouterEvent{Kind: RouterEventMatched, Version: s.version, Space: h.name, Shard: -1})
		}
	}
}

// movedShards returns how many shards of the space at index i route to another member in next
// than in prev, a shard with no member counting as routed to "".
func movedShards(prev, next *routerSnapshot, i int) int {
	owner := func(s *routerSnapshot, shard int) string {
		if m := s.tables[i].owners[shard]; m >= 0 {
			return s.members[m].ID
		}
		return ""
	}
	n := 0
	for shard := range next.tables[i].owners {
		if owner(prev, shard) != owner(next, shard) {
			n++
		}
	}
	return n
}
