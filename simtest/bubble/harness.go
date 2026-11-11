// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package bubble runs the real tenure runtime, three nodes on one in-memory store, inside
// testing/synctest bubbles, and checks the ownership invariants while members crash, lose the
// store, drain and roll out one by one.
//
// The package has no API: it is a set of tests. Unlike the event-mode simulations, the code
// runs on real goroutines with real timers in virtual time, so a run is not a function of a
// seed: the tests check invariants only.
package bubble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/simtest/internal/check"
	"github.com/hmdsefi/tenure/static"
)

// The timing of every node: short, so that a test covers several leases in little virtual time.
const (
	leaseDuration = 3 * time.Second
	renewDeadline = 2 * time.Second
	retryPeriod   = 500 * time.Millisecond
	slotGrace     = 3 * time.Second
)

// timing is the Timing of every node.
var timing = core.Timing{
	LeaseDuration: leaseDuration,
	RenewDeadline: renewDeadline,
	RetryPeriod:   retryPeriod,
	SlotGrace:     slotGrace,
}

// spaces are the spaces of every node: one without fencing and one with.
var spaces = []core.SpaceSpec{
	{Name: "book", Shards: 12},
	{Name: "ledger", Shards: 6, Fenced: true},
}

// memberIDs are the members of the group, in index order.
var memberIDs = []string{"m1", "m2", "m3"}

// errRejected is the error a Run returns when the fence rejects its write.
var errRejected = errors.New("bubble: fence rejected the write")

// options configures a harness. The zero value runs the group without slots, with every call
// let through and crypto/rand naming the sessions.
type options struct {
	// slots is Config.Slots of every node.
	slots int

	// gate gates every call of a member to the store and the fence; nil runs every call.
	gate gateFunc

	// rand returns Config.Rand of a member's next incarnation; nil (or a nil result) means
	// crypto/rand.
	rand func(member string) io.Reader

	// onEvent, when not nil, hears every event of every node, after the harness used it.
	onEvent func(member string, e tenure.Event)
}

// harness is one group of real nodes on one memstore and one static group, with a fence every
// Run writes to and a checker fed from all of them. Its methods other than the ones the nodes
// call (through memberStore, the Runs and OnEvent) are for the test goroutine.
type harness struct {
	t     testing.TB
	opts  options
	gate  gateFunc
	start time.Time // time 0 of the checker

	store   *memstore.Store
	group   *static.Membership
	fence   *fence
	members []*member

	// mu guards everything below. It is taken inside fence.mu (the fence reports an accepted
	// write under its lock), never the other way round, and never with a memberStore.mu held.
	mu         sync.Mutex
	checker    *check.Checker
	grants     <-chan tenure.Holding // the harness's own watch of every record write
	stopGrants context.CancelFunc
	watchErr   error                     // the harness's watch closed while it was needed
	started    map[runKey]bool           // the ownerships whose StartRun was reported
	passes     map[shardKey][]checkPass  // the passing Checks of unfenced shards
	overlap    error                     // the first overlap of two holders' passing Checks
	stopErr    error                     // the first Run whose context ended while Check passed
	holders    map[*incarnation][]string // the holders each incarnation ran shards under
	closing    bool                      // the harness stopped its watch
	counts     counts
}

// counts are what a harness saw, which finish logs.
type counts struct {
	grants, runs, writes, rejected, passes int
}

// runKey names one ownership: one holder's term of one shard.
type runKey struct {
	holder string
	space  string
	shard  int
	term   core.Term
}

// checkPass is the span of one holder's passing Checks of an unfenced shard in one term.
type checkPass struct {
	holder      string
	term        core.Term
	first, last time.Duration
}

// member is one member of the group across its incarnations.
type member struct {
	h     *harness
	id    string
	store *memberStore
	inc   *incarnation // the running incarnation, nil while the member is down
}

// incarnation is one node of a member: one static handle, one Start, one Close.
type incarnation struct {
	m     *member
	share *static.Membership
	node  *tenure.Node

	mu     sync.Mutex
	active int           // Runs in progress
	idle   chan struct{} // closed when active drops to 0, while waitRuns waits
}

// newHarness returns a harness with its members down: start them with startAll or member.start.
// It must be called inside the test's bubble.
func newHarness(t testing.TB, opts options) *harness {
	t.Helper()
	h := &harness{
		t:       t,
		opts:    opts,
		gate:    opts.gate,
		start:   time.Now(),
		store:   memstore.New(nil),
		started: make(map[runKey]bool),
		passes:  make(map[shardKey][]checkPass),
		holders: make(map[*incarnation][]string),
	}
	if h.gate == nil {
		h.gate = runGate
	}
	group, err := static.New(nil)
	if err != nil {
		t.Fatalf("static.New(nil) = %v", err)
	}
	h.group = group
	h.checker = check.New(check.Config{Now: h.now, Spaces: spaces})
	h.fence = newFence(h.accepted)

	ctx, cancel := context.WithCancel(context.Background())
	grants, err := h.store.WatchHolders(ctx)
	if err != nil {
		cancel()
		t.Fatalf("WatchHolders() = %v", err)
	}
	h.grants, h.stopGrants = grants, cancel

	for _, id := range memberIDs {
		m := &member{h: h, id: id}
		m.store = newMemberStore(h, id)
		h.members = append(h.members, m)
	}
	t.Cleanup(h.cleanup)
	return h
}

// cleanup crashes every member still running and stops the harness's watch, so that every
// goroutine of the bubble ends also when the test stopped before finish, with t.Fatal for example.
// The bubble runs it before it waits for its goroutines.
func (h *harness) cleanup() {
	for _, m := range h.members {
		if m.inc != nil {
			m.crash()
		}
	}
	h.mu.Lock()
	h.closing = true
	h.mu.Unlock()
	h.stopGrants()
}

// now is the checker's clock: the bubble's time since the harness was made.
func (h *harness) now() kernel.Time { return kernel.Time(time.Since(h.start)) }

// elapsed is the bubble's time since the harness was made.
func (h *harness) elapsed() time.Duration { return time.Since(h.start) }

// sleepUntil sleeps until the harness's time at.
func (h *harness) sleepUntil(at time.Duration) {
	if d := at - h.elapsed(); d > 0 {
		time.Sleep(d)
	}
}

// member returns the member with id.
func (h *harness) member(id string) *member {
	for _, m := range h.members {
		if m.id == id {
			return m
		}
	}
	panic("bubble: unknown member " + id)
}

// startAll starts every member, in order.
func (h *harness) startAll() {
	h.t.Helper()
	for _, m := range h.members {
		if err := m.start(context.Background()); err != nil {
			h.t.Fatalf("start %s: %v", m.id, err)
		}
	}
}

// syncGrants reports to the checker every record write the store made since the last call. The
// store delivers a write to the harness's watch before the call that made it returns, so after a
// call of a memberStore the checker has heard of its grant.
func (h *harness) syncGrants() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drainGrants()
}

// drainGrants is syncGrants with h.mu held. Every write that leaves a holder on a record is a
// grant: memstore writes a record only to grant it, to clear its holder or to delete it.
func (h *harness) drainGrants() {
	for h.grants != nil {
		select {
		case rec, ok := <-h.grants:
			if !ok {
				if !h.closing && h.watchErr == nil {
					h.watchErr = errors.New("bubble: the harness's watch of the store closed")
				}
				h.grants = nil
				return
			}
			if rec.Holder != "" {
				// Prev, Ended, Unseen and Lease are not observable from outside the store, so g4's
				// takeover rule is not fed: the bubble asserts g1, g3 and g5 only.
				h.counts.grants++
				h.checker.Grant(check.Grant{
					Space: rec.Space, Shard: rec.Shard, Term: rec.Term, Holder: rec.Holder,
				})
			}
		default:
			return
		}
	}
}

// accepted reports a write the fence accepted; the fence calls it under its own lock.
func (h *harness) accepted(w check.Write) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drainGrants()
	h.counts.writes++
	h.checker.Accepted(w)
}

// startRun reports that a Run of inc serves holder's term of the shard: the member learned of
// the grant, and started its run unless an earlier Run of the same ownership did.
func (h *harness) startRun(inc *incarnation, holder, space string, shard int, term core.Term) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drainGrants()
	if !slices.Contains(h.holders[inc], holder) {
		h.holders[inc] = append(h.holders[inc], holder)
	}
	h.checker.Acquired(holder, space, shard, term)
	k := runKey{holder, space, shard, term}
	if !h.started[k] {
		h.started[k] = true
		h.counts.runs++
		h.checker.StartRun(holder, space, shard, term)
	}
}

// stopRun reports that holder's ownership of the shard in term ended. A second report, or one for
// an ownership that is not running, changes nothing.
func (h *harness) stopRun(holder, space string, shard int, term core.Term) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checker.StopRun(holder, space, shard, term)
}

// passed records that holder's Check of an unfenced shard in term returned nil now. Two holders'
// ownerships of an unfenced shard must not overlap: every passing Check of an older term comes
// before every passing Check of a newer one, and no term passes for two holders. With every
// clock the bubble's (no drift, no delay), the store grants a newer term only after the older
// holder's session expired, LeaseDuration after its last renewal, while the older holder's Check
// fails from its session deadline, RenewDeadline after it sent that renewal.
func (h *harness) passed(holder, space string, shard int, term core.Term) {
	now := h.elapsed()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts.passes++
	k := shardKey{space, shard}
	list := h.passes[k]
	i := slices.IndexFunc(list, func(p checkPass) bool { return p.holder == holder && p.term == term })
	if i < 0 {
		list = append(list, checkPass{holder: holder, term: term, first: now, last: now})
		i = len(list) - 1
	} else {
		list[i].last = now
	}
	h.passes[k] = list
	if h.overlap != nil {
		return
	}
	cur := list[i]
	for _, o := range list {
		if o.holder == holder {
			continue
		}
		var older, newer checkPass
		switch {
		case o.term < cur.term:
			older, newer = o, cur
		case o.term > cur.term:
			older, newer = cur, o
		default:
			h.overlap = fmt.Errorf("overlap: %s/%d: term %d passed Check for %s and %s", space, shard, cur.term, o.holder, holder)
			return
		}
		if older.last >= newer.first {
			h.overlap = fmt.Errorf("overlap: %s/%d: %s (term %d) passed Check at %v, after %s (term %d) passed it at %v",
				space, shard, older.holder, older.term, kernel.Time(older.last), newer.holder, newer.term, kernel.Time(newer.first))
			return
		}
	}
}

// violation keeps err as the first Run whose context ended while its Check still passed.
func (h *harness) violation(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopErr == nil {
		h.stopErr = err
	}
}

// liveness returns the check g5 at r + b.
func (h *harness) liveness(r, b time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drainGrants()
	return h.checker.Liveness(kernel.Time(r), b)
}

// bound returns the bound on how long a shard stays without an owner after the last
// disturbance, with no message delay and no drift: LeaseDuration + 3·RetryPeriod, plus SlotGrace
// with slots.
func (h *harness) bound() time.Duration {
	b := leaseDuration + 3*retryPeriod
	if h.opts.slots > 0 {
		b += slotGrace
	}
	return b
}

// finish checks g5 for the disturbance that ended at r, closes every running member (a cut one
// with a done context, the others with a drain), stops the harness's watch and fails the test for
// every violated check.
func (h *harness) finish(r time.Duration) {
	h.t.Helper()
	if err := h.liveness(r, h.bound()); err != nil {
		h.t.Error(err)
	}
	for _, m := range h.members {
		if m.inc == nil {
			continue
		}
		if m.store.isCut() {
			m.crash()
			continue
		}
		if err := m.close(); err != nil {
			h.t.Errorf("close %s: %v", m.id, err)
		}
	}
	h.mu.Lock()
	h.drainGrants()
	h.closing = true
	c := h.counts
	h.mu.Unlock()
	h.t.Logf("%d grants, %d runs started, %d writes accepted and %d rejected, %d passing Checks of unfenced shards",
		c.grants, c.runs, c.writes, c.rejected, c.passes)
	h.stopGrants()
	for _, err := range h.errs() {
		h.t.Error(err)
	}
}

// errs returns the violations of the checks the bubble asserts.
func (h *harness) errs() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []error
	for _, name := range []string{"g1", "g3"} {
		if err := h.checker.Err(name); err != nil {
			out = append(out, err)
		}
	}
	if h.overlap != nil {
		out = append(out, h.overlap)
	}
	if h.stopErr != nil {
		out = append(out, h.stopErr)
	}
	if h.watchErr != nil {
		out = append(out, h.watchErr)
	}
	return out
}

// converged returns nil when the running members agree on the assignment and every shard is
// owned by its assignee alone: in its assignee's table it is owned, the store names the
// assignee's holder, and no other member's table has it in any state but idle.
func (h *harness) converged() error {
	type nodeTable struct {
		id    string
		table tenure.Table
	}
	var tables []nodeTable
	for _, m := range h.members {
		if m.inc != nil {
			tables = append(tables, nodeTable{m.id, m.inc.node.Table()})
		}
	}
	if len(tables) == 0 {
		return errors.New("converged: no member is running")
	}
	for si, sp := range spaces {
		recs, err := h.store.Holdings(context.Background(), sp.Name)
		if err != nil {
			return fmt.Errorf("converged: Holdings(%s) = %v", sp.Name, err)
		}
		for shard := range sp.Shards {
			route := tables[0].table.Spaces[si].Rows[shard].Route
			if route == "" {
				return fmt.Errorf("converged: %s/%d: %s routes it nowhere", sp.Name, shard, tables[0].id)
			}
			for _, nt := range tables {
				row := nt.table.Spaces[si].Rows[shard]
				if row.Route != route {
					return fmt.Errorf("converged: %s/%d: %s routes it to %q, %s to %q",
						sp.Name, shard, tables[0].id, route, nt.id, row.Route)
				}
				switch {
				case nt.id == route && row.State != "owned":
					return fmt.Errorf("converged: %s/%d: assignee %s has it %s", sp.Name, shard, nt.id, row.State)
				case nt.id != route && row.State != "idle":
					return fmt.Errorf("converged: %s/%d: %s, not the assignee %s, has it %s",
						sp.Name, shard, nt.id, route, row.State)
				}
				if nt.id == route {
					i := slices.IndexFunc(recs, func(r tenure.Holding) bool { return r.Shard == shard })
					if i < 0 || recs[i].Holder != nt.table.Holder {
						return fmt.Errorf("converged: %s/%d: the store does not name %s's holder %s",
							sp.Name, shard, nt.id, nt.table.Holder)
					}
				}
			}
		}
	}
	return nil
}

// mustConverge stops the test when the group has not converged; when names the moment.
func (h *harness) mustConverge(when string) {
	h.t.Helper()
	if err := h.converged(); err != nil {
		h.t.Fatalf("%s: %v", when, err)
	}
}

// owned returns how many shards m's running node has in state owned, 0 while m is down.
func (m *member) owned() int {
	if m.inc == nil {
		return 0
	}
	n := 0
	for _, sp := range m.inc.node.Table().Spaces {
		for _, row := range sp.Rows {
			if row.State == "owned" {
				n++
			}
		}
	}
	return n
}

// start starts a new incarnation of m: a new static handle and a new node on m's store.
func (m *member) start(ctx context.Context) error {
	if m.inc != nil {
		return fmt.Errorf("bubble: %s is running", m.id)
	}
	h := m.h
	inc := &incarnation{m: m, share: h.group.Share()}
	var rnd io.Reader
	if h.opts.rand != nil {
		rnd = h.opts.rand(m.id)
	}
	cfg := tenure.Config{
		Self:       tenure.Member{ID: m.id, Addr: m.id + ":7000"},
		Membership: inc.share,
		Store:      m.store,
		Slots:      h.opts.slots,
		Timing:     timing,
		Rand:       rnd,
		OnEvent:    inc.onEvent,
	}
	for _, sp := range spaces {
		cfg.Spaces = append(cfg.Spaces, tenure.Space{
			Name: sp.Name, Shards: sp.Shards, Fenced: sp.Fenced, Run: inc.run(sp),
		})
	}
	node, err := tenure.Start(ctx, cfg)
	if err != nil {
		return err
	}
	inc.node = node
	m.inc = inc
	return nil
}

// close drains m's node and closes it, which leaves the group.
func (m *member) close() error {
	inc := m.inc
	ctx, cancel := context.WithTimeout(context.Background(), 3*leaseDuration)
	defer cancel()
	err := inc.node.Close(ctx)
	inc.waitRuns()
	m.down(inc)
	return err
}

// drain drains m's node, which then hands its shards over and closes its session, and closes it.
func (m *member) drain() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*leaseDuration)
	defer cancel()
	if err := m.inc.node.Drain(ctx); err != nil {
		return err
	}
	return m.close()
}

// crash crashes m: it cuts m off from the store, closes the node with a context done already, so
// that the node neither releases its shards nor closes its session, and leaves the group with
// the incarnation's handle. The store expires the session; m stays cut off until heal.
func (m *member) crash() {
	inc := m.inc
	m.store.Cut()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := inc.node.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
		m.h.t.Errorf("close %s with a done context: %v", m.id, err)
	}
	if err := inc.share.Leave(context.Background()); err != nil {
		m.h.t.Errorf("leave %s: %v", m.id, err)
	}
	inc.waitRuns()
	m.down(inc)
}

// down reports the holders of inc down and marks m down.
func (m *member) down(inc *incarnation) {
	h := m.h
	holder := inc.node.Table().Holder
	h.mu.Lock()
	holders := h.holders[inc]
	if holder != "" && !slices.Contains(holders, holder) {
		holders = append(holders, holder)
	}
	for _, hd := range holders {
		h.checker.Down(hd)
	}
	delete(h.holders, inc)
	h.mu.Unlock()
	m.inc = nil
}

// cut cuts m off from the store; heal ends it.
func (m *member) cut()  { m.store.Cut() }
func (m *member) heal() { m.store.Heal() }

// run returns the Run of the space for inc: it reports the ownership to the checker, raises the
// fence to the shard's term, then writes with the term at intervals uniform in [0, RetryPeriod],
// calling Check before each write. It returns Check's error, ctx's cause, errRejected when the
// fence rejects a write, or the gate's error. The node restarts a Run that returns while it
// still owns the shard.
func (inc *incarnation) run(sp core.SpaceSpec) func(ctx context.Context, s *tenure.Shard) error {
	m, h := inc.m, inc.m.h
	return func(ctx context.Context, s *tenure.Shard) error {
		inc.enter()
		defer inc.leave()
		holder, ok := m.store.holderOf(s.Space, s.ID, s.Term)
		if !ok {
			// The node runs a term this member never acquired: the checker reports it.
			holder = m.id + "/unknown"
		}
		h.startRun(inc, holder, s.Space, s.ID, s.Term)
		defer func() {
			if ctx.Err() != nil || s.Check() != nil {
				h.stopRun(holder, s.Space, s.ID, s.Term)
			}
		}()

		if err := h.gate(m.id, "fence.raise", func() { h.fence.raise(s.Space, s.ID, s.Term) }); err != nil {
			return err
		}
		rng := rand.New(rand.NewPCG(uint64(s.Term), uint64(s.ID))) //nolint:gosec // G404: write intervals, not secrets
		for {
			timer := time.NewTimer(time.Duration(rng.Int64N(int64(retryPeriod) + 1)))
			select {
			case <-ctx.Done():
				timer.Stop()
				// The node cancels ctx only after the ownership stopped, so Check fails from then
				// on.
				if s.Check() == nil {
					h.violation(fmt.Errorf("stop: %s/%d: %s's context ended (%v) in term %d, but Check passes",
						s.Space, s.ID, holder, context.Cause(ctx), s.Term))
				}
				return context.Cause(ctx)
			case <-timer.C:
			}
			if err := s.Check(); err != nil {
				return err
			}
			if !sp.Fenced {
				h.passed(holder, s.Space, s.ID, s.Term)
			}
			var accepted bool
			if err := h.gate(m.id, "fence.write", func() {
				accepted = h.fence.write(holder, s.Space, s.ID, s.Term)
			}); err != nil {
				return err
			}
			if !accepted {
				h.mu.Lock()
				h.counts.rejected++
				h.mu.Unlock()
				return errRejected
			}
		}
	}
}

// onEvent is Config.OnEvent of inc. An ownership can end while its Run is not running, between
// a return and the restart, so the events that end one report its stop too.
func (inc *incarnation) onEvent(e tenure.Event) {
	m := inc.m
	switch e.Kind {
	case tenure.EventShardStopping, tenure.EventShardExpired, tenure.EventShardLost, tenure.EventShardReleased:
		if holder, ok := m.store.holderOf(e.Space, e.Shard, e.Term); ok {
			m.h.stopRun(holder, e.Space, e.Shard, e.Term)
		}
	}
	if m.h.opts.onEvent != nil {
		m.h.opts.onEvent(m.id, e)
	}
}

// enter and leave count the Runs of inc in progress.
func (inc *incarnation) enter() {
	inc.mu.Lock()
	defer inc.mu.Unlock()
	inc.active++
}

func (inc *incarnation) leave() {
	inc.mu.Lock()
	defer inc.mu.Unlock()
	inc.active--
	if inc.active == 0 && inc.idle != nil {
		close(inc.idle)
		inc.idle = nil
	}
}

// waitRuns waits until no Run of inc is in progress. After Close no Run starts, and every Run
// returns once its context is done.
func (inc *incarnation) waitRuns() {
	inc.mu.Lock()
	if inc.active == 0 {
		inc.mu.Unlock()
		return
	}
	idle := make(chan struct{})
	inc.idle = idle
	inc.mu.Unlock()
	<-idle
}
