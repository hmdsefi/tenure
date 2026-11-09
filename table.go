// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Table is the ownership table of a node, as Node.Table returns it: one consistent snapshot of
// what the node believes about itself, its session, its view and every shard of its spaces. All
// of it describes the node at one instant between two steps of its loop. It shares no memory with
// the node. Its times are in UTC, and a zero time means none.
type Table struct {
	// Taken is when the snapshot was taken.
	Taken time.Time

	// Member is this member as the node's latest view lists it, or Config.Self while the view
	// does not list it.
	Member Member

	// Holder is the node's holder: the member ID, "/" and the incarnation in lowercase hex. It
	// changes when the node starts a new incarnation after a lost session.
	Holder string

	// Draining reports that the member is handing its work over: Drain or Close has started the
	// drain. It stays true after Close.
	Draining bool

	// Closed reports that Close has stopped the node.
	Closed bool

	// SessionLive reports that the member's session is live: a renewal was accepted and its
	// deadline has not passed. It is false without a Store, while the session opens, once the
	// deadline has passed, once the session is lost or closed, and after Close.
	SessionLive bool

	// LastRenewal is the wall-clock time at which the node sent the last renewal of its session
	// that the store accepted, zero before the first.
	LastRenewal time.Time

	// SessionDeadline is when the session ends unless a renewal is accepted first, zero before
	// the first accepted renewal of the incarnation and without a Store. It is Taken plus the
	// time to the deadline on the node's monotonic clock, so a step of the wall clock does not
	// move it away from Taken.
	SessionDeadline time.Time

	// ViewVersion is the Version of the node's latest view, 0 before the first.
	ViewVersion uint64

	// Members are the members of the node's latest view, sorted by ID.
	Members []Member

	// Slots is the group's slot count in the node's latest view, 0 without slots.
	Slots int

	// Slot is the slot this member holds, -1 for none.
	Slot int

	// SlotHolders holds the holder of each slot in the node's latest view, "" for a vacant
	// slot.
	SlotHolders []string

	// Spaces holds the node's spaces sorted by name.
	Spaces []TableSpace
}

// TableSpace is one space of a Table.
type TableSpace struct {
	// Name, Shards and Fenced are the space's from Config.Spaces.
	Name   string
	Shards int
	Fenced bool

	// Rows holds one row per shard, indexed by shard number.
	Rows []TableRow
}

// TableRow is one shard of a TableSpace: where the node routes it and what the node knows about
// it and its Run.
type TableRow struct {
	// Shard is the shard's number in its space.
	Shard int

	// Route is the ID of the member that Node.Route names for the shard's keys, "" when Route
	// finds none.
	Route string

	// Slot is the slot the shard is placed on in a group with slots, -1 without slots or when
	// the shard has no placement.
	Slot int

	// State is where the shard stands on this member, the name of a core.ShardState: "idle",
	// "acquiring", "owned", "degraded", "stopping", "expired", "lost" or "releasing". Every row
	// is "idle" after Close.
	State string

	// Holder and Term are the holder the shard's record names, as far as the node knows, and the
	// record's term: this member's holder from the start of its run, another member's when the
	// node reads the record, and "" once the record names no holder. Holder is "" and Term 0
	// before the node knows anything of the record.
	Holder string
	Term   core.Term

	// Since is when State last changed.
	Since time.Time

	// Running reports that a Run of the shard's latest ownership by this member is in progress
	// or about to restart, from the shard's acquisition until its Run has returned for the last
	// time. After Close it reports a Run that has not returned yet.
	Running bool

	// Stuck reports that the Run was told to stop longer than the renewal deadline ago and has
	// not returned yet.
	Stuck bool

	// Restarts is the number of consecutive returns of Run that the node restarted it after, 0
	// from each acquisition of the shard until Run first returns. A Run that lasted the lease
	// duration starts the count again.
	Restarts int

	// Stop is why the node told the shard's latest Run to stop: "moved", "draining", "expired",
	// "lost" or "closed", "" from each acquisition until it is told to stop. It stays once the
	// shard is released.
	Stop string
}

// Table returns the node's ownership table: a snapshot of the node, its session, its view and
// every shard of its spaces, which the node updates while it carries out each step. Table never
// waits for a step to be carried out. It may be called at any time, from Config.OnEvent too, and
// after Close.
func (n *Node) Table() Table {
	n.tableMu.Lock()
	tb := &n.tab
	now := time.Now()
	t := Table{
		Taken:       now.UTC(),
		Holder:      tb.holder,
		Draining:    tb.draining,
		Closed:      tb.closed,
		SessionLive: tb.live,
		LastRenewal: tb.lastRenewal,
		Slot:        tb.slot,
		Spaces:      make([]TableSpace, len(tb.spaces)),
	}
	for i, sp := range tb.spaces {
		t.Spaces[i] = TableSpace{Name: sp.name, Shards: len(sp.rows), Fenced: sp.fenced, Rows: slices.Clone(sp.rows)}
	}
	view, deadline, closed := tb.view, tb.deadline, tb.closed
	n.tableMu.Unlock()

	if deadline != 0 {
		// now and the epoch both carry monotonic readings: Sub measures on that clock.
		t.SessionDeadline = now.Add(deadline - now.Sub(n.epoch)).UTC()
	}
	t.Member = n.cfg.Self
	if view != nil { // the published view is never changed
		t.ViewVersion, t.Slots = view.Version, view.Slots
		t.Members = make([]Member, len(view.Members))
		for i, m := range view.Members {
			m.Meta = maps.Clone(m.Meta)
			t.Members[i] = m
			if m.ID == n.cfg.Self.ID {
				t.Member = m
			}
		}
		t.SlotHolders = slices.Clone(view.SlotHolders)
	}
	t.Member.Meta = maps.Clone(t.Member.Meta)
	if closed {
		for _, sp := range t.Spaces {
			for id := range sp.Rows {
				r := &sp.Rows[id]
				r.Running = r.Running && n.inRunNow(sp.Name, id)
				r.Stuck = r.Stuck && r.Running
			}
		}
	}
	return t
}

// tableState is what the node keeps for Table, guarded by n.tableMu. The loop updates it while it
// carries out each step, under n.stepMu, so a Table shows the node between two steps; publish,
// the supervisors, a forced Close's halt, the first Drain of a node without a store and Close
// update their parts of it.
type tableState struct {
	holder      string
	draining    bool
	closed      bool
	live        bool
	lastRenewal time.Time     // the SentWall of the latest accepted renewal, in UTC
	deadline    time.Duration // the machine's session deadline, 0 for none
	slot        int           // the slot held, -1 for none
	view        *core.View    // the published view; nil before the first
	routes      *routes       // the routing snapshot the rows' Route and Slot come from
	spaces      []tableSpace  // sorted by name
}

// tableSpace is one space of a tableState.
type tableSpace struct {
	name   string
	fenced bool
	rows   []TableRow
	runs   []core.Term // the term of each shard's latest run, 0 before its first
}

// newTableState returns the table of a node with cfg and holder at the start, when the node has
// taken no step and every shard is idle since epoch.
func newTableState(cfg Config, holder string, epoch time.Time) tableState {
	t := tableState{holder: holder, slot: -1, spaces: make([]tableSpace, len(cfg.Spaces))}
	since := epoch.UTC()
	for i, sp := range cfg.Spaces {
		ts := tableSpace{name: sp.Name, fenced: sp.Fenced, rows: make([]TableRow, sp.Shards), runs: make([]core.Term, sp.Shards)}
		for id := range ts.rows {
			ts.rows[id] = TableRow{Shard: id, Slot: -1, State: core.ShardStateIdle.String(), Since: since}
		}
		t.spaces[i] = ts
	}
	slices.SortFunc(t.spaces, func(a, b tableSpace) int { return strings.Compare(a.name, b.name) })
	return t
}

// space returns the table's space name, nil when the node has none of that name.
func (t *tableState) space(name string) *tableSpace {
	for i := range t.spaces {
		if t.spaces[i].name == name {
			return &t.spaces[i]
		}
	}
	return nil
}

// row returns the row of a shard and the term of its latest run, nil for a shard the node does
// not have.
func (t *tableState) row(space string, shard int) (*TableRow, *core.Term) {
	sp := t.space(space)
	if sp == nil || shard < 0 || shard >= len(sp.rows) {
		return nil, nil
	}
	return &sp.rows[shard], &sp.runs[shard]
}

// tableView notes v, which the node publishes, as the table's view.
func (n *Node) tableView(v *core.View) {
	n.tableMu.Lock()
	defer n.tableMu.Unlock()
	n.tab.view = v
}

// tableDrain notes that the drain has started, for a node without a store, whose table takes
// Draining from no machine.
func (n *Node) tableDrain() {
	n.tableMu.Lock()
	defer n.tableMu.Unlock()
	n.tab.draining = true
}

// tableStep updates the table after the step rec, once its actions are executed and its view and
// routes published. The caller holds n.stepMu. The session fields, the holder, the slot held and
// each row's State, Since and Stuck come from the machine's Status after the step. A row's Holder
// and Term come from the StartRun of its run, a stepped HolderChanged and a Released that is not
// older than them; Running, Restarts and Stop start with the StartRun, Stop takes the reason of
// the run's StopRun, and Running ends with its RunStopped. Route and Slot follow the routing
// snapshot.
func (l *loop) tableStep(rec stepRecord) {
	n := l.n
	snap := n.snap.Load()
	n.tableMu.Lock()
	defer n.tableMu.Unlock()
	t := &n.tab
	st := rec.After
	t.holder = n.holder
	t.draining = st.Draining
	t.live = st.Session == core.SessionStateLive
	t.deadline = st.Deadline
	t.slot = st.Slot
	if _, ok := rec.Input.(core.SessionRenewed); ok && st.Deadline != rec.Before.Deadline {
		t.lastRenewal = rec.SentWall.UTC()
	}

	switch in := rec.Input.(type) {
	case core.HolderChanged:
		if r, _ := t.row(in.Space, in.Shard); r != nil {
			r.Holder, r.Term = in.Holder, in.Term
		}
	case core.Released:
		if r, _ := t.row(in.Space, in.Shard); r != nil && r.Term <= in.Term {
			r.Holder, r.Term = "", in.Term
		}
	case core.RunStopped:
		if r, run := t.row(in.Space, in.Shard); r != nil && *run == in.Term {
			r.Running = false
		}
	}
	for _, act := range rec.Actions {
		switch a := act.(type) {
		case core.StartRun:
			if r, run := t.row(a.Space, a.Shard); r != nil {
				*run = a.Term
				r.Holder, r.Term = n.holder, a.Term
				r.Running, r.Restarts, r.Stop = true, 0, ""
			}
		case core.StopRun:
			if r, run := t.row(a.Space, a.Shard); r != nil && *run == a.Term {
				r.Stop = a.Reason.String()
			}
		}
	}

	for _, sp := range st.Spaces {
		ts := t.space(sp.Name)
		if ts == nil {
			continue
		}
		for id, sh := range sp.Shards {
			if id >= len(ts.rows) {
				break
			}
			r := &ts.rows[id]
			state := sh.State.String()
			// A new machine starts with every shard idle at reading 0: a row idle already keeps
			// the time it became idle.
			if state != r.State || sh.Since != 0 {
				r.State, r.Since = state, n.epoch.Add(sh.Since).UTC()
			}
			r.Stuck = sh.Stuck
		}
	}

	if snap != t.routes {
		t.routes = snap
		for i := range t.spaces {
			ts := &t.spaces[i]
			for id := range ts.rows {
				ts.rows[id].Route, ts.rows[id].Slot = snap.tableRoute(ts.name, id)
			}
		}
	}
}

// tableRoute returns the ID of the member that r routes a shard to, "" for none, and the slot the
// shard is placed on, -1 for none.
func (r *routes) tableRoute(space string, shard int) (string, int) {
	slot := -1
	if a := r.assign[space]; a != nil {
		if s, ok := r.slotOf(a[shard]); ok {
			slot = s
		}
	}
	m, err := r.route(space, shard)
	if err != nil {
		return "", slot
	}
	return m.ID, slot
}

// tableRunReturned notes that the Run of sh returned for the count-th time in a row and is
// restarted. It runs on the shard's supervisor goroutine.
func (n *Node) tableRunReturned(sh *Shard, count int) {
	n.tableMu.Lock()
	defer n.tableMu.Unlock()
	if r, run := n.tab.row(sh.Space, sh.ID); r != nil && *run == sh.Term {
		r.Restarts = count
	}
}

// tableStopped notes that a forced Close stopped sh with err, its stop error. The caller holds
// n.stepMu.
func (n *Node) tableStopped(sh *Shard, err error) {
	var se *stopError
	if !errors.As(err, &se) {
		return
	}
	n.tableMu.Lock()
	defer n.tableMu.Unlock()
	if r, run := n.tab.row(sh.Space, sh.ID); r != nil && *run == sh.Term {
		r.Stop = se.reason
	}
}

// closeTable notes that Close has stopped the node: the node is closed and draining, its session
// is not live, and every row is idle, since the node acts on no shard any more. Table then takes
// Running and Stuck from the Runs that have not returned.
func (n *Node) closeTable() {
	n.tableMu.Lock()
	defer n.tableMu.Unlock()
	t := &n.tab
	t.closed, t.draining, t.live = true, true, false
	now := time.Now().UTC()
	idle := core.ShardStateIdle.String()
	for i := range t.spaces {
		for id := range t.spaces[i].rows {
			if r := &t.spaces[i].rows[id]; r.State != idle {
				r.State, r.Since = idle, now
			}
		}
	}
}

// inRunNow reports whether a Run of shard id of space is in progress.
func (n *Node) inRunNow(space string, id int) bool {
	sp := n.shards[space]
	if sp == nil {
		return false
	}
	n.runMu.Lock()
	defer n.runMu.Unlock()
	return sp.inRun[id]
}
