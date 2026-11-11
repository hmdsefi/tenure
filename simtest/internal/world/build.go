// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// The names of the two nodes that are not members. Each is also the name of the role that holds
// the node alone, for the fault rules that target it.
const (
	storeName = "store"
	fenceName = "fence"
)

// The labels of the random streams of the world. The streams of a member end with its name.
const (
	streamClock       = "tenure/clock"
	streamIncarnation = "tenure/incarnation/"
	streamWall        = "tenure/wall/"
)

// wallSteps is the number of times the wall clock of each member and replacement steps in a
// scenario with WallSteps.
const wallSteps = 3

// Phase says where a member is in its life.
type Phase uint8

const (
	// Standby is the phase of a replacement that waits for its turn: its node runs, but it has
	// no holder, and its boots start nothing.
	Standby Phase = iota
	// Active is the phase of a member that takes part: every boot of its node starts a new
	// incarnation.
	Active
	// Exited is the phase of a member that has handed its shards over and left for good: its
	// boots start nothing.
	Exited
)

// String returns "standby", "active" or "exited", and "Phase(7)" for a value that is none.
func (p Phase) String() string {
	switch p {
	case Standby:
		return "standby"
	case Active:
		return "active"
	case Exited:
		return "exited"
	default:
		return "Phase(" + strconv.Itoa(int(p)) + ")"
	}
}

// Member is one member of a world, or the replacement of one.
type Member struct {
	// Name is the name of the member's node, which is also the member's ID: "m3" for the third
	// member and "m3-r" for its replacement.
	Name string
	// Node is the member's node.
	Node *kernel.Node
	// Index is the member's number, from 1: the 3 of "m3" and of "m3-r".
	Index int
	// Replacement reports whether the member is the replacement of the member with its Index.
	Replacement bool

	phase  Phase
	holder string        // the holder of the present incarnation; "" before the first
	wall   time.Duration // the offset of the wall clock from the monotonic one
	// draining reports that the member was told to drain (recover.go): it hands its shards
	// over and exits instead of recovering, and a boot of its node exits it at once.
	draining bool
	// recoveries counts the incarnations that started after a lost session (recover.go).
	recoveries int
}

// Phase returns the member's phase.
func (m *Member) Phase() Phase { return m.phase }

// Holder returns the holder string of the member's present incarnation: the member's name, a
// slash and 32 hexadecimal digits. A member that is down keeps the holder of the incarnation
// that crashed until its node boots again. A member that never started an incarnation, as a
// replacement on standby, has "".
func (m *Member) Holder() string { return m.holder }

// Mono returns the reading of the member's monotonic clock.
func (m *Member) Mono() time.Duration { return Mono(m.Node) }

// Wall returns the reading of the member's wall clock: the start of virtual time, plus the
// monotonic reading, plus the offset of the wall clock, which the scenario's WallSkew draws and
// its WallSteps change.
func (m *Member) Wall() time.Time { return kernel.Epoch.Add(m.Mono() + m.wall) }

// World is one simulated world: a store, a fence and the members of a Scenario, on the nodes of
// one run of faultline. Build returns it.
type World struct {
	fw  *faultline.World
	sim *kernel.Sim
	sc  Scenario    // the scenario with its defaults; its Faults are the world's own copy
	end kernel.Time // the end of the run

	bound    time.Duration // B
	quiet    time.Duration // the quiet window at the end of the run; 0 when nothing disturbs the run
	recovery kernel.Time   // R
	g2       bool          // whether the run asserts G2

	store   *kernel.Node
	fence   *kernel.Node
	members []*Member // m1 to mN, then their replacements in the same order
	clocks  *clocks

	// storeSrv and fenceSrv are the state of the store's node and of the fence's, which
	// survives their crashes (store.go, fence.go).
	storeSrv *storeServer
	fenceSrv *fenceServer

	// The hooks through which the parts of the world give the nodes their behaviour. Build
	// installs them before the first event, in installHooks; a nil hook does nothing.
	//
	// bootStore and bootFence run at every boot of the store's node and of the fence's: they
	// install the node's handler and set its mono timers again. What the two nodes keep through
	// a crash lives in the World, not in these functions.
	bootStore kernel.BootFunc
	bootFence kernel.BootFunc
	// startMember runs when an incarnation of an active member starts, after its holder was
	// drawn and its tenure.boot record emitted: it starts the driver of the incarnation.
	// Everything the driver schedules on the member's node dies with the node, and the
	// member's mono timers are dropped then.
	startMember func(m *Member)

	// drivers holds the drivers of the members' incarnations (driver.go).
	drivers *drivers

	// sessions keeps the session ends the members owe a recovery for (recover.go).
	sessions *sessionWatch

	// rollout keeps what the final tenure/rollout judges (rollout.go); nil without a Rollout.
	rollout *rolloutWatch

	// checker decides the invariants over what the store, the drivers and the fence observe
	// (checks.go).
	checker *check.Checker

	// outage watches the scenario's store outage for tenure/outage (outage.go); nil without one.
	outage *outageWatch

	// finalsOff turns the final checks into checks that pass. The tests of the parts of the world
	// that disturb it by hand, after the recovery time, set it; the scenarios never do.
	finalsOff bool
}

// Build builds the world of sc in w, the world of one run, and returns it. It must be called
// once, from the function that faultline.Run runs for each run, before the simulation advances.
// A scenario that SetupError rejects fails the seed at once with the error, through
// w.T().Fatalf.
//
// The nodes are added in a fixed order, so that a node has the same ID in every run: the store,
// the fence, the members m1 to mN and, when the scenario has a Rollout, their replacements m1-r
// to mN-r. Members and replacements are servers, which a fault rule without a target picks
// from; the store and the fence are not. Three roles name the targets of a rule: "store",
// "fence" and "members", which holds the members and replacements that are active when the
// rule picks.
//
// Every node has a monotonic clock, which runs at a drift drawn from [-Rho, Rho] parts per
// million and keeps counting while the node is down. Members and replacements also have a wall
// clock, which is ahead of the monotonic one by an offset drawn from [-WallSkew, WallSkew]; the
// wall clocks of the store and the fence have no offset. With WallSteps, the wall clock of each
// member and replacement steps three times, each step at a time drawn from the start of the run
// to the recovery time and of a size drawn from [-WallSteps, WallSteps].
//
// Members start active and replacements on standby. Every boot of an active member's node
// starts an incarnation with a new holder string.
//
// Build plans a copy of the scenario's Faults and never changes the scenario, so one value can
// describe every run. The copy's Quiet, when it is 0, is the default that SetupError describes.
// Build emits one global tenure.bound record with the rule, the recovery time, the bound and
// whether the run asserts that no two members own a shard at once.
//
// Every StartRun of a member's machine starts a run that raises the fence of its shard and
// writes to it in its term. A checker observes the store, the drivers and the fence, and Build
// registers its checks as the invariants tenure/g1, tenure/g2 (only when the run asserts it),
// tenure/g3, tenure/g4 and tenure/monotonic, which run after every event. It also registers two
// final checks, which run once after the last event, in this order: tenure/g5, unless the
// scenario has NoLiveness, checks that every shard has an owner at the recovery time plus the
// bound; tenure/converged checks that the members that are active and up agree on where each
// shard goes, that this is where the balanced placement over those members puts it, and that
// each shard with an assignee is owned by that member alone. A scenario with EndAfter also gets
// the final tenure/recovered, after those two: every end of a session of a member that was not
// told to drain is followed by exactly one recovery of the member, whose new session the store
// opened. A scenario with a Rollout gets the final tenure/rollout, after those: each
// replacement took the shards of the slot it claimed once, and no replaced member started a
// shard after it was told to drain. A scenario with an Outage cuts the store off from every
// node at Outage.At and heals the network Outage.For later, which is its recovery time, and gets
// the final tenure/outage, last: an unfenced shard stops at its session deadline, and a fenced
// one degrades and runs on through the outage until a sign that it is lost, and then restores
// or is lost.
//
// With a Rollout, the replacement mi-r is activated at Start + (i−1)·Gap, which starts its
// first incarnation, and mi is told to drain Surge later.
//
// A member whose session is lost starts a new incarnation, cause "recover", once its machine is
// done and every run has returned, in the event of the last of them. A member told to drain
// (Drain) exits once its machine is done, and never boots again.
func Build(w *faultline.World, sc Scenario) *World {
	w.T().Helper()
	st, err := newSetup(sc, w.End())
	if err != nil {
		w.T().Fatalf("%v", err)
	}
	world := &World{
		fw:       w,
		sim:      w.Sim,
		sc:       st.sc,
		end:      w.End(),
		bound:    st.bound,
		quiet:    st.quiet,
		recovery: st.recovery,
		g2:       st.sc.AssertsG2(),
		clocks:   newClocks(w.Sim),
	}
	world.addNodes()
	world.addRoles()
	world.planFaults()
	world.planWallSteps()
	world.installHooks()
	world.planSetSlots()
	world.installChecks()
	world.installFinals()
	world.installRecovered()
	world.planRollout()
	world.installOutage()
	world.emitBound()
	return world
}

// installHooks gives the nodes their behaviour: the store and the fence serve their models
// (store.go, fence.go), and each incarnation of an active member runs a driver (driver.go).
func (w *World) installHooks() {
	w.storeSrv = newStoreServer(w)
	w.bootStore = w.storeSrv.boot
	w.fenceSrv = newFenceServer(w)
	w.bootFence = w.fenceSrv.boot
	w.drivers = newDrivers(w)
	w.startMember = w.drivers.start
}

// Checker returns the world's checker, which every observation of the run feeds: its checks
// g1, g2 (when the run asserts it), g3, g4 and monotonic are the invariants tenure/g1 and so on
// that Build registers, and its Liveness is for the final checks.
func (w *World) Checker() *check.Checker { return w.checker }

// Bound returns the scenario's bound, as ComputeBound gives it: the longest time a shard may
// stay without an owner once the world is left alone.
func (w *World) Bound() time.Duration { return w.bound }

// Recovery returns the time from which the world is left alone: no fault starts after it and no
// wall clock steps. With random faults it is where faultline's recovery window starts. It is
// the start of the run for a scenario that nothing disturbs.
func (w *World) Recovery() kernel.Time { return w.recovery }

// AssertsG2 reports whether the run asserts that no two members own a shard at once, as the
// scenario's AssertsG2 does.
func (w *World) AssertsG2() bool { return w.g2 }

// Scenario returns the scenario of the world, with its defaults. Its Faults are the copy the
// world planned.
func (w *World) Scenario() Scenario { return w.sc }

// Store returns the store's node.
func (w *World) Store() *kernel.Node { return w.store }

// Fence returns the fence's node.
func (w *World) Fence() *kernel.Node { return w.fence }

// Members returns the members in the order of their nodes: m1 to mN, then their replacements
// m1-r to mN-r. The caller must not change the slice.
func (w *World) Members() []*Member { return w.members }

// Wall returns the reading of a node's wall clock: that of the member whose node n is and, for
// the store and the fence, whose wall clocks have no offset, the start of virtual time plus the
// monotonic reading.
func (w *World) Wall(n *kernel.Node) time.Time {
	if m := w.memberOf(n); m != nil {
		return m.Wall()
	}
	return kernel.Epoch.Add(Mono(n))
}

// memberOf returns the member whose node n is, or nil when n is the store or the fence.
func (w *World) memberOf(n *kernel.Node) *Member {
	// The members' nodes follow the store's and the fence's, in the order of w.members.
	i := int(n.ID()) - int(w.fence.ID()) - 1
	if i < 0 || i >= len(w.members) {
		return nil
	}
	return w.members[i]
}

// setTimer sets a mono timer on n that runs fn once when the monotonic clock of n reads at, or
// a few nanoseconds later, whatever the drift of n does until then (SIM-130). A crash of n
// drops it, and on a node that is down it is never pending. label names its kernel events.
func (w *World) setTimer(n *kernel.Node, at time.Duration, label string, fn func()) *monoTimer {
	return w.clocks.set(n, at, label, fn)
}

// addNodes adds the nodes in their fixed order (SIM-100) and draws their clocks from one stream
// as it goes (SIM-110): for each node its drift and, for a member or a replacement, its wall
// offset after that. A value whose range is a single point, as a drift with a Rho of 0, takes
// no draw.
func (w *World) addNodes() {
	r := w.sim.Rand(streamClock)
	clock := func() kernel.NodeOption {
		ppm := int32(0)
		if w.sc.Rho > 0 {
			ppm = int32(symmetric(r, int64(w.sc.Rho))) //nolint:gosec // within [-Rho, Rho]
		}
		return kernel.WithClock(0, ppm)
	}
	w.store = w.sim.AddNode(storeName, func(n *kernel.Node) {
		if w.bootStore != nil {
			w.bootStore(n)
		}
	}, clock())
	w.fence = w.sim.AddNode(fenceName, func(n *kernel.Node) {
		if w.bootFence != nil {
			w.bootFence(n)
		}
	}, clock())

	add := func(index int, replacement bool) {
		m := &Member{Name: "m" + strconv.Itoa(index), Index: index, Replacement: replacement, phase: Active}
		if replacement {
			m.Name += "-r"
			m.phase = Standby
		}
		m.Node = w.fw.AddServer(m.Name, func(*kernel.Node) { w.boot(m) }, clock())
		if w.sc.WallSkew > 0 {
			m.wall = time.Duration(symmetric(r, int64(w.sc.WallSkew)))
		}
		w.members = append(w.members, m)
	}
	for i := 1; i <= w.sc.Members; i++ {
		add(i, false)
	}
	if w.sc.Rollout != nil {
		for i := 1; i <= w.sc.Members; i++ {
			add(i, true)
		}
	}
	w.clocks.track()
}

// addRoles registers the roles that the rules of a fault plan name as their targets.
func (w *World) addRoles() {
	w.fw.Role(storeName, w.storeRole)
	w.fw.Role(fenceName, w.fenceRole)
	w.fw.Role(roleMembers, w.membersRole)
}

// storeRole returns the nodes of the role "store": the store's.
func (w *World) storeRole() []kernel.NodeID { return []kernel.NodeID{w.store.ID()} }

// fenceRole returns the nodes of the role "fence": the fence's.
func (w *World) fenceRole() []kernel.NodeID { return []kernel.NodeID{w.fence.ID()} }

// membersRole returns the nodes of the role "members": the members and replacements that are
// active, in NodeID order. A replacement on standby and a member that exited are left out.
func (w *World) membersRole() []kernel.NodeID {
	ids := make([]kernel.NodeID, 0, len(w.members))
	for _, m := range w.members {
		if m.phase == Active {
			ids = append(ids, m.Node.ID())
		}
	}
	return ids
}

// planFaults plans the scenario's random faults. The plan is a copy, whose Quiet is the quiet
// window of the setup: the function that builds the world runs once for every run, a passing
// seed may run twice, and the caller's value must stay as it is.
//
// Link rules run apart, in twoWayLinks, so that each link fault slows both directions of its
// link; the other rules then run as a Random of their own, and the copy stays the world's
// Scenario().Faults, all rules in order. A plan without link rules is the copy itself.
func (w *World) planFaults() {
	if w.sc.Faults == nil {
		return
	}
	plan := &fault.Random{
		Rules:   slices.Clone(w.sc.Faults.Rules),
		MaxDown: w.sc.Faults.MaxDown,
		Quiet:   w.quiet,
	}
	w.sc.Faults = plan
	others, links := splitLinks(plan.Rules)
	if len(links) == 0 {
		w.fw.Plan(plan)
		return
	}
	w.fw.Plan(&fault.Random{Rules: others, MaxDown: plan.MaxDown, Quiet: plan.Quiet})
	w.fw.Plan(&twoWayLinks{
		plan:  &fault.Random{Rules: links, MaxDown: plan.MaxDown},
		until: w.end - kernel.Time(w.quiet),
	})
}

// planWallSteps schedules the wall-clock steps of a scenario with WallSteps (SIM-110). Each
// member and replacement, in node order, draws from its own stream tenure/wall/<name>, for each
// of its three steps, the time of the step, uniform in [0, R), then its size, uniform in
// [-WallSteps, WallSteps]. WallSteps makes the run disturbed, so R is the start of the quiet
// window and is positive. A step is a global event, not one of the node: a wall clock steps
// whether its node is up or not, and each step emits tenure.wall_step.
func (w *World) planWallSteps() {
	if w.sc.WallSteps <= 0 {
		return
	}
	window := int64(w.recovery)
	for _, m := range w.members {
		label := streamWall + m.Name
		r := w.sim.Rand(label)
		for range wallSteps {
			at := kernel.Time(r.Int64N(window))
			delta := time.Duration(symmetric(r, int64(w.sc.WallSteps)))
			w.sim.At(at, label, func() {
				m.wall += delta
				w.emitWallStep(m, delta)
			})
		}
	}
}

// boot runs at every boot of a member's node. Only an active member starts an incarnation
// (SIM-150): a replacement on standby waits for its turn, and a member that exited stays out.
// A member told to drain whose node crashed exits when the node boots again (recover.go).
func (w *World) boot(m *Member) {
	if m.phase != Active {
		return
	}
	if m.draining {
		w.exit(m)
		return
	}
	w.incarnate(m, bootCauseBoot)
}

// incarnate starts an incarnation of m, which is active and whose node is up (SIM-140): it draws
// the new holder, m's name, a slash and 32 hex digits, from two Uint64 draws of
// tenure/incarnation/<name>, emits tenure.boot with cause, and starts the driver. The cause is
// "boot" when the node booted; a member that starts an incarnation for another reason, as a
// recovery or an activation, passes its own.
func (w *World) incarnate(m *Member, cause string) {
	r := w.sim.Rand(streamIncarnation + m.Name)
	hi, lo := r.Uint64(), r.Uint64()
	m.holder = fmt.Sprintf("%s/%016x%016x", m.Name, hi, lo)
	w.emitBoot(m, cause)
	if w.startMember != nil {
		w.startMember(m)
	}
}
