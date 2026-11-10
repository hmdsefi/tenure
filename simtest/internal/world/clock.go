// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"errors"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/hmdsefi/faultline/kernel"
)

// Mono returns the monotonic reading of a node: what the node's own clock has counted since
// the start of the run. The clock runs at the node's drift, which a fault may change, and it
// keeps counting while the node is down, so a reading never goes back.
func Mono(n *kernel.Node) time.Duration {
	return time.Duration(n.LocalTime())
}

// clocks watches the clock of every node of a simulation and keeps the mono timers: timers set
// for a reading of a node's monotonic clock (SIM-130).
//
// The kernel schedules a node's timer at a global time, which it works out from the node's
// drift when the timer is set, and it keeps that time when the drift changes later. A kernel
// timer that waits for a session deadline would then fire early or late on the clock it was
// meant for. So clocks looks at every node after every event, in NodeID order, and sets the
// pending timers of a node whose drift changed again, in the order they were set, at the new
// drift.
//
// A drift should change inside an event, as a fault changes it. A change made between two
// events, by the body of a test, is seen after the next event only, which may be a timer that
// is late by then.
//
// The look also checks that every clock moved by its drift alone since the last look: the
// kernel's JumpClock is never used (SIM-120), and a clock that jumped all the same, as a
// replayed fault schedule can make it, fails the run with the setup error of a clock-jump rule.
type clocks struct {
	sim   *kernel.Sim
	nodes []*nodeClock // by NodeID - 1: every node of the simulation added before the last look
	// off stops the look after every event, re-arming and the jump check with it. It is for
	// the tests that show what the look is for, which set it before the run: nothing else
	// sets it.
	off bool
}

// nodeClock is one node's clock as the last look found it, and the node's pending timers.
type nodeClock struct {
	node    *kernel.Node
	at      kernel.Time  // the global time of the last look
	reading kernel.Time  // the node's local time at that look
	ppm     int32        // the node's drift at that look
	timers  []*monoTimer // the pending timers, in the order they were set
}

// monoTimer is a mono timer: it runs its function once, in an event of its node, when the
// node's monotonic clock reads its reading or a few nanoseconds later, unless it is cancelled
// or its node crashes first. A node that is paused at that reading runs it when it resumes.
type monoTimer struct {
	clock *nodeClock
	at    time.Duration  // the reading it is set for
	label string         // the label of its kernel event
	fn    func()         // what it runs when it fires
	event kernel.EventID // its pending kernel event; 0 once it fired, was cancelled or was dropped
}

// newClocks returns the clocks of sim: it looks at the clocks after every event, and drops the
// timers of a node that crashes.
func newClocks(sim *kernel.Sim) *clocks {
	c := &clocks{sim: sim}
	sim.OnEvent(c.look)
	sim.OnCrash(c.drop)
	return c
}

// track takes a first look at the nodes added since the last look.
func (c *clocks) track() {
	for {
		n := c.sim.Node(kernel.NodeID(len(c.nodes) + 1)) //nolint:gosec // a simulation has few nodes
		if n == nil {
			return
		}
		c.nodes = append(c.nodes, &nodeClock{node: n, at: c.sim.Now(), reading: n.LocalTime(), ppm: n.Drift()})
	}
}

// of returns the clock of n, a node of the simulation.
func (c *clocks) of(n *kernel.Node) *nodeClock {
	c.track()
	return c.nodes[n.ID()-1]
}

// look runs after every event. It walks the nodes in NodeID order. A node whose clock moved by
// anything but its drift since the last look fails the run. A node whose drift changed gets its
// pending timers set again, in the order they were set.
//
// The kernel counts a drifting clock in whole nanoseconds from the last change of the drift,
// which a look cannot see when the new drift equals the old one: a reading may be a nanosecond
// ahead of what the last look predicts. When the drift changed since the last look, the change
// happened somewhere in between, and the reading lies between what the old drift and the new
// one predict, give or take the rounding. Jumps of those few nanoseconds pass.
func (c *clocks) look() {
	if c.off {
		return
	}
	c.track()
	now := c.sim.Now()
	for _, nc := range c.nodes {
		elapsed := now.Sub(nc.at)
		reading, ppm := nc.node.LocalTime(), nc.node.Drift()
		lo := nc.reading.Add(elapsed + gain(elapsed, nc.ppm))
		hi := lo + 1
		if ppm != nc.ppm {
			other := nc.reading.Add(elapsed + gain(elapsed, ppm))
			lo, hi = min(lo, other)-1, max(lo, other)+2
		}
		if reading < lo || reading > hi {
			c.sim.Fail(errors.New(clockJumpText))
			return
		}
		changed := ppm != nc.ppm
		nc.at, nc.reading, nc.ppm = now, reading, ppm
		if !changed {
			continue
		}
		for _, t := range nc.timers {
			nc.node.Cancel(t.event)
			t.arm()
		}
	}
}

// gain returns what a clock with a drift of ppm parts per million counts over the global time d
// beyond d itself, rounded down as the kernel rounds it. d is not negative.
func gain(d time.Duration, ppm int32) time.Duration {
	whole, rest := int64(d)/million, int64(d)%million
	part := rest * int64(ppm) // below 10¹² either way
	g := part / million
	if part%million < 0 {
		g-- // toward negative infinity
	}
	return time.Duration(whole*int64(ppm) + g)
}

// set sets a timer that runs fn once when the monotonic clock of n reads at, and returns it. A
// reading that has passed runs fn in an event of its own at the present time. label is the
// label of the timer's kernel events. On a node that is down, set schedules nothing: the timer
// it returns is not pending, since every event of the node would die with it anyway.
func (c *clocks) set(n *kernel.Node, at time.Duration, label string, fn func()) *monoTimer {
	t := &monoTimer{clock: c.of(n), at: at, label: label, fn: fn}
	t.arm()
	if t.event != 0 {
		t.clock.timers = append(t.clock.timers, t)
	}
	return t
}

// arm schedules the kernel event of t at the node's present drift. The kernel rounds the wait
// up to a whole nanosecond of global time, so the event is never early on the node's clock.
func (t *monoTimer) arm() {
	n := t.clock.node
	t.event = n.After(t.at-Mono(n), t.label, t.fire)
}

// fire runs the timer.
func (t *monoTimer) fire() {
	t.forget()
	t.fn()
}

// forget takes t off its node's pending timers.
func (t *monoTimer) forget() {
	t.event = 0
	if i := slices.Index(t.clock.timers, t); i >= 0 {
		t.clock.timers = slices.Delete(t.clock.timers, i, i+1)
	}
}

// target returns the reading t is set for.
func (t *monoTimer) target() time.Duration { return t.at }

// pending reports whether t has yet to fire.
func (t *monoTimer) pending() bool { return t.event != 0 }

// cancel stops t from firing and reports whether it was pending.
func (t *monoTimer) cancel() bool {
	if t.event == 0 {
		return false
	}
	t.clock.node.Cancel(t.event)
	t.forget()
	return true
}

// drop cancels every pending timer of n. The kernel never runs the events of a node that
// crashed, and OnCrash calls drop for it; a member that exits drops its timers the same way.
func (c *clocks) drop(n *kernel.Node) {
	nc := c.of(n)
	for _, t := range nc.timers {
		n.Cancel(t.event)
		t.event = 0
	}
	nc.timers = nil
}

// symmetric draws a value from [-bound, bound], each with the same chance, with one Uint64N of
// r. bound is not negative.
func symmetric(r *rand.Rand, bound int64) int64 {
	// 2·bound + 1 values, counted without a sign so that the largest bound still fits; the
	// subtraction wraps around into [-bound, bound].
	n := 2*uint64(bound) + 1                   //nolint:gosec // bound is not negative
	return int64(r.Uint64N(n) - uint64(bound)) //nolint:gosec // see above
}
