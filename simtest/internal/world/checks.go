// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// The checks the world registers as faultline invariants, each the Checker.Err of the name
// after "tenure/". g2 is registered only when the run asserts it.
var invariantChecks = []string{"g1", "g2", "g3", "g4", "monotonic"}

// invariantPrefix starts the name of each invariant the world registers.
const invariantPrefix = "tenure/"

// installChecks makes the world's checker and feeds it every observation of the run (SIM-500 to
// SIM-530), then registers its checks as invariants, which faultline runs after every event:
//
//   - Grant: every grant of the store, in the event of the grant.
//   - Acquired: every stepped Acquired with a term, and every stepped HolderChanged that names
//     the driver's own holder with a term. A grant whose reply came late reaches the machine
//     as such a HolderChanged (MCH-550), which then starts the run (a deviation from SIM-500
//     as written, which reported only Acquired and so failed g1 after a late grant).
//   - Reading: the Now of every stepped input, first, before what its actions report.
//   - StartRun, StopRun, Degrade, Restore: the actions of every step, in order; Renewed: the
//     deadline of every ExtendDeadline.
//   - Accepted: every write the fence accepts, in the event the fence accepts it.
//   - Down: the holder of a member whose node crashes, and the holder of an incarnation that a
//     new one of the same member replaces.
//
// Mono, which g2 reads, is the monotonic reading of the node of the holder's member: the part
// of the holder before its slash names the member. The clock is the node's, so a holder that
// crashed or was replaced still reads its node's clock, which keeps counting; Down has taken
// such a holder's runs off the shards, so g2 asks for no reading of it in practice. A holder of
// no member reads the global time.
func (w *World) installChecks() {
	c := check.New(check.Config{Now: w.sim.Now, Mono: w.holderMono, Spaces: w.sc.Spaces, AssertG2: w.g2})
	w.checker = c

	w.storeSrv.onGrant = c.Grant
	w.fenceSrv.onAccepted = c.Accepted
	w.drivers.onStep = func(d *driver, in core.Input, acts []core.Action) { observeStep(c, d.holder, in, acts) }
	w.drivers.onRetire = func(d *driver) { c.Down(d.holder) }
	w.sim.OnCrash(func(n *kernel.Node) {
		if m := w.memberOf(n); m != nil && m.holder != "" && m.phase == Active {
			c.Down(m.holder)
		}
	})

	// A planted invariant that always fails, for a scratch check of the failing-seed artifact.
	w.fw.Invariant(invariantPrefix+"scratch", func() error { return fmt.Errorf("scratch: always fails") })
	for _, name := range invariantChecks {
		if name == "g2" && !w.g2 {
			continue
		}
		w.fw.Invariant(invariantPrefix+name, func() error { return c.Err(name) })
	}
}

// holderMono is the checker's Mono: the monotonic reading of the node of holder's member, which
// the part of holder before its slash names, whether the holder's incarnation is the member's
// present one or not and whether its node is up or not; the global time for a holder of no
// member.
func (w *World) holderMono(holder string) time.Duration {
	name := sessionMember(holder)
	for _, m := range w.members {
		if m.Name == name {
			return m.Mono()
		}
	}
	return time.Duration(w.sim.Now())
}

// observeStep reports one step of holder's machine to c: the input's reading, the acquisition
// the input tells of, if any, then what the actions report, in order.
func observeStep(c *check.Checker, holder string, in core.Input, acts []core.Action) {
	c.Reading(holder, inputNow(in))
	switch in := in.(type) {
	case core.Acquired:
		if in.Term != 0 {
			c.Acquired(holder, in.Space, in.Shard, in.Term)
		}
	case core.HolderChanged:
		if in.Holder == holder && in.Term != 0 {
			c.Acquired(holder, in.Space, in.Shard, in.Term)
		}
	}
	for _, a := range acts {
		switch a := a.(type) {
		case core.StartRun:
			c.StartRun(holder, a.Space, a.Shard, a.Term)
		case core.StopRun:
			c.StopRun(holder, a.Space, a.Shard, a.Term)
		case core.Degrade:
			c.Degrade(holder, a.Space, a.Shard, a.Term)
		case core.Restore:
			c.Restore(holder, a.Space, a.Shard, a.Term)
		case core.ExtendDeadline:
			c.Renewed(holder, a.Deadline)
		}
	}
}

// inputNow returns the Now of a stepped input.
func inputNow(in core.Input) time.Duration {
	switch v := in.(type) {
	case core.Tick:
		return v.Now
	case core.ViewChanged:
		return v.Now
	case core.SessionRenewed:
		return v.Now
	case core.SessionFailed:
		return v.Now
	case core.SessionClosed:
		return v.Now
	case core.Acquired:
		return v.Now
	case core.AcquireFailed:
		return v.Now
	case core.Released:
		return v.Now
	case core.RunStopped:
		return v.Now
	case core.HolderChanged:
		return v.Now
	case core.SlotClaimed:
		return v.Now
	case core.SlotReleased:
		return v.Now
	case core.Drain:
		return v.Now
	}
	panic(fmt.Sprintf("world: checks: unknown input %T", in))
}
