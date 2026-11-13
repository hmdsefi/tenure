// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The record kinds the session tests read, beyond those of steady_test.go (SIM §8).
const (
	kindSession = "tenure.session"
	kindGrant   = "tenure.grant"
	kindRun     = "tenure.run"
	kindExit    = "tenure.exit"
)

// TestSessionEnds runs SIM-600's session-end row under each rule (AT-SIM-31): the store ends
// every session 40 s after it opened, so each member's first session ends once, at about 40 s,
// and each member recovers with a new holder (SIM-145). Every invariant but G2 holds after
// every event; G2 is not asserted, because the store may grant an ended session's shard while
// its holder, not yet aware, still owns it inside its deadline. The finals tenure/g5,
// tenure/converged and tenure/recovered pass, and m1 recovers exactly once. With
// FAULTLINE_CHECK_DETERMINISM=1, every seed runs twice with equal trace hashes.
func TestSessionEnds(t *testing.T) {
	runRow(t, rowNamed(t, "TestSessionEnds"), func(fw *faultline.World, w *world.World) {
		m1 := w.Members()[0]
		fw.Final("test/m1_recovers_once", func() error {
			if got := m1.Recoveries(); got != 1 {
				return fmt.Errorf("m1 recovered %d times, want 1", got)
			}
			return nil
		})
	})
}

// TestSessionEndsRecords checks AT-SIM-46 in TestSessionEnds's world, under each rule, with
// every record kept. For each member, in the order of the trace:
//
//  1. the store ends the member's first session: tenure.session op=end cause=end_after;
//  2. the member steps SessionFailed with cause lost;
//  3. every run of the first incarnation stops: tenure.run op=stopped for each op=start;
//  4. tenure.boot cause=recover names a new holder, after the last of those stops, in the
//     event of the old incarnation's last input (SIM-145: in one event);
//  5. the store opens the new holder's session: tenure.session op=open;
//  6. the store grants a shard to the new holder: tenure.grant.
//
// The steps were written for this task from the issue and the design note, since the original
// acceptance-test text is lost.
func TestSessionEndsRecords(t *testing.T) {
	r := rowNamed(t, "TestSessionEnds")
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			opts := r.options()
			opts.Seeds = 2
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				fw.Final("test/records", func() error {
					for _, m := range w.Members() {
						if err := recoveryOrder(fw.Sim.Records(), w.Store().ID(), m); err != nil {
							return err
						}
					}
					return nil
				})
			}))
		})
	}
}

// recoveryOrder checks AT-SIM-46's order for member m over a full trace, store being the
// store's node.
func recoveryOrder(recs []kernel.Record, store kernel.NodeID, m *world.Member) error {
	id := m.Node.ID()
	const (
		wantEnd = iota
		wantFailed
		wantRecover
		wantOpen
		wantGrant
		finished
	)
	step := wantEnd
	var first, second string
	running := 0              // the runs of the first incarnation started and not stopped
	var lastInput kernel.Time // the time of the latest input of the first incarnation
	for _, r := range recs {
		switch {
		case r.Node == id && r.Kind == kindBoot && first == "":
			first = attr(r, "holder")
		case r.Node == id && r.Kind == kindInput && step <= wantRecover:
			lastInput = r.At
		case r.Node == id && r.Kind == kindRun && step <= wantRecover:
			switch attr(r, "op") {
			case "start":
				running++
			case "stopped":
				running--
			}
		}
		switch step {
		case wantEnd:
			if r.Node == store && r.Kind == kindSession && attr(r, "op") == "end" && attr(r, "holder") == first {
				if c := attr(r, "cause"); c != "end_after" {
					return fmt.Errorf("%s: session %s ended with cause %q, want end_after", m.Name, first, c)
				}
				step = wantFailed
			}
		case wantFailed:
			if r.Node == id && r.Kind == kindInput && attr(r, "type") == "SessionFailed" && attr(r, "cause") == "lost" {
				step = wantRecover
			}
		case wantRecover:
			if r.Node == id && r.Kind == kindBoot {
				if c := attr(r, "cause"); c != "recover" {
					return fmt.Errorf("%s at %v: boot with cause %q after the loss, want recover", m.Name, r.At, c)
				}
				if running != 0 {
					return fmt.Errorf("%s at %v: recovered with %d runs of %s not stopped", m.Name, r.At, running, first)
				}
				if lastInput != r.At {
					return fmt.Errorf("%s at %v: recovered after its last input at %v, want the same event", m.Name, r.At, lastInput)
				}
				second = attr(r, "holder")
				if second == first || second == "" {
					return fmt.Errorf("%s at %v: recovered with holder %q, want a new one", m.Name, r.At, second)
				}
				step = wantOpen
			}
		case wantOpen:
			if r.Node == store && r.Kind == kindSession && attr(r, "op") == "open" && attr(r, "holder") == second {
				step = wantGrant
			}
		case wantGrant:
			if r.Node == store && r.Kind == kindGrant && attr(r, "holder") == second {
				step = finished
			}
		}
	}
	names := [...]string{
		"the end of the first session", "SessionFailed{lost}", "tenure.boot cause=recover",
		"the open of the new session", "a grant to the new holder", "",
	}
	if step != finished {
		return fmt.Errorf("%s: no %s in the trace (first holder %q, new holder %q)", m.Name, names[step], first, second)
	}
	return nil
}

// drainAt is when TestDrainExit drains m1; crashAt is when it crashes m1's node, which it
// restarts a second later.
const (
	drainAt = 20 * time.Second
	crashAt = 40 * time.Second
)

// TestDrainExit drains m1 of five members at 20 s, under each rule, and crashes and restarts
// m1's node at 40 s (SIM-350, SIM-380). m1 hands its shards over, closes its session and
// exits: its phase is exited, the trace has its tenure.exit after the store closed its
// session, and m1 emits no tenure record after it, not even when its node boots again. The other four
// members converge on every shard, and every invariant and final holds. The drain and the crash
// are not part of the world's plan, so the recovery time stays at 0; the liveness final would
// judge the ownership of the shards at the bound, about 22 s, while m1 may still be handing them
// over, so it is left out, as in TestDrainWhileDown. tenure/converged still checks that the four
// own every shard at the end.
func TestDrainExit(t *testing.T) {
	r := row{
		name: "TestDrainExit", sc: world.Scenario{StopDelay: time.Second, NoLiveness: true},
		seeds: 5, duration: 60 * time.Second, net: defaultNet, g2: true,
	}
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			opts := r.options()
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				m1 := w.Members()[0]
				fw.Sim.At(kernel.Time(drainAt), "test/drain", func() { w.Drain(m1) })
				fw.Sim.At(kernel.Time(crashAt), "test/crash", m1.Node.Crash)
				fw.Sim.At(kernel.Time(crashAt+time.Second), "test/restart", m1.Node.Restart)
				fw.Final("test/exit", func() error {
					if m1.Phase() != world.Exited {
						return fmt.Errorf("m1 is %v at the end, want exited", m1.Phase())
					}
					return exitOrder(fw.Sim.Records(), w.Store().ID(), m1)
				})
			}))
		})
	}
}

// exitOrder checks the trace of a drained member m: one boot, the store's close of its
// session, then one tenure.exit with cause drain and m's holder, and no tenure record of m
// after it (the kernel's and the network's records of the node, as its crash, may follow), and
// no message of the store to m after it: the store pushes to active members only.
func exitOrder(recs []kernel.Record, store kernel.NodeID, m *world.Member) error {
	id := m.Node.ID()
	boots, closed, exited := 0, false, false
	for _, r := range recs {
		if r.Node == store && r.Kind == kindSession && attr(r, "op") == "close" && attr(r, "holder") == m.Holder() {
			closed = true
		}
		if exited && r.Node == store && r.Kind == "net.send" && attr(r, "to") == m.Name {
			return fmt.Errorf("%s at %v: the store sent %q after its exit", m.Name, r.At, attr(r, "payload"))
		}
		if r.Node != id || !strings.HasPrefix(r.Kind, "tenure.") {
			continue
		}
		if exited {
			return fmt.Errorf("%s at %v: %s after its exit", m.Name, r.At, r.Kind)
		}
		switch r.Kind {
		case kindBoot:
			boots++
		case kindExit:
			if !closed {
				return fmt.Errorf("%s at %v: exit before the store closed its session", m.Name, r.At)
			}
			if attr(r, "holder") != m.Holder() || attr(r, "cause") != "drain" {
				return fmt.Errorf("%s at %v: exit holder=%q cause=%q, want %q and drain",
					m.Name, r.At, attr(r, "holder"), attr(r, "cause"), m.Holder())
			}
			exited = true
		}
	}
	if boots != 1 || !exited {
		return fmt.Errorf("%s: %d boots and exited %v, want 1 boot and an exit", m.Name, boots, exited)
	}
	return nil
}

// TestDrainWhileDown drains m1 while its node is down, under each rule: the drain cannot step
// Drain on a node that is down, so m1 exits when its node boots again, with no new
// incarnation. Its old session runs out, and the other four members take its shards and
// converge. The crash is not a fault of the world's plan, so the recovery time stays at 0 and
// the liveness final, which would judge the crash's ownerless shards, is left out.
func TestDrainWhileDown(t *testing.T) {
	r := row{
		name: "TestDrainWhileDown", sc: world.Scenario{NoLiveness: true},
		seeds: 3, duration: 60 * time.Second, net: defaultNet, g2: true,
	}
	runRow(t, r, func(fw *faultline.World, w *world.World) {
		m1 := w.Members()[0]
		fw.Sim.At(kernel.Time(drainAt-time.Second), "test/crash", m1.Node.Crash)
		fw.Sim.At(kernel.Time(drainAt), "test/drain", func() { w.Drain(m1) })
		fw.Sim.At(kernel.Time(drainAt+time.Second), "test/restart", m1.Node.Restart)
		fw.Final("test/exit", func() error {
			if m1.Phase() != world.Exited || m1.Recoveries() != 0 {
				return fmt.Errorf("m1 is %v after %d recoveries at the end, want exited after none", m1.Phase(), m1.Recoveries())
			}
			return nil
		})
	})
}

// steppedLost reports whether the trace has m's SessionFailed with cause lost.
func steppedLost(recs []kernel.Record, m *world.Member) bool {
	for _, r := range recs {
		if r.Node == m.Node.ID() && r.Kind == kindInput && attr(r, "type") == "SessionFailed" && attr(r, "cause") == "lost" {
			return true
		}
	}
	return false
}

// slowStops is the StopDelay of TestDrainSessionLost and TestSessionEndCrash: runs take up to
// 10 s to stop, so a member whose session ends while its runs stop waits for seconds.
const slowStops = 10 * time.Second

// TestDrainSessionLost drains m1 at 39 s, under each rule, in TestSessionEnds's world with runs
// that take up to 10 s to stop: the store ends m1's session at about 40 s, while m1 still
// waits for its runs, and m1 exits instead of recovering (RUN-190). In every seed m1 is exited
// at the end with no recovery, and the finals hold, tenure/recovered among them, which owes m1
// nothing; in at least one seed of each rule m1 stepped SessionFailed{lost} before its exit.
func TestDrainSessionLost(t *testing.T) {
	r := row{
		name: "TestDrainSessionLost", sc: world.Scenario{EndAfter: 40 * time.Second, StopDelay: slowStops},
		seeds: 5, duration: 75 * time.Second, net: defaultNet, g2: false,
	}
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			lost := map[uint64]bool{}
			opts := r.options()
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				m1 := w.Members()[0]
				fw.Sim.At(kernel.Time(39*time.Second), "test/drain", func() { w.Drain(m1) })
				fw.Final("test/exit", func() error {
					if m1.Phase() != world.Exited || m1.Recoveries() != 0 {
						return fmt.Errorf("m1 is %v after %d recoveries at the end, want exited after none", m1.Phase(), m1.Recoveries())
					}
					lost[fw.Seed()] = steppedLost(fw.Sim.Records(), m1)
					return nil
				})
			}))
			if !slices.Contains(slices.Collect(maps.Values(lost)), true) {
				t.Errorf("m1 lost its session during the drain in none of %d seeds", len(lost))
			}
		})
	}
}

// TestSessionEndCrash crashes m1's node at 41 s and restarts it at 42 s, under each rule, in
// TestSessionEnds's world with runs that take up to 10 s to stop: m1's session ends at about
// 40 s, and the crash comes while m1 waits for its runs, so no recovery follows: the new
// incarnation is the boot's. tenure/recovered owes m1 nothing for that end, and the finals
// hold. The crash is not a fault of the world's plan, so the liveness final, which would judge
// its ownerless shards, is left out. In at least one seed of each rule m1 crashed while it
// waited: it had stepped SessionFailed{lost} and never recovered.
func TestSessionEndCrash(t *testing.T) {
	r := row{
		name: "TestSessionEndCrash", sc: world.Scenario{EndAfter: 40 * time.Second, StopDelay: slowStops, NoLiveness: true},
		seeds: 5, duration: 75 * time.Second, net: defaultNet, g2: false,
	}
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			waited := map[uint64]bool{}
			opts := r.options()
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				m1 := w.Members()[0]
				fw.Sim.At(kernel.Time(41*time.Second), "test/crash", m1.Node.Crash)
				fw.Sim.At(kernel.Time(42*time.Second), "test/restart", m1.Node.Restart)
				fw.Final("test/crash", func() error {
					if m1.Recoveries() > 1 {
						return fmt.Errorf("m1 recovered %d times, want at most once", m1.Recoveries())
					}
					waited[fw.Seed()] = m1.Recoveries() == 0 && steppedLost(fw.Sim.Records(), m1)
					return nil
				})
			}))
			if !slices.Contains(slices.Collect(maps.Values(waited)), true) {
				t.Errorf("m1 crashed while it waited for its runs in none of %d seeds", len(waited))
			}
		})
	}
}

// TestDrainAll drains all three members at 20 s, under each rule: each hands its shards over
// to nobody, closes its session and exits. With no member active at the end, tenure/converged
// checks that no shard is owned, and passes; the liveness final, which would want owners, is
// left out.
func TestDrainAll(t *testing.T) {
	r := row{
		name: "TestDrainAll", sc: world.Scenario{Members: 3, StopDelay: time.Second, NoLiveness: true},
		seeds: 3, duration: 40 * time.Second, net: defaultNet, g2: true,
	}
	runRow(t, r, func(fw *faultline.World, w *world.World) {
		fw.Sim.At(kernel.Time(drainAt), "test/drain", func() {
			for _, m := range w.Members() {
				w.Drain(m)
			}
		})
		fw.Final("test/exit", func() error {
			for _, m := range w.Members() {
				if m.Phase() != world.Exited {
					return fmt.Errorf("%s is %v at the end, want exited", m.Name, m.Phase())
				}
			}
			return nil
		})
	})
}
