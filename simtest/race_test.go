// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The confirmation race (SIM-670). Under the Observer rule a member b takes a shard from a
// holder a by reading the record (getShard), reading a's session (getSession) and writing with
// a compare-and-swap (cas) over the record version it read. When a, degraded, renews between
// b's getSession and b's cas, the renewal alone changes nothing b's cas looks at: only a's
// Confirm, which the store answers by writing the record, makes the cas stale (MCH O5).
// TestConfirmRace puts both of a's requests into that window, on purpose, in every seed.
//
// The timeline, with a = m1 and b = m2, two members sharing the fenced space ledger, a owning
// ledger/0. Every clock is exact but b's, and every link has the default 1 ms + up to 4 ms but
// two: b's link to the store has raceLink (500 ms), and the store's link to a raceLag (50 ms).
//
//   - a's machine sends a keepAlive every RetryPeriod (2 s) of its clock, at 2 s, 4 s, ... The
//     one sent at 18 s renews a's session at the store at about 18.004 s, to a version v that
//     b sees in the store's push at about 18.01 s.
//   - At raceCut (19 s) a is cut off from the store both ways, not from the fence: its
//     keepAlives of 20 s to 30 s are dropped, at 28 s (18 s + RenewDeadline) its machine
//     degrades ledger/0, and its run goes on writing to the fence.
//   - b's clock runs raceDrift (17.5 %) fast, so b has seen v unchanged for LeaseDuration
//     (15 s of its clock) at about 18.01 s + 15 s / 1.175 ≈ 30.77 s. b's view drops a, and b
//     sends getShard, which reaches the store at about 31.27 s; getSession reaches it at about
//     31.77 s and shows v; b sends cas, which reaches the store at about 32.28 s. Each request
//     crosses b's 500 ms link; each answer comes back in a few ms.
//   - H = raceHeal (31 s) heals a's links after its keepAlive of 30 s was dropped and before
//     its keepAlive of 32 s, which reaches the store at about 32.002 s: after b's getSession,
//     and still in a's lease on the store's clock, which judges a's own session only (until
//     about 33.004 s). The renewal makes a's session live again, and a's machine returns its
//     Confirm in the step of the answer (about 32.05 s): the store writes ledger/0's record at
//     about 32.055 s, a steps Restore on the answer (src=confirm) at about 32.105 s, and b's
//     cas at about 32.28 s finds a newer version than it read: stale.
//   - From raceMuteFrom (31.9 s) to raceMuteTo (32.15 s) the store's link to b is cut: the
//     pushes of a's renewal and of the confirm's write are lost to b, which keeps a out of its
//     view and goes on wanting ledger/0 until the cas's answer, which comes after the heal.
//
// The margins are about 225 ms from b's getSession to a's renewal, 220 ms from a's confirm to
// b's cas, 1 s to the end of a's lease on the store's clock, and 100 ms or more from each end
// of the mute to the messages around it. test/window checks the order in every run.
//
// Why b's clock drifts: b counts a's lease from the moment it first saw the version, which is
// after the store wrote it. On exact clocks b's getSession therefore reaches the store after
// a's lease has run out on the store's clock, and a renewal after it ends a's session as late;
// a renewal before it shows b a new version, and b sends no cas. Only a clock that counts the
// lease faster than the store's opens the window; 17.5 % is inside the timing's drift bound
// (20 %), so the scenario's Rho is set to it, the bound counts it and G2 is asserted. The
// other drifts drawn from Rho are set back to 0 at the start, which leaves a timeline that
// depends on no seed.
//
// Why only b's link to the store is slow: the takeover's three requests share one RetryPeriod
// timeout from the first, and 500 ms both ways would be four crossings of 500 ms, 2 s of
// global time and more on b's fast clock, so b's cas would never go out.
//
// Why the mute and the lag: they make the race one that only the confirm's write wins. Had b
// seen a's renewal, a would be back in b's view and b would give a grant up at once without a
// run; with the mute, b runs whatever it is granted. The lag makes a hear of a grant to b
// 50 ms after b does. So with a confirm that writes nothing (the planted bug ReadConfirm,
// TestMutantReadConfirm), b's cas is granted and b starts its run while a, restored by the
// confirm's answer, still owns the shard, and tenure/g2 fails; with the protocol as written the
// cas is stale and nothing overlaps.
const (
	raceLink  = 500 * time.Millisecond
	raceLag   = 50 * time.Millisecond
	raceDrift = 175_000
	raceCut   = 19 * time.Second
	raceHeal  = 31 * time.Second

	raceMuteFrom = 31*time.Second + 900*time.Millisecond
	raceMuteTo   = 32*time.Second + 150*time.Millisecond
)

// raceRow is TestConfirmRace's row: the Observer rule only, 3 seeds of 45 s. Its faults are
// raceFaults, which raceSetup plans.
var raceRow = row{
	name: "TestConfirmRace",
	sc: world.Scenario{
		Members: 2, Rho: raceDrift,
		Spaces: []core.SpaceSpec{{Name: "ledger", Shards: 2, Fenced: true}},
	},
	seeds: 3, duration: 45 * time.Second, net: defaultNet, g2: true,
	rules: []world.Takeover{world.Observer},
}

// raceFaults returns the script of the race: at 0 every clock exact but m2's, raceDrift fast,
// m2's link to the store slowed to raceLink and the store's link to m1 to raceLag; at raceCut
// m1 cut off from the store both ways; at raceHeal (H) both links healed; from raceMuteFrom to
// raceMuteTo the store's link to m2 cut.
func raceFaults() fault.Planner {
	drift := func(node string, ppm int64) fault.Event {
		return fault.Event{At: 0, Kind: fault.KindClockDrift, Node: node, N: ppm}
	}
	link := func(from, to string, latency time.Duration) fault.Event {
		return fault.Event{At: 0, Kind: fault.KindLink, Node: from, Peer: to, Link: &simnet.Link{Latency: latency}}
	}
	at := func(t time.Duration, kind fault.Kind, from, to string) fault.Event {
		return fault.Event{At: kernel.Time(t), Kind: kind, Node: from, Peer: to}
	}
	return fault.Script(
		drift("store", 0), drift("fence", 0), drift("m1", 0), drift("m2", raceDrift),
		link("m2", "store", raceLink), link("store", "m1", raceLag),
		at(raceCut, fault.KindCut, "m1", "store"), at(raceCut, fault.KindCut, "store", "m1"),
		at(raceHeal, fault.KindHealLink, "m1", "store"), at(raceHeal, fault.KindHealLink, "store", "m1"),
		at(raceMuteFrom, fault.KindCut, "store", "m2"), at(raceMuteTo, fault.KindHealLink, "store", "m2"),
	)
}

// raceSetup is the part of the race's world function after Build: it plans raceFaults and
// registers the final tenure/race, then test/window.
func raceSetup(fw *faultline.World, w *world.World) {
	fw.Plan(raceFaults())
	fw.Final("tenure/race", func() error { return raceFinal(fw, w) })
	fw.Final("test/window", func() error {
		tl, err := raceTimeline(fw.Sim.Records(), w)
		if err != nil {
			return err
		}
		fw.Logf("race: %s", tl)
		return tl.opened()
	})
}

// TestConfirmRace runs the confirmation race (SIM-670, AT-SIM-43 in a whole world) for 3
// seeds (with FAULTLINE_CHECK_DETERMINISM=1, every seed twice with equal trace hashes): every
// invariant holds after every event, the finals tenure/g5 and tenure/converged pass, then
// tenure/race: b's cas is answered stale and a stepped Restore from a HolderChanged read in a
// Confirm's answer. test/window checks in the trace that the window opened: b's getSession,
// a's renewal, a's confirm and b's cas reach the store in that order.
func TestConfirmRace(t *testing.T) {
	runRace(t, raceRow)
}

// runRace runs r, the race's row or a copy of it, under the Observer rule with every record
// kept.
func runRace(t *testing.T, r row) {
	t.Helper()
	opts := r.options()
	opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
	faultline.Run(t, opts, r.body(world.Observer, raceSetup))
}

// raceFinal is the final tenure/race: in the records of the run, m2's first cas after the cut
// over the record of m1's shard was answered stale, m1 stepped Restore in the event of a
// HolderChanged with src=confirm, and every invariant holds at the end.
func raceFinal(fw *faultline.World, w *world.World) error {
	tl, err := raceTimeline(fw.Sim.Records(), w)
	if err != nil {
		return err
	}
	a, b := w.Members()[0], w.Members()[1]
	if tl.casResult != "stale" {
		return fmt.Errorf("race: %s's cas of %s at %v was answered %s, want stale", b.Name, tl.shard, tl.cas, tl.casResult)
	}
	if tl.restore == 0 || tl.restoreShard != tl.shard {
		return fmt.Errorf("race: %s stepped no Restore of %s from a HolderChanged with src=confirm", a.Name, tl.shard)
	}
	for _, name := range []string{"g1", "g2", "g3", "g4", "monotonic"} {
		if err := w.Checker().Err(name); err != nil {
			return fmt.Errorf("race: tenure/%s: %w", name, err)
		}
	}
	return nil
}

// raceTimes is the timeline of the race as the store and m1 recorded it: the times at which
// m2's getSession of m1's session, m1's renewal, m1's confirm and m2's cas reached the store,
// all after the cut, and the time of m1's Restore from a Confirm's answer, 0 when there was
// none.
type raceTimes struct {
	shard                           string // the space and shard of m2's cas, as ledger/0
	getSession, renew, confirm, cas kernel.Time
	casResult                       string
	restore                         kernel.Time
	// confirmShard and restoreShard are the space and shard of the confirm and the Restore.
	confirmShard, restoreShard string
}

// String returns the timeline, in the order the race wants.
func (tl raceTimes) String() string {
	return fmt.Sprintf("%s: m2's getSession at %v, m1's renewal at %v, m1's confirm at %v, m2's cas at %v (%s), m1's Restore at %v",
		tl.shard, tl.getSession, tl.renew, tl.confirm, tl.cas, tl.casResult, tl.restore)
}

// opened reports whether the window opened: the getSession, the renewal, the confirm and the
// cas reached the store in that order.
func (tl raceTimes) opened() error {
	if tl.getSession < tl.renew && tl.renew < tl.confirm && tl.confirm < tl.cas && tl.confirmShard == tl.shard {
		return nil
	}
	return fmt.Errorf("race: the window did not open: %s", tl)
}

// raceTimeline reads the race's timeline from a full trace of the race's world: m2's first
// getSession of m1's session after the cut, then its first cas, both from the store's
// tenure.observe; m1's first renewal after that getSession (tenure.session op=renew), and its
// first confirm after the renewal (tenure.confirm); and m1's first Restore after the cut that
// it stepped in the event of a HolderChanged with src=confirm. The confirm and the Restore
// keep their shards, which opened and raceFinal compare with the cas's.
func raceTimeline(recs []kernel.Record, w *world.World) (raceTimes, error) {
	a, b, store := w.Members()[0], w.Members()[1], w.Store().ID()
	var tl raceTimes
	cut := kernel.Time(raceCut)
	confirmed := false // m1 stepped a HolderChanged with src=confirm in the present event
	for _, r := range recs {
		if r.At < cut {
			continue
		}
		switch {
		case r.Kind == kindEvent:
			confirmed = false
		case r.Node == store && r.Kind == kindObserve && attr(r, "holder") == b.Holder():
			switch attr(r, "op") {
			case "get_session":
				if tl.getSession == 0 && attr(r, "session") == a.Holder() {
					tl.getSession = r.At
				}
			case "cas":
				if tl.getSession != 0 && tl.cas == 0 {
					tl.cas, tl.casResult = r.At, attr(r, "result")
					tl.shard = recShard(r)
				}
			}
		case r.Node == store && r.Kind == kindSession && attr(r, "holder") == a.Holder() && attr(r, "op") == "renew":
			if tl.getSession != 0 && tl.renew == 0 {
				tl.renew = r.At
			}
		case r.Node == store && r.Kind == kindConfirm && attr(r, "holder") == a.Holder():
			if tl.renew != 0 && tl.confirm == 0 {
				tl.confirm, tl.confirmShard = r.At, recShard(r)
			}
		case r.Node == a.Node.ID() && r.Kind == kindInput:
			confirmed = attr(r, "type") == "HolderChanged" && attr(r, "src") == "confirm"
		case r.Node == a.Node.ID() && r.Kind == kindAction && attr(r, "type") == "Restore":
			if confirmed && tl.restore == 0 {
				tl.restore, tl.restoreShard = r.At, recShard(r)
			}
		}
	}
	if tl.cas == 0 {
		return tl, fmt.Errorf("race: %s sent no getSession of %s's session followed by a cas after the cut at %v", b.Name, a.Name, cut)
	}
	return tl, nil
}

// recShard returns the space and shard attributes of r as ledger/0.
func recShard(r kernel.Record) string { return attr(r, "space") + "/" + attr(r, "shard") }

// The record kinds the race reads, beyond those of the other tests (SIM §8).
const (
	kindObserve = "tenure.observe"
	kindConfirm = "tenure.confirm"
)
