// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// The names of the final checks Build registers, in the order they run (SIM-540, SIM-545). The
// finals of a scenario's own parts, as tenure/recovered or tenure/rollout, are registered after
// them, so they run after them.
const (
	finalG5        = invariantPrefix + "g5"
	finalConverged = invariantPrefix + "converged"
)

// installFinals registers the final checks, which faultline runs once after the last event of
// a run that ended normally, in this order:
//
//   - tenure/g5, unless the scenario has NoLiveness: the checker's Liveness at the recovery
//     time and the bound: every shard has an owner at R + B, so none stays ownerless for
//     longer than B once the world is left alone (SIM-540);
//   - tenure/converged: the members agree on where the shards go, and each shard runs there
//     (SIM-545; see convergedErr); when no member is active, as after every member drained
//     and exited, that no shard is owned (unownedErr).
//
// Finals see the world as the last event left it: no settling time is added, so a scenario's
// run must last until the bound has passed after the recovery time, as SetupError demands.
func (w *World) installFinals() {
	if !w.sc.NoLiveness {
		w.fw.Final(finalG5, func() error {
			if w.finalsOff {
				return nil
			}
			return w.checker.Liveness(w.recovery, w.bound)
		})
	}
	w.fw.Final(finalConverged, func() error {
		if w.finalsOff {
			return nil
		}
		if w.noneActive() {
			return unownedErr(w.sc.Spaces, w.checker.Owners)
		}
		live, err := w.liveMembers()
		if err != nil {
			return err
		}
		return convergedErr(w.sc.Spaces, w.storeSrv.model.slotCount, live, w.checker.Owners)
	})
}

// noneActive reports that no member is active: every member exited, or waits on standby. Then
// nothing may run, and tenure/converged checks only that (unownedErr).
func (w *World) noneActive() bool {
	for _, m := range w.members {
		if m.phase == Active {
			return false
		}
	}
	return true
}

// unownedErr decides tenure/converged when no member is active: every shard of spaces, by space
// name and then shard, must be owned by nobody, by the checker's ownership owners. The first
// owned one is reported as `converged: book/3 owned by m1/<32 hex>, but no member is active`.
func unownedErr(spaces []core.SpaceSpec, owners func(space string, shard int) []string) error {
	sorted := slices.Clone(spaces)
	slices.SortFunc(sorted, func(a, b core.SpaceSpec) int { return cmp.Compare(a.Name, b.Name) })
	for _, sp := range sorted {
		for shard := range sp.Shards {
			if got := owners(sp.Name, shard); len(got) > 0 {
				return fmt.Errorf("converged: %s/%d owned by %s, but no member is active", sp.Name, shard, strings.Join(got, ", "))
			}
		}
	}
	return nil
}

// liveMember is what tenure/converged reads of a member that is active and up.
type liveMember struct {
	name     string
	holder   string
	draining bool
	slot     int                         // the slot the machine holds, or -1
	assign   func(space string) []string // the machine's Assignment
}

// liveMembers returns, in NodeID order, the members that are active and whose node is up: the
// members tenure/converged judges. A member that is down at the end, a standby or one that
// exited owes nothing. An active member that is up must drive its present incarnation: one
// whose driver is missing, stopped or of an earlier holder is reported as
// `converged: m3 is active and up but drives no machine for its holder m3/<32 hex>`, since
// leaving it out would let a member that never came back pass.
func (w *World) liveMembers() ([]liveMember, error) {
	var live []liveMember
	for _, m := range w.members {
		if m.phase != Active || m.Node.State() != kernel.NodeUp {
			continue
		}
		d := w.drivers.current(m)
		if d == nil || d.stopped || d.holder != m.holder {
			return nil, fmt.Errorf("converged: %s is active and up but drives no machine for its holder %s", m.Name, m.holder)
		}
		st := d.status()
		if st.Session == core.SessionStateLost { // gone for good; a new incarnation replaces it (SIM-145)
			continue
		}
		live = append(live, liveMember{
			name: m.Name, holder: d.holder, draining: st.Draining, slot: st.Slot,
			assign: d.machine.Assignment,
		})
	}
	return live, nil
}

// convergedErr decides tenure/converged (SIM-545) at the end of a run, over live, the members
// that are active and up in NodeID order, a store with slots slot count, and owners, the
// checker's ownership. It never passes for want of anything to judge: with no live member it
// reports `converged: no member is active and up`, and when the live members give no placement
// key (every one draining, or every slot vacant) `converged: none of the 3 live members takes
// shards`. With slots, two live members whose machines hold one slot are reported as
// `converged: m2 and m4 both hold slot-1`. Then, for each space, by name:
//
//  1. Every member's machine computes the same assignment: the first member whose Assignment
//     differs from the first member's is reported at the first shard that differs,
//     `converged: book: m2's assignment differs from m1's at shard 3: "m4" != "m5"`.
//  2. That assignment is the one core.Balanced gives over the core.PlacementKeys of the live
//     members (see referenceView): `converged: book: m1's assignment differs from the live
//     members' at shard 3: "m4" != "m5"`. So the members cannot agree on placing a shard on a
//     member that is gone.
//  3. Every shard with an assignee is owned, by the checker's ownership, by that member's
//     present holder and by nobody else: `converged: book/3 assigned to m4, owned by
//     m5/<32 hex>` (several owners joined by ", "), or `… owned by nobody`.
//
// The assignee of a shard is a placement key. Without slots it is a member's ID, which is the
// member's name. With slots it is a key "slot-<k>", which PlacementKeys maps to the slot k, and
// the slot to the live member whose machine holds it (Status().Slot); the message then names
// both, `converged: book/3 assigned to slot-2 (m4), owned by nobody`, and a key that no live
// member's slot is is reported as `converged: book/3 assigned to slot-2, which no live member
// holds`.
func convergedErr(spaces []core.SpaceSpec, slots int, live []liveMember,
	owners func(space string, shard int) []string,
) error {
	if len(live) == 0 {
		return errors.New("converged: no member is active and up")
	}
	for i, m := range live {
		if m.slot < 0 || m.slot >= slots {
			continue
		}
		for _, o := range live[i+1:] {
			if o.slot == m.slot {
				return fmt.Errorf("converged: %s and %s both hold slot-%d", m.name, o.name, m.slot)
			}
		}
	}
	view, expired := referenceView(slots, live)
	keys, keySlots := core.PlacementKeys(view, time.Time{}, expired)
	if len(keys) == 0 {
		return fmt.Errorf("converged: none of the %d live members takes shards", len(live))
	}
	slotOf := make(map[string]int, len(keySlots)) // looked up only
	for i, k := range keySlots {
		slotOf[keys[i]] = k
	}

	sorted := slices.Clone(spaces)
	slices.SortFunc(sorted, func(a, b core.SpaceSpec) int { return cmp.Compare(a.Name, b.Name) })
	for _, sp := range sorted {
		first := live[0]
		want := first.assign(sp.Name)
		for _, m := range live[1:] {
			if shard, ok := firstDifference(m.assign(sp.Name), want); ok {
				return fmt.Errorf("converged: %s: %s's assignment differs from %s's at shard %d: %q != %q",
					sp.Name, m.name, first.name, shard, keyAt(m.assign(sp.Name), shard), keyAt(want, shard))
			}
		}
		ref := balancedAssignment(sp, keys)
		if shard, ok := firstDifference(want, ref); ok {
			return fmt.Errorf("converged: %s: %s's assignment differs from the live members' at shard %d: %q != %q",
				sp.Name, first.name, shard, keyAt(want, shard), keyAt(ref, shard))
		}
		for shard, key := range want {
			if key == "" {
				continue
			}
			assignee, name := assigneeOf(key, slots, slotOf, live)
			if assignee == nil {
				return fmt.Errorf("converged: %s/%d assigned to %s, which no live member holds", sp.Name, shard, key)
			}
			got := owners(sp.Name, shard)
			if len(got) == 1 && got[0] == assignee.holder {
				continue
			}
			by := "nobody"
			if len(got) > 0 {
				by = strings.Join(got, ", ")
			}
			return fmt.Errorf("converged: %s/%d assigned to %s, owned by %s", sp.Name, shard, name, by)
		}
	}
	return nil
}

// referenceView returns the view of the live members that tenure/converged places the shards
// by, and the expiry marks of its slots. Its members are the live members, sorted by ID, each
// with its Draining and its Slot. With slots, each slot's holder is the holder of the live member
// whose machine holds the slot (convergedErr has ruled out two), and a slot that no live member holds
// is vacant and marked expired: the run lasts until the bound, which counts the timing's
// SlotGrace, has passed after the recovery time, so every vacancy has lasted that long by the
// end. A member's machine that holds a slot at or above the count holds a retired slot, which
// is no key. No member has a JoinAt or a LeaveAt, so the wall time PlacementKeys takes does not
// matter.
func referenceView(slots int, live []liveMember) (core.View, []bool) {
	v := core.View{Members: make([]core.Member, len(live)), Slots: slots}
	for i, m := range live {
		v.Members[i] = core.Member{ID: m.name, Draining: m.draining, Slot: m.slot}
	}
	slices.SortFunc(v.Members, func(a, b core.Member) int { return cmp.Compare(a.ID, b.ID) })
	if slots == 0 {
		return v, nil
	}
	v.SlotHolders = make([]string, slots)
	for _, m := range live {
		if m.slot >= 0 && m.slot < slots && v.SlotHolders[m.slot] == "" {
			v.SlotHolders[m.slot] = m.holder
		}
	}
	expired := make([]bool, slots)
	for s, h := range v.SlotHolders {
		expired[s] = h == ""
	}
	return v, expired
}

// balancedAssignment returns the placement key of each shard of sp that core.Balanced gives
// over keys, "" for every shard when there is no key.
func balancedAssignment(sp core.SpaceSpec, keys []string) []string {
	out := make([]string, sp.Shards)
	if len(keys) == 0 {
		return out
	}
	for shard, k := range core.Balanced(sp.Name, sp.Shards, keys) {
		if k >= 0 {
			out[shard] = keys[k]
		}
	}
	return out
}

// assigneeOf returns the live member that the placement key names, and how a message names it:
// the member's name without slots, and the key and the name, as "slot-2 (m4)", with slots. It
// returns nil when no live member is that key.
func assigneeOf(key string, slots int, slotOf map[string]int, live []liveMember) (*liveMember, string) {
	if slots == 0 {
		for i := range live {
			if live[i].name == key {
				return &live[i], key
			}
		}
		return nil, ""
	}
	slot, ok := slotOf[key]
	if !ok {
		return nil, ""
	}
	for i := range live {
		if live[i].slot == slot {
			return &live[i], key + " (" + live[i].name + ")"
		}
	}
	return nil, ""
}

// firstDifference returns the first shard at which got and want differ, a shard that only one
// of them has included.
func firstDifference(got, want []string) (int, bool) {
	for i := range max(len(got), len(want)) {
		if keyAt(got, i) != keyAt(want, i) || i >= len(got) || i >= len(want) {
			return i, true
		}
	}
	return 0, false
}

// keyAt returns keys[i], or "" past its end.
func keyAt(keys []string, i int) string {
	if i < len(keys) {
		return keys[i]
	}
	return ""
}
