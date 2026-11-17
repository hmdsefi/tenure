// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file checks RTR-140: for generated sequences of views and readings, an Assigner's
// assignment equals that of a Machine of the same group that holds no slot and does not drain
// (AT-RTR-01). The machine's holder and member ID never appear in a view, and the machine is
// never renewed, so its session stays opening: it never claims a slot and wants no shard. Both
// get the same views, readings and wall-clock times, and every space's assignment is compared
// after every step.
//
// equivSequences (equiv_short_test.go, equiv_long_test.go) sets how many sequences run: 2000
// in a plain go test, more under -tags long.

// equivSteps is the number of steps of each sequence.
const equivSteps = 50

// equivMembers is the number of member IDs a sequence draws from: m0 … m5.
const equivMembers = 6

// equivMaxSlots is the largest slot count a sequence's views have.
const equivMaxSlots = 4

// equivSpaces are the spaces of the equivalence test: shard counts below, near and above the
// number of placement keys.
var equivSpaces = []AssignerSpace{{Name: "orders", Shards: 8}, {Name: "markets", Shards: 13}, {Name: "one", Shards: 1}}

// equivWall is the wall-clock time at reading 0 of the test's driver.
var equivWall = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// equivTimings are the timings sequences use, by seed: small, so that vacancies expire within a
// sequence, one with SlotGrace equal to LeaseDuration and one longer.
var equivTimings = []Timing{
	{LeaseDuration: 15 * time.Millisecond, RenewDeadline: 10 * time.Millisecond, RetryPeriod: 2 * time.Millisecond, SlotGrace: 15 * time.Millisecond},
	{LeaseDuration: 15 * time.Millisecond, RenewDeadline: 10 * time.Millisecond, RetryPeriod: 2 * time.Millisecond, SlotGrace: 40 * time.Millisecond},
}

// equivOffsets are the offsets of the driver's wall clock from equivWall + reading, by seed.
var equivOffsets = []time.Duration{0, 3 * time.Millisecond, -7*time.Millisecond + 1, time.Hour}

// equivStep is one step of a sequence: a Tick, or a view read at the reading now and the
// wall-clock time wall. note says what the generator meant the view to be.
type equivStep struct {
	tick bool
	now  time.Duration
	view View
	wall time.Time
	note string
}

// String formats s with times relative to equivWall, for failure messages.
func (s equivStep) String() string {
	if s.tick {
		return fmt.Sprintf("@%v tick", s.now)
	}
	v := s.view
	var b strings.Builder
	fmt.Fprintf(&b, "@%v %s: wall %+v version %d slots %d holders %q members [", s.now, s.note, s.wall.Sub(equivWall), v.Version, v.Slots, v.SlotHolders)
	for i, m := range v.Members {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s(slot %d", m.ID, m.Slot)
		if m.Draining {
			b.WriteString(" draining")
		}
		if !m.JoinAt.IsZero() {
			fmt.Fprintf(&b, " join %+v", m.JoinAt.Sub(equivWall))
		}
		if !m.LeaveAt.IsZero() {
			fmt.Fprintf(&b, " leave %+v", m.LeaveAt.Sub(equivWall))
		}
		b.WriteString(")")
	}
	b.WriteString("]")
	return b.String()
}

// equivMember is the generator's record of one member ID.
type equivMember struct {
	present, draining bool
	joinAt, leaveAt   time.Time

	// gen numbers the member's holder strings, "m<i>/g<gen>": a restart takes a new one.
	gen int
}

// equivGen generates one sequence. It keeps the group's state and changes it a little for each
// view, so that consecutive views are related as a membership's are.
type equivGen struct {
	r      *rand.Rand
	grace  time.Duration
	offset time.Duration

	now     time.Duration
	version uint64
	members [equivMembers]equivMember

	// holders[s] is slot s's holder string, "" when vacant, and holderOf[s] the member that
	// holds it, -1 when vacant or when the holder string names a member that has left.
	holders  []string
	holderOf []int

	// targets are readings worth landing on: SlotGrace and SlotGrace−1ns after a view, when a
	// vacancy it shows may start, and the readings of JoinAt and LeaveAt bounds.
	targets []time.Duration
}

// newEquivGen returns the generator of seed's sequence.
func newEquivGen(seed uint64) *equivGen {
	return &equivGen{
		r:      rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)), //nolint:gosec // G404: a test's seeded sequence.
		grace:  equivTimings[seed%uint64(len(equivTimings))].SlotGrace,
		offset: equivOffsets[(seed/2)%uint64(len(equivOffsets))],
		now:    time.Duration(seed%3) * time.Millisecond,
	}
}

// equivSequence returns seed's sequence of steps.
func equivSequence(seed uint64) []equivStep {
	g := newEquivGen(seed)
	steps := make([]equivStep, equivSteps)
	for i := range steps {
		if i > 0 {
			g.advance()
		}
		steps[i] = g.step()
	}
	return steps
}

// advance moves the reading on: not at all, by 1ns, by a little, by exactly SlotGrace or
// SlotGrace−1ns, to a target, or by more than SlotGrace.
func (g *equivGen) advance() {
	switch p := g.r.IntN(100); {
	case p < 12:
	case p < 20:
		g.now++
	case p < 50:
		g.now += g.within(g.grace / 2)
	case p < 55:
		g.now += g.grace
	case p < 60:
		g.now += g.grace - 1
	case p < 85:
		g.targets = slices.DeleteFunc(g.targets, func(t time.Duration) bool { return t <= g.now })
		if len(g.targets) > 0 {
			g.now = g.targets[g.r.IntN(len(g.targets))]
		} else {
			g.now += g.within(g.grace)
		}
	default:
		g.now += g.grace + g.within(3*g.grace)
	}
}

// within returns a duration in [0, d], in whole microseconds or, one time in four, nanoseconds.
func (g *equivGen) within(d time.Duration) time.Duration {
	if g.r.IntN(4) == 0 {
		return time.Duration(g.r.Int64N(int64(d) + 1))
	}
	return time.Duration(g.r.Int64N(int64(d/time.Microsecond)+1)) * time.Microsecond
}

// target records the reading t as worth landing on, with its neighbours.
func (g *equivGen) target(t time.Duration) {
	for _, r := range []time.Duration{t - 1, t, t + 1} {
		if r > g.now {
			g.targets = append(g.targets, r)
		}
	}
	if n := len(g.targets); n > 30 {
		g.targets = slices.Delete(g.targets, 0, n-30)
	}
}

// step returns the step at the current reading: a tick, a view of older version, a view that
// is not valid, or a view of the group after a few changes, of the same or a newer version.
func (g *equivGen) step() equivStep {
	wall := equivWall.Add(g.now + g.offset)
	if g.r.IntN(8) == 0 {
		// A wall clock that steps between views.
		wall = wall.Add(time.Duration(g.r.IntN(2001)-1000) * time.Microsecond)
	}
	p := g.r.IntN(100)
	if p < 30 {
		return equivStep{tick: true, now: g.now}
	}
	g.target(g.now + g.grace)
	switch {
	case p < 36 && g.version > 0:
		// A view the membership sent earlier and delivered late: changed, but older.
		saved := g.save()
		g.mutate()
		v := g.build(g.version - 1 - g.r.Uint64N(min(g.version, 3)))
		g.restore(saved)
		return equivStep{now: g.now, view: v, wall: wall, note: "older view"}
	case p < 42:
		v, how := g.invalid()
		return equivStep{now: g.now, view: v, wall: wall, note: "invalid view (" + how + ")"}
	}
	g.mutate()
	note := "view"
	switch q := g.r.IntN(10); {
	case q < 2:
		note = "same-version view"
	case q < 9:
		g.version++
	default:
		g.version += 2 + g.r.Uint64N(3)
	}
	return equivStep{now: g.now, view: g.build(g.version), wall: wall, note: note}
}

// equivSaved is a copy of a generator's group state.
type equivSaved struct {
	members  [equivMembers]equivMember
	holders  []string
	holderOf []int
}

// save returns a copy of the group state, for restore.
func (g *equivGen) save() equivSaved {
	return equivSaved{members: g.members, holders: slices.Clone(g.holders), holderOf: slices.Clone(g.holderOf)}
}

// restore sets the group state to s, which save returned.
func (g *equivGen) restore(s equivSaved) {
	g.members, g.holders, g.holderOf = s.members, s.holders, s.holderOf
}

// mutate makes one to three changes to the group.
func (g *equivGen) mutate() {
	for range 1 + g.r.IntN(3) {
		switch g.r.IntN(9) {
		case 0, 1:
			g.join()
		case 2:
			g.leave()
		case 3:
			if i, ok := g.pick(g.present); ok {
				g.members[i].draining = !g.members[i].draining
			}
		case 4:
			if i, ok := g.pick(g.present); ok {
				g.bounds(i)
			}
		case 5:
			g.resize(g.r.IntN(equivMaxSlots + 1))
		case 6:
			g.vacate()
		case 7:
			g.claim()
		case 8:
			g.handOver()
		}
	}
}

// pick returns a random member index for which ok holds, and whether there is one.
func (g *equivGen) pick(ok func(i int) bool) (int, bool) {
	var idx []int
	for i := range g.members {
		if ok(i) {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return 0, false
	}
	return idx[g.r.IntN(len(idx))], true
}

// slotOf returns the slot member i holds, or -1.
func (g *equivGen) slotOf(i int) int {
	return slices.Index(g.holderOf, i)
}

// standby returns a random present member that holds no slot, and whether there is one.
func (g *equivGen) standby() (int, bool) {
	return g.pick(func(i int) bool { return g.members[i].present && g.slotOf(i) < 0 })
}

// present reports whether member i is in the group.
func (g *equivGen) present(i int) bool { return g.members[i].present }

// join adds an absent member, with bounds half the time.
func (g *equivGen) join() {
	i, ok := g.pick(func(i int) bool { return !g.members[i].present })
	if !ok {
		return
	}
	g.members[i] = equivMember{present: true, gen: g.members[i].gen}
	if g.r.IntN(2) == 0 {
		g.bounds(i)
	}
}

// leave removes a present member. Its slot becomes vacant, or keeps the member's holder string,
// as a view does while the slot's record outlives the member.
func (g *equivGen) leave() {
	i, ok := g.pick(g.present)
	if !ok {
		return
	}
	g.members[i].present = false
	if s := g.slotOf(i); s >= 0 {
		g.holderOf[s] = -1
		if g.r.IntN(2) == 0 {
			g.holders[s] = ""
		}
	}
}

// bounds sets member i's JoinAt and LeaveAt around the wall-clock time of the current reading:
// one of them, both or neither, at most a few SlotGraces away, sometimes exactly at that time
// or 1ns from it.
func (g *equivGen) bounds(i int) {
	at := equivWall.Add(g.now + g.offset)
	near := func() time.Time {
		var d time.Duration
		switch g.r.IntN(7) {
		case 0:
			d = -g.grace
		case 1:
			d = -1
		case 2:
		case 3:
			d = 1
		case 4:
			d = g.grace
		case 5:
			d = g.grace - 1
		default:
			d = g.within(2 * g.grace)
		}
		t := at.Add(d)
		g.target(t.Sub(equivWall) - g.offset)
		return t
	}
	m := &g.members[i]
	switch g.r.IntN(4) {
	case 0:
		m.joinAt, m.leaveAt = near(), time.Time{}
	case 1:
		m.joinAt, m.leaveAt = time.Time{}, near()
	case 2:
		m.joinAt = near()
		m.leaveAt = m.joinAt.Add(1 + g.within(2*g.grace))
		g.target(m.leaveAt.Sub(equivWall) - g.offset)
	default:
		m.joinAt, m.leaveAt = time.Time{}, time.Time{}
	}
}

// resize changes the slot count to n. Dropped slots free their holders; new slots are vacant
// or claimed by a standby.
func (g *equivGen) resize(n int) {
	if n < len(g.holders) {
		g.holders, g.holderOf = g.holders[:n], g.holderOf[:n]
		return
	}
	for len(g.holders) < n {
		g.holders, g.holderOf = append(g.holders, ""), append(g.holderOf, -1)
		if g.r.IntN(2) == 0 {
			g.claimSlot(len(g.holders) - 1)
		}
	}
}

// vacate makes a random non-vacant slot vacant.
func (g *equivGen) vacate() {
	var held []int
	for s, h := range g.holders {
		if h != "" {
			held = append(held, s)
		}
	}
	if len(held) == 0 {
		return
	}
	s := held[g.r.IntN(len(held))]
	g.holders[s], g.holderOf[s] = "", -1
}

// claim gives a random slot that is vacant, or held by a member that has left, to a standby:
// the same holder string as before when it returns, or a new one.
func (g *equivGen) claim() {
	var free []int
	for s := range g.holders {
		if g.holderOf[s] < 0 {
			free = append(free, s)
		}
	}
	if len(free) > 0 {
		g.claimSlot(free[g.r.IntN(len(free))])
	}
}

// claimSlot gives slot s to a standby, if there is one.
func (g *equivGen) claimSlot(s int) {
	i, ok := g.standby()
	if !ok {
		return
	}
	if g.r.IntN(3) == 0 {
		g.members[i].gen++
	}
	g.holders[s], g.holderOf[s] = "m"+strconv.Itoa(i)+"/g"+strconv.Itoa(g.members[i].gen), i
}

// handOver changes a held slot's holder: to a standby, or to its holder's next holder string, as
// after a restart.
func (g *equivGen) handOver() {
	var held []int
	for s, i := range g.holderOf {
		if i >= 0 {
			held = append(held, s)
		}
	}
	if len(held) == 0 {
		return
	}
	s := held[g.r.IntN(len(held))]
	if i, ok := g.standby(); ok && g.r.IntN(2) == 0 {
		g.holders[s], g.holderOf[s] = "m"+strconv.Itoa(i)+"/g"+strconv.Itoa(g.members[i].gen), i
		return
	}
	i := g.holderOf[s]
	g.members[i].gen++
	g.holders[s] = "m" + strconv.Itoa(i) + "/g" + strconv.Itoa(g.members[i].gen)
}

// build returns the view of the group with version version: present members in ID order, each
// with the slot it holds or -1.
func (g *equivGen) build(version uint64) View {
	v := View{Version: version, Slots: len(g.holders), SlotHolders: slices.Clone(g.holders)}
	for i, m := range g.members {
		if !m.present {
			continue
		}
		id := "m" + strconv.Itoa(i)
		v.Members = append(v.Members, Member{
			ID: id, Addr: id + ":80", Draining: m.draining, Slot: g.slotOf(i), JoinAt: m.joinAt, LeaveAt: m.leaveAt,
		})
	}
	return v
}

// invalid returns a view of the group, changed, of a newer version, that View.Validate rejects,
// and what makes it so. The group itself does not change.
func (g *equivGen) invalid() (View, string) {
	saved := g.save()
	g.mutate()
	v := g.build(g.version + 1 + g.r.Uint64N(2))
	g.restore(saved)

	type fault struct {
		how   string
		apply func()
	}
	faults := []fault{
		{"negative slots", func() { v.Slots, v.SlotHolders = -1, nil }},
		{"SlotHolders length", func() { v.SlotHolders = append(v.SlotHolders, "") }},
	}
	if n := len(v.Members); n > 0 {
		k := g.r.IntN(n)
		faults = append(faults,
			fault{"ID with '/'", func() { v.Members[k].ID += "/x" }},
			fault{"slot below -1", func() { v.Members[k].Slot = -2 }},
			fault{"LeaveAt not after JoinAt", func() {
				v.Members[k].JoinAt = equivWall.Add(g.now)
				v.Members[k].LeaveAt = v.Members[k].JoinAt
			}},
		)
		if n > 1 {
			faults = append(faults, fault{"members out of order", func() {
				v.Members[0], v.Members[n-1] = v.Members[n-1], v.Members[0]
			}})
		}
		if v.Slots > 0 {
			faults = append(faults, fault{"slot out of range", func() { v.Members[k].Slot = v.Slots }})
			if s := slices.Index(v.SlotHolders, ""); s >= 0 {
				faults = append(faults, fault{"member on a vacant slot", func() { v.Members[k].Slot = s }})
			}
		}
	}
	f := faults[g.r.IntN(len(faults))]
	f.apply()
	return v, f.how
}

// equivReach counts the paths the sequences reach, so that the test fails when the generator
// stops reaching what the equivalence is about.
type equivReach struct {
	exactExpiry, oneBefore, tickChanged, viewChanged       int
	expiredEnded, toNoSlots, fromNoSlots, boundExcluded    int
	ignoredOlder, ignoredInvalid, sameVersion, staleHolder int
	boundAtInstant                                         int
}

// runEquiv feeds steps to an Assigner and to a slotless machine with timing, and compares their
// assignments after every step. It returns the index of the first step after which they differ,
// or after which the Assigner breaks its own contract, with the difference; or -1 and nil. It
// counts what the steps reach into reach, if not nil.
func runEquiv(timing Timing, steps []equivStep, reach *equivReach) (int, error) {
	a, err := NewAssigner(equivSpaces, timing)
	if err != nil {
		return 0, err
	}
	specs := make([]SpaceSpec, len(equivSpaces))
	for i, s := range equivSpaces {
		specs[i] = SpaceSpec{Name: s.Name, Shards: s.Shards}
	}
	m := NewMachine(MachineConfig{Holder: "router/h", Member: "router", Spaces: specs, Timing: timing})

	prev := make([][]string, len(equivSpaces))
	for i, s := range equivSpaces {
		prev[i] = a.Assignment(s.Name)
	}
	for k, s := range steps {
		if !s.tick {
			if valid := s.view.Validate() == nil; valid != !strings.HasPrefix(s.note, "invalid") {
				return k, &equivGenError{fmt.Errorf("generator: %s, but Validate() = %v", s.note, s.view.Validate())}
			}
		}
		wake, armed := a.NextWake()
		before := slices.Clone(a.vacancy)
		priorSlots, priorVersion := a.view.Slots, a.view.Version

		var changed bool
		if s.tick {
			changed = a.Tick(s.now)
			m.Step(Tick{Now: s.now})
		} else {
			changed = a.ApplyView(s.view, s.now, s.wall)
			m.Step(ViewChanged{Now: s.now, View: s.view, Wall: s.wall})
		}

		if st := m.Status(); st.Slot != -1 || st.Draining || st.Session != SessionStateOpening {
			return k, fmt.Errorf("machine is not a slotless, non-draining member: slot %d, draining %v, session %v", st.Slot, st.Draining, st.Session)
		}
		anyChanged := false
		for i, sp := range equivSpaces {
			got, want := a.Assignment(sp.Name), m.Assignment(sp.Name)
			if !slices.Equal(got, want) {
				return k, fmt.Errorf("Assignment(%q):\n  Assigner %q\n  Machine  %q", sp.Name, got, want)
			}
			if !slices.Equal(got, prev[i]) {
				anyChanged = true
			}
			prev[i] = got
		}
		if got, want := a.Assignment("unknown"), m.Assignment("unknown"); got != nil || want != nil {
			return k, fmt.Errorf("Assignment of an unknown space: Assigner %q, Machine %q", got, want)
		}
		if changed != anyChanged {
			return k, fmt.Errorf("step reported changed = %v, but the assignments changed = %v", changed, anyChanged)
		}
		if s.tick && changed && (!armed || s.now < wake) {
			return k, fmt.Errorf("Tick(%v) changed an assignment, but NextWake() was %v, %v before it", s.now, wake, armed)
		}

		if reach != nil {
			reach.count(a, s, changed, before, priorSlots, priorVersion, timing.SlotGrace)
		}
	}
	return -1, nil
}

// count records what step s reached: a's vacancies before it were before, and its applied
// view had priorSlots slots and version priorVersion.
func (r *equivReach) count(a *Assigner, s equivStep, changed bool, before []vacancy, priorSlots int, priorVersion uint64, grace time.Duration) {
	for i, vac := range before {
		switch {
		case !vac.streak:
		case !vac.expired && s.now == vac.since+grace:
			r.exactExpiry++
		case !vac.expired && s.now == vac.since+grace-1:
			r.oneBefore++
		case vac.expired && (i >= len(a.vacancy) || !a.vacancy[i].expired):
			r.expiredEnded++
		}
	}
	switch {
	case s.tick && changed:
		r.tickChanged++
	case !s.tick && changed:
		r.viewChanged++
	}
	if s.tick {
		return
	}
	switch {
	case s.note == "older view":
		r.ignoredOlder++
	case strings.HasPrefix(s.note, "invalid"):
		r.ignoredInvalid++
	case s.note == "same-version view" && s.view.Version == priorVersion:
		r.sameVersion++
	}
	if a.view.Version != s.view.Version || a.view.Validate() != nil {
		return
	}
	switch {
	case priorSlots > 0 && a.view.Slots == 0:
		r.toNoSlots++
	case priorSlots == 0 && a.view.Slots > 0:
		r.fromNoSlots++
	}
	at := a.wall.Add(a.now - a.appliedAt)
	for _, m := range a.view.Members {
		if m.JoinAt.Equal(at) || m.LeaveAt.Equal(at) {
			r.boundAtInstant++
			break
		}
	}
	if a.view.Slots == 0 {
		for _, m := range a.view.Members {
			if !m.Draining && !slices.Contains(a.keys, m.ID) && (!m.JoinAt.IsZero() || !m.LeaveAt.IsZero()) {
				r.boundExcluded++
				break
			}
		}
	}
	for s, h := range a.view.SlotHolders {
		if h != "" && !slices.ContainsFunc(a.view.Members, func(m Member) bool { return m.Slot == s }) {
			r.staleHolder++
			break
		}
	}
}

// minimizeEquiv removes steps from a failing sequence while it still fails, one at a time, and
// returns what is left. Removing a step keeps the readings in order.
func minimizeEquiv(timing Timing, steps []equivStep) []equivStep {
	for {
		shrunk := false
		for i := 0; i < len(steps); i++ {
			try := slices.Delete(slices.Clone(steps), i, i+1)
			if k, err := runEquiv(timing, try, nil); err != nil {
				steps, shrunk = try[:k+1], true
				i--
			}
		}
		if !shrunk {
			return steps
		}
	}
}

// formatEquiv formats steps one per line, numbered from 1.
func formatEquiv(steps []equivStep) string {
	var b strings.Builder
	for i, s := range steps {
		fmt.Fprintf(&b, "  %d. %v\n", i+1, s)
	}
	return b.String()
}

// TestATRTR01AssignerEquivMachine checks that an Assigner's assignment equals that of a
// slotless, non-draining machine after every step of equivSequences generated sequences of
// equivSteps views and readings (RTR-140, AT-RTR-01). A failure names the seed, which replays
// the sequence exactly, and the shortest failing sequence found by removing steps.
func TestATRTR01AssignerEquivMachine(t *testing.T) {
	var reach equivReach
	for seed := uint64(1); seed <= equivSequences; seed++ {
		timing := equivTimings[seed%uint64(len(equivTimings))]
		steps := equivSequence(seed)
		k, err := runEquiv(timing, steps, &reach)
		if err == nil {
			continue
		}
		var gen *equivGenError
		if errors.As(err, &gen) {
			t.Fatalf("seed %d step %d: %v", seed, k+1, err)
		}
		short := minimizeEquiv(timing, steps[:k+1])
		_, why := runEquiv(timing, short, nil)
		t.Fatalf("seed %d (timing %+v) differs after step %d:\n%s\nshortest failing sequence (%d steps), which fails with: %v\n%s",
			seed, timing, k+1, err, len(short), why, formatEquiv(short))
	}

	for _, c := range []struct {
		name  string
		count int
	}{
		{"step at exactly since + SlotGrace", reach.exactExpiry},
		{"step at since + SlotGrace − 1ns", reach.oneBefore},
		{"tick that changed an assignment", reach.tickChanged},
		{"view that changed an assignment", reach.viewChanged},
		{"expired slot held again or dropped", reach.expiredEnded},
		{"view from slots to none", reach.toNoSlots},
		{"view from no slots to some", reach.fromNoSlots},
		{"member left out by a bound", reach.boundExcluded},
		{"view applied at a member's JoinAt or LeaveAt", reach.boundAtInstant},
		{"older view", reach.ignoredOlder},
		{"invalid view", reach.ignoredInvalid},
		{"same-version view", reach.sameVersion},
		{"holder string of a member that left", reach.staleHolder},
	} {
		if c.count == 0 {
			t.Errorf("no sequence reached: %s", c.name)
		}
	}
	t.Logf("reach: %+v", reach)
}

// equivGenError is a failure of the generator rather than of the Assigner.
type equivGenError struct{ error }
