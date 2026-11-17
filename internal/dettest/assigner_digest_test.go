// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// This file holds the assigner digest case (AT-RTR-02): fixed sequences of views and readings
// fed to a core.Assigner, one line per step with the input, what the Assigner returned and its
// assignments. The sequences come from a small generator seeded by the sequence number, written
// here with integer arithmetic only, so that every platform builds the same inputs.

// assignerSequences and assignerSeqSteps are the number of sequences of the assigner case and
// the number of steps of each.
const (
	assignerSequences = 8
	assignerSeqSteps  = 40
)

// assignerDigestSpaces are the spaces of the assigner case, in the order its lines list them.
var assignerDigestSpaces = []core.AssignerSpace{{Name: "orders", Shards: 8}, {Name: "markets", Shards: 16}, {Name: "one", Shards: 1}}

// assignerDigestTiming is the assigner case's timing: small, so that vacancies expire within a
// sequence.
var assignerDigestTiming = core.Timing{
	LeaseDuration: 15 * time.Millisecond,
	RenewDeadline: 10 * time.Millisecond,
	RetryPeriod:   2 * time.Millisecond,
	SlotGrace:     20 * time.Millisecond,
}

// assignerDigestWall is the wall-clock time at reading 0 of the assigner case.
var assignerDigestWall = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// assignerInput is one step of an assigner sequence: a tick, or a view read at now and wall.
type assignerInput struct {
	tick bool
	now  time.Duration
	view core.View
	wall time.Time
}

// splitmix is SplitMix64, the assigner case's generator.
type splitmix uint64

func (s *splitmix) next() uint64 {
	*s += 0x9e3779b97f4a7c15
	z := uint64(*s)
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}

// intn returns a number in [0, n), for n > 0.
func (s *splitmix) intn(n int) int {
	return int(s.next() % uint64(n)) //nolint:gosec // n is positive, and the remainder is below n, which is an int
}

// assignerGen generates one assigner sequence. Members m0 … m4, of which m0 … m2 start present, join and leave, drain, get
// JoinAt and LeaveAt bounds near the current wall-clock time; slots are added, dropped, vacated
// and claimed; readings advance by varied amounts, often to exactly SlotGrace, or SlotGrace − 1ns,
// after a view.
type assignerGen struct {
	r        splitmix
	now      time.Duration
	version  uint64
	present  [5]bool
	draining [5]bool
	joinAt   [5]time.Time
	leaveAt  [5]time.Time
	holders  []int // slot -> member holding it, -1 vacant
	gens     [5]int
	viewAt   time.Duration // reading of the latest view
}

// assignerSequence returns sequence seq of the assigner case.
func assignerSequence(seq int) []assignerInput {
	g := &assignerGen{r: splitmix(seq), present: [5]bool{true, true, true}} //nolint:gosec // seq is below assignerSequences
	out := make([]assignerInput, assignerSeqSteps)
	for i := range out {
		if i > 0 {
			g.advance()
		}
		out[i] = g.step()
	}
	return out
}

func (g *assignerGen) advance() {
	grace := assignerDigestTiming.SlotGrace
	switch g.r.intn(8) {
	case 0:
	case 1:
		g.now++
	case 2, 3:
		g.now += time.Duration(g.r.intn(int(grace/time.Millisecond))) * time.Millisecond
	case 4:
		if t := g.viewAt + grace; t > g.now {
			g.now = t
		}
	case 5:
		if t := g.viewAt + grace - 1; t > g.now {
			g.now = t
		}
	case 6:
		g.now += grace
	default:
		g.now += 2*grace + time.Duration(g.r.intn(1000))*time.Microsecond
	}
}

func (g *assignerGen) step() assignerInput {
	wall := assignerDigestWall.Add(g.now)
	switch g.r.intn(10) {
	case 0, 1, 2:
		return assignerInput{tick: true, now: g.now}
	case 3:
		if g.version > 0 {
			// An older view, which the Assigner ignores.
			return assignerInput{now: g.now, view: g.view(g.version - 1), wall: wall}
		}
	case 4:
		// A view that is not valid, which the Assigner ignores.
		v := g.view(g.version + 1)
		v.SlotHolders = append(v.SlotHolders, "")
		return assignerInput{now: g.now, view: v, wall: wall}
	}
	g.change()
	if g.r.intn(4) > 0 {
		g.version++
	}
	g.viewAt = g.now
	return assignerInput{now: g.now, view: g.view(g.version), wall: wall}
}

// change makes one change to the group.
func (g *assignerGen) change() {
	i := g.r.intn(len(g.present))
	slot := slices.Index(g.holders, i)
	switch g.r.intn(7) {
	case 0:
		g.present[i] = !g.present[i]
		if !g.present[i] && slot >= 0 {
			g.holders[slot] = -1
		}
	case 1:
		g.draining[i] = !g.draining[i]
	case 2:
		at := assignerDigestWall.Add(g.now)
		d := []time.Duration{-1, 0, 1, assignerDigestTiming.SlotGrace, assignerDigestTiming.SlotGrace - 1}[g.r.intn(5)]
		switch g.r.intn(3) {
		case 0:
			g.joinAt[i], g.leaveAt[i] = at.Add(d), time.Time{}
		case 1:
			g.joinAt[i], g.leaveAt[i] = time.Time{}, at.Add(d)
		default:
			g.joinAt[i], g.leaveAt[i] = time.Time{}, time.Time{}
		}
	case 3:
		n := g.r.intn(4)
		for len(g.holders) < n {
			g.holders = append(g.holders, -1)
		}
		g.holders = g.holders[:n]
	case 4:
		if len(g.holders) > 0 {
			g.holders[g.r.intn(len(g.holders))] = -1
		}
	default:
		if s := slices.Index(g.holders, -1); s >= 0 && g.present[i] && slot < 0 {
			g.holders[s] = i
			g.gens[i]++
		}
	}
}

// view returns the group's view with version version.
func (g *assignerGen) view(version uint64) core.View {
	v := core.View{Version: version, Slots: len(g.holders), SlotHolders: make([]string, len(g.holders))}
	for s, i := range g.holders {
		if i >= 0 {
			v.SlotHolders[s] = "m" + strconv.Itoa(i) + "/g" + strconv.Itoa(g.gens[i])
		}
	}
	for i := range g.present {
		if g.present[i] {
			id := "m" + strconv.Itoa(i)
			v.Members = append(v.Members, core.Member{
				ID: id, Draining: g.draining[i], Slot: slices.Index(g.holders, i), JoinAt: g.joinAt[i], LeaveAt: g.leaveAt[i],
			})
		}
	}
	return v
}

// assignerSteps feeds every assigner sequence, in order, to a new Assigner, and calls visit
// after each step with the sequence number, the step number counting from 1, the input, the
// Assigner and what the step returned.
func assignerSteps(visit func(seq, n int, in assignerInput, a *core.Assigner, changed bool)) {
	for seq := range assignerSequences {
		a, err := core.NewAssigner(assignerDigestSpaces, assignerDigestTiming)
		if err != nil {
			panic(err)
		}
		for i, in := range assignerSequence(seq) {
			var changed bool
			if in.tick {
				changed = a.Tick(in.now)
			} else {
				changed = a.ApplyView(in.view, in.now, in.wall)
			}
			visit(seq, i+1, in, a, changed)
		}
	}
}

// assignerLines adds the assigner case's lines (AT-RTR-02): for each step n of each sequence
// seq, one line: seq, n, the input, then what the step returned, NextWake, Keys and the
// assignment of each space. Readings and times are integer nanoseconds, times from
// assignerDigestWall; a shard with no key is "-".
func assignerLines(add func(line string)) {
	assignerSteps(func(seq, n int, in assignerInput, a *core.Assigner, changed bool) {
		var b strings.Builder
		b.WriteString(strconv.Itoa(seq) + " " + strconv.Itoa(n) + " " + assignerInputText(in))
		b.WriteString(" | " + strconv.FormatBool(changed))
		if at, ok := a.NextWake(); ok {
			b.WriteString(" wake " + strconv.FormatInt(int64(at), 10))
		} else {
			b.WriteString(" wake -")
		}
		b.WriteString(" keys " + dashJoin(a.Keys()))
		for _, s := range assignerDigestSpaces {
			b.WriteString(" " + s.Name + " " + dashJoin(a.Assignment(s.Name)))
		}
		add(b.String())
	})
}

// assignerInputText formats in for an assigner line.
func assignerInputText(in assignerInput) string {
	if in.tick {
		return "tick " + strconv.FormatInt(int64(in.now), 10)
	}
	v := in.view
	parts := []string{
		"view", strconv.FormatInt(int64(in.now), 10),
		"wall", strconv.FormatInt(int64(in.wall.Sub(assignerDigestWall)), 10),
		"version", strconv.FormatUint(v.Version, 10),
		"slots", strconv.Itoa(v.Slots), dashJoin(v.SlotHolders),
		"members",
	}
	members := make([]string, len(v.Members))
	for i, m := range v.Members {
		s := m.ID + ":" + strconv.Itoa(m.Slot)
		if m.Draining {
			s += ":d"
		}
		if !m.JoinAt.IsZero() {
			s += ":j" + strconv.FormatInt(int64(m.JoinAt.Sub(assignerDigestWall)), 10)
		}
		if !m.LeaveAt.IsZero() {
			s += ":l" + strconv.FormatInt(int64(m.LeaveAt.Sub(assignerDigestWall)), 10)
		}
		members[i] = s
	}
	return strings.Join(parts, " ") + " " + dashJoin(members)
}

// dashJoin joins ss with ",", writing "-" for an empty string and for an empty list.
func dashJoin(ss []string) string {
	if len(ss) == 0 {
		return "-"
	}
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s
		if s == "" {
			out[i] = "-"
		}
	}
	return strings.Join(out, ",")
}

// TestATRTR02AssignerDigest checks the assigner case (AT-RTR-02): its count and first line,
// that after every step the Assigner's assignment equals that of a machine that holds no slot
// and does not drain, fed the same inputs, and that the sequences reach a vacancy expiry, an
// ignored view and a view with no slots after one with slots.
func TestATRTR02AssignerDigest(t *testing.T) {
	lines := caseLines(t, "assigner")
	if want := assignerSequences * assignerSeqSteps; len(lines) != want {
		t.Fatalf("assigner has %d lines, want %d", len(lines), want)
	}
	const first = "0 1 view 0 wall 0 version 1 slots 0 - members m0:-1:l19999999,m1:-1,m2:-1 | true wake - keys m0,m1,m2 " +
		"orders m1,m1,m2,m2,m1,m0,m0,m2 markets m2,m2,m1,m0,m1,m0,m0,m0,m2,m1,m0,m2,m0,m2,m1,m1 one m2"
	if lines[0] != first {
		t.Errorf("first assigner line = %q, want %q", lines[0], first)
	}

	specs := make([]core.SpaceSpec, len(assignerDigestSpaces))
	for i, s := range assignerDigestSpaces {
		specs[i] = core.SpaceSpec{Name: s.Name, Shards: s.Shards}
	}
	var m *core.Machine
	var expiries, ignored, unslotted int
	var slotted bool
	assignerSteps(func(seq, n int, in assignerInput, a *core.Assigner, changed bool) {
		if n == 1 {
			m = core.NewMachine(core.MachineConfig{Holder: "router/h", Member: "router", Spaces: specs, Timing: assignerDigestTiming})
			slotted = false
		}
		if in.tick {
			m.Step(core.Tick{Now: in.now})
			if changed {
				expiries++
			}
		} else {
			m.Step(core.ViewChanged{Now: in.now, View: in.view, Wall: in.wall})
			if m.Status().ViewVersion != in.view.Version || in.view.Validate() != nil {
				ignored++
			} else {
				if slotted && in.view.Slots == 0 {
					unslotted++
				}
				slotted = in.view.Slots > 0
			}
		}
		for _, s := range assignerDigestSpaces {
			if got, want := a.Assignment(s.Name), m.Assignment(s.Name); !slices.Equal(got, want) {
				t.Errorf("sequence %d step %d: Assignment(%q) = %q, machine's %q", seq, n, s.Name, got, want)
			}
		}
	})
	if expiries == 0 || ignored == 0 || unslotted == 0 {
		t.Errorf("the sequences reach %d vacancy expiries, %d ignored views and %d views dropping the slots; want at least one of each", expiries, ignored, unslotted)
	}
}
