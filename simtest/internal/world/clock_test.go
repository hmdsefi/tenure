// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
)

// rawSim returns a bare simulation with three nodes that do nothing, at the drifts 0, ppm and
// -ppm, and its clocks. The nodes boot at the first events.
func rawSim(t *testing.T, ppm int32) (*kernel.Sim, []*kernel.Node, *clocks) {
	t.Helper()
	s := kernel.New(kernel.Config{Seed: 1})
	var nodes []*kernel.Node
	for i, d := range []int32{0, ppm, -ppm} {
		nodes = append(nodes, s.AddNode("n"+string(rune('0'+i)), func(*kernel.Node) {}, kernel.WithClock(0, d)))
	}
	c := newClocks(s)
	c.track()
	return s, nodes, c
}

// reading returns what a clock that has run at ppm since 0 reads at the global time g.
func reading(g time.Duration, ppm int32) time.Duration { return g + gain(g, ppm) }

// TestATSIM05Clocks checks the clocks: one stream, tenure/clock, gives in creation order each
// node's drift in [-Rho, Rho] and, for members and replacements only, a wall offset in
// [-WallSkew, WallSkew], with no draw for a range of one point; the monotonic reading is the
// node's local time and keeps counting across a crash; the wall clock is the epoch plus the
// reading plus the offset, without one for the store and the fence; with WallSteps, each member
// and replacement steps three times before R, at times and by sizes from tenure/wall/<member>,
// each step with tenure.wall_step; no clock ever jumps. The steps were written for this task
// from the issue and the design note.
func TestATSIM05Clocks(t *testing.T) {
	tests := []struct {
		name string
		sc   Scenario
	}{
		{"drifts, offsets and steps", Scenario{Members: 3, Slots: 3, Rollout: rollout,
			Rho: 100_000, WallSkew: 2 * time.Second, WallSteps: 500 * time.Millisecond}},
		{"offsets only", Scenario{Members: 2, WallSkew: time.Second}},
		{"drifts only", Scenario{Members: 2, Rho: 300_000}},
		{"exact clocks", Scenario{Members: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runWorld(t, fullTrace(180*time.Second), tt.sc, func(fw *faultline.World, w *World) {
				t := fw.T()
				sc := w.Scenario()
				r := stream(fw.Seed(), streamClock)
				draw := func(bound int64) int64 {
					if bound == 0 {
						return 0
					}
					return symmetric(r, bound)
				}
				offsets := map[*kernel.Node]time.Duration{}
				for _, n := range fw.Sim.Nodes() {
					want := int32(draw(int64(sc.Rho))) //nolint:gosec // within [-Rho, Rho]
					if n.Drift() != want || want < -sc.Rho || want > sc.Rho {
						t.Errorf("%s: drift %d, want %d, the next draw of %s, in [-%d, %d]", n.Name(), n.Drift(), want, streamClock, sc.Rho, sc.Rho)
					}
					if m := w.memberOf(n); m != nil {
						offsets[n] = time.Duration(draw(int64(sc.WallSkew)))
						if m.wall != offsets[n] || m.wall < -sc.WallSkew || m.wall > sc.WallSkew {
							t.Errorf("%s: wall offset %v, want %v, in ±%v", n.Name(), m.wall, offsets[n], sc.WallSkew)
						}
					}
				}

				// The steps of each member's wall clock, from its own stream.
				type step struct {
					node  kernel.NodeID
					at    kernel.Time
					delta time.Duration
				}
				var want []step
				if sc.WallSteps > 0 {
					for _, m := range w.Members() {
						r := stream(fw.Seed(), streamWall+m.Name)
						for range wallSteps {
							at := kernel.Time(r.Int64N(int64(w.Recovery())))
							want = append(want, step{m.Node.ID(), at, time.Duration(symmetric(r, int64(sc.WallSteps)))})
						}
					}
				}

				// A member crashes and restarts; its clock keeps counting.
				m1 := w.Members()[0]
				fw.Sim.At(at(2*time.Second), "test/crash", m1.Node.Crash)
				fw.Sim.At(at(4*time.Second), "test/restart", m1.Node.Restart)
				const end = 170 * time.Second
				fw.RunFor(end)

				for _, n := range fw.Sim.Nodes() {
					if got, want := Mono(n), reading(end, n.Drift()); got != want || got != time.Duration(n.LocalTime()) {
						t.Errorf("%s: monotonic reading %v, want %v", n.Name(), got, want)
					}
				}
				offset := map[kernel.NodeID]time.Duration{}
				for n, o := range offsets {
					offset[n.ID()] = o
				}
				var got []step
				for _, rec := range records(fw.Sim, kindWallStep) {
					delta, _ := time.ParseDuration(attrValue(rec, "delta_ns") + "ns")
					offset[rec.Node] += delta
					got = append(got, step{rec.Node, rec.At, delta})
					if a := attrs(rec); len(a) != 2 || a[1] != "offset_ns="+recNanos(offset[rec.Node]) {
						t.Errorf("tenure.wall_step %v, want delta_ns and offset_ns %v", a, offset[rec.Node])
					}
					if delta < -sc.WallSteps || delta > sc.WallSteps || rec.At >= w.Recovery() {
						t.Errorf("a step of %v at %v, want within ±%v before %v", delta, rec.At, sc.WallSteps, w.Recovery())
					}
				}
				byNodeAndTime := func(a, b step) int {
					return cmp.Or(cmp.Compare(a.node, b.node), cmp.Compare(a.at, b.at))
				}
				perNode := map[kernel.NodeID]int{}
				for _, s := range got {
					perNode[s.node]++
				}
				steps := 0
				if sc.WallSteps > 0 {
					steps = 3
				}
				for _, m := range w.Members() {
					if n := perNode[m.Node.ID()]; n != steps {
						t.Errorf("%s: %d wall steps, want 3 with WallSteps, else none", m.Name, n)
					}
				}
				slices.SortStableFunc(got, byNodeAndTime)
				slices.SortStableFunc(want, byNodeAndTime)
				if !slices.Equal(got, want) {
					t.Errorf("wall steps:\n got %v\nwant %v", got, want)
				}
				for _, m := range w.Members() {
					if m.wall != offset[m.Node.ID()] || !m.Wall().Equal(kernel.Epoch.Add(Mono(m.Node)+m.wall)) ||
						!w.Wall(m.Node).Equal(m.Wall()) {
						t.Errorf("%s: wall %v with offset %v, want offset %v", m.Name, m.Wall(), m.wall, offset[m.Node.ID()])
					}
				}
				for _, n := range []*kernel.Node{w.Store(), w.Fence()} {
					if !w.Wall(n).Equal(kernel.Epoch.Add(Mono(n))) {
						t.Errorf("%s: wall %v, want the epoch plus %v", n.Name(), w.Wall(n), Mono(n))
					}
				}
				if jumps := records(fw.Sim, "kernel.clock_jump"); len(jumps) != 0 {
					t.Errorf("clocks jumped: %v", jumps)
				}
			})
		})
	}
}

// TestMonoTimerFollowsDrift checks that a mono timer fires at a reading in [T, T + 1 µs] when
// its node's drift changes to 200 000 ppm while it waits, on every node and for 20 seeds, and
// beyond T + 1 ms with the observer that re-arms it disabled.
func TestMonoTimerFollowsDrift(t *testing.T) {
	for _, tt := range []struct {
		name string
		off  bool
	}{{"observer", false}, {"observer disabled", true}} {
		t.Run(tt.name, func(t *testing.T) {
			opts := faultline.Options{Seeds: 20, Duration: 30 * time.Second}
			runWorld(t, opts, Scenario{Rho: 100_000}, func(fw *faultline.World, w *World) {
				t := fw.T()
				w.clocks.off = tt.off
				nodes := fw.Sim.Nodes()
				set := make([]time.Duration, len(nodes))
				fired := make([]time.Duration, len(nodes))
				fw.Sim.At(at(time.Second), "test/set", func() {
					for i, n := range nodes {
						set[i] = Mono(n) + 5*time.Second
						tm := w.setTimer(n, set[i], "test/timer", func() { fired[i] = Mono(n) })
						if !tm.pending() || tm.target() != set[i] {
							t.Errorf("%s: a timer for %v is not pending", n.Name(), set[i])
						}
					}
				})
				fw.Sim.At(at(2*time.Second), "test/drift", func() {
					for _, n := range nodes {
						n.SetDrift(200_000)
					}
				})
				fw.RunFor(10 * time.Second)
				for i, n := range nodes {
					late := fired[i] - set[i]
					switch {
					case fired[i] == 0:
						t.Errorf("%s: the timer for %v never fired", n.Name(), set[i])
					case !tt.off && (late < 0 || late > time.Microsecond):
						t.Errorf("%s: the timer for %v fired at %v, want within [T, T + 1µs]", n.Name(), set[i], fired[i])
					case tt.off && late <= time.Millisecond:
						t.Errorf("%s: without the observer, the timer for %v fired at %v, want after T + 1ms", n.Name(), set[i], fired[i])
					}
				}
			})
		})
	}
}

// TestMonoTimers checks the rest of the timer API: a timer fires once; cancel stops it; a
// reading that has passed fires at once; re-arming keeps the order in which timers were set; a
// crash drops the node's timers, a restart lets it set new ones, and a node that is down sets
// none; a paused node fires when it resumes.
func TestMonoTimers(t *testing.T) {
	s, nodes, c := rawSim(t, 100_000)
	n := nodes[1]
	s.RunFor(time.Second)
	var order []string
	note := func(name string) func() { return func() { order = append(order, name) } }
	a := c.set(n, 3*time.Second, "test/a", note("a"))
	b := c.set(n, 3*time.Second+time.Microsecond, "test/b", note("b"))
	gone := c.set(n, 2*time.Second, "test/gone", note("gone"))
	if !gone.cancel() || gone.cancel() || gone.pending() {
		t.Errorf("cancel of a pending timer, then again: want true, then false")
	}
	past := c.set(n, Mono(n)-time.Millisecond, "test/past", note("past"))
	s.At(at(1500*time.Millisecond), "test/drift", func() { n.SetDrift(-200_000) })
	s.RunFor(5 * time.Second)
	if want := []string{"past", "a", "b"}; !slices.Equal(order, want) {
		t.Errorf("fired %v, want %v", order, want)
	}
	if a.pending() || b.pending() || past.pending() || a.cancel() {
		t.Errorf("a timer that fired is still pending")
	}

	order = nil
	d := c.set(n, Mono(n)+time.Second, "test/dropped", note("dropped"))
	s.At(s.Now().Add(100*time.Millisecond), "test/crash", n.Crash)
	s.RunFor(500 * time.Millisecond)
	down := c.set(n, Mono(n)+time.Second, "test/down", note("down"))
	if d.pending() || down.pending() || len(c.of(n).timers) != 0 {
		t.Errorf("after a crash: dropped pending %v, down pending %v, %d timers", d.pending(), down.pending(), len(c.of(n).timers))
	}
	s.At(s.Now().Add(100*time.Millisecond), "test/restart", n.Restart)
	s.At(s.Now().Add(200*time.Millisecond), "test/set", func() { c.set(n, Mono(n)+time.Second, "test/new", note("new")) })
	s.RunFor(3 * time.Second)
	if want := []string{"new"}; !slices.Equal(order, want) {
		t.Errorf("fired %v after a crash and a restart, want %v", order, want)
	}

	order = nil
	want := Mono(n) + time.Second
	var fired time.Duration
	c.set(n, want, "test/paused", func() { fired = Mono(n) })
	s.At(s.Now().Add(100*time.Millisecond), "test/pause", n.Pause)
	s.At(s.Now().Add(3*time.Second), "test/resume", n.Resume)
	s.RunFor(4 * time.Second)
	if fired < want+time.Second {
		t.Errorf("a timer of a paused node fired at %v, want at its resume, after %v", fired, want+time.Second)
	}
	if err := s.Err(); err != nil {
		t.Errorf("the run failed: %v", err)
	}
}

// TestMonoTimerEdges checks the timer API around a firing and the work of the look: inside its
// callback a timer is no longer pending and cannot be cancelled, and the callback may cancel
// another timer and set a new one; a look re-arms nothing while no drift changes, and re-arms
// a node's timers in the order they were set; a timer that came due on a paused node and was
// re-armed by a drift change fires once, at the resume.
func TestMonoTimerEdges(t *testing.T) {
	s, nodes, c := rawSim(t, 100_000)
	n := nodes[1]
	s.RunFor(time.Second)
	var order []string
	note := func(name string) func() { return func() { order = append(order, name) } }

	var x, y *monoTimer
	x = c.set(n, Mono(n)+time.Second, "test/x", func() {
		order = append(order, "x")
		if x.pending() || x.cancel() {
			t.Errorf("inside its callback, the timer is still pending")
		}
		if !y.cancel() {
			t.Errorf("a callback could not cancel a pending timer")
		}
		c.set(n, Mono(n)+time.Millisecond, "test/z", note("z"))
	})
	y = c.set(n, Mono(n)+time.Second+time.Millisecond/2, "test/y", note("y"))
	s.RunFor(2 * time.Second)
	if want := []string{"x", "z"}; !slices.Equal(order, want) {
		t.Errorf("fired %v, want %v", order, want)
	}

	// No drift change: the kernel events of the timers stay as they were set.
	order = nil
	a := c.set(n, Mono(n)+2*time.Second, "test/a", note("a"))
	b := c.set(n, Mono(n)+time.Second, "test/b", note("b"))
	ea, eb := a.event, b.event
	s.At(s.Now().Add(100*time.Millisecond), "test/other", func() {})
	s.RunFor(200 * time.Millisecond)
	if a.event != ea || b.event != eb {
		t.Errorf("a look without a drift change re-armed the timers")
	}
	// A drift change re-arms them in the order they were set, not by their readings.
	s.At(s.Now().Add(100*time.Millisecond), "test/drift", func() { n.SetDrift(300_000) })
	s.RunFor(200 * time.Millisecond)
	if a.event == ea || b.event == eb || a.event > b.event {
		t.Errorf("after a drift change: events %d and %d, were %d and %d; want new ones in the order set", a.event, b.event, ea, eb)
	}
	s.RunFor(3 * time.Second)
	if want := []string{"b", "a"}; !slices.Equal(order, want) {
		t.Errorf("fired %v, want %v", order, want)
	}

	// A paused node: the timer comes due and waits; a drift change re-arms it; it fires once.
	order = nil
	due := Mono(n) + 200*time.Millisecond
	var fired time.Duration
	c.set(n, due, "test/paused", func() { order = append(order, "paused"); fired = Mono(n) })
	start := s.Now()
	s.At(start.Add(100*time.Millisecond), "test/pause", n.Pause)
	s.At(start.Add(time.Second), "test/drift", func() { n.SetDrift(-100_000) })
	s.At(start.Add(2*time.Second), "test/resume", n.Resume)
	s.RunFor(3 * time.Second)
	if want := []string{"paused"}; !slices.Equal(order, want) || fired < due {
		t.Errorf("a re-armed timer of a paused node fired %v at %v, want once at its resume, after %v", order, fired, due)
	}
	if err := s.Err(); err != nil {
		t.Errorf("the run failed: %v", err)
	}
}

// TestLookNoFalseJump checks that the jump check of the look allows the kernel's rounding:
// drifts that change at random times, inside events and between them, never fail the run.
func TestLookNoFalseJump(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8)) //nolint:gosec // a test's fixed stream

	drift := func() int32 { return int32(r.Int64N(1_000_001)) - 500_000 } //nolint:gosec // in range
	wait := func() time.Duration { return time.Duration(r.Int64N(int64(time.Second))) + 1 }
	for i := range 300 {
		s := kernel.New(kernel.Config{Seed: uint64(i)}) //nolint:gosec // i is small
		n := s.AddNode("n", func(*kernel.Node) {}, kernel.WithClock(0, drift()))
		newClocks(s).track()
		for range 5 {
			// An event at a random time, and a drift change in an event of its own.
			a, b := wait(), wait()
			s.At(s.Now().Add(a), "test/event", func() {})
			ppm, same := drift(), r.IntN(2) == 0
			s.At(s.Now().Add(b), "test/drift", func() {
				if same {
					ppm = n.Drift()
				}
				n.SetDrift(ppm)
			})
			s.RunFor(max(a, b) + wait())
			// A change between two events, a while after the last one and before the next.
			s.RunFor(wait())
			ppm = drift()
			if r.IntN(2) == 0 {
				ppm = n.Drift() // the same drift again, which still restarts the kernel's count
			}
			n.SetDrift(ppm)
			c := wait()
			s.At(s.Now().Add(c), "test/event", func() {})
			s.RunFor(c + wait())
		}
		if err := s.Err(); err != nil {
			t.Fatalf("run %d failed: %v", i, err)
		}
	}
}

// TestWholeSeconds checks the rounding of the default quiet window.
func TestWholeSeconds(t *testing.T) {
	for d, want := range map[time.Duration]time.Duration{
		1:                               time.Second,
		time.Second:                     time.Second,
		time.Second + 1:                 2 * time.Second,
		21_045_000_000:                  22 * time.Second,
		21 * time.Second:                21 * time.Second,
		math.MaxInt64 - time.Second:     9_223_372_036 * time.Second,
		math.MaxInt64 - time.Second + 1: math.MaxInt64 - time.Second + 1,
		math.MaxInt64:                   math.MaxInt64,
	} {
		if got := wholeSeconds(d); got != want {
			t.Errorf("wholeSeconds(%d) = %d, want %d", d, got, want)
		}
	}
}

// TestGain checks gain against the kernel's clocks.
func TestGain(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a test's fixed stream
	for range 2000 {
		ppm := int32(r.Int64N(int64(kernel.MaxDriftPPM-kernel.MinDriftPPM)+1)) + kernel.MinDriftPPM //nolint:gosec // in range
		d := time.Duration(r.Int64N(int64(1000 * time.Hour)))
		s := kernel.New(kernel.Config{})
		n := s.AddNode("n", func(*kernel.Node) {}, kernel.WithClock(0, ppm))
		s.RunFor(d)
		if got, want := reading(d, ppm), time.Duration(n.LocalTime()); got != want {
			t.Fatalf("at %v and %d ppm: gain gives %v, the kernel %v", d, ppm, got, want)
		}
	}
}

// TestSymmetric checks that symmetric covers [-bound, bound], up to the largest bound, with
// one Uint64N.
func TestSymmetric(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // a test's fixed stream
	seen := map[int64]bool{}
	for range 1000 {
		v := symmetric(r, 2)
		if v < -2 || v > 2 {
			t.Fatalf("symmetric(2) = %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 5 {
		t.Errorf("symmetric(2) gave %v, want all of -2 to 2", seen)
	}
	if v := symmetric(r, 0); v != 0 {
		t.Errorf("symmetric(0) = %d", v)
	}
	a, b := rand.New(rand.NewPCG(5, 6)), rand.New(rand.NewPCG(5, 6)) //nolint:gosec // a test's fixed stream
	for _, bound := range []int64{1 << 62, math.MaxInt64} {
		v := symmetric(a, bound)
		if want := int64(b.Uint64N(2*uint64(bound)+1) - uint64(bound)); v != want { //nolint:gosec // wraps as symmetric does
			t.Errorf("symmetric(%d) = %d, want %d", bound, v, want)
		}
	}
}
