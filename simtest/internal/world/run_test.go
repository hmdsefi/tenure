// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"

	"github.com/hmdsefi/tenure/core"
)

// fixedLatency is the one-way delay of every link in the tests that work out when a write went
// out from when the fence took it.
const fixedLatency = 5 * time.Millisecond

// fixedNet returns options that keep every record, for a run of d on links of fixedLatency and
// no jitter.
func fixedNet(d time.Duration) faultline.Options {
	opts := fullTrace(d)
	opts.Net = simnet.Config{Default: simnet.Link{Latency: fixedLatency}}
	return opts
}

// runRecords returns the tenure.run records of node, in trace order.
func runRecords(sim *kernel.Sim, node kernel.NodeID) []kernel.Record {
	var out []kernel.Record
	for _, r := range records(sim, kindRun) {
		if r.Node == node {
			out = append(out, r)
		}
	}
	return out
}

// TestATSIM21RunRestarts checks AT-SIM-21 in a world of one member with no drift, so that the
// member's readings are the global times. The steps were written for this task from the issue
// and the design note, since the original acceptance-test text is lost:
//
//   - restarts: the fence of book/0, a fenced space, is raised to the largest term before the
//     run starts, so every write of the run is rejected and every call returns at once; the
//     restarts come RetryPeriod, 2·RetryPeriod, 4·RetryPeriod, then LeaseDuration after the
//     returns before them, with records start, raise, return, restart, raise, return, …;
//   - expiry: in an unfenced space, the member is cut off from the store at 10 s, so its
//     session deadline D(h) stops moving, and the machine stops the run at D(h). With a stop
//     delay of up to 10 s the run may outlive its StopRun, and it stops writing at its first
//     write time at or after D(h): every write the fence takes went out before D(h), and the
//     call returns with "check: expired" at that write time, at most RetryPeriod after the reply
//     to the last write, unless the stop delay ended first; in at least one of five seeds it
//     does not. With IgnoreCheck the run writes after D(h) all the same, in at least one seed.
func TestATSIM21RunRestarts(t *testing.T) {
	forRules(t, func(t *testing.T, rule Takeover) {
		t.Run("restarts", func(t *testing.T) { testRunRestarts(t, rule) })
		for _, ignore := range []bool{false, true} {
			t.Run("expiry/ignore="+strconv.FormatBool(ignore), func(t *testing.T) { testRunExpiry(t, rule, ignore) })
		}
	})
}

func testRunRestarts(t *testing.T, rule Takeover) {
	sc := Scenario{Takeover: rule, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1, Fenced: true}}}
	runWorld(t, fixedNet(60*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		w.fenceSrv.model.raise("book", 0, ^core.Term(0))
		fw.RunFor(60*time.Second - time.Nanosecond)

		tm := w.Scenario().Timing
		want := []time.Duration{tm.RetryPeriod, 2 * tm.RetryPeriod, 4 * tm.RetryPeriod, tm.LeaseDuration, tm.LeaseDuration}
		var ops []string
		var gaps []time.Duration
		var returned kernel.Time
		for _, r := range runRecords(fw.Sim, w.Members()[0].Node.ID()) {
			op := attrValue(r, "op")
			ops = append(ops, op)
			switch op {
			case runOpReturn:
				if r.Text != runReturnRejected {
					t.Errorf("return %q at %v, want %q", r.Text, r.At, runReturnRejected)
				}
				returned = r.At
			case runOpRestart:
				gaps = append(gaps, r.At.Sub(returned))
			}
		}
		if len(gaps) < len(want) {
			t.Fatalf("%d restarts %v, want at least %d; ops %v", len(gaps), gaps, len(want), ops)
		}
		for i, w := range want {
			// A mono timer fires at its reading or up to a microsecond later.
			if g := gaps[i]; g < w || g > w+time.Microsecond {
				t.Errorf("restart %d came %v after its return, want %v", i+1, g, w)
			}
		}
		// start, then raise and return per call, a restart before every call but the first.
		if len(ops) < 3 || ops[0] != runOpStart || ops[1] != runOpRaise || ops[2] != runOpReturn {
			t.Fatalf("ops %v, want start, raise, return first", ops)
		}
		for i := 3; i+2 < len(ops); i += 3 {
			if ops[i] != runOpRestart || ops[i+1] != runOpRaise || ops[i+2] != runOpReturn {
				t.Fatalf("ops %v from %d, want restart, raise, return", ops[i:], i)
			}
		}
	})
}

func testRunExpiry(t *testing.T, rule Takeover, ignore bool) {
	const cut = 10 * time.Second
	sc := Scenario{
		Takeover: rule, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1}},
		StopDelay: 10 * time.Second, IgnoreCheck: ignore,
	}
	opts := fixedNet(40 * time.Second)
	opts.Seeds = 5
	// The seeds whose run reached a write time at or after D(h) before its stop delay ended:
	// it returned there, or, with IgnoreCheck, wrote.
	returns := 0
	runWorld(t, opts, sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		m := w.Members()[0]
		fw.Sim.At(kernel.Time(cut), "test/cut", func() {
			fw.Net.Cut(m.Node.ID(), w.Store().ID())
			fw.Net.Cut(w.Store().ID(), m.Node.ID())
		})
		fw.RunFor(40*time.Second - time.Nanosecond)

		// D(h): the latest deadline the machine reported.
		var deadline time.Duration
		for _, r := range records(fw.Sim, kindAction) {
			if r.Node == m.Node.ID() && (isAction(r, "ExtendDeadline") || isAction(r, "StartRun")) {
				deadline = max(deadline, nanosAttr(t, r, "deadline_ns"))
			}
		}
		if deadline <= cut || deadline > cut+w.Scenario().Timing.RenewDeadline {
			t.Fatalf("D(h) = %v, want in (%v, %v]", deadline, cut, cut+w.Scenario().Timing.RenewDeadline)
		}
		var lastBefore time.Duration
		after := 0
		for _, r := range records(fw.Sim, kindWrite) {
			if attrValue(r, "result") != "accepted" {
				continue
			}
			sent := time.Duration(r.At) - fixedLatency
			if sent >= deadline {
				after++
			} else {
				lastBefore = max(lastBefore, sent)
			}
		}
		if lastBefore == 0 {
			t.Fatal("no write before D(h)")
		}
		if ignore {
			if after > 0 {
				returns++ // the run wrote past D(h), as it ignores Check
			}
			return
		}
		if after != 0 {
			t.Errorf("%d writes went out at or after D(h) %v", after, deadline)
		}
		var ret, stopped time.Duration
		for _, r := range runRecords(fw.Sim, m.Node.ID()) {
			switch op := attrValue(r, "op"); {
			case op == runOpReturn && r.Text == runReturnCheck+core.StopExpired.String() && ret == 0:
				ret = time.Duration(r.At)
			case op == runOpStopped && stopped == 0:
				stopped = time.Duration(r.At)
			}
		}
		// The first write time at or after D(h) follows the reply to the last write before it
		// by at most RetryPeriod, and comes at or after D(h). A run whose stop delay ended
		// before that write time stopped without it.
		next := lastBefore + 2*fixedLatency + w.Scenario().Timing.RetryPeriod + time.Microsecond
		switch {
		case ret != 0:
			returns++
			if ret < deadline || ret > next {
				t.Errorf("the run returned at %v (last write before at %v), want at its first write time at or after D(h) %v",
					ret, lastBefore, deadline)
			}
		case stopped == 0 || stopped > next:
			t.Errorf("the run neither returned at D(h) %v nor stopped before its next write time (stopped at %v)", deadline, stopped)
		}
	})
	switch {
	case returns > 0:
	case ignore:
		t.Error("IgnoreCheck: in no seed did the run write at or after D(h)")
	default:
		t.Error("in no seed did the run reach a write time at or after D(h) before it stopped")
	}
}

// TestATSIM22StopDelay checks AT-SIM-22 with a stop delay of 3 s in a world of five members:
// m5 crashes at 2 s and comes back at 20 s, so shards move to the others and back. Every
// RunStopped is stepped within [0, 3 s] of its StopRun on the member's clock, and each run's
// tenure.run stop and stopped records come in that order. The steps were written for this task
// from the issue and the design note, since the original acceptance-test text is lost.
func TestATSIM22StopDelay(t *testing.T) {
	forRules(t, testATSIM22StopDelay)
}

func testATSIM22StopDelay(t *testing.T, rule Takeover) {
	const stopDelay = 3 * time.Second
	runWorld(t, fullTrace(40*time.Second), Scenario{Takeover: rule, StopDelay: stopDelay}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m5 := w.Members()[4]
		fw.Sim.At(kernel.Time(2*time.Second), "test/crash", m5.Node.Crash)
		fw.Sim.At(kernel.Time(20*time.Second), "test/restart", m5.Node.Restart)
		fw.RunFor(40*time.Second - time.Nanosecond)

		stops := 0
		for node, recs := range driverRecords(fw.Sim) {
			var now time.Duration
			stopped := map[string]time.Duration{} // shard and term -> the reading of its StopRun
			for _, r := range recs {
				switch {
				case r.Kind == kindBoot:
					clear(stopped) // a crash ends the runs with no RunStopped
				case r.Kind == kindInput:
					now = nanosAttr(t, r, "now_ns")
					if !isInput(r, "RunStopped") {
						continue
					}
					at, ok := stopped[shardOf(r)]
					if !ok {
						t.Errorf("node %d: RunStopped %s without its StopRun", node, shardOf(r))
						continue
					}
					delete(stopped, shardOf(r))
					if d := now - at; d < 0 || d > stopDelay {
						t.Errorf("node %d: RunStopped %s %v after its StopRun, want within [0, %v]", node, shardOf(r), d, stopDelay)
					}
					stops++
				case isAction(r, "StopRun"):
					stopped[shardOf(r)] = now
				}
			}
		}
		if stops == 0 {
			t.Error("no RunStopped")
		}

		// Per node and run: stop, then stopped, at most once each.
		for _, m := range w.Members() {
			state := map[string]string{}
			for _, r := range runRecords(fw.Sim, m.Node.ID()) {
				key, op := shardOf(r), attrValue(r, "op")
				switch op {
				case runOpStart:
					state[key] = op
				case runOpStop:
					if state[key] != runOpStart {
						t.Errorf("%s: stop of %s after %q", m.Name, key, state[key])
					}
					state[key] = op
				case runOpStopped:
					if state[key] != runOpStop {
						t.Errorf("%s: stopped of %s after %q", m.Name, key, state[key])
					}
					state[key] = op
				}
			}
		}
	})
}

// TestRestartBackoff checks RUN-330's backoff and its count: min(RetryPeriod·2^(c−1),
// LeaseDuration), and a count that starts again after a call that lasted LeaseDuration.
func TestRestartBackoff(t *testing.T) {
	tm := core.DefaultTiming
	for c, want := range map[int]time.Duration{
		1: tm.RetryPeriod, 2: 2 * tm.RetryPeriod, 3: 4 * tm.RetryPeriod, 4: tm.LeaseDuration, 1000: tm.LeaseDuration,
	} {
		if got := restartBackoff(tm, c); got != want {
			t.Errorf("restartBackoff(%d) = %v, want %v", c, got, want)
		}
	}
	lease := tm.LeaseDuration
	for _, tc := range []struct {
		count  int
		lasted time.Duration
		want   int
	}{
		{0, 0, 1}, {3, lease - 1, 4}, {3, lease, 1}, {5, 2 * lease, 1},
	} {
		if got := nextReturnCount(tc.count, tc.lasted, lease); got != tc.want {
			t.Errorf("nextReturnCount(%d, %v) = %d, want %d", tc.count, tc.lasted, got, tc.want)
		}
	}
}
