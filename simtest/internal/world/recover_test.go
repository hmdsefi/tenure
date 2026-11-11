// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// TestRecoveredMessages checks the messages of tenure/recovered (SIM-680): an end recovered
// from 0 or 2 times, and a recovery whose session never renewed, reported for the first member
// in node order, and a run in which no end was owed.
func TestRecoveredMessages(t *testing.T) {
	w := &World{members: []*Member{{Name: "m1"}, {Name: "m2"}}}
	at := kernel.Time(41207 * time.Millisecond)
	cases := []struct {
		name string
		ends [][]endOwed
		want string
	}{
		{"none", [][]endOwed{nil, nil}, "recovered: no session of a member ended"},
		{"once", [][]endOwed{{{holder: "m1/a", at: at, recovers: 1, newHolder: "m1/b", renewed: true}}, nil}, ""},
		{
			"zero", [][]endOwed{nil, {{holder: "m2/a", at: at}}},
			"recovered: m2: session m2/a ended at 41.207000000s, recovered 0 times",
		},
		{
			"twice", [][]endOwed{{{holder: "m1/a", at: at, recovers: 2, newHolder: "m1/b", renewed: true}}, {{holder: "m2/a", at: at}}},
			"recovered: m1: session m1/a ended at 41.207000000s, recovered 2 times",
		},
		{
			"unrenewed", [][]endOwed{{{holder: "m1/a", at: at, recovers: 1, newHolder: "m1/b"}}, nil},
			"recovered: m1: session m1/a ended at 41.207000000s, recovered as m1/b, which never renewed",
		},
	}
	for _, c := range cases {
		owed := 0
		for _, e := range c.ends {
			owed += len(e)
		}
		sw := &sessionWatch{w: w, ends: c.ends, owed: owed}
		err := sw.err()
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != c.want {
			t.Errorf("%s: err = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestSessionEndTime checks the recovery time of a scenario with EndAfter: the latest end of a
// session opened within two delays of the start, on a store clock that may run slow by ρ.
func TestSessionEndTime(t *testing.T) {
	sc := Scenario{EndAfter: 40 * time.Second, Delta: 5 * time.Millisecond}
	if got, want := sessionEndTime(sc), kernel.Time(40010*time.Millisecond); got != want {
		t.Errorf("sessionEndTime = %v, want %v", got, want)
	}
	sc.Rho = 100_000 // 40.01 s / 0.9, rounded up
	if got, want := sessionEndTime(sc), kernel.Time(44_455_555_556); got != want {
		t.Errorf("sessionEndTime with Rho = %d, want %d", int64(got), int64(want))
	}
	st, err := newSetup(Scenario{EndAfter: 40 * time.Second}, kernel.Time(75*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if st.recovery != kernel.Time(40010*time.Millisecond) {
		t.Errorf("R = %v, want 40.010000000s", st.recovery)
	}
	if err := SetupError(Scenario{EndAfter: 60 * time.Second}, kernel.Time(75*time.Second)); err == nil {
		t.Error("EndAfter 60s in a 75 s run: no setup error, want the bound after the end")
	}
}

// TestLanesAbandon checks that abandoned lanes drop the queued requests and those in flight,
// and admit nothing from then on (O7).
func TestLanesAbandon(t *testing.T) {
	var sent []*call
	ls := newLanes(func(c *call) { sent = append(sent, c) })
	a := &call{key: shardLane("book", 1), act: core.Acquire{Space: "book", Shard: 1}}
	b := &call{key: shardLane("book", 1), act: core.Release{Space: "book", Shard: 1, Term: 3}}
	ls.submit(a)
	ls.submit(b)
	if len(sent) != 1 || ls.pending() != 1 {
		t.Fatalf("sent %d, pending %d, want 1 and 1", len(sent), ls.pending())
	}
	ls.abandon()
	if ls.pending() != 0 || ls.inFlight != 0 || ls.shardOut != 0 {
		t.Errorf("after abandon: pending %d, in flight %d, shard out %d, want 0", ls.pending(), ls.inFlight, ls.shardOut)
	}
	ls.finish(a) // a late answer frees nothing
	if ls.submit(&call{key: laneKey{kind: laneSession}, act: core.KeepAlive{}}) {
		t.Error("abandoned lanes queued a KeepAlive")
	}
	if len(sent) != 1 {
		t.Errorf("sent %d requests, want 1", len(sent))
	}
}

// lossSetup brings the one member of an injected world to owning book/1 at term 5 with an
// Acquire of book/0 in flight, and returns the request in flight for book/0.
func lossSetup(t testing.TB, d *driver, rule Takeover) *call {
	t.Helper()
	renew(t, d)
	d.view(soloView(1), "", srcPush)
	if rule == Observer {
		replyTo(t, d, shardLane("book", 1), func(h replyHead) reply { return shardReply{h, book(1, "", 0, 0)} })
	}
	grant(t, d, book(1, d.holder, 5, 1))
	if st := shardState(t, d, 1); st != core.ShardStateOwned {
		t.Fatalf("book/1 is %v after its grant, want owned", st)
	}
	acq := d.lanes.laneOf(shardLane("book", 0)).cur
	if acq == nil {
		t.Fatal("no request in flight for book/0")
	}
	return acq
}

// TestDriverLoss checks the loss of a session and the recovery after it (SIM-145, MCH O7), in
// a one-member world of two shards under each rule: at 1 s the member owns book/1 and waits
// for an answer about book/0 when it steps SessionFailed{lost}. In that step the lanes are
// given up, so nothing is in flight or goes out any more, book/0 gets Acquired with no term,
// and the member waits for the run of book/1: the old driver is still the member's. A grant
// that answers the dropped request is dropped with no record. Once the run has stopped, in the
// event of its RunStopped, a new incarnation starts with a new holder (tenure.boot
// cause=recover), its requests have larger IDs, and a reply to a request of the old one is
// dropped by the new driver.
func TestDriverLoss(t *testing.T) {
	forRules(t, testDriverLoss)
}

func testDriverLoss(t *testing.T, rule Takeover) {
	sc := Scenario{Takeover: rule, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 2}}, StopDelay: time.Second}
	var old *driver
	var acqID, lastID uint64
	injected(t, sc, 3*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			acq := lossSetup(t, d, rule)
			old, acqID = d, acq.req.head().ID
			d.step(core.SessionFailed{Cause: core.CauseLost}, "", "")
			lastID = d.md.lastID
			if !d.lost || !d.lanes.abandoned || len(d.inFlight) != 0 || d.lanes.laneOf(shardLane("book", 0)).cur != nil {
				t.Errorf("after the loss: lost %v, abandoned %v, %d in flight, book/0's request %v; want the lanes given up",
					d.lost, d.lanes.abandoned, len(d.inFlight), d.lanes.laneOf(shardLane("book", 0)).cur)
			}
			if st := shardState(t, d, 0); st != core.ShardStateIdle {
				t.Errorf("book/0 is %v after the loss, want idle (Acquired with no term)", st)
			}
			if got := d.w.drivers.current(d.m); got != d || len(d.runs) != 1 || d.m.Recoveries() != 0 {
				t.Errorf("after the loss: a new driver %v, %d runs, %d recoveries; want the member to wait for its run",
					got != d, len(d.runs), d.m.Recoveries())
			}
			n := len(d.w.sim.Records())
			d.reply(grantedReply{replyHead{ID: acqID}, book(0, d.holder, 6, 2)})
			if got := len(d.w.sim.Records()); got != n || shardState(t, d, 0) != core.ShardStateIdle {
				t.Errorf("the dropped Acquire's grant made %d records and left book/0 %v, want none and idle",
					got-n, shardState(t, d, 0))
			}
			d.step(core.Tick{}, "", "")
			if d.md.lastID != lastID {
				t.Errorf("request IDs %d to %d went out after the loss, want none", lastID+1, d.md.lastID)
			}
		}},
		injectStep{2500 * time.Millisecond, func(t testing.TB, d *driver) {
			if d == old || !old.stopped || d.holder == old.holder || d.m.Recoveries() != 1 {
				t.Fatalf("at 2.5 s: new driver %v, old stopped %v, holder %s after %s, %d recoveries; want one recovery",
					d != old, old.stopped, d.holder, old.holder, d.m.Recoveries())
			}
			if d.md.lastID <= lastID {
				t.Errorf("the new incarnation sent no request with an ID above %d", lastID)
			}
			var stoppedAt, bootAt kernel.Time
			for _, r := range d.w.sim.Records() {
				switch {
				case r.Kind == kindRun && attrValue(r, "op") == runOpStopped:
					stoppedAt = r.At
				case r.Kind == kindBoot && attrValue(r, "cause") == bootCauseRecover:
					if attrValue(r, "holder") != d.holder {
						t.Errorf("tenure.boot cause=recover holder %s, want %s", attrValue(r, "holder"), d.holder)
					}
					bootAt = r.At
				}
			}
			if stoppedAt == 0 || bootAt != stoppedAt {
				t.Errorf("recovered at %v, the run stopped at %v; want the same event", bootAt, stoppedAt)
			}
			n := len(d.w.sim.Records())
			d.reply(grantedReply{replyHead{ID: acqID}, book(0, old.holder, 6, 2)})
			if got := len(d.w.sim.Records()); got != n {
				t.Errorf("a reply to the old incarnation made %d records in the new one, want none", got-n)
			}
		}},
	)
}

// TestDriverLossDraining checks that a member whose session is lost while it drains exits
// instead of recovering (RUN-190), under each rule: at 1 s the member of TestDriverLoss steps
// Drain and then SessionFailed{lost}. Once its run has stopped the member is exited, with one
// tenure.exit (holder, cause drain) and no new incarnation, and a restart of its node boots
// nothing.
func TestDriverLossDraining(t *testing.T) {
	forRules(t, testDriverLossDraining)
}

func testDriverLossDraining(t *testing.T, rule Takeover) {
	sc := Scenario{Takeover: rule, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 2}}, StopDelay: time.Second}
	var old *driver
	injected(t, sc, 5*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			lossSetup(t, d, rule)
			old = d
			d.m.draining = true // as Drain sets it, before its post steps Drain
			d.step(core.Drain{}, "", "")
			d.step(core.SessionFailed{Cause: core.CauseLost}, "", "")
			if d.m.Phase() != Active || d.stopped {
				t.Errorf("right after the loss: %v, stopped %v; want the member to wait for its run", d.m.Phase(), d.stopped)
			}
		}},
		injectStep{2500 * time.Millisecond, func(t testing.TB, d *driver) {
			if d != old || !d.stopped || d.m.Phase() != Exited || d.m.Recoveries() != 0 {
				t.Errorf("at 2.5 s: new driver %v, stopped %v, %v, %d recoveries; want the member exited",
					d != old, d.stopped, d.m.Phase(), d.m.Recoveries())
			}
		}},
		injectStep{3 * time.Second, func(t testing.TB, d *driver) { d.m.Node.Crash() }},
		injectStep{3500 * time.Millisecond, func(t testing.TB, d *driver) { d.m.Node.Restart() }},
		injectStep{4 * time.Second, func(t testing.TB, d *driver) {
			var got []string
			for _, r := range d.w.sim.Records() {
				if r.Node == d.m.Node.ID() && (r.Kind == kindBoot || r.Kind == kindExit) {
					got = append(got, r.Kind+" "+attrValue(r, "cause")+" "+strings.ReplaceAll(attrValue(r, "holder"), old.holder, "h1"))
				}
			}
			want := []string{"tenure.boot boot h1", "tenure.exit drain h1"}
			if !slices.Equal(got, want) {
				t.Errorf("boots and exits %q, want %q", got, want)
			}
		}},
	)
}
