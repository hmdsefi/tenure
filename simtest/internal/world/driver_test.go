// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// nanosAttr returns the attribute key of r as a duration, failing the test when it is not one.
func nanosAttr(t testing.TB, r kernel.Record, key string) time.Duration {
	t.Helper()
	v, err := strconv.ParseInt(attrValue(r, key), 10, 64)
	if err != nil {
		t.Fatalf("record %s %v: %s: %v", r.Kind, attrs(r), key, err)
	}
	return time.Duration(v)
}

// driverRecords returns the records of the drivers and the boots, by node, in trace order:
// tenure.boot, tenure.input, tenure.action and tenure.reply_late.
func driverRecords(sim *kernel.Sim) map[kernel.NodeID][]kernel.Record {
	out := make(map[kernel.NodeID][]kernel.Record)
	for _, r := range sim.Records() {
		switch r.Kind {
		case kindBoot, kindInput, kindAction, kindReplyLate:
			out[r.Node] = append(out[r.Node], r)
		}
	}
	return out
}

// isInput reports whether r is a tenure.input of the type typ.
func isInput(r kernel.Record, typ string) bool {
	return r.Kind == kindInput && attrValue(r, "type") == typ
}

// isAction reports whether r is a tenure.action of the type typ.
func isAction(r kernel.Record, typ string) bool {
	return r.Kind == kindAction && attrValue(r, "type") == typ
}

// shardOf returns the space, shard and term attributes of r, joined.
func shardOf(r kernel.Record) string {
	return attrValue(r, "space") + "/" + attrValue(r, "shard") + "@" + attrValue(r, "term")
}

// checkConverged checks that every shard of every space of w is owned by exactly one member,
// the one every member's machine assigns it to.
func checkConverged(t testing.TB, w *World) {
	t.Helper()
	for _, sp := range w.Scenario().Spaces {
		var assign []string
		owners := make([][]string, sp.Shards)
		for _, m := range w.Members() {
			d := w.drivers.current(m)
			if d == nil {
				t.Errorf("%s has no driver", m.Name)
				continue
			}
			got := d.machine.Assignment(sp.Name)
			if assign == nil {
				assign = got
			} else if !slices.Equal(got, assign) {
				t.Errorf("%s assigns %s as %v, another member as %v", m.Name, sp.Name, got, assign)
			}
			for _, st := range d.status().Spaces {
				if st.Name != sp.Name {
					continue
				}
				for shard, sh := range st.Shards {
					if sh.State == core.ShardStateOwned {
						owners[shard] = append(owners[shard], m.Name)
					}
				}
			}
		}
		for shard, o := range owners {
			if len(o) != 1 || (assign != nil && o[0] != assign[shard]) {
				t.Errorf("%s/%d is owned by %v, assigned to %q", sp.Name, shard, o, assign[shard])
			}
		}
	}
}

// TestDriverSteady runs a world of five members under the Server rule with a stop delay: m5
// crashes at 2 s and stays down until 25 s, so the others take its shards once the store has
// ended its session and hand them back when it returns. It checks that the world converges,
// every shard owned by the member every machine assigns it to, and that every RunStopped comes
// within the stop delay of its StopRun.
func TestDriverSteady(t *testing.T) {
	const stopDelay = time.Second
	runWorld(t, fullTrace(50*time.Second), Scenario{StopDelay: stopDelay}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m5 := w.Members()[4]
		fw.Sim.At(kernel.Time(2*time.Second), "test/crash", m5.Node.Crash)
		fw.Sim.At(kernel.Time(25*time.Second), "test/restart", m5.Node.Restart)
		fw.RunFor(45 * time.Second)
		checkConverged(t, w)

		starts, stops := 0, 0
		for node, recs := range driverRecords(fw.Sim) {
			var now time.Duration
			stopped := map[string]time.Duration{} // shard and term -> the reading of its StopRun
			for _, r := range recs {
				switch {
				case r.Kind == kindInput:
					now = nanosAttr(t, r, "now_ns")
					if isInput(r, "RunStopped") {
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
					}
				case isAction(r, "StopRun"):
					stopped[shardOf(r)] = now
				case isAction(r, "StartRun"):
					starts++
				}
			}
		}
		if starts < 16 || stops == 0 {
			t.Errorf("%d StartRun and %d RunStopped, want at least 16 and 1", starts, stops)
		}
	})
}

// TestATSIM07WakeFollowsDrift checks AT-SIM-07: under clock-drift faults, every Tick a driver
// steps, but the first of an incarnation, is stepped at a reading in [T, T + 1 µs], where T is
// the wake the step before it armed. The steps were written for this task from the issue and
// the design note, since the original acceptance-test text is lost.
func TestATSIM07WakeFollowsDrift(t *testing.T) {
	opts := fullTrace(40 * time.Second)
	opts.Seeds = 20
	sc := Scenario{
		Members: 3,
		Rho:     100_000,
		Faults: random(fault.Rule{
			Kind: fault.KindClockDrift, Every: 2 * time.Second, Magnitude: 200_000, Target: roleMembers,
		}),
	}
	runWorld(t, opts, sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		fw.RunFor(40*time.Second - time.Nanosecond)
		if n := len(records(fw.Sim, "fault.clock_drift")); n == 0 {
			t.Fatal("no drift changed")
		}
		ticks := 0
		for node, recs := range driverRecords(fw.Sim) {
			var armed, stepWake time.Duration
			first, hasArmed, hasStepWake, stepped := false, false, false, false
			for _, r := range recs {
				switch {
				case r.Kind == kindBoot:
					first, hasArmed, hasStepWake, stepped = true, false, false, false
				case r.Kind == kindInput:
					if stepped {
						armed, hasArmed = stepWake, hasStepWake
					}
					stepped, hasStepWake = true, false
					if !isInput(r, "Tick") {
						first = false
						continue
					}
					if first {
						first = false
						continue
					}
					now := nanosAttr(t, r, "now_ns")
					if !hasArmed || now < armed || now > armed+time.Microsecond {
						t.Errorf("node %d: Tick at %v, wake %v (armed %v)", node, now, armed, hasArmed)
					}
					ticks++
				case isAction(r, "Wake"):
					stepWake, hasStepWake = nanosAttr(t, r, "at_ns"), true
				}
			}
		}
		if ticks < 20 {
			t.Errorf("%d wake ticks checked, want at least 20", ticks)
		}
	})
}

// TestATSIM15InputActionOrder checks AT-SIM-15 in a steady world: the first input of every
// incarnation is a Tick, every action comes after the input of its step, in the same event,
// and every StartRun comes after an Acquired of the same member for the same shard and term.
// The steps were written for this task from the issue and the design note, since the original
// acceptance-test text is lost.
func TestATSIM15InputActionOrder(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{}, func(fw *faultline.World, w *World) {
		t := fw.T()
		fw.RunFor(25*time.Second - time.Nanosecond)
		starts := 0
		for _, m := range w.Members() {
			boots := 0
			var last *kernel.Record // the member's latest input or action in this event
			acquired := map[string]bool{}
			first := false
			for _, e := range traceEvents(fw.Sim) {
				last = nil
				for _, r := range e.recs {
					if r.Node != m.Node.ID() {
						continue
					}
					switch r.Kind {
					case kindBoot:
						boots++
						first = true
						acquired = map[string]bool{}
					case kindInput:
						if first && !isInput(r, "Tick") {
							t.Errorf("%s: the first input is %v, want a Tick", m.Name, attrs(r))
						}
						first = false
						if isInput(r, "Acquired") {
							acquired[shardOf(r)] = true
						}
						last = &r
					case kindAction:
						if last == nil {
							t.Errorf("%s at %s: action %v comes before any input of its event", m.Name, e.at, attrs(r))
						}
						if isAction(r, "StartRun") {
							starts++
							if !acquired[shardOf(r)] {
								t.Errorf("%s: StartRun %s with no Acquired before it", m.Name, shardOf(r))
							}
						}
						last = &r
					}
				}
			}
			if boots != 1 {
				t.Errorf("%s booted %d times, want 1", m.Name, boots)
			}
		}
		if starts < 16 {
			t.Errorf("%d StartRun, want at least 16", starts)
		}
	})
}

// TestATSIM16LateReplies checks AT-SIM-16: with a link from the store to m1 slower than
// RetryPeriod, every reply to m1's keepAlive comes after its timeout and is recorded as
// tenure.reply_late, m1's machine steps SessionFailed{CauseUnreachable} and never
// SessionRenewed, and m2, on a normal link, renews. The steps were written for this task from
// the issue and the design note, since the original acceptance-test text is lost.
func TestATSIM16LateReplies(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{Members: 2}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m1, m2 := w.Members()[0], w.Members()[1]
		fw.Net.SetLink(w.Store().ID(), m1.Node.ID(), simnet.Link{Latency: 3 * time.Second})
		fw.RunFor(25*time.Second - time.Nanosecond)

		recs := driverRecords(fw.Sim)
		count := func(m *Member, typ, attr, value string) int {
			n := 0
			for _, r := range recs[m.Node.ID()] {
				if r.Kind == kindInput && attrValue(r, "type") == typ && (attr == "" || attrValue(r, attr) == value) {
					n++
				}
			}
			return n
		}
		if n := count(m1, "SessionFailed", "cause", "unreachable"); n == 0 {
			t.Error("m1 stepped no SessionFailed{CauseUnreachable}")
		}
		if n := count(m1, "SessionRenewed", "", ""); n != 0 {
			t.Errorf("m1 stepped %d SessionRenewed, want none", n)
		}
		if n := count(m2, "SessionRenewed", "", ""); n == 0 {
			t.Error("m2 stepped no SessionRenewed")
		}

		// Every renewed reply that reached m1 is late, in the event that delivered it.
		sent := map[string]string{}
		renewals := 0
		for _, e := range traceEvents(fw.Sim) {
			for _, r := range e.of("net.send") {
				sent[attrValue(r, "msg")] = attrValue(r, "payload")
			}
			for _, r := range e.of("net.deliver") {
				payload := sent[attrValue(r, "msg")]
				if attrValue(r, "to") != m1.Name || !strings.HasPrefix(payload, "renewed#") {
					continue
				}
				renewals++
				id, _ := payloadID(payload)
				late := false
				for _, l := range e.of(kindReplyLate) {
					if l.Node == m1.Node.ID() && attrValue(l, "req") == strconv.FormatUint(id, 10) &&
						attrValue(l, "type") == "renewed" && l.Text == payload {
						late = true
					}
				}
				if !late {
					t.Errorf("at %s m1 got %q with no tenure.reply_late", e.at, payload)
				}
			}
		}
		if renewals < 3 {
			t.Errorf("m1 got %d renewed replies, want at least 3", renewals)
		}
	})
}

// TestReplyTable checks SIM-320's table: what each reply to each request steps, what each
// timeout steps, and that a reply no row has is refused.
func TestReplyTable(t *testing.T) {
	const sentAt = 7 * time.Second
	rh := replyHead{ID: 9}
	rec := tenure.Holding{Space: "book", Shard: 3, Holder: "m2/0b", Term: 41, Version: 12}
	acq := core.Acquire{Space: "book", Shard: 3}
	lost := core.SessionFailed{Cause: core.CauseLost}
	tests := []struct {
		act  core.Action
		rep  reply
		want []core.Input
		ok   bool
	}{
		{core.KeepAlive{}, renewedReply{replyHead: rh, Version: 3}, []core.Input{core.SessionRenewed{SentAt: sentAt}}, true},
		{core.KeepAlive{}, renewedReply{replyHead: rh, Version: 1, Opened: true}, []core.Input{core.SessionRenewed{SentAt: sentAt}}, true},
		{core.KeepAlive{}, lostReply{rh}, []core.Input{lost}, true},
		{core.CloseSession{}, closedReply{rh}, []core.Input{core.SessionClosed{}}, true},
		{core.CloseSession{}, lostReply{rh}, []core.Input{core.SessionClosed{}}, true},
		{core.CloseSession{}, renewedReply{replyHead: rh}, nil, false},
		{acq, grantedReply{rh, rec}, []core.Input{core.Acquired{Space: "book", Shard: 3, Term: 41}}, true},
		{
			acq, heldReply{replyHead: rh, Record: rec, RetryAfter: 2 * time.Second},
			[]core.Input{core.AcquireFailed{Space: "book", Shard: 3, Holder: "m2/0b", RetryAfter: 2 * time.Second}}, true,
		},
		{acq, lostReply{rh}, []core.Input{core.Acquired{Space: "book", Shard: 3}, lost}, true},
		{
			core.Release{Space: "book", Shard: 3, Term: 40}, releasedReply{replyHead: rh, Record: rec},
			[]core.Input{core.Released{Space: "book", Shard: 3, Term: 40}}, true,
		},
		{
			core.Release{Space: "book", Shard: 3, Term: 41}, releasedReply{replyHead: rh, Record: rec, Cleared: true},
			[]core.Input{core.Released{Space: "book", Shard: 3, Term: 41}}, true,
		},
		{core.Release{Space: "book", Shard: 3, Term: 41}, lostReply{rh}, []core.Input{lost}, true},
		{
			core.Confirm{Space: "book", Shard: 3, Term: 40}, confirmedReply{rh, rec},
			[]core.Input{core.HolderChanged{Space: "book", Shard: 3, Holder: "m2/0b", Term: 41}}, true,
		},
		{core.Confirm{Space: "book", Shard: 3, Term: 40}, lostReply{rh}, []core.Input{lost}, true},
		{core.ClaimSlot{}, slotClaimedReply{replyHead: rh, Slot: 2, View: 5}, []core.Input{core.SlotClaimed{Slot: 2}}, true},
		{core.ClaimSlot{}, noSlotReply{rh}, nil, true},
		{core.ClaimSlot{}, lostReply{rh}, []core.Input{lost}, true},
		{core.ReleaseSlot{Slot: 2}, slotReleasedReply{replyHead: rh, Slot: 2}, []core.Input{core.SlotReleased{Slot: 2}}, true},
		{core.ReleaseSlot{Slot: 2}, lostReply{rh}, []core.Input{lost}, true},
		// Replies no row has.
		{core.KeepAlive{}, closedReply{rh}, nil, false},
		{acq, releasedReply{replyHead: rh, Record: rec}, nil, false},
		{core.Confirm{Space: "book", Shard: 3, Term: 40}, grantedReply{rh, rec}, nil, false},
	}
	for _, tt := range tests {
		got, ok := replyInputs(tt.act, tt.rep, sentAt)
		if ok != tt.ok || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%T%+v answered by %q: %v, %v; want %v, %v", tt.act, tt.act, tt.rep.Describe(), got, ok, tt.want, tt.ok)
		}
	}

	timeouts := []struct {
		act  core.Action
		want []core.Input
	}{
		{core.KeepAlive{}, []core.Input{core.SessionFailed{Cause: core.CauseUnreachable}}},
		{acq, []core.Input{core.Acquired{Space: "book", Shard: 3}}},
		{core.Release{Space: "book", Shard: 3, Term: 41}, nil},
		{core.Confirm{Space: "book", Shard: 3, Term: 41}, nil},
		{core.ClaimSlot{}, nil},
		{core.ReleaseSlot{Slot: 1}, nil},
		{core.CloseSession{}, nil},
	}
	for _, tt := range timeouts {
		if got := timeoutInputs(tt.act); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("timeout of %T%+v: %v, want %v", tt.act, tt.act, got, tt.want)
		}
	}
}

// TestWithSlots checks that a stepped view has each member's Slot per RUN-220: the lowest slot
// whose holder has the member's ID, else -1, and that the view given is not changed.
func TestWithSlots(t *testing.T) {
	v := core.View{
		Version:     4,
		Members:     []core.Member{{ID: "m1", Slot: 7}, {ID: "m2", Slot: 7}, {ID: "m3", Slot: 7}},
		Slots:       4,
		SlotHolders: []string{"m2/0b", "", "m1/0a", "m2/0b"},
	}
	got := withSlots(v)
	slots := []int{got.Members[0].Slot, got.Members[1].Slot, got.Members[2].Slot}
	if !slices.Equal(slots, []int{2, 0, -1}) {
		t.Errorf("slots %v, want [2 0 -1]", slots)
	}
	if v.Members[0].Slot != 7 {
		t.Error("withSlots changed its argument")
	}
	if got := withSlots(core.View{Members: []core.Member{{ID: "m1"}}}); got.Members[0].Slot != -1 {
		t.Errorf("no slots: Slot %d, want -1", got.Members[0].Slot)
	}
}

// isolated runs body in a world of one member with slots whose node the network cuts off, so
// that only what the test hands the driver reaches its machine. body runs in an event of the
// member's node at 1 s, with the member's driver.
func isolated(t *testing.T, body func(t testing.TB, fw *faultline.World, d *driver)) {
	runWorld(t, fullTrace(90*time.Second), Scenario{Members: 1, Slots: 2}, func(fw *faultline.World, w *World) {
		m := w.Members()[0]
		fw.Net.Isolate(m.Node.ID())
		fw.Sim.At(kernel.Time(time.Second), "test/do", func() {
			body(fw.T(), fw, w.drivers.current(m))
		})
		fw.RunFor(2 * time.Second)
	})
}

// inputsAfter returns the tenure.input records of the driver's node from the time at on, as
// "type key=value ..." for the keys asked.
func inputsAfter(sim *kernel.Sim, d *driver, at kernel.Time, keys ...string) []string {
	var out []string
	for _, r := range records(sim, kindInput) {
		if r.Node != d.m.Node.ID() || r.At < at || attrValue(r, "type") == "Tick" {
			continue
		}
		s := attrValue(r, "type")
		for _, k := range keys {
			if v := attrValue(r, k); v != "" {
				s += " " + k + "=" + v
			}
		}
		out = append(out, s)
	}
	return out
}

// TestDriverViews checks SIM-330 and O8: a view is stepped only when it is newer than the last
// one stepped, and a view read before the latest SlotClaimed or SlotReleased is discarded.
func TestDriverViews(t *testing.T) {
	isolated(t, func(t testing.TB, fw *faultline.World, d *driver) {
		view := func(version uint64) core.View {
			return core.View{Version: version, Members: []core.Member{{ID: "m1"}}, Slots: 2, SlotHolders: []string{"", ""}}
		}
		answer := func(act core.Action, rep reply) {
			c := d.callFor(act)
			c.req = withID(c.req, reqHead{ID: 1, Holder: d.holder})
			d.answer(c, rep)
		}
		d.view(view(3), "", srcPush)
		d.view(view(3), "", srcSnapshot)
		d.view(view(2), "", srcPush)
		answer(core.ClaimSlot{}, slotClaimedReply{replyHead: replyHead{ID: 1}, Slot: 0, View: 6})
		d.view(view(5), "", srcPush)
		d.view(view(6), "", srcSnapshot)
		answer(core.ReleaseSlot{Slot: 0}, slotReleasedReply{replyHead: replyHead{ID: 1}, Slot: 0, Cleared: true, View: 9})
		d.view(view(8), "", srcPush)
		d.view(view(10), "", srcPush)
		got := inputsAfter(fw.Sim, d, kernel.Time(time.Second), "version", "slot", "src")
		want := []string{
			"ViewChanged version=3 src=push",
			"SlotClaimed slot=0",
			"ViewChanged version=6 src=snapshot",
			"SlotReleased slot=0",
			"ViewChanged version=10 src=push",
		}
		if !slices.Equal(got, want) {
			t.Errorf("inputs\n\t%s\nwant\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
		}
	})
}

// TestDriverRecords checks the stepping of pushed and snapshot records: a record of a version
// above the last one stepped for the shard is stepped, with its src; one at or below it is not;
// a Confirm's answer may repeat it; a record naming this holder while an Acquire of the shard
// is in flight is left to the Acquire's reply, and delivers nothing.
func TestDriverRecords(t *testing.T) {
	isolated(t, func(t testing.TB, fw *faultline.World, d *driver) {
		rec := func(shard int, holder string, term core.Term, version uint64) tenure.Holding {
			return tenure.Holding{Space: "book", Shard: shard, Holder: holder, Term: term, Version: version}
		}
		d.record(rec(0, "m2/0b", 5, 3), "", srcPush)
		d.record(rec(0, "m2/0b", 5, 3), "", srcSnapshot)
		d.record(rec(1, "m2/0b", 5, 4), "", srcSnapshot)
		d.record(rec(0, "", 5, 6), "", srcPush)
		d.record(rec(0, "m2/0b", 5, 3), "", srcPush) // a stale state
		c := d.callFor(core.Confirm{Space: "book", Shard: 1, Term: 5})
		c.req = withID(c.req, reqHead{ID: 1, Holder: d.holder})
		d.answer(c, confirmedReply{replyHead{1}, rec(1, "m2/0b", 5, 4)})
		d.record(rec(1, "m2/0b", 5, 4), "", srcPush)

		d.execute(core.Acquire{Space: "book", Shard: 2})
		d.record(rec(2, d.holder, 9, 8), "", srcPush)
		d.record(rec(2, "m2/0b", 9, 7), "", srcPush)
		got := inputsAfter(fw.Sim, d, kernel.Time(time.Second), "shard", "holder", "term", "src")
		want := []string{
			"HolderChanged shard=0 holder=m2/0b term=5 src=push",
			"HolderChanged shard=1 holder=m2/0b term=5 src=snapshot",
			"HolderChanged shard=0 term=5 src=push",
			"HolderChanged shard=1 holder=m2/0b term=5 src=confirm",
			"HolderChanged shard=2 holder=m2/0b term=9 src=push",
		}
		if !slices.Equal(got, want) {
			t.Errorf("inputs\n\t%s\nwant\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
		}
	})
}

// TestDriverTimeouts checks SIM-320's timeouts in a world: a request with no reply times out
// RetryPeriod of the member's clock after it went out, the timeout is stepped, and a reply that
// comes after it is dropped with tenure.reply_late. m1's node is cut off from the store until
// its first keepAlive has timed out.
func TestDriverTimeouts(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{Members: 1}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m := w.Members()[0]
		// The store gets m1's keepAlive, but its reply waits on the link for 2.5 s.
		fw.Net.SetLink(w.Store().ID(), m.Node.ID(), simnet.Link{Latency: 2500 * time.Millisecond})
		fw.Sim.At(kernel.Time(3*time.Second), "test/heal", func() {
			fw.Net.SetLink(w.Store().ID(), m.Node.ID(), simnet.Link{Latency: time.Millisecond})
		})
		fw.RunFor(6 * time.Second)

		recs := driverRecords(fw.Sim)[m.Node.ID()]
		var sentAt, failedAt time.Duration
		var lateIdx, failIdx, renewIdx int
		sent := false
		for i, r := range recs {
			switch {
			case isAction(r, "KeepAlive") && !sent && i > 0:
				sent, sentAt = true, nanosAttr(t, recs[i-1], "now_ns")
			case isInput(r, "SessionFailed") && failIdx == 0:
				failIdx, failedAt = i, nanosAttr(t, r, "now_ns")
			case r.Kind == kindReplyLate && lateIdx == 0:
				lateIdx = i
			case isInput(r, "SessionRenewed") && renewIdx == 0:
				renewIdx = i
			}
		}
		rp := w.Scenario().Timing.RetryPeriod
		if failIdx == 0 || failedAt < sentAt+rp || failedAt > sentAt+rp+time.Microsecond {
			t.Errorf("SessionFailed at %v, keepAlive sent at %v, want RetryPeriod %v later", failedAt, sentAt, rp)
		}
		if attrValue(recs[failIdx], "cause") != "unreachable" {
			t.Errorf("SessionFailed %v, want cause unreachable", attrs(recs[failIdx]))
		}
		if lateIdx == 0 || lateIdx < failIdx {
			t.Errorf("tenure.reply_late at %d, SessionFailed at %d: want a late reply after the timeout", lateIdx, failIdx)
		} else if got := attrs(recs[lateIdx]); got[1] != "type=renewed" {
			t.Errorf("tenure.reply_late %v, want type=renewed", got)
		}
		if renewIdx == 0 {
			t.Error("no SessionRenewed after the link healed")
		}
	})
}

// TestDriverTickFirst checks O2's tick-first rule on its own: an input that comes at a reading at
// or after the machine's wake, before the wake timer has fired, is stepped after a Tick at that
// reading, in the same event. The test cancels every timer of the driver, so that the input
// reaches the driver with the wake unstepped, as an input can when it comes in the same instant
// as the wake timer or in the few nanoseconds before the timer fires.
func TestDriverTickFirst(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{Members: 1}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m := w.Members()[0]
		fw.Net.Isolate(m.Node.ID())
		var at time.Duration // the reading at which the input is stepped
		fw.Sim.At(kernel.Time(time.Second), "test/hold", func() {
			d := w.drivers.current(m)
			wake, ok := d.machine.NextWake()
			if !ok {
				t.Fatal("the machine has no wake")
			}
			d.wake.cancel()
			for _, l := range d.lanes.all {
				if l.cur != nil {
					l.cur.timer.cancel()
				}
			}
			w.setTimer(m.Node, wake, "test/input", func() {
				at = m.Mono()
				if next, ok := d.machine.NextWake(); !ok || next > at {
					t.Fatalf("wake %v at %v: the input would not need a Tick first", next, at)
				}
				d.record(tenure.Holding{Space: defaultSpace, Shard: 0, Holder: "m2/0b", Term: 5, Version: 5}, "", srcPush)
			})
		})
		fw.RunFor(5 * time.Second)
		var got []string
		for _, r := range records(fw.Sim, kindInput) {
			if r.Node == m.Node.ID() && nanosAttr(t, r, "now_ns") == at {
				got = append(got, attrValue(r, "type"))
			}
		}
		if want := []string{"Tick", "HolderChanged"}; at == 0 || !slices.Equal(got, want) {
			t.Errorf("inputs at the wake %v: %v, want %v", at, got, want)
		}
	})
}

// TestDriverOldReplies checks that a reply to a request of an earlier incarnation never reaches
// the machine of a later one: m1 crashes and boots again while the replies to its first requests
// are still on a slow link, which delivers them to the new incarnation's node. Every reply that
// m1's machine steps must have been sent by the store after the boot of the incarnation that
// steps it.
func TestDriverOldReplies(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{Members: 1}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m := w.Members()[0]
		fw.Net.SetLink(w.Store().ID(), m.Node.ID(), simnet.Link{Latency: 300 * time.Millisecond})
		fw.Sim.At(kernel.Time(100*time.Millisecond), "test/crash", m.Node.Crash)
		fw.Sim.At(kernel.Time(150*time.Millisecond), "test/restart", m.Node.Restart)
		fw.RunFor(3 * time.Second)

		sentAt := map[string]kernel.Time{} // by msg: the time the message was sent
		var boot kernel.Time
		old, stepped := 0, 0
		for _, e := range traceEvents(fw.Sim) {
			from, delivered := kernel.Time(0), false
			for _, r := range e.recs {
				switch {
				case r.Kind == "net.send":
					sentAt[attrValue(r, "msg")] = e.at
				case r.Kind == kindBoot && r.Node == m.Node.ID():
					boot = e.at
				case r.Kind == "net.deliver" && attrValue(r, "to") == m.Name && attrValue(r, "from") == storeName:
					from, delivered = sentAt[attrValue(r, "msg")], true
				}
			}
			if !delivered {
				continue
			}
			if from < boot {
				old++
			}
			for _, r := range e.of(kindInput) {
				if _, isReply := payloadID(r.Text); r.Node != m.Node.ID() || !isReply {
					continue
				}
				stepped++
				if from < boot {
					t.Errorf("at %s m1 stepped %s from %q, sent at %s, before its boot at %s", e.at, attrValue(r, "type"), r.Text, from, boot)
				}
			}
		}
		if old == 0 || stepped == 0 {
			t.Errorf("%d replies to the old incarnation delivered after the boot and %d replies stepped, want some of each", old, stepped)
		}
	})
}

// TestDriverNewIncarnation checks that the driver of a new incarnation that starts while the
// node is up, as a recovery does, stops the driver before it: the old machine steps nothing
// more, and the old driver keeps no timer.
func TestDriverNewIncarnation(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{Members: 1}, func(fw *faultline.World, w *World) {
		t := fw.T()
		m := w.Members()[0]
		var prev *driver
		var prevNow time.Duration
		fw.Sim.At(kernel.Time(3*time.Second), "test/incarnate", func() {
			prev = w.drivers.current(m)
			w.incarnate(m, "recover")
			prevNow = prev.status().Now
		})
		fw.RunFor(8 * time.Second)
		if prev == nil || w.drivers.current(m) == prev {
			t.Fatal("no new driver")
		}
		if now := prev.status().Now; now != prevNow {
			t.Errorf("the old machine stepped on to %v after the new incarnation at %v", now, prevNow)
		}
		if prev.wake != nil && prev.wake.pending() {
			t.Error("the old driver's wake timer is still pending")
		}
		if cur := w.drivers.current(m).status(); cur.Session != core.SessionStateLive {
			t.Errorf("the new machine's session is %v, want live", cur.Session)
		}
	})
}

// TestDriverViewWall checks that a stepped view carries the member's wall reading: its
// monotonic reading plus the offset of its wall clock.
func TestDriverViewWall(t *testing.T) {
	runWorld(t, fullTrace(25*time.Second), Scenario{Members: 3, WallSkew: time.Second}, func(fw *faultline.World, w *World) {
		t := fw.T()
		fw.RunFor(4 * time.Second)
		views := 0
		for _, m := range w.Members() {
			if m.wall == 0 {
				t.Fatalf("%s has no wall offset", m.Name)
			}
			for _, r := range records(fw.Sim, kindInput) {
				if r.Node != m.Node.ID() || !isInput(r, "ViewChanged") {
					continue
				}
				views++
				if got, want := nanosAttr(t, r, "wall_ns"), nanosAttr(t, r, "now_ns")+m.wall; got != want {
					t.Errorf("%s: ViewChanged wall %v, want %v", m.Name, got, want)
				}
			}
		}
		if views == 0 {
			t.Error("no view stepped")
		}
	})
}
