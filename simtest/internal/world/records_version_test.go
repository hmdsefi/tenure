// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// injectStep is one event of an injected run: do runs at the time at, in an event of the
// member's node, with the member's driver.
type injectStep struct {
	at time.Duration
	do func(t testing.TB, d *driver)
}

// injected runs sc, which has one member, for the duration d, with the member's node cut off
// from every other node, so that only what the steps hand the driver reaches it (the run itself
// lasts 30 s, which the bound needs). The requests the driver sends are lost; the steps answer
// them with replyTo.
func injected(t *testing.T, sc Scenario, d time.Duration, steps ...injectStep) {
	t.Helper()
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		m := w.Members()[0]
		fw.Net.Isolate(m.Node.ID())
		for _, s := range steps {
			fw.Sim.At(kernel.Time(s.at), "test/inject", func() {
				s.do(fw.T(), w.drivers.current(m))
			})
		}
		fw.RunFor(d)
	})
}

// replyTo hands the driver the reply mk makes for the request in flight on the lane of key.
// It fails the test when no request is in flight there.
func replyTo(t testing.TB, d *driver, key laneKey, mk func(h replyHead) reply) {
	t.Helper()
	c := d.lanes.laneOf(key).cur
	if c == nil {
		t.Fatalf("no request in flight on lane %+v", key)
	}
	d.reply(mk(replyHead{ID: c.req.head().ID}))
}

// sessionLane is the key of the session lane.
var sessionLane = laneKey{kind: laneSession}

// renew answers the keepAlive in flight with renewed.
func renew(t testing.TB, d *driver) {
	t.Helper()
	replyTo(t, d, sessionLane, func(h replyHead) reply { return renewedReply{replyHead: h, Version: 1, Opened: true} })
}

// grant answers the Acquire in flight on shard of book with a grant of rec.
func grant(t testing.TB, d *driver, rec tenure.Holding) {
	t.Helper()
	replyTo(t, d, shardLane(rec.Space, rec.Shard), func(h replyHead) reply { return grantedReply{h, rec} })
}

// book returns a record of the shard of book.
func book(shard int, holder string, term core.Term, version uint64) tenure.Holding {
	return tenure.Holding{Space: "book", Shard: shard, Holder: holder, Term: term, Version: version}
}

// soloView returns a view of m1 alone.
func soloView(version uint64) core.View {
	return core.View{Version: version, Members: []core.Member{{ID: "m1"}}}
}

// shardState returns the state of the shard of book in d's machine.
func shardState(t testing.TB, d *driver, shard int) core.ShardState {
	t.Helper()
	sh, ok := shardIn(d.status(), "book", shard)
	if !ok {
		t.Fatalf("no shard book/%d", shard)
	}
	return sh.State
}

// actionsOf returns the tenure.action records of the driver's node of the types given, as
// "type space/shard@term reason", in trace order.
func actionsOf(sim *kernel.Sim, d *driver, types ...string) []string {
	var out []string
	for _, r := range records(sim, kindAction) {
		if r.Node != d.m.Node.ID() || !slices.Contains(types, attrValue(r, "type")) {
			continue
		}
		s := attrValue(r, "type") + " " + shardOf(r)
		if reason := attrValue(r, "reason"); reason != "" {
			s += " " + reason
		}
		out = append(out, s)
	}
	return out
}

// holderChanges returns the HolderChanged inputs of the driver's node, as "shard holder@term
// src", in trace order.
func holderChanges(sim *kernel.Sim, d *driver) []string {
	var out []string
	for _, r := range records(sim, kindInput) {
		if r.Node == d.m.Node.ID() && isInput(r, "HolderChanged") {
			out = append(out, attrValue(r, "shard")+" "+attrValue(r, "holder")+"@"+attrValue(r, "term")+" "+attrValue(r, "src"))
		}
	}
	return out
}

// TestATSIM44Versions checks AT-SIM-44 with injected replies, in one member's world of a
// fenced space of five shards, granted at terms 5 to 9 with versions 3 to 7. The runs were
// written for this task from the issue, MCH O4 and O5 and the design note, since the original
// acceptance-test text is lost:
//
//   - O4 (shard 1, owned): the grant's own push repeats the delivered version and is not
//     stepped; a newer record naming another holder is stepped as a loss; an older version
//     after it is not stepped; an equal version is not stepped from a snapshot but is from a
//     Confirm's answer; a newer version is stepped from a holdings answer, and the same version
//     from a later snapshot is not, nor an older one from a Confirm's answer.
//   - O5 (shard 0, degraded): after the renewal that sends the Confirm, a push of a newer
//     own-term record, a holdings answer read after the Confirm went out and the answer of a
//     Confirm that a later Confirm action follows are all not stepped; the answer of the latest
//     Confirm is stepped and restores; once owned again, a newer own-term push is stepped.
//   - O5 (shard 2, degraded): the latest Confirm's answer that repeats the delivered version,
//     as under the Server rule, is stepped and restores.
//   - Shard 3, degraded: a record naming another holder is stepped as a loss, and a holdings
//     answer that repeats its version is not stepped.
//   - Shard 4, degraded: a record naming this holder at a larger term is stepped as a loss.
func TestATSIM44Versions(t *testing.T) {
	sc := Scenario{Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 5, Fenced: true}}}
	var got, actions []string
	var owned0, owned2 bool
	injected(t, sc, 12*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			d.view(soloView(1), "", srcPush)
			for i := range 5 {
				grant(t, d, book(i, d.holder, core.Term(5+i), uint64(3+i)))
			}
			d.record(book(1, d.holder, 6, 4), "", srcPush)
			d.record(book(1, "m2/0b", 9, 10), "", srcPush)
			d.record(book(1, "m3/0c", 8, 8), "", srcPush)
			d.record(book(1, "m2/0b", 9, 10), "", srcSnapshot)
			c := d.callFor(core.Confirm{Space: "book", Shard: 1, Term: 6})
			c.req = withID(c.req, reqHead{ID: 1, Holder: d.holder})
			d.answer(c, confirmedReply{replyHead{1}, book(1, "m2/0b", 9, 10)})
		}},
		injectStep{11 * time.Second, func(t testing.TB, d *driver) {
			for _, i := range []int{0, 2, 3, 4} {
				if s := shardState(t, d, i); s != core.ShardStateDegraded {
					t.Fatalf("book/%d is %v at 11 s, want degraded", i, s)
				}
			}
			d.record(book(3, "m2/0b", 9, 20), "", srcPush)
			d.record(book(4, d.holder, 12, 21), "", srcPush)
			renew(t, d) // the keepAlive sent at 10 s: it follows failed renewals, so a resync starts
			d.record(book(0, d.holder, 5, 12), "", srcPush)
			replyTo(t, d, resyncLane(), func(h replyHead) reply {
				return holdingsReply{replyHead: h, Space: "book", Records: []tenure.Holding{
					book(0, d.holder, 5, 12), book(1, "", 9, 11), book(3, "m2/0b", 9, 20),
				}}
			})
			c := d.callFor(core.Confirm{Space: "book", Shard: 1, Term: 6})
			c.req = withID(c.req, reqHead{ID: 2, Holder: d.holder})
			d.answer(c, confirmedReply{replyHead{2}, book(1, "m2/0b", 9, 10)})
			d.execute(core.Confirm{Space: "book", Shard: 0, Term: 5}) // a later Confirm action
			replyTo(t, d, shardLane("book", 0), func(h replyHead) reply { return confirmedReply{h, book(0, d.holder, 5, 12)} })
			replyTo(t, d, shardLane("book", 0), func(h replyHead) reply { return confirmedReply{h, book(0, d.holder, 5, 14)} })
			replyTo(t, d, shardLane("book", 2), func(h replyHead) reply { return confirmedReply{h, book(2, d.holder, 7, 5)} })
			d.record(book(1, "", 9, 11), "", srcSnapshot)
			d.record(book(0, d.holder, 5, 15), "", srcPush)
			owned0 = shardState(t, d, 0) == core.ShardStateOwned
			owned2 = shardState(t, d, 2) == core.ShardStateOwned
			got = holderChanges(d.w.sim, d)
			actions = actionsOf(d.w.sim, d, "Restore", "StopRun")
			for i := range got {
				got[i] = strings.ReplaceAll(got[i], d.holder, "m1")
			}
		}},
	)
	want := []string{
		"1 m2/0b@9 push",
		"1 m2/0b@9 confirm",
		"3 m2/0b@9 push",
		"4 m1@12 push",
		"1 @9 holdings",
		"0 m1@5 confirm",
		"2 m1@7 confirm",
		"0 m1@5 push",
	}
	if !slices.Equal(got, want) {
		t.Errorf("HolderChanged\n\t%s\nwant\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
	}
	wantActions := []string{
		"StopRun book/1@6 lost", "StopRun book/3@8 lost", "StopRun book/4@9 lost", "Restore book/0@5", "Restore book/2@7",
	}
	if !slices.Equal(actions, wantActions) {
		t.Errorf("actions %v, want %v", actions, wantActions)
	}
	if !owned0 || !owned2 {
		t.Errorf("book/0 owned %v, book/2 owned %v after the restores, want both", owned0, owned2)
	}
}

// TestDriverResync checks SIM-340's resync: a renewal that follows no failure starts none; one
// that follows a failed renewal sends holdings for each space in name order, one at a time; a
// holdings request with no answer is sent again RetryPeriod after it went out, and its late
// answer is dropped with tenure.reply_late; the resync ends once every space has been read.
func TestDriverResync(t *testing.T) {
	sc := Scenario{Members: 1, Spaces: []core.SpaceSpec{{Name: "news", Shards: 1}, {Name: "book", Shards: 1}}}
	holdings := func(t testing.TB, d *driver) *call {
		t.Helper()
		return d.lanes.laneOf(resyncLane()).cur
	}
	var first *call
	var late int
	injected(t, sc, 8*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			if c := holdings(t, d); c != nil || d.resync.running {
				t.Errorf("a resync after the first renewal: %v", c)
			}
		}},
		// The keepAlive sent at 2 s times out at 4 s; the one sent at 4 s is answered.
		injectStep{5 * time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			first = holdings(t, d)
			if holdingsSpace(first) != "book" {
				t.Fatalf("after a renewal that follows a failure, holdings %v, want book's", first)
			}
		}},
		injectStep{7500 * time.Millisecond, func(t testing.TB, d *driver) {
			again := holdings(t, d)
			if again == first || holdingsSpace(again) != "book" {
				t.Fatalf("at 7.5 s holdings %v, want book's sent again", again)
			}
			if want := first.sentAt + d.timing.RetryPeriod; again.sentAt < want || again.sentAt > want+time.Microsecond {
				t.Errorf("book's holdings sent again at %v, want %v", again.sentAt, want)
			}
			d.reply(holdingsReply{replyHead: replyHead{ID: first.req.head().ID}, Space: "book"})
			late = 0
			for _, r := range records(d.w.sim, kindReplyLate) {
				if attrValue(r, "type") == "holdings" {
					late++
				}
			}
			replyTo(t, d, resyncLane(), func(h replyHead) reply { return holdingsReply{replyHead: h, Space: "book"} })
			if c := holdings(t, d); holdingsSpace(c) != "news" {
				t.Fatalf("after book's answer holdings %v, want news's", c)
			}
			replyTo(t, d, resyncLane(), func(h replyHead) reply {
				return holdingsReply{replyHead: h, Space: "news", Records: []tenure.Holding{{Space: "news", Holder: "m2/0b", Term: 3, Version: 9}}}
			})
			if c := holdings(t, d); c != nil || d.resync.running {
				t.Errorf("after the last space holdings %v, running %v; want the resync ended", c, d.resync.running)
			}
			if got := holderChanges(d.w.sim, d); !slices.Equal(got, []string{"0 m2/0b@3 holdings"}) {
				t.Errorf("HolderChanged %v, want news/0 from the holdings answer", got)
			}
		}},
	)
	if late != 1 {
		t.Errorf("%d late holdings replies recorded, want 1", late)
	}
}

// holdingsSpace returns the space of c's holdings request, "" for no call or another request.
func holdingsSpace(c *call) string {
	if c == nil {
		return ""
	}
	r, _ := c.req.(holdingsReq)
	return r.Space
}

// TestATSIM18LateGrant checks AT-SIM-18 with injected replies, in one member's world: the first
// Acquire of book/0 is answered held (AcquireFailed); the record is then cleared, so the
// machine acquires again at once; that Acquire times out (Acquired with no term) and its
// grant, at term 9, comes after the timeout and is dropped with tenure.reply_late; the grant's
// record then comes by a push and is stepped as HolderChanged naming m1, and the machine starts
// a run in the grant's term. With a Drain in between, it releases that term instead. book/1,
// granted at once, keeps the session open while draining. The steps were written for this task
// from the issue and the design note, since the original acceptance-test text is lost.
func TestATSIM18LateGrant(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run("drain="+strconv.FormatBool(drain), func(t *testing.T) {
			sc := Scenario{Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 2}}}
			var acquire *call
			var got []string
			steps := []injectStep{
				{time.Second, func(t testing.TB, d *driver) {
					renew(t, d)
					d.view(soloView(1), "", srcPush)
					grant(t, d, book(1, d.holder, 6, 4))
					replyTo(t, d, shardLane("book", 0), func(h replyHead) reply {
						return heldReply{replyHead: h, Record: book(0, "m2/0b", 4, 2), RetryAfter: d.timing.RetryPeriod}
					})
					d.record(book(0, "", 4, 5), "", srcPush)
					acquire = d.lanes.laneOf(shardLane("book", 0)).cur
					if acquire == nil {
						t.Fatal("no Acquire of book/0 after its record was cleared")
					}
				}},
				{4 * time.Second, func(t testing.TB, d *driver) {
					rec := book(0, d.holder, 9, 7)
					d.reply(grantedReply{replyHead{ID: acquire.req.head().ID}, rec})
					d.record(rec, holderPush{rec}.Describe(), srcPush)
					got = lateGrantTrace(d)
				}},
			}
			if drain {
				steps = append(steps, injectStep{3500 * time.Millisecond, func(t testing.TB, d *driver) {
					d.step(core.Drain{}, "", "")
				}})
				slices.SortFunc(steps, func(a, b injectStep) int { return int(a.at - b.at) })
			}
			injected(t, sc, 4500*time.Millisecond, steps...)
			want := []string{
				"Acquire", "AcquireFailed holder=m2/0b", "HolderChanged holder= term=4", "Acquire",
				"Acquired term=0", "reply_late granted", "HolderChanged holder=m1 term=9 src=push", "StartRun term=9",
			}
			if drain {
				want = slices.Insert(want, 5, "Drain")
				want[len(want)-1] = "Release term=9"
			}
			if !slices.Equal(got, want) {
				t.Errorf("book/0\n\t%s\nwant\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
			}
		})
	}
}

// lateGrantTrace returns what d's node recorded about book/0, and the Drain, in trace order:
// the inputs and the actions of the shard, with the attributes TestATSIM18LateGrant checks,
// and the late replies.
func lateGrantTrace(d *driver) []string {
	var out []string
	for _, r := range driverRecords(d.w.sim)[d.m.Node.ID()] {
		typ := attrValue(r, "type")
		switch {
		case r.Kind == kindReplyLate:
			out = append(out, "reply_late "+typ)
		case isInput(r, "Drain"):
			out = append(out, typ)
		case attrValue(r, "space") != "book" || attrValue(r, "shard") != "0":
		case isInput(r, "AcquireFailed"):
			out = append(out, typ+" holder="+attrValue(r, "holder"))
		case isInput(r, "Acquired"):
			out = append(out, typ+" term="+attrValue(r, "term"))
		case isInput(r, "HolderChanged"):
			s := typ + " holder=" + strings.ReplaceAll(attrValue(r, "holder"), d.holder, "m1") + " term=" + attrValue(r, "term")
			if attrValue(r, "holder") == d.holder {
				s += " src=" + attrValue(r, "src")
			}
			out = append(out, s)
		case isAction(r, "Acquire"):
			out = append(out, typ)
		case isAction(r, "StartRun"), isAction(r, "Release"):
			out = append(out, typ+" term="+attrValue(r, "term"))
		}
	}
	return out
}

// TestATSIM14SnapshotCatchUp checks AT-SIM-14: m2 crashes at 1 s, and from 5 s m1 and the
// store are cut off from each other, so m1 misses the records that change once the store has
// ended m2's session. At 17 s the link from the store to m1 heals, while m1's requests still
// do not reach the store, so no renewal and no resync helps: within RetryPeriod/(1−ρ) + δ of
// the heal (plus the 1 µs a mono timer may fire late), m1 has stepped a HolderChanged for every
// record changed during the drop, with the state the record had at the heal or a later one.
// The steps were written for this task from the issue and the design note, since the original
// acceptance-test text is lost.
func TestATSIM14SnapshotCatchUp(t *testing.T) {
	const rho = 10_000 // ppm
	sc := Scenario{Rho: rho}
	drop, heal := 5*time.Second, 17*time.Second
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		m1, m2 := w.Members()[0], w.Members()[1]
		store := w.Store().ID()
		fw.Sim.At(kernel.Time(time.Second), "test/crash", m2.Node.Crash)
		fw.Sim.At(kernel.Time(drop), "test/drop", func() {
			fw.Net.Cut(store, m1.Node.ID())
			fw.Net.Cut(m1.Node.ID(), store)
		})
		fw.Sim.At(kernel.Time(heal), "test/heal", func() { fw.Net.HealLink(store, m1.Node.ID()) })
		fw.Sim.At(kernel.Time(20*time.Second), "test/heal_all", fw.Net.Heal)
		fw.RunFor(21*time.Second - time.Nanosecond)

		rp := w.Scenario().Timing.RetryPeriod
		bound := (rp*1_000_000+(1_000_000-rho)-1)/(1_000_000-rho) + w.Scenario().Delta + time.Microsecond
		by := kernel.Time(heal + bound)

		// The states each shard's record had: at the heal, and written after it until by.
		type state struct{ holder, term string }
		atHeal := map[string]state{}
		later := map[string][]state{}
		for _, r := range fw.Sim.Records() {
			if r.Kind != kindGrant && (r.Kind != kindRelease || attrValue(r, "result") != "cleared") {
				continue
			}
			key := attrValue(r, "space") + "/" + attrValue(r, "shard")
			st := state{attrValue(r, "holder"), attrValue(r, "term")}
			if r.Kind == kindRelease {
				st.holder = ""
			}
			switch {
			case r.At >= kernel.Time(drop) && r.At < kernel.Time(heal):
				atHeal[key] = st
			case r.At >= kernel.Time(heal) && r.At <= by:
				later[key] = append(later[key], st)
			}
		}
		if len(atHeal) == 0 {
			t.Fatal("no record changed during the drop")
		}
		for key, want := range atHeal {
			ok := false
			for _, r := range records(fw.Sim, kindInput) {
				if r.Node != m1.Node.ID() || !isInput(r, "HolderChanged") || r.At < kernel.Time(drop) || r.At > by ||
					attrValue(r, "space")+"/"+attrValue(r, "shard") != key {
					continue
				}
				got := state{attrValue(r, "holder"), attrValue(r, "term")}
				if got == want || slices.Contains(later[key], got) {
					ok = true
				}
			}
			if !ok {
				t.Errorf("m1 stepped no HolderChanged for %s (%+v at the heal) by %s", key, want, by)
			}
		}
	})
}

// TestDriverResyncAgain checks that a resync is never run twice at once: a renewal that follows
// a failure while a resync runs sends nothing more, and one more resync of every space starts
// once the running one ends, and then the resyncs stop.
func TestDriverResyncAgain(t *testing.T) {
	sc := Scenario{Members: 1, Spaces: []core.SpaceSpec{{Name: "news", Shards: 1}, {Name: "book", Shards: 1}}}
	answer := func(t testing.TB, d *driver, space string) {
		t.Helper()
		c := d.lanes.laneOf(resyncLane()).cur
		if holdingsSpace(c) != space {
			t.Fatalf("holdings %v in flight, want %s's", c, space)
		}
		replyTo(t, d, resyncLane(), func(h replyHead) reply { return holdingsReply{replyHead: h, Space: space} })
	}
	injected(t, sc, 9*time.Second,
		injectStep{time.Second, renew},
		// The keepAlive sent at 2 s times out at 4 s; the one sent at 4 s is answered, which
		// starts a resync. The one sent at 6 s times out at 8 s.
		injectStep{5 * time.Second, renew},
		injectStep{8500 * time.Millisecond, func(t testing.TB, d *driver) {
			if f := d.status().LastFailure; f != core.CauseUnreachable || !d.resync.running {
				t.Fatalf("at 8.5 s last failure %v, resync running %v; want a failed renewal during the resync", f, d.resync.running)
			}
			renew(t, d)
			if l := d.lanes.laneOf(resyncLane()); len(l.queue) != 0 {
				t.Fatalf("a renewal during the resync queued %d more holdings requests", len(l.queue))
			}
			answer(t, d, "book")
			answer(t, d, "news")
			answer(t, d, "book") // the resync the renewal asked for
			answer(t, d, "news")
			if c := d.lanes.laneOf(resyncLane()).cur; c != nil || d.resync.running {
				t.Errorf("after the second resync holdings %v, running %v; want none", c, d.resync.running)
			}
		}},
	)
}

// TestDriverResyncLost checks that a resync ends when the session is lost: the holdings request
// that times out after the loss is not sent again.
func TestDriverResyncLost(t *testing.T) {
	sc := Scenario{Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1}}}
	injected(t, sc, 8*time.Second,
		injectStep{time.Second, renew},
		injectStep{5 * time.Second, renew}, // follows the timed-out keepAlive of 2 s: a resync starts
		injectStep{6500 * time.Millisecond, func(t testing.TB, d *driver) {
			if c := d.lanes.laneOf(resyncLane()).cur; holdingsSpace(c) != "book" {
				t.Fatalf("holdings %v in flight at 6.5 s, want book's", c)
			}
			replyTo(t, d, sessionLane, func(h replyHead) reply { return lostReply{h} })
			if s := d.status().Session; s != core.SessionStateLost {
				t.Fatalf("session %v after a lost reply, want lost", s)
			}
		}},
		injectStep{7500 * time.Millisecond, func(t testing.TB, d *driver) {
			if c := d.lanes.laneOf(resyncLane()).cur; c != nil || d.resync.running {
				t.Errorf("after the loss holdings %v in flight, running %v; want the resync ended", c, d.resync.running)
			}
		}},
	)
}

// TestDriverRecordAfterTick checks that the filter decides after the Tick due by the reading's
// time (O2): the answer of a degraded shard's latest Confirm that comes when the machine is due
// to send the next Confirm is no longer the answer of the latest one, so it does not restore.
// The test holds back the wake timer and the Confirm's timeout, so that the answer arrives
// before the wake timer fires, as it may in the same instant.
func TestDriverRecordAfterTick(t *testing.T) {
	sc := Scenario{Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1, Fenced: true}}}
	var restored bool
	var confirms int
	injected(t, sc, 14*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			d.view(soloView(1), "", srcPush)
			grant(t, d, book(0, d.holder, 5, 3))
		}},
		injectStep{11 * time.Second, func(t testing.TB, d *driver) {
			if s := shardState(t, d, 0); s != core.ShardStateDegraded {
				t.Fatalf("book/0 is %v at 11 s, want degraded", s)
			}
			renew(t, d) // live again: the Confirm goes out, and the next is due RetryPeriod later
			c := d.lanes.laneOf(shardLane("book", 0)).cur
			if c == nil {
				t.Fatal("nothing in flight on book/0 after the renewal, want a Confirm")
			}
			if _, ok := c.act.(core.Confirm); !ok {
				t.Fatalf("%v in flight on book/0 after the renewal, want a Confirm", c.act)
			}
			c.timer.cancel()
			d.wake.cancel()
		}},
		injectStep{13500 * time.Millisecond, func(t testing.TB, d *driver) {
			replyTo(t, d, shardLane("book", 0), func(h replyHead) reply { return confirmedReply{h, book(0, d.holder, 5, 3)} })
			restored = shardState(t, d, 0) == core.ShardStateOwned
			confirms = len(actionsOf(d.w.sim, d, "Confirm"))
		}},
	)
	if restored {
		t.Error("book/0 restored by the answer of a Confirm that the Tick due before it followed")
	}
	if confirms != 2 {
		t.Errorf("%d Confirm actions, want 2", confirms)
	}
}
