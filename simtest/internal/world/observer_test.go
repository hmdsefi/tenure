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

	"github.com/hmdsefi/tenure/core"
)

// rig answers the requests of one member's driver, whose node the network cuts off, from a
// store model of the test's own, so that a test can put requests of another holder between the
// driver's. It plays the store's node: a reply goes to the driver first, then a holder push for
// every record written and a view push with the latest view and sessions. The store's time is
// the driver's reading less lag, so that a holder's session can still be live on the store's
// clock when the driver has seen it unrenewed for LeaseDuration.
type rig struct {
	fw  *faultline.World
	w   *World
	m   *storeModel
	lag time.Duration

	// beforeWrite, when set, runs once, before the next cas of the driver is served; hooked is
	// then the text of that cas's reply, and hookedGrants the number of grants after it.
	beforeWrite  func()
	hooked       string
	hookedGrants int
	// grants holds every grant, in order; served the text of every request of the driver
	// served, and replies the text of every reply to it, in order.
	grants  []grantEffect
	served  []string
	replies []string
}

// rigID is the first request ID of the requests the rig makes for other holders.
const rigID = 1 << 40

// runRig runs a world of one member under the Observer rule, with one space of one shard and
// the given slots, for d, with a rig that serves the driver's requests every 10 ms from the
// first event on. setup runs at 5 ms, before the first requests are served; each step runs at
// its time, before the rig serves; check runs at the end of the run.
func runRig(t *testing.T, lag, d time.Duration, setup func(r *rig), check func(t testing.TB, r *rig), steps ...rigStep) {
	t.Helper()
	sc := Scenario{Takeover: Observer, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1}}}
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		m := w.Members()[0]
		fw.Net.Isolate(m.Node.ID())
		r := &rig{fw: fw, w: w, m: newStoreModel(Observer, 0, 0), lag: lag}
		fw.Sim.At(kernel.Time(5*time.Millisecond), "test/setup", func() { setup(r) })
		for _, s := range steps {
			fw.Sim.At(kernel.Time(s.at), "test/inject", func() { s.do(r) })
		}
		for at := 10 * time.Millisecond; at < d; at += 10 * time.Millisecond {
			fw.Sim.At(kernel.Time(at), "test/serve", r.pump)
		}
		fw.RunFor(d)
		check(fw.T(), r)
	})
}

// rigStep is one event of a rig's run: do runs at the time at.
type rigStep struct {
	at time.Duration
	do func(r *rig)
}

// driver returns the driver of the rig's member.
func (r *rig) driver() *driver { return r.w.drivers.current(r.w.Members()[0]) }

// now returns the store's time.
func (r *rig) now() time.Duration { return max(0, r.driver().m.Mono()-r.lag) }

// do hands req to the model and returns its reply, after it took the effects: the grants are
// kept, and the pushes go to the driver after the reply when toDriver.
func (r *rig) do(req request, toDriver bool) reply {
	d := r.driver()
	rep := r.m.handle(r.now(), kernel.Epoch.Add(r.now()), req)
	var pushes []any
	for _, e := range r.m.takeEffects() {
		switch e := e.(type) {
		case grantEffect:
			r.grants = append(r.grants, e)
			r.w.checker.Grant(e.grant()) // the rig's model is the store the checker observes
		case recordEffect:
			pushes = append(pushes, holderPush(e))
		}
	}
	pushes = append(pushes, viewPush{View: r.m.currentView(), Sessions: r.m.sessionList()})
	store := r.w.Store().ID()
	if toDriver {
		r.replies = append(r.replies, rep.Describe())
		d.handle(store, rep)
	}
	for _, p := range pushes {
		d.handle(store, p)
	}
	return rep
}

// other makes a request of holder, which is not the driver's, with the next rig ID.
func (r *rig) other(req request) reply {
	return r.do(req, false)
}

// pump serves every request of the driver in flight, until none is.
func (r *rig) pump() {
	for range 100 {
		var c *call
		for _, l := range r.driver().lanes.all {
			if l.cur != nil {
				c = l.cur
				break
			}
		}
		if c == nil {
			return
		}
		var hook func()
		if _, ok := c.req.(casReq); ok && r.beforeWrite != nil {
			hook, r.beforeWrite = r.beforeWrite, nil
			hook()
		}
		r.served = append(r.served, c.req.Describe())
		rep := r.do(c.req, true)
		if hook != nil {
			r.hooked, r.hookedGrants = rep.Describe(), len(r.grants)
		}
	}
}

// keepAliveOf returns a keepAlive of holder with the rig ID id.
func keepAliveOf(holder string, id uint64) keepAliveReq {
	return keepAliveReq{reqHead: reqHead{ID: rigID + id, Holder: holder}, Timing: core.DefaultTiming}
}

// requestName returns the name of a request's or a reply's text: "getShard", "cas" and so on.
func requestName(text string) string {
	name, _, _ := strings.Cut(text, "#")
	return name
}

// TestATSIM11ObserverTakeover checks AT-SIM-11, the Observer rule's takeover (SIM-230 on the
// member's side). The four cases were written for this task from the issue and the design
// note, since the original acceptance-test text is lost:
//
//   - live: in a world of two members, every Acquire that finds the record held by the other,
//     live, member fails with AcquireFailed naming it, and no member sends getSession or a cas
//     over a record another member holds;
//   - crashed: m2 crashes for good; m1 takes each of its shards only once its own clock has seen
//     m2's last session version for LeaseDuration (the grant's unseen at least the lease, and
//     m1's reading at the grant at least LeaseDuration after its reading at m2's last renewal),
//     with and without clock drift, and in the end owns every shard;
//   - renewal: a holder's renewal and confirmation, or its confirmation alone, between the
//     driver's getSession and its cas make the cas stale: no grant, AcquireFailed naming the
//     holder; a renewal alone does not (the confirmation is what closes that window, AT-SIM-43's
//     Order 2);
//   - ended: a closed session's record is taken in the next round, with no wait, and a replaced
//     session's records are taken by the member's new incarnation at once, with ended=true.
func TestATSIM11ObserverTakeover(t *testing.T) {
	t.Run("live", testTakeoverLive)
	t.Run("crashed", func(t *testing.T) {
		for _, rho := range []int32{0, 100_000} {
			t.Run("rho="+strconv.Itoa(int(rho)), func(t *testing.T) { testTakeoverCrashed(t, rho) })
		}
	})
	t.Run("renewal", testTakeoverRenewal)
	t.Run("ended", func(t *testing.T) {
		t.Run("closed", testTakeoverClosed)
		t.Run("replaced", testTakeoverReplaced)
	})
}

// testTakeoverLive is AT-SIM-11's first case: m2 is cut off until 5 s.
func testTakeoverLive(t *testing.T) {
	sc := Scenario{Takeover: Observer, Members: 2, StopDelay: 2 * time.Second}
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		// m2 reaches no one until 5 s, so m1 takes every shard first and hands m2 its share,
		// each after a stop delay, while m2 acquires it.
		m2 := w.Members()[1]
		fw.Net.Isolate(m2.Node.ID())
		fw.Sim.At(kernel.Time(5*time.Second), "test/heal", fw.Net.Heal)
		fw.RunFor(20*time.Second - time.Nanosecond)
		holders := map[string]bool{}
		for _, m := range w.Members() {
			holders[m.Holder()] = true
		}
		failed := 0
		for _, m := range w.Members() {
			for _, r := range records(fw.Sim, kindInput) {
				if r.Node == m.Node.ID() && isInput(r, "AcquireFailed") {
					if h := attrValue(r, "holder"); h == m.Holder() || !holders[h] {
						t.Errorf("%s: AcquireFailed naming %q, want the other member", m.Name, h)
					}
					failed++
				}
			}
		}
		if failed == 0 {
			t.Error("no Acquire found the record held")
		}
		for _, r := range records(fw.Sim, kindObserve) {
			switch attrValue(r, "op") {
			case "get_session":
				t.Errorf("%s read the session of %s, which renews", attrValue(r, "holder"), attrValue(r, "session"))
			case "cas":
				if h := attrValue(r, "record_holder"); h != "" && h != attrValue(r, "holder") {
					t.Errorf("%s sent cas over a record of %s: %v", attrValue(r, "holder"), h, attrs(r))
				}
			}
		}
		for _, r := range records(fw.Sim, kindGrant) {
			if p := attrValue(r, "prev"); p != "" && p != attrValue(r, "holder") {
				t.Errorf("a grant from the live holder %s: %v", p, attrs(r))
			}
		}
		owned := 0
		for _, m := range w.Members() {
			for _, sh := range w.drivers.current(m).status().Spaces[0].Shards {
				if sh.State == core.ShardStateOwned {
					owned++
				}
			}
		}
		if owned != 16 {
			t.Errorf("%d shards owned at the end, want 16", owned)
		}
	})
}

// testTakeoverCrashed is AT-SIM-11's second case, with the drifts drawn in [-rho, rho].
func testTakeoverCrashed(t *testing.T, rho int32) {
	const crash = 3 * time.Second
	sc := Scenario{Takeover: Observer, Members: 2, Rho: rho}
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		m1, m2 := w.Members()[0], w.Members()[1]
		fw.Sim.At(kernel.Time(crash), "test/crash", m2.Node.Crash)
		fw.RunFor(30*time.Second - time.Nanosecond)

		gone := m2.Holder()
		var lastRenew kernel.Time
		for _, r := range records(fw.Sim, kindSession) {
			if attrValue(r, "holder") == gone {
				lastRenew = r.At
			}
		}
		// m1's reading at a global time: its drift is the one drawn at the start.
		reading := func(at kernel.Time) time.Duration {
			g := time.Duration(at)
			return g + gain(g, m1.Node.Drift())
		}
		lease := w.Scenario().Timing.LeaseDuration
		taken := 0
		for _, r := range records(fw.Sim, kindGrant) {
			if attrValue(r, "prev") != gone {
				continue
			}
			taken++
			if attrValue(r, "holder") != m1.Holder() || attrValue(r, "ended") != "false" {
				t.Errorf("grant %v: want m1, not ended", attrs(r))
			}
			if unseen := nanosAttr(t, r, "unseen_ns"); unseen < lease || nanosAttr(t, r, "lease_ns") != lease {
				t.Errorf("grant %v: unseen below the lease %v", attrs(r), lease)
			}
			if seen := reading(r.At) - reading(lastRenew); seen < lease {
				t.Errorf("book/%s taken at %s, %v of m1's clock after m2's last renewal at %s, want at least %v",
					attrValue(r, "shard"), r.At, seen, lastRenew, lease)
			}
		}
		if taken == 0 {
			t.Fatal("m1 took none of m2's shards")
		}
		for _, sh := range w.drivers.current(m1).status().Spaces[0].Shards {
			if sh.State != core.ShardStateOwned {
				t.Errorf("m1's shard is %v at the end, want owned", sh.State)
			}
		}
	})
}

// testTakeoverRenewal is AT-SIM-11's third case: the holder a renews at the store's time 0 and
// not again until the driver's cas, and the store's clock lags the driver's by 2 s, so a is
// still live on the store's clock when the driver has seen it unrenewed for LeaseDuration.
func testTakeoverRenewal(t *testing.T) {
	const a = "m2/0b"
	cases := []struct {
		name        string
		renew, conf bool
		stale       bool
	}{
		{"renew_confirm", true, true, true},
		{"confirm", false, true, true},
		{"renew_only", true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setup := func(r *rig) {
				r.other(keepAliveOf(a, 1))
				g := r.other(casReq{reqHead: reqHead{ID: rigID + 2, Holder: a}, Space: "book", Wall: kernel.Epoch})
				granted, ok := g.(grantedReply)
				if !ok {
					r.fw.T().Fatalf("a's cas answered %q", g.Describe())
				}
				term := granted.Record.Term
				r.beforeWrite = func() {
					if tc.renew {
						r.other(keepAliveOf(a, 3))
					}
					if tc.conf {
						r.other(confirmReq{reqHead: reqHead{ID: rigID + 4, Holder: a}, Space: "book", Term: term})
					}
				}
			}
			check := func(t testing.TB, r *rig) {
				if r.hooked == "" {
					t.Fatal("the driver sent no cas")
				}
				// The driver's requests other than keepAlive, from its first getSession on.
				var seq []string
				for _, s := range r.served {
					if n := requestName(s); n != "keepAlive" && (len(seq) > 0 || n == "getSession") {
						seq = append(seq, n)
					}
				}
				if len(seq) < 2 || seq[0] != "getSession" || seq[1] != "cas" {
					t.Fatalf("requests from the first getSession %v, want getSession then cas", seq)
				}
				if !tc.stale {
					if requestName(r.hooked) != "granted" || r.grants[r.hookedGrants-1].Record.Holder != r.driver().holder {
						t.Errorf("the cas was answered %q, grants %+v; want a grant to the driver", r.hooked, r.grants)
					}
					return
				}
				if requestName(r.hooked) != "stale" || r.hookedGrants != 1 {
					t.Errorf("the cas was answered %q with %d grants so far, want stale and a's grant alone", r.hooked, r.hookedGrants)
				}
				failed := false
				for _, rec := range records(r.w.sim, kindInput) {
					if isInput(rec, "AcquireFailed") && attrValue(rec, "holder") == a && rec.Text == r.hooked {
						failed = true
					}
				}
				if !failed {
					t.Errorf("no AcquireFailed naming a from %q", r.hooked)
				}
			}
			runRig(t, 2*time.Second, 17*time.Second, setup, check)
		})
	}
}

// testTakeoverClosed is AT-SIM-11's fourth case for a closed session: the holder a closes its
// session at 5 s, which clears its record, and the driver takes the shard within a second.
func testTakeoverClosed(t *testing.T) {
	const a = "m2/0b"
	setup := func(r *rig) {
		r.other(keepAliveOf(a, 1))
		r.other(casReq{reqHead: reqHead{ID: rigID + 2, Holder: a}, Space: "book", Wall: kernel.Epoch})
	}
	check := func(t testing.TB, r *rig) {
		if len(r.grants) != 2 || r.grants[1].Record.Holder != r.driver().holder || r.grants[1].Prev != "" {
			t.Fatalf("grants %+v, want a's, then the driver's of the cleared record", r.grants)
		}
		var at kernel.Time
		for _, rec := range records(r.w.sim, kindInput) {
			if isInput(rec, "Acquired") && attrValue(rec, "term") != "0" {
				at = rec.At
			}
		}
		if at < kernel.Time(5*time.Second) || at > kernel.Time(6*time.Second) {
			t.Errorf("Acquired at %s, want within a second of the close at 5 s", at)
		}
	}
	runRig(t, 0, 8*time.Second, setup, check, rigStep{5 * time.Second, func(r *rig) {
		r.other(closeSessionReq{reqHead{ID: rigID + 3, Holder: a}})
	}})
}

// testTakeoverReplaced is AT-SIM-11's fourth case for a replaced session: m2 crashes at 5 s and
// boots again at 5.5 s; the store ends its old session when the new one opens, and the new
// incarnation takes the shards of the old one at once.
func testTakeoverReplaced(t *testing.T) {
	sc := Scenario{Takeover: Observer, Members: 2}
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		m2 := w.Members()[1]
		var old string
		fw.Sim.At(kernel.Time(5*time.Second), "test/crash", func() {
			old = m2.Holder()
			m2.Node.Crash()
		})
		fw.Sim.At(kernel.Time(5500*time.Millisecond), "test/restart", m2.Node.Restart)
		fw.RunFor(15*time.Second - time.Nanosecond)
		taken := 0
		for _, r := range records(fw.Sim, kindGrant) {
			if attrValue(r, "prev") != old {
				continue
			}
			taken++
			if attrValue(r, "holder") != m2.Holder() || attrValue(r, "ended") != "true" {
				t.Errorf("grant %v: want the new incarnation, ended", attrs(r))
			}
			if r.At > kernel.Time(8*time.Second) {
				t.Errorf("grant %v at %s, want within 2.5 s of the restart", attrs(r), r.At)
			}
		}
		if taken == 0 {
			t.Error("no shard of the old incarnation taken")
		}
	})
}

// TestObserverViews checks the Observer rule's views (SIM-330): the store's view is stepped
// under the driver's own versions, only when the content changes; a member whose session the
// driver has seen unrenewed for LeaseDuration drops out at that reading, by the driver's timer,
// and a slot it holds shows vacant; a renewal brings it back; and while the base view was read
// before the latest slot reply, nothing is stepped (O8).
func TestObserverViews(t *testing.T) {
	sc := Scenario{Takeover: Observer, Members: 1, Slots: 2}
	var got []string
	view := func(version uint64, members ...string) viewPush {
		v := core.View{Version: version, Slots: 2, SlotHolders: []string{"m2/0b", ""}}
		for _, m := range members {
			v.Members = append(v.Members, core.Member{ID: m})
		}
		return viewPush{View: v}
	}
	sessions := func(d *driver, v2, v3 uint64) []sessionInfo {
		lease := d.timing.LeaseDuration
		return []sessionInfo{
			{Holder: d.holder, Version: 1, Lease: lease},
			{Holder: "m2/0b", Version: v2, Lease: lease},
			{Holder: "m3/0c", Version: v3, Lease: lease},
		}
	}
	store := func(d *driver) kernel.NodeID { return d.w.Store().ID() }
	injected(t, sc, 20*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			p := view(3, "m1", "m2", "m3")
			p.Sessions = sessions(d, 1, 1)
			d.handle(store(d), p)
			d.handle(store(d), p)
		}},
		// m2 renews: the same view, newer sessions. m3 does not.
		injectStep{3 * time.Second, func(t testing.TB, d *driver) {
			p := view(3, "m1", "m2", "m3")
			p.Sessions = sessions(d, 2, 1)
			d.handle(store(d), p)
		}},
		// m3 expires at 16 s by the timer. A slot reply at 17 s raises the floor to 5, so m2's
		// expiry at 18 s steps nothing until the view of version 5 comes at 19 s.
		injectStep{17 * time.Second, func(t testing.TB, d *driver) {
			c := d.callFor(core.ReleaseSlot{Slot: 1})
			c.req = withID(c.req, reqHead{ID: 1, Holder: d.holder})
			d.answer(c, slotReleasedReply{replyHead: replyHead{ID: 1}, Slot: 1, View: 5})
		}},
		injectStep{19 * time.Second, func(t testing.TB, d *driver) {
			p := view(5, "m1", "m2", "m3")
			p.Sessions = sessions(d, 2, 1)
			d.handle(store(d), p)
			p.Sessions = sessions(d, 3, 1)
			d.handle(store(d), p)
			got = nil
			for _, s := range inputsAfter(d.w.sim, d, kernel.Time(time.Second), "version", "members", "slot", "slots", "src") {
				if strings.HasPrefix(s, "ViewChanged") || strings.HasPrefix(s, "SlotReleased") {
					got = append(got, s)
				}
			}
		}},
	)
	want := []string{
		"ViewChanged version=1 members=m1,m2,m3 slots=2:m2/0b, src=push",
		"ViewChanged version=2 members=m1,m2 slots=2:m2/0b, src=observe",
		"SlotReleased slot=1",
		"ViewChanged version=3 members=m1 slots=2:, src=push",
		"ViewChanged version=4 members=m1,m2 slots=2:m2/0b, src=push",
	}
	if !slices.Equal(got, want) {
		t.Errorf("inputs\n\t%s\nwant\n\t%s", strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
	}
}

// TestObserverClaimSlot checks ClaimSlot under the Observer rule: with both slots held by live
// holders nothing is sent; once m2, which holds slot 0, is observed expired, the driver reads
// slot 0, reads m2's session, still unrenewed, and writes with casSlot, carrying the time it has
// seen m2 unrenewed; the answer steps SlotClaimed.
func TestObserverClaimSlot(t *testing.T) {
	sc := Scenario{Takeover: Observer, Members: 1, Slots: 2}
	var sent []string
	var unseen time.Duration
	var claimed bool
	push := func(d *driver, m3 uint64) {
		lease := d.timing.LeaseDuration
		d.handle(d.w.Store().ID(), viewPush{
			View: core.View{Version: 2, Members: []core.Member{{ID: "m1"}, {ID: "m2"}, {ID: "m3"}}, Slots: 2, SlotHolders: []string{"m2/0b", "m3/0c"}},
			Sessions: []sessionInfo{
				{Holder: d.holder, Version: 1, Lease: lease},
				{Holder: "m2/0b", Version: 4, Lease: lease},
				{Holder: "m3/0c", Version: m3, Lease: lease},
			},
		})
	}
	slotLane := laneKey{kind: laneSlot}
	steps := []injectStep{{time.Second, func(t testing.TB, d *driver) {
		renew(t, d)
		push(d, 1)
		if c := d.lanes.laneOf(slotLane).cur; c != nil {
			t.Errorf("with no slot to claim, %q went out", c.req.Describe())
		}
	}}}
	// m3 renews every 2 s; the keepAlives of the driver are answered.
	for i, v := 1, uint64(2); i <= 9; i, v = i+1, v+1 {
		steps = append(steps, injectStep{time.Duration(2*i+1) * time.Second, func(t testing.TB, d *driver) {
			push(d, v)
			if c := d.lanes.laneOf(sessionLane).cur; c != nil {
				replyTo(t, d, sessionLane, func(h replyHead) reply { return renewedReply{replyHead: h, Version: v} })
			}
		}})
	}
	steps = append(steps, injectStep{18 * time.Second, func(t testing.TB, d *driver) {
		c := d.lanes.laneOf(slotLane).cur
		if c == nil {
			t.Fatal("no ClaimSlot in flight after m2 expired")
		}
		sent = []string{c.req.Describe()}
		replyTo(t, d, slotLane, func(h replyHead) reply {
			return slotReply{replyHead: h, Record: slotRecord{Slot: 0, Holder: "m2/0b", Version: 7}, Count: 2}
		})
		sent = append(sent, c.req.Describe())
		replyTo(t, d, slotLane, func(h replyHead) reply {
			return sessionReply{h, sessionInfo{Holder: "m2/0b", Version: 4, Lease: d.timing.LeaseDuration}}
		})
		sent = append(sent, c.req.Describe())
		if r, ok := c.req.(casSlotReq); ok {
			unseen = r.Unseen
		}
		replyTo(t, d, slotLane, func(h replyHead) reply { return slotClaimedReply{replyHead: h, Slot: 0, View: 3} })
		claimed = d.status().Slot == 0
	}})
	injected(t, sc, 18500*time.Millisecond, steps...)
	if len(sent) != 3 {
		t.Fatalf("requests %v", sent)
	}
	for i, prefix := range []string{"getSlot#", "getSession#", "casSlot#"} {
		if !strings.HasPrefix(sent[i], prefix) {
			t.Errorf("request %d is %q, want %s…", i, sent[i], prefix)
		}
	}
	if !strings.Contains(sent[0], "slot=0") || !strings.Contains(sent[1], "of=m2/0b") || !strings.Contains(sent[2], "slot=0 expect=7") {
		t.Errorf("requests %v, want slot 0, m2's session, expect=7", sent)
	}
	if unseen < 15*time.Second {
		t.Errorf("casSlot unseen %v, want at least the lease", unseen)
	}
	if !claimed {
		t.Error("slot 0 not held after slotClaimed")
	}
}

// TestObserverTimeout checks that one RetryPeriod timeout, from the first request, covers a
// takeover: an Acquire whose getShard is answered and whose getSession is not steps
// Acquired{Term: 0} once, RetryPeriod after the getShard went out, and the late answer of the
// getSession is dropped with tenure.reply_late.
func TestObserverTimeout(t *testing.T) {
	sc := Scenario{Takeover: Observer, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1}}}
	lane := shardLane("book", 0)
	var c *call
	var sentAt time.Duration
	var sessionID uint64
	var got []string
	injected(t, sc, 5*time.Second,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			d.view(soloView(2), "", srcPush)
			c = d.lanes.laneOf(lane).cur
			if c == nil {
				t.Fatal("no Acquire")
			}
			sentAt = c.sentAt
			replyTo(t, d, lane, func(h replyHead) reply { return shardReply{h, book(0, "m2/0b", 3, 2)} })
			if _, ok := c.req.(getSessionReq); !ok {
				t.Fatalf("after the record, %q in flight, want getSession", c.req.Describe())
			}
			sessionID = c.req.head().ID
		}},
		injectStep{4 * time.Second, func(t testing.TB, d *driver) {
			d.reply(sessionReply{replyHead{sessionID}, sessionInfo{Holder: "m2/0b", Version: 1}})
			got = nil
			for _, r := range driverRecords(d.w.sim)[d.m.Node.ID()] {
				switch {
				case isInput(r, "Acquired"):
					got = append(got, "Acquired term="+attrValue(r, "term")+" at="+strconv.FormatInt(int64(nanosAttr(t, r, "now_ns")-sentAt), 10))
				case r.Kind == kindReplyLate:
					got = append(got, "late "+attrValue(r, "type"))
				}
			}
		}},
	)
	if len(got) != 2 || !strings.HasPrefix(got[0], "Acquired term=0 at=") || got[1] != "late session" {
		t.Fatalf("got %v, want one Acquired with no term, then the late session answer", got)
	}
	at, _ := strconv.ParseInt(strings.TrimPrefix(got[0], "Acquired term=0 at="), 10, 64)
	if rp := core.DefaultTiming.RetryPeriod; time.Duration(at) < rp || time.Duration(at) > rp+time.Microsecond {
		t.Errorf("Acquired %v after the getShard, want RetryPeriod %v", time.Duration(at), rp)
	}
}

// TestObserverSessionRecheck checks the fresh read of a takeover: before m2 is observed
// expired, an Acquire of its record fails at the record, with no getSession; once it is, the
// driver reads m2's session, and a version above the one observed (a renewal the driver had not
// seen) fails the Acquire with no cas, and makes m2 unexpired again.
func TestObserverSessionRecheck(t *testing.T) {
	sc := Scenario{Takeover: Observer, Members: 1, Spaces: []core.SpaceSpec{{Name: "book", Shards: 1}}}
	lane := shardLane("book", 0)
	var got []string
	held := func(h replyHead) reply { return shardReply{h, book(0, "m2/0b", 3, 2)} }
	var steps []injectStep
	// The keepAlives are answered, and every Acquire before 16.5 s finds m2's record.
	for i, v := 1, uint64(2); i <= 7; i, v = i+1, v+1 {
		steps = append(steps, injectStep{time.Duration(2*i+1)*time.Second + time.Millisecond, func(t testing.TB, d *driver) {
			if d.lanes.laneOf(sessionLane).cur != nil {
				replyTo(t, d, sessionLane, func(h replyHead) reply { return renewedReply{replyHead: h, Version: v} })
			}
			if c := d.lanes.laneOf(lane).cur; c != nil && i < 7 {
				replyTo(t, d, lane, held)
			}
		}})
	}
	injected(t, sc, 16600*time.Millisecond, append(steps,
		injectStep{time.Second, func(t testing.TB, d *driver) {
			renew(t, d)
			lease := d.timing.LeaseDuration
			d.handle(d.w.Store().ID(), viewPush{View: soloView(1), Sessions: []sessionInfo{
				{Holder: d.holder, Version: 1, Lease: lease}, {Holder: "m2/0b", Version: 1, Lease: lease},
			}})
			c := d.lanes.laneOf(lane).cur
			replyTo(t, d, lane, held)
			if d.lanes.laneOf(lane).cur != nil {
				t.Errorf("before m2 expired, %q went out after the record", c.req.Describe())
			}
		}},
		// The Acquire sent at 15 s is in flight; m2 has been seen unrenewed since 1 s.
		injectStep{16500 * time.Millisecond, func(t testing.TB, d *driver) {
			c := d.lanes.laneOf(lane).cur
			if c == nil {
				t.Fatal("no Acquire in flight at 16.5 s")
			}
			replyTo(t, d, lane, held)
			got = []string{requestName(c.req.Describe())}
			replyTo(t, d, lane, func(h replyHead) reply {
				return sessionReply{h, sessionInfo{Holder: "m2/0b", Version: 2, Lease: d.timing.LeaseDuration}}
			})
			got = append(got, requestName(c.req.Describe()))
			if d.lanes.laneOf(lane).cur != nil {
				got = append(got, "in flight")
			}
			if d.observedExpired("m2/0b") {
				got = append(got, "expired")
			}
			for _, r := range records(d.w.sim, kindInput) {
				if isInput(r, "AcquireFailed") && strings.HasPrefix(r.Text, "session#") {
					got = append(got, "AcquireFailed "+attrValue(r, "holder"))
				}
			}
		}},
	)...)
	if want := []string{"getSession", "getSession", "AcquireFailed m2/0b"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
