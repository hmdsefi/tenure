// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// requestID matches the name and the ID at the start of a request's or a reply's text.
var requestID = regexp.MustCompile(`^[a-zA-Z]+#([0-9]+)\b`)

// payloadID returns the request ID in the text of a request or a reply, and false when it has
// none.
func payloadID(text string) (uint64, bool) {
	m := requestID.FindStringSubmatch(text)
	if m == nil {
		return 0, false
	}
	id, err := strconv.ParseUint(m[1], 10, 64)
	return id, err == nil
}

// checkRepliesInEvent checks that every request delivered to the store is answered, to its
// sender, in the event that delivered it, before any push of that event. It returns the number
// of requests the store got.
func checkRepliesInEvent(t testing.TB, sim *kernel.Sim) int {
	t.Helper()
	n := 0
	sent := map[string]string{} // message ID -> payload
	for _, e := range traceEvents(sim) {
		for _, r := range e.of("net.send") {
			sent[attrValue(r, "msg")] = attrValue(r, "payload")
		}
		for _, r := range e.of("net.deliver") {
			if attrValue(r, "to") != storeName {
				continue
			}
			n++
			id, ok := payloadID(sent[attrValue(r, "msg")])
			if !ok {
				t.Errorf("at %s the store got %q, which is no request", e.at, sent[attrValue(r, "msg")])
				continue
			}
			from := e.sends(storeName, "")
			if len(from) == 0 {
				t.Errorf("at %s the store sent nothing for request #%d", e.at, id)
				continue
			}
			first := from[0]
			got, ok := payloadID(attrValue(first, "payload"))
			if !ok || got != id || attrValue(first, "to") != attrValue(r, "from") {
				t.Errorf("at %s the store's first send for request #%d from %s is %q to %s",
					e.at, id, attrValue(r, "from"), attrValue(first, "payload"), attrValue(first, "to"))
			}
		}
	}
	return n
}

// keepAlive returns a keepAliveReq of member i with the timing tm.
func keepAlive(w *World, id uint64, i int, tm core.Timing) keepAliveReq {
	return keepAliveReq{reqHead: reqHead{ID: id, Holder: w.Members()[i].Holder()}, Timing: tm}
}

// endEvent returns the event that emitted the tenure.session op=end record of holder, and the
// record.
func endEvent(sim *kernel.Sim, holder string) (traceEvent, kernel.Record, bool) {
	for _, e := range traceEvents(sim) {
		for _, r := range e.of(kindSession) {
			if attrValue(r, "op") == "end" && attrValue(r, "holder") == holder {
				return e, r, true
			}
		}
	}
	return traceEvent{}, kernel.Record{}, false
}

// openReading returns the store's reading when it opened the session of holder.
func openReading(t testing.TB, w *World, sim *kernel.Sim, holder string) time.Duration {
	t.Helper()
	for _, r := range records(sim, kindSession) {
		if attrValue(r, "op") == "open" && attrValue(r, "holder") == holder {
			return storeReading(w, r.At)
		}
	}
	t.Fatalf("no session of %s opened", holder)
	return 0
}

// TestStoreNode checks the store's node (SIM-260): replies in the event that delivered the
// request, holder and view pushes to the members in NodeID order, the session versions of a
// renewal pushed at once, snapshots every RetryPeriod of the store's own clock under drift, the
// view push of an expiry, and an EndAfter end with tenure.session op=end.
func TestStoreNode(t *testing.T) {
	for _, rule := range []Takeover{Server, Observer} {
		t.Run("replies and pushes/"+rule.String(), func(t *testing.T) {
			runWorld(t, fullTrace(30*time.Second), Scenario{Takeover: rule, Members: 3}, func(fw *faultline.World, w *World) {
				t := fw.T()
				s := scripted(fw, w)
				var grants []check.Grant
				w.storeSrv.onGrant = func(g check.Grant) { grants = append(grants, g) }
				ms := time.Millisecond
				store := w.Store()
				fw.RunFor(5 * ms)
				h := func(i int) string { return w.Members()[i].Holder() }
				head := func(id uint64, i int) reqHead { return reqHead{ID: id, Holder: h(i)} }
				for i := range 3 {
					s.send(time.Duration(10*(i+1))*ms, i, store, keepAlive(w, uint64(i+1), i, core.DefaultTiming))
				}
				s.send(100*ms, 0, store, acquireReq{head(4, 0), "book", 0})
				s.send(150*ms, 0, store, acquireReq{head(5, 0), "book", 1})
				s.send(200*ms, 1, store, acquireReq{head(6, 1), "book", 0})
				s.send(300*ms, 0, store, holdingsReq{head(7, 0), "book"})
				s.send(time.Second, 0, store, keepAlive(w, 8, 0, core.DefaultTiming))
				fw.RunFor(2 * time.Second)

				if n := checkRepliesInEvent(t, fw.Sim); n != 8 {
					t.Errorf("the store got %d requests, want 8", n)
				}
				for id := uint64(1); id <= 8; id++ {
					i := 0
					switch id {
					case 2, 6:
						i = 1
					case 3:
						i = 2
					}
					if s.reply(i, id) == nil {
						t.Errorf("m%d got no reply to #%d", i+1, id)
					}
				}
				if rep, ok := s.reply(1, 6).(heldReply); !ok || rep.Record.Holder != h(0) {
					t.Errorf("m2's acquire of book/0 got %v, want held by m1", s.reply(1, 6))
				}

				all := []string{"m1", "m2", "m3"}
				holderEvents, viewEvents := 0, 0
				for _, e := range traceEvents(fw.Sim) {
					if hp := e.sends(storeName, "holder "); len(hp) > 0 {
						holderEvents++
						if got := sendTargets(hp); !slices.Equal(got, all) {
							t.Errorf("holder pushes at %s go to %v, want %v", e.at, got, all)
						}
					}
					if vp := e.sends(storeName, "view "); len(vp) > 0 {
						viewEvents++
						if got := sendTargets(vp); !slices.Equal(got, all) {
							t.Errorf("view pushes at %s go to %v, want %v", e.at, got, all)
						}
					}
				}
				if holderEvents != 2 || len(grants) != 2 {
					t.Errorf("%d events with holder pushes and %d grants, want 2 and 2", holderEvents, len(grants))
				}
				if viewEvents != 4 { // three opens and a renewal
					t.Errorf("%d events with view pushes, want 4", viewEvents)
				}
				for i, g := range grants {
					if g.Holder != h(0) || g.Space != "book" || g.Shard != i || g.Prev != "" || g.Term == 0 {
						t.Errorf("grant %d: %+v, want book/%d to m1 with no previous holder", i, g, i)
					}
				}

				// The renewal reaches m2 at once, under the view it does not change.
				var renewal *viewPush
				for _, r := range s.inbox[1] {
					if vp, ok := r.msg.(viewPush); ok && r.at < kernel.Time(1500*ms) && len(vp.Sessions) == 3 {
						renewal = &vp
					}
				}
				if renewal == nil {
					t.Fatal("m2 got no view push with three sessions")
				}
				if renewal.View.Version != 3 {
					t.Errorf("the renewal's view push has view version %d, want 3", renewal.View.Version)
				}
				i := slices.IndexFunc(renewal.Sessions, func(si sessionInfo) bool { return si.Holder == h(0) })
				if i < 0 || renewal.Sessions[i].Version != 2 {
					t.Errorf("the renewal's view push has sessions %v, want m1 at version 2", renewal.Sessions)
				}
			})
		})
	}

	t.Run("snapshots", func(t *testing.T) {
		sc := Scenario{Members: 2, Rho: 100_000}
		runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			rp := w.Scenario().Timing.RetryPeriod
			fw.RunFor(5 * time.Millisecond)
			s.send(10*time.Millisecond, 0, w.Store(), keepAlive(w, 1, 0, core.DefaultTiming))
			s.send(20*time.Millisecond, 0, w.Store(),
				acquireReq{reqHead{ID: 2, Holder: w.Members()[0].Holder()}, "book", 3})
			for i := range 9 { // m1's session lives on
				s.send(time.Duration(2*i+1)*time.Second, 0, w.Store(), keepAlive(w, uint64(10+i), 0, core.DefaultTiming))
			}
			fw.RunFor(19 * time.Second)

			if w.Store().Drift() == 0 {
				t.Fatal("the store's clock does not drift")
			}
			k, skewed := time.Duration(0), false
			for _, e := range traceEvents(fw.Sim) {
				snaps := e.sends(storeName, "snapshot ")
				if len(snaps) == 0 {
					continue
				}
				k++
				if e.label != labelStoreSnapshot {
					t.Errorf("a snapshot at %s in event %q", e.at, e.label)
				}
				if got := sendTargets(snaps); !slices.Equal(got, []string{"m1", "m2"}) {
					t.Errorf("snapshot at %s goes to %v", e.at, got)
				}
				reading := storeReading(w, e.at)
				if reading < k*rp || reading > k*rp+time.Microsecond {
					t.Errorf("snapshot %d at the store's reading %v, want [%v, %v]", k, reading, k*rp, k*rp+time.Microsecond)
				}
				if d := time.Duration(e.at) - k*rp; d > time.Microsecond || d < -time.Microsecond {
					skewed = true
				}
			}
			if want := storeReading(w, fw.Sim.Now()) / rp; k != want {
				t.Errorf("%d snapshots by the store's reading %v, want %d", k, storeReading(w, fw.Sim.Now()), want)
			}
			if !skewed {
				t.Error("every snapshot is at a multiple of RetryPeriod of global time: the drift was not followed")
			}
			var last *snapshotPush
			for _, r := range s.inbox[1] {
				if sp, ok := r.msg.(snapshotPush); ok {
					last = &sp
				}
			}
			if last == nil {
				t.Fatal("m2 got no snapshot")
			}
			if len(last.Records) != 1 || last.Records[0].Holder != w.Members()[0].Holder() || last.Records[0].Shard != 3 ||
				len(last.View.Members) != 1 || last.View.Members[0].ID != "m1" ||
				len(last.Sessions) != 1 || last.Sessions[0].Holder != w.Members()[0].Holder() {
				t.Errorf("m2's last snapshot is %+v, want book/3 held by m1, a view of m1 and m1's session", *last)
			}
		})
	})

	t.Run("expiry", func(t *testing.T) {
		runWorld(t, fullTrace(30*time.Second), Scenario{Members: 2, Rho: 100_000}, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			short := core.Timing{LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 500 * time.Millisecond}
			fw.RunFor(5 * time.Millisecond)
			s.send(10*time.Millisecond, 0, w.Store(), keepAlive(w, 1, 0, short))
			fw.RunFor(5 * time.Second)

			h := w.Members()[0].Holder()
			e, r, ok := endEvent(fw.Sim, h)
			if !ok {
				t.Fatalf("m1's session did not end")
			}
			if attrValue(r, "cause") != "expired" || r.Text != "" {
				t.Errorf("the end is %v with text %q, want cause=expired and no text", attrs(r), r.Text)
			}
			if e.label != labelStoreEnd {
				t.Errorf("the end is in event %q, want %q", e.label, labelStoreEnd)
			}
			open := openReading(t, w, fw.Sim, h)
			if got := storeReading(w, e.at); got < open+short.LeaseDuration || got > open+short.LeaseDuration+time.Microsecond {
				t.Errorf("the end is at the store's reading %v, want LeaseDuration after the open at %v", got, open)
			}
			views := e.of(kindView)
			if len(views) != 1 || attrValue(views[0], "members") != "" || attrValue(views[0], "version") != "2" {
				t.Errorf("the end's views are %v, want one of version 2 and no members", views)
			}
			if got := sendTargets(e.sends(storeName, "view version=2 members= ")); !slices.Equal(got, []string{"m1", "m2"}) {
				t.Errorf("the end's view pushes go to %v, want m1 and m2", got)
			}
			if w.storeSrv.end != nil && w.storeSrv.end.pending() {
				t.Error("a session-end timer is pending with no session")
			}
		})
	})

	t.Run("end after", func(t *testing.T) {
		runWorld(t, fullTrace(30*time.Second), Scenario{Members: 1, EndAfter: 5 * time.Second}, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			fw.RunFor(5 * time.Millisecond)
			// The open at 10 ms, renewals at 1.5 s to 4.5 s, then three after the end at about 5 s.
			s.send(10*time.Millisecond, 0, w.Store(), keepAlive(w, 1, 0, core.DefaultTiming))
			for i := 1; i < 8; i++ {
				at := time.Duration(i)*time.Second + 500*time.Millisecond
				s.send(at, 0, w.Store(), keepAlive(w, uint64(i+1), 0, core.DefaultTiming))
			}
			fw.RunFor(9 * time.Second)

			if n := checkRepliesInEvent(t, fw.Sim); n != 8 {
				t.Errorf("the store got %d requests, want 8", n)
			}
			h := w.Members()[0].Holder()
			e, r, ok := endEvent(fw.Sim, h)
			if !ok {
				t.Fatalf("m1's session did not end")
			}
			if want := []string{"op=end", "holder=" + h, "version=5", "cause=end_after"}; !slices.Equal(attrs(r), want) {
				t.Errorf("the end is %v, want %v", attrs(r), want)
			}
			if e.label != labelStoreEnd {
				t.Errorf("the end is in event %q, want %q", e.label, labelStoreEnd)
			}
			open := openReading(t, w, fw.Sim, h)
			if got := storeReading(w, e.at); got < open+5*time.Second || got > open+5*time.Second+time.Microsecond {
				t.Errorf("the end is at the store's reading %v, want EndAfter after the open at %v", got, open)
			}
			if got := sendTargets(e.sends(storeName, "view ")); !slices.Equal(got, []string{"m1"}) {
				t.Errorf("the end's view pushes go to %v, want m1", got)
			}
			for id := uint64(6); id <= 8; id++ {
				if _, ok := s.reply(0, id).(lostReply); !ok {
					t.Errorf("keepAlive #%d after the end got %v, want lost", id, s.reply(0, id))
				}
			}
		})
	})

	t.Run("end timer follows renewals", func(t *testing.T) {
		runWorld(t, fullTrace(30*time.Second), Scenario{Members: 1, Rho: 100_000}, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			short := core.Timing{LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 500 * time.Millisecond}
			fw.RunFor(5 * time.Millisecond)
			s.send(10*time.Millisecond, 0, w.Store(), keepAlive(w, 1, 0, short))
			s.send(time.Second, 0, w.Store(), keepAlive(w, 2, 0, short))
			s.send(2*time.Second, 0, w.Store(), keepAlive(w, 3, 0, short))
			fw.RunFor(8 * time.Second)

			// Each renewal moves the end: the timer set before it is cancelled, so the only
			// end event is the one of the expiry, LeaseDuration after the last renewal.
			var ends []traceEvent
			for _, e := range traceEvents(fw.Sim) {
				if e.label == labelStoreEnd {
					ends = append(ends, e)
				}
			}
			if len(ends) != 1 {
				t.Fatalf("%d session-end events, want 1", len(ends))
			}
			var renewed time.Duration
			for _, r := range records(fw.Sim, kindSession) {
				if attrValue(r, "op") == "renew" {
					renewed = storeReading(w, r.At)
				}
			}
			if got := storeReading(w, ends[0].at); got < renewed+short.LeaseDuration || got > renewed+short.LeaseDuration+time.Microsecond {
				t.Errorf("the end is at the store's reading %v, want LeaseDuration after the last renewal at %v", got, renewed)
			}
			if got := ends[0].of(kindSession); len(got) != 1 || attrValue(got[0], "cause") != "expired" {
				t.Errorf("the end event's sessions are %v, want one end, expired", got)
			}
			// Only the snapshot timer is left.
			if n := len(w.clocks.of(w.Store()).timers); n != 1 || !w.storeSrv.snapshot.pending() {
				t.Errorf("the store has %d pending timers, want the snapshot timer alone", n)
			}
		})
	})

	t.Run("receivers", func(t *testing.T) {
		// A rollout that never activates: m1-r to m3-r stay on standby.
		sc := Scenario{Members: 3, Slots: 3, Rollout: rollout}
		runWorld(t, fullTrace(120*time.Second), sc, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			ms := time.Millisecond
			fw.RunFor(5 * ms)
			s.send(10*ms, 0, w.Store(), keepAlive(w, 1, 0, core.DefaultTiming))
			s.at(time.Second, func() {
				w.Members()[1].phase = Exited
				w.Members()[2].Node.Crash()
			})
			s.send(2500*ms, 0, w.Store(), acquireReq{reqHead{ID: 2, Holder: w.Members()[0].Holder()}, "book", 0})
			fw.RunFor(5 * time.Second)

			// Before 1 s the active members get the pushes; after it, m1 and m3, which is down
			// and whose copies the network drops; never the exited m2 nor a replacement on
			// standby.
			pushed := 0
			for _, e := range traceEvents(fw.Sim) {
				for _, prefix := range []string{"holder ", "view ", "snapshot "} {
					p := e.sends(storeName, prefix)
					if len(p) == 0 {
						continue
					}
					pushed++
					want := []string{"m1", "m2", "m3"}
					if e.at > at(time.Second) {
						want = []string{"m1", "m3"}
					}
					if got := sendTargets(p); !slices.Equal(got, want) {
						t.Errorf("%q pushes at %s go to %v, want %v", prefix, e.at, got, want)
					}
				}
			}
			if pushed < 4 { // a view, snapshots at 2 s and 4 s, a holder push
				t.Errorf("%d events with pushes, want at least 4", pushed)
			}
			dropped := 0
			for _, r := range records(fw.Sim, "net.drop") {
				if attrValue(r, "to") == "m3" && attrValue(r, "reason") == "down" {
					dropped++
				}
			}
			if dropped < 3 {
				t.Errorf("%d pushes to the crashed m3 dropped as down, want at least 3", dropped)
			}
		})
	})

	t.Run("close", func(t *testing.T) {
		runWorld(t, fullTrace(30*time.Second), Scenario{Members: 3}, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			ms := time.Millisecond
			fw.RunFor(5 * ms)
			h := func(i int) string { return w.Members()[i].Holder() }
			s.send(10*ms, 0, w.Store(), keepAlive(w, 1, 0, core.DefaultTiming))
			s.send(20*ms, 1, w.Store(), keepAlive(w, 2, 1, core.DefaultTiming))
			s.send(100*ms, 0, w.Store(), acquireReq{reqHead{ID: 3, Holder: h(0)}, "book", 1})
			s.send(150*ms, 0, w.Store(), acquireReq{reqHead{ID: 4, Holder: h(0)}, "book", 0})
			s.send(500*ms, 0, w.Store(), closeSessionReq{reqHead{ID: 5, Holder: h(0)}})
			fw.RunFor(3 * time.Second)

			// The close clears book/0 and book/1, in shard order, and makes a view of m2: the
			// reply, then each holder push to every member, then the view push.
			var got []string
			for _, e := range traceEvents(fw.Sim) {
				if len(e.sends(storeName, "closed#5")) == 0 {
					continue
				}
				for _, r := range e.sends(storeName, "") {
					f := strings.Fields(attrValue(r, "payload"))
					got = append(got, attrValue(r, "to")+" "+strings.Join(f[:min(len(f), 2)], " "))
				}
			}
			want := []string{
				"m1 closed#5",
				"m1 holder book/0", "m2 holder book/0", "m3 holder book/0",
				"m1 holder book/1", "m2 holder book/1", "m3 holder book/1",
				"m1 view version=3", "m2 view version=3", "m3 view version=3",
			}
			if !slices.Equal(got, want) {
				t.Errorf("the close's sends are\n%v\nwant\n%v", got, want)
			}

			// Every receiver gets its own slices.
			v1, v2 := lastOf[viewPush](s, 1), lastOf[viewPush](s, 2)
			if len(v1.View.Members) != 1 || len(v2.View.Members) != 1 || len(v1.Sessions) != 1 || len(v2.Sessions) != 1 {
				t.Fatalf("the last view pushes of m2 and m3 are %+v and %+v, want a view of m2", v1, v2)
			}
			if &v1.View.Members[0] == &v2.View.Members[0] || &v1.Sessions[0] == &v2.Sessions[0] {
				t.Error("the view pushes of m2 and m3 share their slices")
			}
			p1, p2 := lastOf[snapshotPush](s, 1), lastOf[snapshotPush](s, 2)
			if len(p1.Records) != 2 || len(p2.Records) != 2 || len(p1.View.Members) != 1 || len(p2.View.Members) != 1 ||
				len(p1.Sessions) != 1 || len(p2.Sessions) != 1 {
				t.Fatalf("the last snapshots of m2 and m3 are %+v and %+v, want two records and m2", p1, p2)
			}
			if &p1.Records[0] == &p2.Records[0] || &p1.View.Members[0] == &p2.View.Members[0] || &p1.Sessions[0] == &p2.Sessions[0] {
				t.Error("the snapshots of m2 and m3 share their slices")
			}
		})
	})
}

// lastOf returns the last message of type M that member i of s received, or the zero M.
func lastOf[M any](s *script, i int) M {
	var out M
	for _, r := range s.inbox[i] {
		if m, ok := r.msg.(M); ok {
			out = m
		}
	}
	return out
}

// TestStoreCrashKeepsState checks that the store's sessions, records and slots survive a crash
// of its node, that nothing is answered while it is down, and that its timers are set again at
// the boot after: a session that expired while the store was down ends at that boot, the
// session that lives on keeps its expiry timer, and snapshots go on at the multiples of
// RetryPeriod.
func TestStoreCrashKeepsState(t *testing.T) {
	runWorld(t, fullTrace(90*time.Second), Scenario{Members: 2, Slots: 2}, func(fw *faultline.World, w *World) {
		t := fw.T()
		s := scripted(fw, w)
		ms := time.Millisecond
		store := w.Store()
		long := core.Timing{LeaseDuration: 60 * time.Second, RenewDeadline: 40 * time.Second, RetryPeriod: 2 * time.Second}
		fw.RunFor(5 * ms)
		h := func(i int) string { return w.Members()[i].Holder() }
		head := func(id uint64) reqHead { return reqHead{ID: id, Holder: h(0)} }
		s.send(10*ms, 0, store, keepAlive(w, 1, 0, long))
		s.send(20*ms, 0, store, acquireReq{head(2), "book", 0})
		s.send(30*ms, 0, store, claimSlotReq{head(3)})
		s.send(40*ms, 1, store, keepAlive(w, 4, 1, core.DefaultTiming)) // expires at about 15 s
		s.at(3*time.Second, store.Crash)
		s.send(4*time.Second, 0, store, holdingsReq{head(5), "book"}) // down: never answered
		s.at(20*time.Second, store.Restart)
		s.send(21*time.Second, 0, store, keepAlive(w, 6, 0, long))
		s.send(21100*ms, 0, store, holdingsReq{head(7), "book"})
		s.send(21200*ms, 0, store, claimSlotReq{head(8)})
		fw.RunFor(22 * time.Second)

		if n := checkRepliesInEvent(t, fw.Sim); n != 7 {
			t.Errorf("the store got %d requests, want 7", n)
		}
		if rep := s.reply(0, 5); rep != nil {
			t.Errorf("the store answered #5, sent while it was down: %s", rep.Describe())
		}
		dropped := false
		for _, r := range records(fw.Sim, "net.drop") {
			if attrValue(r, "to") == storeName && attrValue(r, "reason") == "down" {
				dropped = true
			}
		}
		if !dropped {
			t.Error("no message to the store was dropped while it was down")
		}

		// The session, the record and the slot survived.
		if rep, ok := s.reply(0, 6).(renewedReply); !ok || rep.Opened || rep.Version != 2 {
			t.Errorf("m1's keepAlive after the restart got %v, want renewed at version 2", s.reply(0, 6))
		}
		granted, ok := s.reply(0, 2).(grantedReply)
		if !ok {
			t.Fatalf("m1's acquire got %v", s.reply(0, 2))
		}
		if rep, ok := s.reply(0, 7).(holdingsReply); !ok || len(rep.Records) != 1 || rep.Records[0] != granted.Record {
			t.Errorf("holdings after the restart are %v, want %v", s.reply(0, 7), granted.Record)
		}
		if rep, ok := s.reply(0, 8).(slotClaimedReply); !ok || rep.Slot != 0 {
			t.Errorf("claimSlot after the restart got %v, want slot 0 kept", s.reply(0, 8))
		}
		claims := records(fw.Sim, kindSlot)
		if len(claims) != 2 || attrValue(claims[1], "result") != "kept" {
			t.Errorf("slot records %v, want a claim and then a kept slot", claims)
		}

		// The session that expired while the store was down ends at the boot.
		e, r, ok := endEvent(fw.Sim, h(1))
		if !ok || attrValue(r, "cause") != "expired" {
			t.Fatalf("m2's session did not end as expired: %v", r)
		}
		if e.label != labelStoreEnd || e.at != kernel.Time(20*time.Second) {
			t.Errorf("m2's session ended in %q at %s, want %q at the restart", e.label, e.at, labelStoreEnd)
		}
		if got := sendTargets(e.sends(storeName, "view ")); !slices.Equal(got, []string{"m1", "m2"}) {
			t.Errorf("the end's view pushes go to %v", got)
		}

		// The timers of the new boot.
		var renewed time.Duration
		for _, r := range records(fw.Sim, kindSession) {
			if attrValue(r, "op") == "renew" && attrValue(r, "holder") == h(0) {
				renewed = storeReading(w, r.At)
			}
		}
		if end := w.storeSrv.end; !end.pending() || end.target() != renewed+long.LeaseDuration {
			t.Errorf("the session-end timer is pending %v for %v, want m1's expiry at %v",
				end.pending(), end.target(), renewed+long.LeaseDuration)
		}
		if !w.storeSrv.snapshot.pending() || w.storeSrv.snapshot.target() != 24*time.Second {
			t.Errorf("the snapshot timer is pending %v for %v, want 24s",
				w.storeSrv.snapshot.pending(), w.storeSrv.snapshot.target())
		}
		var snaps []kernel.Time
		for _, e := range traceEvents(fw.Sim) {
			if len(e.sends(storeName, "snapshot ")) > 0 {
				snaps = append(snaps, e.at)
			}
		}
		// RetryPeriod is 2 s: one snapshot before the crash at 3 s, none while the store is
		// down, the next at the first multiple after the restart at 20 s.
		want := []kernel.Time{at(2 * time.Second), at(22 * time.Second)}
		if !slices.Equal(snaps, want) {
			t.Errorf("snapshots at %v, want %v", snaps, want)
		}
	})
}
