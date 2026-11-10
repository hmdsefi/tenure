// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The holders of the tests: m2/x and m1/c are later incarnations of m2 and m1.
const (
	holderA = "m1/a"
	holderB = "m2/b"
	holderC = "m3/c"
	holderD = "m4/d"
	holderE = "m5/e"
	holderX = "m2/x"
	holderZ = "m9/z"
)

// timingB has a RetryPeriod of its own, so that a held reply shows whose RetryPeriod it carries.
var timingB = core.Timing{
	LeaseDuration: 15 * time.Second,
	RenewDeadline: 10 * time.Second,
	RetryPeriod:   3 * time.Second,
	SlotGrace:     60 * time.Second,
}

// timingShort has a LeaseDuration of 5 s.
var timingShort = core.Timing{
	LeaseDuration: 5 * time.Second,
	RenewDeadline: 3 * time.Second,
	RetryPeriod:   time.Second,
	SlotGrace:     60 * time.Second,
}

// modelStep is one call of the model and what it must give.
type modelStep struct {
	// clause names the step in a failure.
	clause string
	// at is the store's monotonic reading.
	at time.Duration
	// wall is the store's wall time; the zero value means time.UnixMilli of at, so that a term
	// granted at 6 s is 6000.
	wall time.Time
	// req is the request; nil calls expire(at) instead.
	req request
	// reply is the reply's text without its "#<id>"; it is not checked for expire.
	reply string
	// effects are the texts of the effects the call left in the outbox, in order.
	effects []string
	// check, when set, looks further at the model and the reply after the call.
	check func(t *testing.T, m *storeModel, rep reply)
}

// runSteps runs the steps in order on m. Each request gets the step's number as its ID, and
// its reply must carry it.
func runSteps(t *testing.T, m *storeModel, steps []modelStep) {
	t.Helper()
	for i, s := range steps {
		id := uint64(i + 1)
		wall := s.wall
		if wall.IsZero() {
			wall = time.UnixMilli(s.at.Milliseconds())
		}
		var rep reply
		if s.req == nil {
			m.expire(s.at)
		} else {
			req := stampID(s.req, id)
			rep = m.handle(s.at, wall, req)
			if rep.requestID() != id {
				t.Fatalf("step %d (%s): reply %q answers request %d, want %d", id, s.clause, rep.Describe(), rep.requestID(), id)
			}
			got := strings.Replace(rep.Describe(), "#"+strconv.FormatUint(id, 10), "", 1)
			if got != s.reply {
				t.Fatalf("step %d (%s): reply %q, want %q", id, s.clause, got, s.reply)
			}
		}
		got := effectTexts(m.takeEffects())
		if !slices.Equal(got, s.effects) {
			t.Fatalf("step %d (%s): effects\n\t%s\nwant\n\t%s", id, s.clause,
				strings.Join(got, "\n\t"), strings.Join(s.effects, "\n\t"))
		}
		if s.check != nil {
			s.check(t, m, rep)
		}
	}
}

// stamp returns req with the ID id.
func stampID(req request, id uint64) request {
	switch r := req.(type) {
	case keepAliveReq:
		r.ID = id
		return r
	case acquireReq:
		r.ID = id
		return r
	case releaseReq:
		r.ID = id
		return r
	case confirmReq:
		r.ID = id
		return r
	case closeSessionReq:
		r.ID = id
		return r
	case holdingsReq:
		r.ID = id
		return r
	case claimSlotReq:
		r.ID = id
		return r
	case releaseSlotReq:
		r.ID = id
		return r
	case setSlotsReq:
		r.ID = id
		return r
	case getShardReq:
		r.ID = id
		return r
	case getSlotReq:
		r.ID = id
		return r
	case getSessionReq:
		r.ID = id
		return r
	case casReq:
		r.ID = id
		return r
	case casSlotReq:
		r.ID = id
		return r
	}
	panic(fmt.Sprintf("stamp: %T", req))
}

// The requests of the tests, with ID 0 for runSteps to stamp.

func reqKeepAlive(holder string, timing core.Timing) keepAliveReq {
	return keepAliveReq{reqHead{Holder: holder}, timing}
}

func reqAcquire(holder, space string, shard int) acquireReq {
	return acquireReq{reqHead{Holder: holder}, space, shard}
}

func reqRelease(holder, space string, shard int, t core.Term) releaseReq {
	return releaseReq{reqHead{Holder: holder}, space, shard, t}
}

func reqConfirm(holder, space string, shard int, t core.Term) confirmReq {
	return confirmReq{reqHead{Holder: holder}, space, shard, t}
}

func reqCloseSession(holder string) closeSessionReq {
	return closeSessionReq{reqHead{Holder: holder}}
}

func reqHoldings(holder, space string) holdingsReq {
	return holdingsReq{reqHead{Holder: holder}, space}
}

func reqClaimSlot(holder string) claimSlotReq {
	return claimSlotReq{reqHead{Holder: holder}}
}

func reqReleaseSlot(holder string, slot int) releaseSlotReq {
	return releaseSlotReq{reqHead{Holder: holder}, slot}
}

func reqSetSlots(holder string, count int) setSlotsReq {
	return setSlotsReq{reqHead{Holder: holder}, count}
}

func reqGetShard(holder, space string, shard int) getShardReq {
	return getShardReq{reqHead{Holder: holder}, space, shard}
}

func reqGetSlot(holder string, slot int) getSlotReq {
	return getSlotReq{reqHead{Holder: holder}, slot}
}

func reqGetSession(holder, of string) getSessionReq {
	return getSessionReq{reqHead{Holder: holder}, of}
}

func reqCas(holder, space string, shard int, expect uint64, wallMS int64, unseen time.Duration) casReq {
	return casReq{reqHead{Holder: holder}, space, shard, expect, time.UnixMilli(wallMS), unseen}
}

func reqCasSlot(holder string, slot int, expect uint64, unseen time.Duration) casSlotReq {
	return casSlotReq{reqHead{Holder: holder}, slot, expect, unseen}
}

// effectTexts returns the text of each effect.
func effectTexts(effects []effect) []string {
	var out []string
	for _, e := range effects {
		out = append(out, effectText(e))
	}
	return out
}

// effectText returns a text that shows every field of the effect.
func effectText(e effect) string {
	switch e := e.(type) {
	case sessionEffect:
		s := "session " + string(e.Op) + " " + e.Holder + " version=" + strconv.FormatUint(e.Version, 10)
		if e.Cause != "" {
			s += " " + string(e.Cause)
		}
		return s
	case grantEffect:
		return "grant " + msgRecord(e.Record) + " prev=" + msgHolder(e.Prev) +
			" ended=" + strconv.FormatBool(e.Ended) + " unseen=" + msgDuration(e.Unseen) +
			" lease=" + msgDuration(e.Lease)
	case recordEffect:
		return "record " + msgRecord(e.Record)
	case releaseEffect:
		result := "mismatch"
		if e.Cleared {
			result = "cleared"
		}
		return "release " + e.Holder + " term=" + msgTerm(e.Term) + " " + msgRecord(e.Record) + " " + result
	case confirmEffect:
		how := "read"
		if e.Wrote {
			how = "wrote"
		}
		return "confirm " + e.Holder + " term=" + msgTerm(e.Term) + " " + msgRecord(e.Record) + " " + how
	case observeEffect:
		s := "observe " + string(e.Op) + " " + e.Holder + " "
		switch {
		case e.OfSlot:
			s += e.Slot.text() + " count=" + strconv.Itoa(e.Count)
		case e.Op == observeGetSession:
			s += e.Session.text()
		default:
			s += msgRecord(e.Record)
		}
		if e.Op == observeCAS {
			s += " expect=" + strconv.FormatUint(e.Expect, 10)
		}
		return s + " " + string(e.Result)
	case slotEffect:
		return "slot " + string(e.Op) + " " + e.Holder + " slot=" + strconv.Itoa(e.Slot) +
			" count=" + strconv.Itoa(e.Count) + " " + string(e.Result)
	case viewEffect:
		sessions := make([]string, len(e.Sessions))
		for i, s := range e.Sessions {
			sessions[i] = s.Holder + ":" + strconv.FormatUint(s.Version, 10)
		}
		return modelViewText(e.View) + " sessions=" + strings.Join(sessions, ",")
	}
	panic(fmt.Sprintf("effectText: %T", e))
}

// viewText returns, for example, "view version=8 members=m2@0,m4 slots=3:m2/x,,": a member
// with a slot in a view with slots as <id>@<slot>, and the slots as the count, ":" and the
// slot holders.
func modelViewText(v core.View) string {
	ids := make([]string, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.ID
		if v.Slots > 0 && m.Slot >= 0 {
			ids[i] += "@" + strconv.Itoa(m.Slot)
		}
	}
	slots := strconv.Itoa(v.Slots)
	if v.Slots > 0 {
		slots += ":" + strings.Join(v.SlotHolders, ",")
	}
	return "view version=" + strconv.FormatUint(v.Version, 10) + " members=" + strings.Join(ids, ",") +
		" slots=" + slots
}

// wantNextEnd returns a check that nextEnd is at, or that there is none for a negative at.
func wantNextEnd(at time.Duration) func(*testing.T, *storeModel, reply) {
	return func(t *testing.T, m *storeModel, _ reply) {
		t.Helper()
		got, ok := m.nextEnd()
		switch {
		case at < 0 && ok:
			t.Fatalf("nextEnd = %v, want none", got)
		case at >= 0 && (!ok || got != at):
			t.Fatalf("nextEnd = %v, %v, want %v", got, ok, at)
		}
	}
}

// wantRecords returns a check that the reply is a holdingsReply with the records.
func wantRecords(want []tenure.Holding) func(*testing.T, *storeModel, reply) {
	return func(t *testing.T, _ *storeModel, rep reply) {
		t.Helper()
		h, ok := rep.(holdingsReply)
		if !ok {
			t.Fatalf("reply %s, want holdings", rep.Describe())
		}
		got := h.Records
		if !slices.Equal(got, want) || (got == nil) != (want == nil) {
			t.Fatalf("records %+v, want %+v", got, want)
		}
	}
}

// wantRecord returns a check that the store's record of the shard is want.
func wantRecord(want tenure.Holding) func(*testing.T, *storeModel, reply) {
	return func(t *testing.T, m *storeModel, _ reply) {
		t.Helper()
		if got := m.record(want.Space, want.Shard); got != want {
			t.Fatalf("record %+v, want %+v", got, want)
		}
	}
}

// TestATSIM10ServerStore drives the model under the server rule through every request: its
// clauses were written for this test from the store model's requirements and memstore's
// rules, in this order: sessions open; a renewal stores the timing; a grant takes the next
// version and core.NextTerm of the store's wall; a held shard answers with the acquirer's own
// RetryPeriod; a holder takes its own shard again in a new term; releases compare holder and
// term; confirm only reads; holdings are sorted by shard; the store's clock ends a session
// LeaseDuration after its renewal and not a nanosecond before, and the shard is taken then; a
// renewal at the expiry ends the session as late; a newer incarnation replaces the session of
// its member; a release clears and keeps the term; slots are claimed, kept, retired, released
// and set; closeSession clears records and slots; EndAfter ends a session; and the view and its
// counter follow every change of members and slots, as the last effect of the call.
func TestATSIM10ServerStore(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Server, 0, 0)
	runSteps(t, m, []modelStep{
		{
			clause: "open", at: time.Second, req: reqKeepAlive(holderA, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=0 sessions=m1/a:1",
			},
			check: wantNextEnd(16 * time.Second),
		},
		{
			clause: "open a second member", at: time.Second, req: reqKeepAlive(holderB, timingB),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m2/b version=1",
				"view version=2 members=m1,m2 slots=0 sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "renew", at: 5 * time.Second, req: reqKeepAlive(holderA, ta),
			reply:   "renewed version=2",
			effects: []string{"session renew m1/a version=2"},
			check:   wantNextEnd(16 * time.Second), // m2/b's expiry; m1/a's moved to 20 s
		},
		{
			clause: "grant", at: 6 * time.Second, req: reqAcquire(holderA, "book", 3),
			reply: "granted book/3 holder=m1/a term=6000 version=1",
			effects: []string{
				"grant book/3 holder=m1/a term=6000 version=1 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/3 holder=m1/a term=6000 version=1",
			},
		},
		{
			clause: "held, with the acquirer's RetryPeriod", at: 7 * time.Second, req: reqAcquire(holderB, "book", 3),
			reply: "held book/3 holder=m1/a term=6000 version=1 retry=3.000000000s",
		},
		{
			clause: "re-grant to the holder", at: 8 * time.Second, req: reqAcquire(holderA, "book", 3),
			reply: "granted book/3 holder=m1/a term=8000 version=2",
			effects: []string{
				"grant book/3 holder=m1/a term=8000 version=2 prev=m1/a ended=false unseen=3.000000000s lease=15.000000000s",
				"record book/3 holder=m1/a term=8000 version=2",
			},
		},
		{
			clause: "release with an old term", at: 8 * time.Second, req: reqRelease(holderA, "book", 3, 6000),
			reply:   "released book/3 holder=m1/a term=8000 version=2",
			effects: []string{"release m1/a term=6000 book/3 holder=m1/a term=8000 version=2 mismatch"},
		},
		{
			clause: "release by another holder", at: 8 * time.Second, req: reqRelease(holderB, "book", 3, 8000),
			reply:   "released book/3 holder=m1/a term=8000 version=2",
			effects: []string{"release m2/b term=8000 book/3 holder=m1/a term=8000 version=2 mismatch"},
		},
		{
			clause: "release of no record", at: 8 * time.Second, req: reqRelease(holderA, "book", 7, 1),
			reply:   "released book/7 holder=- term=0 version=0",
			effects: []string{"release m1/a term=1 book/7 holder=- term=0 version=0 mismatch"},
		},
		{
			clause: "confirm reads", at: 9 * time.Second, req: reqConfirm(holderA, "book", 3, 8000),
			reply:   "confirmed book/3 holder=m1/a term=8000 version=2",
			effects: []string{"confirm m1/a term=8000 book/3 holder=m1/a term=8000 version=2 read"},
		},
		{
			clause: "confirm of no record", at: 9 * time.Second, req: reqConfirm(holderB, "book", 9, 1),
			reply:   "confirmed book/9 holder=- term=0 version=0",
			effects: []string{"confirm m2/b term=1 book/9 holder=- term=0 version=0 read"},
		},
		{
			clause: "grant book/1", at: 9 * time.Second, req: reqAcquire(holderA, "book", 1),
			reply: "granted book/1 holder=m1/a term=9000 version=3",
			effects: []string{
				"grant book/1 holder=m1/a term=9000 version=3 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/1 holder=m1/a term=9000 version=3",
			},
		},
		{
			clause: "grant in another space", at: 9 * time.Second, req: reqAcquire(holderB, "cart", 0),
			reply: "granted cart/0 holder=m2/b term=9000 version=4",
			effects: []string{
				"grant cart/0 holder=m2/b term=9000 version=4 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record cart/0 holder=m2/b term=9000 version=4",
			},
		},
		{
			clause: "grant book/0", at: 9 * time.Second, req: reqAcquire(holderB, "book", 0),
			reply: "granted book/0 holder=m2/b term=9000 version=5",
			effects: []string{
				"grant book/0 holder=m2/b term=9000 version=5 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/0 holder=m2/b term=9000 version=5",
			},
		},
		{
			clause: "holdings sorted by shard", at: 9 * time.Second, req: reqHoldings(holderB, "book"),
			reply: "holdings book records=3 newest=5",
			check: wantRecords([]tenure.Holding{
				{Space: "book", Shard: 0, Holder: holderB, Term: 9000, Version: 5},
				{Space: "book", Shard: 1, Holder: holderA, Term: 9000, Version: 3},
				{Space: "book", Shard: 3, Holder: holderA, Term: 8000, Version: 2},
			}),
		},
		{
			clause: "holdings of a space with no record", at: 9 * time.Second, req: reqHoldings(holderB, "none"),
			reply: "holdings none records=0 newest=0",
			check: wantRecords(nil),
		},
		{
			clause: "renew m2/b", at: 10 * time.Second, req: reqKeepAlive(holderB, timingB),
			reply: "renewed version=2", effects: []string{"session renew m2/b version=2"},
		},
		{
			clause: "renew m2/b", at: 14 * time.Second, req: reqKeepAlive(holderB, timingB),
			reply: "renewed version=3", effects: []string{"session renew m2/b version=3"},
		},
		{
			clause: "renew m2/b", at: 18 * time.Second, req: reqKeepAlive(holderB, timingB),
			reply: "renewed version=4", effects: []string{"session renew m2/b version=4"},
			check: wantNextEnd(20 * time.Second),
		},
		{
			clause: "held a nanosecond before the expiry", at: 20*time.Second - 1, req: reqAcquire(holderB, "book", 3),
			reply: "held book/3 holder=m1/a term=8000 version=2 retry=3.000000000s",
		},
		{
			clause: "takeover at the expiry on the store's clock", at: 20 * time.Second, req: reqAcquire(holderB, "book", 3),
			reply: "granted book/3 holder=m2/b term=20000 version=6",
			effects: []string{
				"session end m1/a version=2 expired",
				"grant book/3 holder=m2/b term=20000 version=6 prev=m1/a ended=false unseen=15.000000000s lease=15.000000000s",
				"record book/3 holder=m2/b term=20000 version=6",
				"view version=3 members=m2 slots=0 sessions=m2/b:4",
			},
			check: wantNextEnd(33 * time.Second),
		},
		{
			clause: "an expired session is lost", at: 21 * time.Second, req: reqKeepAlive(holderA, ta),
			reply: "lost",
		},
		{
			clause: "an expired session acquires nothing", at: 21 * time.Second, req: reqAcquire(holderA, "book", 1),
			reply: "lost",
		},
		{
			clause: "open m3/c", at: 30 * time.Second, req: reqKeepAlive(holderC, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m3/c version=1",
				"view version=4 members=m2,m3 slots=0 sessions=m2/b:4,m3/c:1",
			},
		},
		{
			clause: "renew m2/b", at: 32 * time.Second, req: reqKeepAlive(holderB, timingB),
			reply: "renewed version=5", effects: []string{"session renew m2/b version=5"},
		},
		{
			clause: "a renewal at the expiry ends the session", at: 45 * time.Second, req: reqKeepAlive(holderC, ta),
			reply: "lost",
			effects: []string{
				"session end m3/c version=1 late",
				"view version=5 members=m2 slots=0 sessions=m2/b:5",
			},
		},
		{
			clause: "a newer incarnation replaces the session", at: 46 * time.Second, req: reqKeepAlive(holderX, timingB),
			reply: "renewed version=1 opened",
			effects: []string{
				"session end m2/b version=5 replaced",
				"session open m2/x version=1",
			},
			check: func(t *testing.T, m *storeModel, _ reply) {
				t.Helper()
				if v := m.currentView(); v.Version != 5 {
					t.Fatalf("view version %d after a replacement within one member, want 5", v.Version)
				}
			},
		},
		{
			clause: "the replaced session is lost", at: 46 * time.Second, req: reqKeepAlive(holderB, timingB),
			reply: "lost",
		},
		{
			clause: "grant over a replaced session", at: 46 * time.Second, req: reqAcquire(holderX, "book", 3),
			reply: "granted book/3 holder=m2/x term=46000 version=7",
			effects: []string{
				"grant book/3 holder=m2/x term=46000 version=7 prev=m2/b ended=true unseen=14.000000000s lease=15.000000000s",
				"record book/3 holder=m2/x term=46000 version=7",
			},
		},
		{
			clause: "release clears and keeps the term", at: 46 * time.Second, req: reqRelease(holderX, "book", 3, 46000),
			reply: "released book/3 holder=- term=46000 version=8 cleared",
			effects: []string{
				"release m2/x term=46000 book/3 holder=- term=46000 version=8 cleared",
				"record book/3 holder=- term=46000 version=8",
			},
			check: wantRecord(tenure.Holding{Space: "book", Shard: 3, Term: 46000, Version: 8}),
		},
		{
			clause: "a grant at the same wall time grows the term by one", at: 46 * time.Second, req: reqAcquire(holderX, "book", 3),
			reply: "granted book/3 holder=m2/x term=46001 version=9",
			effects: []string{
				"grant book/3 holder=m2/x term=46001 version=9 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/3 holder=m2/x term=46001 version=9",
			},
		},
		{
			clause: "open m4/d", at: 46 * time.Second, req: reqKeepAlive(holderD, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m4/d version=1",
				"view version=6 members=m2,m4 slots=0 sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "set slots", at: 46 * time.Second, req: reqSetSlots(holderX, 3),
			reply: "slotsSet count=3",
			effects: []string{
				"slot set m2/x slot=-1 count=3 set",
				"view version=7 members=m2,m4 slots=3:,, sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "claim the lowest free slot", at: 46 * time.Second, req: reqClaimSlot(holderX),
			reply: "slotClaimed slot=0",
			effects: []string{
				"slot claim m2/x slot=0 count=3 claimed",
				"view version=8 members=m2@0,m4 slots=3:m2/x,, sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "claim a held slot again", at: 46 * time.Second, req: reqClaimSlot(holderX),
			reply:   "slotClaimed slot=0",
			effects: []string{"slot claim m2/x slot=0 count=3 kept"},
		},
		{
			clause: "claim the next slot", at: 46 * time.Second, req: reqClaimSlot(holderD),
			reply: "slotClaimed slot=1",
			effects: []string{
				"slot claim m4/d slot=1 count=3 claimed",
				"view version=9 members=m2@0,m4@1 slots=3:m2/x,m4/d, sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "lower the count", at: 46 * time.Second, req: reqSetSlots(holderX, 1),
			reply: "slotsSet count=1",
			effects: []string{
				"slot set m2/x slot=-1 count=1 set",
				"view version=10 members=m2@0,m4 slots=1:m2/x sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "the holder of a retired slot gets none", at: 46 * time.Second, req: reqClaimSlot(holderD),
			reply:   "noSlot",
			effects: []string{"slot claim m4/d slot=-1 count=1 retired"},
		},
		{
			clause: "release a slot of another holder", at: 46 * time.Second, req: reqReleaseSlot(holderD, 0),
			reply:   "slotReleased slot=0",
			effects: []string{"slot release m4/d slot=0 count=1 mismatch"},
		},
		{
			clause: "release a slot", at: 46 * time.Second, req: reqReleaseSlot(holderX, 0),
			reply: "slotReleased slot=0 cleared",
			effects: []string{
				"slot release m2/x slot=0 count=1 cleared",
				"view version=11 members=m2,m4 slots=1: sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "a retired slot still bars a free one", at: 46 * time.Second, req: reqClaimSlot(holderD),
			reply:   "noSlot",
			effects: []string{"slot claim m4/d slot=-1 count=1 retired"},
		},
		{
			clause: "release a retired slot", at: 46 * time.Second, req: reqReleaseSlot(holderD, 1),
			reply:   "slotReleased slot=1 cleared",
			effects: []string{"slot release m4/d slot=1 count=1 cleared"},
		},
		{
			clause: "claim after the release", at: 46 * time.Second, req: reqClaimSlot(holderD),
			reply: "slotClaimed slot=0",
			effects: []string{
				"slot claim m4/d slot=0 count=1 claimed",
				"view version=12 members=m2,m4@0 slots=1:m4/d sessions=m2/x:1,m4/d:1",
			},
		},
		{
			clause: "no slot is free", at: 46 * time.Second, req: reqClaimSlot(holderX),
			reply:   "noSlot",
			effects: []string{"slot claim m2/x slot=-1 count=1 none"},
		},
		{
			clause: "grant to m4/d", at: 47 * time.Second, req: reqAcquire(holderD, "book", 5),
			reply: "granted book/5 holder=m4/d term=47000 version=10",
			effects: []string{
				"grant book/5 holder=m4/d term=47000 version=10 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/5 holder=m4/d term=47000 version=10",
			},
		},
		{
			clause: "closeSession clears records and slots", at: 47 * time.Second, req: reqCloseSession(holderD),
			reply: "closed",
			effects: []string{
				"session close m4/d version=1",
				"record book/5 holder=- term=47000 version=11",
				"slot release m4/d slot=0 count=1 closed",
				"view version=13 members=m2 slots=1: sessions=m2/x:1",
			},
		},
		{
			clause: "a closed session is lost", at: 47 * time.Second, req: reqKeepAlive(holderD, ta),
			reply: "lost",
		},
		{
			clause: "close a holder that never opened", at: 47 * time.Second, req: reqCloseSession(holderZ),
			reply: "closed", effects: []string{"session close m9/z version=0"},
		},
		{
			clause: "a closed holder never opens", at: 47 * time.Second, req: reqKeepAlive(holderZ, ta),
			reply: "lost",
			check: func(t *testing.T, m *storeModel, _ reply) {
				t.Helper()
				want := core.View{
					Version: 13, Members: []core.Member{{ID: "m2", Slot: -1}}, Slots: 1, SlotHolders: []string{""},
				}
				got := m.currentView()
				if modelViewText(got) != modelViewText(want) || len(got.Members) != 1 || got.Members[0].Slot != -1 {
					t.Fatalf("view %+v, want %+v", got, want)
				}
			},
		},
	})

	// EndAfter ends a session that long after it opened, unless its expiry comes first.
	m = newStoreModel(Server, 0, 10*time.Second)
	runSteps(t, m, []modelStep{
		{
			clause: "open", at: time.Second, req: reqKeepAlive(holderA, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=0 sessions=m1/a:1",
			},
			check: wantNextEnd(11 * time.Second),
		},
		{
			clause: "a renewal keeps the end", at: 5 * time.Second, req: reqKeepAlive(holderA, ta),
			reply: "renewed version=2", effects: []string{"session renew m1/a version=2"},
			check: wantNextEnd(11 * time.Second),
		},
		{
			clause: "an expiry before the end", at: 5 * time.Second, req: reqKeepAlive(holderE, timingShort),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m5/e version=1",
				"view version=2 members=m1,m5 slots=0 sessions=m1/a:2,m5/e:1",
			},
			check: wantNextEnd(10 * time.Second),
		},
		{
			clause: "expire at the expiry", at: 10 * time.Second,
			effects: []string{
				"session end m5/e version=1 expired",
				"view version=3 members=m1 slots=0 sessions=m1/a:2",
			},
			check: wantNextEnd(11 * time.Second),
		},
		{
			clause: "nothing ends before the end", at: 11*time.Second - 1,
			check: wantNextEnd(11 * time.Second),
		},
		{
			clause: "EndAfter ends the session", at: 11 * time.Second,
			effects: []string{
				"session end m1/a version=2 end_after",
				"view version=4 members= slots=0 sessions=",
			},
			check: wantNextEnd(-1),
		},
		{
			clause: "the ended session is lost", at: 11 * time.Second, req: reqKeepAlive(holderA, ta),
			reply: "lost",
		},
	})
}

// observerSetup opens m1/a and m2/b under the observer rule and gives book/3 to m1/a, which
// renews at 1 s for the last time before the orders: its session expires at 16 s on the
// store's clock. m2/b renews at 10 s.
func observerSetup(rule Takeover) []modelStep {
	ta := core.DefaultTiming
	grant := modelStep{
		clause: "m1/a takes book/3 with cas", at: time.Second, req: reqCas(holderA, "book", 3, 0, 1000, 0),
		reply: "granted book/3 holder=m1/a term=1000 version=1",
		effects: []string{
			"observe cas m1/a book/3 holder=- term=0 version=0 expect=0 ok",
			"grant book/3 holder=m1/a term=1000 version=1 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
			"record book/3 holder=m1/a term=1000 version=1",
		},
	}
	if rule == Server {
		grant = modelStep{
			clause: "m1/a acquires book/3", at: time.Second, req: reqAcquire(holderA, "book", 3),
			reply: "granted book/3 holder=m1/a term=1000 version=1",
			effects: []string{
				"grant book/3 holder=m1/a term=1000 version=1 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/3 holder=m1/a term=1000 version=1",
			},
		}
	}
	return []modelStep{
		{
			clause: "open m1/a", req: reqKeepAlive(holderA, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=0 sessions=m1/a:1",
			},
		},
		{
			clause: "open m2/b", req: reqKeepAlive(holderB, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m2/b version=1",
				"view version=2 members=m1,m2 slots=0 sessions=m1/a:1,m2/b:1",
			},
		},
		grant,
		{
			clause: "m1/a renews", at: time.Second, req: reqKeepAlive(holderA, ta),
			reply: "renewed version=2", effects: []string{"session renew m1/a version=2"},
		},
		{
			clause: "m2/b renews", at: 10 * time.Second, req: reqKeepAlive(holderB, ta),
			reply: "renewed version=2", effects: []string{"session renew m2/b version=2"},
		},
	}
}

// observerReads are the first two requests of m2/b's takeover at 14 s: m2/b reads the record
// and m1/a's session, unchanged since m2/b saw version 2.
func observerReads() []modelStep {
	return []modelStep{
		{
			clause: "reqGetShard(b)", at: 14 * time.Second, req: reqGetShard(holderB, "book", 3),
			reply:   "shard book/3 holder=m1/a term=1000 version=1",
			effects: []string{"observe get_shard m2/b book/3 holder=m1/a term=1000 version=1 ok"},
		},
		{
			clause: "reqGetSession(b)", at: 14 * time.Second, req: reqGetSession(holderB, holderA),
			reply:   "session m1/a version=2 lease=15.000000000s",
			effects: []string{"observe get_session m2/b m1/a version=2 lease=15.000000000s ok"},
		},
		{
			clause: "reqKeepAlive(a)", at: 15 * time.Second, req: reqKeepAlive(holderA, core.DefaultTiming),
			reply: "renewed version=3", effects: []string{"session renew m1/a version=3"},
		},
	}
}

// TestATSIM43ObserverStoreOrders runs the two orders of a degraded owner m1/a's renewal and
// confirmation around a taker m2/b's takeover under the observer rule. Its steps were written
// for this test from the design of the store model. Order 1: reqGetShard(b), reqGetSession(b),
// reqKeepAlive(a), reqConfirm(a), reqCas(b): the confirm writes, so the cas is stale and the record still
// names a. Order 2: reqGetShard(b), reqGetSession(b), reqKeepAlive(a), reqCas(b), reqConfirm(a): the cas grants,
// with core.NextTerm of the wall it carries, and a's confirm shows b. Under the server rule the
// same confirmations never change a record version.
func TestATSIM43ObserverStoreOrders(t *testing.T) {
	t.Run("order1", func(t *testing.T) {
		m := newStoreModel(Observer, 0, 0)
		steps := append(observerSetup(Observer), observerReads()...)
		steps = append(steps,
			modelStep{
				clause: "reqConfirm(a) writes", at: 15 * time.Second, req: reqConfirm(holderA, "book", 3, 1000),
				reply: "confirmed book/3 holder=m1/a term=1000 version=2",
				effects: []string{
					"confirm m1/a term=1000 book/3 holder=m1/a term=1000 version=2 wrote",
					"record book/3 holder=m1/a term=1000 version=2",
				},
			},
			modelStep{
				clause: "reqCas(b) is stale", at: 15 * time.Second, req: reqCas(holderB, "book", 3, 1, 15000, 15*time.Second),
				reply:   "stale book/3 holder=m1/a term=1000 version=2",
				effects: []string{"observe cas m2/b book/3 holder=m1/a term=1000 version=2 expect=1 stale"},
				check:   wantRecord(tenure.Holding{Space: "book", Shard: 3, Holder: holderA, Term: 1000, Version: 2}),
			},
		)
		runSteps(t, m, steps)
	})

	t.Run("order2", func(t *testing.T) {
		m := newStoreModel(Observer, 0, 0)
		steps := append(observerSetup(Observer), observerReads()...)
		steps = append(steps,
			modelStep{
				// The wall the cas carries is behind the record's term and the store's own wall
				// is far ahead: the term is the record's term plus one, from the cas's wall.
				clause: "reqCas(b) grants", at: 15 * time.Second, wall: time.UnixMilli(900_000),
				req:   reqCas(holderB, "book", 3, 1, 500, 15*time.Second),
				reply: "granted book/3 holder=m2/b term=1001 version=2",
				effects: []string{
					"observe cas m2/b book/3 holder=m1/a term=1000 version=1 expect=1 ok",
					"grant book/3 holder=m2/b term=1001 version=2 prev=m1/a ended=false unseen=15.000000000s lease=15.000000000s",
					"record book/3 holder=m2/b term=1001 version=2",
				},
			},
			modelStep{
				clause: "reqConfirm(a) shows b", at: 15 * time.Second, req: reqConfirm(holderA, "book", 3, 1000),
				reply:   "confirmed book/3 holder=m2/b term=1001 version=2",
				effects: []string{"confirm m1/a term=1000 book/3 holder=m2/b term=1001 version=2 read"},
				check:   wantRecord(tenure.Holding{Space: "book", Shard: 3, Holder: holderB, Term: 1001, Version: 2}),
			},
		)
		runSteps(t, m, steps)
	})

	t.Run("server confirm never writes", func(t *testing.T) {
		m := newStoreModel(Server, 0, 0)
		unchanged := wantRecord(tenure.Holding{Space: "book", Shard: 3, Holder: holderA, Term: 1000, Version: 1})
		steps := append(observerSetup(Server),
			modelStep{
				clause: "confirm by the holder at its term", at: 15 * time.Second, req: reqConfirm(holderA, "book", 3, 1000),
				reply:   "confirmed book/3 holder=m1/a term=1000 version=1",
				effects: []string{"confirm m1/a term=1000 book/3 holder=m1/a term=1000 version=1 read"},
				check:   unchanged,
			},
			modelStep{
				clause: "confirm again", at: 15 * time.Second, req: reqConfirm(holderA, "book", 3, 1000),
				reply:   "confirmed book/3 holder=m1/a term=1000 version=1",
				effects: []string{"confirm m1/a term=1000 book/3 holder=m1/a term=1000 version=1 read"},
				check:   unchanged,
			},
			modelStep{
				clause: "confirm at another term", at: 15 * time.Second, req: reqConfirm(holderA, "book", 3, 7),
				reply:   "confirmed book/3 holder=m1/a term=1000 version=1",
				effects: []string{"confirm m1/a term=7 book/3 holder=m1/a term=1000 version=1 read"},
				check:   unchanged,
			},
			modelStep{
				clause: "confirm by another holder", at: 15 * time.Second, req: reqConfirm(holderB, "book", 3, 1000),
				reply:   "confirmed book/3 holder=m1/a term=1000 version=1",
				effects: []string{"confirm m2/b term=1000 book/3 holder=m1/a term=1000 version=1 read"},
				check: func(t *testing.T, m *storeModel, rep reply) {
					t.Helper()
					unchanged(t, m, rep)
					if m.version != 1 {
						t.Fatalf("store version %d after confirmations, want 1", m.version)
					}
				},
			},
		)
		runSteps(t, m, steps)
	})
}

// TestStoreModelVersions checks the store-wide version of shard records: every write, under
// either rule, takes the next version, whatever its space and shard, and nothing else takes
// one: not a held or lost acquire, a release that clears nothing, a read, a stale cas, a slot
// write or a session change.
func TestStoreModelVersions(t *testing.T) {
	ta := core.DefaultTiming
	for _, rule := range []Takeover{Server, Observer} {
		t.Run(rule.String(), func(t *testing.T) {
			m := newStoreModel(rule, 2, 0)
			wall := time.UnixMilli(1000)
			var want uint64 // the version of the latest write
			do := func(req request, writes int) {
				t.Helper()
				m.handle(time.Second, wall, req)
				for _, e := range m.takeEffects() {
					if r, ok := e.(recordEffect); ok {
						want++
						if r.Record.Version != want {
							t.Fatalf("%s: record version %d, want %d", req.Describe(), r.Record.Version, want)
						}
						writes--
					}
				}
				if writes != 0 || m.version != want {
					t.Fatalf("%s: %d writes missing, store version %d, want %d", req.Describe(), writes, m.version, want)
				}
			}
			do(reqKeepAlive(holderA, ta), 0)
			do(reqKeepAlive(holderB, ta), 0)
			do(reqCas(holderA, "book", 0, 0, 1000, 0), 1)
			do(reqAcquire(holderA, "cart", 4), 1)
			do(reqAcquire(holderB, "cart", 4), 0)         // held
			do(reqAcquire(holderC, "cart", 4), 0)         // lost
			do(reqCas(holderB, "book", 0, 0, 1000, 0), 0) // stale
			do(reqRelease(holderB, "cart", 4, 1000), 0)
			do(reqRelease(holderA, "cart", 4, 999), 0)
			do(reqCas(holderB, "cart", 9, 0, 1000, 0), 1)
			do(reqGetShard(holderB, "book", 0), 0)
			do(reqGetSession(holderA, holderB), 0)
			do(reqHoldings(holderB, "book"), 0)
			do(reqClaimSlot(holderA), 0)
			do(reqCasSlot(holderB, 1, 0, 0), 0)
			do(reqKeepAlive(holderA, ta), 0)
			confirmWrites := 0
			if rule == Observer {
				confirmWrites = 1
			}
			do(reqConfirm(holderA, "book", 0, 1000), confirmWrites)
			do(reqConfirm(holderB, "book", 0, 1000), 0) // not the holder
			do(reqConfirm(holderA, "cart", 77, 1), 0)   // no record
			do(reqRelease(holderA, "cart", 4, 1000), 1)
			do(reqAcquire(holderA, "book", 1), 1)
			do(reqCloseSession(holderA), 2) // book/0 and book/1, not cart/4, which it released
			do(reqCloseSession(holderA), 0)
		})
	}
}

// TestStoreModelTerms checks that a grant's term is core.NextTerm of the record's term and the
// deciding wall: the store's for acquire, the request's for cas. A wall behind the record's
// term, or before 1970, gives the record's term plus one, and a release keeps the term the next
// grant grows from.
func TestStoreModelTerms(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Server, 0, 0)
	grant := func(at time.Duration, wall time.Time, req request) core.Term {
		t.Helper()
		rep, ok := m.handle(at, wall, req).(grantedReply)
		m.takeEffects()
		if !ok {
			t.Fatalf("%s: no grant", req.Describe())
		}
		return rep.Record.Term
	}
	m.handle(0, time.UnixMilli(0), reqKeepAlive(holderA, ta))
	cases := []struct {
		name string
		wall time.Time
		req  request
		want core.Term
	}{
		{"wall before 1970 on the first grant", time.Unix(-5, 0), reqAcquire(holderA, "book", 0), 1},
		{"zero wall", time.Time{}, reqAcquire(holderA, "book", 0), 2},
		{"wall ahead", time.UnixMilli(5000), reqAcquire(holderA, "book", 0), 5000},
		{"wall stepped back", time.UnixMilli(1000), reqAcquire(holderA, "book", 0), 5001},
		{"wall equal to the term", time.UnixMilli(5001), reqAcquire(holderA, "book", 0), 5002},
		{"cas uses its own wall, not the store's", time.UnixMilli(90_000), reqCas(holderA, "book", 0, 5, 7000, 0), 7000},
		{"cas with its wall behind", time.UnixMilli(90_000), reqCas(holderA, "book", 0, 6, 10, 0), 7001},
	}
	for _, c := range cases {
		if got := grant(time.Second, c.wall, c.req); got != c.want {
			t.Errorf("%s: term %d, want %d", c.name, got, c.want)
		}
	}
	m.handle(time.Second, time.UnixMilli(0), reqRelease(holderA, "book", 0, 7001))
	if got := grant(time.Second, time.UnixMilli(0), reqAcquire(holderA, "book", 0)); got != 7002 {
		t.Errorf("grant after a release: term %d, want 7002", got)
	}
}

// TestStoreModelStoreTime checks that the store's time is the largest reading it was given: a
// smaller reading neither moves a renewal back nor revives a session, and requests that look at
// no session read no time.
func TestStoreModelStoreTime(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Server, 0, 0)
	wall := time.UnixMilli(0)
	m.handle(10*time.Second, wall, reqKeepAlive(holderA, ta))
	m.handle(20*time.Second, wall, reqKeepAlive(holderA, ta))
	m.handle(15*time.Second, wall, reqKeepAlive(holderA, ta)) // counts as 20 s
	if at, ok := m.nextEnd(); !ok || at != 35*time.Second {
		t.Fatalf("nextEnd = %v, %v, want 35s", at, ok)
	}
	// A release, holdings, a slot request and setSlots read no time: a reading of 99 s there
	// ends nothing.
	m.handle(99*time.Second, wall, reqRelease(holderA, "book", 0, 1))
	m.handle(99*time.Second, wall, reqHoldings(holderA, "book"))
	m.handle(99*time.Second, wall, reqReleaseSlot(holderA, 0))
	m.handle(99*time.Second, wall, reqSetSlots(holderA, 0))
	m.handle(99*time.Second, wall, reqCloseSession(holderZ))
	m.takeEffects()
	if rep := m.handle(34*time.Second, wall, reqKeepAlive(holderA, ta)); rep.Describe() != "renewed#0 version=4" {
		t.Fatalf("renewal at 34 s: %s", rep.Describe())
	}
	m.handle(60*time.Second, wall, reqKeepAlive(holderB, ta)) // ends m1/a
	if got := effectTexts(m.takeEffects()); !slices.Contains(got, "session end m1/a version=4 expired") {
		t.Fatalf("effects %q, want m1/a expired", got)
	}
	if rep := m.handle(40*time.Second, wall, reqKeepAlive(holderA, ta)); rep.Describe() != "lost#0" {
		t.Fatalf("renewal at an earlier reading: %s, want lost#0", rep.Describe())
	}
}

// TestStoreModelObserverClock checks that under the observer rule the store's clock judges only
// the sender's own session: another session stays in the view and holds its shards however long
// it went unrenewed, while the sender's own expired session ends as late at its request.
func TestStoreModelObserverClock(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Observer, 1, 0)
	runSteps(t, m, []modelStep{
		{
			clause: "open m1/a", req: reqKeepAlive(holderA, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=1: sessions=m1/a:1",
			},
			check: wantNextEnd(-1),
		},
		{
			clause: "m1/a takes book/0", req: reqCas(holderA, "book", 0, 0, 0, 0),
			reply: "granted book/0 holder=m1/a term=1 version=1",
			effects: []string{
				"observe cas m1/a book/0 holder=- term=0 version=0 expect=0 ok",
				"grant book/0 holder=m1/a term=1 version=1 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/0 holder=m1/a term=1 version=1",
			},
		},
		{
			clause: "m1/a takes slot 0", req: reqCasSlot(holderA, 0, 0, 0),
			reply: "slotClaimed slot=0",
			effects: []string{
				"observe cas m1/a slot=0 holder=- version=0 count=1 expect=0 ok",
				"slot claim m1/a slot=0 count=1 claimed",
				"view version=2 members=m1@0 slots=1:m1/a sessions=m1/a:1",
			},
		},
		{clause: "expire ends nothing", at: 100 * time.Second},
		{
			clause: "open m2/b", at: 100 * time.Second, req: reqKeepAlive(holderB, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m2/b version=1",
				"view version=3 members=m1@0,m2 slots=1:m1/a sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "acquire takes only an ended session's shard", at: 100 * time.Second, req: reqAcquire(holderB, "book", 0),
			reply: "held book/0 holder=m1/a term=1 version=1 retry=2.000000000s",
		},
		{
			clause: "claimSlot takes only an ended session's slot", at: 100 * time.Second, req: reqClaimSlot(holderB),
			reply: "noSlot", effects: []string{"slot claim m2/b slot=-1 count=1 none"},
		},
		{
			clause: "getSession tells what the store has", at: 100 * time.Second, req: reqGetSession(holderB, holderA),
			reply:   "session m1/a version=1 lease=15.000000000s",
			effects: []string{"observe get_session m2/b m1/a version=1 lease=15.000000000s ok"},
		},
		{
			clause: "getSession of an unknown holder", at: 100 * time.Second, req: reqGetSession(holderB, holderZ),
			reply:   "session m9/z version=0 lease=0.000000000s ended",
			effects: []string{"observe get_session m2/b m9/z version=0 lease=0.000000000s ended ok"},
		},
		{
			clause: "the sender's own expired session ends as late", at: 100 * time.Second, req: reqConfirm(holderA, "book", 0, 1),
			reply: "lost",
			effects: []string{
				"session end m1/a version=1 late",
				"view version=4 members=m2 slots=1: sessions=m2/b:1",
			},
		},
		{
			clause: "getSession shows the end", at: 100 * time.Second, req: reqGetSession(holderB, holderA),
			reply:   "session m1/a version=1 lease=15.000000000s ended",
			effects: []string{"observe get_session m2/b m1/a version=1 lease=15.000000000s ended ok"},
		},
		{
			clause: "acquire takes the ended session's shard", at: 100 * time.Second, req: reqAcquire(holderB, "book", 0),
			reply: "granted book/0 holder=m2/b term=100000 version=2",
			effects: []string{
				"grant book/0 holder=m2/b term=100000 version=2 prev=m1/a ended=true unseen=100.000000000s lease=15.000000000s",
				"record book/0 holder=m2/b term=100000 version=2",
			},
		},
		{
			clause: "a cas of a sender with no session", at: 100 * time.Second, req: reqCas(holderA, "book", 0, 2, 0, 0),
			reply:   "lost",
			effects: []string{"observe cas m1/a book/0 holder=m2/b term=100000 version=2 expect=2 lost"},
		},
	})
}

// TestStoreModelObserverSlots checks getSlot and casSlot: a casSlot succeeds only on the
// version it read and for a slot below the count, and slot versions are counted apart from
// record versions.
func TestStoreModelObserverSlots(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Observer, 2, 0)
	runSteps(t, m, []modelStep{
		{
			clause: "open m1/a", req: reqKeepAlive(holderA, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=2:, sessions=m1/a:1",
			},
		},
		{
			clause: "open m2/b", req: reqKeepAlive(holderB, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m2/b version=1",
				"view version=2 members=m1,m2 slots=2:, sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "getSlot of no record", req: reqGetSlot(holderA, 1),
			reply:   "slot slot=1 holder=- version=0 count=2",
			effects: []string{"observe get_slot m1/a slot=1 holder=- version=0 count=2 ok"},
		},
		{
			clause: "casSlot", req: reqCasSlot(holderA, 1, 0, 0),
			reply: "slotClaimed slot=1",
			effects: []string{
				"observe cas m1/a slot=1 holder=- version=0 count=2 expect=0 ok",
				"slot claim m1/a slot=1 count=2 claimed",
				"view version=3 members=m1@1,m2 slots=2:,m1/a sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "a casSlot on an old version is stale", req: reqCasSlot(holderB, 1, 0, 20*time.Second),
			reply:   "stale slot=1 holder=m1/a version=1",
			effects: []string{"observe cas m2/b slot=1 holder=m1/a version=1 count=2 expect=0 stale"},
		},
		{
			clause: "release", req: reqReleaseSlot(holderA, 1),
			reply: "slotReleased slot=1 cleared",
			effects: []string{
				"slot release m1/a slot=1 count=2 cleared",
				"view version=4 members=m1,m2 slots=2:, sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "getSlot of a released slot", req: reqGetSlot(holderB, 1),
			reply:   "slot slot=1 holder=- version=2 count=2",
			effects: []string{"observe get_slot m2/b slot=1 holder=- version=2 count=2 ok"},
		},
		{
			clause: "lower the count", req: reqSetSlots(holderA, 1),
			reply: "slotsSet count=1",
			effects: []string{
				"slot set m1/a slot=-1 count=1 set",
				"view version=5 members=m1,m2 slots=1: sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "a casSlot above the count is stale", req: reqCasSlot(holderB, 1, 2, 0),
			reply:   "stale slot=1 holder=- version=2",
			effects: []string{"observe cas m2/b slot=1 holder=- version=2 count=1 expect=2 stale"},
			check: func(t *testing.T, m *storeModel, _ reply) {
				t.Helper()
				if m.version != 0 {
					t.Fatalf("record version %d after slot writes only, want 0", m.version)
				}
			},
		},
	})
}

// TestStoreModelSlotExpiry checks that under the server rule a slot whose holder's session
// expired drops out of the view at the expiry and goes to the next claimer.
func TestStoreModelSlotExpiry(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Server, 1, 0)
	runSteps(t, m, []modelStep{
		{
			clause: "open m1/a", req: reqKeepAlive(holderA, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=1: sessions=m1/a:1",
			},
		},
		{
			clause: "claim", req: reqClaimSlot(holderA),
			reply: "slotClaimed slot=0",
			effects: []string{
				"slot claim m1/a slot=0 count=1 claimed",
				"view version=2 members=m1@0 slots=1:m1/a sessions=m1/a:1",
			},
		},
		{
			clause: "open m2/b", at: 10 * time.Second, req: reqKeepAlive(holderB, ta),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m2/b version=1",
				"view version=3 members=m1@0,m2 slots=1:m1/a sessions=m1/a:1,m2/b:1",
			},
		},
		{
			clause: "held slot", at: 10 * time.Second, req: reqClaimSlot(holderB),
			reply: "noSlot", effects: []string{"slot claim m2/b slot=-1 count=1 none"},
		},
		{
			clause: "expiry", at: 15 * time.Second,
			effects: []string{
				"session end m1/a version=1 expired",
				"view version=4 members=m2 slots=1: sessions=m2/b:1",
			},
		},
		{
			clause: "claim the expired holder's slot", at: 15 * time.Second, req: reqClaimSlot(holderB),
			reply: "slotClaimed slot=0",
			effects: []string{
				"slot claim m2/b slot=0 count=1 claimed",
				"view version=5 members=m2@0 slots=1:m2/b sessions=m2/b:1",
			},
		},
	})
}

// TestStoreModelObserverEndAfter checks that under the observer rule only EndAfter sets the
// node's timer.
func TestStoreModelObserverEndAfter(t *testing.T) {
	m := newStoreModel(Observer, 0, 30*time.Second)
	runSteps(t, m, []modelStep{
		{
			clause: "open", at: time.Second, req: reqKeepAlive(holderA, timingShort),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=0 sessions=m1/a:1",
			},
			check: wantNextEnd(31 * time.Second),
		},
		{clause: "no expiry by the clock", at: 20 * time.Second},
		{
			clause: "EndAfter", at: 31 * time.Second,
			effects: []string{
				"session end m1/a version=1 end_after",
				"view version=2 members= slots=0 sessions=",
			},
			check: wantNextEnd(-1),
		},
	})
}

// TestStoreModelSnapshot checks the snapshot and the view's slices: records sorted by space and
// shard, sessions that have not ended sorted by holder, and a view the caller may change.
func TestStoreModelSnapshot(t *testing.T) {
	ta := core.DefaultTiming
	m := newStoreModel(Server, 2, 0)
	wall := time.UnixMilli(1000)
	for _, req := range []request{
		reqKeepAlive(holderB, ta), reqKeepAlive(holderA, ta), reqKeepAlive(holderC, ta),
		reqAcquire(holderB, "cart", 2), reqAcquire(holderA, "book", 7), reqAcquire(holderA, "book", 1),
		reqClaimSlot(holderB), reqCloseSession(holderC),
	} {
		m.handle(time.Second, wall, req)
	}
	snap := m.snapshot()
	if got, want := snap.Describe(), "snapshot records=3 newest=3 view version=5 members=m1,m2 slots=2 sessions=2"; got != want {
		t.Fatalf("snapshot %q, want %q", got, want)
	}
	var recs []string
	for _, r := range snap.Records {
		recs = append(recs, msgRecord(r))
	}
	wantRecs := []string{
		"book/1 holder=m1/a term=1000 version=3",
		"book/7 holder=m1/a term=1000 version=2",
		"cart/2 holder=m2/b term=1000 version=1",
	}
	if !slices.Equal(recs, wantRecs) {
		t.Fatalf("records %q, want %q", recs, wantRecs)
	}
	if len(snap.Sessions) != 2 || snap.Sessions[0].Holder != holderA || snap.Sessions[1].Holder != holderB {
		t.Fatalf("sessions %+v, want m1/a and m2/b", snap.Sessions)
	}
	if got := modelViewText(snap.View); got != "view version=5 members=m1,m2@0 slots=2:m2/b," {
		t.Fatalf("view %q", got)
	}
	snap.View.Members[0].ID = "changed"
	snap.View.SlotHolders[0] = "changed"
	if got := modelViewText(m.currentView()); got != "view version=5 members=m1,m2@0 slots=2:m2/b," {
		t.Fatalf("view after the caller changed its copy %q", got)
	}
	if err := m.currentView().Validate(); err != nil {
		t.Fatal(err)
	}
}

// TestStoreModelViewOrder checks that the members of a view are sorted by member ID, which
// is not the order of their holders: "m1-r/…" sorts before "m1/…" as a holder and after "m1"
// as an ID.
func TestStoreModelViewOrder(t *testing.T) {
	m := newStoreModel(Server, 0, 0)
	m.handle(0, time.UnixMilli(0), reqKeepAlive("m1/z", core.DefaultTiming))
	m.handle(0, time.UnixMilli(0), reqKeepAlive("m1-r/a", core.DefaultTiming))
	v := m.currentView()
	if got := modelViewText(v); got != "view version=2 members=m1,m1-r slots=0" {
		t.Fatalf("view %q", got)
	}
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := m.sessionList(); got[0].Holder != "m1-r/a" || got[1].Holder != "m1/z" {
		t.Fatalf("sessions %+v, want them sorted by holder", got)
	}
}

// bogusReq is a request the model does not know.
type bogusReq struct{ reqHead }

func (bogusReq) Describe() string { return "bogus" }

// TestStoreModelUnknownRequest checks that a request of a type the model does not know panics.
func TestStoreModelUnknownRequest(t *testing.T) {
	defer func() {
		if r := recover(); r != "world: store model: unknown request world.bogusReq" {
			t.Fatalf("recover() = %v", r)
		}
	}()
	newStoreModel(Server, 0, 0).handle(0, time.Time{}, bogusReq{})
}

// TestStoreModelObserverConfirmTerm checks that under the observer rule a confirm writes only
// at the term the record holds: the holder's confirm of an earlier term, after it took the
// shard again, reads.
func TestStoreModelObserverConfirmTerm(t *testing.T) {
	m := newStoreModel(Observer, 0, 0)
	runSteps(t, m, []modelStep{
		{
			clause: "open m1/a", req: reqKeepAlive(holderA, core.DefaultTiming),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=0 sessions=m1/a:1",
			},
		},
		{
			clause: "take book/0", req: reqCas(holderA, "book", 0, 0, 1000, 0),
			reply: "granted book/0 holder=m1/a term=1000 version=1",
			effects: []string{
				"observe cas m1/a book/0 holder=- term=0 version=0 expect=0 ok",
				"grant book/0 holder=m1/a term=1000 version=1 prev=- ended=false unseen=0.000000000s lease=0.000000000s",
				"record book/0 holder=m1/a term=1000 version=1",
			},
		},
		{
			clause: "take it again", req: reqCas(holderA, "book", 0, 1, 2000, 0),
			reply: "granted book/0 holder=m1/a term=2000 version=2",
			effects: []string{
				"observe cas m1/a book/0 holder=m1/a term=1000 version=1 expect=1 ok",
				"grant book/0 holder=m1/a term=2000 version=2 prev=m1/a ended=false unseen=0.000000000s lease=15.000000000s",
				"record book/0 holder=m1/a term=2000 version=2",
			},
		},
		{
			clause: "a confirm of the earlier term reads", req: reqConfirm(holderA, "book", 0, 1000),
			reply:   "confirmed book/0 holder=m1/a term=2000 version=2",
			effects: []string{"confirm m1/a term=1000 book/0 holder=m1/a term=2000 version=2 read"},
		},
		{
			clause: "a confirm of the record's term writes", req: reqConfirm(holderA, "book", 0, 2000),
			reply: "confirmed book/0 holder=m1/a term=2000 version=3",
			effects: []string{
				"confirm m1/a term=2000 book/0 holder=m1/a term=2000 version=3 wrote",
				"record book/0 holder=m1/a term=2000 version=3",
			},
		},
	})
}

// TestStoreModelEndTie checks that a session whose expiry and EndAfter end fall on one reading
// ends as expired under the server rule, whose clock judges it, and as end_after under the
// observer rule, whose clock does not.
func TestStoreModelEndTie(t *testing.T) {
	for _, c := range []struct {
		rule  Takeover
		cause string
	}{{Server, "expired"}, {Observer, "end_after"}} {
		t.Run(c.rule.String(), func(t *testing.T) {
			m := newStoreModel(c.rule, 0, 15*time.Second)
			runSteps(t, m, []modelStep{
				{
					clause: "open", req: reqKeepAlive(holderA, core.DefaultTiming),
					reply: "renewed version=1 opened",
					effects: []string{
						"session open m1/a version=1",
						"view version=1 members=m1 slots=0 sessions=m1/a:1",
					},
					check: wantNextEnd(15 * time.Second),
				},
				{
					clause: "the end", at: 15 * time.Second,
					effects: []string{
						"session end m1/a version=1 " + c.cause,
						"view version=2 members= slots=0 sessions=",
					},
					check: wantNextEnd(-1),
				},
			})
		})
	}
}

// TestStoreModelObserverTwoSlots checks the view of a holder with two slots, which casSlot
// allows: the member's Slot is the lowest, and releasing the other slot changes the slot
// holders only, which is a new view all the same.
func TestStoreModelObserverTwoSlots(t *testing.T) {
	m := newStoreModel(Observer, 2, 0)
	runSteps(t, m, []modelStep{
		{
			clause: "open m1/a", req: reqKeepAlive(holderA, core.DefaultTiming),
			reply: "renewed version=1 opened",
			effects: []string{
				"session open m1/a version=1",
				"view version=1 members=m1 slots=2:, sessions=m1/a:1",
			},
		},
		{
			clause: "take slot 1", req: reqCasSlot(holderA, 1, 0, 0),
			reply: "slotClaimed slot=1",
			effects: []string{
				"observe cas m1/a slot=1 holder=- version=0 count=2 expect=0 ok",
				"slot claim m1/a slot=1 count=2 claimed",
				"view version=2 members=m1@1 slots=2:,m1/a sessions=m1/a:1",
			},
		},
		{
			clause: "take slot 0 too", req: reqCasSlot(holderA, 0, 0, 0),
			reply: "slotClaimed slot=0",
			effects: []string{
				"observe cas m1/a slot=0 holder=- version=0 count=2 expect=0 ok",
				"slot claim m1/a slot=0 count=2 claimed",
				"view version=3 members=m1@0 slots=2:m1/a,m1/a sessions=m1/a:1",
			},
		},
		{
			clause: "release slot 1", req: reqReleaseSlot(holderA, 1),
			reply: "slotReleased slot=1 cleared",
			effects: []string{
				"slot release m1/a slot=1 count=2 cleared",
				"view version=4 members=m1@0 slots=2:m1/a, sessions=m1/a:1",
			},
		},
	})
}
