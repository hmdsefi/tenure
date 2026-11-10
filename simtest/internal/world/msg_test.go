// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// TestMsgDescribe checks the text of every message, and that each request and reply carries
// its request's ID.
func TestMsgDescribe(t *testing.T) {
	head := reqHead{ID: 7, Holder: "m1/0a"}
	rec := tenure.Holding{Space: "book", Shard: 3, Holder: "m1/0a", Term: 5, Version: 9}
	none := tenure.Holding{Space: "book", Shard: 4}
	slot := slotRecord{Slot: 2, Holder: "m1/0a", Version: 4}
	session := sessionInfo{Holder: "m1/0a", Version: 3, Lease: 15 * time.Second}
	ended := sessionInfo{Holder: "m2/0b", Version: 1, Lease: 15 * time.Second, Ended: true}
	view := core.View{
		Version: 4, Members: []core.Member{{ID: "m1", Slot: 0}, {ID: "m2", Slot: -1}},
		Slots: 2, SlotHolders: []string{"m1/0a", ""},
	}

	requests := []struct {
		req  request
		want string
	}{
		{keepAliveReq{head, core.DefaultTiming}, "keepAlive#7 m1/0a lease=15.000000000s retry=2.000000000s"},
		{acquireReq{head, "book", 3}, "acquire#7 m1/0a book/3"},
		{releaseReq{head, "book", 3, 5}, "release#7 m1/0a book/3 term=5"},
		{confirmReq{head, "book", 3, 5}, "confirm#7 m1/0a book/3 term=5"},
		{closeSessionReq{head}, "closeSession#7 m1/0a"},
		{holdingsReq{head, "book"}, "holdings#7 m1/0a book"},
		{claimSlotReq{head}, "claimSlot#7 m1/0a"},
		{releaseSlotReq{head, 2}, "releaseSlot#7 m1/0a slot=2"},
		{setSlotsReq{head, 3}, "setSlots#7 m1/0a count=3"},
		{getShardReq{head, "book", 3}, "getShard#7 m1/0a book/3"},
		{getSlotReq{head, 2}, "getSlot#7 m1/0a slot=2"},
		{getSessionReq{head, "m2/0b"}, "getSession#7 m1/0a of=m2/0b"},
		{
			casReq{head, "book", 3, 9, kernel.Epoch.Add(41207 * time.Millisecond), 15 * time.Second},
			"cas#7 m1/0a book/3 expect=9 wall=41.207000000s unseen=15.000000000s",
		},
		{
			casReq{head, "book", 3, 0, kernel.Epoch.Add(-time.Second), 0},
			"cas#7 m1/0a book/3 expect=0 wall=-1.000000000s unseen=0.000000000s",
		},
		{casSlotReq{head, 2, 4, 15 * time.Second}, "casSlot#7 m1/0a slot=2 expect=4 unseen=15.000000000s"},
		{getSessionReq{reqHead{ID: 1}, ""}, "getSession#1 - of=-"},
	}
	for _, c := range requests {
		if got := c.req.Describe(); got != c.want {
			t.Errorf("%T: %q, want %q", c.req, got, c.want)
		}
		if got := kernel.Describe(c.req); got != c.want {
			t.Errorf("%T: kernel.Describe %q, want %q", c.req, got, c.want)
		}
		if c.req.head().ID == 0 {
			t.Errorf("%T: no ID", c.req)
		}
	}

	rh := replyHead{ID: 7}
	replies := []struct {
		rep  reply
		want string
	}{
		{renewedReply{rh, 3, false}, "renewed#7 version=3"},
		{renewedReply{rh, 1, true}, "renewed#7 version=1 opened"},
		{lostReply{rh}, "lost#7"},
		{closedReply{rh}, "closed#7"},
		{grantedReply{rh, rec}, "granted#7 book/3 holder=m1/0a term=5 version=9"},
		{heldReply{rh, rec, 2 * time.Second}, "held#7 book/3 holder=m1/0a term=5 version=9 retry=2.000000000s"},
		{releasedReply{rh, none, false}, "released#7 book/4 holder=- term=0 version=0"},
		{releasedReply{rh, rec, true}, "released#7 book/3 holder=m1/0a term=5 version=9 cleared"},
		{confirmedReply{rh, rec}, "confirmed#7 book/3 holder=m1/0a term=5 version=9"},
		{holdingsReply{rh, "book", []tenure.Holding{none, rec}}, "holdings#7 book records=2 newest=9"},
		{holdingsReply{rh, "cart", nil}, "holdings#7 cart records=0 newest=0"},
		{slotClaimedReply{replyHead: rh, Slot: 2}, "slotClaimed#7 slot=2"},
		{noSlotReply{rh}, "noSlot#7"},
		{slotReleasedReply{replyHead: rh, Slot: 2}, "slotReleased#7 slot=2"},
		{slotReleasedReply{replyHead: rh, Slot: 2, Cleared: true}, "slotReleased#7 slot=2 cleared"},
		{slotsSetReply{rh, 3}, "slotsSet#7 count=3"},
		{shardReply{rh, rec}, "shard#7 book/3 holder=m1/0a term=5 version=9"},
		{slotReply{rh, slot, 5}, "slot#7 slot=2 holder=m1/0a version=4 count=5"},
		{slotReply{rh, slotRecord{Slot: 6}, 5}, "slot#7 slot=6 holder=- version=0 count=5"},
		{sessionReply{rh, session}, "session#7 m1/0a version=3 lease=15.000000000s"},
		{sessionReply{rh, ended}, "session#7 m2/0b version=1 lease=15.000000000s ended"},
		{staleReply{replyHead: rh, Record: rec}, "stale#7 book/3 holder=m1/0a term=5 version=9"},
		{staleReply{replyHead: rh, Slot: slot, OfSlot: true}, "stale#7 slot=2 holder=m1/0a version=4"},
	}
	for _, c := range replies {
		if got := c.rep.Describe(); got != c.want {
			t.Errorf("%T: %q, want %q", c.rep, got, c.want)
		}
		if c.rep.requestID() != 7 {
			t.Errorf("%T: request ID %d, want 7", c.rep, c.rep.requestID())
		}
	}

	pushes := []struct {
		msg  kernel.Describer
		want string
	}{
		{holderPush{rec}, "holder book/3 holder=m1/0a term=5 version=9"},
		{viewPush{view, []sessionInfo{session}}, "view version=4 members=m1,m2 slots=2 sessions=1"},
		{viewPush{core.View{}, nil}, "view version=0 members= slots=0 sessions=0"},
		{
			snapshotPush{[]tenure.Holding{rec, none}, view, []sessionInfo{session, ended}},
			"snapshot records=2 newest=9 view version=4 members=m1,m2 slots=2 sessions=2",
		},
	}
	for _, c := range pushes {
		if got := c.msg.Describe(); got != c.want {
			t.Errorf("%T: %q, want %q", c.msg, got, c.want)
		}
	}
}
