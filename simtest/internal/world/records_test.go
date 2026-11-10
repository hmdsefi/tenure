// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// TestRecords checks every emit helper: the kind, the node, the text and the attributes in
// their order.
func TestRecords(t *testing.T) {
	runWorld(t, fullTrace(time.Minute), Scenario{Members: 2}, func(fw *faultline.World, w *World) {
		t := fw.T()
		fw.RunFor(time.Second)
		m := w.Members()[1]
		m.wall = 3 * time.Second
		rec := tenure.Holding{Space: "book", Shard: 3, Holder: "m2/ab", Term: 7, Version: 12}
		view := core.View{Version: 4, Members: []core.Member{{ID: "m1"}, {ID: "m2"}}}
		slotted := core.View{Version: 5, Members: []core.Member{{ID: "m1"}}, Slots: 3, SlotHolders: []string{"m1/aa", "", "m2/bb"}}
		before := len(fw.Sim.Records())

		w.emitBoot(m, "activate")
		w.emitWallStep(m, -time.Second)
		w.emitSession("s", "open", "m1/aa", 1, "")
		w.emitSession("", "end", "m1/aa", 2, "expired")
		w.emitGrant("g", check.Grant{Space: "book", Shard: 3, Term: 7, Holder: "m2/ab", Prev: "m1/aa", Ended: true,
			Unseen: 15 * time.Second, Lease: 15 * time.Second}, 12)
		w.emitRelease("r", "m2/ab", 7, rec, "mismatch")
		w.emitConfirm("c", "m1/aa", 6, rec)
		w.emitObserve("o", "get_session", "m2/ab", []kernel.Attr{recAttr("session", "m1/aa"), recAttr("version", "3")}, "ok")
		w.emitSlot("", "claim", "m1/aa", 2, 3, "claimed")
		w.emitView("v", view)
		w.emitView("", slotted)
		w.emitFence("f", "book", 3, 7, 8)
		w.emitWrite("w", check.Write{Space: "book", Shard: 3, Term: 7, Holder: "m2/ab"}, "rejected")
		w.emitInput(m, "i", "HolderChanged", recAttr("space", "book"), recAttr("src", "push"))
		w.emitAction(m, "", "KeepAlive")
		w.emitReplyLate(m, "late", 41, "renewed")
		w.emitRun(m, "", "stopped", "book", 3, 7)

		store, fence, mn := w.Store().ID(), w.Fence().ID(), m.Node.ID()
		want := []struct {
			kind  string
			node  kernel.NodeID
			text  string
			attrs []string
		}{
			{kindBoot, mn, "", []string{"holder=" + m.Holder(), "cause=activate"}},
			{kindWallStep, mn, "", []string{"delta_ns=-1000000000", "offset_ns=3000000000"}},
			{kindSession, store, "s", []string{"op=open", "holder=m1/aa", "version=1"}},
			{kindSession, store, "", []string{"op=end", "holder=m1/aa", "version=2", "cause=expired"}},
			{kindGrant, store, "g", []string{"space=book", "shard=3", "holder=m2/ab", "term=7", "prev=m1/aa", "version=12",
				"ended=true", "unseen_ns=15000000000", "lease_ns=15000000000"}},
			{kindRelease, store, "r", []string{"space=book", "shard=3", "holder=m2/ab", "term=7", "version=12", "result=mismatch"}},
			{kindConfirm, store, "c", []string{"space=book", "shard=3", "holder=m1/aa", "term=6", "version=12",
				"record_holder=m2/ab", "record_term=7"}},
			{kindObserve, store, "o", []string{"op=get_session", "holder=m2/ab", "session=m1/aa", "version=3", "result=ok"}},
			{kindSlot, store, "", []string{"op=claim", "holder=m1/aa", "slot=2", "count=3", "result=claimed"}},
			{kindView, store, "v", []string{"version=4", "members=m1,m2", "slots=0"}},
			{kindView, store, "", []string{"version=5", "members=m1", "slots=3:m1/aa,,m2/bb"}},
			{kindFence, fence, "f", []string{"space=book", "shard=3", "term=7", "raised=8"}},
			{kindWrite, fence, "w", []string{"space=book", "shard=3", "term=7", "holder=m2/ab", "result=rejected"}},
			{kindInput, mn, "i", []string{"type=HolderChanged", "space=book", "src=push"}},
			{kindAction, mn, "", []string{"type=KeepAlive"}},
			{kindReplyLate, mn, "late", []string{"req=41", "type=renewed"}},
			{kindRun, mn, "", []string{"op=stopped", "space=book", "shard=3", "term=7"}},
		}
		got := fw.Sim.Records()[before:]
		if len(got) != len(want) {
			t.Fatalf("%d records, want %d: %v", len(got), len(want), got)
		}
		for i, r := range got {
			wr := want[i]
			if r.Kind != wr.kind || r.Node != wr.node || r.Text != wr.text || !slices.Equal(attrs(r), wr.attrs) {
				t.Errorf("record %d: %s at node %d %q %v, want %s at node %d %q %v",
					i, r.Kind, r.Node, r.Text, attrs(r), wr.kind, wr.node, wr.text, wr.attrs)
			}
		}
		if got[0].Inc != m.Node.Incarnation() {
			t.Errorf("a member's record has incarnation %d, want %d", got[0].Inc, m.Node.Incarnation())
		}
	})
}
