// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// TestATSIM20Fence checks the fence (SIM-400) as AT-SIM-20. The steps were written for this
// task from the issue and the design note, since the original text of the test is lost:
//
//  1. raise keeps the largest term raised: a raise below it changes nothing;
//  2. a write below the raised term is rejected, one at it is accepted, one above it too, and a
//     write does not raise the fence;
//  3. a shard that was never raised, as a shard of an unfenced space is, accepts a write in
//     any term;
//  4. every shard's fence is its own, also across spaces;
//  5. the log holds the accepted writes in order and no rejected one;
//  6. on the network, the fence answers every request in the event that delivered it, emits
//     tenure.fence and tenure.write with the request as text, calls onAccepted for each
//     accepted write, keeps its state through a crash and answers nothing while it is down.
func TestATSIM20Fence(t *testing.T) {
	t.Run("model", func(t *testing.T) {
		f := newFenceModel()
		w := func(space string, shard int, term core.Term, holder string) check.Write {
			return check.Write{Space: space, Shard: shard, Term: term, Holder: holder}
		}
		raises := []struct {
			space string
			shard int
			term  core.Term
			want  core.Term
		}{
			{"book", 1, 5, 5},
			{"book", 1, 3, 5}, // 1: below the raised term
			{"book", 1, 5, 5},
			{"book", 1, 7, 7},
			{"cart", 1, 2, 2}, // 4: another space, the same shard number
		}
		for _, r := range raises {
			if got := f.raise(r.space, r.shard, r.term); got != r.want {
				t.Errorf("raise(%s/%d, %d) = %d, want %d", r.space, r.shard, r.term, got, r.want)
			}
		}
		writes := []struct {
			wr   check.Write
			want bool
		}{
			{w("book", 1, 6, "m1/a"), false}, // 2: below
			{w("book", 1, 7, "m2/b"), true},  // 2: at
			{w("book", 1, 9, "m3/c"), true},  // 2: above
			{w("book", 1, 8, "m1/a"), true},  // 2: the write in 9 raised nothing
			{w("book", 2, 0, "m1/a"), true},  // 3: never raised
			{w("book", 2, 1, "m1/a"), true},
			{w("cart", 1, 1, "m2/b"), false}, // 4: cart/1 is at 2, book/1's 7 does not apply
			{w("cart", 1, 2, "m2/b"), true},
			{w("cart", 3, 4, "m2/b"), true}, // 4: cart/3 was never raised
		}
		var log []check.Write
		for _, tt := range writes {
			if got := f.write(tt.wr); got != tt.want {
				t.Errorf("write(%+v) = %v, want %v", tt.wr, got, tt.want)
			}
			if tt.want {
				log = append(log, tt.wr)
			}
		}
		if got := f.raisedOf("book", 1); got != 7 {
			t.Errorf("raisedOf(book/1) = %d after writes above it, want 7", got)
		}
		if got := f.raisedOf("book", 2); got != 0 {
			t.Errorf("raisedOf(book/2) = %d, want 0", got)
		}
		got := f.accepted()
		if !slices.Equal(got, log) { // 5
			t.Errorf("accepted() = %v, want %v", got, log)
		}
		got[0].Term = 99
		if f.accepted()[0].Term == 99 {
			t.Error("accepted() shares its slice with the fence")
		}
	})

	t.Run("node", func(t *testing.T) {
		opts := fullTrace(30 * time.Second)
		runWorld(t, opts, Scenario{Members: 2}, func(fw *faultline.World, w *World) {
			t := fw.T()
			s := scripted(fw, w)
			var hooked []check.Write
			w.fenceSrv.onAccepted = func(wr check.Write) { hooked = append(hooked, wr) }
			h := func(i int) string { return w.Members()[i].Holder() }
			ms := time.Millisecond
			raise := func(id uint64, i, shard int, term core.Term) raiseReq {
				return raiseReq{reqHead: reqHead{ID: id, Holder: h(i)}, Space: "book", Shard: shard, Term: term}
			}
			write := func(id uint64, i, shard int, term core.Term) writeReq {
				return writeReq{reqHead: reqHead{ID: id, Holder: h(i)}, Space: "book", Shard: shard, Term: term}
			}
			fence := w.Fence()
			fw.RunFor(100 * ms) // the members boot and draw their holders
			s.send(200*ms, 0, fence, raise(1, 0, 1, 5))
			s.send(300*ms, 0, fence, write(2, 0, 1, 4))
			s.send(400*ms, 0, fence, write(3, 0, 1, 5))
			s.send(500*ms, 1, fence, write(4, 1, 2, 1))
			s.send(600*ms, 1, fence, raise(5, 1, 1, 3))
			s.at(time.Second, fence.Crash)
			s.send(2*time.Second, 0, fence, write(6, 0, 1, 6)) // down: dropped, never answered
			s.at(3*time.Second, fence.Restart)
			s.send(4*time.Second, 0, fence, write(7, 0, 1, 4)) // the raise survived
			s.send(4100*ms, 1, fence, write(8, 1, 1, 6))
			fw.RunFor(5 * time.Second)

			type answer struct {
				i    int
				id   uint64
				want string
			}
			answers := []answer{
				{0, 1, "raised#1 book/1 term=5 raised=5"},
				{0, 2, "written#2 book/1 term=4 raised=5 rejected"},
				{0, 3, "written#3 book/1 term=5 raised=5 accepted"},
				{1, 4, "written#4 book/2 term=1 raised=0 accepted"},
				{1, 5, "raised#5 book/1 term=3 raised=5"},
				{0, 7, "written#7 book/1 term=4 raised=5 rejected"},
				{1, 8, "written#8 book/1 term=6 raised=5 accepted"},
			}
			for _, a := range answers {
				rep := s.reply(a.i, a.id)
				if rep == nil {
					t.Errorf("m%d got no reply to #%d", a.i+1, a.id)
					continue
				}
				if got := rep.Describe(); got != a.want {
					t.Errorf("m%d: reply %q, want %q", a.i+1, got, a.want)
				}
			}
			if rep := s.reply(0, 6); rep != nil {
				t.Errorf("the fence answered #6, sent while it was down: %s", rep.Describe())
			}
			if n := len(s.replies(0)) + len(s.replies(1)); n != len(answers) {
				t.Errorf("%d replies, want %d", n, len(answers))
			}

			wantRecs := []struct {
				kind, text string
				attrs      []string
			}{
				{kindFence, raise(1, 0, 1, 5).Describe(), []string{"space=book", "shard=1", "term=5", "raised=5"}},
				{kindWrite, write(2, 0, 1, 4).Describe(),
					[]string{"space=book", "shard=1", "term=4", "holder=" + h(0), "result=rejected"}},
				{kindWrite, write(3, 0, 1, 5).Describe(),
					[]string{"space=book", "shard=1", "term=5", "holder=" + h(0), "result=accepted"}},
				{kindWrite, write(4, 1, 2, 1).Describe(),
					[]string{"space=book", "shard=2", "term=1", "holder=" + h(1), "result=accepted"}},
				{kindFence, raise(5, 1, 1, 3).Describe(), []string{"space=book", "shard=1", "term=3", "raised=5"}},
				{kindWrite, write(7, 0, 1, 4).Describe(),
					[]string{"space=book", "shard=1", "term=4", "holder=" + h(0), "result=rejected"}},
				{kindWrite, write(8, 1, 1, 6).Describe(),
					[]string{"space=book", "shard=1", "term=6", "holder=" + h(1), "result=accepted"}},
			}
			var got []kernel.Record
			for _, r := range fw.Sim.Records() {
				if r.Kind == kindFence || r.Kind == kindWrite {
					got = append(got, r)
				}
			}
			if len(got) != len(wantRecs) {
				t.Fatalf("%d fence records, want %d", len(got), len(wantRecs))
			}
			for i, r := range got {
				wr := wantRecs[i]
				if r.Kind != wr.kind || r.Node != fence.ID() || r.Text != wr.text || !slices.Equal(attrs(r), wr.attrs) {
					t.Errorf("record %d: %s at node %d %q %v, want %s at the fence %q %v",
						i, r.Kind, r.Node, r.Text, attrs(r), wr.kind, wr.text, wr.attrs)
				}
			}

			accepted := []check.Write{
				{Space: "book", Shard: 1, Term: 5, Holder: h(0)},
				{Space: "book", Shard: 2, Term: 1, Holder: h(1)},
				{Space: "book", Shard: 1, Term: 6, Holder: h(1)},
			}
			if !slices.Equal(hooked, accepted) {
				t.Errorf("onAccepted got %v, want %v", hooked, accepted)
			}
			if got := w.fenceSrv.model.accepted(); !slices.Equal(got, accepted) {
				t.Errorf("the log is %v, want %v", got, accepted)
			}

			// Every reply goes out in the event that delivered its request.
			for _, e := range traceEvents(fw.Sim) {
				delivered := 0
				for _, r := range e.of("net.deliver") {
					if attrValue(r, "to") == "fence" {
						delivered++
					}
				}
				if sent := len(e.sends("fence", "")); sent != delivered {
					t.Errorf("event %s at %s: %d deliveries to the fence, %d sends from it", e.label, e.at, delivered, sent)
				}
			}
		})
	})
}

// TestFenceDescribe checks the texts of the fence's messages.
func TestFenceDescribe(t *testing.T) {
	head := reqHead{ID: 3, Holder: "m1/0a"}
	tests := []struct {
		msg  kernel.Describer
		want string
	}{
		{raiseReq{head, "book", 3, 5}, "raise#3 m1/0a book/3 term=5"},
		{writeReq{reqHead{4, "m1/0a"}, "book", 3, 5}, "write#4 m1/0a book/3 term=5"},
		{raisedReply{replyHead{3}, "book", 3, 5, 7}, "raised#3 book/3 term=5 raised=7"},
		{writtenReply{replyHead{4}, "book", 3, 5, 5, true}, "written#4 book/3 term=5 raised=5 accepted"},
		{writtenReply{replyHead{4}, "book", 3, 4, 5, false}, "written#4 book/3 term=4 raised=5 rejected"},
	}
	for _, tt := range tests {
		if got := tt.msg.Describe(); got != tt.want {
			t.Errorf("Describe() = %q, want %q", got, tt.want)
		}
	}
}
