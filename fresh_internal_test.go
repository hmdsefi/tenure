// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"reflect"
	"testing"
	"testing/synctest"

	"github.com/hmdsefi/tenure/core"
)

// freshHolder is the holder of the freshness filter's tests.
const freshHolder = "a/1"

// TestFreshFilter checks RUN-175's filter one rule at a time. Shard f/0 starts with floor 10,
// last 20 and grant 50; the run's term is 50, and the shard's latest Confirm action has tag 7,
// whose call went out at stamp 30 unless a case says otherwise.
func TestFreshFilter(t *testing.T) {
	const (
		floor = 10
		last  = 20
		grant = 50
	)
	us := func(term core.Term, version uint64) Holding {
		return Holding{Space: "f", Shard: 0, Holder: freshHolder, Term: term, Version: version}
	}
	other := func(term core.Term, version uint64) Holding {
		return Holding{Space: "f", Shard: 0, Holder: "x/1", Term: term, Version: version}
	}
	missing := Holding{Space: "f", Shard: 0}
	deletion := func(version uint64) Holding { return Holding{Space: "f", Shard: 0, Version: version} }
	hc := func(holder string, term core.Term) core.HolderChanged {
		return core.HolderChanged{Space: "f", Shard: 0, Holder: holder, Term: term}
	}
	none := core.HolderChanged{}

	tests := []struct {
		name     string
		rd       reading
		degraded bool
		confirm  uint64      // the shard's latest Confirm tag; 7 when 0, none when noConfirm
		started  confirmMark // the latest Confirm call that went out; {7, 30} when zero
		noCall   bool        // no Confirm call has gone out
		want     core.HolderChanged
		ok       bool
		floor    uint64 // the floor after the reading
		last     uint64 // last after the reading
	}{
		// 1. Spaces and shards the node does not have.
		{name: "unknown space", rd: reading{h: Holding{Space: "g", Holder: "x/1", Term: 60, Version: 11}, start: 40, end: 40},
			want: none, floor: floor, last: last},
		{name: "unknown shard", rd: reading{h: Holding{Space: "f", Shard: 2, Holder: "x/1", Term: 60, Version: 11}, start: 40, end: 40},
			want: none, floor: floor, last: last},
		{name: "negative shard", rd: reading{h: Holding{Space: "f", Shard: -1, Holder: "x/1", Term: 60, Version: 11}, start: 40, end: 40},
			want: none, floor: floor, last: last},

		// 2. The floor (O4).
		{name: "version above the floor steps", rd: reading{h: other(60, 11), start: 40, end: 41},
			want: hc("x/1", 60), ok: true, floor: 11, last: 41},
		{name: "version at the floor is dropped", rd: reading{h: other(60, 10), start: 40, end: 41},
			want: none, floor: floor, last: last},
		{name: "version below the floor is dropped", rd: reading{h: other(60, 9), start: 40, end: 41},
			want: none, floor: floor, last: last},
		{name: "Confirm answer at the floor steps", rd: reading{h: us(50, 10), confirm: true, start: 40, end: 41},
			want: hc(freshHolder, 50), ok: true, floor: 10, last: 41},
		{name: "Confirm answer below the floor is dropped", rd: reading{h: us(50, 9), confirm: true, start: 40, end: 41},
			want: none, floor: floor, last: last},

		// 3. The grant.
		{name: "term below the grant is dropped", rd: reading{h: other(49, 11), start: 40, end: 41},
			want: none, floor: floor, last: last},
		{name: "cleared record below the grant is dropped", rd: reading{h: Holding{Space: "f", Term: 49, Version: 11}, start: 40, end: 41},
			want: none, floor: floor, last: last},
		{name: "term at the grant steps", rd: reading{h: Holding{Space: "f", Holder: "", Term: 50, Version: 11}, start: 40, end: 41},
			want: hc("", 50), ok: true, floor: 11, last: 41},

		// 4. The absent-record rule.
		{name: "missing shard read before the last stepped reading is dropped", rd: reading{h: missing, start: 19, end: 45},
			want: none, floor: floor, last: last},
		{name: "missing shard read after the last stepped reading steps", rd: reading{h: missing, start: 21, end: 45},
			want: hc("", 0), ok: true, floor: floor, last: 45},
		{name: "absent Confirm answer read before the last stepped reading is dropped",
			rd: reading{h: missing, confirm: true, start: 19, end: 45}, want: none, floor: floor, last: last},
		{name: "absent Confirm answer read after the last stepped reading steps",
			rd: reading{h: missing, confirm: true, start: 30, end: 45}, want: hc("", 0), ok: true, floor: floor, last: 45},
		{name: "watch deletion above the floor steps by version", rd: reading{h: deletion(11), start: 5, end: 5},
			want: hc("", 0), ok: true, floor: 11, last: last},
		{name: "watch deletion at the floor is dropped", rd: reading{h: deletion(10), start: 40, end: 40},
			want: none, floor: floor, last: last},

		// 5. The Confirm hold-back (O5).
		{name: "degraded: our record with no Confirm is held back", degraded: true, confirm: noConfirm, noCall: true,
			rd: reading{h: us(50, 11), start: 40, end: 40}, want: none, floor: floor, last: last},
		{name: "degraded: our record before the latest Confirm call went out is held back", degraded: true,
			started: confirmMark{tag: 3, start: 4}, rd: reading{h: us(50, 11), start: 40, end: 40}, want: none, floor: floor, last: last},
		{name: "degraded: our record read before the Confirm call started is held back", degraded: true,
			rd: reading{h: us(50, 11), start: 29, end: 40}, want: none, floor: floor, last: last},
		{name: "degraded: our record read after the Confirm call started steps", degraded: true,
			rd: reading{h: us(50, 11), start: 31, end: 40}, want: hc(freshHolder, 50), ok: true, floor: 11, last: 40},
		{name: "degraded: the latest Confirm's answer steps", degraded: true,
			rd: reading{h: us(50, 10), confirm: true, start: 30, end: 40}, want: hc(freshHolder, 50), ok: true, floor: 10, last: 40},
		{name: "degraded: an earlier Confirm's answer is held back", degraded: true,
			rd: reading{h: us(50, 10), confirm: true, start: 25, end: 40}, want: none, floor: floor, last: last},
		{name: "degraded: our record at a larger term steps", degraded: true,
			rd: reading{h: us(51, 11), start: 1, end: 2}, want: hc(freshHolder, 51), ok: true, floor: 11, last: last},
		{name: "degraded: another holder's record steps", degraded: true,
			rd: reading{h: other(60, 11), start: 1, end: 2}, want: hc("x/1", 60), ok: true, floor: 11, last: last},
		{name: "owned: our record steps without a Confirm", confirm: noConfirm, noCall: true,
			rd: reading{h: us(50, 11), start: 1, end: 2}, want: hc(freshHolder, 50), ok: true, floor: 11, last: last},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			started := tc.started
			if started == (confirmMark{}) && !tc.noCall {
				started = confirmMark{tag: 7, start: 30}
			}
			f := newFreshness(freshHolder, []Space{{Name: "f", Shards: 2, Fenced: true}},
				func(space string, shard int) (uint64, uint64) {
					if space != "f" || shard != 0 {
						t.Errorf("started(%s, %d), want f/0", space, shard)
					}
					return started.tag, started.start
				})
			s := f.shard("f", 0)
			s.floor, s.last, s.grant, s.confirm = floor, last, grant, 7
			if tc.confirm == noConfirm {
				s.confirm = 0
			}
			state := core.ShardStateOwned
			if tc.degraded {
				state = core.ShardStateDegraded
			}
			st := core.Status{Spaces: []core.SpaceStatus{{Name: "f", Fenced: true, Shards: []core.ShardStatus{
				{State: state, Term: 50, Holder: freshHolder},
				{State: core.ShardStateIdle},
			}}}}

			got, ok := f.admit(tc.rd, st)
			if got != tc.want || ok != tc.ok {
				t.Errorf("admit(%+v) = %+v, %t, want %+v, %t", tc.rd, got, ok, tc.want, tc.ok)
			}
			if s.floor != tc.floor || s.last != tc.last {
				t.Errorf("floor, last = %d, %d, want %d, %d", s.floor, s.last, tc.floor, tc.last)
			}
			if s.grant != grant {
				t.Errorf("grant = %d, want %d unchanged", s.grant, grant)
			}
		})
	}
}

// noConfirm marks a TestFreshFilter case whose shard has had no Confirm action.
const noConfirm = ^uint64(0)

// TestFreshGrantAndConfirm checks the state the loop feeds the filter: an Acquired grant sets the
// grant and moves last to the Acquire's end, and a Confirm action sets the shard's tag, counting
// it as issued only when it was queued.
func TestFreshGrantAndConfirm(t *testing.T) {
	f := newFreshness(freshHolder, []Space{{Name: "f", Shards: 1}, {Name: "a", Shards: 1}}, nil)
	if want := []string{"a", "f"}; !reflect.DeepEqual(f.names, want) {
		t.Errorf("names = %v, want %v", f.names, want)
	}
	s := f.shard("f", 0)
	f.granted("f", 0, 50, 12)
	f.granted("f", 0, 60, 9) // an earlier end: last stays
	if s.grant != 60 || s.last != 12 {
		t.Errorf("grant, last = %d, %d, want 60, 12", s.grant, s.last)
	}
	f.granted("f", 1, 70, 99) // no such shard
	f.confirmSent(core.Confirm{Space: "f", Shard: 0, Term: 60}, 13, true)
	f.confirmSent(core.Confirm{Space: "f", Shard: 0, Term: 60}, 14, false)
	if s.confirm != 14 || f.issued != 1 {
		t.Errorf("confirm, issued = %d, %d, want 14, 1", s.confirm, f.issued)
	}
	f.confirmSent(core.Confirm{Space: "g", Shard: 0, Term: 60}, 15, true)
	if f.issued != 2 {
		t.Errorf("issued = %d, want 2", f.issued)
	}
}

// TestConfirmStarted checks that the executor reports the tag and the start stamp of a shard's
// latest Confirm call only once that call has gone out.
func TestConfirmStarted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		h.do(core.Release{Space: "f", Shard: 0, Term: 5}, true)
		synctest.Wait()
		tag, queued := h.e.doConfirm(1, execHolder, core.Confirm{Space: "f", Shard: 0, Term: 5})
		if !queued || tag == 0 {
			t.Fatalf("doConfirm() = %d, %t, want a tag and true", tag, queued)
		}
		if gotTag, gotStart := h.e.confirmStarted("f", 0); gotTag != 0 || gotStart != 0 {
			t.Errorf("confirmStarted() = %d, %d while the call waits behind Release, want 0, 0", gotTag, gotStart)
		}
		h.answer("Release", scriptReply{})
		gotTag, gotStart := h.e.confirmStarted("f", 0)
		if gotTag != tag || gotStart <= tag {
			t.Errorf("confirmStarted() = %d, %d once the call went out, want %d and a later stamp", gotTag, gotStart, tag)
		}
		h.answer("Confirm", scriptReply{holding: Holding{Space: "f", Holder: execHolder, Term: 5, Version: 3}})
		var conf callResult
		for _, r := range h.e.take() {
			if _, ok := r.act.(core.Confirm); ok {
				conf = r
			}
		}
		if conf.start != gotStart || conf.holding.Version != 3 {
			t.Errorf("Confirm result start %d, holding %+v, want start %d and the record", conf.start, conf.holding, gotStart)
		}
		if gotTag, _ := h.e.confirmStarted("f", 1); gotTag != 0 {
			t.Errorf("confirmStarted(f, 1) = %d, want 0", gotTag)
		}
	})
}

// TestHoldingReadings checks that a Holdings answer becomes one reading per shard: the record
// with the highest Version, or no record, leaving out other spaces and shards out of range.
func TestHoldingReadings(t *testing.T) {
	hs := []Holding{
		{Space: "f", Shard: 2, Holder: "x/1", Term: 4, Version: 9},
		{Space: "f", Shard: 0, Holder: "x/1", Term: 3, Version: 5},
		{Space: "f", Shard: 0, Holder: "y/1", Term: 2, Version: 4},
		{Space: "g", Shard: 1, Holder: "x/1", Term: 3, Version: 6},
		{Space: "f", Shard: 3, Holder: "x/1", Term: 3, Version: 7},
		{Space: "f", Shard: -1, Holder: "x/1", Term: 3, Version: 8},
	}
	got := holdingReadings("f", 3, hs, 10, 11)
	want := []reading{
		{h: hs[1], start: 10, end: 11},
		{h: Holding{Space: "f", Shard: 1}, start: 10, end: 11},
		{h: hs[0], start: 10, end: 11},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("holdingReadings() = %+v, want %+v", got, want)
	}
	if !got[1].absent() || got[0].absent() || !(reading{h: Holding{Space: "f", Version: 3}}).absent() {
		t.Errorf("absent() wrong for %+v", got)
	}
}
