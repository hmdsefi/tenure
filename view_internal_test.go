// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"reflect"
	"testing"

	"github.com/hmdsefi/tenure/core"
)

// TestMergeView checks RUN-220's merge: holders cut or padded to the count, each member's Slot
// the lowest slot whose holder's ID part is the member's ID, else -1, and Version as given.
func TestMergeView(t *testing.T) {
	members := func(slots ...int) []core.Member {
		ms := []core.Member{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}, {ID: "n3", Addr: "a3"}}
		for i := range ms {
			ms[i].Slot = slots[i]
		}
		return ms
	}
	tests := []struct {
		name    string
		in      core.View
		count   int
		holders []string
		want    core.View
	}{
		{
			name: "no slots",
			in:   core.View{Version: 7, Members: members(2, -1, 0)},
			want: core.View{Version: 7, Members: members(-1, -1, -1)},
		},
		{
			name:    "no slots, holders cut",
			in:      core.View{Version: 7, Members: members(-1, -1, -1)},
			holders: []string{"n1/a"},
			want:    core.View{Version: 7, Members: members(-1, -1, -1)},
		},
		{
			name:    "holders padded",
			in:      core.View{Version: 3, Members: members(-1, -1, -1)},
			count:   3,
			holders: []string{"n2/aa"},
			want:    core.View{Version: 3, Members: members(-1, 0, -1), Slots: 3, SlotHolders: []string{"n2/aa", "", ""}},
		},
		{
			name:    "holders cut",
			in:      core.View{Version: 3, Members: members(-1, -1, -1)},
			count:   2,
			holders: []string{"", "n3/x", "n1/y", "n2/z"},
			want:    core.View{Version: 3, Members: members(-1, -1, 1), Slots: 2, SlotHolders: []string{"", "n3/x"}},
		},
		{
			name:    "lowest slot wins",
			in:      core.View{Version: 9, Members: members(-1, -1, -1)},
			count:   4,
			holders: []string{"", "n1/b", "n2/c", "n1/a"},
			want:    core.View{Version: 9, Members: members(1, 2, -1), Slots: 4, SlotHolders: []string{"", "n1/b", "n2/c", "n1/a"}},
		},
		{
			name:    "holder without slash and unknown holder",
			in:      core.View{Version: 1, Members: members(0, 1, 2)},
			count:   3,
			holders: []string{"n3", "x/1", "n10/1"},
			want:    core.View{Version: 1, Members: members(-1, -1, 0), Slots: 3, SlotHolders: []string{"n3", "x/1", "n10/1"}},
		},
		{
			name:  "no members",
			in:    core.View{Version: 2},
			count: 1,
			want:  core.View{Version: 2, Slots: 1, SlotHolders: []string{""}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			in.Members = append([]core.Member(nil), tt.in.Members...)
			holders := append([]string(nil), tt.holders...)
			got := mergeView(in, tt.count, holders)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeView(%+v, %d, %q) = %+v, want %+v", tt.in, tt.count, tt.holders, got, tt.want)
			}
			if !reflect.DeepEqual(in.Members, tt.in.Members) {
				t.Errorf("mergeView changed the input members to %+v", in.Members)
			}
			if len(got.Members) > 0 && &got.Members[0] == &in.Members[0] {
				t.Error("mergeView shares the members slice with its input")
			}
			if len(got.SlotHolders) > 0 && len(holders) > 0 && &got.SlotHolders[0] == &holders[0] {
				t.Error("mergeView shares the holders slice with its input")
			}
			if err := got.Validate(); err != nil {
				t.Errorf("mergeView(...).Validate() = %v, want nil", err)
			}
		})
	}
}
