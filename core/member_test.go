// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

func ids(ms []Member) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.ID)
	}
	return out
}

func members(names ...string) []Member {
	ms := make([]Member, len(names))
	for i, id := range names {
		ms[i] = Member{ID: id}
	}
	return ms
}

func TestATCOR01MemberValidate(t *testing.T) {
	tests := []struct {
		name string
		m    Member
		want string // "" means nil
	}{
		{"slot -1", Member{ID: "pod-0", Slot: -1}, ""},
		{"zero slot", Member{ID: "pod-0"}, ""},
		{"empty", Member{}, "core: invalid member: empty ID"},
		{"slash", Member{ID: "a/b"}, "core: invalid member: ID \"a/b\" contains '/'"},
		{"slash before zero byte", Member{ID: "a/b\x00"}, "core: invalid member: ID \"a/b\\x00\" contains '/'"},
		{"zero byte", Member{ID: "a\x00b"}, "core: invalid member: ID \"a\\x00b\" contains a zero byte"},
		{"slot below -1", Member{ID: "a", Slot: -2}, "core: invalid member \"a\": Slot -2 is below -1"},
		{"window equal", Member{ID: "a", JoinAt: t0, LeaveAt: t0}, "core: invalid member \"a\": LeaveAt is not after JoinAt"},
		{"window reversed", Member{ID: "a", JoinAt: t0, LeaveAt: t0.Add(-time.Second)}, "core: invalid member \"a\": LeaveAt is not after JoinAt"},
		{"window one ns", Member{ID: "a", JoinAt: t0, LeaveAt: t0.Add(time.Nanosecond)}, ""},
		{"only JoinAt", Member{ID: "a", JoinAt: t0}, ""},
		{"only LeaveAt", Member{ID: "a", LeaveAt: t0}, ""},
		{"unchecked fields", Member{ID: "a", Addr: "", Meta: nil, Draining: true}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.m.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("Validate() = %q, want %q", err.Error(), tt.want)
			}
			if !errors.Is(err, ErrInvalidMember) {
				t.Errorf("errors.Is(%v, ErrInvalidMember) = false", err)
			}
			if errors.Is(err, ErrInvalidView) {
				t.Errorf("errors.Is(%v, ErrInvalidView) = true", err)
			}
		})
	}
}

func TestATCOR02ViewValidate(t *testing.T) {
	slotted := func(aSlot, bSlot int) View {
		return View{
			Slots:       2,
			SlotHolders: []string{"a/1", ""},
			Members:     []Member{{ID: "a", Slot: aSlot}, {ID: "b", Slot: bSlot}},
		}
	}
	tests := []struct {
		name string
		v    View
		want string // "" means nil
	}{
		{"zero view", View{}, ""},
		{"no slots", View{Members: members("a", "b", "c")}, ""},
		{"version ignored", View{Version: 7, Members: members("a", "b", "c")}, ""},
		{"unsorted", View{Members: members("b", "a")}, "core: invalid view: Members[1]: ID \"a\" does not sort after \"b\""},
		{"duplicate", View{Members: members("a", "a")}, "core: invalid view: Members[1]: ID \"a\" does not sort after \"a\""},
		{"slash in second", View{Members: members("a", "x/y")}, "core: invalid view: Members[1]: ID \"x/y\" contains '/'"},
		{"member check before order", View{Members: members("x/y", "a")}, "core: invalid view: Members[0]: ID \"x/y\" contains '/'"},
		{"empty ID", View{Members: members("")}, "core: invalid view: Members[0]: empty ID"},
		{"zero byte", View{Members: members("a\x00b")}, "core: invalid view: Members[0]: ID \"a\\x00b\" contains a zero byte"},
		{"member slot", View{Members: []Member{{ID: "a", Slot: -2}}}, "core: invalid view: Members[0]: Slot -2 is below -1"},
		{"member window", View{Members: []Member{{ID: "a", JoinAt: t0, LeaveAt: t0}}}, "core: invalid view: Members[0]: LeaveAt is not after JoinAt"},
		{"negative slots", View{Slots: -1}, "core: invalid view: Slots -1 is negative"},
		{"holder count", View{Slots: 2, SlotHolders: []string{"h"}}, "core: invalid view: 1 SlotHolders for 2 slots"},
		{"holders without slots", View{SlotHolders: []string{"h"}}, "core: invalid view: 1 SlotHolders for 0 slots"},
		{"slotted ok", slotted(0, -1), ""},
		{"slot out of range", slotted(2, -1), "core: invalid view: Members[0]: Slot 2 out of range for 2 slots"},
		{"slot vacant", slotted(1, -1), "core: invalid view: Members[0]: Slot 1 is vacant"},
		{"slot shared", slotted(0, 0), "core: invalid view: Members[1]: Slot 0 already held by \"a\""},
		{"opaque holders", View{Slots: 1, SlotHolders: []string{"zz/9"}, Members: []Member{{ID: "a", Slot: -1}}}, ""},
		{"check 1 before 2", View{Members: members("x/y", "a")}, "core: invalid view: Members[0]: ID \"x/y\" contains '/'"},
		{"check 1 before 3", View{Slots: -1, Members: []Member{{ID: "a", Slot: -2}}}, "core: invalid view: Members[0]: Slot -2 is below -1"},
		{"check 2 before 3", View{Slots: -1, Members: members("b", "a")}, "core: invalid view: Members[1]: ID \"a\" does not sort after \"b\""},
		{"check 3 before 4", View{Slots: -1, SlotHolders: []string{"h"}}, "core: invalid view: Slots -1 is negative"},
		{"check 4 before 5", View{Slots: 2, SlotHolders: []string{"h"}, Members: []Member{{ID: "a", Slot: 5}}}, "core: invalid view: 1 SlotHolders for 2 slots"},
		{"slots ignored without slots", View{Members: []Member{{ID: "a", Slot: 5}, {ID: "b", Slot: 5}}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.v.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("Validate() = %q, want %q", err.Error(), tt.want)
			}
			if !errors.Is(err, ErrInvalidView) {
				t.Errorf("errors.Is(%v, ErrInvalidView) = false", err)
			}
			if errors.Is(err, ErrInvalidMember) {
				t.Errorf("errors.Is(%v, ErrInvalidMember) = true", err)
			}
		})
	}
}

// eligibleView is AT-COR-03's view: a (zero), b (draining), c (joins at t0+10s),
// d (leaves at t0+10s), e (t0 to t0+20s).
func eligibleView() View {
	return View{Members: []Member{
		{ID: "a"},
		{ID: "b", Draining: true},
		{ID: "c", JoinAt: t0.Add(10 * time.Second)},
		{ID: "d", LeaveAt: t0.Add(10 * time.Second)},
		{ID: "e", JoinAt: t0, LeaveAt: t0.Add(20 * time.Second)},
	}}
}

func TestATCOR03Eligible(t *testing.T) {
	slotted := eligibleView()
	slotted.Slots = 2
	slotted.SlotHolders = []string{"a/x", "c/y"}
	for i := range slotted.Members {
		slotted.Members[i].Slot = -1
	}
	slotted.Members[0].Slot = 0
	slotted.Members[2].Slot = 1

	plain := View{Members: members("a", "b", "c")}
	plain.Members[1].Draining = false

	allDraining := eligibleView()
	for i := range allDraining.Members {
		allDraining.Members[i].Draining = true
	}

	tests := []struct {
		name string
		v    View
		at   time.Time
		want []string
	}{
		{"t0", eligibleView(), t0, []string{"a", "d", "e"}},
		{"t0+10s", eligibleView(), t0.Add(10 * time.Second), []string{"a", "c", "e"}},
		{"t0+20s", eligibleView(), t0.Add(20 * time.Second), []string{"a", "c"}},
		{"no bounds", plain, t0, []string{"a", "b", "c"}},
		{"slotted", slotted, t0.Add(10 * time.Second), []string{"a", "c"}},
		{"all draining", allDraining, t0, nil},
		{"empty view", View{}, t0, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Eligible(tt.v, tt.at)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("Eligible() = %v, want nil", ids(got))
				}
				return
			}
			if !slices.Equal(ids(got), tt.want) {
				t.Errorf("Eligible() = %v, want %v", ids(got), tt.want)
			}
		})
	}

	t.Run("result is a copy sharing Meta", func(t *testing.T) {
		v := eligibleView()
		v.Members[0].Meta = map[string]string{"role": "x"}
		got := Eligible(v, t0)
		if len(got) == 0 {
			t.Fatal("Eligible() is empty")
		}
		got[0].ID = "zz"
		if v.Members[0].ID != "a" {
			t.Errorf("input ID = %q after changing the result, want \"a\"", v.Members[0].ID)
		}
		if reflect.ValueOf(got[0].Meta).Pointer() != reflect.ValueOf(v.Members[0].Meta).Pointer() {
			t.Error("result Meta is not the input's map")
		}
	})
}

func deepCopyMembers(ms []Member) []Member {
	if ms == nil {
		return nil
	}
	out := make([]Member, len(ms))
	for i, m := range ms {
		out[i] = m
		if m.Meta != nil {
			out[i].Meta = make(map[string]string, len(m.Meta))
			for k, val := range m.Meta {
				out[i].Meta[k] = val
			}
		}
	}
	return out
}

func deepCopyView(v View) View {
	c := v
	c.Members = deepCopyMembers(v.Members)
	if v.SlotHolders != nil {
		c.SlotHolders = slices.Clone(v.SlotHolders)
	}
	return c
}

func TestATCOR19PurityMembers(t *testing.T) {
	withMeta := eligibleView()
	withMeta.Members[0].Meta = map[string]string{"role": "x"}
	slotted := withMeta
	slotted.Members = deepCopyMembers(withMeta.Members)
	slotted.Slots = 2
	slotted.SlotHolders = []string{"a/x", "c/y"}
	for i := range slotted.Members {
		slotted.Members[i].Slot = -1
	}
	slotted.Members[0].Slot = 0
	slotted.Members[2].Slot = 1

	views := []View{
		{},
		{Version: 7, Members: members("a", "b", "c")},
		{Members: members("b", "a")},
		{Members: members("x/y", "a")},
		{Slots: 2, SlotHolders: []string{"a/1", ""}, Members: []Member{{ID: "a", Slot: 0}, {ID: "b", Slot: -1}}},
		withMeta,
		slotted,
	}
	for i, v := range views {
		before := deepCopyView(v)
		_ = v.Validate()
		for _, m := range v.Members {
			_ = m.Validate()
		}
		_ = Eligible(v, t0.Add(10*time.Second))
		if !reflect.DeepEqual(before, v) {
			t.Errorf("view %d changed:\nbefore %+v\nafter  %+v", i, before, v)
		}
	}
}
