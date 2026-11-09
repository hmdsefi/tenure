// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/hmdsefi/tenure/core"
)

// TestATK8S02NameRules checks the name rules on every method that takes a name, each row with
// no request. Each task that adds such a method adds its rows. The acceptance test's steps were
// written from the issue's text: the original text is unavailable.
func TestATK8S02NameRules(t *testing.T) {
	label63 := strings.Repeat("a", 63)
	// g-member-<id> is at most 253 characters for an id of at most 244.
	id244 := strings.Repeat("b", 244)
	id245 := strings.Repeat("b", 245)
	checkNames := func(spaces []string, member string) func(context.Context, *Adapter) error {
		return func(_ context.Context, a *Adapter) error { return a.CheckNames(spaces, member) }
	}
	release := func(holder, space string, shard int, term core.Term) func(context.Context, *Adapter) error {
		return func(ctx context.Context, a *Adapter) error { return a.Release(ctx, holder, space, shard, term) }
	}
	setSlots := func(count int) func(context.Context, *Adapter) error {
		return func(ctx context.Context, a *Adapter) error { return a.SetSlots(ctx, count) }
	}
	releaseSlot := func(holder string, slot int) func(context.Context, *Adapter) error {
		return func(ctx context.Context, a *Adapter) error { return a.ReleaseSlot(ctx, holder, slot) }
	}
	claimSlot := func(holder string) func(context.Context, *Adapter) error {
		return func(ctx context.Context, a *Adapter) error {
			slot, err := a.ClaimSlot(ctx, holder)
			if slot != -1 {
				return fmt.Errorf("slot %d, want -1 with %w", slot, err)
			}
			return err
		}
	}
	space := func(s, detail string) string { return fmt.Sprintf("k8s: invalid space %q: %s", s, detail) }
	member := func(id, detail string) string { return fmt.Sprintf("k8s: invalid member ID %q: %s", id, detail) }
	const (
		notLabel     = "must be a DNS-1123 label"
		reserved     = "is reserved"
		notSubdomain = "must be a DNS-1123 subdomain"
	)
	tests := []struct {
		name string
		call func(context.Context, *Adapter) error
		want string // "" for no error
	}{
		{"CheckNames no spaces", checkNames(nil, "pod-0"), ""},
		{"CheckNames valid spaces", checkNames([]string{"markets", "a", "0", label63, "slot-1", "slots-a", "members", "requests", "generation-1", "x-member-y"}, "pod-0"), ""},
		{"CheckNames space Bad", checkNames([]string{"Bad"}, "pod-0"), space("Bad", notLabel)},
		{"CheckNames space empty", checkNames([]string{""}, "pod-0"), space("", notLabel)},
		{"CheckNames space of 64", checkNames([]string{label63 + "a"}, "pod-0"), space(label63+"a", notLabel)},
		{"CheckNames space a_b", checkNames([]string{"a_b"}, "pod-0"), space("a_b", notLabel)},
		{"CheckNames space a.b", checkNames([]string{"a.b"}, "pod-0"), space("a.b", notLabel)},
		{"CheckNames space -a", checkNames([]string{"-a"}, "pod-0"), space("-a", notLabel)},
		{"CheckNames space a-", checkNames([]string{"a-"}, "pod-0"), space("a-", notLabel)},
		{"CheckNames space zero byte", checkNames([]string{"a\x00"}, "pod-0"), space("a\x00", notLabel)},
		{"CheckNames space generation", checkNames([]string{"generation"}, "pod-0"), space("generation", reserved)},
		{"CheckNames space member", checkNames([]string{"member"}, "pod-0"), space("member", reserved)},
		{"CheckNames space request", checkNames([]string{"request"}, "pod-0"), space("request", reserved)},
		{"CheckNames space slot", checkNames([]string{"slot"}, "pod-0"), space("slot", reserved)},
		{"CheckNames space slots", checkNames([]string{"slots"}, "pod-0"), space("slots", reserved)},
		{"CheckNames space member-0", checkNames([]string{"member-0"}, "pod-0"), space("member-0", `must not start with "member-"`)},
		{"CheckNames space request-x", checkNames([]string{"request-x"}, "pod-0"), space("request-x", `must not start with "request-"`)},
		{"CheckNames first bad space", checkNames([]string{"markets", "Bad", "slots"}, "pod-0"), space("Bad", notLabel)},
		{"CheckNames space before member", checkNames([]string{"slots"}, "Bad"), space("slots", reserved)},
		{"CheckNames valid members", checkNames(nil, "web-7d9f-xk2p"), ""},
		{"CheckNames member with dots", checkNames(nil, "a.b.c"), ""},
		{"CheckNames member 0", checkNames(nil, "0"), ""},
		{"CheckNames member of 244", checkNames(nil, id244), ""},
		{"CheckNames member Bad_ID-history", checkNames(nil, "Bad_ID-history"), member("Bad_ID-history", notSubdomain)},
		{"CheckNames member empty", checkNames(nil, ""), member("", notSubdomain)},
		{"CheckNames member Pod", checkNames(nil, "Pod"), member("Pod", notSubdomain)},
		{"CheckNames member -a", checkNames(nil, "-a"), member("-a", notSubdomain)},
		{"CheckNames member a.", checkNames(nil, "a."), member("a.", notSubdomain)},
		{"CheckNames member a..b", checkNames(nil, "a..b"), member("a..b", notSubdomain)},
		{"CheckNames member a/b", checkNames(nil, "a/b"), member("a/b", notSubdomain)},
		{"CheckNames member zero byte", checkNames(nil, "a\x00"), member("a\x00", notSubdomain)},
		{
			"CheckNames member of 245", checkNames(nil, id245),
			member(id245, "makes Lease name g-member-"+id245+" longer than 253 characters"),
		},
		{"CheckNames member of 254", checkNames(nil, id245+"bbbbbbbbb"), member(id245+"bbbbbbbbb", notSubdomain)},
		{"Release empty holder", release("", "Bad", -1, 0), "k8s: Release: empty holder"},
		{"Release holder zero byte", release("pod-0/\x00", "markets", 0, 1), `k8s: Release: holder "pod-0/\x00": contains a zero byte`},
		{"Release holder without /", release("pod-0", "markets", 0, 1), `k8s: Release: holder "pod-0": has no "/"`},
		{"Release holder empty incarnation", release("pod-0/", "markets", 0, 1), `k8s: Release: holder "pod-0/": has an empty incarnation`},
		{"Release holder Pod/1", release("Pod/1", "markets", 0, 1), `k8s: Release: holder "Pod/1": member ID ` + notSubdomain},
		{"Release space Bad", release("pod-0/1", "Bad", -1, 0), `k8s: Release: space "Bad": ` + notLabel},
		{"Release space empty", release("pod-0/1", "", 0, 1), `k8s: Release: space "": ` + notLabel},
		{"Release space a/0", release("pod-0/1", "a/0", 0, 1), `k8s: Release: space "a/0": ` + notLabel},
		{"Release space slots", release("pod-0/1", "slots", 0, 1), `k8s: Release: space "slots": ` + reserved},
		{"Release space member-0", release("pod-0/1", "member-0", 0, 1), `k8s: Release: space "member-0": must not start with "member-"`},
		{"Release shard -1", release("pod-0/1", "markets", -1, 0), "k8s: Release: shard -1: must not be negative"},
		{"Release term 0", release("pod-0/1", "markets", 0, 0), "k8s: Release: term 0: must not be 0"},
		{"SetSlots 10001", setSlots(10001), "k8s: SetSlots: count 10001: must be in [0, 10000]"},
		{"SetSlots -1", setSlots(-1), "k8s: SetSlots: count -1: must be in [0, 10000]"},
		{"ReleaseSlot 10000", releaseSlot("pod-0/1", 10000), "k8s: ReleaseSlot: slot 10000: must be in [0, 9999]"},
		{"ReleaseSlot -1", releaseSlot("pod-0/1", -1), "k8s: ReleaseSlot: slot -1: must be in [0, 9999]"},
		{"ReleaseSlot empty holder", releaseSlot("", 10000), "k8s: ReleaseSlot: empty holder"},
		{"ReleaseSlot holder without /", releaseSlot("pod-0", 0), `k8s: ReleaseSlot: holder "pod-0": has no "/"`},
		{"ReleaseSlot holder Pod/1", releaseSlot("Pod/1", 0), `k8s: ReleaseSlot: holder "Pod/1": member ID ` + notSubdomain},
		{"ClaimSlot empty holder", claimSlot(""), "k8s: ClaimSlot: empty holder"},
		{"ClaimSlot holder zero byte", claimSlot("pod-0/\x00"), `k8s: ClaimSlot: holder "pod-0/\x00": contains a zero byte`},
		{"ClaimSlot holder empty incarnation", claimSlot("pod-0/"), `k8s: ClaimSlot: holder "pod-0/": has an empty incarnation`},
		{"ClaimSlot holder a/b/c", claimSlot("a/b/c"), `k8s: ClaimSlot: holder "a/b/c": has a "/" in its incarnation`},
	}
	ctx := t.Context()
	client := fake.NewClientset()
	a := New(client, "default", "g")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(ctx, a)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("err = %v, want nil", err)
			case tc.want != "" && (err == nil || err.Error() != tc.want):
				t.Errorf("err = %v, want %s", err, tc.want)
			}
		})
	}
	if n := len(client.Actions()); n != 0 {
		t.Errorf("%d requests, want none: %v", n, client.Actions())
	}
}

// TestATK8S03Objects checks the Lease names of the group's objects. The acceptance test's steps
// were written from the issue's text: the original text is unavailable.
func TestATK8S03Objects(t *testing.T) {
	t.Run("names", func(t *testing.T) {
		type parsed struct {
			kind, space, id string
			n               int
		}
		members := []string{"pod-0", "a.b", "slot-1", "x-5", "member-3", "slots", "0"}
		spaces := []string{"markets", "slot-1", "x", "a-b", "members", "slots-0", "0"}
		shards := []int{0, 1, 10, 123456789}
		slots := []int{0, 1, 42, maxSlot}
		for _, g := range []string{"g", "g0123abcd", "slot", "a-b", strings.Repeat("g", 40)} {
			want := map[string]parsed{slotsName(g): {kind: kindSlots}}
			add := func(name string, p parsed) {
				if prev, dup := want[name]; dup {
					t.Errorf("group %s: name %s for both %+v and %+v", g, name, prev, p)
				}
				want[name] = p
			}
			for _, id := range members {
				add(memberName(g, id), parsed{kind: kindMember, id: id})
			}
			for _, s := range spaces {
				for _, n := range shards {
					add(shardName(g, s, n), parsed{kind: kindShard, space: s, n: n})
				}
			}
			for _, n := range slots {
				add(slotName(g, n), parsed{kind: kindSlot, n: n})
			}
			for name, p := range want {
				kind, space, id, n, ok := parseName(g, name)
				if got := (parsed{kind, space, id, n}); !ok || got != p {
					t.Errorf("parseName(%q, %q) = %+v, %t; want %+v, true", g, name, got, ok, p)
				}
			}
		}
		for name, want := range map[string]string{
			memberName("g", "pod-0"):     "g-member-pod-0",
			shardName("g", "markets", 7): "g-markets-7",
			slotsName("g"):               "g-slots",
			slotName("g", 3):             "g-slot-3",
		} {
			if name != want {
				t.Errorf("name %s, want %s", name, want)
			}
		}
	})
}

// TestK8S020ParseName checks that parseName rejects names that no builder makes for the group.
func TestK8S020ParseName(t *testing.T) {
	for _, name := range []string{
		"", "g", "g-", "gx-markets-1", "h-markets-1", "g-markets", "g-markets-", "g-markets-01", "g-markets--1",
		"g-markets-+1", "g-markets-1a", "g-markets-99999999999999999999", "g-Bad-1", "g-generation-1",
		"g-request-1", "g-request-x-1", "g-slot-01", "g-slot-10000", "g-slot--1", "g-slots-", "g-member-",
		"g-member-Bad", "g-member-a..b",
	} {
		if kind, space, id, n, ok := parseName("g", name); ok {
			t.Errorf("parseName(g, %q) = %s, %q, %q, %d, true; want false", name, kind, space, id, n)
		}
	}
	if _, _, _, _, ok := parseName("G", "G-markets-1"); ok {
		t.Error(`parseName("G", "G-markets-1") is ok for an invalid group`)
	}
}

// TestK8S030Holder checks the holder rule of the store methods.
func TestK8S030Holder(t *testing.T) {
	id245 := strings.Repeat("b", 245)
	for h, want := range map[string]string{
		"pod-0/1":      "",
		"a.b/x/":       `holder "a.b/x/": has a "/" in its incarnation`,
		"pod-0/7f3c-é": "",
		"":             "empty holder",
		"pod-0/\x00":   `holder "pod-0/\x00": contains a zero byte`,
		"pod-0":        `holder "pod-0": has no "/"`,
		"pod-0/":       `holder "pod-0/": has an empty incarnation`,
		"Pod/1":        `holder "Pod/1": member ID must be a DNS-1123 subdomain`,
		"/1":           `holder "/1": member ID must be a DNS-1123 subdomain`,
		id245 + "/1":   `holder "` + id245 + `/1": member ID makes Lease name g-member-` + id245 + ` longer than 253 characters`,
	} {
		err := checkHolder("g", h)
		switch {
		case want == "" && err != nil:
			t.Errorf("checkHolder(%q) = %v, want nil", h, err)
		case want != "" && (err == nil || err.Error() != want):
			t.Errorf("checkHolder(%q) = %v, want %s", h, err, want)
		}
	}
}
