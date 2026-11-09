// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/hmdsefi/tenure/core"
)

// sameMember reports whether a and b are equal, comparing times with Equal.
func sameMember(a, b core.Member) bool {
	return a.ID == b.ID && a.Addr == b.Addr && maps.Equal(a.Meta, b.Meta) && (a.Meta == nil) == (b.Meta == nil) &&
		a.Draining == b.Draining && a.Slot == b.Slot && a.JoinAt.Equal(b.JoinAt) && a.LeaveAt.Equal(b.LeaveAt)
}

// TestATK8S04Encoding checks the member record of the tenure.member annotation against fixed
// vectors, and its decoding. The acceptance test's steps were written from the issue's text:
// the original text is unavailable.
func TestATK8S04Encoding(t *testing.T) {
	noon := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	plus2 := time.FixedZone("UTC+2", 2*60*60)
	vectors := []struct {
		name   string
		member core.Member
		json   string
		// decoded is the member decodeMember returns for json: member with Slot -1, times in
		// UTC and an empty Meta as nil.
		decoded core.Member
	}{
		{
			name:    "minimal",
			member:  core.Member{ID: "pod-0", Slot: 3},
			json:    `{"v":1,"id":"pod-0"}`,
			decoded: core.Member{ID: "pod-0", Slot: -1},
		},
		{
			name:    "empty meta",
			member:  core.Member{ID: "pod-0", Meta: map[string]string{}, Slot: -1},
			json:    `{"v":1,"id":"pod-0"}`,
			decoded: core.Member{ID: "pod-0", Slot: -1},
		},
		{
			name: "every field",
			member: core.Member{
				ID: "web-7d9f-xk2p", Addr: "10.0.0.7:8080", Meta: map[string]string{"zone": "a", "app": "x<y>&"},
				Draining: true, Slot: 2, JoinAt: noon, LeaveAt: noon.Add(time.Hour + 500*time.Millisecond),
			},
			json: `{"v":1,"id":"web-7d9f-xk2p","addr":"10.0.0.7:8080","meta":{"app":"x<y>&","zone":"a"},` +
				`"draining":true,"joinAt":"2026-11-02T12:00:00Z","leaveAt":"2026-11-02T13:00:00.5Z"}`,
			decoded: core.Member{
				ID: "web-7d9f-xk2p", Addr: "10.0.0.7:8080", Meta: map[string]string{"zone": "a", "app": "x<y>&"},
				Draining: true, Slot: -1, JoinAt: noon, LeaveAt: noon.Add(time.Hour + 500*time.Millisecond),
			},
		},
		{
			name:    "leaveAt only, in another zone, with nanoseconds",
			member:  core.Member{ID: "pod-0", LeaveAt: noon.Add(time.Nanosecond).In(plus2)},
			json:    `{"v":1,"id":"pod-0","leaveAt":"2026-11-02T12:00:00.000000001Z"}`,
			decoded: core.Member{ID: "pod-0", Slot: -1, LeaveAt: noon.Add(time.Nanosecond)},
		},
		{
			name:    "joinAt in local time",
			member:  core.Member{ID: "a.b", JoinAt: time.Unix(noon.Unix(), 0)},
			json:    `{"v":1,"id":"a.b","joinAt":"2026-11-02T12:00:00Z"}`,
			decoded: core.Member{ID: "a.b", Slot: -1, JoinAt: noon},
		},
	}
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			got := encodeMember(v.member)
			if got != v.json {
				t.Errorf("encodeMember =\n  %s\nwant\n  %s", got, v.json)
			}
			m, err := decodeMember(v.member.ID, got)
			if err != nil {
				t.Fatalf("decodeMember: %v", err)
			}
			if !sameMember(m, v.decoded) {
				t.Errorf("decodeMember = %+v, want %+v", m, v.decoded)
			}
			for _, tm := range []time.Time{m.JoinAt, m.LeaveAt} {
				if !tm.IsZero() && tm.Location() != time.UTC {
					t.Errorf("decoded time %v is not in UTC", tm)
				}
			}
		})
	}

	t.Run("unknown fields and slot ignored", func(t *testing.T) {
		m, err := decodeMember("pod-0", `{"v":1,"id":"pod-0","slot":4,"weight":3,"loads":{"markets":{"n":8}},"addr":"a:1"}`)
		if err != nil {
			t.Fatal(err)
		}
		if want := (core.Member{ID: "pod-0", Addr: "a:1", Slot: -1}); !sameMember(m, want) {
			t.Errorf("decodeMember = %+v, want %+v", m, want)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		for _, tc := range []struct{ json, want string }{
			{`{"v":2,"id":"pod-0"}`, "k8s: member record: version 2, want 1"},
			{`{"id":"pod-0"}`, "k8s: member record: version 0, want 1"},
			{`{"v":1,"id":"pod-1"}`, `k8s: member record: id "pod-1", want "pod-0"`},
			{`{"v":1}`, `k8s: member record: id "", want "pod-0"`},
			{`{"v":1,"id":"pod-0","joinAt":"yesterday"}`, `k8s: member record: joinAt "yesterday" is not an RFC 3339 time`},
			{`{"v":1,"id":"pod-0","leaveAt":"2026-11-02"}`, `k8s: member record: leaveAt "2026-11-02" is not an RFC 3339 time`},
		} {
			if _, err := decodeMember("pod-0", tc.json); err == nil || err.Error() != tc.want {
				t.Errorf("decodeMember(%s) = %v, want %s", tc.json, err, tc.want)
			}
		}
		for _, s := range []string{"", "not json", `{"v":1,"id":"pod-0"`, `[1]`, `{"v":"1","id":"pod-0"}`, `{"v":1,"id":"pod-0","meta":{"a":1}}`} {
			_, err := decodeMember("pod-0", s)
			var syntax *json.SyntaxError
			var typ *json.UnmarshalTypeError
			isJSON := errors.As(err, &syntax) || errors.As(err, &typ)
			if err == nil || !strings.HasPrefix(err.Error(), "k8s: member record: ") || !isJSON {
				t.Errorf("decodeMember(%q) = %v, want a JSON error", s, err)
			}
		}
	})
}

// TestK8S060Annotations checks the encodings of tenure.term, tenure.session, tenure.confirm and
// tenure.slots, the labels of each kind of Lease and the foreign Lease check.
func TestK8S060Annotations(t *testing.T) {
	malformed := []string{"", "-1", "+1", "017", "1.5", "1e3", " 1", "x", "18446744073709551616"}

	t.Run("term", func(t *testing.T) {
		for term, want := range map[core.Term]string{0: "0", 1: "1", 1 << 40: "1099511627776", ^core.Term(0): "18446744073709551615"} {
			if got := encodeTerm(term); got != want {
				t.Errorf("encodeTerm(%d) = %q, want %q", term, got, want)
			}
			if got, ok := decodeTerm(map[string]string{annotationTerm: want}); !ok || got != term {
				t.Errorf("decodeTerm(%q) = %d, %t; want %d, true", want, got, ok, term)
			}
		}
		if got, ok := decodeTerm(map[string]string{"other": "5"}); !ok || got != 0 {
			t.Errorf("decodeTerm without the annotation = %d, %t; want 0, true", got, ok)
		}
		if got, ok := decodeTerm(nil); !ok || got != 0 {
			t.Errorf("decodeTerm(nil) = %d, %t; want 0, true", got, ok)
		}
		for _, s := range malformed {
			if got, ok := decodeTerm(map[string]string{annotationTerm: s}); ok || got != 0 {
				t.Errorf("decodeTerm(%q) = %d, %t; want 0, false", s, got, ok)
			}
		}
	})

	t.Run("session", func(t *testing.T) {
		for _, s := range []string{"6f1e2d3c-4b5a-4978-8796-a5b4c3d2e1f0", "6F1E2D3C-4B5A-4978-8796-A5B4C3D2E1F0", "00000000-0000-0000-0000-000000000000"} {
			if got, ok := decodeSession(map[string]string{annotationSession: s}); !ok || got != types.UID(s) {
				t.Errorf("decodeSession(%q) = %q, %t; want it, true", s, got, ok)
			}
		}
		if got, ok := decodeSession(nil); ok || got != "" {
			t.Errorf("decodeSession(nil) = %q, %t; want \"\", false", got, ok)
		}
		for _, s := range []string{
			"", "x", "6f1e2d3c4b5a49788796a5b4c3d2e1f0", "6f1e2d3c-4b5a-4978-8796-a5b4c3d2e1f", "6f1e2d3c-4b5a-4978-8796-a5b4c3d2e1f00",
			"6f1e2d3c-4b5a-4978-8796_a5b4c3d2e1f0", "6f1e2d3c-4b5a-4978-8796-a5b4c3d2e1fg", "6f1e2d3c-4b5a-4978-87966a5b4c3d2e1f0",
		} {
			if got, ok := decodeSession(map[string]string{annotationSession: s}); ok || got != "" {
				t.Errorf("decodeSession(%q) = %q, %t; want \"\", false", s, got, ok)
			}
		}
	})

	t.Run("confirm", func(t *testing.T) {
		for n, want := range map[uint64]string{0: "0", 3: "3", 1 << 63: "9223372036854775808"} {
			if got := encodeConfirm(n); got != want {
				t.Errorf("encodeConfirm(%d) = %q, want %q", n, got, want)
			}
			if got, ok := decodeConfirm(map[string]string{annotationConfirm: want}); !ok || got != n {
				t.Errorf("decodeConfirm(%q) = %d, %t; want %d, true", want, got, ok, n)
			}
		}
		if got, ok := decodeConfirm(nil); !ok || got != 0 {
			t.Errorf("decodeConfirm(nil) = %d, %t; want 0, true", got, ok)
		}
		for _, s := range malformed {
			if got, ok := decodeConfirm(map[string]string{annotationConfirm: s}); ok || got != 0 {
				t.Errorf("decodeConfirm(%q) = %d, %t; want 0, false", s, got, ok)
			}
		}
	})

	t.Run("slots", func(t *testing.T) {
		for n, want := range map[int]string{0: "0", 1: "1", 64: "64", maxSlots: "10000"} {
			if got := encodeSlots(n); got != want {
				t.Errorf("encodeSlots(%d) = %q, want %q", n, got, want)
			}
			if got, ok := decodeSlots(map[string]string{annotationSlots: want}); !ok || got != n {
				t.Errorf("decodeSlots(%q) = %d, %t; want %d, true", want, got, ok, n)
			}
		}
		if got, ok := decodeSlots(nil); ok || got != 0 {
			t.Errorf("decodeSlots(nil) = %d, %t; want 0, false", got, ok)
		}
		for _, s := range append(malformed, "10001", "99999999999999999999") {
			if got, ok := decodeSlots(map[string]string{annotationSlots: s}); ok || got != 0 {
				t.Errorf("decodeSlots(%q) = %d, %t; want 0, false", s, got, ok)
			}
		}
	})

	t.Run("labels", func(t *testing.T) {
		for _, tc := range []struct {
			kind, space string
			want        map[string]string
		}{
			{kindMember, "", map[string]string{"tenure.group": "g", "tenure.kind": "member"}},
			{kindShard, "markets", map[string]string{"tenure.group": "g", "tenure.kind": "shard", "tenure.space": "markets"}},
			{kindSlots, "markets", map[string]string{"tenure.group": "g", "tenure.kind": "slots"}},
			{kindSlot, "", map[string]string{"tenure.group": "g", "tenure.kind": "slot"}},
		} {
			if got := leaseLabels("g", tc.kind, tc.space); !maps.Equal(got, tc.want) {
				t.Errorf("leaseLabels(g, %s, %q) = %v, want %v", tc.kind, tc.space, got, tc.want)
			}
		}
	})

	t.Run("foreign lease", func(t *testing.T) {
		lease := func(labels map[string]string) *coordinationv1.Lease {
			return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "g-markets-1", Labels: labels}}
		}
		own := leaseLabels("g", kindShard, "markets")
		own["other"] = "x"
		if err := checkOwned(lease(own), "g", kindShard); err != nil {
			t.Errorf("checkOwned of an own Lease = %v, want nil", err)
		}
		const want = "k8s: lease g-markets-1 is not a shard lease of group g"
		for _, labels := range []map[string]string{
			nil,
			{"tenure.kind": "shard"},
			{"tenure.group": "g"},
			{"tenure.group": "h", "tenure.kind": "shard"},
			{"tenure.group": "g", "tenure.kind": "slot"},
		} {
			if err := checkOwned(lease(labels), "g", kindShard); err == nil || err.Error() != want {
				t.Errorf("checkOwned with labels %v = %v, want %s", labels, err, want)
			}
		}
		const wantMember = "k8s: lease g-member-pod-0 is not a member lease of group g"
		l := lease(map[string]string{"tenure.group": "g", "tenure.kind": "shard"})
		l.Name = "g-member-pod-0"
		if err := checkOwned(l, "g", kindMember); err == nil || err.Error() != wantMember {
			t.Errorf("checkOwned of a shard Lease as a member = %v, want %s", err, wantMember)
		}
	})
}

// TestK8S070ParseVersion checks the parsing of a Lease's resourceVersion.
func TestK8S070ParseVersion(t *testing.T) {
	for rv, want := range map[string]uint64{"0": 0, "1": 1, "17": 17, "18446744073709551615": ^uint64(0)} {
		if got, err := parseVersion("g-markets-1", rv); err != nil || got != want {
			t.Errorf("parseVersion(%q) = %d, %v; want %d, nil", rv, got, err, want)
		}
	}
	for _, rv := range []string{"", "abc", "-1", "+1", "017", "00", "1e3", " 1", "1 ", "0x10", "18446744073709551616"} {
		want := `k8s: lease g-markets-1: resourceVersion "` + rv + `" is not an integer`
		if got, err := parseVersion("g-markets-1", rv); err == nil || err.Error() != want || got != 0 {
			t.Errorf("parseVersion(%q) = %d, %v; want 0, %s", rv, got, err, want)
		}
	}
}
