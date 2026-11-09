// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestKindTable checks each kind's level and message against the table of the design, written
// out here.
func TestKindTable(t *testing.T) {
	want := []struct {
		level slog.Level
		msg   string
	}{
		EventViewChanged:           {slog.LevelInfo, "tenure: view changed"},
		EventSessionOpened:         {slog.LevelInfo, "tenure: session opened"},
		EventSessionRenewFailed:    {slog.LevelWarn, "tenure: session renew failed"},
		EventSessionRenewedLate:    {slog.LevelWarn, "tenure: session renewed late"},
		EventSessionRecovered:      {slog.LevelInfo, "tenure: session recovered"},
		EventSessionExpired:        {slog.LevelError, "tenure: session expired"},
		EventSessionLost:           {slog.LevelError, "tenure: session lost"},
		EventSessionClosed:         {slog.LevelInfo, "tenure: session closed"},
		EventShardAcquiring:        {slog.LevelDebug, "tenure: shard acquiring"},
		EventShardAcquireFailed:    {slog.LevelDebug, "tenure: shard acquire failed"},
		EventShardAcquired:         {slog.LevelInfo, "tenure: shard acquired"},
		EventShardDegraded:         {slog.LevelWarn, "tenure: shard degraded"},
		EventShardRecovered:        {slog.LevelInfo, "tenure: shard recovered"},
		EventShardExpired:          {slog.LevelError, "tenure: shard expired"},
		EventShardLost:             {slog.LevelError, "tenure: shard lost"},
		EventShardStopping:         {slog.LevelInfo, "tenure: shard stopping"},
		EventShardStuck:            {slog.LevelWarn, "tenure: shard stuck"},
		EventShardReleasing:        {slog.LevelDebug, "tenure: shard releasing"},
		EventShardReleased:         {slog.LevelInfo, "tenure: shard released"},
		EventShardHandoffCompleted: {slog.LevelInfo, "tenure: shard handoff completed"},
		EventSlotClaimed:           {slog.LevelInfo, "tenure: slot claimed"},
		EventSlotReleased:          {slog.LevelInfo, "tenure: slot released"},
		EventSlotStandby:           {slog.LevelInfo, "tenure: slot standby"},
		EventSlotCountChanged:      {slog.LevelInfo, "tenure: slot count changed"},
		EventSlotCountMismatch:     {slog.LevelWarn, "tenure: slot count mismatch"},
		EventRunReturned:           {slog.LevelWarn, "tenure: run returned"},
		EventStoreFailed:           {slog.LevelWarn, "tenure: store failed"},
		EventMembershipFailed:      {slog.LevelWarn, "tenure: membership failed"},
		EventNodeDraining:          {slog.LevelInfo, "tenure: node draining"},
		EventNodeDrained:           {slog.LevelInfo, "tenure: node drained"},
		EventNodeClosed:            {slog.LevelInfo, "tenure: node closed"},
		EventSessionRestarted:      {slog.LevelInfo, "tenure: session restarted"},
	}
	if len(kinds) != 33 || len(want) != 33 {
		t.Fatalf("len(kinds) = %d and the test lists %d, want 33 (index 0 and 32 kinds)", len(kinds), len(want))
	}
	if kinds[0].name != "" || kinds[0].msg != "" || kinds[0].attrs != nil {
		t.Errorf("kinds[0] = %+v, want the zero kindInfo", kinds[0])
	}
	for k := EventViewChanged; k <= EventSessionRestarted; k++ {
		info := kinds[k]
		if info.level != want[k].level || info.msg != want[k].msg {
			t.Errorf("kind %v: level %v, msg %q; want %v, %q", k, info.level, info.msg, want[k].level, want[k].msg)
		}
		// The message rule: "tenure: " and the name with "." and "_" turned into spaces.
		if rule := "tenure: " + strings.NewReplacer(".", " ", "_", " ").Replace(info.name); info.msg != rule {
			t.Errorf("kind %v: msg %q, want %q from its name", k, info.msg, rule)
		}
	}
}

// wantAttr is one attribute eventAttrs should return for fullEvent.
type wantAttr struct {
	key  string
	kind slog.Kind
	val  any
}

// errBoom is fullEvent's error.
var errBoom = errors.New("boom")

// fullDuration is fullEvent's Duration.
const fullDuration = 2 * time.Second

// fullEvent returns an event of kind k whose fields all hold used values, each distinct, so an
// attribute's value shows which field it was read from.
func fullEvent(k EventKind) Event {
	return Event{
		Seq:        11,
		Time:       time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Kind:       k,
		Space:      "s",
		Shard:      3,
		Term:       7,
		Slot:       2,
		Reason:     "moved",
		Holder:     "b/2222",
		Duration:   fullDuration,
		Version:    9,
		Count:      4,
		Configured: 5,
		Err:        errBoom,
	}
}

func attrStr(key, v string) wantAttr { return wantAttr{key, slog.KindString, v} }

func attrInt(key string, v int64) wantAttr { return wantAttr{key, slog.KindInt64, v} }

func attrU64(key string, v uint64) wantAttr { return wantAttr{key, slog.KindUint64, v} }

// attrDur is a duration attribute holding fullEvent's Duration.
func attrDur(key string) wantAttr { return wantAttr{key, slog.KindDuration, fullDuration} }

// attrErr is fullEvent's error attribute.
var attrErr = wantAttr{"error", slog.KindAny, errBoom}

// TestEventAttrs checks, for every kind, the keys, their order, their value kinds and values,
// that error is absent when Err is nil, and that only store.failed leaves out space, shard and
// slot when they hold the unused value.
func TestEventAttrs(t *testing.T) {
	space, shard, term := attrStr("space", "s"), attrInt("shard", 3), attrU64("term", 7)
	reason := attrStr("reason", "moved")
	want := map[EventKind][]wantAttr{
		EventViewChanged:        {attrU64("version", 9), attrInt("members", 4)},
		EventSessionOpened:      nil,
		EventSessionRenewFailed: {attrErr},
		EventSessionRenewedLate: {attrDur("late_by")},
		EventSessionRecovered:   {attrInt("failures", 4)},
		EventSessionExpired:     nil,
		EventSessionLost:        {attrErr},
		EventSessionClosed:      nil,
		EventShardAcquiring:     {space, shard},
		EventShardAcquireFailed: {
			space, shard, attrStr("holder", "b/2222"), attrDur("retry_after"), attrErr,
		},
		EventShardAcquired:         {space, shard, term},
		EventShardDegraded:         {space, shard, term},
		EventShardRecovered:        {space, shard, term},
		EventShardExpired:          {space, shard, term},
		EventShardLost:             {space, shard, term},
		EventShardStopping:         {space, shard, term, reason},
		EventShardStuck:            {space, shard, term, reason},
		EventShardReleasing:        {space, shard, term},
		EventShardReleased:         {space, shard, term, reason},
		EventShardHandoffCompleted: {space, shard, term, reason, attrDur("took")},
		EventSlotClaimed:           {attrInt("slot", 2)},
		EventSlotReleased:          {attrInt("slot", 2), reason},
		EventSlotStandby:           nil,
		EventSlotCountChanged:      {attrInt("slots", 4)},
		EventSlotCountMismatch:     {attrInt("stored", 4), attrInt("configured", 5)},
		EventRunReturned: {
			space, shard, term, attrInt("restarts", 4), attrDur("restart_in"), attrErr,
		},
		EventStoreFailed: {
			attrStr("op", "moved"), space, shard, attrInt("slot", 2), attrErr,
		},
		EventMembershipFailed: {attrStr("op", "moved"), attrErr},
		EventNodeDraining:     nil,
		EventNodeDrained:      {attrDur("took")},
		EventNodeClosed:       {reason},
		EventSessionRestarted: {attrStr("previous", "b/2222")},
	}
	if len(want) != 32 {
		t.Fatalf("test lists %d kinds, want 32", len(want))
	}

	for k := EventViewChanged; k <= EventSessionRestarted; k++ {
		full, ok := want[k]
		if !ok {
			t.Fatalf("test lists no attributes for %v", k)
		}
		t.Run(k.String(), func(t *testing.T) {
			e := fullEvent(k)
			checkAttrs(t, "all fields set", eventAttrs(e), full)

			e.Err = nil
			checkAttrs(t, "Err nil", eventAttrs(e), without(full, "error"))

			// Unused space, shard and slot: left out for store.failed only.
			for _, tc := range []struct {
				key string
				set func(*Event)
			}{
				{"space", func(e *Event) { e.Space = "" }},
				{"shard", func(e *Event) { e.Shard = -1 }},
				{"slot", func(e *Event) { e.Slot = -1 }},
			} {
				e := fullEvent(k)
				tc.set(&e)
				var exp []wantAttr
				if k == EventStoreFailed {
					exp = without(full, tc.key)
				} else {
					exp = unused(full, tc.key)
				}
				checkAttrs(t, "unused "+tc.key, eventAttrs(e), exp)
			}

			e = fullEvent(k)
			e.Space, e.Shard, e.Slot, e.Err = "", -1, -1, nil
			exp := without(full, "error")
			if k == EventStoreFailed {
				exp = without(exp, "space")
				exp = without(exp, "shard")
				exp = without(exp, "slot")
			} else {
				for _, key := range []string{"space", "shard", "slot"} {
					exp = unused(exp, key)
				}
			}
			checkAttrs(t, "all unused, Err nil", eventAttrs(e), exp)
		})
	}

	for _, k := range []EventKind{0, 33, 255} {
		if got := eventAttrs(Event{Kind: k, Err: errBoom}); got != nil {
			t.Errorf("eventAttrs(Event{Kind: %d}) = %v, want nil", int(k), got)
		}
	}
}

// without returns attrs without the attribute key.
func without(attrs []wantAttr, key string) []wantAttr {
	var out []wantAttr
	for _, a := range attrs {
		if a.key != key {
			out = append(out, a)
		}
	}
	return out
}

// unused returns attrs with the space, shard or slot attribute key holding its unused value.
func unused(attrs []wantAttr, key string) []wantAttr {
	out := make([]wantAttr, len(attrs))
	copy(out, attrs)
	for i, a := range out {
		if a.key != key {
			continue
		}
		switch key {
		case "space":
			out[i].val = ""
		case "shard", "slot":
			out[i].val = int64(-1)
		}
	}
	return out
}

// checkAttrs compares got with want: keys in order, value kinds and values.
func checkAttrs(t *testing.T, name string, got []slog.Attr, want []wantAttr) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d attributes %v, want %d %v", name, len(got), got, len(want), want)
		return
	}
	for i, a := range got {
		w := want[i]
		if a.Key != w.key {
			t.Errorf("%s: attribute %d has key %q, want %q", name, i, a.Key, w.key)
			continue
		}
		if a.Value.Kind() != w.kind {
			t.Errorf("%s: %q has kind %v, want %v", name, a.Key, a.Value.Kind(), w.kind)
			continue
		}
		if v := a.Value.Any(); v != w.val {
			t.Errorf("%s: %q = %v (%T), want %v (%T)", name, a.Key, v, v, w.val, w.val)
		}
	}
}
