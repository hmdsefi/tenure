// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

func TestMachineStrings(t *testing.T) {
	tests := []struct {
		v    fmt.Stringer
		want string
	}{
		{core.CauseNone, "none"},
		{core.CauseUnreachable, "unreachable"},
		{core.CauseLost, "lost"},
		{core.Cause(3), "Cause(3)"},
		{core.Cause(7), "Cause(7)"},
		{core.Cause(-1), "Cause(-1)"},

		{core.StopNone, "none"},
		{core.StopMoved, "moved"},
		{core.StopDraining, "draining"},
		{core.StopExpired, "expired"},
		{core.StopLost, "lost"},
		{core.StopReason(5), "StopReason(5)"},
		{core.StopReason(-1), "StopReason(-1)"},

		{core.ShardStateIdle, "idle"},
		{core.ShardStateAcquiring, "acquiring"},
		{core.ShardStateOwned, "owned"},
		{core.ShardStateDegraded, "degraded"},
		{core.ShardStateStopping, "stopping"},
		{core.ShardStateExpired, "expired"},
		{core.ShardStateLost, "lost"},
		{core.ShardStateReleasing, "releasing"},
		{core.ShardState(8), "ShardState(8)"},
		{core.ShardState(-1), "ShardState(-1)"},

		{core.SessionStateOpening, "opening"},
		{core.SessionStateLive, "live"},
		{core.SessionStateLate, "late"},
		{core.SessionStateLost, "lost"},
		{core.SessionStateClosing, "closing"},
		{core.SessionStateClosed, "closed"},
		{core.SessionState(6), "SessionState(6)"},
		{core.SessionState(-1), "SessionState(-1)"},
	}
	for _, tc := range tests {
		if got := tc.v.String(); got != tc.want {
			t.Errorf("%T(%d).String() = %q, want %q", tc.v, tc.v, got, tc.want)
		}
	}

	// The zero value of each enum, and StopReason's numbers: a later version adds a reason
	// after StopLost.
	t.Run("values", func(t *testing.T) {
		reasons := [...]core.StopReason{core.StopNone, core.StopMoved, core.StopDraining, core.StopExpired, core.StopLost}
		for i, r := range reasons {
			if int(r) != i {
				t.Errorf("%v = %d, want %d", r, int(r), i)
			}
		}
		if got := fmt.Sprint(core.Cause(0), core.ShardState(0), core.SessionState(0)); got != "none idle opening" {
			t.Errorf("zero values print %q, want %q", got, "none idle opening")
		}
	})

	// All 13 action types, in the form a digest of actions hashes.
	t.Run("actions", func(t *testing.T) {
		tests := []struct {
			a    core.Action
			want string
		}{
			{core.KeepAlive{}, "core.KeepAlive{}"},
			{core.ExtendDeadline{Deadline: 12 * time.Second}, "core.ExtendDeadline{Deadline:12s}"},
			{core.Acquire{Space: "a", Shard: 1}, "core.Acquire{Space:a Shard:1}"},
			{core.Release{Space: "a", Shard: 1, Term: 5}, "core.Release{Space:a Shard:1 Term:5}"},
			{core.StartRun{Space: "a", Shard: 1, Term: 5, Deadline: 12 * time.Second}, "core.StartRun{Space:a Shard:1 Term:5 Deadline:12s}"},
			{core.StopRun{Space: "a", Shard: 1, Term: 5, Reason: core.StopMoved}, "core.StopRun{Space:a Shard:1 Term:5 Reason:moved}"},
			{core.Confirm{Space: "a", Shard: 1, Term: 5}, "core.Confirm{Space:a Shard:1 Term:5}"},
			{core.Degrade{Space: "a", Shard: 1, Term: 5}, "core.Degrade{Space:a Shard:1 Term:5}"},
			{core.Restore{Space: "a", Shard: 1, Term: 5}, "core.Restore{Space:a Shard:1 Term:5}"},
			{core.ClaimSlot{}, "core.ClaimSlot{}"},
			{core.ReleaseSlot{Slot: 2}, "core.ReleaseSlot{Slot:2}"},
			{core.CloseSession{}, "core.CloseSession{}"},
			{core.Wake{At: 2 * time.Second}, "core.Wake{At:2s}"},
		}
		for _, tc := range tests {
			if got := fmt.Sprintf("%T%+v", tc.a, tc.a); got != tc.want {
				t.Errorf("action text = %q, want %q", got, tc.want)
			}
		}
	})
}
