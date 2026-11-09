// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"testing"

	"github.com/hmdsefi/tenure"
)

// TestATOBS01Kinds checks the event kinds' values and names. Its steps were written from the
// issue's text, because the original acceptance-test text is unavailable.
func TestATOBS01Kinds(t *testing.T) {
	names := []string{
		"view.changed",
		"session.opened",
		"session.renew_failed",
		"session.renewed_late",
		"session.recovered",
		"session.expired",
		"session.lost",
		"session.closed",
		"shard.acquiring",
		"shard.acquire_failed",
		"shard.acquired",
		"shard.degraded",
		"shard.recovered",
		"shard.expired",
		"shard.lost",
		"shard.stopping",
		"shard.stuck",
		"shard.releasing",
		"shard.released",
		"shard.handoff_completed",
		"slot.claimed",
		"slot.released",
		"slot.standby",
		"slot.count_changed",
		"slot.count_mismatch",
		"run.returned",
		"store.failed",
		"membership.failed",
		"node.draining",
		"node.drained",
		"node.closed",
		"session.restarted",
	}
	kinds := []tenure.EventKind{
		tenure.EventViewChanged,
		tenure.EventSessionOpened,
		tenure.EventSessionRenewFailed,
		tenure.EventSessionRenewedLate,
		tenure.EventSessionRecovered,
		tenure.EventSessionExpired,
		tenure.EventSessionLost,
		tenure.EventSessionClosed,
		tenure.EventShardAcquiring,
		tenure.EventShardAcquireFailed,
		tenure.EventShardAcquired,
		tenure.EventShardDegraded,
		tenure.EventShardRecovered,
		tenure.EventShardExpired,
		tenure.EventShardLost,
		tenure.EventShardStopping,
		tenure.EventShardStuck,
		tenure.EventShardReleasing,
		tenure.EventShardReleased,
		tenure.EventShardHandoffCompleted,
		tenure.EventSlotClaimed,
		tenure.EventSlotReleased,
		tenure.EventSlotStandby,
		tenure.EventSlotCountChanged,
		tenure.EventSlotCountMismatch,
		tenure.EventRunReturned,
		tenure.EventStoreFailed,
		tenure.EventMembershipFailed,
		tenure.EventNodeDraining,
		tenure.EventNodeDrained,
		tenure.EventNodeClosed,
		tenure.EventSessionRestarted,
	}
	if len(kinds) != 32 || len(names) != 32 {
		t.Fatalf("test lists %d kinds and %d names, want 32 each", len(kinds), len(names))
	}

	if tenure.EventViewChanged != 1 {
		t.Errorf("EventViewChanged = %d, want 1", tenure.EventViewChanged)
	}
	if tenure.EventNodeClosed != 31 {
		t.Errorf("EventNodeClosed = %d, want 31", tenure.EventNodeClosed)
	}
	if tenure.EventSessionRestarted != 32 {
		t.Errorf("EventSessionRestarted = %d, want 32", tenure.EventSessionRestarted)
	}
	for i, k := range kinds {
		if int(k) != i+1 {
			t.Errorf("kind %q = %d, want %d", names[i], k, i+1)
		}
		if got := k.String(); got != names[i] {
			t.Errorf("EventKind(%d).String() = %q, want %q", int(k), got, names[i])
		}
	}

	for _, tc := range []struct {
		k    tenure.EventKind
		want string
	}{
		{0, "EventKind(0)"},
		{33, "EventKind(33)"},
		{255, "EventKind(255)"},
	} {
		if got := tc.k.String(); got != tc.want {
			t.Errorf("EventKind(%d).String() = %q, want %q", int(tc.k), got, tc.want)
		}
	}
}
