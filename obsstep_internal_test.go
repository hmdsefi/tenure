// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestStepCollectorInputs checks the step collector on single steps: an input the machine ignores
// emits nothing, but a stepped view still emits view.changed, and shard.acquire_failed carries
// the step's error unless it is a *HeldError.
func TestStepCollectorInputs(t *testing.T) {
	status := func(now time.Duration, state core.ShardState) core.Status {
		return core.Status{Now: now, Session: core.SessionStateLive, Deadline: time.Minute, Slot: -1,
			ViewVersion: 3, Spaces: []core.SpaceStatus{{Name: "s", Shards: []core.ShardStatus{{State: state}}}}}
	}
	idle, acquiring := status(time.Second, core.ShardStateIdle), status(time.Second, core.ShardStateAcquiring)
	later := status(2*time.Second, core.ShardStateIdle)
	later.Spaces[0].Shards[0].RetryAt = 4 * time.Second
	failed := core.AcquireFailed{Now: 2 * time.Second, Space: "s", Shard: 0, Holder: "b/2", RetryAfter: 3 * time.Second}
	boom := errors.New("boom")
	acquireFailed := func(err error) Event {
		return Event{Kind: EventShardAcquireFailed, Space: "s", Shard: 0, Slot: -1, Holder: "b/2",
			Duration: 3 * time.Second, Err: err}
	}
	wake := []core.Action{core.Wake{At: 4 * time.Second}}

	for _, tc := range []struct {
		name string
		rec  stepRecord
		want []Event
	}{
		{
			name: "IgnoredAnswer",
			rec: stepRecord{Now: 2 * time.Second, Input: failed, Before: idle,
				After: status(2*time.Second, core.ShardStateIdle), Actions: wake, Err: boom},
		},
		{
			name: "IgnoredView",
			rec: stepRecord{Now: 2 * time.Second, Before: idle, After: status(2*time.Second, core.ShardStateIdle),
				Input: core.ViewChanged{View: core.View{Version: 2, Members: []core.Member{{ID: "a"}, {ID: "b"}}}}},
			want: []Event{{Kind: EventViewChanged, Shard: -1, Slot: -1, Version: 2, Count: 2}},
		},
		{
			name: "HeldError",
			rec: stepRecord{Now: 2 * time.Second, Input: failed, Before: acquiring, After: later, Actions: wake,
				Err: &HeldError{Holder: "b/2", RetryAfter: 3 * time.Second}},
			want: []Event{acquireFailed(nil)},
		},
		{
			name: "OtherError",
			rec:  stepRecord{Now: 2 * time.Second, Input: failed, Before: acquiring, After: later, Actions: wake, Err: boom},
			want: []Event{acquireFailed(boom)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []Event
			c := newStepCollector([]Space{{Name: "s", Shards: 1}}, func(e Event) uint64 {
				got = append(got, e)
				return uint64(len(got))
			})
			c.collect(tc.rec)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("events = %+v, want %+v", got, tc.want)
			}
		})
	}
}
