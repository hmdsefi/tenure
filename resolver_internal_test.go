// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestResolverTimerFiresDuringUpdate checks that a timer callback that fires while a view is
// being taken, and so runs after that view re-armed the timer, arms no second timer: Close then
// returns at once instead of waiting for a timer the Resolver no longer knows about.
func TestResolverTimerFiresDuringUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		t0 := time.Now()
		r := newResolver("", core.NewRendezvous)
		v := core.View{Version: 1, Members: []Member{
			{ID: "n1", Addr: "a1", LeaveAt: t0.Add(time.Second)},
			{ID: "n2", Addr: "a2", LeaveAt: t0.Add(time.Hour)},
		}}
		r.update(v)

		r.mu.Lock() // the timer at T0+1s fires during the sleep, and its callback waits for mu
		time.Sleep(time.Second)
		r.view = v
		r.refreshLocked()
		r.mu.Unlock()
		synctest.Wait()

		if got := r.Members(); len(got) != 1 || got[0].ID != "n2" {
			t.Errorf("Members() at T0+1s = %+v, want only n2", got)
		}
		start := time.Now()
		if err := r.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("Close returned %v after it was called, want at once", d)
		}
	})
}
