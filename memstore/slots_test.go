// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/memstore"
)

var _ tenure.SlotStore = (*memstore.Store)(nil)

func claimSlot(t *testing.T, s *memstore.Store, h string, want int) {
	t.Helper()
	if got, err := s.ClaimSlot(ctx, h); got != want || err != nil {
		t.Fatalf("ClaimSlot(%q) = %d, %v, want %d, nil", h, got, err, want)
	}
}

func wantNoSlot(t *testing.T, s *memstore.Store, h string) {
	t.Helper()
	if got, err := s.ClaimSlot(ctx, h); got != -1 || !errors.Is(err, tenure.ErrNoSlot) {
		t.Fatalf("ClaimSlot(%q) = %d, %v, want -1, %v", h, got, err, tenure.ErrNoSlot)
	}
}

func setSlots(t *testing.T, s *memstore.Store, count int) {
	t.Helper()
	if err := s.SetSlots(ctx, count); err != nil {
		t.Fatalf("SetSlots(%d) = %v, want nil", count, err)
	}
}

func releaseSlot(t *testing.T, s *memstore.Store, h string, slot int) {
	t.Helper()
	if err := s.ReleaseSlot(ctx, h, slot); err != nil {
		t.Fatalf("ReleaseSlot(%q, %d) = %v, want nil", h, slot, err)
	}
}

// wantSlots checks that Slots returns count and holders.
func wantSlots(t *testing.T, s *memstore.Store, count int, holders []string) {
	t.Helper()
	gotCount, gotHolders, err := s.Slots(ctx)
	if gotCount != count || !slices.Equal(gotHolders, holders) || err != nil {
		t.Fatalf("Slots() = %d, %q, %v, want %d, %q, nil", gotCount, gotHolders, err, count, holders)
	}
}

func TestSlotRules(t *testing.T) {
	t.Run("no slots", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		if count, holders, err := s.Slots(ctx); count != 0 || holders != nil || err != nil {
			t.Errorf("Slots() = %d, %q, %v, want 0, nil, nil", count, holders, err)
		}
		wantNoSlot(t, s, h1)
	})

	t.Run("lowest free slot, held slot returned again", func(t *testing.T) {
		s, _ := newStore(t)
		setSlots(t, s, 3)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		keepAlive(t, s, h3, timing)
		claimSlot(t, s, h1, 0)
		claimSlot(t, s, h2, 1)
		claimSlot(t, s, h1, 0)
		releaseSlot(t, s, h1, 0)
		claimSlot(t, s, h3, 0)
		claimSlot(t, s, h1, 2)
		wantSlots(t, s, 3, []string{h3, h2, h1})
		h4 := holder(4, 1)
		keepAlive(t, s, h4, timing)
		wantNoSlot(t, s, h4)
	})

	t.Run("session needed", func(t *testing.T) {
		s, _ := newStore(t)
		setSlots(t, s, 2)
		if got, err := s.ClaimSlot(ctx, h1); got != -1 || !errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("ClaimSlot(unknown) = %d, %v, want -1, %v", got, err, tenure.ErrSessionLost)
		}
		wantSlots(t, s, 2, []string{"", ""})
	})

	t.Run("slot of an ended session is free", func(t *testing.T) {
		s, c := newStore(t)
		setSlots(t, s, 2)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		claimSlot(t, s, h1, 0)
		claimSlot(t, s, h2, 1)
		c.advance(lease - time.Millisecond)
		keepAlive(t, s, h2, timing)
		wantSlots(t, s, 2, []string{h1, h2})
		c.advance(time.Millisecond)
		wantSlots(t, s, 2, []string{"", h2})
		keepAlive(t, s, h3, timing)
		claimSlot(t, s, h3, 0)
		if _, err := s.ClaimSlot(ctx, h1); !errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("ClaimSlot(h1) after expiry = %v, want %v", err, tenure.ErrSessionLost)
		}
	})

	t.Run("replaced incarnation frees its slot", func(t *testing.T) {
		s, _ := newStore(t)
		setSlots(t, s, 1)
		keepAlive(t, s, h1, timing)
		claimSlot(t, s, h1, 0)
		next := holder(1, 2)
		keepAlive(t, s, next, timing)
		wantSlots(t, s, 1, []string{""})
		claimSlot(t, s, next, 0)
	})

	t.Run("retired slots stay held", func(t *testing.T) {
		s, _ := newStore(t)
		setSlots(t, s, 3)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		keepAlive(t, s, h3, timing)
		claimSlot(t, s, h1, 0)
		claimSlot(t, s, h2, 1)
		claimSlot(t, s, h3, 2)
		setSlots(t, s, 1)
		wantSlots(t, s, 1, []string{h1})
		wantNoSlot(t, s, h3)
		claimSlot(t, s, h1, 0)

		// A holder of a retired slot gets no slot below the count, even a free one.
		releaseSlot(t, s, h1, 0)
		wantNoSlot(t, s, h3)
		wantSlots(t, s, 1, []string{""})
		claimSlot(t, s, h1, 0)
		setSlots(t, s, 3)
		wantSlots(t, s, 3, []string{h1, h2, h3})
		claimSlot(t, s, h3, 2)
	})

	t.Run("ReleaseSlot compares the holder", func(t *testing.T) {
		s, c := newStore(t)
		setSlots(t, s, 2)
		keepAlive(t, s, h1, timing)
		claimSlot(t, s, h1, 0)
		releaseSlot(t, s, h2, 0)
		releaseSlot(t, s, h1, 1)
		releaseSlot(t, s, h1, 9999)
		wantSlots(t, s, 2, []string{h1, ""})

		// ReleaseSlot does not look at the session: an ended holder still releases its slot.
		c.advance(lease)
		keepAlive(t, s, h2, timing)
		releaseSlot(t, s, h1, 0)
		claimSlot(t, s, h2, 0)
		releaseSlot(t, s, h1, 0)
		wantSlots(t, s, 2, []string{h2, ""})
	})

	t.Run("CloseSession clears slot records, retired ones too", func(t *testing.T) {
		s, _ := newStore(t)
		setSlots(t, s, 2)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		claimSlot(t, s, h1, 0)
		claimSlot(t, s, h2, 1)
		setSlots(t, s, 1)
		if err := s.CloseSession(ctx, h2); err != nil {
			t.Fatalf("CloseSession(h2) = %v, want nil", err)
		}
		keepAlive(t, s, h3, timing)
		setSlots(t, s, 2)
		wantSlots(t, s, 2, []string{h1, ""})
		claimSlot(t, s, h3, 1)
	})

	t.Run("slot writes take no version", func(t *testing.T) {
		s, _ := newStore(t)
		before := version(t, s)
		setSlots(t, s, 2)
		keepAlive(t, s, h1, timing)
		claimSlot(t, s, h1, 0)
		releaseSlot(t, s, h1, 0)
		claimSlot(t, s, h1, 0)
		if err := s.CloseSession(ctx, h1); err != nil {
			t.Fatal(err)
		}
		if got := version(t, s); got != before+1 {
			t.Errorf("next version = %d, want %d: a slot write took a version", got, before+1)
		}
	})
}

// TestATSTO02SlotsNoEffect checks that a slot call returning an error changes nothing: a done
// context, a bad argument, a lost session or no free slot leaves the slot count and records as
// they were (AT-STO-02, slot and watch calls; steps written from issue #36).
func TestATSTO02SlotsNoEffect(t *testing.T) {
	s, _ := newStore(t)
	setSlots(t, s, 2)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	claimSlot(t, s, h1, 0)
	claimSlot(t, s, h2, 1)
	setSlots(t, s, 1)

	done, cancel := context.WithCancel(ctx)
	cancel()
	calls := []struct {
		name string
		call func() error
	}{
		{"ClaimSlot(done)", func() error { _, err := s.ClaimSlot(done, h3); return err }},
		{"ClaimSlot(bad holder)", func() error { _, err := s.ClaimSlot(ctx, ""); return err }},
		{"ClaimSlot(no session)", func() error { _, err := s.ClaimSlot(ctx, h3); return err }},
		{"ClaimSlot(retired)", func() error { _, err := s.ClaimSlot(ctx, h2); return err }},
		{"ReleaseSlot(done)", func() error { return s.ReleaseSlot(done, h1, 0) }},
		{"ReleaseSlot(bad holder)", func() error { return s.ReleaseSlot(ctx, "/x", 0) }},
		{"ReleaseSlot(slot 10000)", func() error { return s.ReleaseSlot(ctx, h1, 10000) }},
		{"ReleaseSlot(slot -1)", func() error { return s.ReleaseSlot(ctx, h1, -1) }},
		{"Slots(done)", func() error { _, _, err := s.Slots(done); return err }},
		{"SetSlots(done)", func() error { return s.SetSlots(done, 5) }},
		{"SetSlots(-1)", func() error { return s.SetSlots(ctx, -1) }},
		{"SetSlots(10001)", func() error { return s.SetSlots(ctx, 10001) }},
		{"WatchHolders(done)", func() error { _, err := s.WatchHolders(done); return err }},
	}
	for _, cl := range calls {
		if err := cl.call(); err == nil {
			t.Errorf("%s = nil, want an error", cl.name)
		}
	}

	wantSlots(t, s, 1, []string{h1})
	if _, err := s.ClaimSlot(ctx, h3); !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("ClaimSlot(h3) = %v, want %v: a failed call opened a session", err, tenure.ErrSessionLost)
	}
	setSlots(t, s, 2)
	wantSlots(t, s, 2, []string{h1, h2})
}

// TestATSTO04SlotMessages checks the error of every slot and watch call: ctx.Err() itself for a
// done context, then the first bad argument in parameter order with its exact text, then the
// session and slot errors (AT-STO-04, slot and watch calls; steps written from issue #36).
func TestATSTO04SlotMessages(t *testing.T) {
	s, _ := newStore(t)
	setSlots(t, s, 1)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	claimSlot(t, s, h1, 0)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancel2 := context.WithDeadline(ctx, time.Unix(0, 0))
	defer cancel2()

	tests := []struct {
		name string
		call func() error
		want string
	}{
		{"ClaimSlot canceled", func() error { _, err := s.ClaimSlot(canceled, ""); return err }, context.Canceled.Error()},
		{"ClaimSlot expired", func() error { _, err := s.ClaimSlot(expired, ""); return err }, context.DeadlineExceeded.Error()},
		{"ClaimSlot empty holder", func() error { _, err := s.ClaimSlot(ctx, ""); return err }, "memstore: empty holder"},
		{"ClaimSlot zero byte", func() error { _, err := s.ClaimSlot(ctx, "m1\x00"); return err }, `memstore: holder "m1\x00" contains a zero byte`},
		{"ClaimSlot empty member", func() error { _, err := s.ClaimSlot(ctx, "/1"); return err }, `memstore: holder "/1" has an empty member ID`},
		{"ClaimSlot no session", func() error { _, err := s.ClaimSlot(ctx, h3); return err }, tenure.ErrSessionLost.Error()},
		{"ClaimSlot none free", func() error { _, err := s.ClaimSlot(ctx, h2); return err }, tenure.ErrNoSlot.Error()},

		{"ReleaseSlot canceled", func() error { return s.ReleaseSlot(canceled, "", -1) }, context.Canceled.Error()},
		{"ReleaseSlot empty holder", func() error { return s.ReleaseSlot(ctx, "", -1) }, "memstore: empty holder"},
		{"ReleaseSlot empty member", func() error { return s.ReleaseSlot(ctx, "/", -1) }, `memstore: holder "/" has an empty member ID`},
		{"ReleaseSlot negative", func() error { return s.ReleaseSlot(ctx, h1, -1) }, "memstore: slot -1 is outside [0, 9999]"},
		{"ReleaseSlot too large", func() error { return s.ReleaseSlot(ctx, h1, 10000) }, "memstore: slot 10000 is outside [0, 9999]"},

		{"Slots canceled", func() error { _, _, err := s.Slots(canceled); return err }, context.Canceled.Error()},

		{"SetSlots canceled", func() error { return s.SetSlots(canceled, -1) }, context.Canceled.Error()},
		{"SetSlots negative", func() error { return s.SetSlots(ctx, -1) }, "memstore: slot count -1 is outside [0, 10000]"},
		{"SetSlots too large", func() error { return s.SetSlots(ctx, 10001) }, "memstore: slot count 10001 is outside [0, 10000]"},

		{"WatchHolders canceled", func() error { _, err := s.WatchHolders(canceled); return err }, context.Canceled.Error()},
		{"WatchHolders expired", func() error { _, err := s.WatchHolders(expired); return err }, context.DeadlineExceeded.Error()},
	}
	for _, tt := range tests {
		err := tt.call()
		if err == nil || err.Error() != tt.want {
			t.Errorf("%s = %v, want %s", tt.name, err, tt.want)
		}
	}

	// A done context's error is ctx.Err() itself, and the failed calls return no value.
	if got, err := s.ClaimSlot(canceled, h1); got != -1 || err != context.Canceled {
		t.Errorf("ClaimSlot(canceled) = %d, %v, want -1, context.Canceled itself", got, err)
	}
	if count, holders, err := s.Slots(expired); count != 0 || holders != nil || err != context.DeadlineExceeded {
		t.Errorf("Slots(expired) = %d, %q, %v, want 0, nil, context.DeadlineExceeded itself", count, holders, err)
	}
	if ch, err := s.WatchHolders(canceled); ch != nil || err != context.Canceled {
		t.Errorf("WatchHolders(canceled) = %v, %v, want nil, context.Canceled itself", ch, err)
	}

	// The boundaries are accepted.
	releaseSlot(t, s, h1, 9999)
	setSlots(t, s, 10000)
	setSlots(t, s, 0)
}
