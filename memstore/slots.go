// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/hmdsefi/tenure"
)

const (
	// maxSlot is the largest slot ReleaseSlot accepts.
	maxSlot = 9999

	// maxSlotCount is the largest count SetSlots accepts.
	maxSlotCount = 10000
)

// ClaimSlot returns the lowest slot below the count whose record names holder, without writing.
// When holder holds no slot below the count but one at or above it, it returns -1 and
// tenure.ErrNoSlot. Otherwise it claims the lowest slot below the count that has no record or a
// holder whose session is not live, or returns -1 and tenure.ErrNoSlot when there is none. It
// returns tenure.ErrSessionLost when holder has no live session. A slot write takes no record
// version and is not delivered to watchers.
func (s *Store) ClaimSlot(ctx context.Context, holder string) (int, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if err := checkHolder(holder); err != nil {
		return -1, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	if sess := s.sessions[holder]; sess == nil || !s.live(sess, now) {
		return -1, tenure.ErrSessionLost
	}
	retired := false
	for _, slot := range slices.Sorted(maps.Keys(s.slots)) {
		if s.slots[slot] != holder {
			continue
		}
		if slot < s.slotCount {
			return slot, nil
		}
		retired = true
	}
	if retired {
		return -1, tenure.ErrNoSlot
	}
	for slot := range s.slotCount {
		if s.slotFree(slot, now) {
			s.slots[slot] = holder
			return slot, nil
		}
	}
	return -1, tenure.ErrNoSlot
}

// ReleaseSlot clears the slot's record when it names holder. Any other record, or none, is left
// as it is, and ReleaseSlot still returns nil. It does not look at the session of holder.
func (s *Store) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkHolder(holder); err != nil {
		return err
	}
	if slot < 0 || slot > maxSlot {
		return fmt.Errorf("memstore: slot %d is outside [0, %d]", slot, maxSlot)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slots[slot] == holder {
		delete(s.slots, slot)
	}
	return nil
}

// Slots returns the slot count and, in a new slice, one entry per slot below it: the holder the
// slot's record names while that holder's session is live, else "". When the count is 0 it
// returns 0, nil, nil.
func (s *Store) Slots(ctx context.Context) (count int, holders []string, err error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slotCount == 0 {
		return 0, nil, nil
	}
	now := s.tick()
	holders = make([]string, s.slotCount)
	for slot := range holders {
		if !s.slotFree(slot, now) {
			holders[slot] = s.slots[slot]
		}
	}
	return s.slotCount, holders, nil
}

// SetSlots sets the slot count. It changes no slot record, so the holder of a slot at or above
// the new count keeps it until it releases the slot or its session is closed.
func (s *Store) SetSlots(ctx context.Context, count int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if count < 0 || count > maxSlotCount {
		return fmt.Errorf("memstore: slot count %d is outside [0, %d]", count, maxSlotCount)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.slotCount = count
	return nil
}

// slotFree reports whether slot has no record or names a holder whose session is not live at
// now.
func (s *Store) slotFree(slot int, now time.Duration) bool {
	h, ok := s.slots[slot]
	if !ok {
		return true
	}
	sess := s.sessions[h]
	return sess == nil || !s.live(sess, now)
}
