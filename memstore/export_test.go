// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore

import "maps"

// SlotRecords returns a copy of the slot records of s, also those of holders whose session has
// ended, which no exported method shows.
func SlotRecords(s *Store) map[int]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.slots)
}
