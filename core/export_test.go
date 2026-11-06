// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

// MachineTiming returns the timing m keeps, for tests in package core_test.
func MachineTiming(m *Machine) Timing { return m.cfg.Timing }

// HighestStarted returns the highest started term of a shard, by the space's index in name order,
// for tests in package core_test. No run ends yet, so a test cannot reach a shard that acquires
// again after a run any other way.
func HighestStarted(m *Machine, space, shard int) *Term {
	return &m.shards[space][shard].highestStarted
}
