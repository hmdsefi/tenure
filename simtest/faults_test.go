// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import "testing"

// The fault scenarios of SIM-600 (AT-SIM-31), each a row of the scenario table run under both
// rules at its seed count, and with FAULTLINE_CHECK_DETERMINISM=1 every seed twice with equal
// trace hashes. Every invariant holds after every event (G2 only where the row's G2 column says
// the timing bound holds), and the finals hold at the end: every shard has an owner once the
// bound has passed after the faults (except beyond the drift bound), and the members agree on
// the placement and each shard runs on its assignee.

// TestCrashRestart crashes members every 10 s on average, for 1 s to 20 s each, at most two
// at once.
func TestCrashRestart(t *testing.T) { runRow(t, rowNamed(t, "TestCrashRestart"), nil) }

// TestPartitions splits the nodes into two groups every 15 s on average, for up to 20 s, and
// slows the link between the store and a member, both ways, to 100 ms plus up to 50 ms of
// jitter every 10 s, for up to 15 s; the bound counts a one-way delay of 150 ms. The link rule
// targets the store, not the members: faultline draws a link's peer from the servers, the
// members, which send each other nothing, so a rule on the members would delay no message.
func TestPartitions(t *testing.T) { runRow(t, rowNamed(t, "TestPartitions"), nil) }

// TestDrift draws initial drifts of up to 10 % and sets a member's drift to up to 15 % either
// way every 10 s: within the timing's drift bound of 20 %, so G2 is asserted.
func TestDrift(t *testing.T) { runRow(t, rowNamed(t, "TestDrift"), nil) }

// TestDriftBeyondBound sets a member's drift to up to 40 % either way every 10 s: beyond the
// drift bound, so only the safety that does not rest on timing is asserted (G1, G3, G4 and
// monotonic readings), with no G2 and no liveness final.
func TestDriftBeyondBound(t *testing.T) { runRow(t, rowNamed(t, "TestDriftBeyondBound"), nil) }

// TestPauses pauses members every 10 s on average, for 1 s to 20 s each, at most two at once.
// A paused member cannot stop its runs at its deadline, so G2 is not asserted.
func TestPauses(t *testing.T) { runRow(t, rowNamed(t, "TestPauses"), nil) }

// TestWallClock gives the members wall clocks up to an hour off, which step three times each
// by up to 10 min either way (SIM-110). Wall time decides only the terms, so G2 holds.
func TestWallClock(t *testing.T) { runRow(t, rowNamed(t, "TestWallClock"), nil) }

// TestStoreFaults crashes the store every 20 s on average, for up to 10 s, and cuts it off
// from every other node every 30 s on average, for up to 10 s.
func TestStoreFaults(t *testing.T) { runRow(t, rowNamed(t, "TestStoreFaults"), nil) }
