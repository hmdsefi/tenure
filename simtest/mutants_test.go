// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build tenure_mutants

package simtest

import (
	"testing"

	"github.com/hmdsefi/faultline"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The planted bugs (SIM-660, AT-SIM-37). Each TestMutant<Name> runs one seed of a world that
// the bug <Name> breaks, and must fail: it passes only when the protocol is broken. They build
// only with the tag tenure_mutants, which plants the bugs; TestMutantsCaught runs each in a
// child process and checks which invariant fails.

// TestMutantGrantHeld plants GrantHeld into TestSteady's row under the Server rule: the store
// grants a shard that another member's live session holds, as members joining at the start ask
// for the shards the first one took. It must fail tenure/g4. The row is the table's own, so
// the bug comes from world.SetMutationForTest, not from the row's scenario.
func TestMutantGrantHeld(t *testing.T) {
	t.Cleanup(world.SetMutationForTest(world.GrantHeld))
	r := rowNamed(t, "TestSteady")
	opts := r.options()
	opts.Seeds = 1
	faultline.Run(t, opts, r.body(world.Server, nil))
}

// TestMutantSkipRaise plants SkipRaise into a world of three members with a fenced space,
// where m1 is cut off from the store from 10 s to 40 s (cutRow): its runs degrade and go on writing,
// the others take its shards and write in newer terms without raising the fence, and the fence
// accepts m1's older writes after theirs. It must fail tenure/g3.
func TestMutantSkipRaise(t *testing.T) {
	runCutMutant(t, world.SkipRaise, true)
}

// TestMutantLateDeadline plants LateDeadline into a world of three members with an unfenced
// space, where m1 is cut off from the store from 10 s to 40 s (cutRow): m1's machine keeps its shards
// RenewDeadline past the deadline its renewals allow, while the others take them once the
// store has ended its session. It must fail tenure/g2.
func TestMutantLateDeadline(t *testing.T) {
	runCutMutant(t, world.LateDeadline, false)
}

// TestMutantReadConfirm plants ReadConfirm into TestConfirmRace's world: m1's confirm in the
// race's window writes nothing, so m2's cas finds the version it read and takes the shard while
// m1, restored by the confirm's answer, still owns it. It must fail tenure/g2.
func TestMutantReadConfirm(t *testing.T) {
	r := raceRow
	r.sc.Mutation = world.ReadConfirm
	r.seeds = 1
	runRace(t, r)
}

// runCutMutant runs one seed of cutRow's world with the bug m planted through the scenario's
// Mutation: three members under the Server rule, the space book of 8 shards, fenced or not, and
// m1 cut off from the store both ways from 10 s to 40 s. TestUnplantedWorlds runs the same
// worlds without a bug, where every check holds.
func runCutMutant(t *testing.T, m world.Mutation, fenced bool) {
	t.Helper()
	r := cutRow(t.Name(), m, fenced)
	r.seeds = 1
	faultline.Run(t, r.options(), r.body(world.Server, cutSetup))
}
