// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build tenure_mutants

package world

// Planted bugs (SIM-660). A build with the tag tenure_mutants can plant four bugs into the
// protocol, each where one hook asks plants:
//
//   - GrantHeld, in the store model's acquire (storemodel.go): the store grants a shard whose
//     record names another holder whose session is live, as if that session had ended;
//   - SkipRaise, in a run's call (run.go): the run writes to the fence without raising it to
//     its term first;
//   - LateDeadline, in the driver's machine (driver.go): the machine is configured with twice
//     the scenario's RenewDeadline, so it places its session deadline, and reports it to its
//     runs, at a renewal plus 2·RenewDeadline (its LeaseDuration grows by the same
//     RenewDeadline, which keeps the timing valid; the store still gets the scenario's);
//   - ReadConfirm, in the store model's confirm (storemodel.go): under the Observer rule a
//     confirmReq only reads the record, as under the Server rule, and gives it no new version.
//
// A world plants the bug its scenario's Mutation names, or the one SetMutationForTest names.
// A build without the tag compiles every hook away (mutants_off.go).

// plantedForTest is the bug SetMutationForTest plants in every world built after it, on top
// of each scenario's own Mutation; MutationNone plants nothing.
//
// It is the one package-level variable of the package, an exception to the rule that a world
// keeps all its state in the World: faultline.Run builds a new World for every run, and a
// passing seed runs twice under determinism checking, so state outside the World could make
// two runs of a seed differ. This variable cannot: it exists only in a build with the tag
// tenure_mutants, a test sets it before faultline.Run starts and resets it after the runs,
// the worlds only read it, and nothing changes it while a run goes on, so every run of every
// seed of the test sees the same value.
var plantedForTest Mutation

// SetMutationForTest plants the bug m names in every world built from now on, whatever the
// scenario's Mutation, and returns the function that plants the previous one again, which the
// test calls once its runs are over (for example with t.Cleanup). Only a test sets it, before
// faultline.Run, and never while a run goes on; a test that sets it must not run in parallel
// with another test that builds worlds. It exists only in a build with the tag
// tenure_mutants.
func SetMutationForTest(m Mutation) (restore func()) {
	prev := plantedForTest
	plantedForTest = m
	return func() { plantedForTest = prev }
}

// plants reports whether the bug that bug names is planted in a world whose scenario's
// Mutation is m: bug is not MutationNone, and m or the mutation of SetMutationForTest is bug.
func (m Mutation) plants(bug Mutation) bool {
	return bug != MutationNone && (m == bug || plantedForTest == bug)
}
