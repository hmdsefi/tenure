// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package simtest holds the deterministic simulations of tenure's ownership protocol.
//
// The simulations run on faultline: every run takes its random choices from one seed and runs
// in virtual time, so a failing seed replays the same run. A simulation drives several members
// against a simulated store and a fenced downstream while faults crash nodes, cut links and
// bend clocks, and it checks after every event that each shard has at most one owner at a
// time, that terms only grow, and that a fenced downstream never accepts a write of an owner
// that was replaced.
//
// The package has no API: it is a set of tests, and nothing imports it. Run the short seed
// counts with
//
//	go test -short ./...
//
// and, to check that every seed replays, run each passing seed twice with
//
//	FAULTLINE_CHECK_DETERMINISM=1 go test -count=1 ./...
//
// Built with the tag tenure_mutants, the simulations can also plant bugs into the protocol: each
// TestMutant test plants one and must fail the check the bug breaks. TestMutantsCaught, in the
// ordinary build, runs each of them in a child process and checks that it does.
package simtest
