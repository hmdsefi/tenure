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
// The TestATRTR13Router tests add routers to such worlds: each follows the store's views with a
// lag of its own and routes requests to the members, correcting its table from their 421
// answers, and the simulation checks after every event that a request reaches a member that
// serves its shard within three sends and that a router whose view equals the members' sends
// each request once. Run them alone with
//
//	go test -run Router ./...
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
// No test turns that check on by itself, so state that makes two runs of a seed differ fails
// only a run with the variable set; TestTraceReplay, which compares two runs of a seed record
// by record, runs only then and skips otherwise.
//
// The file testdata/hashes.txt pins the trace hashes of eight scenarios at three fixed seeds,
// under Go 1.26, so a change that alters a run fails TestGoldenHashes; under another Go minor
// version the test skips. When the change is intended, rewrite the file under Go 1.26 with
//
//	GOTOOLCHAIN=go1.26.0 go test -count=1 -run '^TestGoldenHashes$' -update .
//
// and give the commit a "Golden-Update: <reason>" line. Seeds that once exposed a bug run as
// TestRegression tests.
//
// Built with the tag tenure_mutants, the simulations can also plant bugs into the protocol: each
// TestMutant test plants one and must fail the check the bug breaks. TestMutantsCaught, in the
// ordinary build, runs each of them in a child process and checks that it does.
package simtest
