// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The regression seeds (SIM-630): each test runs one seed that once exposed a bug in the world
// or in core, now fixed, with the options and the body of the row it failed in. A test is
// named TestRegression<Row><Rule><the first 8 hex digits of the seed>, which
// TestATSIM42RegressionNames checks against the call's arguments. The call is
// regression(t, "<row's test name>", world.<Rule>, <seed>, <changes to the row>...).

// TestRegressionFencedStoreOutageObserver80101a0b: TestFencedStoreOutage/observer/14s. A driver
// cut off from the store dropped the other members from its view after LeaseDuration, which
// moved the balanced placement and stopped a fenced run as moved during the outage; fixed by
// freezing the observer's views while its own session is not live.
func TestRegressionFencedStoreOutageObserver80101a0b(t *testing.T) {
	regression(t, "TestFencedStoreOutage", world.Observer, 0x80101a0b443af683, outageFor(14*time.Second))
}

// TestRegressionFencedStoreOutageObserver0a2a20ac: TestFencedStoreOutage/observer/19s, the same
// bug as TestRegressionFencedStoreOutageObserver80101a0b.
func TestRegressionFencedStoreOutageObserver0a2a20ac(t *testing.T) {
	regression(t, "TestFencedStoreOutage", world.Observer, 0x0a2a20ac5ab89fe1, outageFor(19*time.Second))
}

// TestRegressionFencedStoreOutageObserver1733e008: TestFencedStoreOutage/observer/1m0s, the
// same bug as TestRegressionFencedStoreOutageObserver80101a0b.
func TestRegressionFencedStoreOutageObserver1733e008(t *testing.T) {
	regression(t, "TestFencedStoreOutage", world.Observer, 0x1733e00882e0f082, outageFor(time.Minute))
}

// regression runs seed of the row named name under rule, with the row's options and body,
// after applying changes to the row in order.
func regression(t *testing.T, name string, rule world.Takeover, seed uint64, changes ...func(*row)) {
	t.Helper()
	r := rowNamed(t, name)
	for _, change := range changes {
		change(&r)
	}
	faultline.RunSeeds(t, r.options(), r.body(rule, nil), seed)
}

// outageFor returns a change to TestFencedStoreOutage's row that sets its outage to d, as the
// subtest of that length runs it.
func outageFor(d time.Duration) func(*row) {
	return func(r *row) { r.sc = outageScenario(d) }
}
