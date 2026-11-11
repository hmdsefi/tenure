// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// The fixture of TestATSIM42RegressionNames: two regression tests, the second copied from the
// first with its seed changed and its name not.

package simtest

import (
	"testing"
	"time"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

func TestRegressionFencedStoreOutageObserver80101a0b(t *testing.T) {
	regression(t, "TestFencedStoreOutage", world.Observer, 0x80101a0b443af683, outageFor(14*time.Second))
}

func TestRegressionFencedStoreOutageObserver80101a0c(t *testing.T) {
	regression(t, "TestFencedStoreOutage", world.Observer, 0x0a2a20ac5ab89fe1, outageFor(19*time.Second))
}
