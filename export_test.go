// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Test-only exports for the tests of package tenure_test.

// CheckConfig is checkConfig: it applies the defaults to a Config and returns the first failure
// of its checks.
var CheckConfig = checkConfig

// Holder returns the node's holder string: Self.ID, "/" and the incarnation in lowercase hex.
func (n *Node) Holder() string { return n.holder }

// Epoch returns the node's monotonic origin.
func (n *Node) Epoch() time.Time { return n.epoch }

// PublishedView returns the node's published merged view, and false before the first.
func (n *Node) PublishedView() (core.View, bool) {
	v := n.view.Load()
	if v == nil {
		return core.View{}, false
	}
	return *v, true
}
