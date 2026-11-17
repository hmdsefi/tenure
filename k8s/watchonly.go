// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"fmt"

	"github.com/hmdsefi/tenure"
)

// Watch-only Adapters (RTR-700).
//
// A watch-only Adapter follows a group for a router or a relay: its informer selects only the
// member, slots and slot Leases of the group, so shard handoffs never reach it; it runs no member
// GC; and every method that writes, and WatchHolders, whose shard Leases it does not watch,
// refuse with ErrWatchOnly before any request. It needs only get, list and watch on Leases
// (rbac-readonly.yaml).

// ErrWatchOnly is the error that a watch-only Adapter's writing methods wrap. It is
// tenure.ErrWatchOnly, so errors.Is matches either.
var ErrWatchOnly = tenure.ErrWatchOnly

// WithWatchOnly makes the Adapter watch-only, for processes that only follow the group, such as
// routers: its watch receives the group's member and slot Leases but no shard Lease, it never
// deletes member Leases (WithMemberGC does not apply), and Join, SetDraining, Leave, KeepAlive,
// CloseSession, Acquire, Release, Confirm, ClaimSlot, ReleaseSlot, SetSlots and WatchHolders
// return an error wrapping ErrWatchOnly without a request, once the context's error and the
// configuration error have been checked. Watch, Slots and Holdings work as for any Adapter. A
// watch-only Adapter needs only get, list and watch on Leases, as the module's
// rbac-readonly.yaml grants them.
func WithWatchOnly() Option {
	return Option{apply: func(a *Adapter) error {
		a.watchOnly = true
		return nil
	}}
}

// refuse returns the error with which a watch-only Adapter refuses method, and nil for an Adapter
// that writes.
func (a *Adapter) refuse(method string) error {
	if !a.watchOnly {
		return nil
	}
	return fmt.Errorf("k8s: %s: %w", method, ErrWatchOnly)
}

// leaseSelector returns the label selector of the Adapter's informer: every Lease of the group,
// or, for a watch-only Adapter, its member, slots and slot Leases.
func (a *Adapter) leaseSelector() string {
	s := labelGroup + "=" + a.group
	if a.watchOnly {
		s += "," + labelKind + " in (" + kindMember + "," + kindSlots + "," + kindSlot + ")"
	}
	return s
}
