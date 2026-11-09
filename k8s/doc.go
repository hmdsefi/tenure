// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package k8s keeps one tenure group in Kubernetes: an Adapter is the group's store, slot store,
// holder watch and membership, built only on coordination.k8s.io/v1 Leases in one namespace.
// It needs no other infrastructure and one RBAC rule: get, list, watch, create, update and
// delete on Leases in that namespace.
//
// # Objects
//
// Every Lease of a group carries the label tenure.group with the group's name and the label
// tenure.kind with its kind. The group's Leases are:
//
//	<group>-member-<id>       kind member: the member's session and, while it has joined, its
//	                          member record in the annotation tenure.member
//	<group>-<space>-<shard>   kind shard, label tenure.space: a shard record, with its term in
//	                          the annotation tenure.term
//	<group>-slots             kind slots: the slot count in the annotation tenure.slots
//	<group>-slot-<n>          kind slot: a slot record
//
// <shard> and <n> are decimal numbers. A Lease at one of these names that lacks the group's
// labels belongs to someone else, and the Adapter never writes it. The names of two groups in
// one namespace can coincide when one group's name followed by "-" starts the other's, as with
// the groups orders and orders-eu: give such groups their own namespaces.
//
// # Names
//
// The namespace and the group are DNS-1123 labels, the group at most 40 characters long. A
// space is a DNS-1123 label other than generation, member, request, slot and slots that does
// not start with "member-" or "request-". A member ID is a DNS-1123 subdomain that keeps its
// member Lease name within 253 characters, so a pod name serves as a member ID. CheckNames
// reports a space or a member ID the Adapter cannot hold.
//
// # Timing and liveness
//
// A Lease stores its duration in whole seconds, so the LeaseDuration of every Timing the
// Adapter uses must be a whole number of seconds; CheckTiming reports one that is not. Another
// holder takes a shard only once it has itself observed the holder's session unchanged for the
// holder's LeaseDuration, on its own monotonic clock. No time written into a Lease is ever
// compared with a clock.
//
// # Sessions
//
// A holder is <member ID>/<incarnation>, and its session is its member Lease: KeepAlive
// creates the Lease naming the holder, or takes it over from another incarnation of the same
// member, which loses its session, and then renews it with one write each time. Every write is a
// compare-and-swap of the whole Lease that keeps the labels and annotations it does not own.
// While the Adapter has an active session, it watches the group's Leases with one informer,
// which stops once nothing needs it any more. CloseSession ends a session for good.
//
// # Shard records
//
// Acquire grants a shard, with a new term, to a holder with an active session in the Adapter
// when the shard's Lease is absent, names no holder, names the holder itself, or names a holder
// whose session is gone: closed, replaced by another incarnation of its member, or observed
// unchanged for its LeaseDuration. Before it takes a shard from another holder, it reads that
// holder's member Lease from the API server and decides again. At most WithRecordConcurrency
// record operations run at once; the others wait in arrival order.
//
// Release clears a record's holder only when the record names the caller at the caller's term,
// keeping the term. Confirm writes a counter into the holder's own record, so that a takeover
// decided on an earlier state of the record fails; CloseSession releases every record of the
// holder. Holdings lists a space's records as stored, with one request.
//
// # Slots
//
// SetSlots sets the group's slot count, 0 to 10000. ClaimSlot returns the holder's own slot
// below the count, or claims the lowest free one: a slot whose Lease is absent, names no holder,
// or names a holder whose session is gone, judged as for shards. ReleaseSlot clears a slot's
// holder only when the slot names the caller, and CloseSession releases every slot of the
// holder. Slots returns the count and each slot's live holder. While the Adapter has joined or
// holds a session but has not seen one of its own member Lease writes come back from its watch
// within RenewDeadline, it takes its view of the group to be stale: the passing of time alone
// then turns no slot's holder gone.
//
// # Membership
//
// A member that joined has its member record, a small JSON document, in the annotation
// tenure.member of its member Lease. Join writes it, creating the Lease with no holder when there
// is none, so a member needs no session: while no session holds the Lease, the Adapter moves its
// renewTime on every RetryPeriod of the Adapter's Timing. SetDraining rewrites the record. A
// session closed while the member has joined only clears the Lease's holder. Leave deletes the
// Lease, or removes only the record while a session holds it.
//
// # Configuration
//
// New never fails and makes no request. It records the first problem it finds with its
// arguments, and every method of the Adapter returns that error before doing anything else.
// RESTConfig prepares a client configuration with request limits that suit the Adapter.
package k8s
