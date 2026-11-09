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
// # Configuration
//
// New never fails and makes no request. It records the first problem it finds with its
// arguments, and every method of the Adapter returns that error before doing anything else.
// RESTConfig prepares a client configuration with request limits that suit the Adapter.
package k8s
