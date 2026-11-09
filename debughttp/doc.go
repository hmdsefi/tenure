// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package debughttp serves a tenure node's ownership table and recent events as JSON, for
// operators who look at a node over HTTP during deploys and incidents. It only reads the node.
//
// Every body is one JSON object indented by two spaces and followed by one newline. Keys come in
// a fixed order, the same on every node and in every release that does not change them. Times
// are strings in RFC 3339 format with nanoseconds, in UTC, or null for a zero time. Durations are
// strings such as "2s", in the format of time.Duration's String method. Terms, versions and
// counts are JSON numbers. Characters such as '<' and '&' are not escaped.
//
// The table object has the keys taken, member, holder, draining, closed, session_live,
// last_renewal, session_deadline, view_version, members, slots, slot, slot_holders and spaces,
// from the fields of tenure.Table of the same names. A member object has the keys id, addr, meta,
// draining, slot, join_at and leave_at; meta is an object with its keys sorted, {} when the
// member has none. A space object has the keys name, shards, fenced and rows, and a row object
// the keys shard, route, slot, state, holder, term, since, running, stuck, restarts and stop. A
// list that is empty is [], never null.
//
// An event object has the keys seq, time and kind (the kind's name, such as "shard.acquired"),
// then the attributes of the event's log record, with the same keys, in the same order, and
// present under the same conditions: error, the text of the event's error, only when it has one,
// and for a "store.failed" event, space, shard and slot only when the failed call had them.
//
// The events body is {"next_seq": N, "events": [...]}, where N is the sequence number the node
// gives its next event, and the root body is {"table": ..., "next_seq": N, "events": [...]}.
package debughttp
