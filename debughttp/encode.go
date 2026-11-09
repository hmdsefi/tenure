// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package debughttp

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"time"

	"github.com/hmdsefi/tenure"
)

// encodeTable writes t as the table body: the table object and one newline.
func encodeTable(w io.Writer, t tenure.Table) error {
	return encode(w, newTableJSON(t))
}

// encodeEvents writes the events body: next is the next_seq value and events the events, in
// order.
func encodeEvents(w io.Writer, next uint64, events []tenure.Event) error {
	return encode(w, eventsJSON{NextSeq: next, Events: newEventsJSON(events)})
}

// encodeRoot writes the root body: the table t, the next_seq value next and the events, in order.
func encodeRoot(w io.Writer, t tenure.Table, next uint64, events []tenure.Event) error {
	return encode(w, rootJSON{Table: newTableJSON(t), NextSeq: next, Events: newEventsJSON(events)})
}

// encode writes v as JSON indented by two spaces, without escaping HTML characters, and one
// newline (OBS-400).
func encode(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// eventsJSON is the events body.
type eventsJSON struct {
	NextSeq uint64      `json:"next_seq"`
	Events  []eventJSON `json:"events"`
}

// rootJSON is the root body.
type rootJSON struct {
	Table   tableJSON   `json:"table"`
	NextSeq uint64      `json:"next_seq"`
	Events  []eventJSON `json:"events"`
}

// jsonTime is a time that encodes in RFC 3339 format with nanoseconds, in UTC, or as null when
// it is zero.
type jsonTime time.Time

// MarshalJSON implements json.Marshaler.
func (t jsonTime) MarshalJSON() ([]byte, error) {
	tt := time.Time(t)
	if tt.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(tt.UTC().Format(time.RFC3339Nano))
}

// tableJSON is the table object of OBS-410: the fields of tenure.Table in their order, with
// snake_case keys.
type tableJSON struct {
	Taken           jsonTime     `json:"taken"`
	Member          memberJSON   `json:"member"`
	Holder          string       `json:"holder"`
	Draining        bool         `json:"draining"`
	Closed          bool         `json:"closed"`
	SessionLive     bool         `json:"session_live"`
	LastRenewal     jsonTime     `json:"last_renewal"`
	SessionDeadline jsonTime     `json:"session_deadline"`
	ViewVersion     uint64       `json:"view_version"`
	Members         []memberJSON `json:"members"`
	Slots           int          `json:"slots"`
	Slot            int          `json:"slot"`
	SlotHolders     []string     `json:"slot_holders"`
	Spaces          []spaceJSON  `json:"spaces"`
}

// memberJSON is a member object: the fields of core.Member in their order.
type memberJSON struct {
	ID       string            `json:"id"`
	Addr     string            `json:"addr"`
	Meta     map[string]string `json:"meta"` // never nil: encoding/json sorts the keys
	Draining bool              `json:"draining"`
	Slot     int               `json:"slot"`
	JoinAt   jsonTime          `json:"join_at"`
	LeaveAt  jsonTime          `json:"leave_at"`
}

// spaceJSON is a space object: the fields of tenure.TableSpace in their order.
type spaceJSON struct {
	Name   string    `json:"name"`
	Shards int       `json:"shards"`
	Fenced bool      `json:"fenced"`
	Rows   []rowJSON `json:"rows"`
}

// rowJSON is a row object: the fields of tenure.TableRow in their order.
type rowJSON struct {
	Shard    int      `json:"shard"`
	Route    string   `json:"route"`
	Slot     int      `json:"slot"`
	State    string   `json:"state"`
	Holder   string   `json:"holder"`
	Term     uint64   `json:"term"`
	Since    jsonTime `json:"since"`
	Running  bool     `json:"running"`
	Stuck    bool     `json:"stuck"`
	Restarts int      `json:"restarts"`
	Stop     string   `json:"stop"`
}

// newTableJSON returns the table object of t. Its lists and meta maps are never nil, so they
// encode as [] and {} when empty.
func newTableJSON(t tenure.Table) tableJSON {
	j := tableJSON{
		Taken:           jsonTime(t.Taken),
		Member:          newMemberJSON(t.Member),
		Holder:          t.Holder,
		Draining:        t.Draining,
		Closed:          t.Closed,
		SessionLive:     t.SessionLive,
		LastRenewal:     jsonTime(t.LastRenewal),
		SessionDeadline: jsonTime(t.SessionDeadline),
		ViewVersion:     t.ViewVersion,
		Members:         make([]memberJSON, len(t.Members)),
		Slots:           t.Slots,
		Slot:            t.Slot,
		SlotHolders:     make([]string, len(t.SlotHolders)),
		Spaces:          make([]spaceJSON, len(t.Spaces)),
	}
	for i, m := range t.Members {
		j.Members[i] = newMemberJSON(m)
	}
	copy(j.SlotHolders, t.SlotHolders)
	for i, sp := range t.Spaces {
		s := spaceJSON{Name: sp.Name, Shards: sp.Shards, Fenced: sp.Fenced, Rows: make([]rowJSON, len(sp.Rows))}
		for k, r := range sp.Rows {
			s.Rows[k] = rowJSON{
				Shard:    r.Shard,
				Route:    r.Route,
				Slot:     r.Slot,
				State:    r.State,
				Holder:   r.Holder,
				Term:     uint64(r.Term),
				Since:    jsonTime(r.Since),
				Running:  r.Running,
				Stuck:    r.Stuck,
				Restarts: r.Restarts,
				Stop:     r.Stop,
			}
		}
		j.Spaces[i] = s
	}
	return j
}

// newMemberJSON returns the member object of m.
func newMemberJSON(m tenure.Member) memberJSON {
	meta := make(map[string]string, len(m.Meta))
	maps.Copy(meta, m.Meta)
	return memberJSON{
		ID:       m.ID,
		Addr:     m.Addr,
		Meta:     meta,
		Draining: m.Draining,
		Slot:     m.Slot,
		JoinAt:   jsonTime(m.JoinAt),
		LeaveAt:  jsonTime(m.LeaveAt),
	}
}

// field names the tenure.Event field an event key is read from.
type field uint8

const (
	fieldSpace field = iota
	fieldShard
	fieldTerm
	fieldSlot
	fieldReason
	fieldHolder
	fieldDuration
	fieldVersion
	fieldCount
	fieldConfigured
	fieldErr
)

// eventKey is one key of an event object after seq, time and kind: its name and the field it is
// read from. error is present only when Err is not nil. When opt is true, a space, shard or slot
// key is present only when its field is not unused ("" or -1).
type eventKey struct {
	name  string
	field field
	opt   bool
}

// The keys shared by several kinds.
var (
	keySpace  = eventKey{name: "space", field: fieldSpace}
	keyShard  = eventKey{name: "shard", field: fieldShard}
	keyTerm   = eventKey{name: "term", field: fieldTerm}
	keySlot   = eventKey{name: "slot", field: fieldSlot}
	keyReason = eventKey{name: "reason", field: fieldReason}
	keyOp     = eventKey{name: "op", field: fieldReason}
	keyTook   = eventKey{name: "took", field: fieldDuration}
	keyError  = eventKey{name: "error", field: fieldErr}
)

// eventKeys holds each kind's keys after seq, time and kind, in order, indexed by tenure.EventKind.
// It is debughttp's own copy of the log attributes of each kind (OBS-010, OBS-420); a value that
// is not a kind has none.
var eventKeys = [...][]eventKey{
	tenure.EventViewChanged:        {{name: "version", field: fieldVersion}, {name: "members", field: fieldCount}},
	tenure.EventSessionOpened:      nil,
	tenure.EventSessionRenewFailed: {keyError},
	tenure.EventSessionRenewedLate: {{name: "late_by", field: fieldDuration}},
	tenure.EventSessionRecovered:   {{name: "failures", field: fieldCount}},
	tenure.EventSessionExpired:     nil,
	tenure.EventSessionLost:        {keyError},
	tenure.EventSessionClosed:      nil,
	tenure.EventShardAcquiring:     {keySpace, keyShard},
	tenure.EventShardAcquireFailed: {
		keySpace, keyShard, {name: "holder", field: fieldHolder},
		{name: "retry_after", field: fieldDuration}, keyError,
	},
	tenure.EventShardAcquired:         {keySpace, keyShard, keyTerm},
	tenure.EventShardDegraded:         {keySpace, keyShard, keyTerm},
	tenure.EventShardRecovered:        {keySpace, keyShard, keyTerm},
	tenure.EventShardExpired:          {keySpace, keyShard, keyTerm},
	tenure.EventShardLost:             {keySpace, keyShard, keyTerm},
	tenure.EventShardStopping:         {keySpace, keyShard, keyTerm, keyReason},
	tenure.EventShardStuck:            {keySpace, keyShard, keyTerm, keyReason},
	tenure.EventShardReleasing:        {keySpace, keyShard, keyTerm},
	tenure.EventShardReleased:         {keySpace, keyShard, keyTerm, keyReason},
	tenure.EventShardHandoffCompleted: {keySpace, keyShard, keyTerm, keyReason, keyTook},
	tenure.EventSlotClaimed:           {keySlot},
	tenure.EventSlotReleased:          {keySlot, keyReason},
	tenure.EventSlotStandby:           nil,
	tenure.EventSlotCountChanged:      {{name: "slots", field: fieldCount}},
	tenure.EventSlotCountMismatch:     {{name: "stored", field: fieldCount}, {name: "configured", field: fieldConfigured}},
	tenure.EventRunReturned: {
		keySpace, keyShard, keyTerm, {name: "restarts", field: fieldCount},
		{name: "restart_in", field: fieldDuration}, keyError,
	},
	tenure.EventStoreFailed: {
		keyOp,
		{name: "space", field: fieldSpace, opt: true},
		{name: "shard", field: fieldShard, opt: true},
		{name: "slot", field: fieldSlot, opt: true},
		keyError,
	},
	tenure.EventMembershipFailed: {keyOp, keyError},
	tenure.EventNodeDraining:     nil,
	tenure.EventNodeDrained:      {keyTook},
	tenure.EventNodeClosed:       {keyReason},
	tenure.EventSessionRestarted: {{name: "previous", field: fieldHolder}},
}

// value returns the key's value in e, as encoding/json should encode it, and false when the key
// is absent: strings, ints and uint64s as they are, a duration as its String and an error as its
// Error.
func (k eventKey) value(e tenure.Event) (any, bool) {
	switch k.field {
	case fieldSpace:
		return e.Space, !k.opt || e.Space != ""
	case fieldShard:
		return e.Shard, !k.opt || e.Shard >= 0
	case fieldTerm:
		return uint64(e.Term), true
	case fieldSlot:
		return e.Slot, !k.opt || e.Slot >= 0
	case fieldReason:
		return e.Reason, true
	case fieldHolder:
		return e.Holder, true
	case fieldDuration:
		return e.Duration.String(), true
	case fieldVersion:
		return e.Version, true
	case fieldCount:
		return e.Count, true
	case fieldConfigured:
		return e.Configured, true
	case fieldErr:
		if e.Err == nil {
			return nil, false
		}
		return e.Err.Error(), true
	}
	return nil, false
}

// member is one key and value of an eventJSON.
type member struct {
	key   string
	value any
}

// eventJSON is an event object: its keys in order, which depend on the event's kind.
type eventJSON []member

// newEventsJSON returns the event objects of events, never nil, so that no events encode as [].
func newEventsJSON(events []tenure.Event) []eventJSON {
	j := make([]eventJSON, len(events))
	for i, e := range events {
		j[i] = newEventJSON(e)
	}
	return j
}

// newEventJSON returns the event object of e: seq, time and kind, then the keys of its kind that
// are present.
func newEventJSON(e tenure.Event) eventJSON {
	var keys []eventKey
	if int(e.Kind) < len(eventKeys) {
		keys = eventKeys[e.Kind]
	}
	j := make(eventJSON, 0, 3+len(keys))
	j = append(j,
		member{key: "seq", value: e.Seq},
		member{key: "time", value: jsonTime(e.Time)},
		member{key: "kind", value: e.Kind.String()},
	)
	for _, k := range keys {
		if v, ok := k.value(e); ok {
			j = append(j, member{key: k.name, value: v})
		}
	}
	return j
}

// MarshalJSON implements json.Marshaler: the members in order, in one object. The Encoder that
// calls it indents the object; the values are written without escaping HTML characters, as the
// Encoder writes the rest.
func (j eventJSON) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	b.WriteByte('{')
	for i, m := range j {
		if i > 0 {
			b.WriteByte(',')
		}
		// Encode ends each value with a newline, which the calling Encoder drops.
		if err := enc.Encode(m.key); err != nil {
			return nil, err
		}
		b.WriteByte(':')
		if err := enc.Encode(m.value); err != nil {
			return nil, err
		}
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}
