// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package debughttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
)

// at returns 2026-11-09T10:00:00Z plus d.
func at(d time.Duration) time.Time {
	return time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC).Add(d)
}

// atobs21Table is AT-OBS-21's table: a live node with two members that carry Meta, and one space
// with an owned row and an idle row.
func atobs21Table() tenure.Table {
	self := tenure.Member{ID: "node-a", Addr: "10.0.0.1:8080", Meta: map[string]string{"zone": "eu-1", "rack": "r7"}, Slot: -1}
	other := tenure.Member{ID: "node-b", Addr: "10.0.0.2:8080", Meta: map[string]string{"zone": "eu-2"}, Slot: -1}
	return tenure.Table{
		Taken:           at(5*time.Second + 250*time.Millisecond),
		Member:          self,
		Holder:          "node-a/1f",
		SessionLive:     true,
		LastRenewal:     at(4*time.Second + 123456789),
		SessionDeadline: at(14*time.Second + 250*time.Millisecond),
		ViewVersion:     7,
		Members:         []tenure.Member{self, other},
		Slot:            -1,
		Spaces: []tenure.TableSpace{{
			Name:   "orders",
			Shards: 2,
			Fenced: true,
			Rows: []tenure.TableRow{
				{
					Shard: 0, Route: "node-a", Slot: -1, State: "owned", Holder: "node-a/1f", Term: 42,
					Since: at(2 * time.Second), Running: true,
				},
				{
					Shard: 1, Route: "node-b", Slot: -1, State: "idle", Holder: "node-b/3", Term: 9,
					Since: at(0),
				},
			},
		}},
	}
}

// atobs21JSON is the table body of atobs21Table. It was written from the encoding rules of the
// issue that adds the encoders.
const atobs21JSON = `{
  "taken": "2026-11-09T10:00:05.25Z",
  "member": {
    "id": "node-a",
    "addr": "10.0.0.1:8080",
    "meta": {
      "rack": "r7",
      "zone": "eu-1"
    },
    "draining": false,
    "slot": -1,
    "join_at": null,
    "leave_at": null
  },
  "holder": "node-a/1f",
  "draining": false,
  "closed": false,
  "session_live": true,
  "last_renewal": "2026-11-09T10:00:04.123456789Z",
  "session_deadline": "2026-11-09T10:00:14.25Z",
  "view_version": 7,
  "members": [
    {
      "id": "node-a",
      "addr": "10.0.0.1:8080",
      "meta": {
        "rack": "r7",
        "zone": "eu-1"
      },
      "draining": false,
      "slot": -1,
      "join_at": null,
      "leave_at": null
    },
    {
      "id": "node-b",
      "addr": "10.0.0.2:8080",
      "meta": {
        "zone": "eu-2"
      },
      "draining": false,
      "slot": -1,
      "join_at": null,
      "leave_at": null
    }
  ],
  "slots": 0,
  "slot": -1,
  "slot_holders": [],
  "spaces": [
    {
      "name": "orders",
      "shards": 2,
      "fenced": true,
      "rows": [
        {
          "shard": 0,
          "route": "node-a",
          "slot": -1,
          "state": "owned",
          "holder": "node-a/1f",
          "term": 42,
          "since": "2026-11-09T10:00:02Z",
          "running": true,
          "stuck": false,
          "restarts": 0,
          "stop": ""
        },
        {
          "shard": 1,
          "route": "node-b",
          "slot": -1,
          "state": "idle",
          "holder": "node-b/3",
          "term": 9,
          "since": "2026-11-09T10:00:00Z",
          "running": false,
          "stuck": false,
          "restarts": 0,
          "stop": ""
        }
      ]
    }
  ]
}
`

// TestATOBS21TableJSON encodes AT-OBS-21's table and compares it with the expected body byte for
// byte, one trailing newline included (AT-OBS-21). The test's steps and expected JSON were
// written from the issue's text.
func TestATOBS21TableJSON(t *testing.T) {
	var b bytes.Buffer
	if err := encodeTable(&b, atobs21Table()); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != atobs21JSON {
		t.Errorf("encodeTable:\n%s\nwant:\n%s", got, atobs21JSON)
	}
}

// atobs22Events are AT-OBS-22's two events: a shard acquired, then another shard that the node
// failed to acquire because another holder has it.
func atobs22Events() []tenure.Event {
	return []tenure.Event{
		{
			Seq: 1, Time: at(1500 * time.Millisecond), Kind: tenure.EventShardAcquired,
			Space: "orders", Shard: 0, Term: 42, Slot: -1,
		},
		{
			Seq: 2, Time: at(1500*time.Millisecond + 7), Kind: tenure.EventShardAcquireFailed,
			Space: "orders", Shard: 1, Slot: -1, Holder: "node-b/3", Duration: 2 * time.Second,
		},
	}
}

// atobs22JSON is the events body of atobs22Events with next_seq 3. It was written from the
// encoding rules of the issue that adds the encoders.
const atobs22JSON = `{
  "next_seq": 3,
  "events": [
    {
      "seq": 1,
      "time": "2026-11-09T10:00:01.5Z",
      "kind": "shard.acquired",
      "space": "orders",
      "shard": 0,
      "term": 42
    },
    {
      "seq": 2,
      "time": "2026-11-09T10:00:01.500000007Z",
      "kind": "shard.acquire_failed",
      "space": "orders",
      "shard": 1,
      "holder": "node-b/3",
      "retry_after": "2s"
    }
  ]
}
`

// atobs22ErrJSON is the events body of atobs22Events with next_seq 3 when the second event has
// the error "unreachable" and no holder.
const atobs22ErrJSON = `{
  "next_seq": 3,
  "events": [
    {
      "seq": 1,
      "time": "2026-11-09T10:00:01.5Z",
      "kind": "shard.acquired",
      "space": "orders",
      "shard": 0,
      "term": 42
    },
    {
      "seq": 2,
      "time": "2026-11-09T10:00:01.500000007Z",
      "kind": "shard.acquire_failed",
      "space": "orders",
      "shard": 1,
      "holder": "",
      "retry_after": "2s",
      "error": "unreachable"
    }
  ]
}
`

// TestATOBS22EventsJSON encodes AT-OBS-22's two events with next_seq 3 and compares the body
// with the expected one byte for byte, then the variant whose failed acquisition has an error
// and no holder (AT-OBS-22). The test's steps and expected JSON were written from the issue's
// text.
func TestATOBS22EventsJSON(t *testing.T) {
	var b bytes.Buffer
	if err := encodeEvents(&b, 3, atobs22Events()); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != atobs22JSON {
		t.Errorf("encodeEvents:\n%s\nwant:\n%s", got, atobs22JSON)
	}

	events := atobs22Events()
	events[1].Holder, events[1].Err = "", errors.New("unreachable")
	b.Reset()
	if err := encodeEvents(&b, 3, events); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != atobs22ErrJSON {
		t.Errorf("encodeEvents with Err:\n%s\nwant:\n%s", got, atobs22ErrJSON)
	}
}

// TestTableJSONEdges checks the table body at its edges: meta {} when nil and keys sorted, zero
// times null, nil lists [], times in other zones in UTC, member schedules, and no HTML escaping.
func TestTableJSONEdges(t *testing.T) {
	cet := time.FixedZone("CET", 3600)
	tab := tenure.Table{
		Taken:    time.Date(2026, 11, 9, 11, 0, 0, 1, cet),
		Member:   tenure.Member{ID: "a", Slot: 2, JoinAt: at(time.Hour), LeaveAt: time.Date(2026, 11, 9, 13, 0, 0, 0, cet)},
		Draining: true,
		Members: []tenure.Member{
			{ID: "a", Meta: map[string]string{"z": "<&>", "b": "2", "a": "1"}, Draining: true, Slot: 2},
		},
		Slots: 3,
		Slot:  2,
		Spaces: []tenure.TableSpace{
			{Name: "empty"},
			{Name: "s", Shards: 1, Rows: []tenure.TableRow{{Shard: 0, Slot: 1, State: "stopping", Stuck: true, Restarts: 4, Stop: "moved"}}},
		},
	}
	const want = `{
  "taken": "2026-11-09T10:00:00.000000001Z",
  "member": {
    "id": "a",
    "addr": "",
    "meta": {},
    "draining": false,
    "slot": 2,
    "join_at": "2026-11-09T11:00:00Z",
    "leave_at": "2026-11-09T12:00:00Z"
  },
  "holder": "",
  "draining": true,
  "closed": false,
  "session_live": false,
  "last_renewal": null,
  "session_deadline": null,
  "view_version": 0,
  "members": [
    {
      "id": "a",
      "addr": "",
      "meta": {
        "a": "1",
        "b": "2",
        "z": "<&>"
      },
      "draining": true,
      "slot": 2,
      "join_at": null,
      "leave_at": null
    }
  ],
  "slots": 3,
  "slot": 2,
  "slot_holders": [],
  "spaces": [
    {
      "name": "empty",
      "shards": 0,
      "fenced": false,
      "rows": []
    },
    {
      "name": "s",
      "shards": 1,
      "fenced": false,
      "rows": [
        {
          "shard": 0,
          "route": "",
          "slot": 1,
          "state": "stopping",
          "holder": "",
          "term": 0,
          "since": null,
          "running": false,
          "stuck": true,
          "restarts": 4,
          "stop": "moved"
        }
      ]
    }
  ]
}
`
	var b bytes.Buffer
	if err := encodeTable(&b, tab); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != want {
		t.Errorf("encodeTable:\n%s\nwant:\n%s", got, want)
	}

	// A zero Table: every list is [] and every time null.
	b.Reset()
	if err := encodeTable(&b, tenure.Table{}); err != nil {
		t.Fatal(err)
	}
	got := b.String()
	for _, s := range []string{
		`"taken": null,`, `"meta": {},`, `"members": [],`, `"slot_holders": [],`, `"spaces": []` + "\n}\n",
	} {
		if !strings.Contains(got, s) {
			t.Errorf("encodeTable(Table{}) lacks %q:\n%s", s, got)
		}
	}
	if n := strings.Count(got, "null"); n != 5 { // taken, join_at, leave_at, last_renewal, session_deadline
		t.Errorf("encodeTable(Table{}) has %d nulls, want 5:\n%s", n, got)
	}

	// A closed node; the slot holders keep their order and their vacant slots.
	b.Reset()
	if err := encodeTable(&b, tenure.Table{Closed: true, SlotHolders: []string{"b/1", "", "a/2"}}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"\"draining\": false,\n  \"closed\": true,\n",
		"\"slot_holders\": [\n    \"b/1\",\n    \"\",\n    \"a/2\"\n  ],",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("encodeTable lacks %q:\n%s", want, b.String())
		}
	}
}

// TestEventJSONEdges checks event objects at their edges: every kind's name, the optional keys
// of store.failed, a value that is not a kind, a zero time, no events, and no HTML escaping.
func TestEventJSONEdges(t *testing.T) {
	encodeOne := func(e tenure.Event) string {
		t.Helper()
		var b bytes.Buffer
		if err := encodeEvents(&b, e.Seq+1, []tenure.Event{e}); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	body := func(event string) string {
		return "{\n  \"next_seq\": 2,\n  \"events\": [\n    {\n" + event + "    }\n  ]\n}\n"
	}

	tests := []struct {
		name  string
		event tenure.Event
		want  string
	}{
		{
			name:  "store.failed without space, shard and slot",
			event: tenure.Event{Seq: 1, Kind: tenure.EventStoreFailed, Shard: -1, Slot: -1, Reason: "WatchHolders", Err: errors.New("a < b & c")},
			want: `      "seq": 1,
      "time": null,
      "kind": "store.failed",
      "op": "WatchHolders",
      "error": "a < b & c"
`,
		},
		{
			name: "store.failed with space, shard and slot",
			event: tenure.Event{
				Seq: 1, Time: at(0), Kind: tenure.EventStoreFailed, Space: "s", Shard: 0, Slot: 0, Reason: "Release",
				Err: errors.New("x"),
			},
			want: `      "seq": 1,
      "time": "2026-11-09T10:00:00Z",
      "kind": "store.failed",
      "op": "Release",
      "space": "s",
      "shard": 0,
      "slot": 0,
      "error": "x"
`,
		},
		{
			name: "run.returned without an error",
			event: tenure.Event{
				Seq: 1, Time: at(0), Kind: tenure.EventRunReturned, Space: "s", Shard: 3, Term: 1 << 63, Slot: -1,
				Count: 2, Duration: 1500 * time.Millisecond,
			},
			want: `      "seq": 1,
      "time": "2026-11-09T10:00:00Z",
      "kind": "run.returned",
      "space": "s",
      "shard": 3,
      "term": 9223372036854775808,
      "restarts": 2,
      "restart_in": "1.5s"
`,
		},
		{
			name:  "view.changed",
			event: tenure.Event{Seq: 1, Time: at(0), Kind: tenure.EventViewChanged, Shard: -1, Slot: -1, Version: 18446744073709551615, Count: 3},
			want: `      "seq": 1,
      "time": "2026-11-09T10:00:00Z",
      "kind": "view.changed",
      "version": 18446744073709551615,
      "members": 3
`,
		},
		{
			name:  "slot.count_mismatch",
			event: tenure.Event{Seq: 1, Time: at(0), Kind: tenure.EventSlotCountMismatch, Shard: -1, Slot: -1, Count: 4, Configured: 5},
			want: `      "seq": 1,
      "time": "2026-11-09T10:00:00Z",
      "kind": "slot.count_mismatch",
      "stored": 4,
      "configured": 5
`,
		},
		{
			name:  "session.opened",
			event: tenure.Event{Seq: 1, Time: at(0), Kind: tenure.EventSessionOpened, Shard: -1, Slot: -1},
			want: `      "seq": 1,
      "time": "2026-11-09T10:00:00Z",
      "kind": "session.opened"
`,
		},
		{
			name:  "not a kind",
			event: tenure.Event{Seq: 1, Time: at(0), Kind: 255, Space: "s", Err: errors.New("x")},
			want: `      "seq": 1,
      "time": "2026-11-09T10:00:00Z",
      "kind": "EventKind(255)"
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, want := encodeOne(tt.event), body(tt.want); got != want {
				t.Errorf("encodeEvents:\n%s\nwant:\n%s", got, want)
			}
		})
	}

	// Every kind is named, and a kind without keys has only seq, time and kind.
	for k := tenure.EventViewChanged; k <= tenure.EventSessionRestarted; k++ {
		got := encodeOne(tenure.Event{Seq: 1, Kind: k, Shard: -1, Slot: -1})
		if want := "\"kind\": \"" + k.String() + "\""; !strings.Contains(got, want) {
			t.Errorf("kind %d: body lacks %q:\n%s", k, want, got)
		}
		if strings.Contains(got, `"error"`) {
			t.Errorf("kind %s: error present with a nil Err:\n%s", k, got)
		}
	}

	var b bytes.Buffer
	if err := encodeEvents(&b, 1, nil); err != nil {
		t.Fatal(err)
	}
	if want := "{\n  \"next_seq\": 1,\n  \"events\": []\n}\n"; b.String() != want {
		t.Errorf("encodeEvents(1, nil) = %q, want %q", b.String(), want)
	}
}

// TestEventJSONKinds encodes one event of every kind with every field set and checks its keys,
// in order, with their values and JSON types: strings quoted, numbers bare. It pins debughttp's
// own key list for each kind to the log attributes of the issue that adds the kinds.
func TestEventJSONKinds(t *testing.T) {
	const term = `18446744073709551615`
	const loc = `space="sp" shard=5`
	const own = loc + ` term=` + term
	tests := []struct {
		kind tenure.EventKind
		name string
		keys string // after seq, time and kind
	}{
		{tenure.EventViewChanged, "view.changed", `version=8 members=9`},
		{tenure.EventSessionOpened, "session.opened", ``},
		{tenure.EventSessionRenewFailed, "session.renew_failed", `error="boom"`},
		{tenure.EventSessionRenewedLate, "session.renewed_late", `late_by="3s"`},
		{tenure.EventSessionRecovered, "session.recovered", `failures=9`},
		{tenure.EventSessionExpired, "session.expired", ``},
		{tenure.EventSessionLost, "session.lost", `error="boom"`},
		{tenure.EventSessionClosed, "session.closed", ``},
		{tenure.EventShardAcquiring, "shard.acquiring", loc},
		{tenure.EventShardAcquireFailed, "shard.acquire_failed", loc + ` holder="h/1" retry_after="3s" error="boom"`},
		{tenure.EventShardAcquired, "shard.acquired", own},
		{tenure.EventShardDegraded, "shard.degraded", own},
		{tenure.EventShardRecovered, "shard.recovered", own},
		{tenure.EventShardExpired, "shard.expired", own},
		{tenure.EventShardLost, "shard.lost", own},
		{tenure.EventShardStopping, "shard.stopping", own + ` reason="why"`},
		{tenure.EventShardStuck, "shard.stuck", own + ` reason="why"`},
		{tenure.EventShardReleasing, "shard.releasing", own},
		{tenure.EventShardReleased, "shard.released", own + ` reason="why"`},
		{tenure.EventShardHandoffCompleted, "shard.handoff_completed", own + ` reason="why" took="3s"`},
		{tenure.EventSlotClaimed, "slot.claimed", `slot=6`},
		{tenure.EventSlotReleased, "slot.released", `slot=6 reason="why"`},
		{tenure.EventSlotStandby, "slot.standby", ``},
		{tenure.EventSlotCountChanged, "slot.count_changed", `slots=9`},
		{tenure.EventSlotCountMismatch, "slot.count_mismatch", `stored=9 configured=10`},
		{tenure.EventRunReturned, "run.returned", own + ` restarts=9 restart_in="3s" error="boom"`},
		{tenure.EventStoreFailed, "store.failed", `op="why" space="sp" shard=5 slot=6 error="boom"`},
		{tenure.EventMembershipFailed, "membership.failed", `op="why" error="boom"`},
		{tenure.EventNodeDraining, "node.draining", ``},
		{tenure.EventNodeDrained, "node.drained", `took="3s"`},
		{tenure.EventNodeClosed, "node.closed", `reason="why"`},
		{tenure.EventSessionRestarted, "session.restarted", `previous="h/1"`},
		{0, "EventKind(0)", ``},
		{tenure.EventSessionRestarted + 1, "EventKind(33)", ``},
	}
	if n := len(tests) - 2; n != int(tenure.EventSessionRestarted) {
		t.Fatalf("the test lists %d kinds, want %d", n, tenure.EventSessionRestarted)
	}
	for _, tt := range tests {
		e := tenure.Event{
			Seq: 1, Time: at(0), Kind: tt.kind, Space: "sp", Shard: 5, Term: 1<<64 - 1, Slot: 6, Reason: "why",
			Holder: "h/1", Duration: 3 * time.Second, Version: 8, Count: 9, Configured: 10, Err: errors.New("boom"),
		}
		var b bytes.Buffer
		if err := encodeEvents(&b, 2, []tenure.Event{e}); err != nil {
			t.Fatal(err)
		}
		want := `seq=1 time="2026-11-09T10:00:00Z" kind="` + tt.name + `"`
		if tt.keys != "" {
			want += " " + tt.keys
		}
		if got := eventPairs(t, b.Bytes()); got != want {
			t.Errorf("kind %d: got  %s\nwant %s", tt.kind, got, want)
		}
	}
}

// eventPairs returns the one event of an events body as key=value pairs in order, strings
// quoted and numbers as written.
func eventPairs(t *testing.T, body []byte) string {
	t.Helper()
	var v struct{ Events []json.RawMessage }
	if err := json.Unmarshal(body, &v); err != nil || len(v.Events) != 1 {
		t.Fatalf("body %s: %v, %d events", body, err, len(v.Events))
	}
	dec := json.NewDecoder(bytes.NewReader(v.Events[0]))
	dec.UseNumber()
	var pairs []string
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("event %s: %v", v.Events[0], err)
		}
		if tok == json.Delim('{') {
			continue
		}
		if tok == json.Delim('}') {
			return strings.Join(pairs, " ")
		}
		val, err := dec.Token()
		if err != nil {
			t.Fatalf("event %s: %v", v.Events[0], err)
		}
		switch val := val.(type) {
		case string:
			pairs = append(pairs, fmt.Sprintf("%s=%q", tok, val))
		default:
			pairs = append(pairs, fmt.Sprintf("%s=%v", tok, val))
		}
	}
}

// TestRootJSON checks the root body: the table, then next_seq, then the events, each encoded as
// in its own body.
func TestRootJSON(t *testing.T) {
	var b bytes.Buffer
	if err := encodeRoot(&b, atobs21Table(), 3, atobs22Events()); err != nil {
		t.Fatal(err)
	}
	// The table nests one level deeper than in its own body; the events array stays at its depth.
	table := strings.ReplaceAll(strings.TrimSuffix(atobs21JSON, "\n"), "\n", "\n  ")
	events := atobs22JSON[strings.Index(atobs22JSON, "[") : strings.LastIndex(atobs22JSON, "]")+1]
	want := "{\n  \"table\": " + table + ",\n  \"next_seq\": 3,\n  \"events\": " + events + "\n}\n"
	if got := b.String(); got != want {
		t.Errorf("encodeRoot:\n%s\nwant:\n%s", got, want)
	}
}
