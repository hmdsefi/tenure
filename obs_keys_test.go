// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/debughttp"
)

// TestDebughttpEventKeys proves that debughttp's own per-kind key list equals the log attributes.
// For every kind, with every field set, with Err nil and, for store.failed, with Space "", Shard
// -1 and Slot -1 (with and without Err), it serves the events from a node's /events and checks
// that the keys after seq, time and kind are the log record's attribute keys, in order, and that
// each value is the attribute's, as a JSON value of the matching type: a string for a string, a
// duration and an error, and a number for an int and a uint64. It reads the body token by token
// to keep the keys' order.
func TestDebughttpEventKeys(t *testing.T) {
	if s := (tenure.EventSessionRestarted + 1).String(); !strings.HasPrefix(s, "EventKind(") {
		t.Fatalf("kind %d is %q: extend this test and debughttp's key list to the new kinds", tenure.EventSessionRestarted+1, s)
	}
	when := time.Date(2026, 11, 9, 10, 0, 0, 1, time.UTC)
	var events []tenure.Event
	add := func(e tenure.Event) {
		e.Seq, e.Time = uint64(len(events)+1), when
		events = append(events, e)
	}
	for k := tenure.EventViewChanged; k <= tenure.EventSessionRestarted; k++ {
		full := tenure.Event{
			Kind: k, Space: "orders", Shard: 3, Term: 1<<63 + 1, Slot: 2, Reason: "why", Holder: "b/22",
			Duration: 1500 * time.Millisecond, Version: 1<<64 - 1, Count: 4, Configured: 5,
			Err: errors.New("a < b & c"),
		}
		add(full)
		noErr := full
		noErr.Err = nil
		add(noErr)
		if k == tenure.EventStoreFailed {
			unused := full
			unused.Space, unused.Shard, unused.Slot = "", -1, -1
			add(unused)
			unused.Err = nil
			add(unused)
		}
	}

	rec := httptest.NewRecorder()
	debughttp.Handler(tenure.NewNodeForTest(events...)).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /events: status %d, body %s", rec.Code, rec.Body)
	}

	dec := json.NewDecoder(rec.Body)
	dec.UseNumber()
	token := func() json.Token {
		t.Helper()
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("reading the body: %v", err)
		}
		return tok
	}
	expect := func(want json.Token) {
		t.Helper()
		if tok := token(); tok != want {
			t.Fatalf("token %v (%T), want %v (%T)", tok, tok, want, want)
		}
	}
	expect(json.Delim('{'))
	expect("next_seq")
	expect(json.Number(strconv.Itoa(len(events) + 1)))
	expect("events")
	expect(json.Delim('['))
	for _, e := range events {
		if !dec.More() {
			t.Fatalf("the body ends before the event %d", e.Seq)
		}
		expect(json.Delim('{'))
		var keys []string
		var values []json.Token
		for dec.More() {
			key, ok := token().(string)
			if !ok {
				t.Fatalf("event %d: a key that is not a string", e.Seq)
			}
			keys = append(keys, key)
			values = append(values, token())
		}
		expect(json.Delim('}'))
		checkEventObject(t, e, keys, values)
	}
	expect(json.Delim(']'))
	expect(json.Delim('}'))
	if dec.More() {
		t.Errorf("the body has more after the events object")
	}
}

// TestDebughttpNoEvents checks next_seq for a node that keeps no events: 1, also with since.
func TestDebughttpNoEvents(t *testing.T) {
	h := debughttp.Handler(tenure.NewNodeForTest())
	for _, target := range []string{"/events", "/events?since=7"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
		if want := "{\n  \"next_seq\": 1,\n  \"events\": []\n}\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
			t.Errorf("GET %s: status %d, body %q, want 200 and %q", target, rec.Code, rec.Body, want)
		}
	}
}

// checkEventObject checks the keys and values of e's event object: seq, time and kind, then the
// attributes of e's log record.
func checkEventObject(t *testing.T, e tenure.Event, keys []string, values []json.Token) {
	t.Helper()
	name := fmt.Sprintf("event %d (%s, Space %q, Shard %d, Slot %d, Err %v)", e.Seq, e.Kind, e.Space, e.Shard, e.Slot, e.Err)
	attrs := tenure.EventAttrsForTest(e)
	wantKeys := []string{"seq", "time", "kind"}
	wantValues := []json.Token{json.Number(strconv.FormatUint(e.Seq, 10)), e.Time.Format(time.RFC3339Nano), e.Kind.String()}
	for _, a := range attrs {
		wantKeys = append(wantKeys, a.Key)
		wantValues = append(wantValues, jsonValue(t, a.Value))
	}
	if strings.Join(keys, ",") != strings.Join(wantKeys, ",") {
		t.Errorf("%s: keys %v, want %v", name, keys, wantKeys)
		return
	}
	for i := range keys {
		if values[i] != wantValues[i] {
			t.Errorf("%s: %s = %v (%T), want %v (%T)", name, keys[i], values[i], values[i], wantValues[i], wantValues[i])
		}
	}
}

// jsonValue returns the token that a log attribute's value v must read as in an event object:
// a string for a string, a duration and an error, a number for an int and a uint64.
func jsonValue(t *testing.T, v slog.Value) json.Token {
	t.Helper()
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return json.Number(strconv.FormatInt(v.Int64(), 10))
	case slog.KindUint64:
		return json.Number(strconv.FormatUint(v.Uint64(), 10))
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			return err.Error()
		}
	}
	t.Fatalf("attribute value %v of kind %s: no JSON type known for it", v, v.Kind())
	return nil
}
