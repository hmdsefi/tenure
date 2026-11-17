// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// viewJSON is the JSON form of a core.View.
type viewJSON struct {
	Version     uint64       `json:"version"`
	Members     []memberJSON `json:"members"`
	Slots       int          `json:"slots"`
	SlotHolders []string     `json:"slot_holders"`
}

// memberJSON is the JSON form of a core.Member. Times are RFC 3339 with nanoseconds, in UTC, and
// left out when zero; Meta is left out when empty.
type memberJSON struct {
	ID       string            `json:"id"`
	Addr     string            `json:"addr"`
	Meta     map[string]string `json:"meta,omitempty"`
	Draining bool              `json:"draining"`
	Slot     int               `json:"slot"`
	JoinAt   string            `json:"join_at,omitempty"`
	LeaveAt  string            `json:"leave_at,omitempty"`
}

// encodeView returns v as the JSON body the handler serves: one object and a newline. Lists are
// [] when empty, never null.
func encodeView(v core.View) ([]byte, error) {
	out := viewJSON{
		Version:     v.Version,
		Members:     make([]memberJSON, len(v.Members)),
		Slots:       v.Slots,
		SlotHolders: v.SlotHolders,
	}
	if out.SlotHolders == nil {
		out.SlotHolders = []string{}
	}
	for i, m := range v.Members {
		out.Members[i] = memberJSON{
			ID:       m.ID,
			Addr:     m.Addr,
			Meta:     m.Meta,
			Draining: m.Draining,
			Slot:     m.Slot,
			JoinAt:   formatTime(m.JoinAt),
			LeaveAt:  formatTime(m.LeaveAt),
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// formatTime returns t in RFC 3339 with nanoseconds, in UTC, or "" for the zero time.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// decodeView returns the view of a body that encodeView wrote. Empty lists and an empty meta
// decode as nil; times decode in UTC. It fails when the body is not such an object or a time
// does not parse.
func decodeView(b []byte) (core.View, error) {
	var in viewJSON
	if err := json.Unmarshal(b, &in); err != nil {
		return core.View{}, err
	}
	v := core.View{Version: in.Version, Slots: in.Slots}
	if len(in.SlotHolders) > 0 {
		v.SlotHolders = in.SlotHolders
	}
	if len(in.Members) > 0 {
		v.Members = make([]core.Member, len(in.Members))
	}
	for i, m := range in.Members {
		out := core.Member{ID: m.ID, Addr: m.Addr, Draining: m.Draining, Slot: m.Slot}
		if len(m.Meta) > 0 {
			out.Meta = m.Meta
		}
		var err error
		if out.JoinAt, err = parseTime(m.JoinAt); err != nil {
			return core.View{}, fmt.Errorf("members[%d].join_at: %w", i, err)
		}
		if out.LeaveAt, err = parseTime(m.LeaveAt); err != nil {
			return core.View{}, fmt.Errorf("members[%d].leave_at: %w", i, err)
		}
		v.Members[i] = out
	}
	return v, nil
}

// parseTime parses s, a time that formatTime wrote, in UTC; "" is the zero time.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}
