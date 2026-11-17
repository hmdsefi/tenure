// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp

import (
	"encoding/json"
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
