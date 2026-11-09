// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/hmdsefi/tenure/core"
)

// Label keys of the group's Leases (K8S-050).
const (
	labelGroup = "tenure.group"
	labelKind  = "tenure.kind"
	labelSpace = "tenure.space"
)

// Annotation keys of the group's Leases (K8S-060).
const (
	annotationTerm    = "tenure.term"
	annotationSession = "tenure.session"
	annotationConfirm = "tenure.confirm"
	annotationSlots   = "tenure.slots"
	annotationMember  = "tenure.member"
)

// memberRecordVersion is the only version of memberRecord (K8S-060).
const memberRecordVersion = 1

// leaseLabels returns the labels of a Lease of kind in group (K8S-050). space labels shard
// records only; it is ignored for the other kinds.
func leaseLabels(group, kind, space string) map[string]string {
	l := map[string]string{labelGroup: group, labelKind: kind}
	if kind == kindShard {
		l[labelSpace] = space
	}
	return l
}

// checkOwned returns K8S-050's foreign Lease error when l lacks the labels of a kind Lease of
// group: a Lease at one of the group's names that belongs to someone else is never written.
func checkOwned(l *coordinationv1.Lease, group, kind string) error {
	if l.Labels[labelGroup] == group && l.Labels[labelKind] == kind {
		return nil
	}
	return fmt.Errorf("k8s: lease %s is not a %s lease of group %s", l.Name, kind, group)
}

// encodeTerm returns the tenure.term annotation of t (K8S-060).
func encodeTerm(t core.Term) string { return strconv.FormatUint(uint64(t), 10) }

// decodeTerm returns the term in the tenure.term annotation of annotations: 0 when there is
// none. ok is false, with term 0, when the annotation is malformed (K8S-060).
func decodeTerm(annotations map[string]string) (t core.Term, ok bool) {
	s, found := annotations[annotationTerm]
	if !found {
		return 0, true
	}
	v, ok := parseUint(s)
	return core.Term(v), ok
}

// decodeSession returns the member Lease UID in the tenure.session annotation of annotations.
// ok is false when the annotation is missing or is not a UUID (K8S-060).
func decodeSession(annotations map[string]string) (uid types.UID, ok bool) {
	s, found := annotations[annotationSession]
	if !found || !isUUID(s) {
		return "", false
	}
	return types.UID(s), true
}

// encodeConfirm returns the tenure.confirm annotation of counter n (K8S-060).
func encodeConfirm(n uint64) string { return strconv.FormatUint(n, 10) }

// decodeConfirm returns the counter in the tenure.confirm annotation of annotations: 0 when
// there is none. ok is false, with 0, when the annotation is malformed (K8S-060).
func decodeConfirm(annotations map[string]string) (n uint64, ok bool) {
	s, found := annotations[annotationConfirm]
	if !found {
		return 0, true
	}
	return parseUint(s)
}

// encodeSlots returns the tenure.slots annotation of slot count n (K8S-060).
func encodeSlots(n int) string { return strconv.Itoa(n) }

// decodeSlots returns the slot count in the tenure.slots annotation of annotations. ok is
// false, with 0, when the annotation is missing, malformed or outside [0, maxSlots]
// (K8S-060).
func decodeSlots(annotations map[string]string) (n int, ok bool) {
	n, ok = parseDecimal(annotations[annotationSlots])
	if !ok || n > maxSlots {
		return 0, false
	}
	return n, true
}

// parseVersion parses the resourceVersion rv of the Lease name: a uint64 in canonical decimal
// (K8S-070).
func parseVersion(name, rv string) (uint64, error) {
	v, ok := parseUint(rv)
	if !ok {
		return 0, fmt.Errorf("k8s: lease %s: resourceVersion %q is not an integer", name, rv)
	}
	return v, nil
}

// parseUint parses a uint64 in canonical decimal: digits only, with no leading zero unless it
// is 0.
func parseUint(s string) (uint64, bool) {
	if !isCanonicalDecimal(s) {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// isUUID reports whether s is a UUID in its text form: 32 hex digits in groups of 8, 4, 4, 4
// and 12, separated by "-".
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHexDigit(c) {
				return false
			}
		}
	}
	return true
}

// isHexDigit reports whether c is a hexadecimal digit, in either case.
func isHexDigit(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

// memberRecord is the JSON of the tenure.member annotation (K8S-060). Fields that are zero are
// omitted, except v and id. Times are RFC 3339 in UTC with as many fractional digits as they
// need. The slot is not part of it: slots come from slot records.
type memberRecord struct {
	V        int               `json:"v"`
	ID       string            `json:"id"`
	Addr     string            `json:"addr,omitempty"`
	Meta     map[string]string `json:"meta,omitempty"`
	Draining bool              `json:"draining,omitempty"`
	JoinAt   string            `json:"joinAt,omitempty"`
	LeaveAt  string            `json:"leaveAt,omitempty"`
}

// encodeMember returns the tenure.member annotation of m.
func encodeMember(m core.Member) string {
	r := memberRecord{
		V:        memberRecordVersion,
		ID:       m.ID,
		Addr:     m.Addr,
		Meta:     m.Meta,
		Draining: m.Draining,
		JoinAt:   encodeTime(m.JoinAt),
		LeaveAt:  encodeTime(m.LeaveAt),
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		// A memberRecord holds only strings, a map of strings, a bool and an int.
		panic(fmt.Sprintf("k8s: encode member record: %v", err))
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// decodeMember returns the member in s, the tenure.member annotation of the member Lease of id,
// with Slot -1. It rejects invalid JSON, a version other than 1, an id other than id and a
// time that is not RFC 3339; it ignores unknown fields.
func decodeMember(id, s string) (core.Member, error) {
	var r memberRecord
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		return core.Member{}, fmt.Errorf("k8s: member record: %w", err)
	}
	if r.V != memberRecordVersion {
		return core.Member{}, fmt.Errorf("k8s: member record: version %d, want %d", r.V, memberRecordVersion)
	}
	if r.ID != id {
		return core.Member{}, fmt.Errorf("k8s: member record: id %q, want %q", r.ID, id)
	}
	m := core.Member{ID: r.ID, Addr: r.Addr, Meta: r.Meta, Draining: r.Draining, Slot: -1}
	var err error
	if m.JoinAt, err = decodeTime("joinAt", r.JoinAt); err != nil {
		return core.Member{}, err
	}
	if m.LeaveAt, err = decodeTime("leaveAt", r.LeaveAt); err != nil {
		return core.Member{}, err
	}
	return m, nil
}

// encodeTime returns t in RFC 3339 in UTC with nanoseconds, or "" for the zero time.
func encodeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// decodeTime parses the time s of the member record field: the zero time for "", else an
// RFC 3339 time, returned in UTC.
func decodeTime(field, s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("k8s: member record: %s %q is not an RFC 3339 time", field, s)
	}
	return t.UTC(), nil
}
