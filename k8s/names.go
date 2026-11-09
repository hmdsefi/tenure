// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Lease kinds, the values of the tenure.kind label (K8S-020, K8S-050).
const (
	kindMember = "member"
	kindShard  = "shard"
	kindSlots  = "slots"
	kindSlot   = "slot"
)

// Name parts of the Lease kinds (K8S-020).
const (
	memberInfix = "-member-"
	slotInfix   = "-slot-"
	slotsSuffix = "-slots"
)

// Slot limits (K8S-030): slots are 0 to maxSlot, a slot count is 0 to maxSlots.
const (
	maxSlot  = 9999
	maxSlots = maxSlot + 1
)

// maxNameLen is the longest Lease name: a DNS-1123 subdomain.
const maxNameLen = validation.DNS1123SubdomainMaxLength

// reservedSpaces are the space names a group's other Leases use (K8S-030).
var reservedSpaces = []string{"generation", kindMember, "request", kindSlot, kindSlots}

// reservedSpacePrefixes start the space names that would make a shard Lease name look like a
// member or request Lease name (K8S-030).
var reservedSpacePrefixes = []string{"member-", "request-"}

// memberName returns the name of the member Lease of id (K8S-020).
func memberName(group, id string) string { return group + memberInfix + id }

// shardName returns the name of the shard record of space and shard (K8S-020).
func shardName(group, space string, shard int) string {
	return group + "-" + space + "-" + strconv.Itoa(shard)
}

// slotsName returns the name of the Lease that holds the slot count (K8S-020).
func slotsName(group string) string { return group + slotsSuffix }

// slotName returns the name of the record of slot n (K8S-020).
func slotName(group string, n int) string { return group + slotInfix + strconv.Itoa(n) }

// parseName inverts the name builders for the Leases of group: kind is kindMember with id,
// kindShard with space and the shard in n, kindSlots, or kindSlot with the slot in n. ok is
// false for every other name, including a name whose parts break K8S-030.
func parseName(group, name string) (kind, space, id string, n int, ok bool) {
	rest, found := strings.CutPrefix(name, group+"-")
	if !found || !isGroup(group) {
		return "", "", "", 0, false
	}
	if rest == kindSlots {
		return kindSlots, "", "", 0, true
	}
	if m, found := strings.CutPrefix(rest, kindMember+"-"); found {
		if memberDetail(group, m) != "" {
			return "", "", "", 0, false
		}
		return kindMember, "", m, 0, true
	}
	if s, found := strings.CutPrefix(rest, kindSlot+"-"); found {
		if v, ok := parseDecimal(s); ok && v <= maxSlot {
			return kindSlot, "", "", v, true
		}
	}
	i := strings.LastIndexByte(rest, '-')
	if i < 0 {
		return "", "", "", 0, false
	}
	shard, ok := parseDecimal(rest[i+1:])
	if !ok || spaceDetail(rest[:i]) != "" {
		return "", "", "", 0, false
	}
	return kindShard, rest[:i], "", shard, true
}

// parseDecimal parses a non-negative int written in canonical decimal: digits only, with no
// leading zero unless it is 0.
func parseDecimal(s string) (int, bool) {
	if !isCanonicalDecimal(s) {
		return 0, false
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return v, true
}

// isCanonicalDecimal reports whether s is digits only, with no leading zero unless it is "0".
func isCanonicalDecimal(s string) bool {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isDNS1123Label reports whether s is 1 to 63 characters of [a-z0-9-] that start and end with
// a letter or a digit.
func isDNS1123Label(s string) bool { return len(validation.IsDNS1123Label(s)) == 0 }

// isGroup reports whether s is a valid group: a DNS-1123 label of at most maxGroupLen
// characters (K8S-010).
func isGroup(s string) bool { return len(s) <= maxGroupLen && isDNS1123Label(s) }

// spaceDetail returns why space breaks K8S-030's space rule, or "" when it does not.
func spaceDetail(space string) string {
	switch {
	case !isDNS1123Label(space):
		return "must be a DNS-1123 label"
	case slices.Contains(reservedSpaces, space):
		return "is reserved"
	}
	for _, p := range reservedSpacePrefixes {
		if strings.HasPrefix(space, p) {
			return fmt.Sprintf("must not start with %q", p)
		}
	}
	return ""
}

// memberDetail returns why id breaks K8S-030's member rule in group, or "" when it does not.
func memberDetail(group, id string) string {
	if len(validation.IsDNS1123Subdomain(id)) != 0 {
		return "must be a DNS-1123 subdomain"
	}
	if name := memberName(group, id); len(name) > maxNameLen {
		return fmt.Sprintf("makes Lease name %s longer than %d characters", name, maxNameLen)
	}
	return ""
}

// checkHolder returns an error when holder breaks K8S-030's holder rule in group: it is
// <id>/<incarnation>, where id passes the member rule and incarnation is not empty and holds
// neither "/" nor a zero byte. A method wraps the error as "k8s: <Method>: <err>".
func checkHolder(group, holder string) error {
	switch {
	case holder == "":
		return errors.New("empty holder")
	case strings.IndexByte(holder, 0) >= 0:
		return fmt.Errorf("holder %q: contains a zero byte", holder)
	}
	id, inc, found := strings.Cut(holder, "/")
	switch {
	case !found:
		return fmt.Errorf("holder %q: has no \"/\"", holder)
	case inc == "":
		return fmt.Errorf("holder %q: has an empty incarnation", holder)
	case strings.Contains(inc, "/"):
		return fmt.Errorf("holder %q: has a \"/\" in its incarnation", holder)
	}
	if d := memberDetail(group, id); d != "" {
		return fmt.Errorf("holder %q: member ID %s", holder, d)
	}
	return nil
}
