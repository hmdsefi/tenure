// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// auditTrace builds a trace of one member, node 3, for the tests of the audits.
type auditTrace struct {
	recs []kernel.Record
}

func (b *auditTrace) add(r kernel.Record) *auditTrace {
	r.Seq = uint64(len(b.recs) + 1)
	b.recs = append(b.recs, r)
	return b
}

// event starts a kernel event.
func (b *auditTrace) event() *auditTrace { return b.add(kernel.Record{Kind: kindEvent}) }

// boot starts an incarnation.
func (b *auditTrace) boot() *auditTrace { return b.add(kernel.Record{Kind: kindBoot, Node: 3}) }

// input steps an input of type typ at the reading now, with the text and the attributes kv
// (key, value, …) after now_ns.
func (b *auditTrace) input(typ string, now time.Duration, text string, kv ...string) *auditTrace {
	attrs := []kernel.Attr{{Key: "type", Value: typ}, {Key: "now_ns", Value: strconv.FormatInt(int64(now), 10)}}
	return b.add(kernel.Record{Kind: kindInput, Node: 3, Text: text, Attrs: append(attrs, pairs(kv)...)})
}

// action records an action of type typ with the text and the attributes kv.
func (b *auditTrace) action(typ, text string, kv ...string) *auditTrace {
	attrs := []kernel.Attr{{Key: "type", Value: typ}}
	return b.add(kernel.Record{Kind: kindAction, Node: 3, Text: text, Attrs: append(attrs, pairs(kv)...)})
}

// wake records a Wake action at the reading at.
func (b *auditTrace) wake(at time.Duration) *auditTrace {
	return b.action("Wake", "", "at_ns", strconv.FormatInt(int64(at), 10))
}

// late records a late reply to the request n.
func (b *auditTrace) late(n int) *auditTrace {
	return b.add(kernel.Record{Kind: kindReplyLate, Node: 3, Attrs: pairs([]string{"req", strconv.Itoa(n), "type", "renewed"})})
}

func pairs(kv []string) []kernel.Attr {
	attrs := make([]kernel.Attr, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		attrs = append(attrs, kernel.Attr{Key: kv[i], Value: kv[i+1]})
	}
	return attrs
}

// started returns a trace in which the member booted and stepped its first Tick at reading 0,
// which sent keepAlive#1 and armed the wake at 2 s.
func started() *auditTrace {
	b := &auditTrace{}
	return b.boot().event().input("Tick", 0, "").action("KeepAlive", "keepAlive#1 m1/0a lease=15.000000000s").wake(2 * time.Second)
}

// release records a Release of book/3 in term 5 sent as request n.
func (b *auditTrace) release(n int) *auditTrace {
	return b.action("Release", "release#"+strconv.Itoa(n)+" m1/0a book/3 term=5", "space", "book", "shard", "3", "term", "5")
}

// holder steps a HolderChanged of book/3 at the reading now with the version v from src.
func (b *auditTrace) holder(now time.Duration, v int, src string) *auditTrace {
	return b.input("HolderChanged", now, "holder book/3", "space", "book", "shard", "3", "holder", "m1/0a",
		"term", "5", "src", src, "version", strconv.Itoa(v))
}

// TestDriverAuditCatches checks that auditMember accepts a trace that keeps the driver contract
// and reports each break of it that TestATSIM48DriverAudit's audit lists.
func TestDriverAuditCatches(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name  string
		trace *auditTrace
		want  string // "" for a trace the audit accepts
	}{
		{"kept", started().event().input("SessionRenewed", 5*ms, "renewed#1 version=1").wake(2*time.Second).
			event().input("Tick", 2*time.Second, "").release(2).wake(4*time.Second).
			event().input("Tick", 4*time.Second, "").release(3).
			event().holder(4*time.Second, 5, "push").holder(4*time.Second, 5, "confirm").
			event().input("Acquired", 4*time.Second, "granted#9 book/3 holder=m1/0a term=6 version=8", "space", "book", "shard", "3", "term", "6").
			holder(4*time.Second, 9, "snapshot"), ""},
		{"first input", (&auditTrace{}).boot().event().input("SessionRenewed", 0, ""), "want a Tick"},
		{"reading back", started().event().input("Tick", -ms, ""), "after reading"},
		{"no tick at the wake", started().event().input("SessionRenewed", 2*time.Second, "renewed#1 version=1"), "with no Tick first"},
		{"action alone", started().event().action("KeepAlive", ""), "no input before it"},
		{"answered twice", started().event().input("SessionRenewed", ms, "renewed#1 version=1").
			event().input("SessionRenewed", 2*ms, "renewed#1 version=1"), "again, in a later event"},
		{"answered after late", started().late(1).event().input("SessionRenewed", ms, "renewed#1 version=1"), "after its reply came late"},
		{"late twice", started().late(1).late(1), "answered or late before"},
		{"sent twice", started().event().input("Tick", ms, "").action("KeepAlive", "keepAlive#1 m1/0a"), "sent twice"},
		{"release in flight", started().event().input("Tick", ms, "").release(2).
			event().input("Tick", time.Second, "").release(3), "while request 2 of it is in flight"},
		{"version repeated", started().event().holder(ms, 5, "push").holder(ms, 5, "snapshot"), "at version 5, the machine had version 5"},
		{"version below a grant", started().event().input("Acquired", ms, "granted#9 book/3 holder=m1/0a term=6 version=8", "space", "book", "shard", "3", "term", "6").
			holder(ms, 7, "push"), "at version 7, the machine had version 8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := auditMember(tt.trace.recs, 3, "m1", 2*time.Second)
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("audit = %v, want nil", err)
			case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
				t.Errorf("audit = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

// TestTermsGrowCatches checks that termsGrow reports a term that does not grow and a term that
// is not NextTerm of the previous one and the wall reading, and counts the grants of step 3.
func TestTermsGrowCatches(t *testing.T) {
	wallMs := kernel.Epoch.UnixMilli()
	grant := func(at time.Duration, holder string, term int64, wall string) kernel.Record {
		return kernel.Record{Kind: kindGrant, At: kernel.Time(at), Text: "cas#1 m1/0a book/3 expect=0 wall=" + wall + " unseen=0.000000000s",
			Attrs: pairs([]string{"space", "book", "shard", "3", "holder", holder, "term", strconv.FormatInt(term, 10)})}
	}
	step := kernel.Record{Kind: kindWallStep, Node: 3, Attrs: pairs([]string{"delta_ns", "-5"})}
	names := kernel.Record{Kind: "kernel.add_node", Node: 3, Text: "m1"}

	recs := []kernel.Record{
		names,
		grant(0, "m2/0b", wallMs+10_000, "10.000000000s"),
		step,
		grant(time.Second, "m1/0a", wallMs+10_001, "5.000000000s"),
	}
	cover, err := termsGrow(recs, world.Observer)
	if err != nil || cover != (termCover{fromPrev: 1, afterBackStep: 1}) {
		t.Errorf("termsGrow = %+v, %v; want one grant of each kind and no error", cover, err)
	}
	recs[3] = grant(time.Second, "m1/0a", wallMs+10_000, "5.000000000s")
	if _, err := termsGrow(recs, world.Observer); err == nil || !strings.Contains(err.Error(), "after term") {
		t.Errorf("termsGrow = %v, want a term that does not grow", err)
	}
	recs[3] = grant(time.Second, "m1/0a", wallMs+10_005, "5.000000000s")
	if _, err := termsGrow(recs, world.Observer); err == nil || !strings.Contains(err.Error(), "want NextTerm") {
		t.Errorf("termsGrow = %v, want a term that is not NextTerm's", err)
	}
	server := []kernel.Record{{Kind: kindGrant, At: kernel.Time(3 * time.Second), Attrs: pairs([]string{
		"space", "book", "shard", "3", "holder", "m1/0a", "term", strconv.FormatUint(uint64(core.NextTerm(0, kernel.Epoch.Add(3*time.Second))), 10),
	})}}
	if _, err := termsGrow(server, world.Server); err != nil {
		t.Errorf("termsGrow (server) = %v, want nil", err)
	}
}
