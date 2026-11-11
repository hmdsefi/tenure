// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// The records of the world (SIM §8). A record is part of the trace of a run, and the hash of
// the trace is what shows that a seed replays: so every kind has one function here that emits
// it, with its attributes in one order, and nothing else in the package calls Sim.Emit.
//
// A record belongs to the node it is about: the store, the fence or a member. Only
// tenure.bound is global. The text of a record is the Describe of the message that caused it,
// or empty. An attribute that holds a time or a duration is in nanoseconds, in decimal, and its
// key ends in _ns.
const (
	kindBound     = "tenure.bound"
	kindBoot      = "tenure.boot"
	kindWallStep  = "tenure.wall_step"
	kindSession   = "tenure.session"
	kindGrant     = "tenure.grant"
	kindRelease   = "tenure.release"
	kindConfirm   = "tenure.confirm"
	kindObserve   = "tenure.observe"
	kindSlot      = "tenure.slot"
	kindView      = "tenure.view"
	kindFence     = "tenure.fence"
	kindWrite     = "tenure.write"
	kindInput     = "tenure.input"
	kindAction    = "tenure.action"
	kindReplyLate = "tenure.reply_late"
	kindTimeout   = "tenure.timeout"
	kindRun       = "tenure.run"
	kindExit      = "tenure.exit"
)

// bootCauseBoot is the cause of tenure.boot for an incarnation that starts because its node
// booted. The later causes are "recover" and "activate".
const bootCauseBoot = "boot"

// emit emits one record of the node n, or a global one when n is nil.
func (w *World) emit(kind string, n *kernel.Node, text string, attrs ...kernel.Attr) {
	r := kernel.Record{Kind: kind, Text: text, Attrs: attrs}
	if n != nil {
		r.Node = n.ID()
	}
	w.sim.Emit(r)
}

// emitBound emits tenure.bound, the one global record: takeover, the rule; r_ns, the recovery
// time; b_ns, the bound; g2, whether the run asserts G2.
func (w *World) emitBound() {
	w.emit(kindBound, nil, "",
		recAttr("takeover", w.sc.Takeover.String()),
		recAttr("r_ns", recNanos(time.Duration(w.recovery))),
		recAttr("b_ns", recNanos(w.bound)),
		recAttr("g2", strconv.FormatBool(w.g2)),
	)
}

// emitBoot emits tenure.boot at m for the incarnation that has just started: holder, its
// holder string; cause, what started it ("boot", "recover" or "activate").
func (w *World) emitBoot(m *Member, cause string) {
	w.emit(kindBoot, m.Node, "", recAttr("holder", m.holder), recAttr("cause", cause))
}

// emitExit emits tenure.exit at m, which has just left for good: holder, the holder of its
// last incarnation; cause, why it left ("drain").
func (w *World) emitExit(m *Member, cause string) {
	w.emit(kindExit, m.Node, "", recAttr("holder", m.holder), recAttr("cause", cause))
}

// emitWallStep emits tenure.wall_step at m, whose wall clock has just stepped: delta_ns, the
// step; offset_ns, the offset of the wall clock from the monotonic one after it.
func (w *World) emitWallStep(m *Member, delta time.Duration) {
	w.emit(kindWallStep, m.Node, "", recAttr("delta_ns", recNanos(delta)), recAttr("offset_ns", recNanos(m.wall)))
}

// emitSession emits tenure.session at the store: op, "open", "renew", "end" or "close";
// holder; version, the version of the holder's session after it; and, for "end" only, cause:
// "expired", "replaced", "late" or "end_after". The other ops pass an empty cause.
func (w *World) emitSession(text, op, holder string, version uint64, cause string) {
	attrs := []kernel.Attr{recAttr("op", op), recAttr("holder", holder), recAttr("version", recUint(version))}
	if cause != "" {
		attrs = append(attrs, recAttr("cause", cause))
	}
	w.emit(kindSession, w.store, text, attrs...)
}

// emitGrant emits tenure.grant at the store: the grant as the checker takes it (space, shard,
// holder, term, prev), version, the version of the shard's record after the grant, then ended,
// unseen_ns and lease_ns.
func (w *World) emitGrant(text string, g check.Grant, version uint64) {
	w.emit(kindGrant, w.store, text,
		recAttr("space", g.Space),
		recAttr("shard", strconv.Itoa(g.Shard)),
		recAttr("holder", g.Holder),
		recAttr("term", recTerm(g.Term)),
		recAttr("prev", g.Prev),
		recAttr("version", recUint(version)),
		recAttr("ended", strconv.FormatBool(g.Ended)),
		recAttr("unseen_ns", recNanos(g.Unseen)),
		recAttr("lease_ns", recNanos(g.Lease)),
	)
}

// emitRelease emits tenure.release at the store, for a release by holder of its term t: space
// and shard of rec, the record after the request; holder; term; version, rec's version; result,
// "cleared" when the request cleared the record, "mismatch" when it did not.
func (w *World) emitRelease(text, holder string, t core.Term, rec tenure.Holding, result string) {
	w.emit(kindRelease, w.store, text,
		recAttr("space", rec.Space),
		recAttr("shard", strconv.Itoa(rec.Shard)),
		recAttr("holder", holder),
		recAttr("term", recTerm(t)),
		recAttr("version", recUint(rec.Version)),
		recAttr("result", result),
	)
}

// emitConfirm emits tenure.confirm at the store, for a confirmation by holder of its term t:
// space and shard of rec, the record after the answer; holder; term; version, rec's version;
// record_holder and record_term, what rec names.
func (w *World) emitConfirm(text, holder string, t core.Term, rec tenure.Holding) {
	w.emit(kindConfirm, w.store, text,
		recAttr("space", rec.Space),
		recAttr("shard", strconv.Itoa(rec.Shard)),
		recAttr("holder", holder),
		recAttr("term", recTerm(t)),
		recAttr("version", recUint(rec.Version)),
		recAttr("record_holder", rec.Holder),
		recAttr("record_term", recTerm(rec.Term)),
	)
}

// emitObserve emits tenure.observe at the store, for a request of the observer rule: op,
// "get_shard", "get_slot", "get_session" or "cas"; holder, the sender; then read, the
// attributes of the record or the session the store read, in the caller's order; result, "ok",
// "stale" or "lost".
func (w *World) emitObserve(text, op, holder string, read []kernel.Attr, result string) {
	attrs := make([]kernel.Attr, 0, len(read)+3)
	attrs = append(attrs, recAttr("op", op), recAttr("holder", holder))
	attrs = append(attrs, read...)
	attrs = append(attrs, recAttr("result", result))
	w.emit(kindObserve, w.store, text, attrs...)
}

// emitSlot emits tenure.slot at the store: op, "claim", "release" or "set"; holder; slot, the
// slot claimed or released, or -1 for none; count, the store's slot count after it; result.
func (w *World) emitSlot(text, op, holder string, slot, count int, result string) {
	w.emit(kindSlot, w.store, text,
		recAttr("op", op),
		recAttr("holder", holder),
		recAttr("slot", strconv.Itoa(slot)),
		recAttr("count", strconv.Itoa(count)),
		recAttr("result", result),
	)
}

// emitView emits tenure.view at the store, for a new version of its view v: version; members,
// the member IDs joined by commas; slots, the slot count, and for a view with slots a colon and
// the slot holders in slot order joined by commas, a vacant slot empty ("3:m1/6f…,,m3/09…").
func (w *World) emitView(text string, v core.View) {
	ids := make([]string, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.ID
	}
	slots := strconv.Itoa(v.Slots)
	if v.Slots > 0 {
		slots += ":" + strings.Join(v.SlotHolders, ",")
	}
	w.emit(kindView, w.store, text,
		recAttr("version", recUint(v.Version)),
		recAttr("members", strings.Join(ids, ",")),
		recAttr("slots", slots),
	)
}

// emitFence emits tenure.fence at the fence, for a raise of a shard's fence to t: space, shard,
// term; raised, the largest term the shard's fence has been raised to, after it.
func (w *World) emitFence(text, space string, shard int, t, raised core.Term) {
	w.emit(kindFence, w.fence, text,
		recAttr("space", space),
		recAttr("shard", strconv.Itoa(shard)),
		recAttr("term", recTerm(t)),
		recAttr("raised", recTerm(raised)),
	)
}

// emitWrite emits tenure.write at the fence: the write's space, shard, term and holder; result,
// "accepted" or "rejected".
func (w *World) emitWrite(text string, wr check.Write, result string) {
	w.emit(kindWrite, w.fence, text,
		recAttr("space", wr.Space),
		recAttr("shard", strconv.Itoa(wr.Shard)),
		recAttr("term", recTerm(wr.Term)),
		recAttr("holder", wr.Holder),
		recAttr("result", result),
	)
}

// emitInput emits tenure.input at m, for an input its machine is stepped with: type, such as
// "Tick", then the fields of the input in the order its type declares them. A HolderChanged and
// a ViewChanged end their fields with src, where the driver read the state: "push", "snapshot",
// "reply", "confirm" or "holdings"; a HolderChanged then has version, the version of the record
// state it steps.
func (w *World) emitInput(m *Member, text, typ string, fields ...kernel.Attr) {
	w.emit(kindInput, m.Node, text, recTyped(typ, fields)...)
}

// emitAction emits tenure.action at m, for an action its machine returned: type, such as
// "KeepAlive", then the fields of the action in the order its type declares them.
func (w *World) emitAction(m *Member, text, typ string, fields ...kernel.Attr) {
	w.emit(kindAction, m.Node, text, recTyped(typ, fields)...)
}

// emitReplyLate emits tenure.reply_late at m, for a reply that came after its request timed out
// and is dropped: req, the request ID; type, the reply's type.
func (w *World) emitReplyLate(m *Member, text string, req uint64, typ string) {
	w.emit(kindReplyLate, m.Node, text, recAttr("req", recUint(req)), recAttr("type", typ))
}

// emitTimeout emits tenure.timeout at m, for a request that had no reply for RetryPeriod, in
// the event that steps what its timeout means: req, the request ID; act, the type of the
// machine action the request carries out, as "Acquire", or "" for a request the driver makes
// on its own account. text is the request's.
func (w *World) emitTimeout(m *Member, text string, req uint64, act string) {
	w.emit(kindTimeout, m.Node, text, recAttr("req", recUint(req)), recAttr("act", act))
}

// emitRun emits tenure.run at m: op, "start", "raise", "return", "restart", "stop" or
// "stopped"; then the space, the shard and the term of the run.
func (w *World) emitRun(m *Member, text, op, space string, shard int, t core.Term) {
	w.emit(kindRun, m.Node, text,
		recAttr("op", op),
		recAttr("space", space),
		recAttr("shard", strconv.Itoa(shard)),
		recAttr("term", recTerm(t)),
	)
}

// recAttr returns one attribute of a record.
func recAttr(key, value string) kernel.Attr {
	return kernel.Attr{Key: key, Value: value}
}

// recTyped returns the attributes of an input or an action: its type, then its fields.
func recTyped(typ string, fields []kernel.Attr) []kernel.Attr {
	attrs := make([]kernel.Attr, 0, len(fields)+1)
	attrs = append(attrs, recAttr("type", typ))
	return append(attrs, fields...)
}

// recNanos formats a duration as an attribute: its nanoseconds, in decimal.
func recNanos(d time.Duration) string {
	return strconv.FormatInt(int64(d), 10)
}

// recUint formats a version or a request ID as an attribute.
func recUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// recTerm formats a term as an attribute.
func recTerm(t core.Term) string {
	return strconv.FormatUint(uint64(t), 10)
}
