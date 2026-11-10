// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// AT-SIM-12 (SIM-280): under the Server rule, with no EndAfter, the store model gives the
// results memstore gives for the same operations and the same clock readings. Every sequence
// below runs once against memstore.New(clock) and once against newStoreModel(Server, 0, 0), with
// one fake clock: each call of the model gets the monotonic reading and the wall time that
// memstore reads for the same call. Terms, held errors with their RetryAfter, ErrSessionLost,
// ErrNoSlot, Confirm and Holdings results, Slots counts and holders, and slot indices must be
// equal. Record versions must agree in order: every model version maps to one memstore
// version, and the mapping is strictly increasing.
//
// Only well-formed operations are compared: the model checks no argument, so memstore's
// argument errors have no counterpart. A sequence that gives memstore a malformed operation
// fails.
//
// After every operation the test also compares Holdings of both spaces the sequences use, which
// neither store counts as a read of its clock, so record versions are compared at every step.

// diffCases are the cases of AT-SIM-12; each sequence declares the ones it covers, and together
// the sequences cover them all. The original list is lost, so this one was written from the
// issue and the store rules. The last four go beyond it.
var diffCases = []string{
	"open",                    // a holder's first KeepAlive opens its session
	"renew",                   // a KeepAlive in time renews the session
	"renew-new-timing",        // a renewal stores its new timing: lease and RetryPeriod
	"late-renewal",            // a renewal at or after expiry ends the session
	"replaced",                // a newer incarnation of the same member ends the session
	"acquire-absent",          // Acquire of an absent record
	"regrant",                 // Acquire by the record's own holder grants again
	"held-retry",              // held, with RetryAfter the acquirer's own RetryPeriod
	"takeover-at-expiry",      // a takeover exactly at the holder's LeaseDuration
	"held-before-expiry",      // held 1 ns before it
	"wall-behind",             // a wall clock behind the record's term: the term still grows
	"release-own",             // Release by the holder in its term
	"release-wrong-term",      // Release with a wrong term: no change, nil
	"release-other-holder",    // Release by another holder: no change, nil
	"release-ended",           // Release by a holder whose session has ended
	"confirm-own",             // Confirm of the caller's own record
	"confirm-other",           // Confirm of a record another holder holds
	"confirm-absent",          // Confirm of an absent record
	"close-clears",            // CloseSession clears records and slots
	"close-unknown",           // CloseSession of a holder the store never heard of
	"holdings-sorted-cleared", // Holdings sorted by shard, cleared records included
	"claim-lowest-free",       // ClaimSlot takes the lowest free slot
	"claim-own",               // ClaimSlot of a holder's own slot below the count
	"claim-retired",           // ClaimSlot of a holder whose slot is at or above the count
	"claim-none",              // ClaimSlot with no free slot
	"release-slot",            // ReleaseSlot, of the holder's slot and of others
	"setslots-up",             // SetSlots raises the count
	"setslots-down",           // SetSlots lowers it, leaving retired slots held
	"after-end",               // operations after the session ended
	// Beyond the list:
	"mono-back",      // a monotonic reading behind the store's time counts as that time
	"slots-zero",     // Slots at a count of 0 reads no clock
	"slot-expired",   // a slot whose holder's session expired is free
	"release-absent", // Release of an absent record
}

// diffSequenceCount is the number of sequences the issue asks for.
const diffSequenceCount = 40

// The holders of the sequences: diffA2 is a later incarnation of diffA's member m1.
const (
	diffA  = "m1/a"
	diffA2 = "m1/b"
	diffB  = "m2/b"
	diffC  = "m3/c"
	diffD  = "m4/d"
	diffZ  = "m9/z"
)

// The spaces of the sequences. After every operation both are compared.
const (
	diffBook = "book"
	diffPage = "page"
)

// The timings of the sequences; each RetryPeriod differs, so a held error shows whose it is.
var (
	diffTiming       = core.DefaultTiming // 15 s lease, 2 s retry
	diffTimingRetry3 = core.Timing{
		LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second,
		RetryPeriod: 3 * time.Second, SlotGrace: 60 * time.Second,
	}
	diffTimingShort = core.Timing{
		LeaseDuration: 5 * time.Second, RenewDeadline: 3 * time.Second,
		RetryPeriod: time.Second, SlotGrace: 60 * time.Second,
	}
	diffTimingLong = core.Timing{
		LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second,
		RetryPeriod: 4 * time.Second, SlotGrace: 60 * time.Second,
	}
)

// diffWall0 is the wall time at the start of every sequence: 1000 ms after 1970, so that a term
// granted on an absent record at monotonic reading d is 1000 plus d in milliseconds.
var diffWall0 = time.UnixMilli(1000).UTC()

// diffClock is the fake memstore.Clock of a sequence; the model reads the same fields.
type diffClock struct {
	mono time.Duration
	wall time.Time
}

// Monotonic returns the monotonic reading.
func (c *diffClock) Monotonic() time.Duration { return c.mono }

// Wall returns the wall time.
func (c *diffClock) Wall() time.Time { return c.wall }

// diffKind is an operation of a sequence.
type diffKind uint8

// The operations: the store calls, then the clock steps, which call no store.
const (
	diffKeepAlive diffKind = iota
	diffAcquire
	diffRelease
	diffConfirm
	diffClose
	diffHoldings
	diffClaimSlot
	diffReleaseSlot
	diffSlots
	diffSetSlots
	diffAdvance // both clocks move forward by d
	diffMonoAt  // the monotonic reading is set to d
	diffWallAt  // the wall time is set to wall
)

// diffKindNames are the names of the operations in failures.
var diffKindNames = [...]string{
	"KeepAlive", "Acquire", "Release", "Confirm", "CloseSession", "Holdings", "ClaimSlot",
	"ReleaseSlot", "Slots", "SetSlots", "advance", "monotonic", "wall",
}

// diffOp is one operation of a sequence.
type diffOp struct {
	kind   diffKind
	holder string
	timing core.Timing
	space  string
	shard  int
	// term is the term of a Release or Confirm; 0 means the term of the shard's record in
	// memstore plus delta.
	term  core.Term
	delta int64
	slot  int
	count int
	d     time.Duration
	wall  time.Time
	// want, when set, is the text memstore's result must have (see diffResult.text).
	want string
}

// expect returns op with the text its result must have.
func (op diffOp) expect(want string) diffOp {
	op.want = want
	return op
}

// The constructors of the operations.

func diffKA(holder string, timing core.Timing) diffOp {
	return diffOp{kind: diffKeepAlive, holder: holder, timing: timing}
}

func diffAcq(holder, space string, shard int) diffOp {
	return diffOp{kind: diffAcquire, holder: holder, space: space, shard: shard}
}

// diffRel releases at the record's term plus delta.
func diffRel(holder, space string, shard int, delta int64) diffOp {
	return diffOp{kind: diffRelease, holder: holder, space: space, shard: shard, delta: delta}
}

// diffRelT releases at a given term.
func diffRelT(holder, space string, shard int, term core.Term) diffOp {
	return diffOp{kind: diffRelease, holder: holder, space: space, shard: shard, term: term}
}

// diffConf confirms at the record's term.
func diffConf(holder, space string, shard int) diffOp {
	return diffOp{kind: diffConfirm, holder: holder, space: space, shard: shard}
}

// diffConfT confirms at a given term.
func diffConfT(holder, space string, shard int, term core.Term) diffOp {
	return diffOp{kind: diffConfirm, holder: holder, space: space, shard: shard, term: term}
}

func diffCloseS(holder string) diffOp { return diffOp{kind: diffClose, holder: holder} }

func diffHold(space string) diffOp { return diffOp{kind: diffHoldings, space: space} }

func diffClaim(holder string) diffOp { return diffOp{kind: diffClaimSlot, holder: holder} }

func diffRelSlot(holder string, slot int) diffOp {
	return diffOp{kind: diffReleaseSlot, holder: holder, slot: slot}
}

func diffSlotsOp() diffOp { return diffOp{kind: diffSlots} }

func diffSet(count int) diffOp { return diffOp{kind: diffSetSlots, count: count} }

func diffAdv(d time.Duration) diffOp { return diffOp{kind: diffAdvance, d: d} }

func diffMono(d time.Duration) diffOp { return diffOp{kind: diffMonoAt, d: d} }

func diffWall(wall time.Time) diffOp { return diffOp{kind: diffWallAt, wall: wall.UTC()} }

// diffSequence is one sequence: a name that says what it covers, the cases it covers, and the
// operations.
type diffSequence struct {
	name  string
	cases []string
	ops   []diffOp
}

// diffResult is the result of one store call, as both stores give it.
type diffResult struct {
	kind diffKind
	// err is "lost" for ErrSessionLost, "noslot" for ErrNoSlot, "held" for a HeldError, else "".
	err string
	// held is the HeldError's holder and RetryAfter.
	held tenure.HeldError
	// term is the term Acquire granted, slot the slot ClaimSlot gave (-1 for none).
	term core.Term
	slot int
	// recs is the record of Confirm, or the records of Holdings.
	recs []tenure.Holding
	// count and holders are the result of Slots.
	count   int
	holders []string
}

// text returns the result as a sequence's want states it, for example "lost",
// "held by m2/b retry 3s", "term=16000", "book/0 m1/a t1000 v1" for Confirm,
// "[book/0 - t1000 v2, book/1 m2/b t1001 v3]" or "nil" for Holdings, "slot 1", "noslot",
// "count 2 [m1/a -]" or "count 0 nil" for Slots, and "ok" for the other calls.
func (r diffResult) text() string {
	switch r.err {
	case "lost", "noslot":
		return r.err
	case "held":
		return fmt.Sprintf("held by %s retry %v", r.held.Holder, r.held.RetryAfter)
	}
	switch r.kind {
	case diffAcquire:
		return fmt.Sprintf("term=%d", r.term)
	case diffConfirm:
		return diffRecText(r.recs[0])
	case diffHoldings:
		if r.recs == nil {
			return "nil"
		}
		texts := make([]string, len(r.recs))
		for i, rec := range r.recs {
			texts[i] = diffRecText(rec)
		}
		return "[" + strings.Join(texts, ", ") + "]"
	case diffClaimSlot:
		return fmt.Sprintf("slot %d", r.slot)
	case diffSlots:
		if r.holders == nil {
			return fmt.Sprintf("count %d nil", r.count)
		}
		hs := make([]string, len(r.holders))
		for i, h := range r.holders {
			hs[i] = diffOr(h, "-")
		}
		return fmt.Sprintf("count %d [%s]", r.count, strings.Join(hs, " "))
	default:
		return "ok"
	}
}

// diffRecText returns a record as "book/0 m1/a t1000 v1", with "-" for no holder.
func diffRecText(rec tenure.Holding) string {
	return fmt.Sprintf("%s/%d %s t%d v%d", rec.Space, rec.Shard, diffOr(rec.Holder, "-"), rec.Term, rec.Version)
}

// diffOr returns s, or alt when s is empty.
func diffOr(s, alt string) string {
	if s == "" {
		return alt
	}
	return s
}

// diffVersions maps the record versions of the model to those of memstore. The mapping must be
// a function, one to one, strictly increasing, and map 0, no record, to 0.
type diffVersions struct {
	pairs [][2]uint64 // model version, memstore version
}

// match records that model version a showed where memstore version b did.
func (v *diffVersions) match(a, b uint64) error {
	if a == 0 || b == 0 {
		if a != b {
			return fmt.Errorf("model version %d where memstore has %d", a, b)
		}
		return nil
	}
	for _, p := range v.pairs {
		if p[0] == a && p[1] == b {
			return nil
		}
		if (p[0] < a) != (p[1] < b) || p[0] == a || p[1] == b {
			return fmt.Errorf("model version %d against memstore %d breaks the order of model %d against memstore %d",
				a, b, p[0], p[1])
		}
	}
	v.pairs = append(v.pairs, [2]uint64{a, b})
	return nil
}

// sameRecords compares the records of the model with those of memstore: every field equal but
// the version, and the versions matched. It returns the model's records with memstore's
// versions, for the comparison of the rest of a result.
func (v *diffVersions) sameRecords(model, mem []tenure.Holding) ([]tenure.Holding, error) {
	if len(model) != len(mem) {
		return model, nil // the texts differ
	}
	out := slices.Clone(model)
	for i := range out {
		if err := v.match(out[i].Version, mem[i].Version); err != nil {
			return model, fmt.Errorf("%s against %s: %w", diffRecText(out[i]), diffRecText(mem[i]), err)
		}
		out[i].Version = mem[i].Version
	}
	return out, nil
}

// diffRunner runs one sequence against both stores.
type diffRunner struct {
	t     *testing.T
	ctx   context.Context
	clock *diffClock
	mem   *memstore.Store
	model *storeModel
	vers  diffVersions
	id    uint64
}

// TestATSIM12StoreMatchesMemstore checks the store model against memstore. The steps of the
// sequences were written for this test from the issue's text and the design of the store
// model, because the original acceptance-test text is unavailable.
func TestATSIM12StoreMatchesMemstore(t *testing.T) {
	seqs := diffSequences()
	if len(seqs) != diffSequenceCount {
		t.Fatalf("%d sequences, want %d", len(seqs), diffSequenceCount)
	}
	known := make(map[string]bool, len(diffCases))
	for _, c := range diffCases {
		known[c] = true
	}
	covered := make(map[string]bool)
	names := make(map[string]bool)
	for _, s := range seqs {
		if names[s.name] {
			t.Errorf("sequence %s: name used twice", s.name)
		}
		names[s.name] = true
		if len(s.cases) == 0 {
			t.Errorf("sequence %s covers no case", s.name)
		}
		for _, c := range s.cases {
			if !known[c] {
				t.Errorf("sequence %s: unknown case %q", s.name, c)
			}
			covered[c] = true
		}
	}
	for _, c := range diffCases {
		if !covered[c] {
			t.Errorf("no sequence covers case %q", c)
		}
	}
	for _, s := range seqs {
		t.Run(s.name, func(t *testing.T) {
			clock := &diffClock{wall: diffWall0}
			r := &diffRunner{
				t: t, ctx: context.Background(), clock: clock,
				mem: memstore.New(clock), model: newStoreModel(Server, 0, 0),
			}
			for i, op := range s.ops {
				r.step(i, op)
			}
		})
	}
}

// step runs one operation and compares the results, then Holdings of both spaces.
func (r *diffRunner) step(i int, op diffOp) {
	t := r.t
	t.Helper()
	switch op.kind {
	case diffAdvance:
		r.clock.mono += op.d
		r.clock.wall = r.clock.wall.Add(op.d)
		return
	case diffMonoAt:
		r.clock.mono = op.d
		return
	case diffWallAt:
		r.clock.wall = op.wall
		return
	}
	where := fmt.Sprintf("op %d %s %s at %v", i, diffKindNames[op.kind], op.holder, r.clock.mono)
	if (op.kind == diffRelease || op.kind == diffConfirm) && op.term == 0 {
		op.term = r.recordTerm(op.space, op.shard)
		switch {
		case op.delta > 0:
			op.term += core.Term(op.delta)
		case op.delta < 0:
			op.term -= core.Term(-op.delta)
		}
		if op.term == 0 || op.term > 1<<62 {
			t.Fatalf("%s: term 0 is not a well-formed argument", where)
		}
	}
	mem := r.callMem(where, op)
	model := r.callModel(where, op)
	r.compare(where, model, mem)
	if op.want != "" && mem.text() != op.want {
		t.Errorf("%s: memstore gives %q, the sequence wants %q", where, mem.text(), op.want)
	}
	for _, space := range []string{diffBook, diffPage} {
		check := diffOp{kind: diffHoldings, space: space}
		r.compare(where+" then Holdings "+space, r.callModel(where, check), r.callMem(where, check))
	}
}

// recordTerm returns the term of the shard's record in memstore, 0 when there is none.
func (r *diffRunner) recordTerm(space string, shard int) core.Term {
	recs, err := r.mem.Holdings(r.ctx, space)
	if err != nil {
		r.t.Fatalf("memstore Holdings: %v", err)
	}
	for _, rec := range recs {
		if rec.Shard == shard {
			return rec.Term
		}
	}
	return 0
}

// compare compares the results of the two stores.
func (r *diffRunner) compare(where string, model, mem diffResult) {
	r.t.Helper()
	recs, err := r.vers.sameRecords(model.recs, mem.recs)
	if err != nil {
		r.t.Errorf("%s: %v", where, err)
	}
	model.recs = recs
	if model.text() != mem.text() || model.slot != mem.slot || model.term != mem.term {
		r.t.Errorf("%s: model gives %q (term %d, slot %d), memstore %q (term %d, slot %d)",
			where, model.text(), model.term, model.slot, mem.text(), mem.term, mem.slot)
	}
}

// callMem calls memstore and returns its result. An error other than ErrSessionLost, ErrNoSlot
// and a HeldError is an argument error: the sequence is malformed.
func (r *diffRunner) callMem(where string, op diffOp) diffResult {
	t := r.t
	t.Helper()
	res := diffResult{kind: op.kind, slot: -1}
	var err error
	switch op.kind {
	case diffKeepAlive:
		if verr := op.timing.Validate(); verr != nil {
			t.Fatalf("%s: malformed timing: %v", where, verr)
		}
		err = r.mem.KeepAlive(r.ctx, op.holder, op.timing)
	case diffAcquire:
		res.term, err = r.mem.Acquire(r.ctx, op.holder, op.space, op.shard)
	case diffRelease:
		err = r.mem.Release(r.ctx, op.holder, op.space, op.shard, op.term)
	case diffConfirm:
		var rec tenure.Holding
		rec, err = r.mem.Confirm(r.ctx, op.holder, op.space, op.shard, op.term)
		if err == nil {
			res.recs = []tenure.Holding{rec}
		}
	case diffClose:
		err = r.mem.CloseSession(r.ctx, op.holder)
	case diffHoldings:
		res.recs, err = r.mem.Holdings(r.ctx, op.space)
	case diffClaimSlot:
		res.slot, err = r.mem.ClaimSlot(r.ctx, op.holder)
	case diffReleaseSlot:
		err = r.mem.ReleaseSlot(r.ctx, op.holder, op.slot)
	case diffSlots:
		res.count, res.holders, err = r.mem.Slots(r.ctx)
	case diffSetSlots:
		err = r.mem.SetSlots(r.ctx, op.count)
	default:
		t.Fatalf("%s: unknown operation %d", where, op.kind)
	}
	var held *tenure.HeldError
	switch {
	case err == nil:
	case errors.Is(err, tenure.ErrSessionLost):
		res.err = "lost"
	case errors.Is(err, tenure.ErrNoSlot):
		res.err = "noslot"
	case errors.As(err, &held):
		res.err, res.held = "held", *held
	default:
		t.Fatalf("%s: memstore: %v (a malformed operation)", where, err)
	}
	if err != nil && (res.term != 0 || (op.kind == diffClaimSlot && res.slot != -1)) {
		t.Errorf("%s: memstore returns term %d and slot %d with %v", where, res.term, res.slot, err)
	}
	return res
}

// callModel sends the operation to the model with the clock's readings and returns its result.
func (r *diffRunner) callModel(where string, op diffOp) diffResult {
	t := r.t
	t.Helper()
	res := diffResult{kind: op.kind, slot: -1}
	if op.kind == diffSlots {
		// memstore's Slots: expire at the reading and the view, or the view alone at a slot
		// count of 0, where memstore reads no clock.
		if r.model.slotCount > 0 {
			r.model.expire(r.clock.mono)
		}
		v := r.model.currentView()
		res.count, res.holders = v.Slots, v.SlotHolders
		r.model.takeEffects()
		return res
	}
	r.id++
	head := reqHead{ID: r.id, Holder: op.holder}
	var req request
	switch op.kind {
	case diffKeepAlive:
		req = keepAliveReq{reqHead: head, Timing: op.timing}
	case diffAcquire:
		req = acquireReq{reqHead: head, Space: op.space, Shard: op.shard}
	case diffRelease:
		req = releaseReq{reqHead: head, Space: op.space, Shard: op.shard, Term: op.term}
	case diffConfirm:
		req = confirmReq{reqHead: head, Space: op.space, Shard: op.shard, Term: op.term}
	case diffClose:
		req = closeSessionReq{reqHead: head}
	case diffHoldings:
		req = holdingsReq{reqHead: reqHead{ID: r.id, Holder: diffZ}, Space: op.space}
	case diffClaimSlot:
		req = claimSlotReq{reqHead: head}
	case diffReleaseSlot:
		req = releaseSlotReq{reqHead: head, Slot: op.slot}
	case diffSetSlots:
		req = setSlotsReq{reqHead: reqHead{ID: r.id, Holder: diffZ}, Count: op.count}
	default:
		t.Fatalf("%s: unknown operation %d", where, op.kind)
	}
	rep := r.model.handle(r.clock.mono, r.clock.wall, req)
	r.model.takeEffects()
	if rep.requestID() != r.id {
		t.Errorf("%s: model reply %s answers request %d, want %d", where, rep.Describe(), rep.requestID(), r.id)
	}
	switch x := rep.(type) {
	case lostReply:
		res.err = "lost"
	case noSlotReply:
		res.err = "noslot"
	case heldReply:
		res.err, res.held = "held", tenure.HeldError{Holder: x.Record.Holder, RetryAfter: x.RetryAfter}
	case grantedReply:
		res.term = x.Record.Term
	case confirmedReply:
		res.recs = []tenure.Holding{x.Record}
	case holdingsReply:
		res.recs = x.Records
	case slotClaimedReply:
		res.slot = x.Slot
	case renewedReply, releasedReply, closedReply, slotReleasedReply, slotsSetReply:
	default:
		t.Fatalf("%s: unexpected model reply %s", where, rep.Describe())
	}
	if !diffReplyFits(op.kind, rep) {
		t.Errorf("%s: model reply %s does not answer this call", where, rep.Describe())
	}
	return res
}

// diffReplyFits reports whether rep is a reply the model may give to a call of kind.
func diffReplyFits(kind diffKind, rep reply) bool {
	switch rep.(type) {
	case lostReply:
		return kind == diffKeepAlive || kind == diffAcquire || kind == diffConfirm || kind == diffClaimSlot
	case renewedReply:
		return kind == diffKeepAlive
	case grantedReply, heldReply:
		return kind == diffAcquire
	case releasedReply:
		return kind == diffRelease
	case confirmedReply:
		return kind == diffConfirm
	case closedReply:
		return kind == diffClose
	case holdingsReply:
		return kind == diffHoldings
	case slotClaimedReply, noSlotReply:
		return kind == diffClaimSlot
	case slotReleasedReply:
		return kind == diffReleaseSlot
	case slotsSetReply:
		return kind == diffSetSlots
	}
	return false
}

// diffSequences returns the 40 sequences.
func diffSequences() []diffSequence {
	const ns = time.Nanosecond
	s := time.Second
	return []diffSequence{
		// Sessions.
		{"open_then_renew", []string{"open", "renew"}, []diffOp{
			diffKA(diffA, diffTiming).expect("ok"),
			diffAdv(5 * s),
			diffKA(diffA, diffTiming).expect("ok"),
			diffAdv(14 * s),
			diffKA(diffA, diffTiming).expect("ok"),
			diffAdv(15*s - ns),
			diffKA(diffA, diffTiming).expect("ok"),
		}},
		{"renew_with_longer_lease", []string{"renew-new-timing", "renew"}, []diffOp{
			diffKA(diffA, diffTimingShort).expect("ok"),
			diffAdv(4 * s),
			diffKA(diffA, diffTimingLong).expect("ok"),
			diffAdv(20 * s),
			diffKA(diffA, diffTimingLong).expect("ok"),
		}},
		{"renew_with_shorter_lease_then_late", []string{"renew-new-timing", "late-renewal"}, []diffOp{
			diffKA(diffA, diffTimingLong).expect("ok"),
			diffAdv(10 * s),
			diffKA(diffA, diffTimingShort).expect("ok"),
			diffAdv(5 * s),
			diffKA(diffA, diffTimingShort).expect("lost"),
		}},
		{"renew_with_new_retry_period", []string{"renew-new-timing", "held-retry"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffAcq(diffB, diffBook, 0).expect("term=1000"),
			diffAcq(diffA, diffBook, 0).expect("held by m2/b retry 2s"),
			diffKA(diffA, diffTimingRetry3).expect("ok"),
			diffAcq(diffA, diffBook, 0).expect("held by m2/b retry 3s"),
		}},
		{"late_renewal_at_expiry", []string{"late-renewal", "after-end"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffAdv(5 * s),
			diffKA(diffA, diffTimingShort).expect("lost"),
			diffAdv(s),
			diffKA(diffA, diffTimingShort).expect("lost"),
			diffAcq(diffA, diffBook, 0).expect("lost"),
			diffConfT(diffA, diffBook, 0, 1).expect("lost"),
			diffSet(1),
			diffClaim(diffA).expect("lost"),
			diffHold(diffBook).expect("nil"),
		}},
		{"renewal_just_before_expiry_and_clock_behind", []string{"renew", "mono-back"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffAdv(5*s - ns),
			diffKA(diffA, diffTimingShort).expect("ok"), // renewed at 5 s - 1 ns
			diffMono(s),
			diffKA(diffA, diffTimingShort).expect("ok"), // the store's time stays 5 s - 1 ns
			diffMono(10*s - 2*ns),
			diffKA(diffA, diffTimingShort).expect("ok"), // 1 ns before expiry
			diffMono(3 * s),
			diffKA(diffA, diffTimingShort).expect("ok"),
			diffMono(15*s - 2*ns),
			diffKA(diffA, diffTimingShort).expect("lost"), // at expiry
		}},
		{"slots_at_count_zero_read_no_clock", []string{"slots-zero", "mono-back", "setslots-up"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffMono(10 * s),
			diffSlotsOp().expect("count 0 nil"),
			diffMono(4 * s),
			diffKA(diffA, diffTimingShort).expect("ok"), // Slots read no clock: the time is 4 s
			diffSet(1),
			diffMono(10 * s),
			diffSlotsOp().expect("count 1 [-]"),
			diffMono(4 * s),
			diffKA(diffA, diffTimingShort).expect("lost"), // Slots read 10 s
		}},
		{"replaced_by_new_incarnation", []string{"replaced", "after-end", "regrant"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAdv(s),
			diffKA(diffA2, diffTiming).expect("ok"),
			diffKA(diffA, diffTiming).expect("lost"),
			diffAcq(diffA, diffBook, 1).expect("lost"),
			diffAcq(diffA2, diffBook, 0).expect("term=2000"),
			diffHold(diffBook).expect("[book/0 m1/b t2000 v2]"),
			// A member ID without "/" is the whole holder.
			diffKA("m7", diffTiming).expect("ok"),
			diffKA("m7/a", diffTiming).expect("ok"),
			diffKA("m7", diffTiming).expect("lost"),
			diffKA("m7/b", diffTiming).expect("ok"),
			diffKA("m7/a", diffTiming).expect("lost"),
		}},
		{"replacement_frees_slot", []string{"replaced", "claim-lowest-free", "setslots-up"}, []diffOp{
			diffSet(2),
			diffKA(diffA, diffTiming),
			diffClaim(diffA).expect("slot 0"),
			diffKA(diffA2, diffTiming),
			diffSlotsOp().expect("count 2 [- -]"),
			diffClaim(diffA2).expect("slot 0"),
			diffSlotsOp().expect("count 2 [m1/b -]"),
		}},

		// Acquire.
		{"acquire_absent_records", []string{"acquire-absent"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAdv(time.Millisecond),
			diffAcq(diffA, diffBook, 1).expect("term=1001"),
			diffAdv(s),
			diffAcq(diffA, diffPage, 0).expect("term=2001"),
			diffHold(diffBook).expect("[book/0 m1/a t1000 v1, book/1 m1/a t1001 v2]"),
			diffConf(diffA, diffPage, 0).expect("page/0 m1/a t2001 v3"),
			diffRel(diffA, diffPage, 0, 0).expect("ok"),
			diffConfT(diffA, diffPage, 0, 2001).expect("page/0 - t2001 v4"),
		}},
		{"regrant_to_self", []string{"regrant", "confirm-own"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAcq(diffA, diffBook, 4).expect("term=1000"),
			diffAdv(2 * s),
			diffAcq(diffA, diffBook, 0).expect("term=3000"),
			diffConf(diffA, diffBook, 0).expect("book/0 m1/a t3000 v3"),
			diffConf(diffA, diffBook, 4).expect("book/4 m1/a t1000 v2"),
		}},
		{"regrant_with_wall_behind", []string{"regrant", "wall-behind"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffWall(time.UnixMilli(500)),
			diffAcq(diffA, diffBook, 0).expect("term=1001"),
			diffWall(time.UnixMilli(0)),
			diffAcq(diffA, diffBook, 0).expect("term=1002"),
		}},
		{"held_with_acquirer_retry_period", []string{"held-retry", "regrant"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTimingRetry3),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAcq(diffB, diffBook, 0).expect("held by m1/a retry 3s"),
			diffAcq(diffA, diffBook, 0).expect("term=1001"),
		}},
		{"held_retry_of_each_acquirer", []string{"held-retry"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffKA(diffB, diffTimingRetry3),
			diffKA(diffC, diffTimingLong),
			diffAcq(diffC, diffBook, 0).expect("term=1000"),
			diffAcq(diffA, diffBook, 0).expect("held by m3/c retry 1s"),
			diffAcq(diffB, diffBook, 0).expect("held by m3/c retry 3s"),
		}},
		{"takeover_exactly_at_expiry", []string{"takeover-at-expiry", "confirm-own"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAdv(10 * s),
			diffKA(diffB, diffTiming),
			diffAdv(5 * s),
			diffAcq(diffB, diffBook, 0).expect("term=16000"),
			diffConf(diffB, diffBook, 0).expect("book/0 m2/b t16000 v2"),
		}},
		{"held_just_before_expiry", []string{"held-before-expiry", "takeover-at-expiry"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffAdv(10 * s),
			diffKA(diffB, diffTiming),
			diffAdv(5*s - ns),
			diffAcq(diffB, diffBook, 0).expect("held by m1/a retry 2s"),
			diffAdv(ns),
			diffAcq(diffB, diffBook, 0).expect("term=16000"),
		}},
		{"takeover_judged_by_holder_lease", []string{"held-before-expiry", "takeover-at-expiry"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffKA(diffB, diffTimingLong),
			diffAcq(diffA, diffBook, 0),
			diffAdv(5*s - ns),
			diffAcq(diffB, diffBook, 0).expect("held by m1/a retry 4s"),
			diffAdv(ns),
			diffAcq(diffB, diffBook, 0).expect("term=6000"),
		}},
		{"takeover_after_renewal_with_new_lease", []string{"renew-new-timing", "held-before-expiry", "takeover-at-expiry"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffKA(diffB, diffTimingLong),
			diffAcq(diffA, diffBook, 0),
			diffAdv(4 * s),
			diffKA(diffA, diffTimingLong), // now A expires at 34 s
			diffAdv(20 * s),
			diffKA(diffB, diffTimingLong),
			diffAdv(10*s - ns),
			diffAcq(diffB, diffBook, 0).expect("held by m1/a retry 4s"),
			diffAdv(ns),
			diffAcq(diffB, diffBook, 0).expect("term=35000"),
		}},
		{"takeover_with_wall_behind", []string{"wall-behind", "takeover-at-expiry"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAdv(10 * s),
			diffKA(diffB, diffTiming),
			diffAdv(5 * s),
			diffWall(time.UnixMilli(500)),
			diffAcq(diffB, diffBook, 0).expect("term=1001"),
		}},
		{"wall_before_1970", []string{"wall-behind", "acquire-absent"}, []diffOp{
			diffWall(time.Unix(-5, 0)),
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1"),
			diffAcq(diffA, diffBook, 0).expect("term=2"),
			diffWall(time.UnixMilli(0)),
			diffAcq(diffA, diffBook, 0).expect("term=3"),
		}},

		// Release.
		{"release_by_holder_in_term", []string{"release-own", "holdings-sorted-cleared"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffRel(diffA, diffBook, 0, 0).expect("ok"),
			diffHold(diffBook).expect("[book/0 - t1000 v2]"),
			diffKA(diffB, diffTiming),
			diffAcq(diffB, diffBook, 0).expect("term=1001"),
			diffHold(diffBook).expect("[book/0 m2/b t1001 v3]"),
		}},
		{"release_with_wrong_term", []string{"release-wrong-term"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffRel(diffA, diffBook, 0, 1).expect("ok"),
			diffRel(diffA, diffBook, 0, -1).expect("ok"),
			diffHold(diffBook).expect("[book/0 m1/a t1000 v1]"),
		}},
		{"release_by_other_holder", []string{"release-other-holder", "held-retry"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffRel(diffB, diffBook, 0, 0).expect("ok"),
			diffHold(diffBook).expect("[book/0 m1/a t1000 v1]"),
			diffAcq(diffB, diffBook, 0).expect("held by m1/a retry 2s"),
		}},
		{"release_of_absent_record", []string{"release-absent"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffRelT(diffA, diffBook, 3, 7).expect("ok"),
			diffHold(diffBook).expect("nil"),
			diffAcq(diffA, diffBook, 0),
			diffRelT(diffA, diffPage, 0, 1000).expect("ok"),
			diffHold(diffPage).expect("nil"),
		}},
		{"release_after_takeover_with_old_term", []string{"release-other-holder", "release-wrong-term"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0).expect("term=1000"),
			diffAdv(10 * s),
			diffKA(diffB, diffTiming),
			diffAdv(5 * s),
			diffAcq(diffB, diffBook, 0).expect("term=16000"),
			diffRelT(diffA, diffBook, 0, 1000).expect("ok"),
			diffHold(diffBook).expect("[book/0 m2/b t16000 v2]"),
		}},
		{"release_by_ended_session", []string{"release-ended", "late-renewal", "after-end"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffAcq(diffA, diffBook, 0),
			diffAdv(5 * s),
			diffKA(diffA, diffTimingShort).expect("lost"),
			diffRel(diffA, diffBook, 0, 0).expect("ok"),
			diffHold(diffBook).expect("[book/0 - t1000 v2]"),
		}},
		{"release_after_replacement", []string{"release-ended", "replaced"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffAcq(diffA, diffBook, 1),
			diffKA(diffA2, diffTiming),
			diffRel(diffA, diffBook, 0, 0).expect("ok"),
			diffHold(diffBook).expect("[book/0 - t1000 v3, book/1 m1/a t1000 v2]"),
		}},

		// Confirm.
		{"confirm_own_other_absent_cleared", []string{"confirm-own", "confirm-other", "confirm-absent"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffConf(diffA, diffBook, 0).expect("book/0 m1/a t1000 v1"),
			diffConf(diffB, diffBook, 0).expect("book/0 m1/a t1000 v1"),
			diffConfT(diffA, diffBook, 7, 3).expect("book/7 - t0 v0"),
			diffConfT(diffA, diffPage, 0, 3).expect("page/0 - t0 v0"),
			diffRel(diffA, diffBook, 0, 0),
			diffConfT(diffA, diffBook, 0, 1000).expect("book/0 - t1000 v2"),
		}},
		{"confirm_after_takeover", []string{"confirm-other", "replaced", "after-end"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffKA(diffA2, diffTiming),
			diffKA(diffB, diffTiming),
			diffAcq(diffB, diffBook, 0).expect("term=1001"),
			diffConfT(diffA2, diffBook, 0, 1000).expect("book/0 m2/b t1001 v2"),
			diffConfT(diffA, diffBook, 0, 1000).expect("lost"),
		}},

		// CloseSession and Holdings.
		{"close_clears_records_and_slots", []string{"close-clears", "holdings-sorted-cleared", "claim-own", "after-end"}, []diffOp{
			diffSet(3),
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffAcq(diffA, diffBook, 2),
			diffAcq(diffA, diffBook, 0),
			diffAcq(diffB, diffBook, 1),
			diffAcq(diffA, diffPage, 1),
			diffClaim(diffA).expect("slot 0"),
			diffClaim(diffB).expect("slot 1"),
			diffCloseS(diffA).expect("ok"),
			diffHold(diffBook).expect("[book/0 - t1000 v5, book/1 m2/b t1000 v3, book/2 - t1000 v6]"),
			diffHold(diffPage).expect("[page/1 - t1000 v7]"),
			diffSlotsOp().expect("count 3 [- m2/b -]"),
			diffKA(diffA, diffTiming).expect("lost"),
			diffClaim(diffB).expect("slot 1"),
			diffClaim(diffA).expect("lost"),
		}},
		{"close_clears_retired_slot", []string{"close-clears", "claim-retired", "setslots-down", "setslots-up"}, []diffOp{
			diffSet(2),
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffClaim(diffA).expect("slot 0"),
			diffClaim(diffB).expect("slot 1"),
			diffSet(1),
			diffClaim(diffB).expect("noslot"),
			diffSlotsOp().expect("count 1 [m1/a]"),
			diffCloseS(diffB),
			diffClaim(diffB).expect("lost"),
			diffKA(diffC, diffTiming),
			diffSet(2),
			diffSlotsOp().expect("count 2 [m1/a -]"),
			diffClaim(diffC).expect("slot 1"),
		}},
		{"close_of_unknown_holder", []string{"close-unknown", "after-end"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 0),
			diffCloseS(diffZ).expect("ok"),
			diffKA(diffZ, diffTiming).expect("lost"),
			diffAcq(diffZ, diffBook, 1).expect("lost"),
			diffSet(1),
			diffClaim(diffZ).expect("lost"),
			diffConfT(diffZ, diffBook, 0, 1).expect("lost"),
			diffHold(diffBook).expect("[book/0 m1/a t1000 v1]"),
			diffKA(diffA, diffTiming).expect("ok"),
		}},
		{"close_after_expiry_twice_then_release", []string{"close-clears", "after-end", "release-ended"}, []diffOp{
			diffKA(diffA, diffTimingShort),
			diffAcq(diffA, diffBook, 0),
			diffAdv(6 * s),
			diffCloseS(diffA).expect("ok"),
			diffHold(diffBook).expect("[book/0 - t1000 v2]"),
			diffCloseS(diffA).expect("ok"),
			diffRel(diffA, diffBook, 0, 0).expect("ok"),
			diffHold(diffBook).expect("[book/0 - t1000 v2]"),
			diffKA(diffA, diffTimingShort).expect("lost"),
		}},
		{"holdings_sorted_with_cleared", []string{"holdings-sorted-cleared"}, []diffOp{
			diffKA(diffA, diffTiming),
			diffAcq(diffA, diffBook, 5),
			diffAcq(diffA, diffBook, 1),
			diffAcq(diffA, diffBook, 3),
			diffAcq(diffA, diffBook, 0),
			diffRel(diffA, diffBook, 3, 0),
			diffHold(diffBook).expect("[book/0 m1/a t1000 v4, book/1 m1/a t1000 v2, book/3 - t1000 v5, book/5 m1/a t1000 v1]"),
			diffHold(diffPage).expect("nil"),
		}},

		// Slots.
		{"claim_lowest_free_slot", []string{"claim-lowest-free", "release-slot", "setslots-up"}, []diffOp{
			diffSet(3),
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffKA(diffC, diffTiming),
			diffKA(diffD, diffTiming),
			diffClaim(diffB).expect("slot 0"),
			diffClaim(diffA).expect("slot 1"),
			diffClaim(diffC).expect("slot 2"),
			diffRelSlot(diffA, 1).expect("ok"),
			diffSlotsOp().expect("count 3 [m2/b - m3/c]"),
			diffClaim(diffD).expect("slot 1"),
			diffSlotsOp().expect("count 3 [m2/b m4/d m3/c]"),
		}},
		{"claim_own_slot_below_count", []string{"claim-own", "setslots-up"}, []diffOp{
			diffSet(2),
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffClaim(diffA).expect("slot 0"),
			diffClaim(diffB).expect("slot 1"),
			diffClaim(diffA).expect("slot 0"),
			diffSet(4),
			diffClaim(diffB).expect("slot 1"),
			diffClaim(diffA).expect("slot 0"),
			diffSlotsOp().expect("count 4 [m1/a m2/b - -]"),
		}},
		{"claim_with_no_free_slot", []string{"claim-none", "setslots-down", "claim-retired", "slots-zero"}, []diffOp{
			diffSet(1),
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffClaim(diffA).expect("slot 0"),
			diffClaim(diffB).expect("noslot"),
			diffSlotsOp().expect("count 1 [m1/a]"),
			diffSet(0),
			diffSlotsOp().expect("count 0 nil"),
			diffClaim(diffA).expect("noslot"),
			diffClaim(diffB).expect("noslot"),
		}},
		{"claim_slot_of_expired_holder", []string{"claim-lowest-free", "slot-expired", "release-slot"}, []diffOp{
			diffSet(1),
			diffKA(diffA, diffTimingShort),
			diffClaim(diffA).expect("slot 0"),
			diffAdv(4 * s),
			diffKA(diffB, diffTiming),
			diffAdv(s),
			diffSlotsOp().expect("count 1 [-]"),
			diffClaim(diffB).expect("slot 0"),
			diffRelSlot(diffA, 0).expect("ok"),
			diffSlotsOp().expect("count 1 [m2/b]"),
		}},
		{"release_slot_mismatch_and_ended", []string{"release-slot", "after-end", "late-renewal"}, []diffOp{
			diffSet(2),
			diffKA(diffA, diffTimingShort),
			diffKA(diffB, diffTimingLong),
			diffClaim(diffA).expect("slot 0"),
			diffRelSlot(diffB, 0).expect("ok"),
			diffRelSlot(diffA, 1).expect("ok"),
			diffSlotsOp().expect("count 2 [m1/a -]"),
			diffAdv(5 * s),
			diffKA(diffA, diffTimingShort).expect("lost"),
			diffRelSlot(diffA, 0).expect("ok"),
			diffClaim(diffB).expect("slot 0"),
			diffSlotsOp().expect("count 2 [m2/b -]"),
		}},
		{"setslots_down_and_up", []string{"setslots-down", "setslots-up", "claim-retired", "claim-own", "claim-none"}, []diffOp{
			diffSet(3),
			diffKA(diffA, diffTiming),
			diffKA(diffB, diffTiming),
			diffKA(diffC, diffTiming),
			diffClaim(diffA).expect("slot 0"),
			diffClaim(diffB).expect("slot 1"),
			diffClaim(diffC).expect("slot 2"),
			diffSet(1),
			diffClaim(diffC).expect("noslot"),
			diffClaim(diffA).expect("slot 0"),
			diffSlotsOp().expect("count 1 [m1/a]"),
			diffSet(3),
			diffClaim(diffC).expect("slot 2"),
			diffSlotsOp().expect("count 3 [m1/a m2/b m3/c]"),
			diffRelSlot(diffC, 2).expect("ok"),
			diffSet(2),
			diffClaim(diffC).expect("noslot"),
		}},
	}
}
