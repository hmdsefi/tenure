// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bubble

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The goroutine-mode world: the harness's group run as faultline processes. The store and the
// fence are event-mode nodes, "store" and "fence", so that the network faults of a run can cut a
// member off from them; the members m1 to m3 are processes, one incarnation of the node per boot.
// Every call of a member to the store or the fence runs inside the member's Process.Do, where the
// gate asks the network whether the member and the callee reach each other, both ways, and where
// the call's records are emitted.

// The kinds of the records the goroutine-mode world emits, besides one "tenure.<kind>" record per
// runtime event (eventKind).
const (
	kindBoot  = "tenure.boot"
	kindFence = "tenure.fence"
	kindWrite = "tenure.write"
)

// The node names and the roles of the goroutine-mode world.
const (
	storeNode   = "store"
	fenceNode   = "fence"
	roleStore   = "store"
	roleMembers = "members"
)

// randLabelPrefix starts the label of the kernel stream that names a member's sessions: the
// member's ID follows.
const randLabelPrefix = "tenure/incarnation/"

// The errors of a call the network refuses: the member and the callee do not reach each other in
// at least one direction. The call does not run.
var (
	errStoreUnreachable = errors.New("bubble: store unreachable")
	errFenceUnreachable = errors.New("bubble: fence unreachable")
)

// procNet is the part of a goroutine-mode world that gates the calls of the processes: the
// world and its store and fence nodes.
type procNet struct {
	w     *faultline.World
	store *kernel.Node
	fence *kernel.Node
}

// newProcNet adds the store and fence nodes to w and registers the role "store". It must be
// called from the world function.
func newProcNet(w *faultline.World) *procNet {
	idle := func(*kernel.Node) {}
	pn := &procNet{w: w, store: w.Sim.AddNode(storeNode, idle), fence: w.Sim.AddNode(fenceNode, idle)}
	w.Role(roleStore, func() []kernel.NodeID { return []kernel.NodeID{pn.store.ID()} })
	return pn
}

// gate returns the gate of p's incarnation. It runs each call inside p.Do, labelled with the
// call's op, so the call runs on the driver goroutine as one event at the current virtual time:
//
//   - once the incarnation is no longer alive (crashed, exited or the run ended), Do refuses the
//     call at once with context.Cause of p's context, faultline.ErrCrashed after a crash;
//   - when the network has no link from p's node to the callee or none back,
//     errStoreUnreachable or errFenceUnreachable, at once;
//   - otherwise the call runs, and the gate returns nil.
//
// A call of op "fence.raise" or "fence.write" goes to the fence, every other one to the store:
// the store's methods, and "store.watch", one value of a watch on its way to the member.
func (pn *procNet) gate(p *faultline.Process) gateFunc {
	self := p.Node.ID()
	return func(op string, call func()) error {
		peer, refused := pn.store.ID(), errStoreUnreachable
		if strings.HasPrefix(op, "fence.") {
			peer, refused = pn.fence.ID(), errFenceUnreachable
		}
		var err error
		if derr := p.Do(op, func() {
			if !pn.w.Net.Connected(self, peer) || !pn.w.Net.Connected(peer, self) {
				err = refused
				return
			}
			call()
		}); derr != nil {
			return derr
		}
		return err
	}
}

// procWorld is a goroutine-mode world of the harness: the store and fence nodes, and one process
// per member, which starts an incarnation of the member at every boot of its node.
type procWorld struct {
	*procNet
	h     *harness
	nodes []*kernel.Node // the members' process nodes, in memberIDs order
}

// newProcWorld builds the harness's group in w: the store and fence nodes, the processes m1 to
// m3 and the roles "store" and "members". It must be called from the world function.
func newProcWorld(w *faultline.World, opts options) *procWorld {
	pw := &procWorld{procNet: newProcNet(w), h: newHarness(w.T(), opts)}
	for _, m := range pw.h.members {
		pw.nodes = append(pw.nodes, w.AddProcess(m.id, pw.main(m)))
	}
	w.Role(roleMembers, func() []kernel.NodeID {
		ids := make([]kernel.NodeID, len(pw.nodes))
		for i, n := range pw.nodes {
			ids[i] = n.ID()
		}
		return ids
	})
	return pw
}

// check registers the checks of a run whose disturbances end by recoverAt: the safety checks of
// the bubble as invariants, and g5 from recoverAt and the convergence as finals.
func (pw *procWorld) check(recoverAt time.Duration) {
	h := pw.h
	for _, name := range safetyChecks {
		pw.w.Invariant("tenure/"+name, func() error { return h.errOf(name) })
	}
	pw.w.Final("tenure/g5", func() error { return h.liveness(recoverAt, h.bound()) })
	pw.w.Final("tenure/converged", h.converged)
}

// main returns the process function of m. Each run of it is one incarnation: it waits until the
// member's previous incarnation is down, starts a node with the process's hooks (again every
// RetryPeriod while Start fails, until the process ends), and when the process's context ends,
// on a crash or at the end of the run, crashes the incarnation: Node.Close with a done context
// and Leave.
func (pw *procWorld) main(m *member) faultline.ProcessFunc {
	return func(ctx context.Context, p *faultline.Process) error {
		if prev := m.current(); prev != nil {
			select {
			case <-prev.gone:
			case <-ctx.Done():
				return nil
			}
		}
		var inc *incarnation
		for {
			var err error
			if inc, err = m.startWith(ctx, pw.hooks(p, m.id)); err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			p.Logf("start %s: %v; again in %v", m.id, err, retryPeriod)
			timer := time.NewTimer(retryPeriod)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
		<-ctx.Done()
		inc.crash()
		return nil
	}
}

// hooks returns the hooks of one incarnation of member, run by p: the gate of p, Config.Rand
// drawn from the kernel inside p.Do, the fence's records and one record per event.
func (pw *procWorld) hooks(p *faultline.Process, member string) hooks {
	self := p.Node.ID()
	return hooks{
		gate: pw.gate(p),
		rand: &procRand{pn: pw.procNet, p: p, member: member},
		fenceRecord: func(kind, text string, attrs ...kernel.Attr) {
			// Called from inside the gate's call, that is inside p.Do, on the driver goroutine.
			pw.w.Sim.Emit(kernel.Record{Node: pw.fence.ID(), Kind: kind, Text: text, Attrs: attrs})
		},
		onEvent: func(e tenure.Event) {
			r := kernel.Record{Node: self, Kind: eventKind(e), Attrs: eventAttrs(e)}
			// An event of an incarnation that is no longer alive is not recorded.
			_ = p.Do("event", func() { pw.w.Sim.Emit(r) })
		},
	}
}

// procRand is Config.Rand of one incarnation: it reads the kernel stream
// "tenure/incarnation/<member>" inside Process.Do. Each read of incarnationBytes bytes is one
// draw of a session name, which it records as a tenure.boot record: the holder the node makes of
// the bytes, and the cause, "boot" for the incarnation's first draw and "recover" for a later
// one (a new session after the store ended the last).
type procRand struct {
	pn     *procNet
	p      *faultline.Process
	member string
	draws  int // read and written only inside p.Do, on the driver goroutine
}

// incarnationBytes is how many random bytes the runtime reads to name a session.
const incarnationBytes = 16

// Read fills b from the stream, or returns the error of p.Do when the incarnation is no longer
// alive.
func (r *procRand) Read(b []byte) (int, error) {
	err := r.p.Do("rand", func() {
		src := r.pn.w.Sim.Rand(randLabelPrefix + r.member)
		var word [8]byte
		for i := 0; i < len(b); i += len(word) {
			binary.LittleEndian.PutUint64(word[:], src.Uint64())
			copy(b[i:], word[:])
		}
		if len(b) != incarnationBytes {
			return
		}
		cause := "boot"
		if r.draws > 0 {
			cause = "recover"
		}
		r.draws++
		holder := r.member + "/" + hex.EncodeToString(b)
		r.pn.w.Sim.Emit(kernel.Record{Node: r.p.Node.ID(), Kind: kindBoot, Text: holder + " " + cause,
			Attrs: []kernel.Attr{recAttr("holder", holder), recAttr("cause", cause)}})
	})
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// eventKind returns the record kind of e: "tenure." and the kind's name.
func eventKind(e tenure.Event) string { return "tenure." + e.Kind.String() }

// eventAttrs returns the attributes of e's record: seq, then the attributes of e's log record,
// with the keys, in the order and under the conditions of the event object of package
// debughttp after its seq, time and kind; the wall-clock time is left out. Values are written as
// that object writes them: strings as they are, numbers in decimal, a duration as its String and
// an error as its Error.
func eventAttrs(e tenure.Event) []kernel.Attr {
	attrs := []kernel.Attr{recAttr("seq", strconv.FormatUint(e.Seq, 10))}
	var keys []eventKey
	if int(e.Kind) < len(eventKeys) {
		keys = eventKeys[e.Kind]
	}
	for _, k := range keys {
		if v, ok := k.value(e); ok {
			attrs = append(attrs, recAttr(k.name, v))
		}
	}
	return attrs
}

// eventField names the Event field an attribute is read from.
type eventField uint8

const (
	fieldSpace eventField = iota
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

// eventKey is one attribute of an event record after seq: its key and the field it is read from.
// error is present only when Err is not nil. When opt is true, a space, shard or slot key is
// present only when its field is not unused ("" or -1).
type eventKey struct {
	name  string
	field eventField
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

// eventKeys holds each kind's keys after seq, in order, indexed by tenure.EventKind: a copy of
// debughttp's table of the event object's keys after seq, time and kind, which
// TestEventRecordAttrs compares with debughttp's output. A value that is not a kind has none.
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

// value returns the key's value in e, and false when the key is absent.
func (k eventKey) value(e tenure.Event) (string, bool) {
	switch k.field {
	case fieldSpace:
		return e.Space, !k.opt || e.Space != ""
	case fieldShard:
		return strconv.Itoa(e.Shard), !k.opt || e.Shard >= 0
	case fieldTerm:
		return recTerm(e.Term), true
	case fieldSlot:
		return strconv.Itoa(e.Slot), !k.opt || e.Slot >= 0
	case fieldReason:
		return e.Reason, true
	case fieldHolder:
		return e.Holder, true
	case fieldDuration:
		return e.Duration.String(), true
	case fieldVersion:
		return strconv.FormatUint(e.Version, 10), true
	case fieldCount:
		return strconv.Itoa(e.Count), true
	case fieldConfigured:
		return strconv.Itoa(e.Configured), true
	case fieldErr:
		if e.Err == nil {
			return "", false
		}
		return e.Err.Error(), true
	}
	return "", false
}

// recAttr returns the record attribute key=value.
func recAttr(key, value string) kernel.Attr { return kernel.Attr{Key: key, Value: value} }

// recTerm returns a term as a record value: in decimal.
func recTerm(t core.Term) string { return strconv.FormatUint(uint64(t), 10) }
