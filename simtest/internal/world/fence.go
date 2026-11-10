// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"slices"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// fenceModel is the downstream system every simulated Run writes to, as a state machine
// (SIM-400): for each shard, of a fenced space or not, the largest term its fence has been
// raised to, and the log of the writes it accepted. It has no network and no clock.
//
// raise keeps the largest term raised. A write is accepted iff its term is at least the
// shard's raised term; a write does not raise the fence, so a shard that was never raised
// accepts a write in any term. What the shards' fences are is independent: a raise or a write
// of one shard changes nothing of another. A fenceModel is not safe for concurrent use.
type fenceModel struct {
	raised map[fenceKey]core.Term
	log    []check.Write
}

// fenceKey names one shard of the fence.
type fenceKey struct {
	space string
	shard int
}

// newFenceModel returns a fence that no term has been raised at and that has accepted no write.
func newFenceModel() *fenceModel {
	return &fenceModel{raised: make(map[fenceKey]core.Term)}
}

// raise raises the shard's fence to t, unless it is raised to a larger term already, and
// returns the shard's raised term after it.
func (f *fenceModel) raise(space string, shard int, t core.Term) core.Term {
	k := fenceKey{space, shard}
	if t > f.raised[k] {
		f.raised[k] = t
	}
	return f.raised[k]
}

// write reports whether the fence accepts the write: whether its term is at least the shard's
// raised term. An accepted write is added to the log.
func (f *fenceModel) write(wr check.Write) bool {
	if wr.Term < f.raisedOf(wr.Space, wr.Shard) {
		return false
	}
	f.log = append(f.log, wr)
	return true
}

// raisedOf returns the largest term the shard's fence has been raised to, 0 for none.
func (f *fenceModel) raisedOf(space string, shard int) core.Term {
	return f.raised[fenceKey{space, shard}]
}

// accepted returns the accepted writes in the order they were accepted, in a slice of the
// caller's own.
func (f *fenceModel) accepted() []check.Write {
	return slices.Clone(f.log)
}

// raiseReq raises the fence of the shard to Term. A Run sends it when it starts.
type raiseReq struct {
	reqHead
	Space string
	Shard int
	Term  core.Term
}

// Describe returns, for example, "raise#3 m1/0a book/3 term=5".
func (r raiseReq) Describe() string {
	return r.text("raise") + " " + msgShard(r.Space, r.Shard) + " term=" + msgTerm(r.Term)
}

// writeReq writes to the shard in Term for the sender, the Run's holder.
type writeReq struct {
	reqHead
	Space string
	Shard int
	Term  core.Term
}

// Describe returns, for example, "write#4 m1/0a book/3 term=5".
func (r writeReq) Describe() string {
	return r.text("write") + " " + msgShard(r.Space, r.Shard) + " term=" + msgTerm(r.Term)
}

// raisedReply answers a raiseReq.
type raisedReply struct {
	replyHead
	Space string
	Shard int
	// Term is the term the request named.
	Term core.Term
	// Raised is the shard's raised term after the request: Term, or a larger term raised
	// before.
	Raised core.Term
}

// Describe returns, for example, "raised#3 book/3 term=5 raised=7".
func (r raisedReply) Describe() string {
	return r.text("raised") + " " + msgShard(r.Space, r.Shard) + " term=" + msgTerm(r.Term) +
		" raised=" + msgTerm(r.Raised)
}

// writtenReply answers a writeReq.
type writtenReply struct {
	replyHead
	Space string
	Shard int
	// Term is the term the request named.
	Term core.Term
	// Raised is the shard's raised term the write was judged by.
	Raised core.Term
	// Accepted reports that the fence accepted the write: Term is at least Raised.
	Accepted bool
}

// Describe returns, for example, "written#4 book/3 term=5 raised=5 accepted", and
// "written#4 book/3 term=4 raised=5 rejected" for a write the fence rejected.
func (r writtenReply) Describe() string {
	result := " rejected"
	if r.Accepted {
		result = " accepted"
	}
	return r.text("written") + " " + msgShard(r.Space, r.Shard) + " term=" + msgTerm(r.Term) +
		" raised=" + msgTerm(r.Raised) + result
}

// fenceServer is the fence's node: it serves the fence model over the simulated network. Like
// the store's, its state lives in the World and survives the crashes of its node, as a
// downstream system keeps what it accepted; a message that arrives while the fence is down is
// dropped by the network and never answered.
//
// Every request is answered, to its sender, in the event that delivered it. A raiseReq emits
// tenure.fence with the term and the raised term after it; a writeReq emits tenure.write with
// result "accepted" or "rejected". The text of both is the request's Describe.
type fenceServer struct {
	w     *World
	model *fenceModel

	// onAccepted, when set, is called for every accepted write, in the event the fence
	// accepted it, after its tenure.write record. The checks of the run set it to feed
	// Checker.Accepted (1.8-10).
	onAccepted func(check.Write)
}

// newFenceServer returns the fence of w, with no term raised.
func newFenceServer(w *World) *fenceServer {
	return &fenceServer{w: w, model: newFenceModel()}
}

// boot runs at every boot of the fence's node: it installs the handler. The fence has no
// timers.
func (f *fenceServer) boot(n *kernel.Node) {
	f.w.fw.Net.Handle(n, f.handle)
}

// handle answers one delivered message: a raiseReq or a writeReq. Anything else fails the run.
func (f *fenceServer) handle(from kernel.NodeID, payload any) {
	var rep reply
	switch r := payload.(type) {
	case raiseReq:
		raised := f.model.raise(r.Space, r.Shard, r.Term)
		f.w.emitFence(r.Describe(), r.Space, r.Shard, r.Term, raised)
		rep = raisedReply{replyHead: replyHead{r.ID}, Space: r.Space, Shard: r.Shard, Term: r.Term, Raised: raised}
	case writeReq:
		wr := check.Write{Space: r.Space, Shard: r.Shard, Term: r.Term, Holder: r.Holder}
		raised := f.model.raisedOf(r.Space, r.Shard)
		accepted := f.model.write(wr)
		result := "rejected"
		if accepted {
			result = "accepted"
		}
		f.w.emitWrite(r.Describe(), wr, result)
		if accepted && f.onAccepted != nil {
			f.onAccepted(wr)
		}
		rep = writtenReply{
			replyHead: replyHead{r.ID}, Space: r.Space, Shard: r.Shard, Term: r.Term, Raised: raised, Accepted: accepted,
		}
	default:
		f.w.sim.Fail(fmt.Errorf("world: fence: unexpected message %T from node %d", payload, from))
		return
	}
	f.w.fw.Net.Send(f.w.fence, from, rep)
}
