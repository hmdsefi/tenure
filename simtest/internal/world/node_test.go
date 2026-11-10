// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strings"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
)

// The helpers of the tests of the store's and the fence's nodes: scripted members, which send
// what a test tells them and keep what they receive, and a view of the trace by event.

// received is one message a scripted member received.
type received struct {
	at   kernel.Time
	from kernel.NodeID
	msg  any
}

// script drives the members of a world as a test tells them: every incarnation of an active
// member installs a handler that keeps what it receives, in inbox by the member's place in
// Members.
type script struct {
	fw    *faultline.World
	w     *World
	inbox [][]received
}

// scripted makes the members of w scripted members. It must run before the first event.
func scripted(fw *faultline.World, w *World) *script {
	s := &script{fw: fw, w: w, inbox: make([][]received, len(w.Members()))}
	w.startMember = func(m *Member) {
		i := int(m.Node.ID() - w.Fence().ID() - 1)
		fw.Net.Handle(m.Node, func(from kernel.NodeID, msg any) {
			s.inbox[i] = append(s.inbox[i], received{at: fw.Sim.Now(), from: from, msg: msg})
		})
	}
	return s
}

// send makes member i of Members send msg to the node to at the time at.
func (s *script) send(at time.Duration, i int, to *kernel.Node, msg any) {
	m := s.w.Members()[i]
	s.fw.Sim.At(kernel.Time(at), "test/send", func() { s.fw.Net.Send(m.Node, to.ID(), msg) })
}

// at runs fn in a global event at the time at.
func (s *script) at(at time.Duration, fn func()) {
	s.fw.Sim.At(kernel.Time(at), "test/do", fn)
}

// replies returns the messages member i received that are replies, in order.
func (s *script) replies(i int) []reply {
	var out []reply
	for _, r := range s.inbox[i] {
		if rep, ok := r.msg.(reply); ok {
			out = append(out, rep)
		}
	}
	return out
}

// reply returns the reply member i received to the request id, or nil.
func (s *script) reply(i int, id uint64) reply {
	for _, rep := range s.replies(i) {
		if rep.requestID() == id {
			return rep
		}
	}
	return nil
}

// traceEvent is one event of the trace: its kernel.event record's label and time, and the
// records it emitted after that one, up to the next event.
type traceEvent struct {
	label string
	at    kernel.Time
	recs  []kernel.Record
}

// traceEvents returns the events of a full trace, in order.
func traceEvents(sim *kernel.Sim) []traceEvent {
	var out []traceEvent
	for _, r := range sim.Records() {
		switch {
		case r.Kind == "kernel.event":
			out = append(out, traceEvent{label: r.Text, at: r.At})
		case len(out) > 0:
			out[len(out)-1].recs = append(out[len(out)-1].recs, r)
		}
	}
	return out
}

// sends returns the net.send records of the event from the node named from whose payload
// starts with prefix, in order.
func (e traceEvent) sends(from, prefix string) []kernel.Record {
	var out []kernel.Record
	for _, r := range e.recs {
		if r.Kind == "net.send" && attrValue(r, "from") == from && strings.HasPrefix(attrValue(r, "payload"), prefix) {
			out = append(out, r)
		}
	}
	return out
}

// of returns the records of the event of the kind, in order.
func (e traceEvent) of(kind string) []kernel.Record {
	var out []kernel.Record
	for _, r := range e.recs {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// sendTargets returns the to attributes of records, in order.
func sendTargets(recs []kernel.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = attrValue(r, "to")
	}
	return out
}

// storeReading returns the store's monotonic reading at the global time at: the store's clock
// has no offset and keeps the drift it was built with, which no test changes.
func storeReading(w *World, at kernel.Time) time.Duration {
	g := time.Duration(at)
	return g + gain(g, w.Store().Drift())
}
