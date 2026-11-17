// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
)

// maxErrors is how many error texts Stats keeps.
const maxErrors = 5

// Stats is what a load has done so far, as GET /stats answers it. The counts only grow, so the
// difference of two Stats is what happened between them.
type Stats struct {
	// Pod is the client's pod name, the prefix of its order IDs.
	Pod string `json:"pod"`

	// Sent counts the orders started, OK those that got a 200, and Failed the others by reason:
	// "status <code>" for another answer, "timeout" when the order's time ran out, "error" for
	// any other failure.
	Sent   int64            `json:"sent"`
	OK     int64            `json:"ok"`
	Failed map[string]int64 `json:"failed"`

	// Sends counts the finished orders by the number of HTTP requests each took, 503 retries and
	// resends after a route loop included, and Hops by the highest Tenure-Hops among them.
	Sends map[int]int64 `json:"sends"`
	Hops  map[int]int64 `json:"hops"`

	// Resent409 counts the orders counted ok because a resend after a lost answer got a 409:
	// the lost send had journaled them.
	Resent409 int64 `json:"resent409"`

	// Errors holds the texts of the last few errors of sends, also of those sent again.
	Errors []string `json:"errors,omitempty"`

	// Retries503 counts the sends that got a 503 back, each followed by a retry.
	Retries503 int64 `json:"retries503"`

	// Done is true once the load has stopped and every order it started has finished.
	Done bool `json:"done"`

	// Routes is the member each shard of the markets space is routed to by the client's Router,
	// "" for none, and Stale whether the Router reported its view stale. Run does not set
	// them; the stats handler does.
	Routes []string `json:"routes,omitempty"`
	Stale  bool     `json:"stale,omitempty"`
}

// load sends orders at a fixed rate through a demo.Client and counts what happened to them. It
// has no tie to Kubernetes, so that a load test with many client pods can run the same loop.
type load struct {
	dc  *demo.Client
	url string // where the orders go: the Fallback's base URL followed by /orders
	pod string // the prefix of the order IDs: <pod>-<k>

	rate         float64       // orders per second
	duration     time.Duration // how long to send; 0 means until ctx ends
	orderTimeout time.Duration // how long one order may take
	seed         uint64        // of the orders' rng

	mu sync.Mutex
	st Stats
}

// newLoad returns a load of pod's orders at rate orders per second.
func newLoad(dc *demo.Client, url, pod string, rate float64) *load {
	return &load{
		dc: dc, url: url, pod: pod, rate: rate, orderTimeout: 20 * time.Second,
		seed: uint64(time.Now().UnixNano()),
		st:   Stats{Pod: pod, Failed: map[string]int64{}, Sends: map[int]int64{}, Hops: map[int]int64{}},
	}
}

// retry503 counts a 503 answer of one send; it is the demo.Client's OnSend.
func (l *load) retry503(status int) {
	if status != http.StatusServiceUnavailable {
		return
	}
	l.mu.Lock()
	l.st.Retries503++
	l.mu.Unlock()
}

// stats returns a copy of the counts.
func (l *load) stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.st
	st.Failed, st.Sends, st.Hops = maps.Clone(st.Failed), maps.Clone(st.Sends), maps.Clone(st.Hops)
	st.Errors = slices.Clone(st.Errors)
	return st
}

// run starts one order every 1/rate seconds until ctx ends or the duration passes, then waits
// for the orders in flight, whose own time does not end with ctx, sets Done and returns. Order
// k is named <pod>-<k>, from k = 0, and is keyed by its market.
func (l *load) run(ctx context.Context) {
	if l.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.duration)
		defer cancel()
	}
	rng := rand.New(rand.NewPCG(l.seed, 0)) //nolint:gosec // orders for a demo, not secrets
	tick := time.NewTicker(time.Duration(float64(time.Second) / l.rate))
	defer tick.Stop()
	var inFlight sync.WaitGroup
	for k := 0; ; k++ {
		select {
		case <-ctx.Done():
			inFlight.Wait()
			l.mu.Lock()
			l.st.Done = true
			l.mu.Unlock()
			return
		case <-tick.C:
		}
		o := orderbook.RandomOrder(rng, fmt.Sprintf("%s-%d", l.pod, k))
		l.mu.Lock()
		l.st.Sent++
		l.mu.Unlock()
		inFlight.Go(func() { l.order(context.WithoutCancel(ctx), o) })
	}
}

// order posts o until it gets an answer, or its time runs out, and counts the result. It sends
// the order again after a route loop, and after a send that failed: one that never reached a
// server (a dial error) had no effect, and after one whose answer was lost (a connection that a
// stopping pod closed) the order's ID makes the resend safe: the orderbook journals an ID once
// and answers a repeat with 409, which then means the lost send was journaled and counts as ok,
// in Resent409.
func (l *load) order(ctx context.Context, o orderbook.Order) {
	ctx, cancel := context.WithTimeout(ctx, l.orderTimeout)
	defer cancel()
	body, err := json.Marshal(o)
	if err != nil {
		l.finish(0, 0, 0, false, err)
		return
	}
	// hops counts the moves of the order across Posts: a Post sent again starts its own
	// Tenure-Hops at 0, so each earlier Post adds its hops and the move to the next one.
	sends, hops, earlier := 0, 0, 0
	lost := false // a send whose answer was lost may have journaled the order
	for {
		res, err := l.dc.Post(ctx, l.url, o.Market, body)
		sends += res.Sends
		hops = max(hops, earlier+res.Hops)
		if err != nil && ctx.Err() == nil {
			if !errors.Is(err, demo.ErrRouteLoop) && !unsent(err) {
				lost = true
				l.noteError(err)
			}
			earlier += res.Hops + 1
			select {
			case <-time.After(50 * time.Millisecond):
				continue
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		l.finish(sends, hops, res.Status, lost, err)
		return
	}
}

// unsent reports whether err says that the last send of a Post never reached a server: a dial
// error, such as the Transport's Fallback refusing the connection while the Service still lists
// a pod that has stopped.
func unsent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// noteError keeps the text of an error, among the last maxErrors.
func (l *load) noteError(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.st.Errors) == maxErrors {
		l.st.Errors = l.st.Errors[1:]
	}
	l.st.Errors = append(l.st.Errors, err.Error())
}

// finish counts one finished order.
func (l *load) finish(sends, hops, status int, lost bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.st.Sends[sends]++
	l.st.Hops[hops]++
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		l.st.Failed["timeout"]++
	case err != nil:
		l.st.Failed["error"]++
	case status == http.StatusOK:
		l.st.OK++
	case status == http.StatusConflict && lost:
		l.st.OK++
		l.st.Resent409++
	default:
		l.st.Failed[fmt.Sprintf("status %d", status)]++
	}
}
