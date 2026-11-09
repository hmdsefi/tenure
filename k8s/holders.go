// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"fmt"
	"sync"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/hmdsefi/tenure"
)

// Holder watches (K8S-500 to K8S-520).
//
// A WatchHolders channel holds the informer (cache.go) while it is open, and belongs to that
// run of it. The informer's event handler hands every change of a shard record to the open
// channels of its run, after the cache has observed the change, so a receiver that acts on a
// value finds the cache at least that new. A change is the creation of a record, a change of
// its holder or term, or its deletion; Confirm's writes change neither and send nothing, nor do
// writes of other Leases. The informer delivers the changes in the order the API server
// committed them.
//
// A channel is registered first, and then WatchHolders lists the group's shard Leases with
// limit 1 for R, the list's resourceVersion (K8S-500): a write committed after WatchHolders
// returns has a version above R, and a change at or below R was committed before. Only changes
// above R are sent; the changes delivered before R is known wait in the channel's backlog.
//
// The handler never blocks the informer: a channel whose buffer is full is closed instead. Every
// channel of a run is closed when the run may skip a change (K8S-520): when its informer lists
// the Leases again, at the start and at the end of the list, as a list shows only each Lease's
// latest state; when its watch error handler is called; and at a deletion whose final state is
// unknown. A change whose resourceVersion does not parse can not be ordered: it closes every
// channel too. A channel is also closed when its context ends.

// holderBuffer is the capacity of a WatchHolders channel.
const holderBuffer = 1024

var _ tenure.Watcher = (*Adapter)(nil)

// holderWatch is one WatchHolders channel.
type holderWatch struct {
	ch chan tenure.Holding
	// from is R: only changes above it are sent. ready is false until it is known, and the
	// changes delivered until then wait in backlog.
	from    uint64
	ready   bool
	backlog []tenure.Holding
	// end is closed with ch.
	end    chan struct{}
	closed bool
}

// holderWatches are the WatchHolders channels of one run of the informer. The zero value is
// ready to use.
type holderWatches struct {
	mu  sync.Mutex
	set map[*holderWatch]struct{}
	// logged is, by shard Lease name, the resourceVersion at which its malformed term was
	// logged last.
	logged map[string]string
}

// add registers a new channel, not ready yet.
func (s *holderWatches) add() *holderWatch {
	w := &holderWatch{ch: make(chan tenure.Holding, holderBuffer), end: make(chan struct{})}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.set == nil {
		s.set = map[*holderWatch]struct{}{}
	}
	s.set[w] = struct{}{}
	return w
}

// start makes w ready from R = from: the changes in its backlog above from are sent.
func (s *holderWatches) start(w *holderWatch, from uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.closed {
		return
	}
	w.from, w.ready = from, true
	backlog := w.backlog
	w.backlog = nil
	for _, h := range backlog {
		s.sendLocked(w, h)
	}
}

// sendLocked sends change h to w: into its backlog while w is not ready, else into its channel
// when h is above R. A full backlog or channel closes w. The caller holds s.mu.
func (s *holderWatches) sendLocked(w *holderWatch, h tenure.Holding) {
	switch {
	case w.closed:
	case !w.ready:
		if len(w.backlog) == holderBuffer {
			s.closeLocked(w)
			return
		}
		w.backlog = append(w.backlog, h)
	case h.Version > w.from:
		select {
		case w.ch <- h:
		default:
			s.closeLocked(w)
		}
	}
}

// remove closes w unless it is closed already.
func (s *holderWatches) remove(w *holderWatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(w)
}

// closeAll closes every channel.
func (s *holderWatches) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.set {
		s.closeLocked(w)
	}
}

// closeLocked closes w once and forgets it. The caller holds s.mu.
func (s *holderWatches) closeLocked(w *holderWatch) {
	if w.closed {
		return
	}
	w.closed = true
	w.backlog = nil
	close(w.ch)
	close(w.end)
	delete(s.set, w)
}

// WatchHolders returns a channel that receives every change of a shard record of the group
// committed after WatchHolders returns, in the order the API server committed them: a grant,
// a re-grant and a cleared holder as the record after the write, and a deletion as a Holding
// with only Space, Shard and Version set. A write that changes neither the record's holder nor
// its term, as Confirm's does, sends nothing. Version is the resourceVersion of the write. The
// channel buffers 1024 values; it is closed when its buffer is full, when the Adapter's watch
// of the Leases may have missed a change, when a change's resourceVersion is not an integer, and
// once ctx is done. WatchHolders lists the group's shard Leases once, for a version to start
// from, and keeps the Adapter's watch of the Leases running while the channel is open.
func (a *Adapter) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.err != nil {
		return nil, a.err
	}
	c, err := a.holdCache(ctx)
	if err != nil {
		return nil, err
	}
	w := c.holders.add()
	from, err := a.holdersFrom(ctx)
	if err != nil {
		c.holders.remove(w)
		a.releaseCache(c)
		return nil, err
	}
	c.holders.start(w, from)
	a.runs.Go(func() {
		select {
		case <-ctx.Done():
		case <-w.end:
		}
		c.holders.remove(w)
		a.releaseCache(c)
	})
	return w.ch, nil
}

// holdersFrom returns R, the resourceVersion of a LIST of the group's shard Leases with limit 1
// (K8S-500).
func (a *Adapter) holdersFrom(ctx context.Context) (uint64, error) {
	selector := labelGroup + "=" + a.group + "," + labelKind + "=" + kindShard
	list, err := a.leases().List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 1})
	if err != nil {
		return 0, fmt.Errorf("k8s: watch holders: list leases: %w", err)
	}
	v, ok := parseUint(list.ResourceVersion)
	if !ok {
		return 0, fmt.Errorf("k8s: watch holders: list resourceVersion %q is not an integer", list.ResourceVersion)
	}
	return v, nil
}

// holderEvent hands a change of a Lease, delivered by the informer of run c, to the run's
// channels (K8S-510, K8S-520): l is the Lease after the change, old the Lease before it, nil
// for a creation; deleted reports the deletion of l, and gap a deletion whose final state is
// unknown, which closes every channel.
func (a *Adapter) holderEvent(c *leaseCache, old, l *coordinationv1.Lease, deleted, gap bool) {
	s := &c.holders
	if gap {
		s.closeAll()
		return
	}
	h, malformed, ok := a.holdingIn(l)
	if !ok {
		return
	}
	if deleted {
		h.Holder, h.Term = "", 0
	} else if old != nil {
		if p, _, ok := a.holdingIn(old); ok && p.Holder == h.Holder && p.Term == h.Term {
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.set) == 0 {
		return
	}
	v, ok := parseUint(l.ResourceVersion)
	if !ok {
		for w := range s.set {
			s.closeLocked(w)
		}
		return
	}
	h.Version = v
	if malformed && !deleted {
		if s.logged == nil {
			s.logged = map[string]string{}
		}
		if s.logged[l.Name] != l.ResourceVersion {
			s.logged[l.Name] = l.ResourceVersion
			a.logger.Warn("k8s: malformed term", "lease", l.Name, "resourceVersion", l.ResourceVersion,
				"term", l.Annotations[annotationTerm])
		}
	}
	for w := range s.set {
		s.sendLocked(w, h)
	}
}

// holdingIn returns l as a shard record of the group, with no Version, and whether its term is
// malformed, which reads as 0. ok is false when l is not a shard Lease of the group.
func (a *Adapter) holdingIn(l *coordinationv1.Lease) (h tenure.Holding, malformed, ok bool) {
	if l == nil {
		return tenure.Holding{}, false, false
	}
	kind, space, _, shard, ok := parseName(a.group, l.Name)
	if !ok || kind != kindShard || checkOwned(l, a.group, kindShard) != nil || l.Labels[labelSpace] != space {
		return tenure.Holding{}, false, false
	}
	term, valid := decodeTerm(l.Annotations)
	return tenure.Holding{Space: space, Shard: shard, Holder: holderOf(l), Term: term}, !valid, true
}

// leaseInformer returns the informer of run c: the group's Leases in the namespace, with no
// resync, whose every list of the Leases closes the run's channels at its start and at its end
// (K8S-520), as does its watch error handler. A watch that streams the Leases' current state
// first, in place of a list, ends its list at the bookmark that closes that state.
func (a *Adapter) leaseInformer(c *leaseCache) cache.SharedIndexInformer {
	leases := a.leases()
	selector := labelGroup + "=" + a.group
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
			o.LabelSelector = selector
			c.holders.closeAll()
			defer c.holders.closeAll()
			return leases.List(ctx, o)
		},
		WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
			o.LabelSelector = selector
			if o.SendInitialEvents == nil || !*o.SendInitialEvents {
				return leases.Watch(ctx, o)
			}
			c.holders.closeAll()
			w, err := leases.Watch(ctx, o)
			if err != nil {
				return nil, err
			}
			return newListedWatch(w, c.holders.closeAll), nil
		},
	}
	informer := cache.NewSharedIndexInformerWithOptions(cache.ToListWatcherWithWatchListSemantics(lw, a.client),
		&coordinationv1.Lease{}, cache.SharedIndexInformerOptions{ResyncPeriod: resyncPeriod, Indexers: cache.Indexers{}})
	err := informer.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
		cache.DefaultWatchErrorHandler(ctx, r, err)
		c.holders.closeAll()
	})
	if err != nil {
		// SetWatchErrorHandler fails only on an informer that has started; this one has not.
		panic("k8s: set watch error handler: " + err.Error())
	}
	return informer
}

// listedWatch passes on the events of a watch that streams the current state first, and calls
// listed at the bookmark that ends that state.
type listedWatch struct {
	in     watch.Interface
	out    chan watch.Event
	stop   chan struct{}
	once   sync.Once
	listed func()
}

// newListedWatch returns w as a listedWatch that calls listed.
func newListedWatch(w watch.Interface, listed func()) *listedWatch {
	lw := &listedWatch{in: w, out: make(chan watch.Event), stop: make(chan struct{}), listed: listed}
	go lw.run()
	return lw
}

func (w *listedWatch) run() {
	defer close(w.out)
	for {
		select {
		case e, ok := <-w.in.ResultChan():
			if !ok {
				return
			}
			if e.Type == watch.Bookmark {
				if m, ok := e.Object.(metav1.Object); ok && m.GetAnnotations()[metav1.InitialEventsAnnotationKey] == "true" {
					w.listed()
				}
			}
			select {
			case w.out <- e:
			case <-w.stop:
				return
			}
		case <-w.stop:
			return
		}
	}
}

// ResultChan returns the events of the watch.
func (w *listedWatch) ResultChan() <-chan watch.Event { return w.out }

// Stop stops the watch.
func (w *listedWatch) Stop() {
	w.once.Do(func() {
		close(w.stop)
		w.in.Stop()
	})
}
