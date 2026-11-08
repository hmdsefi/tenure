// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore

import (
	"context"
	"slices"

	"github.com/hmdsefi/tenure"
)

// watchBuffer is the capacity of every WatchHolders channel.
const watchBuffer = 1024

// watcher is one WatchHolders channel.
type watcher struct {
	ch chan tenure.Holding

	// stop unregisters the context.AfterFunc that closes ch.
	stop func() bool
}

// WatchHolders returns a channel with room for 1024 values that receives every shard-record
// write committed after WatchHolders returns, each as the record after the write; a deletion
// arrives as a tenure.Holding with only Space, Shard and Version set. A write is delivered before
// the method that makes it returns, to every channel in the order the channels were created, and
// never waits for a receiver: when a channel's buffer is full, the Store closes that channel
// instead and sends it nothing more. The channel is also closed once ctx is done.
func (s *Store) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	w := &watcher{ch: make(chan tenure.Holding, watchBuffer)}
	s.watchers = append(s.watchers, w)
	w.stop = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closeWatcher(w)
	})
	return w.ch, nil
}

// deliver sends rec to every watcher, in registration order, without blocking. A watcher whose
// buffer is full is closed. The caller holds s.mu.
func (s *Store) deliver(rec tenure.Holding) {
	for _, w := range slices.Clone(s.watchers) {
		select {
		case w.ch <- rec:
		default:
			s.closeWatcher(w)
		}
	}
}

// closeWatcher closes w's channel and forgets w, unless that has happened already. The caller
// holds s.mu.
func (s *Store) closeWatcher(w *watcher) {
	i := slices.Index(s.watchers, w)
	if i < 0 {
		return
	}
	s.watchers = slices.Delete(s.watchers, i, i+1)
	close(w.ch)
	w.stop()
}
