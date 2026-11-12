// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package readme holds the Go block of the README that shows how a service starts tenure on
// Kubernetes, so that the block is compiled and cannot drift from the API.
package readme

import (
	"context"
	"os"

	"k8s.io/client-go/kubernetes"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/k8s"
)

// Start starts a node of the group "orderbook" in the namespace "trading", with the Leases of
// client as its membership and store. The body between the markers is the README's first Go
// block, one tab deeper.
//
//nolint:gocritic // unlambda: the README's closure carries comments that explain Run
func Start(ctx context.Context, client kubernetes.Interface) (*tenure.Node, error) {
	// README begin
	kube := k8s.New(client, "trading", "orderbook") // namespace, group

	node, err := tenure.Start(ctx, tenure.Config{
		Self:       tenure.Member{ID: os.Getenv("POD_NAME"), Addr: os.Getenv("POD_IP") + ":8080"},
		Membership: kube,
		Store:      kube,
		Slots:      3, // one per replica: a rollout moves each shard once
		Spaces: []tenure.Space{{
			Name:   "markets",
			Shards: 64,
			Fenced: true,
			Run: func(ctx context.Context, s *tenure.Shard) error {
				// This member owns shard s.ID until ctx is done.
				// Raise the shard's fence to s.Term before reading state,
				// then write fills in transactions that check the fence.
				return serveMarkets(ctx, s)
			},
		}},
	})
	// README end
	return node, err
}

// serveMarkets stands for the service's own code that serves one shard of the markets space.
func serveMarkets(ctx context.Context, _ *tenure.Shard) error {
	<-ctx.Done()
	return ctx.Err()
}
