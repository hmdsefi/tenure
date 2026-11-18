// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
)

// RouteTrailer is the trailer of a FailedPrecondition answer that names the host:port of the
// member to send the call to instead.
const RouteTrailer = "tenure-route"

// Misdirected answers a call that the member serving it does not own. It sets the tenure-route
// trailer to m.Addr, the address of the member that owns the call's key in this member's view,
// and returns a FailedPrecondition error for the handler to return, with the message
// "tenure: misdirected call: send it to <addr>". The answer promises that the call had no
// effect, so that the caller may send it again: call Misdirected only before the call has had
// any, in a unary handler or a stream handler before it sends a message.
//
// A member without an address cannot be named, so for an m whose Addr is empty Misdirected sets
// no trailer and returns an Unavailable error, "tenure: no member can serve the call yet".
func Misdirected(ctx context.Context, m tenure.Member) error {
	if m.Addr == "" {
		return status.Error(codes.Unavailable, "tenure: no member can serve the call yet")
	}
	_ = grpc.SetTrailer(ctx, metadata.Pairs(RouteTrailer, m.Addr))
	return status.Error(codes.FailedPrecondition, "tenure: misdirected call: send it to "+m.Addr)
}
