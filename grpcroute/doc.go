// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package grpcroute is the gRPC side of routing calls to the members of a tenure group: a
// resolver that lists the members a tenure.Router sees, and the answer a member gives to a call
// for a key it does not own. It runs in the caller's and the members' own processes: no gateway
// or proxy stands between them.
//
// # The protocol
//
// A member checks every call with Node.Local before it acts on it. When the member does not own
// the key, it acts on nothing and answers FailedPrecondition with a tenure-route trailer that
// holds the host:port of the member that owns the key in this member's view. Misdirected
// writes that answer. The answer promises that the call had no effect, so the caller may send
// it again to the address in the trailer, a bounded number of times, so that two members whose
// views disagree cannot bounce a call forever. A member that knows no owner yet, or that is
// closing, answers Unavailable without the trailer.
//
// A server uses Misdirected in its handlers. Errors that are not about ownership, such as an
// unknown space, would fail again on every member, so they get a final answer instead:
//
//	func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
//		sh, err := s.node.Local("orders", req.Key)
//		var notOwner *tenure.NotOwnerError
//		switch {
//		case err == nil:
//			// Serve the call with sh.
//		case errors.As(err, &notOwner) && notOwner.Route.ID != "" && notOwner.Route.ID != s.self:
//			return nil, grpcroute.Misdirected(ctx, notOwner.Route)
//		case errors.As(err, &notOwner), errors.Is(err, tenure.ErrClosed):
//			// No owner yet, this member is about to own the shard, or it is closing.
//			return nil, status.Error(codes.Unavailable, "no member can serve the call yet")
//		default:
//			return nil, status.Error(codes.Internal, err.Error())
//		}
//		...
//	}
//
// # Finding the members
//
// NewResolverBuilder returns a resolver for targets of the form "tenure:///<space>": it gives
// gRPC the address of every member of the Router's latest snapshot and updates the list on
// every snapshot that changes it, so the channel keeps a connection open to each member:
//
//	conn, err := grpc.NewClient("tenure:///orders",
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//		grpc.WithResolvers(grpcroute.NewResolverBuilder(router)),
//	)
package grpcroute
