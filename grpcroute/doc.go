// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package grpcroute is the gRPC side of routing calls to the members of a tenure group: a
// resolver that lists the members a tenure.Router sees, a load-balancing policy that sends each
// call to the member that owns its key, interceptors that follow a member's redirect, and the
// answer a member gives to a call for a key it does not own. It runs in the caller's and the members' own processes: no gateway
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
// # Sending calls to their owners
//
// A caller with a tenure.Router sends each call straight to the member that owns its key with
// two pieces. NewResolverBuilder returns a resolver for targets of the form "tenure:///<space>":
// it gives gRPC the address of every member of the Router's latest snapshot and updates the list
// on every snapshot that changes it, so the channel keeps a connection open to each member. The
// tenure_key policy, which ServiceConfig selects, picks for each call the connection of the
// member that owns the key that WithKey put in the call's context:
//
//	conn, err := grpc.NewClient("tenure:///orders",
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//		grpc.WithResolvers(grpcroute.NewResolverBuilder(router)),
//		grpc.WithDefaultServiceConfig(grpcroute.ServiceConfig),
//		grpc.WithChainUnaryInterceptor(grpcroute.UnaryClientInterceptor(router)),
//		grpc.WithChainStreamInterceptor(grpcroute.StreamClientInterceptor(router)),
//	)
//	...
//	reply, err := client.Get(grpcroute.WithKey(ctx, req.Key), req)
//
// A call without a key goes to the members in turn. A call whose owner's connection is not
// ready, or whose key the Router has no member for, goes to any ready connection; a member that
// does not own the key answers it with Misdirected. A pick reads the Router's snapshot without a
// lock and allocates nothing.
//
// # Following redirects
//
// A Router's view can lag the members' own, for example while a member drains: the picker then
// sends some calls to a member that no longer owns their key, which answers with Misdirected.
// UnaryClientInterceptor and StreamClientInterceptor follow that answer: they record the
// address with Router.Correct, so the next calls for the shard go there at once, and send the
// call again to it. They follow at most DefaultMaxRedirects redirects per call, so a call takes
// at most three attempts; WithMaxRedirects changes the bound. A stream is followed only until
// its first message is received, and the messages sent until then are sent again. Put the
// interceptors first in the chain, so that the interceptors after them see each attempt.
//
// The resolver sends no service config of its own, because gRPC would let it replace the
// caller's whole default service config. A caller with a service config of its own adds the
// tenure_key policy to it:
//
//	{"loadBalancingConfig": [{"tenure_key": {}}], "methodConfig": [...]}
package grpcroute
