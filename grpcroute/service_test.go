// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/grpcroute"
)

// The tests' gRPC service, written by hand so that it needs no generated code. Who takes a key
// and answers with the ID of the member that served it. Watch takes a key and streams the
// member's ID three times.
const (
	whoMethod   = "/grpcroute.test.Members/Who"
	watchMethod = "/grpcroute.test.Members/Watch"
)

// testServer is one member's server of the test service.
type testServer struct {
	id   string
	addr string
	srv  *grpc.Server

	mu    sync.Mutex
	calls []string // the key of each call received, Who's and Watch's
}

// newServer starts a server of the test service for member id on a loopback port.
func newServer(t *testing.T, id string) *testServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &testServer{id: id, addr: lis.Addr().String(), srv: grpc.NewServer()}
	s.srv.RegisterService(&testServiceDesc, s)
	go func() { _ = s.srv.Serve(lis) }()
	t.Cleanup(s.srv.Stop)
	return s
}

// member returns the member the server stands for.
func (s *testServer) member() tenure.Member { return tenure.Member{ID: s.id, Addr: s.addr} }

// received returns the keys of the calls the server received so far.
func (s *testServer) received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// answer records a call for key and answers it with the server's ID.
func (s *testServer) answer(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, key)
	return s.id, nil
}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "grpcroute.test.Members",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Who",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			s, _ := srv.(*testServer)
			id, err := s.answer(ctx, in.GetValue())
			if err != nil {
				return nil, err
			}
			return wrapperspb.String(id), nil
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Watch",
		ServerStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			in := new(wrapperspb.StringValue)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			s, _ := srv.(*testServer)
			id, err := s.answer(stream.Context(), in.GetValue())
			if err != nil {
				return err
			}
			for range 3 {
				if err := stream.SendMsg(wrapperspb.String(id)); err != nil {
					return err
				}
			}
			return nil
		},
	}},
	Metadata: "grpcroute_test",
}

// dial opens a channel on tenure:///orders through r with the tenure_key policy and opts.
func dial(t *testing.T, r *tenure.Router, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts = append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(grpcroute.NewResolverBuilder(r)),
		grpc.WithDefaultServiceConfig(grpcroute.ServiceConfig),
	}, opts...)
	conn, err := grpc.NewClient("tenure:///orders", opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// who calls Who for key on conn, with key in the context when withKey is set, and returns the
// ID of the member that answered.
func who(ctx context.Context, conn *grpc.ClientConn, key string, withKey bool) (string, error) {
	if withKey {
		ctx = grpcroute.WithKey(ctx, key)
	}
	out := new(wrapperspb.StringValue)
	if err := conn.Invoke(ctx, whoMethod, wrapperspb.String(key), out); err != nil {
		return "", err
	}
	return out.GetValue(), nil
}
