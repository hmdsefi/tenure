// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/grpcroute"
)

// The tests' gRPC service, written by hand so that it needs no generated code. Who takes a key
// and answers with the ID of the member that served it. Watch takes a key and streams the
// member's ID three times. Collect takes a stream of keys, checks the first, and answers with
// the ID and every key it received, as "<id>:<key>,<key>,…". Late takes a key, streams the
// member's ID once, and only then checks the key.
const (
	whoMethod     = "/grpcroute.test.Members/Who"
	watchMethod   = "/grpcroute.test.Members/Watch"
	collectMethod = "/grpcroute.test.Members/Collect"
	lateMethod    = "/grpcroute.test.Members/Late"
)

// handler checks one call of the test service for key on the member with id before the server
// answers: nil lets it answer, an error is the call's answer.
type handler func(ctx context.Context, id, key string) error

// testServer is one member's server of the test service.
type testServer struct {
	id   string
	addr string
	srv  *grpc.Server

	mu     sync.Mutex
	handle handler
	calls  []string // the key of each call received
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

// set makes h check the server's calls.
func (s *testServer) set(h handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handle = h
}

// answer records a call for key and answers it with the server's ID, unless its handler fails
// the call.
func (s *testServer) answer(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	s.calls = append(s.calls, key)
	h := s.handle
	s.mu.Unlock()
	if h != nil {
		if err := h(ctx, s.id, key); err != nil {
			return "", err
		}
	}
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
	}, {
		StreamName:    "Collect",
		ClientStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			var keys []string
			for {
				in := new(wrapperspb.StringValue)
				err := stream.RecvMsg(in)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				keys = append(keys, in.GetValue())
			}
			if len(keys) == 0 {
				return status.Error(codes.InvalidArgument, "no keys")
			}
			s, _ := srv.(*testServer)
			id, err := s.answer(stream.Context(), keys[0])
			if err != nil {
				return err
			}
			return stream.SendMsg(wrapperspb.String(id + ":" + strings.Join(keys, ",")))
		},
	}, {
		StreamName:    "Late",
		ServerStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			in := new(wrapperspb.StringValue)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			s, _ := srv.(*testServer)
			if err := stream.SendMsg(wrapperspb.String(s.id)); err != nil {
				return err
			}
			_, err := s.answer(stream.Context(), in.GetValue())
			return err
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
