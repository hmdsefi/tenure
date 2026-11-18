// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/grpcroute"
)

// attemptsCtx is the context key of the counter of a call's attempts.
type attemptsCtx struct{}

// countUnary is a unary interceptor, after grpcroute's, that counts each attempt in the counter
// of the call's context.
func countUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if n, ok := ctx.Value(attemptsCtx{}).(*atomic.Int32); ok {
		n.Add(1)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// countStream is countUnary for streams: it counts each stream opened.
func countStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if n, ok := ctx.Value(attemptsCtx{}).(*atomic.Int32); ok {
		n.Add(1)
	}
	return streamer(ctx, desc, cc, method, opts...)
}

// interceptors returns the dial options that install grpcroute's interceptors on r with opts,
// followed by the counting ones.
func interceptors(r *tenure.Router, opts ...grpcroute.Option) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(grpcroute.UnaryClientInterceptor(r, opts...), countUnary),
		grpc.WithChainStreamInterceptor(grpcroute.StreamClientInterceptor(r, opts...), countStream),
	}
}

// counted returns a context with a new attempt counter.
func counted() (context.Context, *atomic.Int32) {
	n := new(atomic.Int32)
	return context.WithValue(context.Background(), attemptsCtx{}, n), n
}

// call calls Who for key with key in the context and returns the member that answered and the
// number of attempts.
func call(conn *grpc.ClientConn, key string) (string, int, error) {
	ctx, n := counted()
	id, err := who(ctx, conn, key, true)
	return id, int(n.Load()), err
}

// watch opens Watch for key with key in the context, and returns the IDs it streamed and the
// number of attempts.
func watch(conn *grpc.ClientConn, method, key string) ([]string, int, error) {
	ctx, n := counted()
	ctx = grpcroute.WithKey(ctx, key)
	cs, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	if err != nil {
		return nil, int(n.Load()), err
	}
	if err := cs.SendMsg(wrapperspb.String(key)); err != nil {
		return nil, int(n.Load()), err
	}
	if err := cs.CloseSend(); err != nil {
		return nil, int(n.Load()), err
	}
	var ids []string
	for {
		out := new(wrapperspb.StringValue)
		err := cs.RecvMsg(out)
		if errors.Is(err, io.EOF) {
			return ids, int(n.Load()), nil
		}
		if err != nil {
			return ids, int(n.Load()), err
		}
		ids = append(ids, out.GetValue())
	}
}

// misdirectTo returns a handler that answers every call with Misdirected to m.
func misdirectTo(m tenure.Member) handler {
	return func(ctx context.Context, _, _ string) error { return grpcroute.Misdirected(ctx, m) }
}

// fail returns a handler that answers every call with err.
func fail(err error) handler {
	return func(context.Context, string, string) error { return err }
}

// pairKey is the key of the tests of a pairCluster.
const pairKey = "order-1"

// pairCluster returns a ready cluster of two servers, the one that owns pairKey in the Router's
// snapshot first, and a channel through grpcroute's interceptors with opts.
func pairCluster(t *testing.T, opts ...grpcroute.Option) (owner, other *testServer, r *tenure.Router, conn *grpc.ClientConn) {
	t.Helper()
	a, b := newServer(t, "n1"), newServer(t, "n2")
	_, r = newGroup(t, a.member(), b.member())
	conn = dial(t, r, interceptors(r, opts...)...)
	c := &cluster{servers: []*testServer{a, b}, router: r}
	c.waitReady(t, conn)
	if c.owner(t, pairKey) == a.id {
		return a, b, r, conn
	}
	return b, a, r, conn
}

// corrected returns the live correction of key's shard in r's table.
func corrected(t *testing.T, r *tenure.Router, key string) string {
	t.Helper()
	h, err := r.Space("orders")
	if err != nil {
		t.Fatal(err)
	}
	shard, err := h.Shard(key)
	if err != nil {
		t.Fatal(err)
	}
	return r.Table().Spaces[0].Routes[shard].Corrected
}

// TestRTR920UnaryFollowsTrailer checks that a Misdirected answer corrects the Router and that the
// call is sent again to the member it names, and that the next call goes there at once.
func TestRTR920UnaryFollowsTrailer(t *testing.T) {
	const key = pairKey
	owner, other, r, conn := pairCluster(t)
	owner.set(misdirectTo(other.member()))

	id, attempts, err := call(conn, key)
	if err != nil || id != other.id || attempts != 2 {
		t.Fatalf("call = %s in %d attempts, %v; want %s in 2", id, attempts, err, other.id)
	}
	if got := corrected(t, r, key); got != other.addr {
		t.Fatalf("correction = %q, want %s", got, other.addr)
	}
	id, attempts, err = call(conn, key)
	if err != nil || id != other.id || attempts != 1 {
		t.Fatalf("call after the correction = %s in %d attempts, %v; want %s in 1", id, attempts, err, other.id)
	}
}

// TestRTR920UnaryBound checks that a call between two members that name each other stops after
// MaxRedirects redirects with the last answer.
func TestRTR920UnaryBound(t *testing.T) {
	const key = pairKey
	for _, tc := range []struct {
		name     string
		opts     []grpcroute.Option
		attempts int
	}{
		{"default", nil, 3},
		{"zero", []grpcroute.Option{grpcroute.WithMaxRedirects(0)}, 1},
		{"negative", []grpcroute.Option{grpcroute.WithMaxRedirects(-1)}, 1},
		{"five", []grpcroute.Option{grpcroute.WithMaxRedirects(5)}, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, other, _, conn := pairCluster(t, tc.opts...)
			owner.set(misdirectTo(other.member()))
			other.set(misdirectTo(owner.member()))
			_, attempts, err := call(conn, key)
			if status.Code(err) != codes.FailedPrecondition || attempts != tc.attempts {
				t.Fatalf("call = %v in %d attempts, want FailedPrecondition in %d", err, attempts, tc.attempts)
			}
		})
	}
}

// TestRTR920UnaryNotFollowed checks that answers other than FailedPrecondition with a usable
// tenure-route trailer are returned after one attempt.
func TestRTR920UnaryNotFollowed(t *testing.T) {
	const key = pairKey
	for name, h := range map[string]handler{
		"unavailable":  fail(status.Error(codes.Unavailable, "later")),
		"no trailer":   fail(status.Error(codes.FailedPrecondition, "no route")),
		"not hostport": misdirectTo(tenure.Member{ID: "n9", Addr: "nonsense"}),
		"empty addr":   misdirectTo(tenure.Member{ID: "n9"}),
	} {
		t.Run(name, func(t *testing.T) {
			owner, _, _, conn := pairCluster(t)
			owner.set(h)
			_, attempts, err := call(conn, key)
			if err == nil || attempts != 1 {
				t.Fatalf("call = %v in %d attempts, want the error in 1", err, attempts)
			}
		})
	}
}

// TestRTR920UnaryNoKey checks that a call without a key follows the trailer without correcting
// the Router.
func TestRTR920UnaryNoKey(t *testing.T) {
	const key = pairKey
	owner, other, r, conn := pairCluster(t)
	owner.set(misdirectTo(other.member()))
	for range 6 {
		ctx, n := counted()
		id, err := who(ctx, conn, key, false)
		if err != nil || id != other.id || n.Load() > 2 {
			t.Fatalf("call without a key = %s in %d attempts, %v; want %s in at most 2", id, n.Load(), err, other.id)
		}
	}
	if got := corrected(t, r, key); got != "" {
		t.Fatalf("correction = %q after calls without a key, want none", got)
	}
}

// TestRTR920UnaryRefusedCorrection checks that a redirect to an address the Router does not know
// records no correction and still ends within the bound.
func TestRTR920UnaryRefusedCorrection(t *testing.T) {
	const key = pairKey
	owner, other, r, conn := pairCluster(t)
	stranger := tenure.Member{ID: "n9", Addr: "127.0.0.1:1"}
	owner.set(misdirectTo(stranger))
	other.set(misdirectTo(stranger))
	_, attempts, err := call(conn, key)
	if status.Code(err) != codes.FailedPrecondition || attempts != 3 {
		t.Fatalf("call = %v in %d attempts, want FailedPrecondition in 3", err, attempts)
	}
	if got := corrected(t, r, key); got != "" {
		t.Fatalf("correction = %q, want none for an unknown address", got)
	}
}

// TestRTR920UnaryOtherTarget checks that the interceptor leaves calls on a channel whose target
// is not tenure:/// alone.
func TestRTR920UnaryOtherTarget(t *testing.T) {
	s := newServer(t, "n1")
	s.set(misdirectTo(tenure.Member{ID: "n2", Addr: s.addr}))
	_, r := newGroup(t, s.member())
	conn, err := grpc.NewClient("passthrough:///"+s.addr,
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, interceptors(r)...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_, attempts, err := call(conn, "order-1")
	if status.Code(err) != codes.FailedPrecondition || attempts != 1 {
		t.Fatalf("call = %v in %d attempts, want FailedPrecondition in 1", err, attempts)
	}
	ids, attempts, err := watch(conn, watchMethod, "order-1")
	if status.Code(err) != codes.FailedPrecondition || attempts != 1 || len(ids) != 0 {
		t.Fatalf("stream = %v, %v in %d attempts, want FailedPrecondition in 1", ids, err, attempts)
	}
}

// TestRTR920StreamFollowsTrailer checks that a stream redirected before its first message opens
// again at the named member and corrects the Router.
func TestRTR920StreamFollowsTrailer(t *testing.T) {
	const key = pairKey
	owner, other, r, conn := pairCluster(t)
	owner.set(misdirectTo(other.member()))
	ids, attempts, err := watch(conn, watchMethod, key)
	want := []string{other.id, other.id, other.id}
	if err != nil || fmt.Sprint(ids) != fmt.Sprint(want) || attempts != 2 {
		t.Fatalf("stream = %v in %d attempts, %v; want %v in 2", ids, attempts, err, want)
	}
	if got := corrected(t, r, key); got != other.addr {
		t.Fatalf("correction = %q, want %s", got, other.addr)
	}
}

// TestRTR920StreamBound checks that a stream between two members that name each other stops
// after MaxRedirects redirects.
func TestRTR920StreamBound(t *testing.T) {
	const key = pairKey
	owner, other, _, conn := pairCluster(t)
	owner.set(misdirectTo(other.member()))
	other.set(misdirectTo(owner.member()))
	_, attempts, err := watch(conn, watchMethod, key)
	if status.Code(err) != codes.FailedPrecondition || attempts != 3 {
		t.Fatalf("stream = %v in %d attempts, want FailedPrecondition in 3", err, attempts)
	}
}

// TestRTR920StreamAfterFirstMessage checks that a stream that fails after a message was received
// is not opened again.
func TestRTR920StreamAfterFirstMessage(t *testing.T) {
	const key = pairKey
	owner, other, _, conn := pairCluster(t)
	owner.set(misdirectTo(other.member()))
	ids, attempts, err := watch(conn, lateMethod, key)
	if status.Code(err) != codes.FailedPrecondition || attempts != 1 || fmt.Sprint(ids) != fmt.Sprint([]string{owner.id}) {
		t.Fatalf("stream = %v, %v in %d attempts; want [%s] then FailedPrecondition in 1", ids, err, attempts, owner.id)
	}
}

// TestRTR920StreamReplaysSent checks that a client stream redirected before its answer sends the
// new attempt every message the caller sent.
func TestRTR920StreamReplaysSent(t *testing.T) {
	const key = pairKey
	owner, other, _, conn := pairCluster(t)
	owner.set(misdirectTo(other.member()))

	ctx, n := counted()
	cs, err := conn.NewStream(grpcroute.WithKey(ctx, key), &grpc.StreamDesc{ClientStreams: true}, collectMethod)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{key, "a", "b"} {
		if err := cs.SendMsg(wrapperspb.String(k)); err != nil {
			t.Fatalf("SendMsg(%s): %v", k, err)
		}
	}
	if err := cs.CloseSend(); err != nil {
		t.Fatal(err)
	}
	out := new(wrapperspb.StringValue)
	if err := cs.RecvMsg(out); err != nil {
		t.Fatalf("RecvMsg: %v", err)
	}
	if want := other.id + ":" + key + ",a,b"; out.GetValue() != want || n.Load() != 2 {
		t.Fatalf("answer = %q in %d attempts, want %q in 2", out.GetValue(), n.Load(), want)
	}
	if _, err := cs.Header(); err != nil {
		t.Fatalf("Header: %v", err)
	}
	if cs.Context() == nil {
		t.Fatal("Context() = nil")
	}
	if err := cs.RecvMsg(out); !errors.Is(err, io.EOF) {
		t.Fatalf("second RecvMsg = %v, want EOF", err)
	}
	_ = cs.Trailer()
}
