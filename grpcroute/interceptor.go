// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute

import (
	"context"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
)

// DefaultMaxRedirects is how many redirects an interceptor follows for one call unless
// WithMaxRedirects says otherwise, so a call takes at most three attempts.
const DefaultMaxRedirects = 2

// An Option changes how UnaryClientInterceptor and StreamClientInterceptor follow redirects.
type Option func(*options)

type options struct {
	maxRedirects int
}

// WithMaxRedirects sets how many redirects an interceptor follows for one call: a call takes at
// most n+1 attempts, and the answer of the last is returned as it is. n of 0 or less follows
// none. The default is DefaultMaxRedirects.
func WithMaxRedirects(n int) Option {
	return func(o *options) { o.maxRedirects = max(n, 0) }
}

func newOptions(opts []Option) options {
	o := options{maxRedirects: DefaultMaxRedirects}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// routeCtx is the context key of the address of a redirect being followed. The tenure_key picker
// sends the call there while it has a ready connection to it.
type routeCtx struct{}

// routeFrom returns the address of the redirect ctx follows.
func routeFrom(ctx context.Context) (string, bool) {
	addr, ok := ctx.Value(routeCtx{}).(string)
	return addr, ok
}

// UnaryClientInterceptor returns an interceptor that follows the redirects of the members for
// calls on a channel whose target is "tenure:///<space>". When an attempt fails with
// FailedPrecondition and a tenure-route trailer, as Misdirected answers, the interceptor
// records the address in the trailer with r.Correct for the shard of the call's key (WithKey),
// and sends the call again to that address. It follows at most DefaultMaxRedirects redirects,
// or the number WithMaxRedirects sets, so that two members whose views disagree cannot bounce
// a call forever. Any other answer, and every answer on a channel with another target, is
// returned as it is.
//
// Use it with grpc.WithUnaryInterceptor, or first in grpc.WithChainUnaryInterceptor so that the
// interceptors after it see each attempt. The channel must use the tenure_key policy, which
// sends the attempt that follows a redirect to the address the redirect names while the channel
// has a ready connection to it, and to the key's owner otherwise.
func UnaryClientInterceptor(r *tenure.Router, opts ...Option) grpc.UnaryClientInterceptor {
	o := newOptions(opts)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		space, ok := spaceOf(cc.Target())
		if !ok || o.maxRedirects == 0 {
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		var trailer metadata.MD
		callOpts = append(slices.Clip(callOpts), grpc.Trailer(&trailer))
		for redirects := 0; ; redirects++ {
			trailer = nil
			err := invoker(ctx, method, req, reply, cc, callOpts...)
			if redirects == o.maxRedirects {
				return err
			}
			addr, ok := redirect(err, trailer)
			if !ok {
				return err
			}
			ctx = follow(ctx, r, space, addr)
		}
	}
}

// StreamClientInterceptor returns an interceptor that follows the redirects of the members for
// streams, as UnaryClientInterceptor does for unary calls, until the first message of the
// stream is received. When the stream fails before then with FailedPrecondition and a
// tenure-route trailer, the interceptor opens the stream again at the address in the trailer
// and sends it every message for which SendMsg returned nil, and CloseSend if it was called, so
// the caller's RecvMsg sees the new stream's messages. Once a message has been received, the
// stream is never opened again. The stream keeps the messages it sent until then, so a caller
// must not change a message after SendMsg, as gRPC already requires.
//
// Header, Trailer and Context read the current attempt: a Header that returned before a
// redirect describes the attempt that was redirected.
func StreamClientInterceptor(r *tenure.Router, opts ...Option) grpc.StreamClientInterceptor {
	o := newOptions(opts)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
		space, ok := spaceOf(cc.Target())
		if !ok || o.maxRedirects == 0 {
			return streamer(ctx, desc, cc, method, callOpts...)
		}
		cs, err := streamer(ctx, desc, cc, method, callOpts...)
		if err != nil {
			return nil, err
		}
		return &redirectStream{
			router:   r,
			space:    space,
			left:     o.maxRedirects,
			ctx:      ctx,
			desc:     desc,
			cc:       cc,
			method:   method,
			streamer: streamer,
			opts:     callOpts,
			cur:      cs,
		}, nil
	}
}

// redirectStream is a client stream that opens itself again at the address of a redirect until
// its first message is received.
type redirectStream struct {
	router   *tenure.Router
	space    string
	left     int // redirects it may still follow; only RecvMsg reads and writes it
	ctx      context.Context
	desc     *grpc.StreamDesc
	cc       *grpc.ClientConn
	method   string
	streamer grpc.Streamer
	opts     []grpc.CallOption

	// mu guards the fields below. SendMsg and CloseSend hold it while the stream may still be
	// opened again, so that a message is never sent to an attempt that is being replaced.
	mu         sync.Mutex
	cur        grpc.ClientStream
	settled    bool  // a message was received or the stream will not be opened again
	sent       []any // the messages SendMsg accepted, while not settled
	closedSend bool
}

func (s *redirectStream) current() grpc.ClientStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

func (s *redirectStream) Header() (metadata.MD, error) { return s.current().Header() }
func (s *redirectStream) Trailer() metadata.MD         { return s.current().Trailer() }
func (s *redirectStream) Context() context.Context     { return s.current().Context() }

func (s *redirectStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closedSend = true
	return s.cur.CloseSend()
}

func (s *redirectStream) SendMsg(m any) error {
	s.mu.Lock()
	if s.settled {
		cs := s.cur
		s.mu.Unlock()
		return cs.SendMsg(m)
	}
	defer s.mu.Unlock()
	err := s.cur.SendMsg(m)
	if err == nil {
		s.sent = append(s.sent, m)
	}
	return err
}

func (s *redirectStream) RecvMsg(m any) error {
	for {
		s.mu.Lock()
		cs, settled := s.cur, s.settled
		s.mu.Unlock()
		err := cs.RecvMsg(m)
		if settled {
			return err
		}
		if err == nil || s.left == 0 || status.Code(err) != codes.FailedPrecondition {
			s.settle()
			return err
		}
		// Trailer may be read only now that RecvMsg has failed.
		addr, ok := redirect(err, cs.Trailer())
		if !ok {
			s.settle()
			return err
		}
		s.left--
		if err := s.reopen(follow(s.ctx, s.router, s.space, addr)); err != nil {
			s.settle()
			return err
		}
	}
}

// settle stops the stream from being opened again and drops the messages kept for it.
func (s *redirectStream) settle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settled = true
	s.sent = nil
}

// reopen opens the stream again with ctx and sends it what the caller sent so far. A send that
// fails ends the replay: the new attempt's RecvMsg then reports why.
func (s *redirectStream) reopen(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs, err := s.streamer(ctx, s.desc, s.cc, s.method, s.opts...)
	if err != nil {
		return err
	}
	s.cur = cs
	for _, m := range s.sent {
		if cs.SendMsg(m) != nil {
			return nil
		}
	}
	if s.closedSend {
		_ = cs.CloseSend()
	}
	return nil
}

// redirect returns the address of the redirect that err and trailer make: FailedPrecondition
// with a tenure-route trailer that holds a host:port.
func redirect(err error, trailer metadata.MD) (string, bool) {
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		return "", false
	}
	v := trailer.Get(RouteTrailer)
	if len(v) == 0 {
		return "", false
	}
	if _, _, err := net.SplitHostPort(v[0]); err != nil {
		return "", false
	}
	return v[0], true
}

// follow records addr as the server of the shard of ctx's key, when ctx has one, and returns a
// copy of ctx that sends the next attempt to addr. Correct may refuse addr, when the Router's
// snapshot has no member with it; the attempt is still sent there if the channel has a ready
// connection to it, and to the key's owner otherwise.
func follow(ctx context.Context, r *tenure.Router, space, addr string) context.Context {
	if key, ok := keyFrom(ctx); ok && r != nil {
		if h, err := r.Space(space); err == nil {
			if shard, err := h.Shard(key); err == nil {
				r.Correct(space, shard, addr)
			}
		}
	}
	return context.WithValue(ctx, routeCtx{}, addr)
}

// spaceOf returns the space of a target "tenure:///<space>", or false for another target.
func spaceOf(target string) (string, bool) {
	u, err := url.Parse(target)
	if err != nil || u.Scheme != Scheme {
		return "", false
	}
	space := u.Opaque
	if space == "" {
		space = strings.TrimPrefix(u.Path, "/")
	}
	return space, space != ""
}
