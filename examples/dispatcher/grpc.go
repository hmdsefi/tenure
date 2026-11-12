// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

const (
	// serviceName is the gRPC service the members serve, with no generated code.
	serviceName = "dispatcher.Dispatcher"

	addTaskMethod = "/" + serviceName + "/AddTask"
	pollMethod    = "/" + serviceName + "/Poll"

	// codecName is the name of the JSON codec, and so the content subtype of every call.
	codecName = "json"

	// routeKey is the trailer in which a member that does not own a partition names the one that
	// does.
	routeKey = "tenure-route"

	// maxResends is how many times a client sends a call on to the member a route names.
	maxResends = 2

	// pollMargin is how much earlier than its caller's deadline a poll gives up waiting, at most,
	// so that an empty answer can still reach a caller that has not given up yet and a task that
	// was handed over is never sent to a caller that has.
	pollMargin = 100 * time.Millisecond
)

// ErrNotAccepted means the call changed nothing: no member took the task, or served the poll.
// Resending an AddTask after an error that matches it cannot duplicate the task; after any other
// error the task may be somewhere, so it must not be sent again.
var ErrNotAccepted = errors.New("dispatcher: not accepted")

// ErrRoute means a call was sent on to the member a route names more than maxResends times
// without finding an owner. It matches ErrNotAccepted.
var ErrRoute = fmt.Errorf("%w: route did not settle", ErrNotAccepted)

// AddTaskRequest is the request of AddTask: add Task at Partition.
type AddTaskRequest struct {
	Partition int  `json:"partition"`
	Task      Task `json:"task"`
}

// AddTaskResponse is the response of AddTask. It has no fields: the status is the answer.
type AddTaskResponse struct{}

// PollRequest is the request of Poll: wait on Partition for a task.
type PollRequest struct {
	Partition int `json:"partition"`
}

// PollResponse is the response of Poll. Task is nil when the poll ended with no task.
type PollResponse struct {
	Task *Task `json:"task,omitempty"`
}

// jsonCodec is the gRPC codec that carries the messages as JSON.
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return codecName }

var registerCodecOnce sync.Once

// RegisterCodec registers the JSON codec, named "json", with gRPC. It is safe to call from
// several goroutines and any number of times. NewClient and Server.Register call it.
func RegisterCodec() {
	registerCodecOnce.Do(func() { encoding.RegisterCodec(jsonCodec{}) })
}

// Lookuper names the member that serves a key. *tenure.Resolver is one.
type Lookuper interface {
	Lookup(key string) (tenure.Member, error)
}

// Server is the dispatcher service of one member. It answers calls for the partitions the
// member's resolver places on it, refers the others to their owner, and forwards through a
// Client.
type Server struct {
	self   string
	lookup Lookuper
	client *Client
	parts  *Partitions
}

// NewServer returns the service of member self, which places partitions with lookup, over tree.
// The server forwards tasks and polls with client.
func NewServer(self string, lookup Lookuper, tree core.Tree, client *Client) *Server {
	s := &Server{self: self, lookup: lookup, client: client}
	s.parts = NewPartitions(tree, s)
	return s
}

// Partitions returns the member's partitions.
func (s *Server) Partitions() *Partitions { return s.parts }

// Register adds the service to g.
func (s *Server) Register(g grpc.ServiceRegistrar) {
	RegisterCodec()
	g.RegisterService(&serviceDesc, s)
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "AddTask", Handler: addTaskHandler},
		{MethodName: "Poll", Handler: pollHandler},
	},
	Metadata: "dispatcher",
}

func addTaskHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(AddTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	s, _ := srv.(*Server)
	call := func(ctx context.Context, req any) (any, error) {
		r, _ := req.(*AddTaskRequest)
		return s.addTask(ctx, r)
	}
	if interceptor == nil {
		return call(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: addTaskMethod}, call)
}

func pollHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(PollRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	s, _ := srv.(*Server)
	call := func(ctx context.Context, req any) (any, error) {
		r, _ := req.(*PollRequest)
		return s.poll(ctx, r)
	}
	if interceptor == nil {
		return call(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: pollMethod}, call)
}

// owns checks that this member serves partition i. When it does not, the error is
// FailedPrecondition with the owner's address in the route trailer; when no member serves it
// now, it is FailedPrecondition without a route.
func (s *Server) owns(ctx context.Context, i int) error {
	key := s.parts.Tree().Key(max(i, 0))
	m, err := s.lookup.Lookup(key)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "no owner for partition %d: %v", i, err)
	}
	if m.ID == s.self {
		return nil
	}
	_ = grpc.SetTrailer(ctx, metadata.Pairs(routeKey, m.Addr))
	return status.Errorf(codes.FailedPrecondition, "partition %d is served by %s", i, m.ID)
}

func (s *Server) addTask(ctx context.Context, req *AddTaskRequest) (*AddTaskResponse, error) {
	if req.Partition < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "partition %d", req.Partition)
	}
	if err := s.owns(ctx, req.Partition); err != nil {
		return nil, err
	}
	if err := s.parts.AddTask(ctx, req.Partition, req.Task); err != nil {
		return nil, statusOf(err)
	}
	return &AddTaskResponse{}, nil
}

func (s *Server) poll(ctx context.Context, req *PollRequest) (*PollResponse, error) {
	if req.Partition < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "partition %d", req.Partition)
	}
	if err := s.owns(ctx, req.Partition); err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		left := time.Until(dl)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, dl.Add(-min(pollMargin, left/4)))
		defer cancel()
	}
	t, err := s.parts.Poll(ctx, req.Partition)
	if err != nil {
		return nil, statusOf(err)
	}
	return &PollResponse{Task: t}, nil
}

// statusOf turns an error of the partitions or of a forward into the status the caller sees. A
// call that changed nothing is FailedPrecondition without a route, which a client reports as
// ErrNotAccepted.
func statusOf(err error) error {
	if _, ok := status.FromError(err); ok && status.Code(err) != codes.Unknown {
		return err
	}
	if errors.Is(err, ErrPartition) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, ErrNotAccepted) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

// owner returns the address of the member that serves partition i according to this member.
func (s *Server) owner(i int) (string, error) {
	m, err := s.lookup.Lookup(s.parts.Tree().Key(i))
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrNotAccepted, err)
	}
	return m.Addr, nil
}

// AddTask implements Forwarder: it sends the task to the owner of the partition.
func (s *Server) AddTask(ctx context.Context, partition int, t Task) error {
	addr, err := s.owner(partition)
	if err != nil {
		return err
	}
	return s.client.AddTask(ctx, addr, AddTaskRequest{Partition: partition, Task: t})
}

// Poll implements Forwarder: it polls the owner of the partition.
func (s *Server) Poll(ctx context.Context, partition int) (*Task, error) {
	addr, err := s.owner(partition)
	if err != nil {
		return nil, err
	}
	return s.client.Poll(ctx, addr, PollRequest{Partition: partition})
}

// Client calls the dispatcher service of the members. It keeps one connection to each address and
// follows the routes the members answer with. Its methods are safe for concurrent use.
type Client struct {
	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

// NewClient returns a Client with no connections; it dials on first use.
func NewClient() *Client {
	RegisterCodec()
	return &Client{conns: make(map[string]*grpc.ClientConn)}
}

// Close closes the connections. Later calls fail.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for addr, cc := range c.conns {
		errs = append(errs, cc.Close())
		delete(c.conns, addr)
	}
	return errors.Join(errs...)
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("dispatcher: client closed")
	}
	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName)),
	)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

// AddTask sends req to the member at addr. When that member answers that another member owns the
// partition, it sends req there, at most maxResends times, and then returns an error that
// matches ErrRoute. It resends only after an answer that says the task was not accepted, so a
// task is never added twice. An error that does not match ErrNotAccepted leaves it open whether
// the task was added.
func (c *Client) AddTask(ctx context.Context, addr string, req AddTaskRequest) error {
	return c.invoke(ctx, addr, addTaskMethod, &req, new(AddTaskResponse))
}

// Poll sends req to the member at addr, following routes as AddTask does. It returns the task
// the poll received, or nil when the poll ended with none.
func (c *Client) Poll(ctx context.Context, addr string, req PollRequest) (*Task, error) {
	var resp PollResponse
	if err := c.invoke(ctx, addr, pollMethod, &req, &resp); err != nil {
		return nil, err
	}
	return resp.Task, nil
}

func (c *Client) invoke(ctx context.Context, addr, method string, req, resp any) error {
	for resends := 0; ; resends++ {
		cc, err := c.conn(addr)
		if err != nil {
			return err
		}
		var trailer metadata.MD
		err = cc.Invoke(ctx, method, req, resp, grpc.Trailer(&trailer))
		if err == nil || status.Code(err) != codes.FailedPrecondition {
			return err
		}
		route := trailer.Get(routeKey)
		if len(route) == 0 {
			return fmt.Errorf("%w: %w", ErrNotAccepted, err)
		}
		if resends == maxResends {
			return fmt.Errorf("%w (after %d resends, last route %s): %w", ErrRoute, maxResends, route[0], err)
		}
		addr = route[0]
	}
}
