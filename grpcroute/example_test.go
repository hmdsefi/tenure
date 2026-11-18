// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"errors"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/grpcroute"
)

// A client opens one channel to the members of a space through a Router, sends each call to
// the owner of its key, and follows the members' redirects.
func ExampleNewResolverBuilder() {
	var router *tenure.Router // from tenure.NewRouter, on the group's membership

	conn, err := grpc.NewClient("tenure:///orders",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(grpcroute.NewResolverBuilder(router)),
		grpc.WithDefaultServiceConfig(grpcroute.ServiceConfig),
		grpc.WithChainUnaryInterceptor(grpcroute.UnaryClientInterceptor(router)),
		grpc.WithChainStreamInterceptor(grpcroute.StreamClientInterceptor(router)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// The call goes to the member that owns "order-17" in the Router's snapshot.
	ctx := grpcroute.WithKey(context.Background(), "order-17")
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: "order-17"})
	if err != nil {
		log.Print(err)
	}
}

// orderServer is a member's service. Its handlers check ownership before they act.
type orderServer struct {
	healthpb.UnimplementedHealthServer
	node *tenure.Node
	self string // the member's own ID
}

// Check stands in for any handler of a call whose key is in the request: here, Service.
func (s *orderServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	sh, err := s.node.Local("orders", req.GetService())
	var notOwner *tenure.NotOwnerError
	switch {
	case err == nil:
	case errors.As(err, &notOwner) && notOwner.Route.ID != "" && notOwner.Route.ID != s.self:
		return nil, grpcroute.Misdirected(ctx, notOwner.Route)
	case errors.As(err, &notOwner), errors.Is(err, tenure.ErrClosed):
		return nil, status.Error(codes.Unavailable, "no member can serve the call yet")
	default:
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := sh.Check(); err != nil { // before each side effect
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// A member answers a call for a key it does not own with Misdirected.
func ExampleMisdirected() {
	var node *tenure.Node // from tenure.Start

	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, &orderServer{node: node, self: "orders-0"})
}
