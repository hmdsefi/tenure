// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"net"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/grpcroute"
)

// misdirectingHealth is a health service whose Check answers with Misdirected(ctx, route).
type misdirectingHealth struct {
	healthpb.UnimplementedHealthServer
	route tenure.Member
}

func (h misdirectingHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return nil, grpcroute.Misdirected(ctx, h.route)
}

// checkMisdirected calls a server that answers with Misdirected(ctx, route) and returns the
// call's status and trailer.
func checkMisdirected(t *testing.T, route tenure.Member) (*status.Status, metadata.MD) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, misdirectingHealth{route: route})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var trailer metadata.MD
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Trailer(&trailer))
	return status.Convert(err), trailer
}

// TestRTR920Misdirected checks the answer of Misdirected: FailedPrecondition with the owner's
// address in the tenure-route trailer.
func TestRTR920Misdirected(t *testing.T) {
	st, trailer := checkMisdirected(t, tenure.Member{ID: "n2", Addr: "10.0.0.2:8080"})
	if st.Code() != codes.FailedPrecondition || st.Message() != "tenure: misdirected call: send it to 10.0.0.2:8080" {
		t.Errorf("status = %v %q, want FailedPrecondition with the address", st.Code(), st.Message())
	}
	if got := trailer.Get(grpcroute.RouteTrailer); !reflect.DeepEqual(got, []string{"10.0.0.2:8080"}) {
		t.Errorf("tenure-route = %q, want [10.0.0.2:8080]", got)
	}
}

// TestRTR920MisdirectedNoAddr checks that Misdirected answers Unavailable without a trailer for a
// member with no address.
func TestRTR920MisdirectedNoAddr(t *testing.T) {
	st, trailer := checkMisdirected(t, tenure.Member{ID: "n2"})
	if st.Code() != codes.Unavailable || st.Message() != "tenure: no member can serve the call yet" {
		t.Errorf("status = %v %q, want Unavailable", st.Code(), st.Message())
	}
	if got := trailer.Get(grpcroute.RouteTrailer); got != nil {
		t.Errorf("tenure-route = %q, want none", got)
	}
}

// TestRTR920MisdirectedOutsideCall checks that Misdirected outside a server call still returns
// its error.
func TestRTR920MisdirectedOutsideCall(t *testing.T) {
	err := grpcroute.Misdirected(context.Background(), tenure.Member{ID: "n2", Addr: "10.0.0.2:8080"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Misdirected = %v, want FailedPrecondition", err)
	}
}
