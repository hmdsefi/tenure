// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// TestNoncesvcHTTP checks that the HTTP clients over the nonce service give the results of the
// in-memory Nonces and chain, including stale, conflict and gap.
func TestNoncesvcHTTP(t *testing.T) {
	ctx := context.Background()
	mem := NewNonces(fencedb.New())
	memChain := NewChain()

	remoteChain := NewChain()
	srv := httptest.NewServer(NewNoncesvc(NewNonces(fencedb.New()), remoteChain))
	t.Cleanup(srv.Close)
	nonces := NewHTTPNonces(srv.URL+"/", srv.Client())
	chain := NewHTTPChain(srv.URL, srv.Client())

	both := func(name string, f func(Nonces) (uint64, error)) {
		t.Helper()
		want, wantErr := f(mem)
		got, gotErr := f(nonces)
		if got != want || (wantErr == nil) != (gotErr == nil) ||
			errors.Is(wantErr, ErrStale) != errors.Is(gotErr, ErrStale) {
			t.Fatalf("%s: HTTP gave (%d, %v), memory (%d, %v)", name, got, gotErr, want, wantErr)
		}
	}

	both("raise 2", func(n Nonces) (uint64, error) { return 0, n.Raise(ctx, 0, 2) })
	both("reserve w-00", func(n Nonces) (uint64, error) { return n.Reserve(ctx, 0, 2, "w-00") })
	both("reserve w-00 again", func(n Nonces) (uint64, error) { return n.Reserve(ctx, 0, 2, "w-00") })
	both("reserve w-01", func(n Nonces) (uint64, error) { return n.Reserve(ctx, 0, 3, "w-01") })
	both("stale reserve", func(n Nonces) (uint64, error) { return n.Reserve(ctx, 0, 1, "w-00") })
	both("stale raise", func(n Nonces) (uint64, error) { return 0, n.Raise(ctx, 0, 1) })
	both("reserve w-00 after stale", func(n Nonces) (uint64, error) { return n.Reserve(ctx, 0, 3, "w-00") })

	if _, err := nonces.Reserve(ctx, 0, 1, "w-00"); !errors.Is(err, ErrStale) {
		t.Errorf("stale reserve: error %v, want ErrStale", err)
	}
	if err := nonces.Raise(ctx, 0, 1); !errors.Is(err, ErrStale) {
		t.Errorf("stale raise: error %v, want ErrStale", err)
	}

	tx := func(nonce uint64) Tx {
		return Tx{Wallet: "w-00", Nonce: nonce, Payload: "p", Sig: Signature("w-00", nonce, "p")}
	}
	steps := []struct {
		nonce uint64
		want  error
	}{{0, nil}, {1, nil}, {0, ErrConflict}, {5, ErrGap}, {2, nil}}
	for _, s := range steps {
		memErr := memChain.Broadcast(ctx, tx(s.nonce))
		err := chain.Broadcast(ctx, tx(s.nonce))
		if !errors.Is(memErr, s.want) || !errors.Is(err, s.want) || (err == nil) != (s.want == nil) {
			t.Errorf("broadcast nonce %d: HTTP %v, memory %v, want %v", s.nonce, err, memErr, s.want)
		}
	}
	rep, err := chain.Report(ctx)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if want := memChain.Report(); !reflect.DeepEqual(rep, want) {
		t.Errorf("HTTP report %+v, memory report %+v", rep, want)
	}
	if rep.Accepted != 3 || len(rep.Conflicts) != 1 || len(rep.Gaps) != 1 {
		t.Errorf("report %+v, want 3 accepted, 1 conflict, 1 gap", rep)
	}
	if got := remoteChain.Report(); !reflect.DeepEqual(got, rep) {
		t.Errorf("service chain %+v differs from the report %+v", got, rep)
	}
}

func TestNoncesvcBadRequests(t *testing.T) {
	srv := httptest.NewServer(NewNoncesvc(NewNonces(fencedb.New()), NewChain()))
	t.Cleanup(srv.Close)
	cases := []struct {
		name, path, body string
		want             int
	}{
		{"malformed", ReservePath, `{`, http.StatusBadRequest},
		{"unknown field", RaisePath, `{"shard":0,"term":1,"x":1}`, http.StatusBadRequest},
		{"trailing data", RaisePath, `{"shard":0,"term":1} {}`, http.StatusBadRequest},
		{"empty wallet", ReservePath, `{"shard":0,"term":1}`, http.StatusBadRequest},
		{"bad tx", BroadcastPath, `[]`, http.StatusBadRequest},
		{"too large", BroadcastPath, `{"payload":"` + strings.Repeat("a", maxBody) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		resp, err := srv.Client().Post(srv.URL+c.path, "application/json", strings.NewReader(c.body))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: status %d, want %d", c.name, resp.StatusCode, c.want)
		}
	}

	hc := NewHTTPChain(srv.URL, nil)
	if _, err := hc.Report(context.Background()); err != nil {
		t.Errorf("report with the default client: %v", err)
	}
	down := NewHTTPNonces("http://127.0.0.1:1", nil)
	if err := down.Raise(context.Background(), 0, 1); err == nil {
		t.Error("raise against a closed port succeeded")
	}
}
