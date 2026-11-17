// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/static"
	"github.com/hmdsefi/tenure/viewhttp"
)

// A node serves its views; a client fetches the current one, then long-polls with its ETag.
func ExampleHandler() {
	group, err := static.New(nil)
	if err != nil {
		panic(err)
	}
	node, err := tenure.Start(context.Background(), tenure.Config{
		Self:       tenure.Member{ID: "m1", Addr: "10.0.0.1:8080"},
		Membership: group,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = node.Close(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/tenure/view", viewhttp.Handler(node))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/tenure/view")
	if err != nil {
		panic(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	fmt.Print(resp.Status, "\n", string(body))

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/tenure/view?wait=10ms", nil)
	req.Header.Set("If-None-Match", resp.Header.Get("ETag"))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	_ = resp.Body.Close()
	fmt.Println(resp.Status)
	// Output:
	// 200 OK
	// {"version":2,"members":[{"id":"m1","addr":"10.0.0.1:8080","draining":false,"slot":-1}],"slots":0,"slot_holders":[]}
	// 304 Not Modified
}
