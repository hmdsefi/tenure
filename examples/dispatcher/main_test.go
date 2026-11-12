// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestMain fails the run when a test leaves a goroutine behind: the clusters, servers and clients
// the tests start must stop every goroutine they started.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestCommand runs the program in process: it exits 0, prints one line for each of the 8
// partitions, and delivers every task.
func TestCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	var out bytes.Buffer
	if err := execute(ctx, []string{"-tasks", "100"}, &out); err != nil {
		t.Fatalf("execute: %v\n%s", err, &out)
	}
	lines := regexp.MustCompile(`(?m)^partition \d: \d+ matches$`).FindAllString(out.String(), -1)
	if len(lines) != 8 {
		t.Fatalf("%d partition lines, want 8:\n%s", len(lines), &out)
	}
	if !strings.Contains(out.String(), "tasks delivered: 100\n") {
		t.Fatalf("no \"tasks delivered: 100\" in:\n%s", &out)
	}
	total := 0
	for _, l := range lines {
		var i, n int
		if _, err := fmt.Sscanf(l, "partition %d: %d matches", &i, &n); err != nil {
			t.Fatal(err)
		}
		total += n
	}
	if total != 100 {
		t.Fatalf("the partitions' matches add up to %d, want 100:\n%s", total, &out)
	}
}

func TestCommandFlags(t *testing.T) {
	var out bytes.Buffer
	if err := execute(t.Context(), []string{"-tasks", "0"}, &out); err == nil {
		t.Fatal("-tasks 0 succeeded")
	}
}

// stubAdder answers each AddTask with the next of errs, and counts the calls.
type stubAdder struct {
	errs  []error
	calls int
}

func (s *stubAdder) AddTask(context.Context, string, AddTaskRequest) error {
	err := s.errs[min(s.calls, len(s.errs)-1)]
	s.calls++
	return err
}

// TestAddResendsOnlyWhenNotAccepted checks the producers' resend rule: a task is sent again after
// an answer that says it was not accepted, and never after an answer that leaves that open.
func TestAddResendsOnlyWhenNotAccepted(t *testing.T) {
	ctx := t.Context()
	req := AddTaskRequest{Partition: 1, Task: Task{ID: "t-1"}}

	notAccepted := &stubAdder{errs: []error{ErrNotAccepted, ErrRoute, nil}}
	if err := add(ctx, notAccepted, "x", req); err != nil || notAccepted.calls != 3 {
		t.Fatalf("after not-accepted answers: err %v, %d calls; want nil and 3", err, notAccepted.calls)
	}

	unsure := errors.New("connection lost")
	open := &stubAdder{errs: []error{unsure, nil}}
	if err := add(ctx, open, "x", req); !errors.Is(err, unsure) || open.calls != 1 {
		t.Fatalf("after an unsure answer: err %v, %d calls; want it returned after 1 call", err, open.calls)
	}
}
