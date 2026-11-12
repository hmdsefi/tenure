// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

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
