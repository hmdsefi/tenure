// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"
)

var errMissing = errors.New("missing")

func lookup(t, u time.Time, d time.Duration, names []string, ids []int, a, b float64, key string) (string, error) {
	deadline := t.Add(d*2 + time.Second)
	if deadline.Compare(u) < 0 {
		return "", fmt.Errorf("lookup %q: %w", key, errMissing)
	}
	slices.SortStableFunc(ids, func(x, y int) int { return x - y })
	sort.Strings(names)
	ratio := math.Ceil(a / b)
	return strconv.Itoa(len(ids) + int(ratio)), nil
}
