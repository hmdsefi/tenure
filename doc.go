// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package tenure shards single-writer services: each shard of a space has one owner at a time, and every ownership carries a term that a downstream fence can check.
package tenure
