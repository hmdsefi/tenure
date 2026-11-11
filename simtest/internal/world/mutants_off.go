// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !tenure_mutants

package world

// plants reports whether the bug that bug names is planted in a world whose scenario's
// Mutation is m. Without the build tag tenure_mutants no bug ever is: every hook of
// mutants_on.go's list asks plants and gets a constant false, so the compiler drops the code
// each hook guards, and an ordinary build runs the protocol as written.
func (Mutation) plants(Mutation) bool { return false }
