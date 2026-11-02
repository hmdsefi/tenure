// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import (
	_ "cmp"
	_ "encoding/binary" // want DT001
	_ "hash/maphash"    // want DT001
	_ "math/rand"       // want DT001
	_ "os"              // want DT001
	_ "strings"
	_ "sync" // want DT001
	_ "time"
	_ "unsafe" // want DT001
)
