package common

import (
	"strings"
	"sync"
)

var (
	SliceStringPool   = sync.Pool{New: func() any { return []string{} }}
	StringBuilderPool = sync.Pool{New: func() any { return strings.Builder{} }}
)
