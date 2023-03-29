package common

import (
	"bytes"
	"strings"
	"sync"
)

var (
	SliceStringPool   = sync.Pool{New: func() any { return []string{} }}
	StringBuilderPool = sync.Pool{New: func() any { return strings.Builder{} }}
	BytesPool         = sync.Pool{New: func() any { return bytes.Buffer{} }}
)
